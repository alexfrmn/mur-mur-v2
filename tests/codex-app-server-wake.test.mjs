import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { WebSocketServer } from "ws";
import { ChannelRosterStore } from "../packages/core/dist/src/index.js";
import { WakeMonitor, normalizeWakeConfig } from "../scripts/wake-monitor.mjs";
import {
  buildCodexTurnText,
  buildThreadStartParams,
  buildTurnStartRequest,
  CodexAppServerClient,
  createChannelThreadStartBindingResolver,
  createCodexAppServerInjector,
} from "../scripts/codex-app-server-wake.mjs";

const payload = {
  from: "agent-jarvis",
  text: "hello codex",
  msgId: "msg-codex-1",
  conversationId: "codex:task:test",
  cursor: 1,
};

test("normalizeWakeConfig accepts Codex app-server peer settings", () => {
  const config = normalizeWakeConfig({
    wake: {
      peers: {
        "agent-jarvis": {
          mode: "codex_app_server",
          socketPath: "/tmp/codex.sock",
          threadId: "thread-1",
        },
      },
    },
  });

  assert.deepEqual(config.peers["agent-jarvis"], {
    mode: "codex_app_server",
    socketPath: "/tmp/codex.sock",
    threadId: "thread-1",
  });
});

test("buildTurnStartRequest builds Codex turn/start params", () => {
  const request = buildTurnStartRequest({
    id: 7,
    threadId: "thread-1",
    text: buildCodexTurnText(payload),
    metadata: { murmur_msg_id: payload.msgId },
  });

  assert.equal(request.id, 7);
  assert.equal(request.method, "turn/start");
  assert.equal(request.params.threadId, "thread-1");
  assert.equal(request.params.responsesapiClientMetadata.murmur_msg_id, payload.msgId);
  assert.match(request.params.input[0].text, /msgId=msg-codex-1/);
});

test("buildThreadStartParams applies optional channel personality binding", () => {
  const params = buildThreadStartParams({
    model: "gpt-5",
    personality: "codex-writer",
    baseInstructions: "Write concise engineering notes.",
    metadata: { murmur_channel_id: "chan-1" },
  });

  assert.equal(params.model, "gpt-5");
  assert.equal(params.personality, "codex-writer");
  assert.equal(params.baseInstructions, "Write concise engineering notes.");
  assert.equal(params.modelProvider, null);
  assert.equal(params.ephemeral, false);
});

test("buildThreadStartParams preserves legacy nulled defaults without binding", () => {
  const params = buildThreadStartParams();

  assert.equal(params.model, null);
  assert.equal(params.personality, null);
  assert.equal(params.baseInstructions, null);
  assert.equal(params.modelProvider, null);
  assert.equal(params.ephemeral, false);
});

test("Codex app-server client initializes before turn/start over WS-over-UDS", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "murmur-codex-wake-"));
  const socketPath = path.join(dir, "codex.sock");
  const received = [];
  const httpServer = http.createServer();
  const wsServer = new WebSocketServer({ server: httpServer });

  wsServer.on("connection", (socket) => {
    socket.on("message", (data) => {
      const request = JSON.parse(data.toString("utf8"));
      received.push(request);
      if (request.method === "initialize") {
        socket.send(JSON.stringify({ id: request.id, result: { protocolVersion: "0.1.0" } }));
      }
      if (request.method === "turn/start") {
        socket.send(JSON.stringify({ id: request.id, result: { turn: { id: "turn-1" } } }));
      }
    });
  });
  httpServer.listen(socketPath);
  await once(httpServer, "listening");

  const client = new CodexAppServerClient({ socketPath });
  const result = await client.request("turn/start", {
    threadId: "thread-1",
    input: [{ type: "text", text: buildCodexTurnText(payload), text_elements: [] }],
  });

  wsServer.close();
  httpServer.close();

  assert.deepEqual(result, { turn: { id: "turn-1" } });
  assert.equal(received[0].method, "initialize");
  assert.equal(received[0].params.clientInfo.name, "murmur-codex-app-server-wake");
  assert.equal(received[1].method, "initialized");
  assert.equal(received[2].method, "turn/start");
  assert.equal(received[2].params.threadId, "thread-1");
  assert.match(received[2].params.input[0].text, /msgId=msg-codex-1/);
  assert.match(received[2].params.input[0].text, /hello codex/);
});

test("Codex app-server client fails loud on initialize errors", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "murmur-codex-wake-"));
  const socketPath = path.join(dir, "codex.sock");
  const httpServer = http.createServer();
  const wsServer = new WebSocketServer({ server: httpServer });

  wsServer.on("connection", (socket) => {
    socket.on("message", (data) => {
      const request = JSON.parse(data.toString("utf8"));
      if (request.method === "initialize") {
        socket.send(JSON.stringify({ id: request.id, error: { message: "denied" } }));
      }
    });
  });
  httpServer.listen(socketPath);
  await once(httpServer, "listening");

  const client = new CodexAppServerClient({ socketPath });
  await assert.rejects(
    () => client.request("turn/start", { threadId: "thread-1", input: [] }),
    /codex-app-server-initialize-error:denied/,
  );

  wsServer.close();
  httpServer.close();
});

test("Codex app-server client reports close before response", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "murmur-codex-wake-"));
  const socketPath = path.join(dir, "codex.sock");
  const httpServer = http.createServer();
  const wsServer = new WebSocketServer({ server: httpServer });

  wsServer.on("connection", (socket) => {
    socket.close();
  });
  httpServer.listen(socketPath);
  await once(httpServer, "listening");

  const client = new CodexAppServerClient({ socketPath });
  await assert.rejects(
    () => client.request("turn/start", { threadId: "thread-1", input: [] }),
    /codex-app-server-closed-before-response:.*:before-initialize/,
  );

  wsServer.close();
  httpServer.close();
});

test("WakeMonitor gates Codex app-server wake before injector", async () => {
  const injected = [];
  let now = 1000;
  const monitor = new WakeMonitor({
    peers: {
      "agent-jarvis": {
        mode: "codex_app_server",
        socketPath: "/tmp/codex.sock",
        threadId: "thread-1",
      },
    },
    dedup: { cooldownMs: 300000 },
    loopBreaker: { maxWakes: 1, windowMs: 60000 },
    auditHook: async (item) => item.msgId === "msg-deny" ? "deny" : "allow",
    injector: async (item, peer) => injected.push({ msgId: item.msgId, mode: peer.mode, threadId: peer.threadId }),
    now: () => now,
  });

  await monitor.onInbound(payload);
  now += 1000;
  await monitor.onInbound({ ...payload, cursor: 2 });
  now += 61000;
  await monitor.onInbound({ ...payload, msgId: "msg-deny", cursor: 3 });

  assert.deepEqual(injected, [{ msgId: "msg-codex-1", mode: "codex_app_server", threadId: "thread-1" }]);
});

test("Codex app-server injector re-seeds stale app-server threads", async () => {
  const calls = [];
  const logs = [];
  class FakeClient {
    async request(method, params) {
      calls.push({ method, params });
      if (method === "turn/start" && params.threadId === "stale-thread") {
        throw new Error("codex-app-server-error:thread not found: stale-thread");
      }
      if (method === "thread/start") return { thread: { id: "fresh-thread" } };
      return { turn: { id: "turn-1" } };
    }
  }
  const peer = { mode: "codex_app_server", socketPath: "/tmp/codex.sock", threadId: "stale-thread" };
  const injector = createCodexAppServerInjector({
    Client: FakeClient,
    log: (level, message, data) => logs.push({ level, message, data }),
  });

  const result = await injector(payload, peer);

  assert.deepEqual(result, { turn: { id: "turn-1" } });
  assert.equal(peer.threadId, "fresh-thread");
  assert.deepEqual(calls.map((call) => call.method), ["turn/start", "thread/start", "turn/start"]);
  assert.equal(calls[0].params.threadId, "stale-thread");
  assert.equal(calls[2].params.threadId, "fresh-thread");
  assert.equal(logs[0].message, "Codex app-server wake thread re-seeded");
});

test("Codex app-server injector seeds missing app-server threads", async () => {
  const calls = [];
  class FakeClient {
    async request(method, params) {
      calls.push({ method, params });
      if (method === "thread/start") return { thread: { id: "fresh-thread" } };
      return { turn: { id: "turn-1" } };
    }
  }
  const peer = { mode: "codex_app_server", socketPath: "/tmp/codex.sock" };
  const injector = createCodexAppServerInjector({ Client: FakeClient });

  const result = await injector(payload, peer);

  assert.deepEqual(result, { turn: { id: "turn-1" } });
  assert.equal(peer.threadId, "fresh-thread");
  assert.deepEqual(calls.map((call) => call.method), ["thread/start", "turn/start"]);
  assert.equal(calls[1].params.threadId, "fresh-thread");
});

test("Codex app-server injector seeds thread with resolved channel member binding", async () => {
  const calls = [];
  class FakeClient {
    async request(method, params) {
      calls.push({ method, params });
      if (method === "thread/start") return { thread: { id: "fresh-thread" } };
      return { turn: { id: "turn-1" } };
    }
  }
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "murmur-codex-roster-"));
  const roster = new ChannelRosterStore(path.join(dir, "channel-roster.db"));
  roster.createChannel({
    channelId: "chan:codex:writer",
    conversationId: payload.conversationId,
    type: "dm",
    members: [{
      memberId: "codex-writer",
      memberSlot: "agent-codex-volt:writer",
      agentId: "agent-codex-volt",
      personaId: "codex-writer",
      model: "gpt-5",
      baseInstructionsHash: "sha256:writer-v1",
    }],
  });
  const injector = createCodexAppServerInjector({
    Client: FakeClient,
    resolveThreadStartBinding: createChannelThreadStartBindingResolver({
      rosterStore: roster,
      agentId: "agent-codex-volt",
      baseInstructionsResolver: () => "Write concise engineering notes.",
    }),
  });

  const result = await injector(payload, { mode: "codex_app_server", socketPath: "/tmp/codex.sock" });

  assert.deepEqual(result, { turn: { id: "turn-1" } });
  assert.equal(calls[0].method, "thread/start");
  assert.equal(calls[0].params.model, "gpt-5");
  assert.equal(calls[0].params.personality, "codex-writer");
  assert.equal(calls[0].params.baseInstructions, "Write concise engineering notes.");
  assert.equal(calls[1].method, "turn/start");
  assert.equal(calls[1].params.responsesapiClientMetadata.murmur_channel_id, "chan:codex:writer");
  assert.equal(calls[1].params.responsesapiClientMetadata.murmur_member_id, "codex-writer");
  assert.equal(calls[1].params.responsesapiClientMetadata.murmur_base_instructions_hash, "sha256:writer-v1");
});

test("channel thread-start binding resolver returns null without member or agent identity", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "murmur-codex-roster-no-id-"));
  const roster = new ChannelRosterStore(path.join(dir, "channel-roster.db"));
  roster.createChannel({
    channelId: "chan:codex:no-id",
    conversationId: payload.conversationId,
    type: "dm",
    members: [{ memberId: "codex-writer", agentId: "agent-codex-volt" }],
  });
  const resolveBinding = createChannelThreadStartBindingResolver({ rosterStore: roster });

  const binding = await resolveBinding(payload, {});

  assert.equal(binding, null);
  roster.close();
});

test("Codex app-server injector ignores remote payload thread-start binding", async () => {
  const calls = [];
  class FakeClient {
    async request(method, params) {
      calls.push({ method, params });
      if (method === "thread/start") return { thread: { id: "fresh-thread" } };
      return { turn: { id: "turn-1" } };
    }
  }
  const injector = createCodexAppServerInjector({ Client: FakeClient });

  await injector({
    ...payload,
    threadStartBinding: {
      model: "remote-controlled-model",
      personality: "remote-controlled-persona",
      baseInstructions: "remote instructions",
    },
  }, { mode: "codex_app_server", socketPath: "/tmp/codex.sock" });

  assert.equal(calls[0].method, "thread/start");
  assert.equal(calls[0].params.model, null);
  assert.equal(calls[0].params.personality, null);
  assert.equal(calls[0].params.baseInstructions, null);
});

test("Codex app-server injector fails loud without socket", async () => {
  const injector = createCodexAppServerInjector();

  await assert.rejects(() => injector(payload, { mode: "codex_app_server", threadId: "thread-1" }), /socket-missing/);
});
