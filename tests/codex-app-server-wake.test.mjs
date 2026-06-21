import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { WebSocketServer } from "ws";
import { WakeMonitor, normalizeWakeConfig } from "../scripts/wake-monitor.mjs";
import { buildCodexTurnText, buildTurnStartRequest, CodexAppServerClient, createCodexAppServerInjector } from "../scripts/codex-app-server-wake.mjs";

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

test("Codex app-server injector fails loud without socket", async () => {
  const injector = createCodexAppServerInjector();

  await assert.rejects(() => injector(payload, { mode: "codex_app_server", threadId: "thread-1" }), /socket-missing/);
});
