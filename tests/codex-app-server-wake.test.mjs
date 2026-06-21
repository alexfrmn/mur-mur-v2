import net from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { WakeMonitor, normalizeWakeConfig } from "../scripts/wake-monitor.mjs";
import { buildCodexTurnText, CodexAppServerClient, createCodexAppServerInjector } from "../scripts/codex-app-server-wake.mjs";

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

test("Codex app-server injector sends turn/start over Unix socket", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "murmur-codex-wake-"));
  const socketPath = path.join(dir, "codex.sock");
  const received = [];
  const server = net.createServer((socket) => {
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      for (const line of chunk.split(/\r?\n/).filter(Boolean)) {
        const request = JSON.parse(line);
        received.push(request);
        socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: request.id, result: { turn: { id: "turn-1" } } })}\n`);
      }
    });
  });
  server.listen(socketPath);
  await once(server, "listening");

  const client = new CodexAppServerClient({ socketPath });
  const result = await client.request("turn/start", {
    threadId: "thread-1",
    input: [{ type: "text", text: buildCodexTurnText(payload), text_elements: [] }],
  });

  server.close();
  assert.deepEqual(result, { turn: { id: "turn-1" } });
  assert.equal(received[0].method, "turn/start");
  assert.equal(received[0].params.threadId, "thread-1");
  assert.match(received[0].params.input[0].text, /msgId=msg-codex-1/);
  assert.match(received[0].params.input[0].text, /hello codex/);
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

test("Codex app-server injector fails loud without socket or thread", async () => {
  const injector = createCodexAppServerInjector();

  await assert.rejects(() => injector(payload, { mode: "codex_app_server", threadId: "thread-1" }), /socket-missing/);
  await assert.rejects(() => injector(payload, { mode: "codex_app_server", socketPath: "/tmp/codex.sock" }), /thread-missing/);
});
