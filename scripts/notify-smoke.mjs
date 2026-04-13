#!/usr/bin/env node
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { NotifyQueue, flushNotifyQueue } from "./notify-router.mjs";

const dir = mkdtempSync(path.join(tmpdir(), "murmur-notify-smoke-"));
const dbPath = path.join(dir, "murmur.db");

const received = [];
const server = createServer((req, res) => {
  if (req.method !== "POST") {
    res.statusCode = 405;
    return res.end();
  }
  let data = "";
  req.on("data", (c) => { data += c; });
  req.on("end", () => {
    try {
      received.push(JSON.parse(data));
    } catch {
      received.push({ raw: data });
    }
    res.statusCode = 200;
    res.end("ok");
  });
});

const listen = (port) => new Promise((resolve) => server.listen(port, "127.0.0.1", resolve));
const close = () => new Promise((resolve) => server.close(resolve));

try {
  await listen(0);
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  assert.ok(port > 0, "server port should be allocated");

  const queue = new NotifyQueue(dbPath);
  const payload = {
    from: "agent-a",
    text: "hello smoke",
    msgId: "smoke-msg-1",
    conversationId: "conv-smoke",
    ts: new Date().toISOString(),
  };
  const targets = [{ type: "webhook", channel: "smoke", url: `http://127.0.0.1:${port}/hook`, headers: {} }];

  queue.enqueueMessage(payload, targets);
  queue.enqueueMessage(payload, targets); // idempotent insert

  await flushNotifyQueue({
    queue,
    log: () => {},
    limit: 10,
  });

  assert.equal(received.length, 1, "should notify exactly once for same msg/channel");
  assert.equal(received[0].msgId, payload.msgId);
  assert.equal(queue.pendingCount(), 0);

  console.log("[notify-smoke] PASS");
} finally {
  await close().catch(() => {});
  rmSync(dir, { recursive: true, force: true });
}
