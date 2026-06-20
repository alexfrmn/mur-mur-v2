import test from "node:test";
import assert from "node:assert/strict";
import { WakeMonitor } from "../scripts/wake-monitor.mjs";

const message = (msgId, cursor, text = msgId) => ({
  from: "agent-a",
  text,
  msgId,
  conversationId: "conv-1",
  ts: `2026-06-20T00:00:${String(cursor).padStart(2, "0")}.000Z`,
  cursor,
});

test("WakeMonitor calls hook once for a new msgId", async () => {
  const calls = [];
  const monitor = new WakeMonitor({
    hook: async (payload) => calls.push(payload.msgId),
    now: () => 1000,
  });

  await monitor.onInbound(message("msg-1", 1));

  assert.deepEqual(calls, ["msg-1"]);
});

test("WakeMonitor drops duplicate msgId within cooldown window", async () => {
  const calls = [];
  let now = 1000;
  const monitor = new WakeMonitor({
    dedup: { cooldownMs: 300000 },
    hook: async (payload) => calls.push(payload.msgId),
    now: () => now,
  });

  await monitor.onInbound(message("msg-1", 1));
  now += 1000;
  await monitor.onInbound(message("msg-1", 2));

  assert.deepEqual(calls, ["msg-1"]);
});

test("WakeMonitor drains inbound backlog FIFO until idle", async () => {
  const calls = [];
  const backlog = [
    message("msg-2", 2),
    message("msg-3", 3),
  ];
  const monitor = new WakeMonitor({
    hook: async (payload) => calls.push(payload.msgId),
    loadBacklogAfter: async (cursor) => {
      const rows = backlog.filter((row) => row.cursor > cursor);
      backlog.length = 0;
      return rows;
    },
    now: () => 1000,
  });

  await monitor.onInbound(message("msg-1", 1));

  assert.deepEqual(calls, ["msg-1", "msg-2", "msg-3"]);
});
