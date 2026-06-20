import test from "node:test";
import assert from "node:assert/strict";
import { WakeMonitor } from "../scripts/wake-monitor.mjs";

const message = (msgId, cursor, from = "agent-a") => ({
  from,
  text: msgId,
  msgId,
  conversationId: "conv-guards",
  ts: `2026-06-20T00:01:${String(cursor).padStart(2, "0")}.000Z`,
  cursor,
});

test("WakeMonitor loop-breaker trips after maxWakes and suspends sender", async () => {
  const calls = [];
  const notifications = [];
  let now = 1000;
  const monitor = new WakeMonitor({
    loopBreaker: { maxWakes: 2, windowMs: 60000 },
    hook: async (payload) => calls.push(payload.msgId),
    notify: async (payload, reason) => notifications.push({ msgId: payload.msgId, reason }),
    now: () => now,
  });

  await monitor.onInbound(message("msg-1", 1));
  now += 1000;
  await monitor.onInbound(message("msg-2", 2));
  now += 1000;
  await monitor.onInbound(message("msg-3", 3));
  now += 1000;
  await monitor.onInbound(message("msg-4", 4));

  assert.deepEqual(calls, ["msg-1", "msg-2"]);
  assert.deepEqual(notifications.map((n) => n.msgId), ["msg-3", "msg-4"]);
  assert.ok(notifications.every((n) => n.reason === "loop-breaker"));
});

test("WakeMonitor loop-breaker suspend clears after quiet window", async () => {
  const calls = [];
  const notifications = [];
  let now = 1000;
  const monitor = new WakeMonitor({
    loopBreaker: { maxWakes: 1, windowMs: 60000 },
    hook: async (payload) => calls.push(payload.msgId),
    notify: async (payload, reason) => notifications.push({ msgId: payload.msgId, reason }),
    now: () => now,
  });

  await monitor.onInbound(message("msg-1", 1));
  now += 1000;
  await monitor.onInbound(message("msg-2", 2));
  now += 61000;
  await monitor.onInbound(message("msg-3", 3));

  assert.deepEqual(calls, ["msg-1", "msg-3"]);
  assert.deepEqual(notifications.map((n) => n.msgId), ["msg-2"]);
});

test("WakeMonitor audit deny skips hook", async () => {
  const calls = [];
  const monitor = new WakeMonitor({
    auditHook: async () => "deny",
    hook: async (payload) => calls.push(payload.msgId),
    now: () => 1000,
  });

  await monitor.onInbound(message("msg-1", 1));

  assert.deepEqual(calls, []);
});

test("WakeMonitor audit require_approval skips hook and notifies", async () => {
  const calls = [];
  const notifications = [];
  const monitor = new WakeMonitor({
    auditHook: async () => "require_approval",
    hook: async (payload) => calls.push(payload.msgId),
    notify: async (payload, reason) => notifications.push({ msgId: payload.msgId, reason }),
    now: () => 1000,
  });

  await monitor.onInbound(message("msg-1", 1));

  assert.deepEqual(calls, []);
  assert.deepEqual(notifications, [{ msgId: "msg-1", reason: "require_approval" }]);
});

test("WakeMonitor audit allow calls hook", async () => {
  const calls = [];
  const monitor = new WakeMonitor({
    auditHook: async () => "allow",
    hook: async (payload) => calls.push(payload.msgId),
    now: () => 1000,
  });

  await monitor.onInbound(message("msg-1", 1));

  assert.deepEqual(calls, ["msg-1"]);
});
