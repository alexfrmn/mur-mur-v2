import test from "node:test";
import assert from "node:assert/strict";
import { WakeMonitor } from "../scripts/wake-monitor.mjs";

const message = (msgId, cursor, from = "agent-a") => ({
  from,
  text: `payload:${msgId}`,
  msgId,
  conversationId: "conv-persistent",
  ts: `2026-06-20T00:02:${String(cursor).padStart(2, "0")}.000Z`,
  cursor,
});

test("WakeMonitor persistent mode injects text and a separate Enter event", async () => {
  const events = [];
  const monitor = new WakeMonitor({
    peers: { "agent-a": { mode: "persistent", target: "codex:1" } },
    injector: async (payload, peer) => {
      events.push({ type: "text", target: peer.target, text: payload.text });
      events.push({ type: "enter", target: peer.target });
    },
    now: () => 1000,
  });

  await monitor.onInbound(message("msg-1", 1));

  assert.deepEqual(events, [
    { type: "text", target: "codex:1", text: "payload:msg-1" },
    { type: "enter", target: "codex:1" },
  ]);
});

test("WakeMonitor persistent mode is gated by dedup, loop-breaker, and audit", async () => {
  const injected = [];
  const notifications = [];
  let now = 1000;
  const monitor = new WakeMonitor({
    peers: { "agent-a": { mode: "persistent", target: "codex:1" } },
    dedup: { cooldownMs: 300000 },
    loopBreaker: { maxWakes: 1, windowMs: 60000 },
    auditHook: async (payload) => payload.msgId === "msg-deny" ? "deny" : "allow",
    injector: async (payload) => injected.push(payload.msgId),
    notify: async (payload, reason) => notifications.push({ msgId: payload.msgId, reason }),
    now: () => now,
  });

  await monitor.onInbound(message("msg-1", 1));
  now += 1000;
  await monitor.onInbound(message("msg-1", 2));
  now += 1000;
  await monitor.onInbound(message("msg-2", 3));
  now += 61000;
  await monitor.onInbound(message("msg-deny", 4));

  assert.deepEqual(injected, ["msg-1"]);
  assert.deepEqual(notifications, [{ msgId: "msg-2", reason: "loop-breaker" }]);
});

test("WakeMonitor keeps stateless peers on hook path", async () => {
  const hooks = [];
  const injected = [];
  const monitor = new WakeMonitor({
    peers: { "agent-a": { mode: "stateless" } },
    hook: async (payload) => hooks.push(payload.msgId),
    injector: async (payload) => injected.push(payload.msgId),
    now: () => 1000,
  });

  await monitor.onInbound(message("msg-1", 1));

  assert.deepEqual(hooks, ["msg-1"]);
  assert.deepEqual(injected, []);
});
