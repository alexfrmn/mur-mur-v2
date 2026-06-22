import test from "node:test";
import assert from "node:assert/strict";
import {
  buildReplyMatcher,
  waitForReply,
} from "../packages/mcp-server/dist/src/request-reply.js";

const reply = (msgId) => ({
  id: msgId,
  conversationId: "conv-1",
  msgId,
  direction: "inbound",
  sender: "agent.b",
  text: "pong",
  createdAt: new Date().toISOString(),
  transport: "nats",
});

// --- buildReplyMatcher -------------------------------------------------------

test("buildReplyMatcher matches same conversation + peer, rejects others", () => {
  const match = buildReplyMatcher("conv-1", "agent.b");
  assert.equal(match({ conversationId: "conv-1", senderAgentId: "agent.b" }), true);
  assert.equal(match({ conversationId: "conv-1", senderAgentId: "agent.c" }), false);
  assert.equal(match({ conversationId: "conv-2", senderAgentId: "agent.b" }), false);
});

// --- A: durability when live-wait is OFF (pure store polling, no signal) ------

test("A: resolves via store-poll fallback when no wake signal is wired", async () => {
  let calls = 0;
  const res = await waitForReply({
    checkStore: async () => (++calls >= 2 ? reply("r-poll") : null),
    pollMs: 20,
    graceMs: 5,
    deadline: Date.now() + 2000,
    // onSignal intentionally omitted — proves durability without NATS
  });
  assert.equal(res?.msgId, "r-poll");
  assert.ok(calls >= 2, `expected >=2 store checks, got ${calls}`);
});

// --- B: timeout returns null (caller maps to status:timeout) ------------------

test("B: returns null on timeout when no reply ever lands", async () => {
  let calls = 0;
  const res = await waitForReply({
    checkStore: async () => {
      calls++;
      return null;
    },
    pollMs: 20,
    graceMs: 5,
    deadline: Date.now() + 120,
  });
  assert.equal(res, null);
  assert.ok(calls >= 2, `expected several poll attempts, got ${calls}`);
});

// --- C: wake signal accelerates the wait past a long poll interval ------------

test("C: a wake signal resolves the reply faster than the poll interval", async () => {
  let calls = 0;
  let fired = false;
  const start = Date.now();
  const res = await waitForReply({
    checkStore: async () => (++calls >= 2 ? reply("r-signal") : null),
    pollMs: 10_000, // a pure poll would never re-check within the deadline
    graceMs: 5,
    deadline: Date.now() + 1000,
    onSignal: (wake) => {
      setTimeout(() => {
        fired = true;
        wake();
      }, 30);
    },
  });
  const elapsed = Date.now() - start;
  assert.equal(res?.msgId, "r-signal");
  assert.ok(fired, "wake signal should have fired");
  assert.ok(elapsed < 500, `signal path should resolve fast, took ${elapsed}ms`);
});

test("C2: without a signal the same long-poll setup times out", async () => {
  let calls = 0;
  const res = await waitForReply({
    checkStore: async () => (++calls >= 2 ? reply("never") : null),
    pollMs: 10_000,
    graceMs: 5,
    deadline: Date.now() + 150, // shorter than poll interval → only one check
  });
  assert.equal(res, null);
});

// --- D: lost-wakeup safety (signal fires DURING the store check) --------------

test("D: signal fired during checkStore is not lost (armed before check)", async () => {
  let calls = 0;
  let wakeCb = null;
  const res = await waitForReply({
    checkStore: async () => {
      calls++;
      if (calls === 1 && wakeCb) wakeCb(); // fire after arm, before the race
      return calls >= 2 ? reply("r-lostwake") : null;
    },
    pollMs: 10_000,
    graceMs: 5,
    deadline: Date.now() + 1000,
    onSignal: (wake) => {
      wakeCb = wake;
    },
  });
  assert.equal(res?.msgId, "r-lostwake");
  assert.equal(calls, 2);
});
