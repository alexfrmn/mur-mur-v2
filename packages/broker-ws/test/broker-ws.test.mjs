import test from "node:test";
import assert from "node:assert/strict";
import { WebSocket } from "ws";
import { InMemoryDedupeStore } from "../../core/dist/src/index.js";
import { WebSocketBroker, WebSocketRelay, wsSubjectMatches } from "../dist/src/index.js";

function envelope(overrides = {}) {
  return {
    schemaVersion: "1.0",
    msgId: "msg-1",
    conversationId: "conv-1",
    senderAgentId: "alice",
    recipients: ["bob"],
    createdAt: "2026-06-21T23:10:00.000Z",
    payloadCiphertext: "cipher",
    payloadNonce: "nonce",
    signature: "sig",
    ...overrides,
  };
}

async function eventually(fn, timeoutMs = 1500) {
  const start = Date.now();
  let lastErr;
  while (Date.now() - start < timeoutMs) {
    try {
      return fn();
    } catch (err) {
      lastErr = err;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  throw lastErr;
}

async function withRelay(fn) {
  const relay = new WebSocketRelay({ host: "127.0.0.1", port: 0 });
  const { url } = await relay.listen();
  try {
    await fn(url);
  } finally {
    await relay.close();
  }
}

class MemoryOutbox {
  acked = [];
  failed = [];

  async markAcked(msgId) {
    this.acked.push(msgId);
  }

  async markFailed(msgId, error) {
    this.failed.push({ msgId, error });
  }
}

test("wsSubjectMatches supports exact, star, and tail wildcards", () => {
  assert.equal(wsSubjectMatches("msg.bob", "msg.bob"), true);
  assert.equal(wsSubjectMatches("msg.*", "msg.bob"), true);
  assert.equal(wsSubjectMatches("msg.>", "msg.bob.one"), true);
  assert.equal(wsSubjectMatches("msg.*", "msg.bob.one"), false);
  assert.equal(wsSubjectMatches("ack.alice", "ack.bob"), false);
});

test("WebSocketBroker publishes envelopes through a relay and correlates ACKs", async () => {
  await withRelay(async (url) => {
    const alice = new WebSocketBroker({ url });
    const bob = new WebSocketBroker({ url });
    const seen = [];
    const outbox = new MemoryOutbox();

    await alice.startAckCorrelation({ ackSubject: "ack.alice", outbox });
    await bob.subscribeWithAck({
      subject: "msg.bob",
      consumerId: "bob",
      dedupe: new InMemoryDedupeStore(),
      onMessage: async (msg) => {
        seen.push(msg.msgId);
      },
    });

    await alice.publish("msg.bob", envelope());

    await eventually(() => assert.deepEqual(seen, ["msg-1"]));
    await eventually(() => assert.deepEqual(outbox.acked, ["msg-1"]));

    await alice.close();
    await bob.close();
  });
});

test("WebSocketBroker dedupes duplicate envelope delivery and still ACKs the duplicate", async () => {
  await withRelay(async (url) => {
    const alice = new WebSocketBroker({ url });
    const bob = new WebSocketBroker({ url });
    const seen = [];
    const outbox = new MemoryOutbox();

    await alice.startAckCorrelation({ ackSubject: "ack.alice", outbox });
    await bob.subscribeWithAck({
      subject: "msg.bob",
      consumerId: "bob",
      dedupe: new InMemoryDedupeStore(),
      onMessage: async (msg) => {
        seen.push(msg.msgId);
      },
    });

    await alice.publish("msg.bob", envelope());
    await alice.publish("msg.bob", envelope());

    await eventually(() => assert.deepEqual(seen, ["msg-1"]));
    await eventually(() => assert.deepEqual(outbox.acked, ["msg-1", "msg-1"]));

    await alice.close();
    await bob.close();
  });
});

test("WebSocketBroker NACKs invalid envelope frames", async () => {
  await withRelay(async (url) => {
    const alice = new WebSocketBroker({ url });
    const bob = new WebSocketBroker({ url });
    const outbox = new MemoryOutbox();

    await alice.startAckCorrelation({ ackSubject: "ack.alice", outbox });
    await bob.subscribeWithAck({
      subject: "msg.bob",
      consumerId: "bob",
      dedupe: new InMemoryDedupeStore(),
      onMessage: async () => {
        throw new Error("should-not-run");
      },
    });

    const raw = new WebSocket(url);
    await new Promise((resolve) => raw.once("open", resolve));
    raw.send(JSON.stringify({ type: "message", subject: "msg.bob", envelope: { msgId: "bad", senderAgentId: "alice" } }));

    await eventually(() => assert.deepEqual(outbox.failed, [{ msgId: "unknown", error: "invalid-envelope" }]));
    raw.close();
    await alice.close();
    await bob.close();
  });
});
