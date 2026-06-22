import test from "node:test";
import assert from "node:assert/strict";
import { NatsBroker } from "../packages/broker-nats/dist/src/index.js";

const envelope = {
  schemaVersion: "1.0",
  msgId: "msg-1",
  conversationId: "conv-1",
  senderAgentId: "agent.a",
  recipients: ["agent.b"],
  createdAt: new Date().toISOString(),
  payloadCiphertext: Buffer.from("x").toString("base64"),
  payloadNonce: "nonce",
  signature: "sig",
};

const makeOutbox = (records) => {
  const state = new Map(records.map((r) => [r.msgId, { ...r }]));
  return {
    async claimDue() {
      return [...state.values()].filter((r) => ["pending", "failed"].includes(r.status));
    },
    async markSent(msgId) {
      state.get(msgId).status = "sent";
    },
    async markFailed(msgId, err, nextAttemptAt) {
      const row = state.get(msgId);
      row.status = "failed";
      row.lastError = err;
      row.nextAttemptAt = nextAttemptAt;
    },
    async markDlq(msgId, err) {
      const row = state.get(msgId);
      row.status = "dlq";
      row.lastError = err;
    },
    async markAcked(msgId) {
      state.get(msgId).status = "acked";
    },
    async enqueue() {},
    async listInFlight() {
      return [...state.values()].filter((r) => r.status === "sent");
    },
    __state: state,
  };
};

test("flushOutbox marks policy failures as DLQ", async () => {
  const outbox = makeOutbox([
    { msgId: envelope.msgId, subject: "s", envelope, attempts: 0, status: "pending", nextAttemptAt: new Date().toISOString() },
  ]);
  const broker = new NatsBroker({ url: "nats://invalid:4222" });
  broker.publish = async () => {
    throw new Error("policy-rejected:recipient-not-allowed:agent.b");
  };

  await broker.flushOutbox({ outbox, maxAttempts: 3 });
  assert.equal(outbox.__state.get(envelope.msgId).status, "dlq");
});

test("flushOutbox retries transient failures with failed status", async () => {
  const outbox = makeOutbox([
    { msgId: envelope.msgId, subject: "s", envelope, attempts: 0, status: "pending", nextAttemptAt: new Date().toISOString() },
  ]);
  const broker = new NatsBroker({ url: "nats://invalid:4222" });
  broker.publish = async () => {
    throw new Error("network-timeout");
  };

  await broker.flushOutbox({ outbox, maxAttempts: 3, baseBackoffMs: 10, jitterRatio: 0 });
  const row = outbox.__state.get(envelope.msgId);
  assert.equal(row.status, "failed");
  assert.equal(row.lastError, "network-timeout");
  assert.ok(new Date(row.nextAttemptAt).getTime() >= Date.now());
});

test("flushOutbox respects durable ACK window before publishing more chunks", async () => {
  const sentEnvelope = {
    ...envelope,
    msgId: "msg-sent",
    payloadCiphertext: Buffer.from("already in flight").toString("base64"),
  };
  const pendingEnvelope = {
    ...envelope,
    msgId: "msg-pending",
    payloadCiphertext: Buffer.from("next chunk").toString("base64"),
  };
  const outbox = makeOutbox([
    { msgId: sentEnvelope.msgId, subject: "s", envelope: sentEnvelope, attempts: 1, status: "sent", nextAttemptAt: new Date().toISOString() },
    { msgId: pendingEnvelope.msgId, subject: "s", envelope: pendingEnvelope, attempts: 0, status: "pending", nextAttemptAt: new Date().toISOString() },
  ]);
  const published = [];
  const broker = new NatsBroker({ url: "nats://invalid:4222" });
  broker.publish = async (_subject, env) => {
    published.push(env.msgId);
  };

  await broker.flushOutbox({
    outbox,
    ackWindow: {
      maxInFlightChunks: 1,
      maxInFlightBytes: 1024,
    },
  });

  assert.deepEqual(published, []);
  assert.equal(outbox.__state.get(pendingEnvelope.msgId).status, "pending");

  await outbox.markAcked(sentEnvelope.msgId);
  await broker.flushOutbox({
    outbox,
    ackWindow: {
      maxInFlightChunks: 1,
      maxInFlightBytes: 1024,
    },
  });

  assert.deepEqual(published, [pendingEnvelope.msgId]);
  assert.equal(outbox.__state.get(pendingEnvelope.msgId).status, "sent");
});
