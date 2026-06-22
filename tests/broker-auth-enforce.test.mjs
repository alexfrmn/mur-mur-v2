// PR-D2: broker-nats ingress auth enforcement. Verifies the injected `authorize`
// hook gates delivery — without touching a live NATS server (fake nc, same pattern as
// broker-ack-subject.test.mjs). The authorizer itself (authorizeInbound) is unit-tested
// in @murmurv2/federation; this proves the WIRING: reject -> NACK auth-rejected, never
// delivered; accept -> delivered + ack; no authorizer -> pass-through (default OFF).
import test from "node:test";
import assert from "node:assert/strict";
import { StringCodec } from "nats";
import { NatsBroker } from "../packages/broker-nats/dist/src/index.js";

const sc = StringCodec();

const envelope = {
  schemaVersion: "1.0",
  msgId: "msg-auth",
  conversationId: "conv-auth",
  senderAgentId: "agent-sender",
  recipients: ["agent-receiver"],
  createdAt: new Date().toISOString(),
  payloadCiphertext: Buffer.from("x").toString("base64"),
  payloadNonce: "nonce",
  signature: "sig",
  authToken: "MURMUR-AUTH:tok",
};

function harness() {
  const published = [];
  const fakeSub = {
    async *[Symbol.asyncIterator]() {
      yield { data: sc.encode(JSON.stringify(envelope)) };
    },
  };
  const fakeNc = {
    subscribe() {
      return fakeSub;
    },
    publish(subject, data) {
      published.push({ subject, body: JSON.parse(sc.decode(data)) });
    },
    async drain() {},
  };
  const dedupe = { async seen() { return false; }, async markSeen() {} };
  const broker = new NatsBroker({ url: "nats://example.invalid" });
  broker.nc = fakeNc;
  return { broker, dedupe, published };
}

const settle = () => new Promise((r) => setTimeout(r, 20));

test("authorize reject -> NACK auth-rejected:<reason>, onMessage NOT called", async () => {
  const { broker, dedupe, published } = harness();
  let delivered = false;
  await broker.subscribeWithAck({
    subject: "msg.agent-receiver",
    consumerId: "agent-receiver",
    dedupe,
    onMessage: async () => { delivered = true; },
    authorize: async () => ({ accepted: false, reason: "subject-mismatch" }),
  });
  await settle();
  assert.equal(delivered, false, "a rejected envelope must never reach onMessage");
  assert.equal(published.length, 1);
  assert.equal(published[0].subject, "ack.agent-sender");
  assert.equal(published[0].body.status, "nack");
  assert.equal(published[0].body.reason, "auth-rejected:subject-mismatch");
});

test("authorize accept -> delivered + ack", async () => {
  const { broker, dedupe, published } = harness();
  let delivered = false;
  await broker.subscribeWithAck({
    subject: "msg.agent-receiver",
    consumerId: "agent-receiver",
    dedupe,
    onMessage: async () => { delivered = true; },
    authorize: async () => ({ accepted: true }),
  });
  await settle();
  assert.equal(delivered, true);
  assert.equal(published[0].body.status, "ack");
});

test("no authorizer -> pass-through (enforcement OFF by default)", async () => {
  const { broker, dedupe, published } = harness();
  let delivered = false;
  await broker.subscribeWithAck({
    subject: "msg.agent-receiver",
    consumerId: "agent-receiver",
    dedupe,
    onMessage: async () => { delivered = true; },
  });
  await settle();
  assert.equal(delivered, true);
  assert.equal(published[0].body.status, "ack");
});

test("authorize reject reason defaults to 'denied' when omitted", async () => {
  const { broker, dedupe, published } = harness();
  await broker.subscribeWithAck({
    subject: "msg.agent-receiver",
    consumerId: "agent-receiver",
    dedupe,
    onMessage: async () => {},
    authorize: async () => ({ accepted: false }),
  });
  await settle();
  assert.equal(published[0].body.status, "nack");
  assert.equal(published[0].body.reason, "auth-rejected:denied");
});
