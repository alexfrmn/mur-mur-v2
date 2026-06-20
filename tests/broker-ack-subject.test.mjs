import test from "node:test";
import assert from "node:assert/strict";
import { StringCodec } from "nats";
import { NatsBroker } from "../packages/broker-nats/dist/src/index.js";

const sc = StringCodec();

const envelope = {
  schemaVersion: "1.0",
  msgId: "msg-ack-subject",
  conversationId: "conv-ack-subject",
  senderAgentId: "agent-sender",
  recipients: ["agent-receiver"],
  createdAt: new Date().toISOString(),
  payloadCiphertext: Buffer.from("x").toString("base64"),
  payloadNonce: "nonce",
  signature: "sig",
};

test("subscribeWithAck publishes ack to original sender ack subject", async () => {
  const published = [];
  const fakeSub = {
    async *[Symbol.asyncIterator]() {
      yield { data: sc.encode(JSON.stringify(envelope)) };
    },
  };
  const fakeNc = {
    subscribe(subject) {
      assert.equal(subject, "msg.agent-receiver");
      return fakeSub;
    },
    publish(subject, data) {
      published.push({ subject, body: JSON.parse(sc.decode(data)) });
    },
    async drain() {},
  };
  const dedupe = {
    async seen() { return false; },
    async markSeen() {},
  };
  const broker = new NatsBroker({ url: "nats://example.invalid" });
  broker.nc = fakeNc;

  await broker.subscribeWithAck({
    subject: "msg.agent-receiver",
    consumerId: "agent-receiver",
    dedupe,
    onMessage: async () => {},
  });

  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(published.length, 1);
  assert.equal(published[0].subject, "ack.agent-sender");
  assert.equal(published[0].body.msgId, envelope.msgId);
  assert.equal(published[0].body.consumerId, "agent-receiver");
  assert.equal(published[0].body.status, "ack");
});
