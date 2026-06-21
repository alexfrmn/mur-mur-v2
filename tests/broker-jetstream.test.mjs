import test from "node:test";
import assert from "node:assert/strict";
import { StringCodec } from "nats";
import { NatsBroker } from "../packages/broker-nats/dist/src/index.js";
import { createAck } from "../packages/core/dist/src/index.js";

const sc = StringCodec();

const envelope = {
  schemaVersion: "1.0",
  msgId: "msg-js-1",
  conversationId: "conv-js-1",
  senderAgentId: "agent-sender",
  recipients: ["agent-receiver"],
  createdAt: new Date().toISOString(),
  payloadCiphertext: Buffer.from("x").toString("base64"),
  payloadNonce: "nonce",
  signature: "sig",
};

const makeJetStreamBroker = ({ streamInfoThrows = true, messages = [] } = {}) => {
  const streamsAdded = [];
  const consumersAdded = [];
  const published = [];
  const acked = [];
  const nacked = [];

  const fakeMessages = {
    async *[Symbol.asyncIterator]() {
      for (const data of messages) {
        yield {
          data,
          ack() {
            acked.push(data);
          },
          nak() {
            nacked.push(data);
          },
        };
      }
    },
    async close() {},
  };

  const fakeJsm = {
    streams: {
      async info() {
        if (streamInfoThrows) throw new Error("stream-missing");
        return { config: { name: "MURMUR", subjects: ["msg.>"] } };
      },
      async add(config) {
        streamsAdded.push(config);
      },
      async update(_stream, config) {
        streamsAdded.push(config);
      },
    },
    consumers: {
      async info() {
        throw new Error("consumer-missing");
      },
      async add(stream, config) {
        consumersAdded.push({ stream, config });
      },
    },
  };

  const fakeJs = {
    async publish(subject, data, opts) {
      published.push({ subject, body: JSON.parse(sc.decode(data)), opts });
    },
    consumers: {
      async get(stream, durable) {
        return {
          stream,
          durable,
          async consume() {
            return fakeMessages;
          },
        };
      },
    },
  };

  const fakeNc = {
    async jetstreamManager() {
      return fakeJsm;
    },
    jetstream() {
      return fakeJs;
    },
    async drain() {},
  };

  const broker = new NatsBroker({
    url: "nats://example.invalid",
    jetstream: true,
    stream: "MURMUR",
    streamSubjects: ["msg.>", "ack.>"],
  });
  broker.nc = fakeNc;

  return { broker, streamsAdded, consumersAdded, published, acked, nacked };
};

test("JetStream publish ensures stream and uses envelope msgId as dedupe id", async () => {
  const { broker, streamsAdded, published } = makeJetStreamBroker();

  await broker.publish("msg.agent-receiver", envelope);

  assert.deepEqual(streamsAdded[0], { name: "MURMUR", subjects: ["msg.>", "ack.>"] });
  assert.equal(published.length, 1);
  assert.equal(published[0].subject, "msg.agent-receiver");
  assert.equal(published[0].body.msgId, envelope.msgId);
  assert.equal(published[0].opts.msgID, envelope.msgId);
});

test("JetStream disabled keeps core NATS publish path", async () => {
  const published = [];
  const broker = new NatsBroker({ url: "nats://example.invalid" });
  broker.nc = {
    publish(subject, data) {
      published.push({ subject, body: JSON.parse(sc.decode(data)) });
    },
    async jetstreamManager() {
      throw new Error("jetstream-manager-should-not-be-called");
    },
    async drain() {},
  };

  await broker.publish("msg.agent-receiver", envelope);

  assert.deepEqual(published, [{ subject: "msg.agent-receiver", body: envelope }]);
});

test("JetStream subscribeWithAck creates durable explicit-ack consumer", async () => {
  const { broker, consumersAdded, published, acked } = makeJetStreamBroker({
    messages: [sc.encode(JSON.stringify(envelope))],
  });
  const dedupe = {
    async seen() { return false; },
    async markSeen() {},
  };

  await broker.subscribeWithAck({
    subject: "msg.agent-receiver",
    consumerId: "agent-receiver",
    dedupe,
    onMessage: async () => {},
  });

  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(consumersAdded.length, 1);
  assert.equal(consumersAdded[0].stream, "MURMUR");
  assert.equal(consumersAdded[0].config.durable_name, "agent-receiver");
  assert.equal(consumersAdded[0].config.filter_subject, "msg.agent-receiver");
  assert.equal(consumersAdded[0].config.ack_policy, "explicit");
  assert.equal(published[0].subject, "ack.agent-sender");
  assert.equal(published[0].body.status, "ack");
  assert.equal(published[0].opts.msgID, `ack:${envelope.msgId}:agent-receiver:ack`);
  assert.equal(acked.length, 1);
});

test("JetStream ACK correlation consumes durable ack subject and updates outbox", async () => {
  const ack = createAck(envelope.msgId, "agent-receiver", "ack");
  const { broker, consumersAdded, acked } = makeJetStreamBroker({
    messages: [sc.encode(JSON.stringify(ack))],
  });
  const marked = [];
  const outbox = {
    async markAcked(msgId) {
      marked.push(["acked", msgId]);
    },
    async markFailed(msgId, reason) {
      marked.push(["failed", msgId, reason]);
    },
  };

  await broker.startAckCorrelation({
    outbox,
    ackSubject: "ack.agent-sender",
    consumerId: "agent-sender-ack",
  });

  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(consumersAdded[0].config.durable_name, "agent-sender-ack");
  assert.equal(consumersAdded[0].config.filter_subject, "ack.agent-sender");
  assert.deepEqual(marked, [["acked", envelope.msgId]]);
  assert.equal(acked.length, 1);
});
