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

const makeJetStreamBroker = ({
  streamInfoThrows = true,
  consumerInfo,
  messages = [],
  advisoryMessages = {},
  storedMessages = new Map(),
  brokerConfig = {},
} = {}) => {
  const streamsAdded = [];
  const consumersAdded = [];
  const consumersUpdated = [];
  const published = [];
  const acked = [];
  const nacked = [];
  const subscribed = [];
  const getMessageQueries = [];

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
      async getMessage(stream, query) {
        getMessageQueries.push({ stream, query });
        const msg = storedMessages.get(`${stream}:${query.seq}`);
        if (!msg) throw new Error("message-missing");
        return msg;
      },
    },
    consumers: {
      async info() {
        if (consumerInfo) return consumerInfo;
        throw new Error("consumer-missing");
      },
      async add(stream, config) {
        consumersAdded.push({ stream, config });
      },
      async update(stream, durable, config) {
        consumersUpdated.push({ stream, durable, config });
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
    subscribe(subject) {
      subscribed.push(subject);
      const frames = advisoryMessages[subject] ?? [];
      return {
        async *[Symbol.asyncIterator]() {
          for (const data of frames) {
            yield { data };
          }
        },
        unsubscribe() {},
      };
    },
    async drain() {},
  };

  const broker = new NatsBroker({
    url: "nats://example.invalid",
    jetstream: true,
    stream: "MURMUR",
    streamSubjects: ["msg.>", "ack.>"],
    ...brokerConfig,
  });
  broker.nc = fakeNc;

  return { broker, streamsAdded, consumersAdded, consumersUpdated, published, acked, nacked, subscribed, getMessageQueries };
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
  assert.equal(consumersAdded[0].config.max_deliver, 5);
  assert.equal(consumersAdded[0].config.ack_wait, 30_000_000_000);
  assert.equal(published[0].subject, "ack.agent-sender");
  assert.equal(published[0].body.status, "ack");
  assert.equal(published[0].opts.msgID, `ack:${envelope.msgId}:agent-receiver:ack`);
  assert.equal(acked.length, 1);
});

test("JetStream consumer delivery limits are configurable", async () => {
  const { broker, consumersAdded } = makeJetStreamBroker({
    brokerConfig: {
      jetstreamMaxDeliver: 2,
      jetstreamAckWaitMs: 750,
    },
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

  assert.equal(consumersAdded[0].config.max_deliver, 2);
  assert.equal(consumersAdded[0].config.ack_wait, 750_000_000);
});

test("JetStream existing durable consumer is repaired when delivery limits drift", async () => {
  const { broker, consumersAdded, consumersUpdated } = makeJetStreamBroker({
    consumerInfo: {
      config: {
        durable_name: "agent-receiver",
        filter_subject: "msg.agent-receiver",
        max_deliver: -1,
        ack_wait: 30_000_000_000,
      },
    },
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

  assert.equal(consumersAdded.length, 0);
  assert.deepEqual(consumersUpdated, [{
    stream: "MURMUR",
    durable: "agent-receiver",
    config: {
      max_deliver: 5,
      ack_wait: 30_000_000_000,
    },
  }]);
});

test("JetStream retryable handler failures are nacked for redelivery", async () => {
  const { broker, published, acked, nacked } = makeJetStreamBroker({
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
    maxPoisonAttempts: 99,
    onMessage: async () => {
      throw new Error("handler-down");
    },
  });

  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(acked.length, 0);
  assert.equal(nacked.length, 1);
  assert.equal(published[0].subject, "ack.agent-sender");
  assert.equal(published[0].body.status, "nack");
  assert.equal(published[0].body.reason, "handler-down");
});

test("JetStream poison-message terminal failure is acked", async () => {
  const { broker, published, acked, nacked } = makeJetStreamBroker({
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
    maxPoisonAttempts: 1,
    onMessage: async () => {
      throw new Error("bad-payload");
    },
  });

  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.equal(acked.length, 1);
  assert.equal(nacked.length, 0);
  assert.equal(published[0].subject, "ack.agent-sender");
  assert.equal(published[0].body.status, "nack");
  assert.equal(published[0].body.reason, "poison-message:bad-payload");
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

test("JetStream DLQ advisory resolves stream sequence and marks original outbox row DLQ", async () => {
  const advisorySubject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.MURMUR.*";
  const advisory = {
    type: "io.nats.jetstream.advisory.v1.max_deliver",
    id: "advisory-1",
    timestamp: "2026-06-21T18:00:00.000Z",
    stream: "MURMUR",
    consumer: "agent-receiver",
    stream_seq: 42,
    deliveries: 5,
  };
  const { broker, subscribed, getMessageQueries } = makeJetStreamBroker({
    streamInfoThrows: false,
    advisoryMessages: {
      [advisorySubject]: [sc.encode(JSON.stringify(advisory))],
    },
    storedMessages: new Map([
      ["MURMUR:42", { data: sc.encode(JSON.stringify(envelope)) }],
    ]),
  });
  const marked = [];
  const outbox = {
    async markDlq(msgId, reason) {
      marked.push([msgId, reason]);
    },
  };

  await broker.startJetStreamAdvisoryDlq({ outbox });
  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.deepEqual(subscribed, [
    "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.MURMUR.*",
    "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.MURMUR.*",
  ]);
  assert.deepEqual(getMessageQueries, [{ stream: "MURMUR", query: { seq: 42 } }]);
  assert.deepEqual(marked, [[envelope.msgId, "jetstream-advisory:max_deliver:agent-receiver:deliveries=5:stream_seq=42"]]);
});
