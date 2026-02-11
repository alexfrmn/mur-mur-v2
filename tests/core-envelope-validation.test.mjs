import test from "node:test";
import assert from "node:assert/strict";
import { isEnvelopeV1 } from "../packages/core/dist/src/index.js";

const baseEnvelope = {
  schemaVersion: "1.0",
  msgId: "msg-1",
  conversationId: "conv-1",
  senderAgentId: "agent.a",
  recipients: ["agent.b"],
  createdAt: new Date().toISOString(),
  payloadCiphertext: Buffer.from("hello").toString("base64"),
  payloadNonce: "nonce",
  signature: "sig",
};

test("isEnvelopeV1 accepts valid envelope", () => {
  assert.equal(isEnvelopeV1(baseEnvelope), true);
});

test("isEnvelopeV1 rejects null/undefined/string payloads", () => {
  assert.equal(isEnvelopeV1(null), false);
  assert.equal(isEnvelopeV1(undefined), false);
  assert.equal(isEnvelopeV1("envelope"), false);
});

test("isEnvelopeV1 rejects malformed msgId", () => {
  const { msgId: _drop, ...withoutMsgId } = baseEnvelope;
  assert.equal(isEnvelopeV1(withoutMsgId), false);
  assert.equal(isEnvelopeV1({ ...baseEnvelope, msgId: "" }), false);
});

test("isEnvelopeV1 rejects missing payloadCiphertext", () => {
  const { payloadCiphertext: _drop, ...withoutCiphertext } = baseEnvelope;
  assert.equal(isEnvelopeV1(withoutCiphertext), false);
});

test("isEnvelopeV1 accepts optional ttlSeconds and traceId", () => {
  assert.equal(isEnvelopeV1({ ...baseEnvelope, ttlSeconds: 60, traceId: "trace-1" }), true);
});
