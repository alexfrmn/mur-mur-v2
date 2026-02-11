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

test("isEnvelopeV1 rejects malformed recipients and timestamps", () => {
  assert.equal(isEnvelopeV1({ ...baseEnvelope, recipients: [] }), false);
  assert.equal(isEnvelopeV1({ ...baseEnvelope, recipients: ["", "agent.b"] }), false);
  assert.equal(isEnvelopeV1({ ...baseEnvelope, createdAt: "not-a-date" }), false);
});

test("isEnvelopeV1 rejects missing required security fields", () => {
  assert.equal(isEnvelopeV1({ ...baseEnvelope, payloadNonce: "" }), false);
  assert.equal(isEnvelopeV1({ ...baseEnvelope, signature: "" }), false);
});
