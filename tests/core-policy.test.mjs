import test from "node:test";
import assert from "node:assert/strict";
import {
  applyJitter,
  computeBackoffMs,
  validateEnvelopePolicy,
} from "../packages/core/dist/src/index.js";

test("computeBackoffMs grows exponentially and caps", () => {
  assert.equal(computeBackoffMs(1, 100, 10_000), 100);
  assert.equal(computeBackoffMs(2, 100, 10_000), 200);
  assert.equal(computeBackoffMs(10, 100, 1_000), 1_000);
});

test("applyJitter keeps value in range", () => {
  const value = applyJitter(1000, 0.2);
  assert.ok(value >= 800);
  assert.ok(value <= 1200);
});

test("validateEnvelopePolicy enforces route + payload size", () => {
  const envelope = {
    schemaVersion: "1.0",
    msgId: "m1",
    conversationId: "c1",
    senderAgentId: "agent.a",
    recipients: ["agent.b", "agent.c"],
    createdAt: new Date().toISOString(),
    payloadCiphertext: Buffer.from("hello world", "utf8").toString("base64"),
    payloadNonce: "n",
    signature: "s",
  };

  const violations = validateEnvelopePolicy(envelope, {
    maxPayloadBytes: 4,
    allowedRoutes: { "agent.a": ["agent.b"] },
  });

  assert.equal(violations.length, 2);
  assert.ok(violations.some((v) => v.startsWith("payload-too-large:")));
  assert.ok(violations.some((v) => v.startsWith("recipient-not-allowed:")));
});
