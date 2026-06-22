// Golden test for the canonical envelope signing payload (single source of truth).
// stableEnvelopePayload was previously copy-pasted across mcp-server / daemon / bridges
// / demos; this locks its exact byte output so a change can't silently break cross-agent
// signature interop. If this test fails, EVERY signer must change together (wire-breaking).
import test from "node:test";
import assert from "node:assert/strict";
import { stableEnvelopePayload } from "../dist/src/index.js";

const ENV = Object.freeze({
  schemaVersion: "1.0",
  msgId: "m1",
  conversationId: "c1",
  senderAgentId: "agent-a",
  recipients: ["agent-b", "agent-c"],
  createdAt: "2026-06-22T00:00:00.000Z",
  payloadCiphertext: "ct",
  payloadNonce: "no",
  signature: "SIG-SHOULD-BE-EXCLUDED",
});

test("stableEnvelopePayload emits the exact canonical string (golden)", () => {
  assert.equal(
    stableEnvelopePayload(ENV),
    '{"schemaVersion":"1.0","msgId":"m1","conversationId":"c1","senderAgentId":"agent-a","recipients":["agent-b","agent-c"],"createdAt":"2026-06-22T00:00:00.000Z","payloadCiphertext":"ct","payloadNonce":"no"}',
  );
});

test("stableEnvelopePayload excludes the signature field (it is what gets signed)", () => {
  const signed = stableEnvelopePayload(ENV);
  const unsigned = stableEnvelopePayload({ ...ENV, signature: "" });
  assert.equal(signed, unsigned);
  assert.ok(!signed.includes("signature"));
});

test("stableEnvelopePayload field order is fixed regardless of input key order", () => {
  const reordered = {
    payloadNonce: "no",
    signature: "x",
    recipients: ["agent-b", "agent-c"],
    msgId: "m1",
    schemaVersion: "1.0",
    payloadCiphertext: "ct",
    createdAt: "2026-06-22T00:00:00.000Z",
    senderAgentId: "agent-a",
    conversationId: "c1",
  };
  assert.equal(stableEnvelopePayload(reordered), stableEnvelopePayload(ENV));
});

test("stableEnvelopePayload copies recipients (no shared mutable reference)", () => {
  const recipients = ["agent-b", "agent-c"];
  const out = stableEnvelopePayload({ ...ENV, recipients });
  recipients.push("agent-d");
  // the serialized string already captured the 2-recipient state
  assert.ok(out.includes('"recipients":["agent-b","agent-c"]'));
  assert.ok(!out.includes("agent-d"));
});
