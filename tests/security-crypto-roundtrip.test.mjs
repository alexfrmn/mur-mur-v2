import test from "node:test";
import assert from "node:assert/strict";
import {
  createKeyPair,
  createSigningKeyPair,
  decryptPayload,
  encryptPayload,
  signEnvelope,
  verifyEnvelopeSignature,
} from "../packages/security/dist/src/index.js";

test("encrypt/decrypt roundtrip", async () => {
  const sender = await createKeyPair();
  const recipient = await createKeyPair();
  const payload = await encryptPayload("hello", recipient.publicKey, sender.privateKey);
  const plaintext = await decryptPayload(payload, recipient.privateKey);
  assert.equal(plaintext, "hello");
});

test("decrypt with wrong key fails", async () => {
  const sender = await createKeyPair();
  const recipient = await createKeyPair();
  const wrongRecipient = await createKeyPair();
  const payload = await encryptPayload("hello", recipient.publicKey, sender.privateKey);
  await assert.rejects(() => decryptPayload(payload, wrongRecipient.privateKey));
});

test("sign/verify roundtrip", async () => {
  const signing = await createSigningKeyPair();
  const body = "payload";
  const signature = await signEnvelope(body, signing.privateKey);
  const valid = await verifyEnvelopeSignature(body, signature, signing.publicKey);
  assert.equal(valid, true);
});

test("tampered payload fails signature verification", async () => {
  const signing = await createSigningKeyPair();
  const signature = await signEnvelope("payload", signing.privateKey);
  const valid = await verifyEnvelopeSignature("payload-modified", signature, signing.publicKey);
  assert.equal(valid, false);
});
