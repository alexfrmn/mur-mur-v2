import test from "node:test";
import assert from "node:assert/strict";
import {
  NaClCryptoProvider,
  createSigningKeyPair,
  decryptPayload,
  encryptPayload,
  setCryptoProvider,
  signEnvelope,
  verifyEnvelopeSignature,
} from "../packages/security/dist/src/index.js";

test("NaCl provider encrypt/decrypt roundtrip", async () => {
  const provider = new NaClCryptoProvider();
  setCryptoProvider(provider);
  const alice = await provider.generateKeyPair();
  const bob = await provider.generateKeyPair();

  const encrypted = await encryptPayload("hello-secure", bob.publicKey, alice.privateKey);
  const decrypted = await decryptPayload(encrypted, bob.privateKey);

  assert.equal(decrypted, "hello-secure");
  assert.ok(encrypted.senderPublicKey);
});

test("signature helpers sign and verify", async () => {
  const signing = await createSigningKeyPair();
  const payload = JSON.stringify({ msg: "signed" });
  const signature = await signEnvelope(payload, signing.privateKey);
  const ok = await verifyEnvelopeSignature(payload, signature, signing.publicKey);
  assert.equal(ok, true);
});
