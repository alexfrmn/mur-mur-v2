import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  createKeyPair,
  createSigningKeyPair,
  decryptPayload,
  encryptPayload,
  verifyEnvelopeSignature,
} from "../../security/dist/src/index.js";
import {
  A2AMurmurBridge,
  extractText,
  openReplyEnvelope,
  sealTaskEnvelope,
  stableEnvelopePayload,
} from "../dist/src/index.js";

// Build a complete bridge config with fresh, real keys.
async function fixture() {
  const bridgeEnc = await createKeyPair(); // X25519
  const bridgeSign = await createSigningKeyPair(); // Ed25519
  const agentEnc = await createKeyPair(); // the internal target's X25519 keypair
  const cfg = {
    natsUrl: "nats://127.0.0.1:4222",
    agentId: "a2a-bridge",
    defaultTargetAgentId: "agent-jarvis",
    a2aPort: 0,
    signingPrivateKey: bridgeSign.privateKey,
    encryptionPrivateKey: bridgeEnc.privateKey,
    recipientPublicKeys: { "agent-jarvis": agentEnc.publicKey },
    allowedExternalAgents: ["ext-1"],
  };
  return { cfg, bridgeEnc, bridgeSign, agentEnc };
}

test("sealTaskEnvelope: target decrypts payload + signature verifies", async () => {
  const { cfg, bridgeEnc, bridgeSign, agentEnc } = await fixture();

  const env = await sealTaskEnvelope(
    { externalAgentId: "ext-1", conversationId: "c1", text: "build me a thing" },
    cfg,
  );

  assert.equal(env.senderAgentId, "a2a-bridge");
  assert.deepEqual(env.recipients, ["agent-jarvis"]);
  assert.equal(env.schemaVersion, "1.0");
  assert.ok(env.signature.length > 0, "envelope is signed");

  // The internal target opens the sealed payload (bridge pubkey supplied out-of-band).
  const plain = await decryptPayload(
    {
      ciphertext: env.payloadCiphertext,
      nonce: env.payloadNonce,
      senderPublicKey: bridgeEnc.publicKey,
    },
    agentEnc.privateKey,
  );
  const obj = JSON.parse(plain);
  assert.equal(obj.text, "build me a thing");
  assert.equal(obj.externalAgentId, "ext-1");
  assert.equal(obj.source, "a2a");
  assert.equal(obj.intent, "task");

  // Signature verifies against the mesh-canonical form with the bridge signing key.
  const ok = await verifyEnvelopeSignature(
    stableEnvelopePayload(env),
    env.signature,
    bridgeSign.publicKey,
  );
  assert.equal(ok, true);
});

test("openReplyEnvelope: bridge decrypts an internal agent reply", async () => {
  const { cfg, bridgeEnc, agentEnc } = await fixture();

  // The internal agent seals a reply back to the bridge.
  const enc = await encryptPayload("done: thing built", bridgeEnc.publicKey, agentEnc.privateKey);
  const replyEnv = {
    schemaVersion: "1.0",
    msgId: randomUUID(),
    parentMsgId: randomUUID(),
    conversationId: "c1",
    senderAgentId: "agent-jarvis",
    recipients: ["a2a-bridge"],
    createdAt: new Date().toISOString(),
    payloadCiphertext: enc.ciphertext,
    payloadNonce: enc.nonce,
    signature: "sig",
  };

  const reply = await openReplyEnvelope(replyEnv, cfg);
  assert.equal(reply, "done: thing built");
});

test("openReplyEnvelope: rejects an unknown sender (no key in directory)", async () => {
  const { cfg } = await fixture();
  const bogus = {
    schemaVersion: "1.0",
    msgId: randomUUID(),
    conversationId: "c1",
    senderAgentId: "agent-unknown",
    recipients: ["a2a-bridge"],
    createdAt: new Date().toISOString(),
    payloadCiphertext: "x",
    payloadNonce: "y",
    signature: "z",
  };
  await assert.rejects(() => openReplyEnvelope(bogus, cfg), /no public key to open reply/);
});

test("extractText: flattens text parts", () => {
  const message = {
    messageId: "m",
    contextId: "c",
    taskId: "t",
    role: 2,
    parts: [
      { content: { $case: "text", value: "hello" }, metadata: undefined, filename: "", mediaType: "text/plain" },
      { content: { $case: "text", value: "world" }, metadata: undefined, filename: "", mediaType: "text/plain" },
    ],
    metadata: undefined,
    extensions: [],
    referenceTaskIds: [],
  };
  assert.equal(extractText(message), "hello\nworld");
});

test("dispatchInboundTask: rejects a non-allowlisted external agent (no NATS needed)", async () => {
  const { cfg } = await fixture();
  const bridge = new A2AMurmurBridge(cfg);
  await assert.rejects(
    () => bridge.dispatchInboundTask({ externalAgentId: "evil", conversationId: "c", text: "x" }),
    /not allowlisted/,
  );
});

test("sealTaskEnvelope: throws when target has no recipient key", async () => {
  const { cfg } = await fixture();
  await assert.rejects(
    () =>
      sealTaskEnvelope(
        { externalAgentId: "ext-1", conversationId: "c1", text: "x" },
        { ...cfg, recipientPublicKeys: {} },
      ),
    /no recipient public key/,
  );
});
