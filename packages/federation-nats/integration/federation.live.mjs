// LIVE federation interop test (JARVIS half of #14, isolated two-org).
// Proves the full cross-org path over REAL NATS account isolation:
//   1. Each org publishes an Ed25519-signed roster of its agents.
//   2. The partner verifies that roster against a PINNED org key (and rejects a
//      wrong pinned key + a tampered roster) — the only net-new crypto surface.
//   3. org A seals+signs an EnvelopeV1 to org B's agent (keys taken from B's
//      verified roster) and publishes it on the fed.* subject; NATS routes it
//      across accounts (A imports B's `fed.partner.>` service); B verifies A's
//      agent signature (key from A's verified roster) and decrypts.
// Account config is generated from buildFederationAccountContract (gen-accounts-conf.mjs)
// so the server config can't drift from the contract. Run via run-fed-live.sh
// (boots an isolated local nats-server with the accounts config — NOT the prod broker).
import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { randomUUID } from "node:crypto";
import { connect, StringCodec } from "nats";
import {
  createKeyPair,
  createSigningKeyPair,
  encryptPayload,
  decryptPayload,
  signEnvelope,
  verifyEnvelopeSignature,
} from "../../security/dist/src/index.js";
import { signRoster, verifyRoster, lookupAgentKeys } from "../../federation/dist/src/index.js";
import { stableEnvelopePayload } from "../../core/dist/src/index.js";
import { federationMessageSubject } from "../dist/src/index.js";

const PORT = process.env.FED_NATS_PORT || "14333";
const NATS_URL = `nats://127.0.0.1:${PORT}`;
const sc = StringCodec();

// Mesh-canonical signing input imported from @murmurv2/core (single source of truth)
// so this cross-org live test can't drift from the helper every other signer uses.

function natsReachable(url) {
  const { hostname, port } = new URL(url);
  return new Promise((resolve) => {
    const s = net.connect({ host: hostname, port: Number(port) }, () => { s.destroy(); resolve(true); });
    s.on("error", () => resolve(false));
    s.setTimeout(1500, () => { s.destroy(); resolve(false); });
  });
}

test("LIVE federation: roster sign/verify + cross-org sealed+signed envelope over isolated NATS accounts", async (t) => {
  if (!(await natsReachable(NATS_URL))) {
    t.skip(`no accounts nats-server at ${NATS_URL}; run via integration/run-fed-live.sh`);
    return;
  }

  // org-level directory signing keypairs + per-agent keypairs
  const aOrgSign = await createSigningKeyPair();
  const bOrgSign = await createSigningKeyPair();
  const aAgentEnc = await createKeyPair();
  const aAgentSign = await createSigningKeyPair();
  const bAgentEnc = await createKeyPair();
  const bAgentSign = await createSigningKeyPair();

  const rosterA = await signRoster(
    { org: "aimindset", version: 1, issuedAt: "2026-06-21T00:00:00Z",
      agents: { "agent-jarvis": { encryptPublicKey: aAgentEnc.publicKey, verifyPublicKey: aAgentSign.publicKey } } },
    aOrgSign.privateKey, aOrgSign.publicKey,
  );
  const rosterB = await signRoster(
    { org: "partner", version: 1, issuedAt: "2026-06-21T00:00:00Z",
      agents: { "agent-codex": { encryptPublicKey: bAgentEnc.publicKey, verifyPublicKey: bAgentSign.publicKey } } },
    bOrgSign.privateKey, bOrgSign.publicKey,
  );

  // roster trust: pinned key verifies; wrong pinned key + tampered roster are rejected
  assert.equal(await verifyRoster(rosterA, aOrgSign.publicKey), true, "valid roster verifies with pinned org key");
  assert.equal(await verifyRoster(rosterA, bOrgSign.publicKey), false, "wrong pinned key rejected");
  const tampered = {
    ...rosterA,
    agents: { "agent-jarvis": { encryptPublicKey: bAgentEnc.publicKey, verifyPublicKey: aAgentSign.publicKey } },
  };
  assert.equal(await verifyRoster(tampered, aOrgSign.publicKey), false, "tampered roster rejected");

  const aKeys = lookupAgentKeys(rosterA, "agent-jarvis"); // B's view of A's agent
  const bKeys = lookupAgentKeys(rosterB, "agent-codex");  // A's view of B's agent

  // live cross-org over isolated accounts
  const a = await connect({ servers: NATS_URL, user: "aimindset", pass: "pw_aimindset", name: "orgA" });
  const b = await connect({ servers: NATS_URL, user: "partner", pass: "pw_partner", name: "orgB" });
  t.after(async () => { await a.drain(); await b.drain(); });

  const subject = federationMessageSubject("partner/agent-codex", "aimindset"); // fed.partner.msg.agent-codex
  const sub = b.subscribe(subject);
  let received = null;
  const waiter = (async () => { for await (const m of sub) { received = JSON.parse(sc.decode(m.data)); break; } })();

  // A seals to B's agent (B's enc key from verified roster) + signs with A's agent key
  const plaintext = JSON.stringify({ intent: "task", text: "cross-org hello from aimindset/agent-jarvis" });
  const enc = await encryptPayload(plaintext, bKeys.encryptPublicKey, aAgentEnc.privateKey);
  const unsigned = {
    schemaVersion: "1.0",
    msgId: randomUUID(),
    conversationId: "fed-conv-1",
    senderAgentId: "aimindset/agent-jarvis",
    recipients: ["partner/agent-codex"],
    createdAt: new Date().toISOString(),
    payloadCiphertext: enc.ciphertext,
    payloadNonce: enc.nonce,
  };
  const envelope = { ...unsigned, signature: await signEnvelope(stableEnvelopePayload(unsigned), aAgentSign.privateKey) };

  await new Promise((r) => setTimeout(r, 250)); // sub propagation across accounts
  a.publish(subject, sc.encode(JSON.stringify(envelope)));
  await Promise.race([waiter, new Promise((r) => setTimeout(r, 1500))]);

  assert.ok(received, "B received the cross-org envelope");
  assert.equal(received.senderAgentId, "aimindset/agent-jarvis", "sender preserved");
  // B verifies A's agent signature via the key from A's verified roster
  assert.equal(
    await verifyEnvelopeSignature(stableEnvelopePayload(received), received.signature, aKeys.verifyPublicKey),
    true, "A's agent signature verifies via roster key",
  );
  // B decrypts with its agent key + A's agent enc key (from verified roster)
  const opened = await decryptPayload(
    { ciphertext: received.payloadCiphertext, nonce: received.payloadNonce, senderPublicKey: aKeys.encryptPublicKey },
    bAgentEnc.privateKey,
  );
  assert.equal(JSON.parse(opened).text, "cross-org hello from aimindset/agent-jarvis", "payload decrypts intact");
});
