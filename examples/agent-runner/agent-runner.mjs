#!/usr/bin/env node
/**
 * Minimal Murmur V2 agent runner — over the published @murmurv2/* packages.
 *
 * This is the portable core an external agent needs to join the mesh:
 *   send (encrypt + sign + outbox) and receive (verify + decrypt + store),
 *   backed by the same SQLite store/outbox and NATS broker the reference
 *   daemon uses. There is NO wake/notify/orchestration here — that part is
 *   host-specific. Keep this tiny and explicit; build your automation on top.
 *
 * Usage:
 *   node agent-runner.mjs run                     # start the agent (continuous)
 *   node agent-runner.mjs send <peerId> <text>    # one-shot send, then exit
 *
 * Config: ./agent-config.json (override with AGENT_CONFIG=/path).
 * Store:  ./murmur.db (override with MURMUR_STORE_PATH=/path).
 */
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { NatsBroker } from "@murmurv2/broker-nats";
import { SQLiteDedupeOutboxStore, SQLiteMessageStore } from "@murmurv2/core";
import {
  decryptPayload,
  encryptPayload,
  signEnvelope,
  verifyEnvelopeSignature,
} from "@murmurv2/security";

// --- config ---
const configPath = process.env.AGENT_CONFIG || "./agent-config.json";
const config = JSON.parse(readFileSync(configPath, "utf8"));
const { agentId, natsUrl, natsToken, subject, keys, peers } = config;
const dbPath = process.env.MURMUR_STORE_PATH || "./murmur.db";
const flushIntervalMs = Number(process.env.FLUSH_INTERVAL_MS) || 2000;

const log = (level, msg, data = {}) =>
  console.log(JSON.stringify({ ts: new Date().toISOString(), level, msg, ...data }));

const store = new SQLiteMessageStore(dbPath);
const outbox = new SQLiteDedupeOutboxStore(dbPath);
const broker = new NatsBroker({ url: natsUrl, token: natsToken });

// Stable payload that gets signed. The canonical source of truth is
// `stableEnvelopePayload` in @murmurv2/core, golden-locked in
// packages/core/test/stable-envelope-payload.test.mjs — this MUST stay byte-identical
// (field set + order) or signatures will not verify against the mesh.
//
// It is intentionally INLINED here (not imported from core) because this example pins
// the PUBLISHED `@murmurv2/core@^0.1.0`, which predates the exported helper; importing
// it would break a standalone `npm install` of this template. Switch to
// `import { stableEnvelopePayload } from "@murmurv2/core"` when bumping to the core
// version that exports it — tracked for auth/authz #47 PR-C (when authToken enters the
// signed payload and core is re-published).
const stableEnvelopePayload = (e) =>
  JSON.stringify({
    schemaVersion: e.schemaVersion,
    msgId: e.msgId,
    conversationId: e.conversationId,
    senderAgentId: e.senderAgentId,
    recipients: [...e.recipients],
    createdAt: e.createdAt,
    payloadCiphertext: e.payloadCiphertext,
    payloadNonce: e.payloadNonce,
  });

// --- send: encrypt -> sign -> enqueue to outbox (daemon flushes to NATS) ---
async function sendMessage(to, text, conversationId) {
  const peer = peers[to];
  if (!peer) throw new Error(`unknown peer: ${to} — add it to peers in agent-config.json`);

  const convId = conversationId || `dm:${agentId}:${to}`;
  const msgId = randomUUID();

  const encrypted = await encryptPayload(text, peer.encryption.publicKey, keys.encryption.privateKey);

  const envelope = {
    schemaVersion: "1.0",
    msgId,
    conversationId: convId,
    senderAgentId: agentId,
    recipients: [to],
    createdAt: new Date().toISOString(),
    payloadCiphertext: encrypted.ciphertext,
    payloadNonce: encrypted.nonce,
    signature: "",
  };
  envelope.signature = await signEnvelope(stableEnvelopePayload(envelope), keys.signing.privateKey);

  await outbox.enqueue(peer.subject, envelope);
  await store.append({
    conversationId: convId,
    msgId,
    direction: "outbound",
    sender: agentId,
    text,
    createdAt: envelope.createdAt,
    transport: "nats",
  });

  log("info", "queued", { msgId, to, conversationId: convId });
  return { msgId, conversationId: convId };
}

// --- receive: verify -> decrypt -> store as inbound ---
async function onMessage(envelope) {
  const senderId = envelope.senderAgentId;
  const peer = peers[senderId];
  if (!peer) throw new Error(`unknown-sender:${senderId}`);

  const valid = await verifyEnvelopeSignature(
    stableEnvelopePayload(envelope),
    envelope.signature,
    peer.signing.publicKey,
  );
  if (!valid) throw new Error(`signature-invalid:${senderId}`);

  const plaintext = await decryptPayload(
    {
      ciphertext: envelope.payloadCiphertext,
      nonce: envelope.payloadNonce,
      senderPublicKey: peer.encryption.publicKey,
    },
    keys.encryption.privateKey,
  );

  await store.append({
    conversationId: envelope.conversationId,
    msgId: envelope.msgId,
    direction: "inbound",
    sender: senderId,
    text: plaintext,
    createdAt: envelope.createdAt,
    transport: "nats",
  });

  log("info", "received", {
    msgId: envelope.msgId,
    from: senderId,
    conversationId: envelope.conversationId,
    text: plaintext,
  });
}

// --- continuous run: subscribe + ack-correlation + outbox flush loop ---
let running = true;

async function flushLoop() {
  while (running) {
    try {
      await broker.flushOutbox({ outbox, maxAttempts: 5, ackTimeoutMs: 15_000 });
    } catch (err) {
      log("error", "flush-error", { error: err.message });
    }
    await new Promise((r) => setTimeout(r, flushIntervalMs));
  }
}

async function run() {
  await broker.connect();
  log("info", "connected", { natsUrl });

  // outbox doubles as the dedupe store (it implements DedupeStore)
  await broker.subscribeWithAck({ subject, consumerId: agentId, dedupe: outbox, onMessage });
  log("info", "subscribed", { subject });

  await broker.startAckCorrelation({ outbox, ackSubject: `ack.${agentId}`, consumerId: `${agentId}-ack` });
  log("info", "ack-correlation-started", { ackSubject: `ack.${agentId}` });

  flushLoop();
  log("info", "ready", { agentId, peers: Object.keys(peers) });
}

const shutdown = async (sig) => {
  log("info", "shutdown", { sig });
  running = false;
  try {
    await broker.close();
  } catch {
    /* ignore */
  }
  process.exit(0);
};
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// --- CLI ---
const cmd = process.argv[2];

if (cmd === "send") {
  const to = process.argv[3];
  const text = process.argv.slice(4).join(" ");
  if (!to || !text) {
    console.error("usage: node agent-runner.mjs send <peerId> <text>");
    process.exit(1);
  }
  (async () => {
    await broker.connect();
    await sendMessage(to, text);
    // flush once so the one-shot send actually leaves the outbox before we exit
    await broker.flushOutbox({ outbox, maxAttempts: 5, ackTimeoutMs: 15_000 });
    await broker.close();
    process.exit(0);
  })().catch((err) => {
    log("fatal", err.message);
    process.exit(1);
  });
} else if (cmd === "run" || cmd === undefined) {
  run().catch((err) => {
    log("fatal", err.message);
    process.exit(1);
  });
} else {
  console.error(`unknown command: ${cmd} (use 'run' or 'send <peerId> <text>')`);
  process.exit(1);
}
