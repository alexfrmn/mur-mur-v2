import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { SQLiteDedupeOutboxStore, SQLiteMessageStore } from "@murmurv2/core";
import { encryptPayload, signEnvelope } from "@murmurv2/security";

const dataDir = process.env.DATA_DIR || ".data";
const config = JSON.parse(readFileSync(`${dataDir}/agent-config.json`, "utf8"));
const store = new SQLiteMessageStore(`${dataDir}/murmur.db`);
const outbox = new SQLiteDedupeOutboxStore(`${dataDir}/murmur.db`);

const to = process.argv[2] || "agent-codex";
const text = process.argv[3] || "PING from JARVIS (Claude Code). Respond with PONG and your current model/status.";

const peer = config.peers[to];
if (!peer) {
  console.error("Unknown peer:", to, "| Available:", Object.keys(config.peers).join(", "));
  process.exit(1);
}

const conversationId = `dm:${config.agentId}:${to}`;
const msgId = randomUUID();

const encrypted = await encryptPayload(text, peer.encryption.publicKey, config.keys.encryption.privateKey);

const envelope = {
  schemaVersion: "1.0",
  msgId,
  conversationId,
  senderAgentId: config.agentId,
  recipients: [to],
  createdAt: new Date().toISOString(),
  payloadCiphertext: encrypted.ciphertext,
  payloadNonce: encrypted.nonce,
  signature: "",
};

const stablePayload = JSON.stringify({
  schemaVersion: envelope.schemaVersion,
  msgId: envelope.msgId,
  conversationId: envelope.conversationId,
  senderAgentId: envelope.senderAgentId,
  recipients: [...envelope.recipients],
  createdAt: envelope.createdAt,
  payloadCiphertext: envelope.payloadCiphertext,
  payloadNonce: envelope.payloadNonce,
});

envelope.signature = await signEnvelope(stablePayload, config.keys.signing.privateKey);

await outbox.enqueue(peer.subject, envelope);
await store.append({
  conversationId,
  msgId,
  direction: "outbound",
  sender: config.agentId,
  text,
  createdAt: envelope.createdAt,
  transport: "nats",
});

console.log(`SENT to ${to} | msgId: ${msgId}`);
console.log(`Text: ${text.substring(0, 100)}...`);
console.log("Queued in outbox → daemon delivers via NATS");
