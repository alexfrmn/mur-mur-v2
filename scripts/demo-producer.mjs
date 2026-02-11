import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { DatabaseSync } from "node:sqlite";
import { NatsBroker } from "@murmurv2/broker-nats";
import { SQLiteDedupeOutboxStore } from "@murmurv2/core";
import { encryptPayload, getCryptoProvider, signEnvelope } from "@murmurv2/security";
import { ensureDemoKeys, loadDemoConfig, policyFromConfig, stableEnvelopePayload } from "./demo-secure-common.mjs";

const cfg = loadDemoConfig();
const keys = await ensureDemoKeys(cfg.keysPath);

const broker = new NatsBroker({ url: cfg.natsUrl, token: cfg.natsToken });
const outbox = new SQLiteDedupeOutboxStore(cfg.outboxDbPath);

const waitForAck = async (dbPath, msgId, timeoutMs) => {
  const db = new DatabaseSync(dbPath);
  try {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const row = db
        .prepare("SELECT status, last_error as lastError FROM outbox WHERE msg_id = ?")
        .get(msgId);
      if (row?.status === "acked") return row;
      if (row?.status === "dlq") {
        throw new Error(`dlq:${row.lastError ?? "unknown"}`);
      }
      await sleep(150);
    }
    throw new Error("ack-timeout");
  } finally {
    db.close();
  }
};

const run = async () => {
  const encrypted = await encryptPayload(
    cfg.message,
    keys.recipient.encryption.publicKey,
    keys.sender.encryption.privateKey,
  );

  const envelope = {
    schemaVersion: "1.0",
    msgId: randomUUID(),
    conversationId: cfg.conversationId,
    senderAgentId: cfg.senderAgentId,
    recipients: [cfg.recipientAgentId],
    createdAt: new Date().toISOString(),
    payloadCiphertext: encrypted.ciphertext,
    payloadNonce: encrypted.nonce,
    signature: "",
  };

  envelope.signature = await signEnvelope(stableEnvelopePayload(envelope), keys.sender.signing.privateKey);

  await broker.startAckCorrelation({
    outbox,
    ackSubject: `ack.${cfg.consumerId}`,
  });

  await outbox.enqueue(cfg.subject, envelope);
  await broker.flushOutbox({
    outbox,
    maxAttempts: cfg.flushMaxAttempts,
    ackTimeoutMs: cfg.ackTimeoutMs,
    policy: policyFromConfig(cfg),
  });

  const acked = await waitForAck(cfg.outboxDbPath, envelope.msgId, cfg.waitForAckMs);
  console.log("[producer] secure envelope acked", {
    msgId: envelope.msgId,
    conversationId: envelope.conversationId,
    cryptoProvider: getCryptoProvider().name,
    outboxStatus: acked.status,
  });
};

try {
  await run();
  await broker.close();
} catch (err) {
  console.error("[producer] secure demo failed", {
    error: err instanceof Error ? err.message : String(err),
  });
  await broker.close().catch(() => undefined);
  process.exitCode = 1;
}
