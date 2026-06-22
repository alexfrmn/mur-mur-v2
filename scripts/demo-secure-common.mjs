import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { stableEnvelopePayload } from "@murmurv2/core";
import { createKeyPair, createSigningKeyPair, getCryptoProvider } from "@murmurv2/security";

// Re-export the canonical signing form from @murmurv2/core for the demo scripts.
export { stableEnvelopePayload };

const DEFAULT_KEYS_PATH = process.env.DEMO_KEYS_PATH || ".data/demo-keys.json";

const toInt = (value, fallback) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
};

export const loadDemoConfig = () => {
  const senderAgentId = process.env.SENDER_AGENT_ID || "alice";
  const recipientAgentId = process.env.RECIPIENT_AGENT_ID || "bob";

  return {
    natsUrl: process.env.NATS_URL || "nats://127.0.0.1:4222",
    natsToken: process.env.NATS_TOKEN || undefined,
    subject: process.env.SUBJECT || "msg.demo.secure",
    consumerId: process.env.CONSUMER_ID || recipientAgentId,
    outboxDbPath: process.env.OUTBOX_DB_PATH || ".data/demo-outbox.db",
    dedupeDbPath: process.env.DEDUPE_DB_PATH || ".data/demo-dedupe.db",
    keysPath: DEFAULT_KEYS_PATH,
    conversationId: process.env.CONVERSATION_ID || "demo-room",
    senderAgentId,
    recipientAgentId,
    message: process.env.MESSAGE || "ClawDigest secure demo payload",
    maxPayloadBytes: toInt(process.env.MAX_PAYLOAD_BYTES, 64 * 1024),
    ackTimeoutMs: toInt(process.env.ACK_TIMEOUT_MS, 15_000),
    waitForAckMs: toInt(process.env.WAIT_FOR_ACK_MS, 15_000),
    flushMaxAttempts: toInt(process.env.FLUSH_MAX_ATTEMPTS, 5),
    exitAfterOne: process.env.DEMO_EXIT_AFTER_ONE !== "0",
  };
};

export const policyFromConfig = (cfg) => ({
  maxPayloadBytes: cfg.maxPayloadBytes,
  allowedRoutes: {
    [cfg.senderAgentId]: [cfg.recipientAgentId],
  },
});


export const ensureDemoKeys = async (keysPath = DEFAULT_KEYS_PATH) => {
  try {
    const raw = await readFile(keysPath, "utf8");
    return JSON.parse(raw);
  } catch {
    const senderEncryption = await createKeyPair();
    const recipientEncryption = await createKeyPair();
    const senderSigning = await createSigningKeyPair();

    const keys = {
      cryptoProvider: getCryptoProvider().name,
      createdAt: new Date().toISOString(),
      sender: {
        encryption: senderEncryption,
        signing: senderSigning,
      },
      recipient: {
        encryption: recipientEncryption,
      },
    };

    await mkdir(path.dirname(keysPath), { recursive: true });
    await writeFile(keysPath, `${JSON.stringify(keys, null, 2)}\n`, "utf8");
    return keys;
  }
};
