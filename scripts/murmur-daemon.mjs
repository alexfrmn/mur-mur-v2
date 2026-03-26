#!/usr/bin/env node
/**
 * murmur-daemon.mjs — Persistent agent-to-agent messaging daemon.
 */
import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { NatsBroker } from "@murmurv2/broker-nats";
import { SQLiteDedupeOutboxStore, SQLiteMessageStore } from "@murmurv2/core";
import { decryptPayload, encryptPayload, signEnvelope, verifyEnvelopeSignature } from "@murmurv2/security";
import { NotifyQueue, flushNotifyQueue, normalizeNotifyTargets } from "./notify-router.mjs";
import { OpenClawBridgeQueue, flushOpenClawBridgeQueue, normalizeOpenClawTargets } from "./openclaw-bridge.mjs";
import { vaultGuardCheck } from "./vault-guard.mjs";

const log = (level, msg, data) => {
  const entry = { ts: new Date().toISOString(), level, msg, ...data };
  console.log(JSON.stringify(entry));
};

const dataDir = process.env.DATA_DIR || ".data";
const configPath = path.join(dataDir, "agent-config.json");

let config;
try {
  config = JSON.parse(await readFile(configPath, "utf8"));
} catch (err) {
  log("fatal", "Cannot load agent config", { path: configPath, error: err.message });
  log("info", "Run: node scripts/agent-config-init.mjs");
  process.exit(1);
}

const { agentId, natsUrl, natsToken, subject, peers, keys } = config;
const dbPath = path.join(dataDir, "murmur.db");
const flushIntervalMs = Number(process.env.FLUSH_INTERVAL_MS) || 2000;
let openclawFlushLock = false;
const guardedFlushOpenClaw = async (opts) => {
  if (openclawFlushLock) return 0;
  openclawFlushLock = true;
  try { return await flushOpenClawBridgeQueue(opts); }
  finally { openclawFlushLock = false; }
};
const notifyTargets = normalizeNotifyTargets(config.notify);
const envTelegramFallback = (() => {
  const botToken = process.env.MURMUR_TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.MURMUR_TELEGRAM_CHAT_ID || process.env.TELEGRAM_CHAT_ID;
  const topicId = process.env.MURMUR_TELEGRAM_TOPIC_ID || process.env.TELEGRAM_TOPIC_ID;
  if (notifyTargets.length > 0 || !botToken || !chatId) return [];
  return [{ type: "telegram", channel: "telegram", botToken, chatId, ...(topicId ? { topicId } : {}) }];
})();
const effectiveNotifyTargets = notifyTargets.length > 0 ? notifyTargets : envTelegramFallback;
const openclawTargets = normalizeOpenClawTargets(config.notify);
const notifyQueue = new NotifyQueue(dbPath);
const openclawQueue = new OpenClawBridgeQueue(dbPath);

log("info", "Daemon starting", {
  agentId,
  subject,
  natsUrl,
  dbPath,
  flushIntervalMs,
  notifyTargets: effectiveNotifyTargets.map((t) => `${t.type}:${t.channel}`),
  notifyFallbackFromEnv: envTelegramFallback.length > 0,
  openclawTargets: openclawTargets.map((t) => `openclaw:${t.channel}`),
});

const store = new SQLiteDedupeOutboxStore(dbPath);
const msgStore = new SQLiteMessageStore(dbPath);
const broker = new NatsBroker({ url: natsUrl, token: natsToken });

const stableEnvelopePayload = (envelope) => JSON.stringify({
  schemaVersion: envelope.schemaVersion,
  msgId: envelope.msgId,
  conversationId: envelope.conversationId,
  senderAgentId: envelope.senderAgentId,
  recipients: [...envelope.recipients],
  createdAt: envelope.createdAt,
  payloadCiphertext: envelope.payloadCiphertext,
  payloadNonce: envelope.payloadNonce,
});

/**
 * Send a reply back to the original sender via Murmur (encrypt + sign + enqueue to outbox).
 * Used by OpenClaw bridge when replyViaMurmur is enabled.
 */
const sendReply = async (originalPayload, responseText) => {
  const to = originalPayload.from;
  const peer = peers[to];
  if (!peer) {
    log("warn", "Cannot reply — unknown peer", { to });
    return;
  }

  const msgId = randomUUID();
  const conversationId = originalPayload.conversationId || `dm:${agentId}:${to}`;
  const createdAt = new Date().toISOString();

  const encrypted = await encryptPayload(responseText, peer.encryption.publicKey, keys.encryption.privateKey);

  const envelope = {
    schemaVersion: "1.0",
    msgId,
    conversationId,
    senderAgentId: agentId,
    recipients: [to],
    createdAt,
    payloadCiphertext: encrypted.ciphertext,
    payloadNonce: encrypted.nonce,
    signature: "",
  };

  envelope.signature = await signEnvelope(stableEnvelopePayload(envelope), keys.signing.privateKey);

  // Vault guard: warn if vault content being sent to non-vault agent
  vaultGuardCheck(to, responseText, log);

  await store.enqueue(peer.subject, envelope);
  await msgStore.append({ conversationId, msgId, direction: "outbound", sender: agentId, text: responseText, createdAt, transport: "nats" });

  log("info", "Reply enqueued to outbox", { to, msgId, conversationId, textLen: responseText.length });
};

const onMessage = async (envelope) => {
  const senderId = envelope.senderAgentId;
  const peer = peers[senderId];

  if (!peer) throw new Error(`unknown-sender:${senderId}`);

  const sigPayload = stableEnvelopePayload(envelope);
  const valid = await verifyEnvelopeSignature(sigPayload, envelope.signature, peer.signing.publicKey);
  if (!valid) throw new Error(`signature-invalid:${senderId}`);

  const plaintext = await decryptPayload(
    {
      ciphertext: envelope.payloadCiphertext,
      nonce: envelope.payloadNonce,
      senderPublicKey: peer.encryption.publicKey,
    },
    keys.encryption.privateKey,
  );

  await msgStore.append({
    conversationId: envelope.conversationId,
    msgId: envelope.msgId,
    direction: "inbound",
    sender: senderId,
    text: plaintext,
    createdAt: envelope.createdAt,
    transport: "nats",
  });

  log("info", "Message received", {
    msgId: envelope.msgId,
    from: senderId,
    conversationId: envelope.conversationId,
    textLen: plaintext.length,
  });

  const payload = {
    from: senderId,
    text: plaintext,
    msgId: envelope.msgId,
    conversationId: envelope.conversationId,
    ts: new Date().toISOString(),
  };

  if (openclawTargets.length > 0) {
    openclawQueue.enqueueMessage(payload, openclawTargets);
    log("info", "OpenClaw bridge queued", {
      msgId: envelope.msgId,
      targetCount: openclawTargets.length,
    });
    await guardedFlushOpenClaw({ queue: openclawQueue, log, limit: 20, onResponse: sendReply });
  }

  if (effectiveNotifyTargets.length > 0) {
    notifyQueue.enqueueMessage(payload, effectiveNotifyTargets);
    log("info", "Notifications queued", {
      msgId: envelope.msgId,
      targetCount: effectiveNotifyTargets.length,
    });
  }

  if (config.onReceive) {
    try {
      const env = {
        ...process.env,
        MURMUR_FROM: senderId,
        MURMUR_TEXT: plaintext,
        MURMUR_MSG_ID: envelope.msgId,
        MURMUR_CONVERSATION_ID: envelope.conversationId,
      };
      execFile("sh", ["-c", config.onReceive], { env, timeout: 10_000 }, (err) => {
        if (err) log("warn", "onReceive hook failed", { error: err.message });
      });
    } catch (err) {
      log("warn", "onReceive hook error", { error: err.message });
    }
  }
};

let running = true;

const flushLoop = async () => {
  while (running) {
    try {
      await broker.flushOutbox({ outbox: store, maxAttempts: 5, ackTimeoutMs: 15_000 });
    } catch (err) {
      log("error", "Outbox flush error", { error: err.message });
    }

    try {
      await guardedFlushOpenClaw({ queue: openclawQueue, log, limit: 100, onResponse: sendReply });
    } catch (err) {
      log("error", "OpenClaw bridge flush error", { error: err.message });
    }

    try {
      await flushNotifyQueue({ queue: notifyQueue, log, limit: 100 });
    } catch (err) {
      log("error", "Notify flush error", { error: err.message });
    }

    await sleep(flushIntervalMs);
  }
};

const shutdown = async (signal) => {
  log("info", "Shutdown signal received", { signal });
  running = false;
  try {
    await broker.close();
  } catch (err) {
    log("error", "Broker close error", { error: err.message });
  }
  log("info", "Daemon stopped", { agentId });
  process.exit(0);
};

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

try {
  await broker.connect();
  log("info", "NATS connected", { url: natsUrl });

  await broker.subscribeWithAck({ subject, consumerId: agentId, dedupe: store, onMessage });
  log("info", "Subscribed", { subject });

  // Also subscribe to proxy subjects (agents without their own daemon)
  const proxySubjects = (config.proxySubjects || []);
  for (const ps of proxySubjects) {
    const proxyOnMessage = async (envelope, plaintext) => {
      const senderId = envelope.senderAgentId || "unknown";
      log("info", "Proxy message received", { subject: ps, from: senderId, len: plaintext?.length });
      // Run LLM handler for proxy agents
      if (config.proxyOnReceive) {
        try {
          const env = {
            ...process.env,
            MURMUR_FROM: senderId,
            MURMUR_TEXT: plaintext,
            MURMUR_MSG_ID: envelope.msgId,
            MURMUR_CONVERSATION_ID: envelope.conversationId,
            MURMUR_PROXY_AGENT: ps.replace("msg.", ""),
          };
          execFile("sh", ["-c", config.proxyOnReceive], { env, timeout: 60_000 }, (err, stdout, stderr) => {
            if (err) log("warn", "proxyOnReceive hook failed", { error: err.message, stderr: stderr?.slice(0,200) });
            else log("info", "proxyOnReceive hook completed", { stdout: stdout?.slice(0,100) });
          });
        } catch (err) {
          log("warn", "proxyOnReceive hook error", { error: err.message });
        }
      }
    };
    await broker.subscribeWithAck({ subject: ps, consumerId: `${agentId}-proxy`, dedupe: store, onMessage: proxyOnMessage });
    log("info", "Subscribed (proxy)", { subject: ps });
  }

  await broker.startAckCorrelation({ outbox: store, ackSubject: `ack.${agentId}` });
  log("info", "ACK correlation started", { ackSubject: `ack.${agentId}` });

  const pendingBridge = openclawQueue.pendingCount();
  if (pendingBridge > 0) {
    log("info", "Resuming pending OpenClaw bridge dispatches", { pendingBridge });
    await guardedFlushOpenClaw({ queue: openclawQueue, log, limit: 250, onResponse: sendReply });
  }

  const pendingNotify = notifyQueue.pendingCount();
  if (pendingNotify > 0) {
    log("info", "Resuming pending notifications", { pendingNotify });
    await flushNotifyQueue({ queue: notifyQueue, log, limit: 250 });
  }

  flushLoop();
  log("info", "Daemon ready", { agentId, peers: Object.keys(peers) });
} catch (err) {
  log("fatal", "Daemon startup failed", { error: err.message });
  await broker.close().catch(() => {});
  process.exit(1);
}
