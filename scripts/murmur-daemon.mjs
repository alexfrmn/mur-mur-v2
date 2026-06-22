#!/usr/bin/env node
/**
 * murmur-daemon.mjs — Persistent agent-to-agent messaging daemon.
 */
import { readFile } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { NatsBroker } from "@murmurv2/broker-nats";
import { SQLiteDedupeOutboxStore, SQLiteMessageStore } from "@murmurv2/core";
import { decryptPayload, verifyEnvelopeSignature } from "@murmurv2/security";
import { NotifyQueue, flushNotifyQueue, normalizeNotifyTargets } from "./notify-router.mjs";
import { createCodexAppServerInjector } from "./codex-app-server-wake.mjs";
import { startJetStreamAdvisoryDlqIfEnabled } from "./murmur-jetstream-advisory.mjs";
import { WakeMonitor, createAuditShellHook, createShellHook, normalizeWakeConfig } from "./wake-monitor.mjs";
// vault-guard: optional content policy hook (not included in OSS release)

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
const jetstreamConfig = config.jetstream || {};
const jetstreamEnabled = jetstreamConfig.enabled ?? process.env.MURMUR_JETSTREAM === "1";
const jetstreamStream = jetstreamConfig.stream || process.env.MURMUR_JETSTREAM_STREAM || "MURMUR";
const jetstreamSubjects = jetstreamConfig.subjects || ["msg.>", "ack.>"];
const streamingConfig = config.streaming || {};
const ackWindowConfig = streamingConfig.ackWindow || {};
const firstDefined = (...values) => values.find((value) => value !== undefined && value !== null && value !== "");
const optionalPositiveInteger = (name, value) => {
  if (value === undefined || value === null || value === "") return undefined;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`${name}-invalid`);
  return parsed;
};
const jetstreamMaxDeliver = optionalPositiveInteger(
  "jetstream-max-deliver",
  firstDefined(jetstreamConfig.maxDeliver, process.env.MURMUR_JETSTREAM_MAX_DELIVER),
);
const jetstreamAckWaitMs = optionalPositiveInteger(
  "jetstream-ack-wait-ms",
  firstDefined(jetstreamConfig.ackWaitMs, process.env.MURMUR_JETSTREAM_ACK_WAIT_MS),
);
const ackTimeoutMs = optionalPositiveInteger(
  "ack-timeout-ms",
  firstDefined(streamingConfig.ackTimeoutMs, process.env.MURMUR_ACK_TIMEOUT_MS),
) ?? 15_000;
const ackWindowEnabled = ackWindowConfig.enabled ?? process.env.MURMUR_STREAM_ACK_WINDOW === "1";
const ackWindow = ackWindowEnabled
  ? {
      maxInFlightChunks: optionalPositiveInteger(
        "stream-max-in-flight-chunks",
        firstDefined(ackWindowConfig.maxInFlightChunks, process.env.MURMUR_STREAM_MAX_IN_FLIGHT_CHUNKS),
      ) ?? 64,
      maxInFlightBytes: optionalPositiveInteger(
        "stream-max-in-flight-bytes",
        firstDefined(ackWindowConfig.maxInFlightBytes, process.env.MURMUR_STREAM_MAX_IN_FLIGHT_BYTES),
      ) ?? 4 * 1024 * 1024,
    }
  : undefined;
const notifyTargets = normalizeNotifyTargets(config.notify);
const envTelegramFallback = (() => {
  const botToken = process.env.MURMUR_TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.MURMUR_TELEGRAM_CHAT_ID || process.env.TELEGRAM_CHAT_ID;
  const topicId = process.env.MURMUR_TELEGRAM_TOPIC_ID || process.env.TELEGRAM_TOPIC_ID;
  if (notifyTargets.length > 0 || !botToken || !chatId) return [];
  return [{ type: "telegram", channel: "telegram", botToken, chatId, ...(topicId ? { topicId } : {}) }];
})();
const effectiveNotifyTargets = notifyTargets.length > 0 ? notifyTargets : envTelegramFallback;
const notifyQueue = new NotifyQueue(dbPath);
const wakeDb = new DatabaseSync(dbPath);
const wakeConfig = normalizeWakeConfig(config);

log("info", "Daemon starting", {
  agentId,
  subject,
  natsUrl,
  dbPath,
  flushIntervalMs,
  jetstreamEnabled,
  jetstreamStream: jetstreamEnabled ? jetstreamStream : undefined,
  jetstreamMaxDeliver,
  jetstreamAckWaitMs,
  ackTimeoutMs,
  ackWindow,
  notifyTargets: effectiveNotifyTargets.map((t) => `${t.type}:${t.channel}`),
  notifyFallbackFromEnv: envTelegramFallback.length > 0,
});

const store = new SQLiteDedupeOutboxStore(dbPath);
const msgStore = new SQLiteMessageStore(dbPath);
const broker = new NatsBroker({
  url: natsUrl,
  token: natsToken,
  jetstream: jetstreamEnabled,
  stream: jetstreamEnabled ? jetstreamStream : undefined,
  streamSubjects: jetstreamSubjects,
  jetstreamMaxDeliver,
  jetstreamAckWaitMs,
});

const durableSafe = (value) => value.replace(/[^A-Za-z0-9_-]/g, "-");

const inboundCursor = () => {
  const row = wakeDb.prepare("SELECT COALESCE(MAX(rowid), 0) as cursor FROM local_messages WHERE direction = 'inbound'").get();
  return Number(row?.cursor ?? 0);
};

const inboundCursorForMsg = (msgId) => {
  const row = wakeDb.prepare("SELECT rowid as cursor FROM local_messages WHERE direction = 'inbound' AND msg_id = ? ORDER BY rowid DESC LIMIT 1").get(msgId);
  return Number(row?.cursor ?? 0);
};

const loadInboundAfter = async (cursor) => {
  const rows = wakeDb
    .prepare(
      `SELECT
         rowid as cursor,
         conversation_id as conversationId,
         msg_id as msgId,
         sender as "from",
         text,
         created_at as ts
       FROM local_messages
       WHERE direction = 'inbound' AND rowid > ?
       ORDER BY rowid ASC
       LIMIT 100`,
    )
    .all(cursor);
  return rows.map((row) => ({
    from: row.from,
    text: row.text,
    msgId: row.msgId,
    conversationId: row.conversationId,
    ts: row.ts,
    cursor: Number(row.cursor),
  }));
};

const enqueueWakeNotification = async (payload, reason) => {
  log("warn", "WakeMonitor fallback notify", { reason, msgId: payload.msgId, from: payload.from });
  if (effectiveNotifyTargets.length === 0) return;
  notifyQueue.enqueueMessage({
    ...payload,
    text: `[WakeMonitor ${reason}] ${payload.text}`,
  }, effectiveNotifyTargets);
};

const wakeMonitor = new WakeMonitor({
  ...wakeConfig,
  initialCursor: inboundCursor(),
  loadBacklogAfter: loadInboundAfter,
  auditHook: createAuditShellHook({ command: wakeConfig.auditHook, log }),
  hook: createShellHook({ command: config.onReceive, log }),
  injector: async (payload, peer) => {
    if (peer.mode === "codex_app_server") {
      return createCodexAppServerInjector({ log })(payload, peer);
    }
    throw new Error(`wake-native-mode-unsupported:${peer.mode}`);
  },
  notify: enqueueWakeNotification,
  log,
});

const proxyWakeMonitor = new WakeMonitor({
  ...wakeConfig,
  initialCursor: inboundCursor(),
  auditHook: createAuditShellHook({ command: wakeConfig.auditHook, log }),
  hook: createShellHook({ command: config.proxyOnReceive, log }),
  injector: async (payload, peer) => {
    if (peer.mode === "codex_app_server") {
      return createCodexAppServerInjector({ log })(payload, peer);
    }
    throw new Error(`wake-native-mode-unsupported:${peer.mode}`);
  },
  notify: enqueueWakeNotification,
  log,
});

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
    cursor: inboundCursorForMsg(envelope.msgId),
  };

  if (effectiveNotifyTargets.length > 0) {
    notifyQueue.enqueueMessage(payload, effectiveNotifyTargets);
    log("info", "Notifications queued", {
      msgId: envelope.msgId,
      targetCount: effectiveNotifyTargets.length,
    });
  }

  await wakeMonitor.onInbound(payload);
};

let running = true;

const flushLoop = async () => {
  while (running) {
    try {
      await broker.flushOutbox({ outbox: store, maxAttempts: 5, ackTimeoutMs, ackWindow });
    } catch (err) {
      log("error", "Outbox flush error", { error: err.message });
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
  log("info", "Shutdown signal received, draining NATS", { signal });
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
      await proxyWakeMonitor.onInbound({
        from: senderId,
        text: plaintext ?? "",
        msgId: envelope.msgId,
        conversationId: envelope.conversationId,
        ts: new Date().toISOString(),
        env: { MURMUR_PROXY_AGENT: ps.replace("msg.", "") },
      });
    };
    await broker.subscribeWithAck({ subject: ps, consumerId: `${agentId}-proxy-${durableSafe(ps)}`, dedupe: store, onMessage: proxyOnMessage });
    log("info", "Subscribed (proxy)", { subject: ps });
  }

  await broker.startAckCorrelation({ outbox: store, ackSubject: `ack.${agentId}`, consumerId: `${agentId}-ack` });
  log("info", "ACK correlation started", { ackSubject: `ack.${agentId}` });
  await startJetStreamAdvisoryDlqIfEnabled({
    broker,
    outbox: store,
    jetstreamEnabled,
    log,
  });

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
