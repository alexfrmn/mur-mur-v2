#!/usr/bin/env node
/**
 * murmur-daemon.mjs — Persistent agent-to-agent messaging daemon.
 */
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { NatsBroker } from "@murmurv2/broker-nats";
import { SQLiteDedupeOutboxStore, SQLiteMessageStore } from "@murmurv2/core";
import { decryptPayload, verifyEnvelopeSignature } from "@murmurv2/security";
import { NotifyQueue, flushNotifyQueue, normalizeNotifyTargets } from "./notify-router.mjs";
import { OpenClawBridgeQueue, flushOpenClawBridgeQueue, normalizeOpenClawTargets } from "./openclaw-bridge.mjs";

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
const notifyTargets = normalizeNotifyTargets(config.notify);
const openclawTargets = normalizeOpenClawTargets(config.notify);
const notifyQueue = new NotifyQueue(dbPath);
const openclawQueue = new OpenClawBridgeQueue(dbPath);

log("info", "Daemon starting", {
  agentId,
  subject,
  natsUrl,
  dbPath,
  flushIntervalMs,
  notifyTargets: notifyTargets.map((t) => `${t.type}:${t.channel}`),
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
    await flushOpenClawBridgeQueue({ queue: openclawQueue, log, limit: 20 });
  }

  if (notifyTargets.length > 0) {
    notifyQueue.enqueueMessage(payload, notifyTargets);
    log("info", "Notifications queued", {
      msgId: envelope.msgId,
      targetCount: notifyTargets.length,
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
      await flushOpenClawBridgeQueue({ queue: openclawQueue, log, limit: 100 });
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
    await flushOpenClawBridgeQueue({ queue: openclawQueue, log, limit: 250 });
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
