#!/usr/bin/env node
/**
 * murmur-daemon.mjs — Persistent agent-to-agent messaging daemon.
 *
 * - Loads agent config from .data/agent-config.json
 * - Connects to NATS, subscribes for incoming messages
 * - Verifies signature + decrypts payload → stores inbound in SQLite
 * - Flushes outbox every 2s (messages enqueued by MCP server)
 * - ACK correlation for reliable delivery
 * - Graceful shutdown on SIGTERM/SIGINT
 *
 * Usage: node scripts/murmur-daemon.mjs
 * Env: DATA_DIR (default: .data), FLUSH_INTERVAL_MS (default: 2000)
 */
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { NatsBroker } from "@murmurv2/broker-nats";
import { SQLiteDedupeOutboxStore, SQLiteMessageStore } from "@murmurv2/core";
import {
  decryptPayload,
  verifyEnvelopeSignature,
} from "@murmurv2/security";

// --- Structured logging for journald ---
const log = (level, msg, data) => {
  const entry = { ts: new Date().toISOString(), level, msg, ...data };
  console.log(JSON.stringify(entry));
};

// --- Load agent config ---
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

log("info", "Daemon starting", { agentId, subject, natsUrl, dbPath, flushIntervalMs });

// --- Stores (shared SQLite with MCP server) ---
const store = new SQLiteDedupeOutboxStore(dbPath);
const msgStore = new SQLiteMessageStore(dbPath);

// --- NATS broker ---
const broker = new NatsBroker({ url: natsUrl, token: natsToken });

// --- Stable envelope payload for signature verification ---
const stableEnvelopePayload = (envelope) => {
  return JSON.stringify({
    schemaVersion: envelope.schemaVersion,
    msgId: envelope.msgId,
    conversationId: envelope.conversationId,
    senderAgentId: envelope.senderAgentId,
    recipients: [...envelope.recipients],
    createdAt: envelope.createdAt,
    payloadCiphertext: envelope.payloadCiphertext,
    payloadNonce: envelope.payloadNonce,
  });
};

// --- Incoming message handler ---
const onMessage = async (envelope) => {
  const senderId = envelope.senderAgentId;
  const peer = peers[senderId];

  if (!peer) {
    throw new Error(`unknown-sender:${senderId}`);
  }

  // Verify signature
  const sigPayload = stableEnvelopePayload(envelope);
  const valid = await verifyEnvelopeSignature(sigPayload, envelope.signature, peer.signing.publicKey);
  if (!valid) {
    throw new Error(`signature-invalid:${senderId}`);
  }

  // Decrypt payload
  const plaintext = await decryptPayload(
    {
      ciphertext: envelope.payloadCiphertext,
      nonce: envelope.payloadNonce,
      senderPublicKey: peer.encryption.publicKey,
    },
    keys.encryption.privateKey,
  );

  // Store inbound message
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

  // --- onReceive hook: notify external system ---
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

// --- Outbox flush loop ---
let running = true;

const flushLoop = async () => {
  while (running) {
    try {
      await broker.flushOutbox({
        outbox: store,
        maxAttempts: 5,
        ackTimeoutMs: 15_000,
      });
    } catch (err) {
      log("error", "Outbox flush error", { error: err.message });
    }
    await sleep(flushIntervalMs);
  }
};

// --- Graceful shutdown ---
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

// --- Main startup ---
try {
  await broker.connect();
  log("info", "NATS connected", { url: natsUrl });

  // Subscribe for incoming messages
  await broker.subscribeWithAck({
    subject,
    consumerId: agentId,
    dedupe: store,
    onMessage,
  });
  log("info", "Subscribed", { subject });

  // Start ACK correlation for outbound messages
  await broker.startAckCorrelation({
    outbox: store,
    ackSubject: `ack.${agentId}`,
  });
  log("info", "ACK correlation started", { ackSubject: `ack.${agentId}` });

  // Start outbox flush loop
  flushLoop();

  log("info", "Daemon ready", { agentId, peers: Object.keys(peers) });
} catch (err) {
  log("fatal", "Daemon startup failed", { error: err.message });
  await broker.close().catch(() => {});
  process.exit(1);
}
