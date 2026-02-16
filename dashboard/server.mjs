#!/usr/bin/env node
/**
 * Murmur V2 Observability Dashboard — Backend
 * 
 * Subscribes to NATS wildcard msg.> and ack.>
 * Decrypts messages when possible
 * Streams events to browser via WebSocket
 * Serves static dashboard.html
 * 
 * Usage: node dashboard/server.mjs
 * Env: NATS_URL, NATS_TOKEN, DATA_DIR, DASHBOARD_PORT
 */

import { createServer } from "node:http";
import { readFile, readdir } from "node:fs/promises";
import { WebSocketServer } from "ws";
import path from "node:path";
import { connect, StringCodec } from "nats";
import { x25519 } from "@noble/curves/ed25519";
import { xchacha20poly1305 } from "@noble/ciphers/chacha";
import { createHash } from "node:crypto";
import Database from "better-sqlite3";

const PORT = Number(process.env.DASHBOARD_PORT) || 4280;
const NATS_URL = process.env.NATS_URL || "nats://5.181.3.139:4222";
const NATS_TOKEN = process.env.NATS_TOKEN || "94c4105cfc57ebc5d93498f6bd0d71f4ffdffb825eea8249c71407f1971ebc12";
const DATA_DIR = process.env.DATA_DIR || path.join(path.dirname(new URL(import.meta.url).pathname), "..", ".data");
const DASHBOARD_DIR = path.dirname(new URL(import.meta.url).pathname);

// Load agent config
let config;
try {
  config = JSON.parse(await readFile(path.join(DATA_DIR, "agent-config.json"), "utf8"));
} catch (e) {
  console.error("[dashboard] Cannot load agent-config.json:", e.message);
  process.exit(1);
}

// Crypto helpers
const fromB64 = (s) => new Uint8Array(Buffer.from(s, "base64"));
const deriveSymmetricKey = (shared) => new Uint8Array(createHash("sha256").update(shared).digest());

function tryDecrypt(envelope) {
  try {
    const senderId = envelope.senderAgentId;
    const peer = config.peers[senderId];
    if (!peer) return { decrypted: false, text: `[encrypted from ${senderId}]` };

    const senderPubKey = fromB64(peer.encryption.publicKey);
    const myPrivKey = fromB64(config.keys.encryption.privateKey);
    const shared = x25519.getSharedSecret(myPrivKey, senderPubKey);
    const key = deriveSymmetricKey(shared);
    const nonce = fromB64(envelope.payloadNonce);
    const ciphertext = fromB64(envelope.payloadCiphertext);
    const cipher = xchacha20poly1305(key, nonce);
    const plaintext = new TextDecoder().decode(cipher.decrypt(ciphertext));
    return { decrypted: true, text: plaintext };
  } catch (e) {
    return { decrypted: false, text: `[decrypt failed: ${e.message}]` };
  }
}

// Load message history from SQLite
function loadHistory(limit = 50) {
  try {
    const dbPath = path.join(DATA_DIR, "murmur.db");
    const db = new Database(dbPath, { readonly: true });
    const rows = db.prepare(`
      SELECT conversation_id, msg_id, direction, sender, text, created_at, transport 
      FROM local_messages 
      ORDER BY rowid DESC 
      LIMIT ?
    `).all(limit);
    db.close();
    return rows.reverse();
  } catch (e) {
    console.warn("[dashboard] Cannot load history:", e.message);
    return [];
  }
}

// HTTP server — serves dashboard.html
const httpServer = createServer(async (req, res) => {
  if (req.url === "/3d" || req.url === "/3d/") {
    try {
      const html = await readFile(path.join(DASHBOARD_DIR, "3d.html"), "utf8");
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(html);
    } catch {
      res.writeHead(404);
      res.end("3d.html not found");
    }
  } else if (req.url === "/" || req.url === "/index.html") {
    try {
      const html = await readFile(path.join(DASHBOARD_DIR, "dashboard.html"), "utf8");
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(html);
    } catch {
      res.writeHead(404);
      res.end("dashboard.html not found");
    }
  } else if (req.url === "/api/agents") {
    const agents = [config.agentId, ...Object.keys(config.peers)];
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ agents, self: config.agentId }));
  } else if (req.url === "/api/history") {
    const history = loadHistory(100);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(history));
  } else {
    res.writeHead(404);
    res.end("Not found");
  }
});

// WebSocket server
const wss = new WebSocketServer({ server: httpServer });
const clients = new Set();

wss.on("connection", (ws) => {
  clients.add(ws);
  console.log(`[dashboard] Client connected (${clients.size} total)`);

  // Send agent list
  ws.send(JSON.stringify({
    type: "init",
    self: config.agentId,
    agents: [config.agentId, ...Object.keys(config.peers)],
    ts: new Date().toISOString(),
  }));

  // Send history
  const history = loadHistory(50);
  for (const row of history) {
    ws.send(JSON.stringify({
      type: "message",
      from: row.sender,
      to: row.direction === "inbound" ? config.agentId : row.conversation_id?.split(":")?.[1] || "unknown",
      text: row.text?.slice(0, 500),
      ts: row.created_at,
      msgId: row.msg_id,
      encrypted: false, // already decrypted in store
      direction: row.direction,
      historical: true,
    }));
  }

  ws.on("close", () => {
    clients.delete(ws);
    console.log(`[dashboard] Client disconnected (${clients.size} total)`);
  });
});

function broadcast(data) {
  const json = JSON.stringify(data);
  for (const ws of clients) {
    if (ws.readyState === 1) ws.send(json);
  }
}

// NATS subscription — listen to all agent messages
const sc = StringCodec();
let nc;
let msgCount = 0;
const startTime = Date.now();

try {
  nc = await connect({ servers: NATS_URL, token: NATS_TOKEN });
  console.log(`[dashboard] NATS connected: ${NATS_URL}`);

  // Subscribe to all msg.* subjects
  const sub = nc.subscribe("msg.>");
  console.log(`[dashboard] Subscribed to msg.>`);

  (async () => {
    for await (const m of sub) {
      msgCount++;
      try {
        const raw = sc.decode(m.data);
        const envelope = JSON.parse(raw);

        if (envelope.payloadCiphertext) {
          // Encrypted envelope
          const { decrypted, text } = tryDecrypt(envelope);
          broadcast({
            type: "message",
            from: envelope.senderAgentId || "unknown",
            to: (envelope.recipients || [])[0] || m.subject.replace("msg.", ""),
            text: text.slice(0, 1000),
            ts: envelope.createdAt || new Date().toISOString(),
            msgId: envelope.msgId,
            encrypted: true,
            decrypted,
            direction: m.subject === `msg.${config.agentId}` ? "inbound" : "outbound",
          });
        } else if (envelope.payload) {
          // Plain message
          broadcast({
            type: "message",
            from: envelope.from || "unknown",
            to: envelope.to || m.subject.replace("msg.", ""),
            text: (envelope.payload || "").slice(0, 1000),
            ts: envelope.ts || new Date().toISOString(),
            msgId: envelope.msgId,
            encrypted: false,
            decrypted: true,
            direction: m.subject === `msg.${config.agentId}` ? "inbound" : "outbound",
          });
        }
      } catch (e) {
        broadcast({
          type: "error",
          text: `Parse error: ${e.message}`,
          ts: new Date().toISOString(),
        });
      }
    }
  })();

  // Periodic stats
  setInterval(() => {
    broadcast({
      type: "stats",
      totalMessages: msgCount,
      activeClients: clients.size,
      uptimeMs: Date.now() - startTime,
      agents: [config.agentId, ...Object.keys(config.peers)],
      ts: new Date().toISOString(),
    });
  }, 5000);

} catch (e) {
  console.error(`[dashboard] NATS failed: ${e.message}`);
}

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log(`[dashboard] 🚀 http://localhost:${PORT}`);
  console.log(`[dashboard] WebSocket: ws://localhost:${PORT}`);
  console.log(`[dashboard] Agents: ${config.agentId}, ${Object.keys(config.peers).join(", ")}`);
});
