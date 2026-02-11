#!/usr/bin/env node
/**
 * murmur-add-peer.mjs — Add a peer using their reply blob.
 * Completes the invite handshake.
 *
 * Usage: node scripts/murmur-add-peer.mjs MURMUR-REPLY:eyJ...
 * Env: DATA_DIR (default: .data)
 */
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const blob = process.argv[2];
if (!blob || !blob.startsWith("MURMUR-REPLY:")) {
  console.error("Usage: node scripts/murmur-add-peer.mjs MURMUR-REPLY:eyJ...");
  console.error("Get the reply blob from your peer after they ran murmur-join.mjs");
  process.exit(1);
}

// Decode reply
let reply;
try {
  reply = JSON.parse(Buffer.from(blob.slice(13), "base64").toString("utf8"));
  if (reply.type !== "reply" || !reply.agentId) throw new Error("bad format");
} catch {
  console.error("[add-peer] Invalid reply blob. Make sure you copied the full MURMUR-REPLY:... string.");
  process.exit(1);
}

// Load config
const dataDir = process.env.DATA_DIR || ".data";
const configPath = path.join(dataDir, "agent-config.json");

let config;
try {
  config = JSON.parse(await readFile(configPath, "utf8"));
} catch {
  console.error("[add-peer] No agent config found. Run first: node scripts/agent-config-init.mjs");
  process.exit(1);
}

// Add peer
if (!config.peers) config.peers = {};
config.peers[reply.agentId] = {
  encryption: { publicKey: reply.encryption.publicKey },
  signing: { publicKey: reply.signing.publicKey },
  subject: reply.subject,
};

await writeFile(configPath, JSON.stringify(config, null, 2) + "\n", "utf8");

console.log(`[add-peer] Added: ${reply.agentId} (${reply.subject})`);
console.log("");
console.log("Connection complete! Restart your daemon if running:");
console.log("  sudo systemctl restart murmur-daemon");
