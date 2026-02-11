#!/usr/bin/env node
/**
 * murmur-invite.mjs — Generate invite blob for a remote peer.
 * The invite contains NATS connection info + your public keys.
 * Send the blob to your friend via any messenger.
 *
 * Usage: node scripts/murmur-invite.mjs
 * Env: DATA_DIR (default: .data)
 */
import { readFile } from "node:fs/promises";
import path from "node:path";

const dataDir = process.env.DATA_DIR || ".data";
const configPath = path.join(dataDir, "agent-config.json");

let config;
try {
  config = JSON.parse(await readFile(configPath, "utf8"));
} catch {
  console.error("[invite] No agent config found. Run first: node scripts/agent-config-init.mjs");
  process.exit(1);
}

const invite = {
  v: 1,
  type: "invite",
  agentId: config.agentId,
  natsUrl: config.natsUrl,
  natsToken: config.natsToken || undefined,
  subject: config.subject,
  encryption: { publicKey: config.keys.encryption.publicKey },
  signing: { publicKey: config.keys.signing.publicKey },
};

const blob = "MURMUR:" + Buffer.from(JSON.stringify(invite)).toString("base64");

console.log("");
console.log("=== Send this invite to your peer ===");
console.log("");
console.log(blob);
console.log("");
console.log(`Your agent: ${config.agentId}`);
console.log(`NATS: ${config.natsUrl}`);
console.log("");
console.log("Peer should run: node scripts/murmur-join.mjs MURMUR:...");
console.log("Then send you back the MURMUR-REPLY:... blob.");
console.log("You finish with: node scripts/murmur-add-peer.mjs MURMUR-REPLY:...");
