#!/usr/bin/env node
/**
 * murmur-join.mjs — Join a peer using their invite blob.
 * Auto-creates agent config (if needed) and adds the inviter as peer.
 * Prints a reply blob to send back.
 *
 * Usage: node scripts/murmur-join.mjs MURMUR:eyJ...
 * Env: AGENT_ID (default: prompted), DATA_DIR (default: .data)
 */
import { createInterface } from "node:readline/promises";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { createKeyPair, createSigningKeyPair, getCryptoProvider } from "@murmurv2/security";

const blob = process.argv[2];
if (!blob || !blob.startsWith("MURMUR:")) {
  console.error("Usage: node scripts/murmur-join.mjs MURMUR:eyJ...");
  console.error("Get the invite blob from the host agent.");
  process.exit(1);
}

// Decode invite
let invite;
try {
  invite = JSON.parse(Buffer.from(blob.slice(7), "base64").toString("utf8"));
  if (invite.type !== "invite" || !invite.agentId || !invite.natsUrl) throw new Error("bad format");
} catch {
  console.error("[join] Invalid invite blob. Make sure you copied the full MURMUR:... string.");
  process.exit(1);
}

console.log(`[join] Invite from: ${invite.agentId}`);
console.log(`[join] NATS: ${invite.natsUrl}`);

const dataDir = process.env.DATA_DIR || ".data";
const configPath = path.join(dataDir, "agent-config.json");

// Check if config exists
let config;
try {
  config = JSON.parse(await readFile(configPath, "utf8"));
  console.log(`[join] Using existing config: ${config.agentId}`);
} catch {
  // Need to create config — ask for agent ID
  let agentId = process.env.AGENT_ID;
  if (!agentId) {
    const rl = createInterface({ input: process.stdin, output: process.stdout });
    agentId = (await rl.question(`Your agent ID [agent-friend]: `)).trim() || "agent-friend";
    rl.close();
  }

  console.log(`[join] Generating keypairs for ${agentId}...`);
  const encryption = await createKeyPair();
  const signing = await createSigningKeyPair();

  config = {
    agentId,
    natsUrl: invite.natsUrl,
    natsToken: invite.natsToken || undefined,
    subject: `msg.${agentId}`,
    dataDir,
    cryptoProvider: getCryptoProvider().name,
    keys: { encryption, signing },
    peers: {},
  };

  await mkdir(dataDir, { recursive: true });
  await writeFile(configPath, JSON.stringify(config, null, 2) + "\n", "utf8");
  console.log(`[join] Config created: ${configPath}`);
}

// Add inviter as peer
if (!config.peers) config.peers = {};
config.peers[invite.agentId] = {
  encryption: { publicKey: invite.encryption.publicKey },
  signing: { publicKey: invite.signing.publicKey },
  subject: invite.subject,
};

// Also update natsUrl/natsToken if config was pre-existing but pointed elsewhere
if (config.natsUrl !== invite.natsUrl) {
  console.log(`[join] Note: your NATS URL (${config.natsUrl}) differs from invite (${invite.natsUrl}).`);
  console.log(`[join] Keeping yours. Edit .data/agent-config.json if needed.`);
}

await writeFile(configPath, JSON.stringify(config, null, 2) + "\n", "utf8");
console.log(`[join] Added peer: ${invite.agentId}`);

// Generate reply blob
const reply = {
  v: 1,
  type: "reply",
  agentId: config.agentId,
  subject: config.subject,
  encryption: { publicKey: config.keys.encryption.publicKey },
  signing: { publicKey: config.keys.signing.publicKey },
};

const replyBlob = "MURMUR-REPLY:" + Buffer.from(JSON.stringify(reply)).toString("base64");

console.log("");
console.log("=== Send this reply back to the host ===");
console.log("");
console.log(replyBlob);
console.log("");
console.log("Done! Start your daemon: node scripts/murmur-daemon.mjs");
