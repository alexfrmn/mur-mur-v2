#!/usr/bin/env node
/**
 * delegate-task.mjs — Send a coding task to CODEX and poll for response.
 *
 * Usage:
 *   node delegate-task.mjs <agent-id> <task-description> [--timeout 120] [--context file.py]
 *
 * Examples:
 *   node delegate-task.mjs agent-codex "Write a Python function that parses CSV"
 *   node delegate-task.mjs codex2-agent-hq "Fix the bug in this code" --context /opt/lifecoach/bot/bot.py
 *   node delegate-task.mjs agent-codex "Refactor this function" --timeout 180
 */

import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { SQLiteDedupeOutboxStore, SQLiteMessageStore } from "@murmurv2/core";
import { encryptPayload, signEnvelope } from "@murmurv2/security";

const dataDir = process.env.DATA_DIR || ".data";
const config = JSON.parse(readFileSync(`${dataDir}/agent-config.json`, "utf8"));
const store = new SQLiteMessageStore(`${dataDir}/murmur.db`);
const outbox = new SQLiteDedupeOutboxStore(`${dataDir}/murmur.db`);

// Parse args
const args = process.argv.slice(2);
if (args.length < 2) {
  console.error("Usage: delegate-task.mjs <agent-id> <task-description> [--timeout N] [--context file]");
  console.error("Agents:", Object.keys(config.peers).join(", "));
  process.exit(1);
}

const to = args[0];
let taskText = args[1];
let timeoutSec = 120;
let contextFile = null;

for (let i = 2; i < args.length; i++) {
  if (args[i] === "--timeout" && args[i + 1]) {
    timeoutSec = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === "--context" && args[i + 1]) {
    contextFile = args[i + 1];
    i++;
  }
}

// Attach file context if provided
if (contextFile) {
  try {
    const content = readFileSync(contextFile, "utf8");
    const lines = content.split("\n").length;
    if (lines > 500) {
      // Truncate to first 500 lines with warning
      const truncated = content.split("\n").slice(0, 500).join("\n");
      taskText += `\n\n--- FILE CONTEXT (${contextFile}, first 500 of ${lines} lines) ---\n${truncated}\n--- END FILE CONTEXT ---`;
    } else {
      taskText += `\n\n--- FILE CONTEXT (${contextFile}, ${lines} lines) ---\n${content}\n--- END FILE CONTEXT ---`;
    }
  } catch (e) {
    console.error(`Warning: could not read context file: ${contextFile}`);
  }
}

const peer = config.peers[to];
if (!peer) {
  console.error("Unknown peer:", to);
  console.error("Available:", Object.keys(config.peers).join(", "));
  process.exit(1);
}

// Format the task as a clear instruction
const fullText = `[TASK FROM JARVIS]
${taskText}

[INSTRUCTIONS]
- Respond with the complete solution
- Include code in markdown code blocks
- Be concise but complete
- If you need clarification, ask specific questions`;

const conversationId = `task:${randomUUID().substring(0, 8)}`;
const msgId = randomUUID();
const sentAt = new Date().toISOString();

// Encrypt and send
const encrypted = await encryptPayload(fullText, peer.encryption.publicKey, config.keys.encryption.privateKey);

const envelope = {
  schemaVersion: "1.0",
  msgId,
  conversationId,
  senderAgentId: config.agentId,
  recipients: [to],
  createdAt: sentAt,
  payloadCiphertext: encrypted.ciphertext,
  payloadNonce: encrypted.nonce,
  signature: "",
};

const stablePayload = JSON.stringify({
  schemaVersion: envelope.schemaVersion,
  msgId: envelope.msgId,
  conversationId: envelope.conversationId,
  senderAgentId: envelope.senderAgentId,
  recipients: [...envelope.recipients],
  createdAt: envelope.createdAt,
  payloadCiphertext: envelope.payloadCiphertext,
  payloadNonce: envelope.payloadNonce,
});

envelope.signature = await signEnvelope(stablePayload, config.keys.signing.privateKey);

await outbox.enqueue(peer.subject, envelope);
await store.append({
  conversationId,
  msgId,
  direction: "outbound",
  sender: config.agentId,
  text: fullText,
  createdAt: sentAt,
  transport: "nats",
});

console.error(`[delegate] Sent task to ${to} | conv: ${conversationId} | timeout: ${timeoutSec}s`);
console.error(`[delegate] Task: ${taskText.substring(0, 100)}...`);

// Poll for response
const pollIntervalMs = 3000;
const deadline = Date.now() + timeoutSec * 1000;
let found = false;

while (Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, pollIntervalMs));

  // Check for inbound messages in this conversation
  const messages = await store.searchMessages(conversationId, 50);
  const responses = messages.filter(
    (m) => m.direction === "inbound" && new Date(m.createdAt) > new Date(sentAt)
  );

  if (responses.length > 0) {
    // Return the response to stdout (for Claude Code to capture)
    const response = responses[responses.length - 1];
    console.log(response.text);
    found = true;
    break;
  }

  const remaining = Math.round((deadline - Date.now()) / 1000);
  if (remaining % 15 === 0 || remaining <= 10) {
    console.error(`[delegate] Waiting for response... ${remaining}s remaining`);
  }
}

if (!found) {
  console.error(`[delegate] TIMEOUT after ${timeoutSec}s — no response from ${to}`);
  console.error(`[delegate] Check if ${to} is online. Conversation: ${conversationId}`);
  // Output a structured timeout response
  console.log(JSON.stringify({ error: "timeout", agent: to, conversationId, timeoutSec }));
  process.exit(2);
}
