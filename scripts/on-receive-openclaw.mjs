#!/usr/bin/env node
/**
 * Default local helper for Mur-Mur -> OpenClaw bridge.
 * Reads MURMUR_* env vars and sends the inbound payload into an OpenClaw session.
 */
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const text = process.env.MURMUR_TEXT || "";
const from = process.env.MURMUR_FROM || "unknown";
const msgId = process.env.MURMUR_MSG_ID || "unknown";
const conversationId = process.env.MURMUR_CONVERSATION_ID || "unknown";

const routeChannel = process.env.MURMUR_OPENCLAW_CHANNEL || "telegram";
const sessionId = process.env.MURMUR_OPENCLAW_SESSION_ID || "";
const to = process.env.MURMUR_OPENCLAW_TO || "";
const agent = process.env.MURMUR_OPENCLAW_AGENT || "";

const message = [
  "[MURMUR_INBOUND]",
  `from: ${from}`,
  `conversationId: ${conversationId}`,
  `msgId: ${msgId}`,
  "",
  text,
].join("\n");

if (!sessionId && !to) {
  console.error("[openclaw-helper] Missing target: set MURMUR_OPENCLAW_SESSION_ID or MURMUR_OPENCLAW_TO");
  process.exit(2);
}

const args = ["agent", "--channel", routeChannel, "--message", message, "--json"];
if (agent) args.push("--agent", agent);
if (sessionId) args.push("--session-id", sessionId);
else args.push("--to", to);

try {
  const { stdout } = await execFileAsync("openclaw", args, { timeout: 30_000, env: process.env });
  process.stdout.write((stdout || "").trim() + "\n");
} catch (err) {
  console.error("[openclaw-helper] dispatch failed:", err instanceof Error ? err.message : String(err));
  process.exit(1);
}
