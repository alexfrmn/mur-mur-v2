#!/usr/bin/env node
/**
 * Mur-Mur -> OpenClaw bridge helper (v2).
 *
 * Dispatches inbound Murmur messages to the local OpenClaw agent via CLI.
 * Used as helperScript in agent-config.json notify.openclaw.
 *
 * Environment variables (set by dispatchViaHelper in openclaw-bridge.mjs):
 *   MURMUR_FROM, MURMUR_TEXT, MURMUR_MSG_ID, MURMUR_CONVERSATION_ID,
 *   MURMUR_OPENCLAW_CHANNEL, MURMUR_OPENCLAW_AGENT,
 *   MURMUR_OPENCLAW_SESSION_ID, MURMUR_OPENCLAW_TO,
 *   MURMUR_OPENCLAW_GATEWAY_URL, MURMUR_OPENCLAW_GATEWAY_TOKEN
 *
 * Outputs OpenClaw JSON response to stdout (parsed by extractResponseText).
 */
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const text = process.env.MURMUR_TEXT || "";
const from = process.env.MURMUR_FROM || "unknown";
const msgId = process.env.MURMUR_MSG_ID || "unknown";
const conversationId = process.env.MURMUR_CONVERSATION_ID || "";
const channel = process.env.MURMUR_OPENCLAW_CHANNEL || "telegram";
const agent = process.env.MURMUR_OPENCLAW_AGENT || "";
const sessionId = process.env.MURMUR_OPENCLAW_SESSION_ID || "";
const to = process.env.MURMUR_OPENCLAW_TO || "";
const gatewayUrl = process.env.MURMUR_OPENCLAW_GATEWAY_URL || "";
const gatewayToken = process.env.MURMUR_OPENCLAW_GATEWAY_TOKEN || "";

if (!text) {
  console.log("[openclaw-helper] No text, skipping");
  process.exit(0);
}

const message = [
  "[MURMUR_INBOUND]",
  `from: ${from}`,
  `conversationId: ${conversationId}`,
  `msgId: ${msgId}`,
  "",
  text,
].join("\n");

const args = ["agent", "--channel", channel, "--message", message, "--json"];
if (agent) args.push("--agent", agent);
if (sessionId) args.push("--session-id", sessionId);
else if (to) args.push("--to", to);

const env = {
  ...process.env,
  ...(gatewayUrl ? { OPENCLAW_GATEWAY_URL: gatewayUrl } : {}),
  ...(gatewayToken ? { OPENCLAW_GATEWAY_TOKEN: gatewayToken } : {}),
};

try {
  const { stdout, stderr } = await execFileAsync("openclaw", args, {
    env,
    timeout: 120_000,
    maxBuffer: 10 * 1024 * 1024,
  });
  if (stderr) console.error(`[openclaw-helper] stderr: ${stderr.slice(0, 200)}`);
  process.stdout.write(stdout);
} catch (err) {
  console.error(`[openclaw-helper] FAILED: ${err.message?.slice(0, 300)}`);
  if (err.stderr) console.error(`[openclaw-helper] stderr: ${err.stderr.slice(0, 500)}`);
  process.exit(1);
}
