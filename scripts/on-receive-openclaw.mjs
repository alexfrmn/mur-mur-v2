#!/usr/bin/env node
/**
 * Mur-Mur -> OpenClaw bridge helper.
 * Injects inbound Murmur messages into the MAIN OpenClaw session
 * via Gateway /tools/invoke API (cron wake).
 */
import http from "node:http";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

const text = process.env.MURMUR_TEXT || "";
const from = process.env.MURMUR_FROM || "unknown";
const msgId = process.env.MURMUR_MSG_ID || "unknown";

// Load gateway token from env or agent-config.json fallback
let gatewayToken = process.env.MURMUR_OPENCLAW_GATEWAY_TOKEN || "";
if (!gatewayToken) {
  try {
    const cfg = JSON.parse(readFileSync(join(__dirname, "..", ".data", "agent-config.json"), "utf8"));
    gatewayToken = cfg.gatewayToken || "";
  } catch {}
}
const gatewayUrl = process.env.MURMUR_OPENCLAW_GATEWAY_URL || "http://127.0.0.1:18789";

if (!text) {
  console.log("[openclaw-helper] No text, skipping");
  process.exit(0);
}

const wakeText = `[MURMUR_INBOUND] from: ${from} | msgId: ${msgId}\n\n${text}`;

const body = JSON.stringify({
  tool: "cron",
  args: {
    action: "wake",
    text: wakeText,
    mode: "now",
  },
});

const url = new URL("/tools/invoke", gatewayUrl);

const req = http.request(url, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    ...(gatewayToken ? { "Authorization": `Bearer ${gatewayToken}` } : {}),
  },
}, (res) => {
  let data = "";
  res.on("data", (chunk) => data += chunk);
  res.on("end", () => {
    if (res.statusCode < 300) {
      console.log(`[openclaw-helper] OK: ${data.slice(0, 100)}`);
    } else {
      console.error(`[openclaw-helper] HTTP ${res.statusCode}: ${data.slice(0, 200)}`);
      process.exit(1);
    }
  });
});

req.on("error", (err) => {
  console.error(`[openclaw-helper] Request failed: ${err.message}`);
  process.exit(1);
});

req.setTimeout(15000, () => {
  req.destroy();
  console.error("[openclaw-helper] Timeout 15s");
  process.exit(1);
});

req.write(body);
req.end();
