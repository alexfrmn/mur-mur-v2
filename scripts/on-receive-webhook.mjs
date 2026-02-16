#!/usr/bin/env node
/**
 * on-receive-webhook.mjs — Murmur daemon onReceive hook
 *
 * POSTs JSON payload to any HTTP endpoint.
 *
 * Env (set by daemon):
 *   MURMUR_FROM, MURMUR_TEXT, MURMUR_MSG_ID, MURMUR_CONVERSATION_ID
 *
 * Env (user-configured):
 *   MURMUR_WEBHOOK_URL — Target URL (required)
 */

const { MURMUR_FROM, MURMUR_TEXT, MURMUR_MSG_ID, MURMUR_CONVERSATION_ID, MURMUR_WEBHOOK_URL } = process.env;

if (!MURMUR_WEBHOOK_URL) {
  console.error("Missing MURMUR_WEBHOOK_URL");
  process.exit(1);
}

const payload = {
  from: MURMUR_FROM || "unknown",
  text: MURMUR_TEXT || "",
  msgId: MURMUR_MSG_ID || "",
  conversationId: MURMUR_CONVERSATION_ID || "",
  ts: new Date().toISOString(),
};

try {
  const res = await fetch(MURMUR_WEBHOOK_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    console.error(`Webhook error ${res.status}: ${await res.text()}`);
    process.exit(1);
  }

  console.log("Webhook delivered");
} catch (err) {
  console.error(`Fetch error: ${err.message}`);
  process.exit(1);
}
