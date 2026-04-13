#!/usr/bin/env node
/**
 * on-receive-telegram.mjs — Murmur daemon onReceive hook
 *
 * Called by daemon when an encrypted message is received and decrypted.
 * Sends a Telegram notification via Bot API.
 *
 * Env (set by daemon):
 *   MURMUR_FROM, MURMUR_TEXT, MURMUR_MSG_ID, MURMUR_CONVERSATION_ID
 *
 * Env (user-configured):
 *   MURMUR_TELEGRAM_BOT_TOKEN  — Bot API token (required)
 *   MURMUR_TELEGRAM_CHAT_ID    — Target chat (required)
 *   MURMUR_TELEGRAM_TOPIC_ID   — Forum thread_id (optional)
 */

const { MURMUR_FROM, MURMUR_TEXT, MURMUR_TELEGRAM_BOT_TOKEN, MURMUR_TELEGRAM_CHAT_ID, MURMUR_TELEGRAM_TOPIC_ID } = process.env;

if (!MURMUR_TELEGRAM_BOT_TOKEN || !MURMUR_TELEGRAM_CHAT_ID) {
  console.error("Missing MURMUR_TELEGRAM_BOT_TOKEN or MURMUR_TELEGRAM_CHAT_ID");
  process.exit(1);
}

if (!MURMUR_TEXT) {
  console.error("No MURMUR_TEXT — nothing to send");
  process.exit(0);
}

const text = `📨 [${MURMUR_FROM || "unknown"}]:\n${MURMUR_TEXT}`;

const body = {
  chat_id: MURMUR_TELEGRAM_CHAT_ID,
  text,
  parse_mode: "HTML",
  disable_web_page_preview: true,
};

if (MURMUR_TELEGRAM_TOPIC_ID) {
  body.message_thread_id = Number(MURMUR_TELEGRAM_TOPIC_ID);
}

const url = `https://api.telegram.org/bot${MURMUR_TELEGRAM_BOT_TOKEN}/sendMessage`;

try {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const err = await res.text();
    console.error(`Telegram API error ${res.status}: ${err}`);
    process.exit(1);
  }

  console.log("Telegram notification sent");
} catch (err) {
  console.error(`Fetch error: ${err.message}`);
  process.exit(1);
}
