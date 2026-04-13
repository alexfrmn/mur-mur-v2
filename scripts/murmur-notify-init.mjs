#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const dataDir = process.env.DATA_DIR || ".data";
const configPath = path.join(dataDir, "agent-config.json");
const preset = (process.argv[2] || "telegram").toLowerCase();

const requireConfig = async () => {
  try {
    return JSON.parse(await readFile(configPath, "utf8"));
  } catch (err) {
    console.error(`[notify-init] Failed to read ${configPath}: ${err.message}`);
    process.exit(1);
  }
};

const pushWebhook = (cfg, entry) => {
  if (!cfg.notify) cfg.notify = {};
  const existing = cfg.notify.webhook;
  if (!existing) cfg.notify.webhook = [entry];
  else if (Array.isArray(existing)) cfg.notify.webhook.push(entry);
  else cfg.notify.webhook = [existing, entry];
};

const run = async () => {
  const cfg = await requireConfig();
  if (!cfg.notify) cfg.notify = {};

  if (preset === "telegram") {
    const botToken = process.env.MURMUR_TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN;
    const chatId = process.env.MURMUR_TELEGRAM_CHAT_ID || process.env.TELEGRAM_CHAT_ID;
    const topicId = process.env.MURMUR_TELEGRAM_TOPIC_ID || process.env.TELEGRAM_TOPIC_ID;
    if (!botToken || !chatId) {
      console.error("[notify-init] telegram requires MURMUR_TELEGRAM_BOT_TOKEN and MURMUR_TELEGRAM_CHAT_ID (or TELEGRAM_* aliases)");
      process.exit(1);
    }
    cfg.notify.telegram = { botToken, chatId, ...(topicId ? { topicId } : {}) };
    console.log("[notify-init] Configured telegram notifier");
  } else if (preset === "discord") {
    const url = process.env.MURMUR_DISCORD_WEBHOOK_URL || process.env.DISCORD_WEBHOOK_URL;
    if (!url) {
      console.error("[notify-init] discord requires MURMUR_DISCORD_WEBHOOK_URL (or DISCORD_WEBHOOK_URL)");
      process.exit(1);
    }
    pushWebhook(cfg, { channel: "discord", url });
    console.log("[notify-init] Added discord webhook notifier");
  } else if (preset === "whatsapp") {
    const url = process.env.MURMUR_WHATSAPP_WEBHOOK_URL || process.env.WHATSAPP_WEBHOOK_URL || "https://example.invalid/whatsapp-bridge";
    pushWebhook(cfg, {
      channel: "whatsapp",
      url,
      headers: { "x-murmur-provider": "whatsapp-bridge" },
    });
    console.log("[notify-init] Added whatsapp webhook-bridge placeholder notifier");
    if (url.includes("example.invalid")) {
      console.log("[notify-init] TODO: set MURMUR_WHATSAPP_WEBHOOK_URL to your bridge endpoint");
    }
  } else if (preset === "openclaw") {
    const sessionId = process.env.MURMUR_OPENCLAW_SESSION_ID || "";
    const to = process.env.MURMUR_OPENCLAW_TO || "";
    const routeChannel = process.env.MURMUR_OPENCLAW_CHANNEL || "telegram";
    const helperScript = process.env.MURMUR_OPENCLAW_HELPER_SCRIPT || "scripts/on-receive-openclaw.mjs";
    const command = process.env.MURMUR_OPENCLAW_COMMAND || "";
    if (!sessionId && !to && !command) {
      console.error("[notify-init] openclaw requires MURMUR_OPENCLAW_SESSION_ID or MURMUR_OPENCLAW_TO (or MURMUR_OPENCLAW_COMMAND)");
      process.exit(1);
    }
    cfg.notify.openclaw = {
      enabled: true,
      channel: "openclaw-main",
      routeChannel,
      ...(sessionId ? { sessionId } : {}),
      ...(to ? { to } : {}),
      ...(command ? { command } : { helperScript }),
    };
    console.log("[notify-init] Configured openclaw bridge");
  } else {
    console.error(`[notify-init] Unknown preset '${preset}'. Use: telegram | discord | whatsapp | openclaw`);
    process.exit(1);
  }

  await writeFile(configPath, JSON.stringify(cfg, null, 2) + "\n", "utf8");
  console.log(`[notify-init] Updated ${configPath}`);
};

run().catch((err) => {
  console.error("[notify-init] Failed:", err.message);
  process.exit(1);
});
