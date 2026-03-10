#!/usr/bin/env node
import { DatabaseSync } from "node:sqlite";

const nowIso = () => new Date().toISOString();

const ensureArray = (value) => (Array.isArray(value) ? value : value ? [value] : []);

const normalizeTelegram = (value, channelName = "telegram") => {
  const list = ensureArray(value).filter(Boolean);
  return list
    .map((entry, i) => ({
      type: "telegram",
      channel: entry?.channel || `${channelName}${list.length > 1 ? `-${i + 1}` : ""}`,
      botToken: entry?.botToken,
      chatId: entry?.chatId,
      topicId: entry?.topicId,
    }))
    .filter((entry) => entry.botToken && entry.chatId);
};

const normalizeWebhook = (value, channelName = "webhook") => {
  const list = ensureArray(value).filter(Boolean);
  return list
    .map((entry, i) => ({
      type: "webhook",
      channel: entry?.channel || entry?.name || `${channelName}${list.length > 1 ? `-${i + 1}` : ""}`,
      url: entry?.url,
      headers: entry?.headers && typeof entry.headers === "object" ? entry.headers : {},
    }))
    .filter((entry) => entry.url);
};

export const normalizeNotifyTargets = (notifyConfig) => {
  if (!notifyConfig) return [];

  if (Array.isArray(notifyConfig)) {
    return notifyConfig.flatMap((entry) => {
      if (!entry || typeof entry !== "object") return [];
      if (entry.type === "telegram") return normalizeTelegram(entry, entry.channel || "telegram");
      if (entry.type === "webhook") return normalizeWebhook(entry, entry.channel || "webhook");
      return [];
    });
  }

  const targets = [];

  if (notifyConfig.telegram) targets.push(...normalizeTelegram(notifyConfig.telegram, "telegram"));
  if (notifyConfig.webhook) targets.push(...normalizeWebhook(notifyConfig.webhook, "webhook"));

  // Backward compatibility: notify: { botToken, chatId, topicId }
  if (!notifyConfig.telegram && notifyConfig.botToken && notifyConfig.chatId) {
    targets.push(...normalizeTelegram({ botToken: notifyConfig.botToken, chatId: notifyConfig.chatId, topicId: notifyConfig.topicId }, "telegram"));
  }

  // Backward compatibility: notify: { url, headers }
  if (!notifyConfig.webhook && notifyConfig.url) {
    targets.push(...normalizeWebhook({ url: notifyConfig.url, headers: notifyConfig.headers }, "webhook"));
  }

  return targets;
};

export class NotifyQueue {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(`
      PRAGMA journal_mode=WAL;
      CREATE TABLE IF NOT EXISTS notify_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dedupe_key TEXT NOT NULL UNIQUE,
        msg_id TEXT NOT NULL,
        channel_type TEXT NOT NULL,
        channel_name TEXT NOT NULL,
        target_json TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        next_attempt_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        sent_at TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_notify_queue_due ON notify_queue(status, next_attempt_at);
    `);
  }

  enqueueMessage(payload, targets) {
    const now = nowIso();
    for (const target of targets) {
      const dedupeKey = `${payload.msgId}:${target.type}:${target.channel}`;
      this.db.prepare(`
        INSERT OR IGNORE INTO notify_queue
        (dedupe_key, msg_id, channel_type, channel_name, target_json, payload_json, status, attempts, next_attempt_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
      `).run(
        dedupeKey,
        payload.msgId,
        target.type,
        target.channel,
        JSON.stringify(target),
        JSON.stringify(payload),
        now,
        now,
        now,
      );
    }
  }

  claimDue(limit = 50) {
    return this.db.prepare(`
      SELECT * FROM notify_queue
      WHERE status IN ('pending', 'failed') AND next_attempt_at <= ?
      ORDER BY next_attempt_at ASC
      LIMIT ?
    `).all(nowIso(), limit);
  }

  markSent(id) {
    this.db.prepare(`
      UPDATE notify_queue
      SET status = 'sent', attempts = attempts + 1, updated_at = ?, sent_at = ?, last_error = NULL
      WHERE id = ?
    `).run(nowIso(), nowIso(), id);
  }

  markFailed(id, reason, backoffMs = 5000) {
    const nextAttemptAt = new Date(Date.now() + backoffMs).toISOString();
    this.db.prepare(`
      UPDATE notify_queue
      SET status = 'failed', attempts = attempts + 1, updated_at = ?, next_attempt_at = ?, last_error = ?
      WHERE id = ?
    `).run(nowIso(), nextAttemptAt, reason, id);
  }

  markDead(id, reason) {
    this.db.prepare(`
      UPDATE notify_queue
      SET status = 'dead', attempts = attempts + 1, updated_at = ?, last_error = ?
      WHERE id = ?
    `).run(nowIso(), reason, id);
  }

  pendingCount() {
    const row = this.db.prepare(`
      SELECT COUNT(*) as count FROM notify_queue WHERE status IN ('pending', 'failed')
    `).get();
    return Number(row?.count ?? 0);
  }
}

const formatNotifyText = (payload) => `📨 [${payload.from}]\n${payload.text}`;

const TG_MAX_LENGTH = 4000; // Telegram limit is 4096, leave margin

const sendTelegram = async (target, payload) => {
  let text = formatNotifyText(payload);

  // Truncate if too long for Telegram
  if (text.length > TG_MAX_LENGTH) {
    text = text.slice(0, TG_MAX_LENGTH - 40) + "\n\n... [truncated, full in DB]";
  }

  const body = {
    chat_id: target.chatId,
    text,
    disable_web_page_preview: true,
  };
  if (target.topicId) body.message_thread_id = Number(target.topicId);

  const res = await fetch(`https://api.telegram.org/bot${target.botToken}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const errText = await res.text().catch(() => "");
    const err = new Error(`telegram-http-${res.status}:${errText.slice(0, 280)}`);
    err.httpStatus = res.status;
    throw err;
  }
};

const sendWebhook = async (target, payload) => {
  const res = await fetch(target.url, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...(target.headers || {}) },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`webhook-http-${res.status}:${text.slice(0, 280)}`);
  }
};

export const dispatchNotification = async (target, payload) => {
  if (target.type === "telegram") return sendTelegram(target, payload);
  if (target.type === "webhook") return sendWebhook(target, payload);
  throw new Error(`unsupported-notify-target:${target.type}`);
};

export const flushNotifyQueue = async ({ queue, log, limit = 50 }) => {
  const due = queue.claimDue(limit);
  for (const row of due) {
    const target = JSON.parse(String(row.target_json));
    const payload = JSON.parse(String(row.payload_json));

    log("info", "Notify attempt", {
      queueId: row.id,
      msgId: row.msg_id,
      target: `${row.channel_type}:${row.channel_name}`,
      attempt: Number(row.attempts) + 1,
    });

    try {
      await dispatchNotification(target, payload);
      queue.markSent(row.id);
      log("info", "Notify success", {
        queueId: row.id,
        msgId: row.msg_id,
        target: `${row.channel_type}:${row.channel_name}`,
      });
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      const httpStatus = err instanceof Error ? err.httpStatus : undefined;
      const attempts = Number(row.attempts) + 1;

      // Dead-letter on permanent failures (4xx) or too many retries
      if ((httpStatus && httpStatus >= 400 && httpStatus < 500) || attempts >= 10) {
        queue.markDead(row.id, `permanent: ${reason}`);
        log("error", "Notify dead-lettered", {
          queueId: row.id,
          msgId: row.msg_id,
          target: `${row.channel_type}:${row.channel_name}`,
          reason,
          attempts,
        });
      } else {
        queue.markFailed(row.id, reason, Math.min(60_000, 2_000 * attempts));
        log("warn", "Notify failed", {
          queueId: row.id,
          msgId: row.msg_id,
          target: `${row.channel_type}:${row.channel_name}`,
          reason,
        });
      }
    }
  }

  return due.length;
};
