#!/usr/bin/env node
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { DatabaseSync } from "node:sqlite";

const execFileAsync = promisify(execFile);
const nowIso = () => new Date().toISOString();
const ensureArray = (value) => (Array.isArray(value) ? value : value ? [value] : []);

export const normalizeOpenClawTargets = (notifyConfig) => {
  if (!notifyConfig) return [];
  const source = notifyConfig.openclaw || notifyConfig;
  const list = ensureArray(source).filter(Boolean);

  return list
    .map((entry, i) => ({
      type: "openclaw",
      channel: entry?.channel || `openclaw${list.length > 1 ? `-${i + 1}` : ""}`,
      enabled: entry?.enabled !== false,
      agent: entry?.agent,
      sessionId: entry?.sessionId,
      sessionLabel: entry?.sessionLabel,
      sessionKey: entry?.sessionKey,
      to: entry?.to,
      routeChannel: entry?.routeChannel || entry?.channelName || "telegram",
      command: entry?.command,
      helperScript: entry?.helperScript,
      gatewayUrl: entry?.gatewayUrl,
      gatewayToken: entry?.gatewayToken,
      extraEnv: entry?.extraEnv && typeof entry.extraEnv === "object" ? entry.extraEnv : {},
    }))
    .filter((entry) => entry.enabled);
};

export class OpenClawBridgeQueue {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(`
      PRAGMA journal_mode=WAL;
      CREATE TABLE IF NOT EXISTS openclaw_bridge_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dedupe_key TEXT NOT NULL UNIQUE,
        msg_id TEXT NOT NULL,
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
      CREATE INDEX IF NOT EXISTS idx_openclaw_bridge_due
      ON openclaw_bridge_queue(status, next_attempt_at);
    `);
  }

  enqueueMessage(payload, targets) {
    const now = nowIso();
    for (const target of targets) {
      const dedupeKey = `${payload.msgId}:openclaw:${target.channel}`;
      this.db.prepare(`
        INSERT OR IGNORE INTO openclaw_bridge_queue
        (dedupe_key, msg_id, channel_name, target_json, payload_json, status, attempts, next_attempt_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
      `).run(
        dedupeKey,
        payload.msgId,
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
      SELECT * FROM openclaw_bridge_queue
      WHERE status IN ('pending', 'failed') AND next_attempt_at <= ?
      ORDER BY next_attempt_at ASC
      LIMIT ?
    `).all(nowIso(), limit);
  }

  markSent(id) {
    this.db.prepare(`
      UPDATE openclaw_bridge_queue
      SET status = 'sent', attempts = attempts + 1, updated_at = ?, sent_at = ?, last_error = NULL
      WHERE id = ?
    `).run(nowIso(), nowIso(), id);
  }

  markFailed(id, reason, backoffMs = 5000) {
    const nextAttemptAt = new Date(Date.now() + backoffMs).toISOString();
    this.db.prepare(`
      UPDATE openclaw_bridge_queue
      SET status = 'failed', attempts = attempts + 1, updated_at = ?, next_attempt_at = ?, last_error = ?
      WHERE id = ?
    `).run(nowIso(), nextAttemptAt, reason, id);
  }

  pendingCount() {
    const row = this.db.prepare(`
      SELECT COUNT(*) as count FROM openclaw_bridge_queue WHERE status IN ('pending', 'failed')
    `).get();
    return Number(row?.count ?? 0);
  }
}

const buildBridgeMessage = (payload) => {
  return [
    "[MURMUR_INBOUND]",
    `from: ${payload.from}`,
    `conversationId: ${payload.conversationId}`,
    `msgId: ${payload.msgId}`,
    "",
    payload.text,
  ].join("\n");
};

const dispatchViaHelper = async (target, payload) => {
  const env = {
    ...process.env,
    MURMUR_FROM: payload.from,
    MURMUR_TEXT: payload.text,
    MURMUR_MSG_ID: payload.msgId,
    MURMUR_CONVERSATION_ID: payload.conversationId,
    MURMUR_OPENCLAW_CHANNEL: target.routeChannel,
    MURMUR_OPENCLAW_SESSION_ID: target.sessionId || "",
    MURMUR_OPENCLAW_SESSION_LABEL: target.sessionLabel || "",
    MURMUR_OPENCLAW_SESSION_KEY: target.sessionKey || "",
    MURMUR_OPENCLAW_TO: target.to || "",
    MURMUR_OPENCLAW_AGENT: target.agent || "",
    MURMUR_OPENCLAW_GATEWAY_URL: target.gatewayUrl || "",
    MURMUR_OPENCLAW_GATEWAY_TOKEN: target.gatewayToken || "",
    MURMUR_OPENCLAW_PAYLOAD_JSON: JSON.stringify(payload),
    ...(target.extraEnv || {}),
  };

  const { stdout, stderr } = await execFileAsync("node", [target.helperScript], { env, timeout: 20_000 });
  return { stdout: (stdout || "").trim(), stderr: (stderr || "").trim() };
};

const dispatchViaCommand = async (target, payload) => {
  const env = {
    ...process.env,
    MURMUR_FROM: payload.from,
    MURMUR_TEXT: payload.text,
    MURMUR_MSG_ID: payload.msgId,
    MURMUR_CONVERSATION_ID: payload.conversationId,
  };
  const { stdout, stderr } = await execFileAsync("sh", ["-c", target.command], { env, timeout: 20_000 });
  return { stdout: (stdout || "").trim(), stderr: (stderr || "").trim() };
};

const dispatchViaOpenClawCli = async (target, payload) => {
  const message = buildBridgeMessage(payload);
  const args = ["agent", "--channel", target.routeChannel || "telegram", "--message", message, "--json"];

  if (target.agent) args.push("--agent", target.agent);
  if (target.sessionId) args.push("--session-id", target.sessionId);
  else if (target.to) args.push("--to", target.to);

  const env = {
    ...process.env,
    ...(target.gatewayUrl ? { OPENCLAW_GATEWAY_URL: target.gatewayUrl } : {}),
    ...(target.gatewayToken ? { OPENCLAW_GATEWAY_TOKEN: target.gatewayToken } : {}),
  };

  const { stdout, stderr } = await execFileAsync("openclaw", args, { env, timeout: 30_000 });
  return { stdout: (stdout || "").trim(), stderr: (stderr || "").trim() };
};

export const dispatchOpenClawBridge = async (target, payload) => {
  if (target.helperScript) return dispatchViaHelper(target, payload);
  if (target.command) return dispatchViaCommand(target, payload);
  return dispatchViaOpenClawCli(target, payload);
};

export const flushOpenClawBridgeQueue = async ({ queue, log, limit = 20 }) => {
  const due = queue.claimDue(limit);
  for (const row of due) {
    const target = JSON.parse(String(row.target_json));
    const payload = JSON.parse(String(row.payload_json));

    log("info", "OpenClaw bridge dispatch attempt", {
      queueId: row.id,
      msgId: row.msg_id,
      target: row.channel_name,
      attempt: Number(row.attempts) + 1,
    });

    try {
      const out = await dispatchOpenClawBridge(target, payload);
      queue.markSent(row.id);
      log("info", "OpenClaw bridge dispatch success", {
        queueId: row.id,
        msgId: row.msg_id,
        target: row.channel_name,
        stdout: out.stdout?.slice(0, 240) || undefined,
      });
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      queue.markFailed(row.id, reason, Math.min(60_000, 2_000 * (Number(row.attempts) + 1)));
      log("warn", "OpenClaw bridge dispatch failed", {
        queueId: row.id,
        msgId: row.msg_id,
        target: row.channel_name,
        reason,
      });
    }
  }
  return due.length;
};
