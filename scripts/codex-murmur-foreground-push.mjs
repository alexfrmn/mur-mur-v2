#!/usr/bin/env node
import { appendFileSync, existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import path, { dirname } from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  CodexAppServerClient,
  buildTurnStartRequest,
} from "./codex-app-server-wake.mjs";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, "..");
const requireFromMurmur = createRequire(path.join(repoRoot, "package.json"));
const Database = requireFromMurmur("better-sqlite3");

const DEFAULT_MURMUR_ROOT = process.env.MURMUR_ROOT || repoRoot;
const DEFAULT_DATA_DIR = process.env.DATA_DIR || path.join(DEFAULT_MURMUR_ROOT, ".data");
const DEFAULT_STATE_DIR = process.env.XDG_STATE_HOME || path.join(homedir(), ".local", "state");
const DEFAULT_CODEX_HOME = process.env.CODEX_HOME || path.join(homedir(), ".codex");
const DEFAULT_DB = process.env.MURMUR_STORE_PATH || path.join(DEFAULT_DATA_DIR, "murmur.db");
const DEFAULT_STATE = path.join(DEFAULT_STATE_DIR, "codex", "murmur-foreground-push.state");
const DEFAULT_THREAD_STATE = path.join(DEFAULT_STATE_DIR, "codex", "murmur-foreground-thread.json");
const DEFAULT_LOG = process.env.MURMUR_FOREGROUND_PUSH_LOG || path.join(DEFAULT_STATE_DIR, "codex", "murmur-foreground-push.log");
const DEFAULT_SOCKET = path.join(DEFAULT_CODEX_HOME, "app-server-control", "app-server-control.sock");

let running = true;

const args = new Map();
for (let i = 2; i < process.argv.length; i += 1) {
  const arg = process.argv[i];
  if (arg.startsWith("--")) {
    const key = arg.slice(2);
    const next = process.argv[i + 1];
    if (!next || next.startsWith("--")) args.set(key, "true");
    else {
      args.set(key, next);
      i += 1;
    }
  }
}

const opt = (name, fallback) => args.get(name) ?? fallback;
const num = (name, fallback) => Number(opt(name, String(fallback)));
const flag = (name) => args.get(name) === "true";

const dbPath = opt("db", DEFAULT_DB);
const statePath = opt("state", DEFAULT_STATE);
const threadStatePath = opt("thread-state", DEFAULT_THREAD_STATE);
const logPath = opt("log", DEFAULT_LOG);
const socketPath = opt("socket", DEFAULT_SOCKET);
const murmurRoot = opt("murmur-root", DEFAULT_MURMUR_ROOT);
const leaseDbPath = opt("lease-db", path.join(dirname(dbPath), "lease.db"));
const leaseModuleUrl = opt("lease-module-url", pathToFileURL(path.join(murmurRoot, "scripts", "lease.mjs")).href);
const sender = opt("sender", "agent-jarvis");
const agentId = opt("agent-id", "agent-codex-volt");
const memberSlot = opt("member-slot", agentId);
const intervalMs = Math.max(250, num("interval", 2) * 1000);
const limit = Math.max(1, num("limit", 10));
const timeoutMs = Math.max(1000, num("timeout-ms", 10000));
const leaseTtlMs = Math.max(1000, num("lease-ttl-ms", 20000));
const { SessionLeaseStore } = await import(leaseModuleUrl);

const ensureParent = (path) => mkdirSync(dirname(path), { recursive: true });

const log = (event) => {
  ensureParent(logPath);
  appendFileSync(logPath, `${JSON.stringify({ ts: new Date().toISOString(), ...event })}\n`);
};

const readState = (db) => {
  if (existsSync(statePath)) {
    const value = readFileSync(statePath, "utf8").trim();
    if (value) return Number(value);
  }
  const row = db.prepare("SELECT COALESCE(MAX(rowid), 0) AS rowid FROM local_messages WHERE direction='inbound'").get();
  return Number(row?.rowid || 0);
};

const writeState = (rowid) => {
  ensureParent(statePath);
  const tmp = `${statePath}.tmp`;
  writeFileSync(tmp, `${Number(rowid)}\n`);
  renameSync(tmp, statePath);
};

const readThreadState = () => {
  const parsed = JSON.parse(readFileSync(threadStatePath, "utf8"));
  const threadId = typeof parsed.threadId === "string" ? parsed.threadId.trim() : "";
  const sessionId = typeof parsed.sessionId === "string" ? parsed.sessionId.trim() : threadId;
  return { threadId: threadId || undefined, sessionId: sessionId || undefined };
};

const tryReadThreadState = () => {
  try {
    return readThreadState();
  } catch {
    return {};
  }
};

const ensureProcessedTable = (db) => {
  db.exec(`
    CREATE TABLE IF NOT EXISTS codex_one_shot_processed (
      inbound_msg_id TEXT PRIMARY KEY,
      inbound_rowid INTEGER NOT NULL,
      conversation_id TEXT NOT NULL,
      reply_msg_id TEXT,
      status TEXT NOT NULL DEFAULT 'sent',
      processed_at TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT ''
    )
  `);
};

const alreadyProcessed = (db, msgId) =>
  db.prepare("SELECT status FROM codex_one_shot_processed WHERE inbound_msg_id = ?").get(msgId)?.status;

const markProcessed = (db, row, status) => {
  const now = new Date().toISOString();
  db.prepare(`
    INSERT INTO codex_one_shot_processed
      (inbound_msg_id, inbound_rowid, conversation_id, reply_msg_id, status, processed_at, updated_at)
    VALUES (?, ?, ?, NULL, ?, ?, ?)
    ON CONFLICT(inbound_msg_id) DO UPDATE SET
      status=excluded.status,
      updated_at=excluded.updated_at
  `).run(row.msg_id, row.rowid, row.conversation_id, status, now, now);
};

const fetchRows = (db, afterRowid) =>
  db.prepare(`
    SELECT rowid, conversation_id, msg_id, sender, text, created_at
    FROM local_messages
    WHERE direction='inbound'
      AND sender = ?
      AND rowid > ?
    ORDER BY rowid ASC
    LIMIT ?
  `).all(sender, afterRowid, limit);

const buildPushText = (row) => `[MURMUR PUSH]
from=${row.sender}
conversationId=${row.conversation_id}
msgId=${row.msg_id}
localRowid=${row.rowid}
createdAt=${row.created_at}

${row.text}

Instruction: handle this inbound Murmur message in the current foreground Codex chat. If it needs a reply to the sender, send it through Murmur in the same conversationId.`;

const registerForegroundSession = (lease, state) => {
  if (!state.sessionId) return;
  lease.registerSession({
    sessionId: state.sessionId,
    agentId,
    threadId: state.threadId,
    pid: process.pid,
    mode: "foreground",
  });
};

const heartbeatForegroundSession = (lease, state) => {
  if (!state.sessionId) return false;
  if (lease.sessionHeartbeat(state.sessionId) > 0) return true;
  registerForegroundSession(lease, state);
  return true;
};

const pushRow = async (row, lease, ownerToken, state) => {
  if (!lease.isCurrentToken(row.conversation_id, memberSlot, ownerToken)) {
    return { suppressed: true, reason: "stale-token-before-send" };
  }
  const client = new CodexAppServerClient({ socketPath, timeoutMs });
  return client.send(
    buildTurnStartRequest({
      id: 1,
      threadId: state.threadId,
      text: buildPushText(row),
      metadata: {
        murmur_msg_id: row.msg_id,
        murmur_conversation_id: row.conversation_id,
        murmur_from: row.sender,
        murmur_foreground_push: "true",
        murmur_owner_session: state.sessionId,
        murmur_owner_token: String(ownerToken),
      },
    }),
    1,
  );
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

process.on("SIGINT", () => {
  running = false;
});
process.on("SIGTERM", () => {
  running = false;
});

const db = new Database(dbPath);
db.pragma("busy_timeout = 10000");
ensureProcessedTable(db);
const lease = new SessionLeaseStore(leaseDbPath);

let lastRowid = flag("catch-up") ? 0 : readState(db);
if (args.has("set-last-rowid")) {
  lastRowid = Number(args.get("set-last-rowid"));
  writeState(lastRowid);
  log({ event: "set_last_rowid", rowid: lastRowid });
  process.exit(0);
}
writeState(lastRowid);
log({ event: "foreground_push_started", dbPath, leaseDbPath, statePath, threadStatePath, socketPath, sender, agentId, memberSlot, lastRowid });

while (running) {
  try {
    const foregroundState = tryReadThreadState();
    heartbeatForegroundSession(lease, foregroundState);
    const rows = fetchRows(db, lastRowid);
    for (const row of rows) {
      lastRowid = Number(row.rowid);
      writeState(lastRowid);
      const status = alreadyProcessed(db, row.msg_id);
      if (status) {
        log({ event: "skip_already_processed", rowid: row.rowid, msgId: row.msg_id, status });
        continue;
      }
      if (flag("dry-run")) {
        log({ event: "dry_run", rowid: row.rowid, msgId: row.msg_id, conversationId: row.conversation_id });
        continue;
      }
      const threadState = foregroundState.sessionId ? foregroundState : readThreadState();
      if (!threadState.sessionId) {
        log({ event: "skip_no_foreground_session", rowid: row.rowid, msgId: row.msg_id, conversationId: row.conversation_id });
        continue;
      }
      registerForegroundSession(lease, threadState);
      const claim = lease.claimOrSkip(row.conversation_id, memberSlot, threadState.sessionId, leaseTtlMs, Date.now(), "native:");
      if (!claim.won) {
        log({
          event: "skip_non_owner",
          rowid: row.rowid,
          msgId: row.msg_id,
          conversationId: row.conversation_id,
          ownerSessionId: claim.ownerSessionId,
          ownerToken: claim.token,
          sessionId: threadState.sessionId,
        });
        continue;
      }
      const result = await pushRow(row, lease, claim.token, threadState);
      if (result?.suppressed) {
        log({ event: "foreground_push_suppressed", rowid: row.rowid, msgId: row.msg_id, conversationId: row.conversation_id, reason: result.reason, ownerToken: claim.token, sessionId: threadState.sessionId });
        continue;
      }
      if (!lease.isCurrentToken(row.conversation_id, memberSlot, claim.token)) {
        log({ event: "skip_mark_processed_stale_token", rowid: row.rowid, msgId: row.msg_id, conversationId: row.conversation_id, ownerToken: claim.token, sessionId: threadState.sessionId });
        continue;
      }
      markProcessed(db, row, "foreground_pushed");
      log({ event: "foreground_pushed", rowid: row.rowid, msgId: row.msg_id, conversationId: row.conversation_id, turnId: result?.turn?.id, ownerToken: claim.token, ownerSessionId: threadState.sessionId });
    }
  } catch (err) {
    log({ event: "foreground_push_error", error: err instanceof Error ? err.message : String(err) });
  }
  if (flag("once")) break;
  await sleep(intervalMs);
}

lease.close();
log({ event: "foreground_push_stopped", lastRowid });
