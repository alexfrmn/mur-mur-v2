#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, "..");
const dataDir = process.env.DATA_DIR || path.join(repoRoot, ".data");

const EXIT = {
  sent: 0,
  alreadyProcessed: 10,
  invalidEnvelope: 20,
  generationFailed: 30,
  sendFailed: 40,
  usage: 64,
  staleLease: 70,
};

const defaults = {
  db: process.env.MURMUR_STORE_PATH || path.join(dataDir, "murmur.db"),
  sender: "agent-jarvis",
  recipient: "agent-jarvis",
  agentId: "agent-codex-volt",
  murmurRoot: process.env.MURMUR_ROOT || repoRoot,
  project: process.env.CODEX_PROJECT || process.cwd(),
  sendMode: "print",
};

function usage() {
  console.error(`Usage:
  codex-murmur-one-shot-responder.mjs --rowid N --reply-text TEXT [options]
  codex-murmur-one-shot-responder.mjs --msg-id ID --reply-file PATH [options]

Options:
  --db PATH              SQLite murmur.db path
  --rowid N              inbound local_messages rowid to process
  --msg-id ID            inbound local_messages msg_id to process
  --sender AGENT         expected inbound sender, default agent-jarvis
  --recipient AGENT      Murmur recipient for --send-mode murmur
  --agent-id AGENT       outbound sender for --send-mode append-local
  --project PATH         project label for reply metadata
  --reply-text TEXT      reply payload body
  --reply-file PATH      reply payload body from file
  --send-mode MODE       print | append-local | murmur, default print
  --murmur-root PATH     root for scripts/murmur-shell-send.mjs
  --lease-db PATH        lease.db path for outbound fence
  --lease-member-slot ID member_slot for outbound fence
  --lease-token N        expected owner token for outbound fence

Exit codes:
  0 sent/planned
  10 already processed
  20 invalid envelope
  30 missing reply payload
  40 send failed
  70 stale lease token
  64 usage error`);
}

function parseArgs(argv) {
  const args = { ...defaults };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const value = argv[i + 1];
    if (!key.startsWith("--")) {
      usage();
      process.exit(EXIT.usage);
    }
    if (value === undefined || value.startsWith("--")) {
      usage();
      process.exit(EXIT.usage);
    }
    i += 1;
    switch (key) {
      case "--db": args.db = value; break;
      case "--rowid": args.rowid = Number(value); break;
      case "--msg-id": args.msgId = value; break;
      case "--sender": args.sender = value; break;
      case "--recipient": args.recipient = value; break;
      case "--agent-id": args.agentId = value; break;
      case "--project": args.project = value; break;
      case "--reply-text": args.replyText = value; break;
      case "--reply-file": args.replyText = readFileSync(value, "utf8"); break;
      case "--send-mode": args.sendMode = value; break;
      case "--murmur-root": args.murmurRoot = value; break;
      case "--lease-db": args.leaseDb = value; break;
      case "--lease-member-slot": args.leaseMemberSlot = value; break;
      case "--lease-token": args.leaseToken = Number(value); break;
      default:
        usage();
        process.exit(EXIT.usage);
    }
  }

  if (!args.rowid && !args.msgId) {
    usage();
    process.exit(EXIT.usage);
  }
  if (!["print", "append-local", "murmur"].includes(args.sendMode)) {
    usage();
    process.exit(EXIT.usage);
  }
  return args;
}

function requireNonEmpty(value, name) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`invalid ${name}`);
  }
}

function ensureTables(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS codex_one_shot_processed (
      inbound_msg_id TEXT PRIMARY KEY,
      inbound_rowid INTEGER NOT NULL,
      conversation_id TEXT NOT NULL,
      reply_msg_id TEXT,
      status TEXT NOT NULL DEFAULT 'sent',
      processed_at TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT ''
    );
  `);
  const columns = db.prepare("PRAGMA table_info(codex_one_shot_processed)").all();
  const names = new Set(columns.map((row) => String(row.name)));
  if (!names.has("status")) {
    db.exec("ALTER TABLE codex_one_shot_processed ADD COLUMN status TEXT NOT NULL DEFAULT 'sent';");
  }
  if (!names.has("updated_at")) {
    db.exec("ALTER TABLE codex_one_shot_processed ADD COLUMN updated_at TEXT NOT NULL DEFAULT '';");
  }
}

function getInbound(db, args) {
  if (args.rowid) {
    return db.prepare(`
      SELECT rowid, conversation_id, msg_id, direction, sender, text, created_at
      FROM local_messages
      WHERE rowid = ?
    `).get(args.rowid);
  }
  return db.prepare(`
    SELECT rowid, conversation_id, msg_id, direction, sender, text, created_at
    FROM local_messages
    WHERE msg_id = ?
    ORDER BY rowid DESC
    LIMIT 1
  `).get(args.msgId);
}

function buildReply({ conversationId, replyTo, project, body }) {
  return [
    "[CODEX->JARVIS]",
    "source=codex-cli-one-shot",
    `project=${project}`,
    `conversationId=${conversationId}`,
    `replyTo=${replyTo}`,
    "",
    body.trim(),
  ].join("\n");
}

function markProcessed(db, inbound, replyMsgId) {
  const now = new Date().toISOString();
  db.prepare(`
    INSERT INTO codex_one_shot_processed
      (inbound_msg_id, inbound_rowid, conversation_id, reply_msg_id, status, processed_at, updated_at)
    VALUES (?, ?, ?, ?, 'sent', ?, ?)
  `).run(inbound.msg_id, inbound.rowid, inbound.conversation_id, replyMsgId, now, now);
}

function sendAppendLocal(db, args, inbound, replyText) {
  const replyMsgId = randomUUID();
  const now = new Date().toISOString();
  db.exec("BEGIN IMMEDIATE;");
  try {
    db.prepare(`
      INSERT INTO local_messages
        (id, conversation_id, msg_id, direction, sender, text, created_at, transport)
      VALUES (?, ?, ?, 'outbound', ?, ?, ?, 'one-shot-test')
    `).run(replyMsgId, inbound.conversation_id, replyMsgId, args.agentId, replyText, now);
    db.prepare(`
      INSERT INTO codex_one_shot_processed
        (inbound_msg_id, inbound_rowid, conversation_id, reply_msg_id, status, processed_at, updated_at)
      VALUES (?, ?, ?, ?, 'sent', ?, ?)
    `).run(inbound.msg_id, inbound.rowid, inbound.conversation_id, replyMsgId, now, now);
    db.exec("COMMIT;");
  } catch (err) {
    db.exec("ROLLBACK;");
    throw err;
  }
  return replyMsgId;
}

function claimBeforeExternalSend(db, inbound) {
  const now = new Date().toISOString();
  db.prepare(`
    INSERT INTO codex_one_shot_processed
      (inbound_msg_id, inbound_rowid, conversation_id, reply_msg_id, status, processed_at, updated_at)
    VALUES (?, ?, ?, NULL, 'pending', ?, ?)
  `).run(inbound.msg_id, inbound.rowid, inbound.conversation_id, now, now);
}

function markExternalSendComplete(db, inbound) {
  db.prepare(`
    UPDATE codex_one_shot_processed
    SET status = 'sent', updated_at = ?
    WHERE inbound_msg_id = ? AND status = 'pending'
  `).run(new Date().toISOString(), inbound.msg_id);
}

function assertCurrentLeaseToken(args, inbound) {
  if (!args.leaseDb && args.leaseToken === undefined) return;
  requireNonEmpty(args.leaseDb, "lease-db");
  requireNonEmpty(args.leaseMemberSlot, "lease-member-slot");
  if (!Number.isInteger(args.leaseToken) || args.leaseToken <= 0) {
    throw new Error("invalid lease-token");
  }
  const leaseDb = new DatabaseSync(args.leaseDb);
  try {
    leaseDb.exec("PRAGMA busy_timeout=5000;");
    const row = leaseDb
      .prepare("SELECT token FROM channel_owner WHERE conversation_id = ? AND member_slot = ?")
      .get(inbound.conversation_id, args.leaseMemberSlot);
    if (!row || Number(row.token) !== args.leaseToken) {
      throw new Error(`stale lease token for ${inbound.conversation_id}/${args.leaseMemberSlot}`);
    }
  } finally {
    leaseDb.close();
  }
}

function sendMurmur(args, inbound, replyText) {
  const script = path.join(args.murmurRoot, "scripts", "murmur-shell-send.mjs");
  const workdir = mkdtempSync(path.join(tmpdir(), "codex-one-shot-send."));
  const replyFile = path.join(workdir, "reply.txt");
  const env = {
    ...process.env,
    DATA_DIR: path.dirname(args.db),
  };
  try {
    writeFileSync(replyFile, replyText, { mode: 0o600 });
    const res = spawnSync("node", [script, "--to", args.recipient, "--conv", inbound.conversation_id, "--text-file", replyFile], {
      encoding: "utf8",
      env,
      stdio: ["ignore", "pipe", "pipe"],
      timeout: 30_000,
    });
    if (res.status !== 0) {
      if (res.error) {
        throw res.error;
      }
      const detail = [res.stderr, res.stdout].filter(Boolean).join("\n").slice(0, 1200);
      throw new Error(detail || `murmur-shell-send exited ${res.status}`);
    }
  } finally {
    rmSync(workdir, { recursive: true, force: true });
  }
  return null;
}

const args = parseArgs(process.argv.slice(2));
const db = new DatabaseSync(args.db);
db.exec("PRAGMA busy_timeout=5000;");
ensureTables(db);

const inbound = getInbound(db, args);
try {
  if (!inbound) throw new Error("inbound message not found");
  if (inbound.direction !== "inbound") throw new Error("message is not inbound");
  if (inbound.sender !== args.sender) throw new Error(`unexpected sender ${inbound.sender}`);
  requireNonEmpty(inbound.conversation_id, "conversation_id");
  requireNonEmpty(inbound.msg_id, "msg_id");
  requireNonEmpty(inbound.text, "text");
} catch (err) {
  console.error(`INVALID_ENVELOPE: ${err.message}`);
  process.exit(EXIT.invalidEnvelope);
}

const existing = db.prepare(`
  SELECT inbound_msg_id, reply_msg_id, processed_at
  FROM codex_one_shot_processed
  WHERE inbound_msg_id = ?
`).get(inbound.msg_id);

if (!args.replyText || args.replyText.trim() === "") {
  console.error("GENERATION_FAILED: reply payload is empty");
  process.exit(EXIT.generationFailed);
}

if (existing) {
  console.error(`ALREADY_PROCESSED: ${inbound.msg_id}`);
  process.exit(EXIT.alreadyProcessed);
}

const reply = buildReply({
  conversationId: inbound.conversation_id,
  replyTo: inbound.msg_id,
  project: args.project,
  body: args.replyText,
});

try {
  try {
    assertCurrentLeaseToken(args, inbound);
  } catch (err) {
    console.error(`STALE_LEASE_TOKEN: ${err.message}`);
    process.exit(EXIT.staleLease);
  }

  if (args.sendMode === "append-local") {
    const replyMsgId = sendAppendLocal(db, args, inbound, reply);
    console.log(JSON.stringify({ status: "sent", mode: args.sendMode, inbound: inbound.msg_id, replyMsgId }));
  } else if (args.sendMode === "murmur") {
    claimBeforeExternalSend(db, inbound);
    sendMurmur(args, inbound, reply);
    markExternalSendComplete(db, inbound);
    console.log(JSON.stringify({ status: "sent", mode: args.sendMode, inbound: inbound.msg_id }));
  } else {
    console.log(reply);
  }
} catch (err) {
  console.error(`SEND_FAILED: ${err.message}`);
  process.exit(EXIT.sendFailed);
}
