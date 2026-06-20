import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";

const script = path.resolve("scripts/wake-drain-claude.sh");

function withDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "murmur-wake-drain-"));
  const dbPath = path.join(dir, "murmur.db");
  const cursorPath = path.join(dir, "cursor");
  const db = new DatabaseSync(dbPath);
  db.exec(`
    CREATE TABLE local_messages (
      msg_id TEXT PRIMARY KEY,
      created_at TEXT,
      sender TEXT,
      conversation_id TEXT,
      direction TEXT,
      text TEXT
    );
  `);
  return { db, dbPath, cursorPath };
}

function insertMessage(db, { msgId, direction = "inbound", sender = "agent-jarvis", text = "hello" }) {
  db.prepare(`
    INSERT INTO local_messages (msg_id, created_at, sender, conversation_id, direction, text)
    VALUES (?, '2026-06-20T00:00:00.000Z', ?, 'codex:task:test', ?, ?)
  `).run(msgId, sender, direction, text);
}

function drain({ dbPath, cursorPath }) {
  return spawnSync(script, [], {
    env: {
      ...process.env,
      MURMUR_DB: dbPath,
      MURMUR_WAKE_CURSOR: cursorPath,
    },
    encoding: "utf8",
  });
}

test("Claude wake drain exits 0 when there are no new inbound messages", () => {
  const ctx = withDb();
  insertMessage(ctx.db, { msgId: "outbound-1", direction: "outbound", text: "ignore me" });

  const result = drain(ctx);

  assert.equal(result.status, 0);
  assert.equal(result.stderr, "");
  assert.equal(fs.existsSync(ctx.cursorPath), false);
});

test("Claude wake drain emits new inbound rows and advances the cursor", () => {
  const ctx = withDb();
  insertMessage(ctx.db, { msgId: "inbound-1", text: "first\nline" });
  insertMessage(ctx.db, { msgId: "outbound-1", direction: "outbound", text: "ignore me" });
  insertMessage(ctx.db, { msgId: "inbound-2", sender: "agent-peer", text: "second" });

  const result = drain(ctx);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /Murmur wake: 2 new inbound message\(s\):/);
  assert.match(result.stderr, /rowid=1 \[agent-jarvis\] first line/);
  assert.match(result.stderr, /rowid=3 \[agent-peer\] second/);
  assert.doesNotMatch(result.stderr, /ignore me/);
  assert.equal(fs.readFileSync(ctx.cursorPath, "utf8").trim(), "3");
});

test("Claude wake drain cursor dedup prevents repeat wakes", () => {
  const ctx = withDb();
  insertMessage(ctx.db, { msgId: "inbound-1", text: "first" });

  const first = drain(ctx);
  const second = drain(ctx);

  assert.equal(first.status, 2);
  assert.equal(second.status, 0);
  assert.equal(second.stderr, "");
  assert.equal(fs.readFileSync(ctx.cursorPath, "utf8").trim(), "1");
});
