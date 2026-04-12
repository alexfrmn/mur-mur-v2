import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { collectPrometheusSnapshot, renderPrometheusMetrics } from "../packages/observability/dist/src/index.js";

const makeDb = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "murmur-metrics-"));
  const dbPath = path.join(dir, "murmur.db");
  const db = new DatabaseSync(dbPath);
  db.exec(`
    CREATE TABLE outbox (
      msg_id TEXT PRIMARY KEY,
      subject TEXT NOT NULL,
      envelope_json TEXT NOT NULL,
      status TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      next_attempt_at TEXT NOT NULL,
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      version INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE local_messages (
      id TEXT PRIMARY KEY,
      conversation_id TEXT NOT NULL,
      msg_id TEXT NOT NULL,
      direction TEXT NOT NULL,
      sender TEXT NOT NULL,
      text TEXT NOT NULL,
      created_at TEXT NOT NULL,
      transport TEXT
    );
  `);
  return { dir, dbPath, db };
};

test("collectPrometheusSnapshot aggregates outbox and message metrics", () => {
  const { dir, dbPath, db } = makeDb();
  const baseNow = Date.parse("2026-04-12T20:00:00.000Z");

  db.prepare(`INSERT INTO outbox (msg_id, subject, envelope_json, status, attempts, next_attempt_at, last_error, created_at, updated_at, version)
    VALUES (?, ?, '{}', ?, ?, ?, ?, ?, ?, 1)`).run(
    "msg-pending", "msg.alpha", "pending", 0, "2026-04-12T19:40:00.000Z", null, "2026-04-12T19:30:00.000Z", "2026-04-12T19:30:00.000Z",
  );
  db.prepare(`INSERT INTO outbox (msg_id, subject, envelope_json, status, attempts, next_attempt_at, last_error, created_at, updated_at, version)
    VALUES (?, ?, '{}', ?, ?, ?, ?, ?, ?, 1)`).run(
    "msg-acked", "msg.alpha", "acked", 1, "2026-04-12T19:41:00.000Z", null, "2026-04-12T19:40:00.000Z", "2026-04-12T19:40:05.000Z",
  );
  db.prepare(`INSERT INTO outbox (msg_id, subject, envelope_json, status, attempts, next_attempt_at, last_error, created_at, updated_at, version)
    VALUES (?, ?, '{}', ?, ?, ?, ?, ?, ?, 1)`).run(
    "msg-failed", "msg.beta", "failed", 2, "2026-04-12T19:59:00.000Z", "timeout", "2026-04-12T19:50:00.000Z", "2026-04-12T19:59:00.000Z",
  );
  db.prepare(`INSERT INTO outbox (msg_id, subject, envelope_json, status, attempts, next_attempt_at, last_error, created_at, updated_at, version)
    VALUES (?, ?, '{}', ?, ?, ?, ?, ?, ?, 1)`).run(
    "msg-dlq", "msg.gamma", "dlq", 5, "2026-04-12T19:00:00.000Z", "poison", "2026-04-12T18:00:00.000Z", "2026-04-12T19:10:00.000Z",
  );

  db.prepare(`INSERT INTO local_messages (id, conversation_id, msg_id, direction, sender, text, created_at, transport)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
    .run("1", "conv-1", "msg-a", "inbound", "alice", "hello", "2026-04-12T19:45:00.000Z", "nats");
  db.prepare(`INSERT INTO local_messages (id, conversation_id, msg_id, direction, sender, text, created_at, transport)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
    .run("2", "conv-1", "msg-b", "outbound", "bob", "world", "2026-04-12T19:50:00.000Z", "nats");
  db.prepare(`INSERT INTO local_messages (id, conversation_id, msg_id, direction, sender, text, created_at, transport)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
    .run("3", "conv-2", "msg-c", "outbound", "bob", "older", "2026-04-12T15:00:00.000Z", "nats");

  db.close();

  const snapshot = collectPrometheusSnapshot(dbPath, () => baseNow);
  assert.equal(snapshot.outboxDepth.pending, 1);
  assert.equal(snapshot.outboxDepth.acked, 1);
  assert.equal(snapshot.outboxDepth.failed, 1);
  assert.equal(snapshot.outboxDepth.dlq, 1);
  assert.equal(snapshot.outboxOldestPendingAgeSeconds, 1800);
  assert.equal(snapshot.localMessagesTotal.inbound, 1);
  assert.equal(snapshot.localMessagesTotal.outbound, 2);
  assert.equal(snapshot.localMessagesLastHour.inbound, 1);
  assert.equal(snapshot.localMessagesLastHour.outbound, 1);
  assert.ok(Math.abs(snapshot.ackLatencyAvgSeconds - 5) < 0.01);
  assert.ok(Math.abs(snapshot.ackLatencyP95Seconds - 5) < 0.01);
  assert.equal(snapshot.retryRows, 1);
  assert.equal(snapshot.deadLetterRows, 1);
  assert.equal(snapshot.errorRowsLastHour, 2);

  const metrics = renderPrometheusMetrics(snapshot);
  assert.match(metrics, /murmur_outbox_depth\{status="pending"\} 1/);
  assert.match(metrics, /murmur_ack_latency_avg_seconds 5/);
  assert.match(metrics, /murmur_outbox_dead_letter_rows 1/);

  fs.rmSync(dir, { recursive: true, force: true });
});
