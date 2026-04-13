import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SQLiteDedupeOutboxStore } from "../packages/core/dist/src/index.js";

const envelope = {
  schemaVersion: "1.0",
  msgId: "msg-1",
  conversationId: "conv-1",
  senderAgentId: "agent.a",
  recipients: ["agent.b"],
  createdAt: new Date().toISOString(),
  payloadCiphertext: Buffer.from("x").toString("base64"),
  payloadNonce: "nonce",
  signature: "sig",
};

test("requeueStaleSent moves stale sent rows back to failed", async () => {
  const dir = mkdtempSync(join(tmpdir(), "murmur-outbox-"));
  const dbPath = join(dir, "murmur.db");
  const store = new SQLiteDedupeOutboxStore(dbPath);

  await store.enqueue("agent.b", envelope);
  await store.markSent(envelope.msgId);

  await new Promise((r) => setTimeout(r, 15));
  const changed = await store.requeueStaleSent(1);
  assert.equal(changed, 1);

  const due = await store.claimDue(10);
  assert.equal(due.length, 1);
  assert.equal(due[0].status, "failed");
  assert.equal(due[0].lastError, "ack-timeout");
});
