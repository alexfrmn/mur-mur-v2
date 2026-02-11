import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { InMemoryDedupeStore, SQLiteDedupeOutboxStore } from "../packages/core/dist/src/index.js";

test("InMemoryDedupeStore seen/markSeen roundtrip", async () => {
  const store = new InMemoryDedupeStore();
  assert.equal(await store.seen("m1", "c1"), false);
  await store.markSeen("m1", "c1");
  assert.equal(await store.seen("m1", "c1"), true);
  assert.equal(await store.seen("m1", "c2"), false);
});

test("InMemoryDedupeStore evicts oldest entries when maxSize exceeded", async () => {
  const store = new InMemoryDedupeStore(5);
  await store.markSeen("m1", "c");
  await store.markSeen("m2", "c");
  await store.markSeen("m3", "c");
  await store.markSeen("m4", "c");
  await store.markSeen("m5", "c");
  await store.markSeen("m6", "c");

  assert.equal(await store.seen("m1", "c"), false);
  assert.equal(await store.seen("m2", "c"), true);
  assert.equal(await store.seen("m6", "c"), true);
});

test("SQLiteDedupeOutboxStore markSeen/seen roundtrip", async () => {
  const dir = mkdtempSync(join(tmpdir(), "murmur-dedupe-"));
  const dbPath = join(dir, "murmur.db");

  try {
    const store = new SQLiteDedupeOutboxStore(dbPath);
    assert.equal(await store.seen("m1", "consumer-1"), false);
    await store.markSeen("m1", "consumer-1");
    assert.equal(await store.seen("m1", "consumer-1"), true);
    assert.equal(await store.seen("m1", "consumer-2"), false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
