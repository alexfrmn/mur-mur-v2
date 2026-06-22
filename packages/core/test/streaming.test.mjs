import test from "node:test";
import assert from "node:assert/strict";
import {
  InMemoryStreamReassembler,
  SQLiteStreamReassembler,
  chunkStreamText,
  createStreamEnd,
  createStreamChunk,
  createStreamStart,
  sha256Hex,
  streamBackpressureAllowsSend,
} from "../dist/src/index.js";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("createStreamStart and createStreamEnd frame stream boundaries without payload data", () => {
  const startedAt = "2026-06-22T13:00:00.000Z";
  assert.deepEqual(
    createStreamStart({
      streamId: "stream-0",
      chunkCount: 3,
      totalBytes: 42,
      contentType: "text/plain",
      startedAt,
    }),
    {
      kind: "stream.start",
      streamId: "stream-0",
      chunkCount: 3,
      totalBytes: 42,
      contentType: "text/plain",
      startedAt,
    },
  );
  assert.deepEqual(
    createStreamEnd({
      streamId: "stream-0",
      chunkCount: 3,
      totalBytes: 42,
    }),
    {
      kind: "stream.end",
      streamId: "stream-0",
      chunkCount: 3,
      totalBytes: 42,
    },
  );
});

test("chunkStreamText splits UTF-8 text without exceeding max payload bytes", () => {
  const text = "hello Привет 👋".repeat(24);
  const chunks = chunkStreamText({
    streamId: "stream-1",
    text,
    maxChunkBytes: 37,
  });

  assert.ok(chunks.length > 1);
  assert.equal(chunks[0].kind, "stream.chunk");
  assert.equal(chunks.at(-1).isLast, true);
  assert.deepEqual(chunks.map((chunk) => chunk.chunkIndex), chunks.map((_, index) => index));
  assert.equal(new Set(chunks.map((chunk) => chunk.chunkCount)).size, 1);

  for (const chunk of chunks) {
    assert.ok(Buffer.byteLength(chunk.data, "utf8") <= 37);
  }
  assert.equal(chunks.map((chunk) => chunk.data).join(""), text);
  assert.equal(chunks[0].sha256, sha256Hex(chunks[0].data));
});

test("InMemoryStreamReassembler verifies per-chunk and total sha256", () => {
  const text = "integrity checked Привет";
  const chunks = chunkStreamText({ streamId: "stream-sha", text, maxChunkBytes: 9 });
  const end = createStreamEnd({
    streamId: "stream-sha",
    chunkCount: chunks.length,
    totalBytes: Buffer.byteLength(text, "utf8"),
    sha256: sha256Hex(text),
  });
  const reassembler = new InMemoryStreamReassembler();

  assert.equal(reassembler.acceptEnd(end).status, "pending");
  let result;
  for (const chunk of chunks) result = reassembler.accept(chunk);

  assert.equal(result.status, "complete");
  assert.equal(result.text, text);
  assert.equal(result.sha256, sha256Hex(text));
});

test("InMemoryStreamReassembler rejects chunk sha256 mismatch", () => {
  const reassembler = new InMemoryStreamReassembler();
  const chunk = createStreamChunk({
    streamId: "stream-bad-sha",
    chunkIndex: 0,
    chunkCount: 1,
    data: "hello",
  });

  assert.throws(
    () => reassembler.accept({ ...chunk, sha256: sha256Hex("HELLO") }),
    /stream-chunk-sha256-mismatch:stream-bad-sha:0/,
  );
});

test("chunkStreamText rejects invalid byte budgets", () => {
  assert.throws(
    () => chunkStreamText({ streamId: "stream-1", text: "abc", maxChunkBytes: 0 }),
    /stream-max-chunk-bytes-invalid/,
  );
});

test("InMemoryStreamReassembler deduplicates chunks and completes out of order", () => {
  const reassembler = new InMemoryStreamReassembler();
  const chunks = chunkStreamText({
    streamId: "stream-2",
    text: "chunked message payload",
    maxChunkBytes: 6,
  });

  const first = reassembler.accept(chunks[1]);
  assert.equal(first.status, "pending");
  assert.equal(first.receivedChunks, 1);

  const duplicate = reassembler.accept(chunks[1]);
  assert.equal(duplicate.status, "duplicate");
  assert.equal(duplicate.receivedChunks, 1);

  let result;
  for (const chunk of [chunks[0], ...chunks.slice(2)]) {
    result = reassembler.accept(chunk);
  }

  assert.equal(result.status, "complete");
  assert.equal(result.text, "chunked message payload");
  assert.equal(result.receivedChunks, chunks.length);
  assert.equal(result.chunkCount, chunks.length);
  assert.equal(reassembler.get("stream-2"), undefined);
});

test("InMemoryStreamReassembler rejects conflicting duplicate chunk data", () => {
  const reassembler = new InMemoryStreamReassembler();
  const chunk = createStreamChunk({
    streamId: "stream-3",
    chunkIndex: 0,
    chunkCount: 2,
    data: "hello ",
  });
  reassembler.accept(chunk);

  assert.throws(
    () => reassembler.accept({ ...chunk, data: "HELLO ", sha256: sha256Hex("HELLO ") }),
    /stream-chunk-conflict:stream-3:0/,
  );
});

test("SQLiteStreamReassembler durably completes after restart", () => {
  const dir = mkdtempSync(join(tmpdir(), "murmur-stream-"));
  const dbPath = join(dir, "stream.db");
  const text = "durable stream payload survives process restart";
  const chunks = chunkStreamText({ streamId: "stream-sqlite", text, maxChunkBytes: 8 });
  const end = createStreamEnd({
    streamId: "stream-sqlite",
    chunkCount: chunks.length,
    totalBytes: Buffer.byteLength(text, "utf8"),
    sha256: sha256Hex(text),
  });

  const first = new SQLiteStreamReassembler(dbPath);
  first.accept(chunks[1]);
  first.acceptEnd(end);

  const second = new SQLiteStreamReassembler(dbPath);
  let result;
  for (const chunk of [chunks[0], ...chunks.slice(2)]) {
    result = second.accept(chunk);
  }

  assert.equal(result.status, "complete");
  assert.equal(result.text, text);
  assert.equal(result.sha256, sha256Hex(text));
  assert.equal(second.get("stream-sqlite"), undefined);
});

test("streamBackpressureAllowsSend enforces chunk and byte windows", () => {
  assert.equal(
    streamBackpressureAllowsSend({
      inFlightChunks: 3,
      inFlightBytes: 120,
      nextChunkBytes: 30,
      maxInFlightChunks: 4,
      maxInFlightBytes: 150,
    }),
    true,
  );
  assert.equal(
    streamBackpressureAllowsSend({
      inFlightChunks: 4,
      inFlightBytes: 120,
      nextChunkBytes: 1,
      maxInFlightChunks: 4,
      maxInFlightBytes: 150,
    }),
    false,
  );
  assert.equal(
    streamBackpressureAllowsSend({
      inFlightChunks: 2,
      inFlightBytes: 140,
      nextChunkBytes: 11,
      maxInFlightChunks: 4,
      maxInFlightBytes: 150,
    }),
    false,
  );
});
