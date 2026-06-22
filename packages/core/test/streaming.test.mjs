import test from "node:test";
import assert from "node:assert/strict";
import {
  InMemoryStreamReassembler,
  createStreamChunks,
  streamChunksToText,
  streamPayloadVisibleEnvelopeFields,
} from "../dist/src/index.js";

const text = (value) => new TextEncoder().encode(value);

test("createStreamChunks splits payloads and preserves byte order", () => {
  const chunks = createStreamChunks(text("abcdefghij"), { streamId: "stream-a", maxChunkBytes: 4 });

  assert.equal(chunks.length, 3);
  assert.deepEqual(chunks.map((chunk) => chunk.chunkIndex), [0, 1, 2]);
  assert.deepEqual(chunks.map((chunk) => chunk.payloadBase64), ["YWJjZA==", "ZWZnaA==", "aWo="]);
  assert.equal(streamChunksToText(chunks), "abcdefghij");
});

test("chunk metadata is payload-only and does not require visible envelope fields", () => {
  const [chunk] = createStreamChunks("hello", { streamId: "stream-no-leak", maxChunkBytes: 64 });

  assert.deepEqual(streamPayloadVisibleEnvelopeFields(chunk), []);
  assert.ok("streamId" in chunk);
  assert.ok("chunkIndex" in chunk);
  assert.ok(!("parentMsgId" in chunk));
  assert.ok(!("sequence" in chunk));
});

test("reassembler completes when chunks arrive out of order", () => {
  const chunks = createStreamChunks("abcdefghij", { streamId: "stream-b", maxChunkBytes: 3 });
  const reassembler = new InMemoryStreamReassembler();

  assert.equal(reassembler.accept(chunks[2]).status, "accepted");
  assert.equal(reassembler.accept(chunks[0]).status, "accepted");
  const result = reassembler.accept(chunks[1]);

  assert.equal(result.status, "accepted");
  const final = reassembler.accept(chunks[3]);
  assert.equal(final.status, "complete");
  assert.equal(new TextDecoder().decode(final.payload), "abcdefghij");
});

test("duplicate chunks with the same hash are idempotent", () => {
  const [chunk] = createStreamChunks("hello", { streamId: "stream-c", maxChunkBytes: 64 });
  const reassembler = new InMemoryStreamReassembler();

  assert.equal(reassembler.accept(chunk).status, "complete");
  const duplicate = reassembler.accept(chunk);

  assert.equal(duplicate.status, "complete");
  assert.equal(new TextDecoder().decode(duplicate.payload), "hello");
});

test("same streamId and chunkIndex with a different hash is rejected", () => {
  const [chunk] = createStreamChunks("hello", { streamId: "stream-d", maxChunkBytes: 64 });
  const [different] = createStreamChunks("world", { streamId: "stream-d", maxChunkBytes: 64 });
  const reassembler = new InMemoryStreamReassembler();

  assert.equal(reassembler.accept(chunk).status, "complete");
  const conflict = reassembler.accept({
    ...chunk,
    payloadBase64: different.payloadBase64,
    payloadBytes: different.payloadBytes,
    payloadSha256: different.payloadSha256,
  });

  assert.equal(conflict.status, "nack");
  assert.equal(conflict.reason, "stream-chunk-hash-mismatch");
});

test("backpressure rejects streams above max in-flight chunk count", () => {
  const reassembler = new InMemoryStreamReassembler({ maxInFlightChunks: 2, maxPendingBytes: 1024 });
  const chunks = createStreamChunks("abcdefghi", { streamId: "stream-e", maxChunkBytes: 3 });

  assert.equal(reassembler.accept(chunks[0]).status, "accepted");
  assert.equal(reassembler.accept(chunks[1]).status, "accepted");
  const result = reassembler.accept(chunks[2]);

  assert.equal(result.status, "nack");
  assert.equal(result.reason, "stream-backpressure-chunks");
});

test("backpressure rejects streams above max pending bytes", () => {
  const reassembler = new InMemoryStreamReassembler({ maxInFlightChunks: 10, maxPendingBytes: 5 });
  const chunks = createStreamChunks("abcdefghi", { streamId: "stream-f", maxChunkBytes: 3 });

  assert.equal(reassembler.accept(chunks[0]).status, "accepted");
  const result = reassembler.accept(chunks[1]);

  assert.equal(result.status, "nack");
  assert.equal(result.reason, "stream-backpressure-bytes");
});

test("invalid chunk hashes are rejected before storage", () => {
  const [chunk] = createStreamChunks("hello", { streamId: "stream-g", maxChunkBytes: 64 });
  const reassembler = new InMemoryStreamReassembler();

  const result = reassembler.accept({ ...chunk, payloadSha256: "bad" });

  assert.equal(result.status, "nack");
  assert.equal(result.reason, "stream-chunk-hash-invalid");
  assert.equal(reassembler.pendingStats().streamCount, 0);
});

test("inconsistent chunk count for the same stream is rejected", () => {
  const chunks = createStreamChunks("abcdefghi", { streamId: "stream-h", maxChunkBytes: 3 });
  const reassembler = new InMemoryStreamReassembler();

  assert.equal(reassembler.accept(chunks[0]).status, "accepted");
  const result = reassembler.accept({ ...chunks[1], chunkCount: 4 });

  assert.equal(result.status, "nack");
  assert.equal(result.reason, "stream-chunk-count-mismatch");
});

test("completed streams are retained without pending-byte pressure", () => {
  const chunks = createStreamChunks("hello", { streamId: "stream-i", maxChunkBytes: 2 });
  const reassembler = new InMemoryStreamReassembler({ maxPendingBytes: 5 });

  assert.equal(reassembler.accept(chunks[0]).status, "accepted");
  assert.equal(reassembler.accept(chunks[1]).status, "accepted");
  const complete = reassembler.accept(chunks[2]);

  assert.equal(complete.status, "complete");
  assert.equal(reassembler.pendingStats().pendingBytes, 0);
  assert.equal(new TextDecoder().decode(complete.payload), "hello");
});
