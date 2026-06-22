import { createHash } from "node:crypto";

export interface StreamChunkPayloadV1 {
  type: "murmur.stream.chunk.v1";
  streamId: string;
  chunkIndex: number;
  chunkCount: number;
  totalBytes: number;
  payloadBytes: number;
  payloadBase64: string;
  payloadSha256: string;
  streamSha256: string;
}

export interface CreateStreamChunksOptions {
  streamId: string;
  maxChunkBytes: number;
}

export type StreamNackReason =
  | "stream-chunk-invalid"
  | "stream-chunk-hash-invalid"
  | "stream-chunk-hash-mismatch"
  | "stream-chunk-count-mismatch"
  | "stream-total-bytes-mismatch"
  | "stream-hash-mismatch"
  | "stream-backpressure-chunks"
  | "stream-backpressure-bytes";

export type StreamAcceptResult =
  | { status: "accepted"; streamId: string; receivedChunks: number; chunkCount: number }
  | { status: "complete"; streamId: string; payload: Uint8Array }
  | { status: "nack"; streamId?: string; reason: StreamNackReason };

export interface StreamReassemblerOptions {
  maxInFlightChunks?: number;
  maxPendingBytes?: number;
}

interface PendingStream {
  chunkCount: number;
  totalBytes: number;
  streamSha256: string;
  pendingBytes: number;
  chunks: Map<number, StreamChunkPayloadV1>;
}

interface CompletedStream {
  payload: Uint8Array;
  hashesByIndex: Map<number, string>;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const sha256 = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");

const toBytes = (payload: Uint8Array | string): Uint8Array => (
  typeof payload === "string" ? encoder.encode(payload) : payload
);

const fromBase64 = (payloadBase64: string): Uint8Array => Buffer.from(payloadBase64, "base64");

const validatePositiveInteger = (value: number): boolean => Number.isInteger(value) && value > 0;

const validateNonNegativeInteger = (value: number): boolean => Number.isInteger(value) && value >= 0;

export const createStreamChunks = (
  payload: Uint8Array | string,
  options: CreateStreamChunksOptions,
): StreamChunkPayloadV1[] => {
  if (!options.streamId) throw new Error("stream-id-required");
  if (!validatePositiveInteger(options.maxChunkBytes)) throw new Error("max-chunk-bytes-invalid");

  const bytes = toBytes(payload);
  const chunkCount = Math.max(1, Math.ceil(bytes.byteLength / options.maxChunkBytes));
  const streamSha256 = sha256(bytes);
  const chunks: StreamChunkPayloadV1[] = [];

  for (let index = 0; index < chunkCount; index += 1) {
    const start = index * options.maxChunkBytes;
    const end = Math.min(start + options.maxChunkBytes, bytes.byteLength);
    const slice = bytes.slice(start, end);
    chunks.push({
      type: "murmur.stream.chunk.v1",
      streamId: options.streamId,
      chunkIndex: index,
      chunkCount,
      totalBytes: bytes.byteLength,
      payloadBytes: slice.byteLength,
      payloadBase64: Buffer.from(slice).toString("base64"),
      payloadSha256: sha256(slice),
      streamSha256,
    });
  }

  return chunks;
};

export const streamChunksToBytes = (chunks: StreamChunkPayloadV1[]): Uint8Array => {
  const sorted = [...chunks].sort((a, b) => a.chunkIndex - b.chunkIndex);
  const parts = sorted.map((chunk) => fromBase64(chunk.payloadBase64));
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const output = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
};

export const streamChunksToText = (chunks: StreamChunkPayloadV1[]): string => decoder.decode(streamChunksToBytes(chunks));

export const streamPayloadVisibleEnvelopeFields = (payload: Record<string, unknown>): string[] => {
  const visibleEnvelopeFields = [
    "msgId",
    "conversationId",
    "senderAgentId",
    "recipients",
    "createdAt",
    "ttlSeconds",
    "traceId",
    "sequence",
    "parentMsgId",
  ];
  return visibleEnvelopeFields.filter((field) => field in payload);
};

export class InMemoryStreamReassembler {
  private readonly pending = new Map<string, PendingStream>();
  private readonly completed = new Map<string, CompletedStream>();
  private readonly maxInFlightChunks: number;
  private readonly maxPendingBytes: number;
  private pendingBytes = 0;

  constructor(options: StreamReassemblerOptions = {}) {
    this.maxInFlightChunks = Math.max(1, Math.floor(options.maxInFlightChunks ?? 64));
    this.maxPendingBytes = Math.max(1, Math.floor(options.maxPendingBytes ?? 16 * 1024 * 1024));
  }

  accept(chunk: StreamChunkPayloadV1): StreamAcceptResult {
    const validation = this.validateChunk(chunk);
    if (validation) return validation;

    const completed = this.completed.get(chunk.streamId);
    if (completed) {
      const existingHash = completed.hashesByIndex.get(chunk.chunkIndex);
      if (existingHash !== chunk.payloadSha256) {
        return { status: "nack", streamId: chunk.streamId, reason: "stream-chunk-hash-mismatch" };
      }
      return { status: "complete", streamId: chunk.streamId, payload: completed.payload };
    }

    const pending = this.pending.get(chunk.streamId) ?? this.createPending(chunk);
    const consistency = this.validateConsistency(pending, chunk);
    if (consistency) return consistency;

    const existing = pending.chunks.get(chunk.chunkIndex);
    if (existing) {
      if (existing.payloadSha256 !== chunk.payloadSha256) {
        return { status: "nack", streamId: chunk.streamId, reason: "stream-chunk-hash-mismatch" };
      }
      return { status: "accepted", streamId: chunk.streamId, receivedChunks: pending.chunks.size, chunkCount: pending.chunkCount };
    }

    const backpressure = this.validateBackpressure(pending, chunk);
    if (backpressure) return backpressure;

    pending.chunks.set(chunk.chunkIndex, chunk);
    pending.pendingBytes += chunk.payloadBytes;
    this.pendingBytes += chunk.payloadBytes;

    if (!this.pending.has(chunk.streamId)) this.pending.set(chunk.streamId, pending);
    if (pending.chunks.size < pending.chunkCount) {
      return { status: "accepted", streamId: chunk.streamId, receivedChunks: pending.chunks.size, chunkCount: pending.chunkCount };
    }

    return this.completeStream(chunk.streamId, pending);
  }

  pendingStats(): { streamCount: number; pendingChunks: number; pendingBytes: number } {
    let pendingChunks = 0;
    for (const stream of this.pending.values()) pendingChunks += stream.chunks.size;
    return { streamCount: this.pending.size, pendingChunks, pendingBytes: this.pendingBytes };
  }

  private validateChunk(chunk: StreamChunkPayloadV1): StreamAcceptResult | undefined {
    if (
      chunk?.type !== "murmur.stream.chunk.v1" ||
      !chunk.streamId ||
      !validateNonNegativeInteger(chunk.chunkIndex) ||
      !validatePositiveInteger(chunk.chunkCount) ||
      chunk.chunkIndex >= chunk.chunkCount ||
      !validateNonNegativeInteger(chunk.totalBytes) ||
      !validateNonNegativeInteger(chunk.payloadBytes) ||
      typeof chunk.payloadBase64 !== "string" ||
      typeof chunk.payloadSha256 !== "string" ||
      typeof chunk.streamSha256 !== "string"
    ) {
      return { status: "nack", streamId: chunk?.streamId, reason: "stream-chunk-invalid" };
    }

    const payload = fromBase64(chunk.payloadBase64);
    if (payload.byteLength !== chunk.payloadBytes || sha256(payload) !== chunk.payloadSha256) {
      return { status: "nack", streamId: chunk.streamId, reason: "stream-chunk-hash-invalid" };
    }
    return undefined;
  }

  private createPending(chunk: StreamChunkPayloadV1): PendingStream {
    return {
      chunkCount: chunk.chunkCount,
      totalBytes: chunk.totalBytes,
      streamSha256: chunk.streamSha256,
      pendingBytes: 0,
      chunks: new Map<number, StreamChunkPayloadV1>(),
    };
  }

  private validateConsistency(pending: PendingStream, chunk: StreamChunkPayloadV1): StreamAcceptResult | undefined {
    if (pending.chunkCount !== chunk.chunkCount) {
      return { status: "nack", streamId: chunk.streamId, reason: "stream-chunk-count-mismatch" };
    }
    if (pending.totalBytes !== chunk.totalBytes) {
      return { status: "nack", streamId: chunk.streamId, reason: "stream-total-bytes-mismatch" };
    }
    if (pending.streamSha256 !== chunk.streamSha256) {
      return { status: "nack", streamId: chunk.streamId, reason: "stream-hash-mismatch" };
    }
    return undefined;
  }

  private validateBackpressure(pending: PendingStream, chunk: StreamChunkPayloadV1): StreamAcceptResult | undefined {
    if (pending.chunks.size + 1 > this.maxInFlightChunks) {
      return { status: "nack", streamId: chunk.streamId, reason: "stream-backpressure-chunks" };
    }
    if (this.pendingBytes + chunk.payloadBytes > this.maxPendingBytes) {
      return { status: "nack", streamId: chunk.streamId, reason: "stream-backpressure-bytes" };
    }
    return undefined;
  }

  private completeStream(streamId: string, pending: PendingStream): StreamAcceptResult {
    const chunks = [...pending.chunks.values()].sort((a, b) => a.chunkIndex - b.chunkIndex);
    const payload = streamChunksToBytes(chunks);
    if (payload.byteLength !== pending.totalBytes || sha256(payload) !== pending.streamSha256) {
      return { status: "nack", streamId, reason: "stream-hash-mismatch" };
    }

    const hashesByIndex = new Map(chunks.map((chunk) => [chunk.chunkIndex, chunk.payloadSha256]));
    this.pending.delete(streamId);
    this.pendingBytes -= pending.pendingBytes;
    this.completed.set(streamId, { payload, hashesByIndex });
    return { status: "complete", streamId, payload };
  }
}
