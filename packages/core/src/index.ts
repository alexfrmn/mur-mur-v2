import { promises as fs } from "node:fs";
import path from "node:path";

export type DeliveryMode = "at-least-once";

export interface EnvelopeV1 {
  schemaVersion: "1.0";
  msgId: string;
  conversationId: string;
  senderAgentId: string;
  recipients: string[];
  createdAt: string;
  ttlSeconds?: number;
  traceId?: string;
  sequence?: number;
  parentMsgId?: string;
  payloadCiphertext: string;
  payloadNonce: string;
  signature: string;
}

export interface AckV1 {
  msgId: string;
  consumerId: string;
  status: "ack" | "nack";
  reason?: string;
  at: string;
}

export interface DedupeStore {
  seen(msgId: string, consumerId: string): Promise<boolean>;
  markSeen(msgId: string, consumerId: string): Promise<void>;
}

export class InMemoryDedupeStore implements DedupeStore {
  private readonly keys = new Set<string>();

  async seen(msgId: string, consumerId: string): Promise<boolean> {
    return this.keys.has(`${consumerId}:${msgId}`);
  }

  async markSeen(msgId: string, consumerId: string): Promise<void> {
    this.keys.add(`${consumerId}:${msgId}`);
  }
}

interface JsonDedupeState {
  seen: string[];
}

export class JsonFileDedupeStore implements DedupeStore {
  private readonly filePath: string;

  constructor(filePath = ".data/dedupe.json") {
    this.filePath = filePath;
  }

  private async load(): Promise<Set<string>> {
    try {
      const raw = await fs.readFile(this.filePath, "utf8");
      const data = JSON.parse(raw) as JsonDedupeState;
      return new Set(data.seen ?? []);
    } catch {
      return new Set();
    }
  }

  private async save(set: Set<string>): Promise<void> {
    const dir = path.dirname(this.filePath);
    await fs.mkdir(dir, { recursive: true });
    const state: JsonDedupeState = { seen: [...set] };
    await fs.writeFile(this.filePath, JSON.stringify(state, null, 2), "utf8");
  }

  async seen(msgId: string, consumerId: string): Promise<boolean> {
    const set = await this.load();
    return set.has(`${consumerId}:${msgId}`);
  }

  async markSeen(msgId: string, consumerId: string): Promise<void> {
    const set = await this.load();
    set.add(`${consumerId}:${msgId}`);
    await this.save(set);
  }
}

export type OutboxStatus = "pending" | "sent" | "acked" | "failed" | "dlq";

export interface OutboxRecord {
  msgId: string;
  subject: string;
  envelope: EnvelopeV1;
  status: OutboxStatus;
  attempts: number;
  nextAttemptAt: string;
  lastError?: string;
  createdAt: string;
  updatedAt: string;
}

export interface OutboxStore {
  enqueue(subject: string, envelope: EnvelopeV1): Promise<void>;
  claimDue(limit?: number): Promise<OutboxRecord[]>;
  markSent(msgId: string): Promise<void>;
  markAcked(msgId: string): Promise<void>;
  markFailed(msgId: string, error: string, nextAttemptAt: string): Promise<void>;
  markDlq(msgId: string, error: string): Promise<void>;
}

interface JsonOutboxState {
  records: OutboxRecord[];
}

export class JsonFileOutboxStore implements OutboxStore {
  constructor(private readonly filePath = ".data/outbox.json") {}

  private async load(): Promise<JsonOutboxState> {
    try {
      const raw = await fs.readFile(this.filePath, "utf8");
      return JSON.parse(raw) as JsonOutboxState;
    } catch {
      return { records: [] };
    }
  }

  private async save(state: JsonOutboxState): Promise<void> {
    const dir = path.dirname(this.filePath);
    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(this.filePath, JSON.stringify(state, null, 2), "utf8");
  }

  async enqueue(subject: string, envelope: EnvelopeV1): Promise<void> {
    const state = await this.load();
    if (state.records.find((r) => r.msgId === envelope.msgId)) return;

    const now = new Date().toISOString();
    state.records.push({
      msgId: envelope.msgId,
      subject,
      envelope,
      status: "pending",
      attempts: 0,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    });
    await this.save(state);
  }

  async claimDue(limit = 50): Promise<OutboxRecord[]> {
    const state = await this.load();
    const now = Date.now();
    return state.records
      .filter((r) => ["pending", "failed"].includes(r.status) && new Date(r.nextAttemptAt).getTime() <= now)
      .slice(0, limit);
  }

  async markSent(msgId: string): Promise<void> {
    const state = await this.load();
    const row = state.records.find((r) => r.msgId === msgId);
    if (!row) return;
    row.status = "sent";
    row.attempts += 1;
    row.updatedAt = new Date().toISOString();
    await this.save(state);
  }

  async markAcked(msgId: string): Promise<void> {
    const state = await this.load();
    const row = state.records.find((r) => r.msgId === msgId);
    if (!row) return;
    row.status = "acked";
    row.updatedAt = new Date().toISOString();
    await this.save(state);
  }

  async markFailed(msgId: string, error: string, nextAttemptAt: string): Promise<void> {
    const state = await this.load();
    const row = state.records.find((r) => r.msgId === msgId);
    if (!row) return;
    row.status = "failed";
    row.lastError = error;
    row.nextAttemptAt = nextAttemptAt;
    row.updatedAt = new Date().toISOString();
    await this.save(state);
  }

  async markDlq(msgId: string, error: string): Promise<void> {
    const state = await this.load();
    const row = state.records.find((r) => r.msgId === msgId);
    if (!row) return;
    row.status = "dlq";
    row.lastError = error;
    row.updatedAt = new Date().toISOString();
    await this.save(state);
  }
}

export const isEnvelopeV1 = (v: unknown): v is EnvelopeV1 => {
  if (!v || typeof v !== "object") return false;
  const o = v as Record<string, unknown>;
  return (
    o.schemaVersion === "1.0" &&
    typeof o.msgId === "string" &&
    typeof o.conversationId === "string" &&
    typeof o.senderAgentId === "string" &&
    Array.isArray(o.recipients) &&
    typeof o.payloadCiphertext === "string"
  );
};

export const createAck = (
  msgId: string,
  consumerId: string,
  status: AckV1["status"],
  reason?: string,
): AckV1 => ({
  msgId,
  consumerId,
  status,
  reason,
  at: new Date().toISOString(),
});

export const computeBackoffMs = (attempt: number, baseMs = 500, maxMs = 60_000): number => {
  const raw = baseMs * Math.pow(2, Math.max(0, attempt - 1));
  return Math.min(maxMs, raw);
};
