import { randomUUID } from "node:crypto";
import { mkdirSync, promises as fs } from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

// Agent discovery (presence frames + candidate registry)
export * from "./discovery.js";

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
  private readonly keys: Map<string, true>;
  private readonly maxSize: number;

  constructor(maxSize = 10_000) {
    this.keys = new Map<string, true>();
    this.maxSize = Math.max(1, Math.floor(maxSize));
  }

  async seen(msgId: string, consumerId: string): Promise<boolean> {
    return this.keys.has(`${consumerId}:${msgId}`);
  }

  async markSeen(msgId: string, consumerId: string): Promise<void> {
    this.keys.set(`${consumerId}:${msgId}`, true);
    this.evictIfNeeded();
  }

  private evictIfNeeded(): void {
    if (this.keys.size <= this.maxSize) return;
    const evictCount = Math.max(1, Math.ceil(this.maxSize * 0.1));
    const oldest = this.keys.keys();
    for (let i = 0; i < evictCount; i += 1) {
      const next = oldest.next();
      if (next.done) break;
      this.keys.delete(next.value);
    }
  }
}

interface JsonDedupeState {
  seen: string[];
}

const warnedJsonPaths = new Set<string>();
const warnIfJsonStoreMayRace = (filePath: string): void => {
  if (process.env.MURMUR_JSON_STORE_LOCKING === "1") return;
  if (warnedJsonPaths.has(filePath)) return;
  warnedJsonPaths.add(filePath);
  console.warn(
    `[murmur/core] JSON store at ${filePath} has no inter-process locking. Use single-process mode or set MURMUR_JSON_STORE_LOCKING=1 once external locking is guaranteed.`,
  );
};

export class JsonFileDedupeStore implements DedupeStore {
  private readonly filePath: string;

  constructor(filePath = ".data/dedupe.json") {
    this.filePath = filePath;
    warnIfJsonStoreMayRace(this.filePath);
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
  version?: number;
}

export interface OutboxStore {
  enqueue(subject: string, envelope: EnvelopeV1): Promise<void>;
  claimDue(limit?: number): Promise<OutboxRecord[]>;
  markSent(msgId: string): Promise<void>;
  markAcked(msgId: string): Promise<void>;
  markFailed(msgId: string, error: string, nextAttemptAt: string): Promise<void>;
  markDlq(msgId: string, error: string): Promise<void>;
  requeueStaleSent?(ackTimeoutMs: number, reason?: string): Promise<number>;
}

interface JsonOutboxState {
  records: OutboxRecord[];
}

export class JsonFileOutboxStore implements OutboxStore {
  constructor(private readonly filePath = ".data/outbox.json") {
    warnIfJsonStoreMayRace(this.filePath);
  }

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
      version: 1,
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
    row.version = (row.version ?? 0) + 1;
    await this.save(state);
  }

  async markAcked(msgId: string): Promise<void> {
    const state = await this.load();
    const row = state.records.find((r) => r.msgId === msgId);
    if (!row) return;
    row.status = "acked";
    row.updatedAt = new Date().toISOString();
    row.version = (row.version ?? 0) + 1;
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
    row.version = (row.version ?? 0) + 1;
    await this.save(state);
  }

  async markDlq(msgId: string, error: string): Promise<void> {
    const state = await this.load();
    const row = state.records.find((r) => r.msgId === msgId);
    if (!row) return;
    row.status = "dlq";
    row.lastError = error;
    row.updatedAt = new Date().toISOString();
    row.version = (row.version ?? 0) + 1;
    await this.save(state);
  }

  async requeueStaleSent(ackTimeoutMs: number, reason = "ack-timeout"): Promise<number> {
    const state = await this.load();
    const now = Date.now();
    let changed = 0;
    for (const row of state.records) {
      if (row.status !== "sent") continue;
      const ageMs = now - new Date(row.updatedAt).getTime();
      if (ageMs < ackTimeoutMs) continue;
      row.status = "failed";
      row.lastError = reason;
      row.nextAttemptAt = new Date(now).toISOString();
      row.updatedAt = new Date(now).toISOString();
      row.version = (row.version ?? 0) + 1;
      changed += 1;
    }
    if (changed > 0) {
      await this.save(state);
    }
    return changed;
  }
}

const ensureDir = (filePath: string): void => {
  const dir = path.dirname(filePath);
  mkdirSync(dir, { recursive: true });
};

export class SQLiteDedupeOutboxStore implements DedupeStore, OutboxStore {
  private readonly db: DatabaseSync;

  constructor(dbPath = ".data/murmur.db") {
    ensureDir(dbPath);
    this.db = new DatabaseSync(dbPath);
    this.db.exec(`
      PRAGMA journal_mode=WAL;
      CREATE TABLE IF NOT EXISTS dedupe_seen (
        consumer_id TEXT NOT NULL,
        msg_id TEXT NOT NULL,
        seen_at TEXT NOT NULL,
        PRIMARY KEY (consumer_id, msg_id)
      );
      CREATE TABLE IF NOT EXISTS outbox (
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
      CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(status, next_attempt_at);
    `);
  }

  async seen(msgId: string, consumerId: string): Promise<boolean> {
    const row = this.db
      .prepare("SELECT 1 FROM dedupe_seen WHERE consumer_id = ? AND msg_id = ? LIMIT 1")
      .get(consumerId, msgId) as { 1: number } | undefined;
    return !!row;
  }

  async markSeen(msgId: string, consumerId: string): Promise<void> {
    this.db
      .prepare(
        "INSERT OR IGNORE INTO dedupe_seen (consumer_id, msg_id, seen_at) VALUES (?, ?, ?)",
      )
      .run(consumerId, msgId, new Date().toISOString());
  }

  async enqueue(subject: string, envelope: EnvelopeV1): Promise<void> {
    const now = new Date().toISOString();
    this.db
      .prepare(
        `INSERT OR IGNORE INTO outbox
        (msg_id, subject, envelope_json, status, attempts, next_attempt_at, created_at, updated_at, version)
        VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, 1)`,
      )
      .run(envelope.msgId, subject, JSON.stringify(envelope), now, now, now);
  }

  async claimDue(limit = 50): Promise<OutboxRecord[]> {
    const rows = this.db
      .prepare(
        `SELECT * FROM outbox
         WHERE status IN ('pending', 'failed') AND next_attempt_at <= ?
         ORDER BY next_attempt_at ASC
         LIMIT ?`,
      )
      .all(new Date().toISOString(), limit) as Array<Record<string, unknown>>;

    return rows.map((row) => this.toOutboxRecord(row));
  }

  async markSent(msgId: string): Promise<void> {
    await this.updateOutboxOptimistic(msgId, (row) => ({
      status: "sent",
      attempts: row.attempts + 1,
      updatedAt: new Date().toISOString(),
    }));
  }

  async markAcked(msgId: string): Promise<void> {
    await this.updateOutboxOptimistic(msgId, () => ({
      status: "acked",
      updatedAt: new Date().toISOString(),
    }));
  }

  async markFailed(msgId: string, error: string, nextAttemptAt: string): Promise<void> {
    await this.updateOutboxOptimistic(msgId, () => ({
      status: "failed",
      lastError: error,
      nextAttemptAt,
      updatedAt: new Date().toISOString(),
    }));
  }

  async markDlq(msgId: string, error: string): Promise<void> {
    await this.updateOutboxOptimistic(msgId, () => ({
      status: "dlq",
      lastError: error,
      updatedAt: new Date().toISOString(),
    }));
  }

  async requeueStaleSent(ackTimeoutMs: number, reason = "ack-timeout"): Promise<number> {
    const threshold = new Date(Date.now() - ackTimeoutMs).toISOString();
    const res = this.db
      .prepare(
        `UPDATE outbox
         SET status = 'failed',
             last_error = ?,
             next_attempt_at = ?,
             updated_at = ?,
             version = version + 1
         WHERE status = 'sent' AND updated_at <= ?`,
      )
      .run(reason, new Date().toISOString(), new Date().toISOString(), threshold);
    return Number(res.changes ?? 0);
  }

  private toOutboxRecord(row: Record<string, unknown>): OutboxRecord {
    return {
      msgId: String(row.msg_id),
      subject: String(row.subject),
      envelope: JSON.parse(String(row.envelope_json)) as EnvelopeV1,
      status: String(row.status) as OutboxStatus,
      attempts: Number(row.attempts),
      nextAttemptAt: String(row.next_attempt_at),
      lastError: row.last_error ? String(row.last_error) : undefined,
      createdAt: String(row.created_at),
      updatedAt: String(row.updated_at),
      version: Number(row.version),
    };
  }

  private getOutboxRow(msgId: string): OutboxRecord | undefined {
    const row = this.db.prepare("SELECT * FROM outbox WHERE msg_id = ?").get(msgId) as Record<string, unknown> | undefined;
    return row ? this.toOutboxRecord(row) : undefined;
  }

  private async updateOutboxOptimistic(
    msgId: string,
    mutate: (current: OutboxRecord) => Partial<OutboxRecord>,
  ): Promise<void> {
    for (let i = 0; i < 3; i += 1) {
      const current = this.getOutboxRow(msgId);
      if (!current) return;

      const patch = mutate(current);
      const nextStatus = patch.status ?? current.status;
      const nextAttempts = patch.attempts ?? current.attempts;
      const nextNextAttemptAt = patch.nextAttemptAt ?? current.nextAttemptAt;
      const nextLastError = patch.lastError ?? current.lastError ?? null;
      const nextUpdatedAt = patch.updatedAt ?? new Date().toISOString();
      const changed = this.db
        .prepare(
          `UPDATE outbox
           SET status = ?, attempts = ?, next_attempt_at = ?, last_error = ?, updated_at = ?, version = version + 1
           WHERE msg_id = ? AND version = ?`,
        )
        .run(
          nextStatus,
          nextAttempts,
          nextNextAttemptAt,
          nextLastError,
          nextUpdatedAt,
          msgId,
          current.version ?? 1,
        );

      if (changed.changes > 0) return;
    }
    throw new Error(`optimistic-lock-failed: ${msgId}`);
  }
}

export interface SqlExecutor {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: T[]; rowCount: number }>;
}

export class PgSqlExecutor implements SqlExecutor {
  private readonly poolPromise: Promise<import("pg").Pool>;

  constructor(private readonly connectionString: string) {
    this.poolPromise = import("pg").then(({ Pool }) => new Pool({ connectionString: this.connectionString }));
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<{ rows: T[]; rowCount: number }> {
    const pool = await this.poolPromise;
    const res = await pool.query(sql, params);
    return { rows: res.rows as T[], rowCount: res.rowCount ?? 0 };
  }

  async close(): Promise<void> {
    const pool = await this.poolPromise;
    await pool.end();
  }
}

export interface LocalMessageRecord {
  id: string;
  conversationId: string;
  msgId: string;
  direction: "inbound" | "outbound";
  sender: string;
  text: string;
  createdAt: string;
  transport?: string;
}

export class SQLiteMessageStore {
  private readonly db: DatabaseSync;

  constructor(dbPath = ".data/murmur.db") {
    ensureDir(dbPath);
    this.db = new DatabaseSync(dbPath);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS local_messages (
        id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL,
        msg_id TEXT NOT NULL,
        direction TEXT NOT NULL,
        sender TEXT NOT NULL,
        text TEXT NOT NULL,
        created_at TEXT NOT NULL,
        transport TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_local_messages_conversation ON local_messages(conversation_id, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_local_messages_text ON local_messages(text);
    `);
  }

  async append(input: Omit<LocalMessageRecord, "id">): Promise<LocalMessageRecord> {
    const row: LocalMessageRecord = { id: randomUUID(), ...input };
    this.db
      .prepare(
        `INSERT INTO local_messages
         (id, conversation_id, msg_id, direction, sender, text, created_at, transport)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        row.id,
        row.conversationId,
        row.msgId,
        row.direction,
        row.sender,
        row.text,
        row.createdAt,
        row.transport ?? null,
      );
    return row;
  }

  async listConversations(limit = 50): Promise<Array<{ conversationId: string; lastMessageAt: string; messageCount: number }>> {
    const rows = this.db
      .prepare(
        `SELECT conversation_id as conversationId, MAX(created_at) as lastMessageAt, COUNT(*) as messageCount
         FROM local_messages
         GROUP BY conversation_id
         ORDER BY lastMessageAt DESC
         LIMIT ?`,
      )
      .all(limit) as Array<{ conversationId: string; lastMessageAt: string; messageCount: number }>;
    return rows;
  }

  async getInboundAfter(conversationId: string, afterTimestamp: string, limit = 10): Promise<LocalMessageRecord[]> {
    const rows = this.db
      .prepare(
        `SELECT
           id,
           conversation_id as conversationId,
           msg_id as msgId,
           direction,
           sender,
           text,
           created_at as createdAt,
           transport
         FROM local_messages
         WHERE conversation_id = ? AND direction = 'inbound' AND created_at > ?
         ORDER BY created_at ASC
         LIMIT ?`,
      )
      .all(conversationId, afterTimestamp, limit) as unknown as LocalMessageRecord[];
    return rows;
  }

  async searchMessages(query: string, limit = 50): Promise<LocalMessageRecord[]> {
    const q = `%${query}%`;
    const rows = this.db
      .prepare(
        `SELECT
           id,
           conversation_id as conversationId,
           msg_id as msgId,
           direction,
           sender,
           text,
           created_at as createdAt,
           transport
         FROM local_messages
         WHERE text LIKE ? OR sender LIKE ? OR conversation_id LIKE ?
         ORDER BY created_at DESC
         LIMIT ?`,
      )
      .all(q, q, q, limit) as unknown as LocalMessageRecord[];
    return rows;
  }
}

export const isEnvelopeV1 = (v: unknown): v is EnvelopeV1 => {
  if (!v || typeof v !== "object") return false;
  const o = v as Record<string, unknown>;
  const hasOptional = (key: keyof EnvelopeV1, type: "string" | "number"): boolean => {
    const value = o[key as string];
    return value === undefined || typeof value === type;
  };

  return (
    o.schemaVersion === "1.0" &&
    typeof o.msgId === "string" && o.msgId.length > 0 &&
    typeof o.conversationId === "string" && o.conversationId.length > 0 &&
    typeof o.senderAgentId === "string" && o.senderAgentId.length > 0 &&
    Array.isArray(o.recipients) && o.recipients.length > 0 && o.recipients.every((r) => typeof r === "string" && r.length > 0) &&
    typeof o.createdAt === "string" && !Number.isNaN(Date.parse(o.createdAt)) &&
    typeof o.payloadCiphertext === "string" && o.payloadCiphertext.length > 0 &&
    typeof o.payloadNonce === "string" && o.payloadNonce.length > 0 &&
    typeof o.signature === "string" && o.signature.length > 0 &&
    hasOptional("ttlSeconds", "number") &&
    hasOptional("traceId", "string") &&
    hasOptional("sequence", "number") &&
    hasOptional("parentMsgId", "string")
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

export const applyJitter = (baseMs: number, jitterRatio = 0.2): number => {
  const ratio = Math.max(0, Math.min(1, jitterRatio));
  const min = Math.max(0, baseMs * (1 - ratio));
  const max = baseMs * (1 + ratio);
  return Math.round(min + Math.random() * (max - min));
};

export interface SecurityPolicy {
  maxPayloadBytes?: number;
  allowedRoutes?: Record<string, string[]>;
}

export const estimateBase64DecodedBytes = (base64: string): number => {
  const normalized = base64.replace(/\s+/g, "");
  if (normalized.length === 0) return 0;
  const padding = (normalized.match(/=+$/)?.[0].length ?? 0);
  return Math.floor((normalized.length * 3) / 4) - padding;
};

export const validateEnvelopePolicy = (envelope: EnvelopeV1, policy?: SecurityPolicy): string[] => {
  if (!policy) return [];
  const violations: string[] = [];

  if (typeof policy.maxPayloadBytes === "number") {
    const size = estimateBase64DecodedBytes(envelope.payloadCiphertext);
    if (size > policy.maxPayloadBytes) {
      violations.push(`payload-too-large:${size}>${policy.maxPayloadBytes}`);
    }
  }

  if (policy.allowedRoutes) {
    const allowed = policy.allowedRoutes[envelope.senderAgentId] ?? [];
    const denied = envelope.recipients.filter((r) => !allowed.includes(r));
    if (denied.length > 0) {
      violations.push(`recipient-not-allowed:${denied.join(",")}`);
    }
  }

  return violations;
};
