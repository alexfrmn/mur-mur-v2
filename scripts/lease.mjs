// Murmur Scoped Channels — session-ownership lease (spike #80, S0.2).
// Atomic CAS claim + heartbeat + per-turn fencing token over a dedicated SQLite file
// (separate WAL from local_messages, per review). Contract: packages/core/lease-schema.sql.
// Cross-runtime sibling: a Python wrapper over the SAME SQL is built by codex coldstart (#83).
import { DatabaseSync } from "node:sqlite";
import { dirname } from "node:path";
import { mkdirSync } from "node:fs";

const DDL = `
  PRAGMA journal_mode=WAL;
  PRAGMA busy_timeout=10000;
  CREATE TABLE IF NOT EXISTS channel_owner (
    conversation_id   TEXT    NOT NULL,
    member_slot       TEXT    NOT NULL,
    owner_session_id  TEXT    NOT NULL,
    token             INTEGER NOT NULL,
    epoch             INTEGER NOT NULL,
    heartbeat_at      INTEGER NOT NULL,
    ttl_ms            INTEGER NOT NULL,
    PRIMARY KEY (conversation_id, member_slot)
  );
  CREATE TABLE IF NOT EXISTS session_presence (
    session_id    TEXT    PRIMARY KEY,
    agent_id      TEXT    NOT NULL,
    thread_id     TEXT,
    pid           INTEGER,
    mode          TEXT    NOT NULL,
    heartbeat_at  INTEGER NOT NULL,
    started_at    INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_session_presence_agent ON session_presence(agent_id, mode);
`;

// Single-statement atomic compare-and-swap. WHERE on DO UPDATE makes a live, non-stale
// foreign owner a no-op (no RETURNING row) — that is the "skip" branch.
// The optional `extraWhere` lets a real session preempt a low-priority fallback owner
// (e.g. a foreground chat preempting a `native:` cold-wake owner) even when not stale.
const buildClaimSql = (extraWhere) => `
  INSERT INTO channel_owner
    (conversation_id, member_slot, owner_session_id, token, epoch, heartbeat_at, ttl_ms)
    VALUES (?, ?, ?, 1, 1, ?, ?)
  ON CONFLICT(conversation_id, member_slot) DO UPDATE SET
    owner_session_id = excluded.owner_session_id,
    token            = channel_owner.token + 1,
    epoch            = channel_owner.epoch + (CASE WHEN channel_owner.owner_session_id <> excluded.owner_session_id THEN 1 ELSE 0 END),
    heartbeat_at     = excluded.heartbeat_at,
    ttl_ms           = excluded.ttl_ms
  WHERE channel_owner.owner_session_id = excluded.owner_session_id
     OR (excluded.heartbeat_at - channel_owner.heartbeat_at) > channel_owner.ttl_ms${extraWhere}
  RETURNING owner_session_id AS ownerSessionId, token, epoch
`;

const CLAIM_SQL = buildClaimSql("");
const CLAIM_PREEMPT_SQL = buildClaimSql("\n     OR channel_owner.owner_session_id LIKE ?");

export class SessionLeaseStore {
  constructor(dbPath = ".data/lease.db") {
    mkdirSync(dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec(DDL);
    this._claim = this.db.prepare(CLAIM_SQL);
    this._claimPreempt = this.db.prepare(CLAIM_PREEMPT_SQL);
  }

  // S0.0 session registry. Upsert a live session's presence row.
  registerSession({ sessionId, agentId, threadId = null, pid = null, mode, now = Date.now() }) {
    this.db
      .prepare(
        `INSERT INTO session_presence (session_id, agent_id, thread_id, pid, mode, heartbeat_at, started_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(session_id) DO UPDATE SET
           thread_id = excluded.thread_id, pid = excluded.pid,
           mode = excluded.mode, heartbeat_at = excluded.heartbeat_at`,
      )
      .run(sessionId, agentId, threadId, pid, mode, now, now);
  }

  sessionHeartbeat(sessionId, now = Date.now()) {
    return this.db
      .prepare(`UPDATE session_presence SET heartbeat_at = ? WHERE session_id = ?`)
      .run(now, sessionId).changes;
  }

  // Atomic claim. Call BEFORE any side-effect. Returns { won, token, ownerSessionId }.
  // `preemptPrefix` (optional): take over a current owner whose session id starts with it,
  //   even when that owner is still live — used so real chat sessions beat `native:` fallbacks.
  claimOrSkip(conversationId, memberSlot, sessionId, ttlMs, now = Date.now(), preemptPrefix = null) {
    const row = preemptPrefix
      ? this._claimPreempt.get(conversationId, memberSlot, sessionId, now, ttlMs, `${preemptPrefix}%`)
      : this._claim.get(conversationId, memberSlot, sessionId, now, ttlMs);
    if (row && row.ownerSessionId === sessionId) {
      return { won: true, token: Number(row.token), ownerSessionId: sessionId };
    }
    const cur = this.getOwner(conversationId, memberSlot);
    return { won: false, token: cur?.token ?? null, ownerSessionId: cur?.ownerSessionId ?? null };
  }

  // Keep the lease alive without bumping the token. Returns 0 if ownership was lost.
  heartbeat(conversationId, memberSlot, sessionId, now = Date.now()) {
    return this.db
      .prepare(
        `UPDATE channel_owner SET heartbeat_at = ?
         WHERE conversation_id = ? AND member_slot = ? AND owner_session_id = ?`,
      )
      .run(now, conversationId, memberSlot, sessionId).changes;
  }

  // Outbound fence: is `token` still the current owner token for this channel?
  isCurrentToken(conversationId, memberSlot, token) {
    const row = this.db
      .prepare(`SELECT token FROM channel_owner WHERE conversation_id = ? AND member_slot = ?`)
      .get(conversationId, memberSlot);
    return !!row && Number(row.token) === token;
  }

  getOwner(conversationId, memberSlot) {
    const row = this.db
      .prepare(
        `SELECT owner_session_id AS ownerSessionId, token, epoch, heartbeat_at AS heartbeatAt, ttl_ms AS ttlMs
         FROM channel_owner WHERE conversation_id = ? AND member_slot = ?`,
      )
      .get(conversationId, memberSlot);
    if (!row) return null;
    return { ownerSessionId: row.ownerSessionId, token: Number(row.token), epoch: Number(row.epoch), heartbeatAt: Number(row.heartbeatAt), ttlMs: Number(row.ttlMs) };
  }

  release(conversationId, memberSlot, sessionId) {
    return this.db
      .prepare(
        `DELETE FROM channel_owner WHERE conversation_id = ? AND member_slot = ? AND owner_session_id = ?`,
      )
      .run(conversationId, memberSlot, sessionId).changes;
  }

  close() {
    this.db.close();
  }
}

// Native-daemon wake is a FALLBACK owner (#82): it must NOT spawn a competing thread when a
// live chat session already owns the channel. Returns a leaseGate(payload, peer) for WakeMonitor:
//   - claims as `native:<agentId>` (no preempt);
//   - allow=true only if it wins (no live owner, or stale, or it already owns);
//   - allow=false when a live non-native session holds the channel -> native wake is muted.
// A real chat session (foreground/mcp, #83) claims with preemptPrefix="native:" so it always
// takes the channel back from this fallback.
export function createNativeLeaseGate({ store, agentId, ttlMs = 20000, memberSlot, now = () => Date.now(), log = () => {} }) {
  const sessionId = `native:${agentId}`;
  const slot = memberSlot || agentId;
  return async (payload) => {
    const conversationId = payload?.conversationId;
    if (!conversationId) return { allow: true, token: null, reason: "no-conversation" };
    const res = store.claimOrSkip(conversationId, slot, sessionId, ttlMs, now());
    if (res.won) {
      log("info", "native lease claimed (fallback wake)", { conversationId, token: res.token, ownerSessionId: sessionId });
      return { allow: true, token: res.token, ownerSessionId: sessionId };
    }
    log("info", "native lease skip (live owner holds channel)", { conversationId, ownerSessionId: res.ownerSessionId });
    return { allow: false, token: null, ownerSessionId: res.ownerSessionId, reason: "live-owner" };
  };
}
