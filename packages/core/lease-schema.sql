-- Murmur Scoped Channels — session-ownership lease contract (spike #78/#79/#80)
-- Canonical SQL. Cross-runtime: Node (scripts/lease.mjs) + Python (codex coldstart, #83).
-- Lives in its OWN SQLite file (separate WAL) to avoid contention with local_messages.
--
-- Invariants (S0.0, issue #78):
--   * owner scope = (conversation_id, member_slot)
--   * claim is acquired atomically BEFORE any side-effect (wake/inject/push/spawn/send)
--   * token is monotonic per channel; bumps on every successful claim (per-turn fence)
--   * heartbeat keeps the lease alive WITHOUT bumping token
--   * a stale owner (now - heartbeat_at > ttl_ms) can be taken over
--   * outbound must re-check token == current (fence) or be suppressed

PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=10000;

CREATE TABLE IF NOT EXISTS channel_owner (
  conversation_id   TEXT    NOT NULL,
  member_slot       TEXT    NOT NULL,   -- recipient agent / persona / session-class
  owner_session_id  TEXT    NOT NULL,
  token             INTEGER NOT NULL,   -- monotonic; fencing token
  epoch             INTEGER NOT NULL,   -- bumps on ownership change
  heartbeat_at      INTEGER NOT NULL,   -- ms epoch
  ttl_ms            INTEGER NOT NULL,
  PRIMARY KEY (conversation_id, member_slot)
);

CREATE TABLE IF NOT EXISTS session_presence (
  session_id    TEXT    PRIMARY KEY,
  agent_id      TEXT    NOT NULL,
  thread_id     TEXT,
  pid           INTEGER,
  mode          TEXT    NOT NULL,       -- foreground|native|coldstart|mcp-channel
  heartbeat_at  INTEGER NOT NULL,       -- ms epoch
  started_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_presence_agent ON session_presence(agent_id, mode);

-- claim_or_skip (atomic CAS). Positional params:
--   1=conversation_id  2=member_slot  3=owner_session_id  4=now_ms  5=ttl_ms
-- Win  => RETURNING row whose owner_session_id == caller (fresh insert, same-session re-claim,
--         or stale takeover). token is the caller's fencing token for this turn.
-- Lose => no row returned (a live, non-stale owner holds the channel).
--
-- INSERT INTO channel_owner
--   (conversation_id, member_slot, owner_session_id, token, epoch, heartbeat_at, ttl_ms)
--   VALUES (?, ?, ?, 1, 1, ?, ?)
-- ON CONFLICT(conversation_id, member_slot) DO UPDATE SET
--   owner_session_id = excluded.owner_session_id,
--   token            = channel_owner.token + 1,
--   epoch            = channel_owner.epoch + (CASE WHEN channel_owner.owner_session_id <> excluded.owner_session_id THEN 1 ELSE 0 END),
--   heartbeat_at     = excluded.heartbeat_at,
--   ttl_ms           = excluded.ttl_ms
-- WHERE channel_owner.owner_session_id = excluded.owner_session_id
--    OR (excluded.heartbeat_at - channel_owner.heartbeat_at) > channel_owner.ttl_ms
-- RETURNING owner_session_id AS ownerSessionId, token, epoch;
