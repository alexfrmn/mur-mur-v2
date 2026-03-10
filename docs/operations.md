# Murmur V2 — Operations Guide

## Architecture Overview

```
MAIN (5.181.3.139)                    Agent-HQ (89.185.80.152)
┌────────────────────┐                ┌─────────────────────────┐
│ murmur-daemon      │◄──── NATS ────►│ murmur-codex2.service   │
│ (agent-jarvis)     │   port 4222    │ murmur-glm.service      │
│                    │                │ murmur-haiku.service     │
│ openclaw-gateway   │                │ murmur-glm-worker.service│
│ (localhost:18789)  │                │                          │
└────────────────────┘                │ openclaw-gateway          │
                                      │ (localhost:18789)         │
                                      └─────────────────────────┘
```

## Message Flow

1. JARVIS sends task via `test-send.mjs` or `delegate-task.mjs`
2. `murmur-daemon` encrypts → NATS → agent-hq daemon decrypts
3. Daemon enqueues to `openclaw_bridge_queue` (SQLite)
4. `flushOpenClawBridgeQueue` dispatches via `openclaw agent` CLI
5. OpenClaw processes, returns JSON with response
6. If `replyViaMurmur: true` → reply encrypted → NATS → MAIN inbox
7. `notify_queue` sends Telegram notification

## Queue Management

### Check queue state
```bash
# On any server with murmur DB
sqlite3 .data/murmur.db "SELECT status, COUNT(*) FROM openclaw_bridge_queue GROUP BY status;"
sqlite3 .data/murmur.db "SELECT status, COUNT(*) FROM notify_queue GROUP BY status;"
```

### Kill stuck items (dead-letter manually)
```bash
sqlite3 .data/murmur.db "UPDATE openclaw_bridge_queue SET status='dead', updated_at=datetime('now') WHERE status='failed';"
sqlite3 .data/murmur.db "UPDATE notify_queue SET status='dead', updated_at=datetime('now') WHERE status='failed';"
```

### Agent-HQ per-agent DBs
```bash
sqlite3 /opt/mur-mur-v2/.data/codex2/murmur.db "..."
sqlite3 /opt/mur-mur-v2/.data/glm/murmur.db "..."
sqlite3 /opt/mur-mur-v2/.data/haiku/murmur.db "..."
sqlite3 /opt/mur-mur-v2/.data/glm-worker/murmur.db "..."
```

## Retry & Dead-Letter Policy

### notify-router.mjs (commit 4324488)
- Telegram messages truncated at 4000 chars (margin from 4096 limit)
- HTTP 4xx → immediate dead-letter (permanent failure)
- Max 10 attempts → dead-letter
- Backoff: `min(60s, 2s × attempt)`

### openclaw-bridge.mjs (commit 109f27f)
- Max 3 attempts → dead-letter
- Dedup key: `{from}:{conversationId}:{msgId}:openclaw:{channel}`
- Dispatch timeout: 120 seconds
- Backoff: `min(60s, 2s × attempt)`

## on-receive-openclaw.mjs (commit aaec353)

Helper script for OpenClaw dispatch. Previous version used Gateway `/tools/invoke`
with `cron` tool (removed in OpenClaw 2026.2.17). Rewritten to use `openclaw agent` CLI.

**When used:** Only when `helperScript` is set in `notify.openclaw` config.
Without `helperScript`, `openclaw-bridge.mjs` calls CLI natively (same mechanism).

## Service Management

### MAIN
```bash
systemctl restart murmur-daemon
journalctl -u murmur-daemon -f
```

### Agent-HQ
```bash
systemctl restart murmur-codex2 murmur-glm murmur-haiku murmur-glm-worker
journalctl -u murmur-codex2 -f  # or murmur-glm, murmur-haiku, murmur-glm-worker
```

## Common Issues

### Infinite retry loop
**Symptom:** Queue item with attempts >10, retrying every ~2 minutes.
**Cause:** Old code (pre-commit 4324488) without dead-letter logic.
**Fix:** `git pull` + restart daemons + dead-letter stuck items manually.

### NATS silent disconnect
**Symptom:** Daemons show `active` but no messages flow.
**Cause:** iptables blocks port 4222 after TCP reconnect (fixed 2026-03-06).
**Verify:** `journalctl -u murmur-<agent> | grep "NATS connected"`

### Telegram "message too long"
**Symptom:** notify_queue retries with 400 error.
**Cause:** Message >4096 chars. Fixed in commit 4324488 with auto-truncation.

### OpenClaw dispatch fails
**Symptom:** openclaw_bridge_queue items fail repeatedly.
**Check:** `openclaw agent --message "ping" --json --agent <name>` on the server.
**Common causes:** Gateway not running, agent not configured, auth error.
