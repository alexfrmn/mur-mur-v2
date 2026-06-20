# Native Wake-On-Message (tmux-Free, OpenClaw-Free)

Murmur wakes agents on new messages using each CLI's **native** mechanism: no
`tmux send-keys`, no OpenClaw bridge, no polling daemon. Human notification stays
on the Telegram bot (`notify_queue`).

## Claude Code - `asyncRewake` Hook

`scripts/wake-drain-claude.sh` reads new inbound messages from the daemon's
SQLite store (`local_messages`). Run as a Claude Code hook with
`asyncRewake: true`: a non-empty result prints to stderr and exits `2`, and
Claude Code wraps the output in a `<system-reminder>` and wakes the idle session.
A cursor file (`MURMUR_WAKE_CURSOR`) makes each message wake exactly once.

Register it out-of-the-box in the agent's `.claude/settings.json`:

```json
{
  "hooks": {
    "Stop": [{ "hooks": [{ "type": "command", "command": "/path/to/scripts/wake-drain-claude.sh", "asyncRewake": true }] }],
    "PostToolUse": [{ "hooks": [{ "type": "command", "command": "/path/to/scripts/wake-drain-claude.sh", "asyncRewake": true }] }],
    "UserPromptSubmit": [{ "hooks": [{ "type": "command", "command": "/path/to/scripts/wake-drain-claude.sh", "asyncRewake": true }] }]
  }
}
```

Env:

- `MURMUR_DB`: daemon SQLite store path.
- `MURMUR_WAKE_CURSOR`: per-session cursor file.

Drain semantics:

- Only `local_messages.direction='inbound'` rows are surfaced.
- The cursor advances to the current max inbound `rowid` after a non-empty
  drain, so repeated hook invocations do not double-wake the same message.
- Cursor writes use a temporary file plus rename where the filesystem allows it.

## Codex CLI - App-Server UDS

Codex is woken over the `codex app-server` Unix-socket transport
(`--listen unix://…`): a NATS subscriber acts as a JSON-RPC client and issues
`turn/start` on the live thread when a Murmur message arrives. Replaces the old
`codex-murmur-watch` polling.

Codex runtime facts verified against Codex CLI `0.141.0`:

- `codex app-server --help` is present.
- `--listen unix://PATH` is supported.
- `codex app-server generate-ts --experimental` exposes JSON-RPC method
  `turn/start` with `TurnStartParams { threadId, input }`.

Configure a Codex peer as a persistent app-server wake target:

```json
{
  "wake": {
    "peers": {
      "agent-jarvis": {
        "mode": "codex_app_server",
        "socketPath": "/home/codexworker/.codex/app-server.sock",
        "threadId": "thread-id-of-live-codex-session"
      }
    }
  }
}
```

The daemon sends:

```json
{
  "jsonrpc": "2.0",
  "method": "turn/start",
  "params": {
    "threadId": "...",
    "input": [{ "type": "text", "text": "[MURMUR WAKE]...", "text_elements": [] }]
  }
}
```

If the Unix socket or live `threadId` is absent, wake fails loud in daemon logs
instead of silently falling back to polling.

## Why not tmux / OpenClaw

- **tmux send-keys** is fragile (races with human typing; `TIOCSTI` disabled on
  Linux 6.2+) and this environment does not use tmux.
- **OpenClaw** as a wake/delivery dependency is non-standard and a community
  minus; native hooks + app-server + Telegram are standard and dependency-free.

Prior art: `ExaDev/agent-comms`, `cocodrino/bridge-harness` (asyncRewake + NATS),
`synadia-ai` NATS Agent Protocol.
