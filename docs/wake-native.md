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

## Codex CLI - App-Server WS-over-UDS

Codex is woken over the `codex app-server` WebSocket protocol on a Unix-domain
socket (`--listen unix://...` or managed remote-control socket): the daemon acts
as a WebSocket client, sends the app-server `initialize` handshake, and then
issues `turn/start` on the live thread when a Murmur message arrives. This
replaces the old `codex-murmur-watch` polling path.

Codex runtime facts verified against Codex CLI `0.141.0`:

- `codex app-server --help` is present.
- `--listen unix://PATH` and `--remote unix://PATH` are supported.
- `codex remote-control start --json` is idempotent and returns
  `.daemon.socketPath`.
- `codex app-server generate-ts --experimental` exposes JSON-RPC method
  `turn/start` with `TurnStartParams { threadId, input }`.

Configure a Codex peer as an app-server wake target:

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

The daemon connects with a `ws+unix://SOCKET:/` client URL. It first sends:

```json
{
  "id": "init-1",
  "method": "initialize",
  "params": {
    "clientInfo": { "name": "murmur-codex-app-server-wake" },
    "capabilities": { "experimentalApi": true, "requestAttestation": false }
  }
}
```

After the app-server responds, the daemon sends `initialized` and then:

```json
{
  "id": 1,
  "method": "turn/start",
  "params": {
    "threadId": "...",
    "input": [{ "type": "text", "text": "[MURMUR WAKE]...", "text_elements": [] }]
  }
}
```

If the Unix socket or live `threadId` is absent, wake fails loud in daemon logs
instead of silently falling back to polling.

The previous `persistent` tmux backend has been removed from the wake path.
Native wake modes are `stateless` shell hooks and `codex_app_server`.

### Codex Autostart Sequence

> **Diagnostic / opt-in only.** This sequence only covers
> app-server-managed Codex sessions launched with `--remote`. It is not a
> community-grade native wake solution: plain `codex` and desktop-launched Codex
> sessions are not covered because Codex app-server instances are isolated per
> client and currently do not talk to each other. Keep issue #25 open for the
> real product goal. The strategic community-grade path is MCP-channel wake:
> MCP custom notification -> active-session user submission through a supported
> `[mcp_servers]` configuration surface.

Autostart belongs in the Codex launcher wrapper, not in the Murmur daemon. The
launcher owns the live TUI process and is the only layer that can know which
session thread was just attached.

1. Start or attach the managed app-server before launching the TUI:

   ```bash
   codex remote-control start --json
   ```

   Parse `.daemon.socketPath` from stdout. Current Codex `0.141.0` returns a
   shape like:

   ```json
   {
     "status": "connected",
     "daemon": {
       "status": "alreadyRunning",
       "socketPath": "/home/codexworker/.codex/app-server-control/app-server-control.sock"
     }
   }
   ```

2. Launch the interactive session against that socket:

   ```bash
   codex --remote "unix://${CODEX_APP_SERVER_SOCKET}" "$@"
   ```

   For the vault launcher, this belongs in the session startup entrypoint that
   wraps the real Codex process, not in `codexx` if that wrapper is intentionally
   kept as a dumb `cd && exec codex "$@"` launcher.

3. Capture the live `threadId` automatically after the remote session starts.
   Codex writes session JSONL under
   `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-*.jsonl`. The first line is a
   `session_meta` event; use `.payload.id` as the app-server `threadId`. The
   same UUID is embedded in the rollout file name, but the JSONL field is the
   stable capture point.

   Launcher implementation sketch:

   ```bash
   session_file="$(ls -t "$CODEX_HOME"/sessions/*/*/*/rollout-*.jsonl | head -n 1)"
   thread_id="$(head -n 1 "$session_file" | jq -r 'select(.type=="session_meta") | .payload.id')"
   ```

   A production launcher should filter by launch timestamp and current `$PWD`
   (`.payload.cwd`) before accepting the file, so parallel Codex sessions do not
   race the capture.

4. Wire the Codex peer in the agent config before daemon restart:

   ```json
   {
     "wake": {
       "peers": {
         "agent-jarvis": {
           "mode": "codex_app_server",
           "socketPath": "/home/codexworker/.codex/app-server-control/app-server-control.sock",
           "threadId": "019ee8b4-382a-76c3-a266-16056dc6108b"
         }
       }
     }
   }
   ```

   For the Codex worker deployment, the target file is
   `.data-codex-volt/agent-config.json`. Update it atomically: write a temporary
   JSON file, validate it with `jq empty`, then rename over the old config.

5. Restart the Murmur daemon after the config write:

   ```bash
   sudo systemctl restart murmur-daemon
   ```

   If the daemon is run directly instead of systemd, stop the old process and
   start `node scripts/murmur-daemon.mjs` with the same `DATA_DIR` and
   environment. The daemon reads `wake.peers` only at startup.

With those five steps in the launcher, a new Codex session brings up remote
control, captures its own app-server thread, wires Murmur wake routing, and
restarts the daemon without a manual copy/paste step.

## Scoped Channels & Session Affinity

The Codex autostart sequence above documents a real open problem: app-server
instances are **isolated per client**, so a naive wake can fire the wrong
session — or, when an agent has several sessions open, *every* session reacts to
the same message (double-emit) and the daemon spawns a **competing thread**
alongside the one a human is already attending. Native wake alone has no notion
of which session "owns" the conversation.

Scoped Channels closes that gap with a DB-backed **session-ownership lease**. For
an addressed conversation, exactly one session of the addressed agent holds the
lease; only that session emits, and every other session — and the native daemon
wake — stays silent.

**Lease store.** `SessionLeaseStore` lives in its own SQLite file with a separate
WAL from `local_messages` (tables `channel_owner` + `session_presence`):

- `claim_or_skip` — atomic single-statement compare-and-swap
  (`INSERT … ON CONFLICT(conversation_id, member_slot) DO UPDATE … WHERE stale OR
  same_session`, `token = token + 1 RETURNING token`). The caller that gets a
  token owns the channel; everyone else skips.
- `heartbeat` — keeps a held lease alive without bumping the token.
- `isCurrentToken` — a per-turn fencing token, re-checked at outbound, so a
  resurrected/raced session can't emit under a stale claim.
- `registerSession` / `hasLiveInteractiveSession` — the presence registry the
  wake gate consults.
- `preemptPrefix` (optional) — lets a real interactive chat session reclaim a
  channel from a fallback owner.

**Wake becomes a presence-deferring, lease-gated fallback.** `createNativeLeaseGate`
is injected into `WakeMonitor` as `leaseGate`. Before waking, the daemon checks
session presence:

- a **live interactive session** exists for the agent → the daemon wake
  **defers** (it does *not* spawn a competing thread; the attended session
  handles the message);
- **no** live session → the daemon claims the lease and performs the **cold-wake**
  fallback, exactly as native wake does today.

**One claim across every delivery path.** Foreground-push, cold-start, and
in-session MCP-channel delivery all follow the same rule: claim before any
side-effect, fence the outbound by the lease token, suppress non-owners. The
result is live-verified — N delivery sessions for one message resolve to
**exactly one emit**, and the native daemon wake defers to the attended session
instead of spawning a new thread.

**Compatibility.** The whole feature sits behind `MURMUR_SCOPED_CHANNELS`
(default **OFF**). With no lease gate set, `WakeMonitor` behaves exactly as
described in the sections above — scoped channels is purely additive.

## Why not tmux / OpenClaw

- **tmux send-keys** is fragile (races with human typing; `TIOCSTI` disabled on
  Linux 6.2+) and this environment does not use tmux.
- **OpenClaw** as a wake/delivery dependency is non-standard and a community
  minus; native hooks + app-server + Telegram are standard and dependency-free.

Prior art: `ExaDev/agent-comms`, `cocodrino/bridge-harness` (asyncRewake + NATS),
`synadia-ai` NATS Agent Protocol.
