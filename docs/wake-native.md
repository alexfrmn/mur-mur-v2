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

## Why not tmux / OpenClaw

- **tmux send-keys** is fragile (races with human typing; `TIOCSTI` disabled on
  Linux 6.2+) and this environment does not use tmux.
- **OpenClaw** as a wake/delivery dependency is non-standard and a community
  minus; native hooks + app-server + Telegram are standard and dependency-free.

Prior art: `ExaDev/agent-comms`, `cocodrino/bridge-harness` (asyncRewake + NATS),
`synadia-ai` NATS Agent Protocol.
