#!/usr/bin/env bash
# wake-drain-claude.sh - native tmux-free / OpenClaw-free wake for Claude Code agents.
#
# Registered as a Claude Code hook (Stop / PostToolUse / UserPromptSubmit) with
# `asyncRewake: true`. On each invocation it drains NEW inbound Murmur messages
# from the daemon's SQLite store; if any exist it prints them to STDERR and exits 2,
# so Claude Code wraps the output in a <system-reminder> and wakes the idle session.
#
# No tmux send-keys, no OpenClaw bridge, no polling daemon: native Claude Code wake.
# Dedup is cursor-based (last drained rowid), so a message wakes exactly once.
#
# Env:
#   MURMUR_DB         path to the agent daemon DB (default: jarvis store)
#   MURMUR_WAKE_CURSOR file holding the last-drained rowid (per session/agent)
set -uo pipefail

DB="${MURMUR_DB:-/opt/lifecoach/mur-mur-v2/.data/murmur.db}"
CURSOR="${MURMUR_WAKE_CURSOR:-$HOME/.murmur-wake-cursor}"

[ -r "$DB" ] || exit 0

last="$(cat "$CURSOR" 2>/dev/null || printf '0\n')"
case "$last" in ''|*[!0-9]*) last=0 ;; esac

# New inbound (from any peer) since the last drained rowid.
rows="$(sqlite3 "$DB" \
  "SELECT '  rowid='||rowid||' ['||sender||'] '||substr(replace(replace(text,char(10),' '),char(13),' '),1,360) \
   FROM local_messages \
   WHERE direction='inbound' AND rowid > $last \
   ORDER BY rowid;" 2>/dev/null || true)"

[ -z "$rows" ] && exit 0

# Advance cursor to the current max inbound rowid (drain-to-tip, no double-wake).
maxid="$(sqlite3 "$DB" \
  "SELECT COALESCE(MAX(rowid), $last) FROM local_messages WHERE direction='inbound';" \
  2>/dev/null || echo "$last")"
case "$maxid" in ''|*[!0-9]*) maxid="$last" ;; esac
cursor_dir="$(dirname "$CURSOR")"
mkdir -p "$cursor_dir" 2>/dev/null || true
tmp_cursor="${CURSOR}.$$"
if printf '%s\n' "$maxid" > "$tmp_cursor" 2>/dev/null; then
  mv "$tmp_cursor" "$CURSOR" 2>/dev/null || rm -f "$tmp_cursor"
else
  rm -f "$tmp_cursor"
fi

count="$(printf '%s\n' "$rows" | grep -c '^')"

# stderr + exit 2 => Claude Code injects a <system-reminder> and wakes the session.
{
  printf 'Murmur wake: %s new inbound message(s):\n' "$count"
  printf '%s\n' "$rows"
  printf 'Reply via murmur_send or act on them.\n'
} >&2
exit 2
