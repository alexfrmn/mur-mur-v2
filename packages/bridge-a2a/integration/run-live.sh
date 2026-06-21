#!/usr/bin/env bash
# Boot an isolated local nats-server, run the live A2A integration test, tear down.
# Uses a nats-server on PATH (or the downloaded /tmp/nats-server-test), port 14222 —
# NOT the prod broker.
#
# Unlike a bare `node --test`, this wrapper HARD-FAILS if the broker does not come up,
# so `test:live` can NEVER report green without exercising the real NATS/HTTP/crypto
# path. (CODEX review of PR #41: a fixed /tmp log owned by another user + no `set -e`
# let a failed broker boot fall through to the JS self-skip and still exit 0.)
set -euo pipefail
PORT="${TEST_NATS_PORT:-14222}"
NATS_BIN="${NATS_BIN:-$(command -v nats-server || echo /tmp/nats-server-test)}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"   # package root

# Genuine no-infra skip (no binary at all) — distinct from a FAILED boot below.
if [ ! -x "$NATS_BIN" ]; then
  echo "SKIP: no nats-server binary ($NATS_BIN). Install nats-server or set NATS_BIN to run the live proof." >&2
  exit 0
fi

LOG="$(mktemp "${TMPDIR:-/tmp}/nats-live.XXXXXX.log")"
"$NATS_BIN" -p "$PORT" -a 127.0.0.1 >"$LOG" 2>&1 &
NPID=$!
cleanup() { kill "$NPID" 2>/dev/null || true; rm -f "$LOG"; }
trap cleanup EXIT

# Once we have selected + started a broker, a failed boot is a HARD FAILURE, never a skip.
reachable=""
for _ in $(seq 1 30); do
  if ! kill -0 "$NPID" 2>/dev/null; then
    echo "FAIL: nats-server exited during boot. Log:" >&2; cat "$LOG" >&2; exit 1
  fi
  if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then exec 3>&- 3<&-; reachable=1; break; fi
  sleep 0.2
done
if [ -z "$reachable" ]; then
  echo "FAIL: nats-server port $PORT not reachable after wait. Log:" >&2; cat "$LOG" >&2; exit 1
fi

cd "$HERE"
# `set -e` propagates a real test failure as a non-zero exit.
TEST_NATS_URL="nats://127.0.0.1:$PORT" node --test integration/bridge-a2a.live.mjs
