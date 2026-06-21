#!/usr/bin/env bash
# Boot an isolated local nats-server, run the live A2A integration test, tear down.
# Uses /tmp/nats-server-test (downloaded binary), port 14222 — NOT the prod broker.
set -uo pipefail
PORT="${TEST_NATS_PORT:-14222}"
NATS_BIN="${NATS_BIN:-$(command -v nats-server || echo /tmp/nats-server-test)}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"   # package root

if [ ! -x "$NATS_BIN" ]; then
  echo "SKIP: no nats-server binary ($NATS_BIN). Install nats-server or set NATS_BIN." >&2
  exit 0
fi

"$NATS_BIN" -p "$PORT" -a 127.0.0.1 >/tmp/nats-test.log 2>&1 &
NPID=$!
trap 'kill "$NPID" 2>/dev/null' EXIT

# wait for nats to accept connections
for i in $(seq 1 30); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then exec 3>&- 3<&-; break; fi
  sleep 0.2
done

cd "$HERE"
TEST_NATS_URL="nats://127.0.0.1:$PORT" node --test integration/bridge-a2a.live.mjs
RC=$?
exit $RC
