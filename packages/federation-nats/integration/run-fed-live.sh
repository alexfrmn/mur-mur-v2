#!/usr/bin/env bash
# Boot an isolated local nats-server with a 2-org accounts config generated from the
# federation account contract, run the LIVE federation interop test, tear down.
# Hard-fails if the broker does not come up (no silent skip-to-green).
set -euo pipefail
PORT="${FED_NATS_PORT:-14333}"
NATS_BIN="${NATS_BIN:-$(command -v nats-server || echo /tmp/nats-server-test)}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"   # package root

if [ ! -x "$NATS_BIN" ]; then
  echo "SKIP: no nats-server binary ($NATS_BIN). Install nats-server or set NATS_BIN." >&2
  exit 0
fi

cd "$HERE"
CONF="$(mktemp "${TMPDIR:-/tmp}/fed-accounts.XXXXXX.conf")"
LOG="$(mktemp "${TMPDIR:-/tmp}/fed-nats.XXXXXX.log")"
FED_NATS_PORT="$PORT" node integration/gen-accounts-conf.mjs > "$CONF"

"$NATS_BIN" -c "$CONF" >"$LOG" 2>&1 &
NPID=$!
cleanup() { kill "$NPID" 2>/dev/null || true; rm -f "$CONF" "$LOG"; }
trap cleanup EXIT

reachable=""
for _ in $(seq 1 30); do
  if ! kill -0 "$NPID" 2>/dev/null; then
    echo "FAIL: nats-server exited during boot. Conf:" >&2; cat "$CONF" >&2; echo "Log:" >&2; cat "$LOG" >&2; exit 1
  fi
  if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then exec 3>&- 3<&-; reachable=1; break; fi
  sleep 0.2
done
if [ -z "$reachable" ]; then
  echo "FAIL: nats-server port $PORT not reachable. Log:" >&2; cat "$LOG" >&2; exit 1
fi

FED_NATS_PORT="$PORT" node --test integration/federation.live.mjs
