#!/usr/bin/env bash
# Boot an isolated local nats-server with a RESTRICTED federation accounts config
# (restrictUserPermissions=true), run the acceptance #2/#3 permission-boundary test,
# tear down. Hard-fails if the broker does not boot.
set -euo pipefail
PORT="${FED_NATS_PORT:-14601}"
NATS_BIN="${NATS_BIN:-$(command -v nats-server || echo /tmp/nats-server-test)}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
[ -x "$NATS_BIN" ] || { echo "SKIP: no nats-server ($NATS_BIN)"; exit 0; }
cd "$HERE"

CONF="$(mktemp "${TMPDIR:-/tmp}/fed-perm.XXXXXX.conf")"
LOG="$(mktemp "${TMPDIR:-/tmp}/fed-perm.XXXXXX.log")"
FED_NATS_PORT="$PORT" node -e '
const { renderFederationNatsAccountsConfig } = require("./dist/src/index.js");
process.stdout.write(renderFederationNatsAccountsConfig({ orgs: ["aimindset","partner"], port: process.env.FED_NATS_PORT, restrictUserPermissions: true }));
' > "$CONF"

"$NATS_BIN" -c "$CONF" >"$LOG" 2>&1 &
NPID=$!
cleanup(){ kill "$NPID" 2>/dev/null || true; rm -f "$CONF" "$LOG"; }
trap cleanup EXIT

reachable=""
for _ in $(seq 1 30); do
  kill -0 "$NPID" 2>/dev/null || { echo "FAIL: nats died on boot. Conf:"; cat "$CONF"; echo "Log:"; cat "$LOG"; exit 1; }
  if (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; then exec 3>&- 3<&-; reachable=1; break; fi
  sleep 0.2
done
[ -n "$reachable" ] || { echo "FAIL: port $PORT not reachable. Log:"; cat "$LOG"; exit 1; }

FED_NATS_PORT="$PORT" node --test integration/perm-boundary.mjs
