#!/usr/bin/env bash
# File-level production deploy for the live /opt/lifecoach/mur-mur-v2 tree.
#
# The production tree intentionally keeps local state, secrets, node_modules, and
# site glue outside a normal git checkout. This script clones main, builds dist,
# runs the non-network regression gates, then copies only an audited allowlist of
# runtime files. It never restarts the shared NATS broker.
set -euo pipefail

DST="${MURMUR_DEPLOY_DST:-/opt/lifecoach/mur-mur-v2}"
REPO="${MURMUR_DEPLOY_REPO:-https://github.com/alexfrmn/mur-mur-v2.git}"
REF="${MURMUR_DEPLOY_REF:-main}"
RUN_TESTS="${MURMUR_DEPLOY_TESTS:-1}"
RESTART_SERVICES="${MURMUR_DEPLOY_RESTART:-1}"
RESTART_ORDER="${MURMUR_DEPLOY_RESTART_ORDER:-murmur-daemon-codex-volt murmur-daemon-jarvis}"

FILES=(
  "package.json"
  "package-lock.json"
  "tsconfig.json"
  "packages/core/package.json"
  "packages/core/tsconfig.json"
  "packages/core/src/index.ts"
  "packages/core/src/channel.ts"
  "packages/core/src/discovery.ts"
  "packages/core/src/lease.ts"
  "packages/core/dist/src/index.js"
  "packages/core/dist/src/index.d.ts"
  "packages/core/dist/src/channel.js"
  "packages/core/dist/src/channel.d.ts"
  "packages/core/dist/src/discovery.js"
  "packages/core/dist/src/discovery.d.ts"
  "packages/core/dist/src/lease.js"
  "packages/core/dist/src/lease.d.ts"
  "packages/broker-nats/package.json"
  "packages/broker-nats/tsconfig.json"
  "packages/broker-nats/src/index.ts"
  "packages/broker-nats/dist/src/index.js"
  "packages/broker-nats/dist/src/index.d.ts"
  "packages/mcp-server/package.json"
  "packages/mcp-server/tsconfig.json"
  "packages/mcp-server/src/index.ts"
  "packages/mcp-server/src/request-reply.ts"
  "packages/mcp-server/dist/src/index.js"
  "packages/mcp-server/dist/src/index.d.ts"
  "packages/mcp-server/dist/src/request-reply.js"
  "packages/mcp-server/dist/src/request-reply.d.ts"
  "scripts/murmur-daemon.mjs"
  "scripts/murmur-jetstream-advisory.mjs"
  "scripts/codex-app-server-wake.mjs"
  "scripts/wake-monitor.mjs"
  "scripts/prometheus-exporter.mjs"
)

require_root() {
  if [ "$(id -u)" != "0" ]; then
    echo "FAIL: must run as root" >&2
    exit 1
  fi
}

copy_one() {
  local src="$1" dst="$2" backup_dir="$3"
  if [ ! -f "$src" ]; then
    echo "FAIL: missing built source file: $src" >&2
    exit 1
  fi
  mkdir -p "$(dirname "$dst")" "$(dirname "$backup_dir/$dst")"
  if [ -f "$dst" ]; then
    cp -a "$dst" "$backup_dir/$dst"
  fi
  install -o root -g root -m 0644 "$src" "$dst"
  echo "  deployed: ${dst#$DST/}"
}

health() {
  systemctl show "$1" -p ActiveState,SubState,MainPID,NRestarts --value | paste -sd' '
}

require_root
if [ ! -d "$DST" ]; then
  echo "FAIL: destination does not exist: $DST" >&2
  exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "== fetch =="
git clone --quiet --depth 30 --branch "$REF" "$REPO" "$tmp/repo"
cd "$tmp/repo"
echo "  source HEAD: $(git rev-parse HEAD)"

echo "== install + build =="
npm ci
npm run build

if [ "$RUN_TESTS" = "1" ]; then
  echo "== tests =="
  npm run test:unit
  npm run test:core
fi

echo "== sanity gates =="
test -f packages/core/dist/src/channel.js || { echo "FAIL: core channel dist missing"; exit 1; }
grep -q "ChannelRosterStore" packages/core/dist/src/channel.js || { echo "FAIL: core dist missing ChannelRosterStore"; exit 1; }
grep -q "./channel.js" packages/core/dist/src/index.js || { echo "FAIL: core index missing channel export"; exit 1; }
grep -q "max_deliver" packages/broker-nats/dist/src/index.js || { echo "FAIL: broker missing JetStream max_deliver"; exit 1; }
grep -q "ChannelRosterStore" scripts/murmur-daemon.mjs || { echo "FAIL: daemon missing channel roster wiring"; exit 1; }
grep -q "buildChannelThreadStartBinding" scripts/codex-app-server-wake.mjs || { echo "FAIL: wake script missing N3 binding"; exit 1; }
if grep -q "payload\\??\\.threadStartBinding\\|payload\\[.*threadStartBinding" scripts/codex-app-server-wake.mjs; then
  echo "FAIL: wake script must not trust remote payload threadStartBinding" >&2
  exit 1
fi

echo "== backup + copy allowlist =="
ts="$(date -u +%Y%m%dT%H%M%SZ)"
backup_dir="/opt/lifecoach/backups/murmur-file-deploy/$ts"
mkdir -p "$backup_dir"
printf '%s\n' "$(git rev-parse HEAD)" > "$backup_dir/source-head.txt"
for file in "${FILES[@]}"; do
  copy_one "$tmp/repo/$file" "$DST/$file" "$backup_dir"
done

echo "== runtime import smoke =="
(cd "$DST" && node --input-type=module -e "import { ChannelRosterStore, buildChannelThreadStartBinding } from '@murmurv2/core'; if (!ChannelRosterStore || !buildChannelThreadStartBinding) process.exit(1); console.log('  core channel exports ok');")

if [ "$RESTART_SERVICES" = "1" ]; then
  echo "== restart daemons (shared NATS untouched) =="
  for svc in $RESTART_ORDER; do
    systemctl restart "$svc"
    sleep 4
    echo "  $svc: $(health "$svc")"
    systemctl is-active --quiet "$svc" || { echo "FAIL: $svc not active after restart"; exit 1; }
  done
else
  echo "== restart skipped (MURMUR_DEPLOY_RESTART=0) =="
fi

echo "== DONE: production file deploy complete =="
