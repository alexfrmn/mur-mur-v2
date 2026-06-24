#!/usr/bin/env bash
# Enable/disable/status helper for Phase N channel roster personality binding.
# This edits only daemon agent-config.json files and restarts only Murmur daemons.
# It never restarts the shared NATS broker.
set -euo pipefail

BOEVOY="${MURMUR_RUNTIME_DIR:-/opt/lifecoach/mur-mur-v2}"
JARVIS_CFG="${MURMUR_JARVIS_CONFIG:-$BOEVOY/.data/agent-config.json}"
CODEX_CFG="${MURMUR_CODEX_CONFIG:-$BOEVOY/.data-codex-volt/agent-config.json}"
JARVIS_DB="${MURMUR_JARVIS_ROSTER_DB:-$BOEVOY/.data/channel-roster.db}"
CODEX_DB="${MURMUR_CODEX_ROSTER_DB:-$BOEVOY/.data-codex-volt/channel-roster.db}"
SERVICES="${MURMUR_CHANNEL_ROSTER_SERVICES:-murmur-daemon-codex-volt murmur-daemon-jarvis}"

set_channel_roster() {
  local cfg="$1" enabled="$2" path="$3"
  python3 - "$cfg" "$enabled" "$path" <<'PY'
import json
import os
import shutil
import sys

cfg, enabled, db_path = sys.argv[1], sys.argv[2] == "true", sys.argv[3]
with open(cfg, encoding="utf-8") as f:
    data = json.load(f)
backup = f"{cfg}.bak-channel-roster"
if not os.path.exists(backup):
    shutil.copy2(cfg, backup)
roster = data.setdefault("channelRoster", {})
roster["enabled"] = enabled
roster["path"] = db_path
with open(cfg, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
print(f"  {cfg}: channelRoster.enabled={enabled}, path={db_path}")
PY
}

show_channel_roster() {
  local name="$1" cfg="$2"
  python3 - "$name" "$cfg" <<'PY'
import json
import sys

name, cfg = sys.argv[1], sys.argv[2]
try:
    with open(cfg, encoding="utf-8") as f:
        roster = json.load(f).get("channelRoster", {})
except FileNotFoundError:
    print(f"{name}: missing config {cfg}")
    raise SystemExit(0)
print(f"{name}: enabled={roster.get('enabled')!r} path={roster.get('path')!r}")
PY
}

restart_services() {
  for svc in $SERVICES; do
    systemctl restart "$svc"
    sleep 3
    systemctl is-active --quiet "$svc" || { echo "FAIL: $svc not active"; exit 1; }
    echo "  $svc: active"
  done
}

if [ "$(id -u)" != "0" ]; then
  echo "FAIL: must run as root" >&2
  exit 1
fi

case "${1:-status}" in
  enable)
    set_channel_roster "$CODEX_CFG" true "$CODEX_DB"
    set_channel_roster "$JARVIS_CFG" true "$JARVIS_DB"
    restart_services
    ;;
  disable)
    set_channel_roster "$CODEX_CFG" false "$CODEX_DB"
    set_channel_roster "$JARVIS_CFG" false "$JARVIS_DB"
    restart_services
    ;;
  status)
    show_channel_roster "codex-volt" "$CODEX_CFG"
    show_channel_roster "jarvis" "$JARVIS_CFG"
    for svc in $SERVICES; do
      systemctl is-active "$svc" | sed "s/^/$svc: /"
    done
    ;;
  *)
    echo "usage: $0 {enable|disable|status}" >&2
    exit 2
    ;;
esac
