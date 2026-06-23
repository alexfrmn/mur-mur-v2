#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
CODEX_HOME_DIR="${CODEX_HOME:-${HOME}/.codex}"
CODEX_CONFIG_FILE="${CODEX_CONFIG:-${CODEX_HOME_DIR}/config.toml}"
SERVER_SOURCE="${BASE_DIR}/scripts/murmur-mcp-channel-server.mjs"
SERVER_DEST_DIR="${CODEX_HOME_DIR}/mcp-channel-server"
SERVER_DEST="${SERVER_DEST_DIR}/index.mjs"
LOG_FILE="${CODEX_HOME_DIR}/log/murmur-channel-autostart.log"
DATA_DIR="${MURMUR_CODEX_DATA_DIR:-${BASE_DIR}/.data}"
STORE_PATH="${MURMUR_CODEX_STORE_PATH:-${DATA_DIR}/murmur.db}"
MURMUR_ROOT="${MURMUR_ROOT:-${BASE_DIR}}"
LEASE_DB="${MURMUR_LEASE_DB:-${DATA_DIR}/lease.db}"
SESSION_ID="${CODEX_SESSION_ID:-${CODEX_THREAD_ID:-}}"
THREAD_ID="${CODEX_THREAD_ID:-${CODEX_SESSION_ID:-}}"

log() {
  mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null || true
  printf '[%s] %s\n' "$(date -Is)" "$*" >>"$LOG_FILE" 2>/dev/null || true
}

warn() {
  log "WARN $*"
  if [[ "${CODEX_MURMUR_AUTOSTART_VERBOSE:-0}" == "1" ]]; then
    printf '[codex-murmur-autostart] WARN %s\n' "$*" >&2
  fi
}

info() {
  log "INFO $*"
  if [[ "${CODEX_MURMUR_AUTOSTART_VERBOSE:-0}" == "1" ]]; then
    printf '[codex-murmur-autostart] %s\n' "$*" >&2
  fi
}

render_stanza() {
  cat <<EOF
[mcp_servers.murmur-channel]
command = "node"
args = ["${SERVER_DEST}"]
surface_notifications = true
required = true

[mcp_servers.murmur-channel.env]
DATA_DIR = "${DATA_DIR}"
MURMUR_STORE_PATH = "${STORE_PATH}"
MURMUR_ROOT = "${MURMUR_ROOT}"
MURMUR_LEASE_DB = "${LEASE_DB}"
CODEX_SESSION_ID = "${SESSION_ID}"
CODEX_THREAD_ID = "${THREAD_ID}"
MURMUR_MCP_TO_SESSION = "1"
EOF
}

sync_server() {
  if [[ ! -r "$SERVER_SOURCE" ]]; then
    warn "missing server source: $SERVER_SOURCE"
    return 0
  fi

  mkdir -p "$SERVER_DEST_DIR"
  if [[ ! -f "$SERVER_DEST" ]] || ! cmp -s "$SERVER_SOURCE" "$SERVER_DEST"; then
    install -m 755 "$SERVER_SOURCE" "$SERVER_DEST"
    info "synced MCP channel server to $SERVER_DEST"
  else
    info "MCP channel server up to date"
  fi
}

ensure_config() {
  mkdir -p "$(dirname "$CODEX_CONFIG_FILE")"
  touch "$CODEX_CONFIG_FILE"
  chmod 600 "$CODEX_CONFIG_FILE" 2>/dev/null || true

  local tmp stanza
  tmp="$(mktemp)"
  stanza="$(mktemp)"
  render_stanza >"$stanza"

  awk '
    /^\[mcp_servers\.murmur-channel\]$/ { skip = 1; next }
    /^\[mcp_servers\.murmur-channel\.env\]$/ { skip = 1; next }
    /^\[/ { skip = 0 }
    !skip { print }
  ' "$CODEX_CONFIG_FILE" >"$tmp"

  awk '
    NF {
      while (blank > 0) {
        print ""
        blank--
      }
      print
      next
    }
    {
      blank++
    }
  ' "$tmp" >"${tmp}.trim"
  mv "${tmp}.trim" "$tmp"

  {
    cat "$tmp"
    printf '\n'
    cat "$stanza"
    printf '\n'
  } >"${tmp}.new"

  if ! cmp -s "$CODEX_CONFIG_FILE" "${tmp}.new"; then
    cp "$CODEX_CONFIG_FILE" "${CODEX_CONFIG_FILE}.bak-murmur-channel-autostart-$(date +%Y%m%dT%H%M%SZ)"
    mv "${tmp}.new" "$CODEX_CONFIG_FILE"
    chmod 600 "$CODEX_CONFIG_FILE" 2>/dev/null || true
    info "ensured murmur-channel MCP config in $CODEX_CONFIG_FILE"
  else
    rm -f "${tmp}.new"
    info "murmur-channel MCP config up to date"
  fi

  rm -f "$tmp" "$stanza"
}

main() {
  sync_server
  ensure_config
}

main "$@"
