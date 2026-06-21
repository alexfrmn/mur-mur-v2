#!/usr/bin/env bash
# Real-mesh proof: org-per-server topology connected by a NATS leaf node, using the
# #43 renderFederationNatsAccountsConfig output for the hub accounts. Validates that
# cross-org fed.* delivery survives a real leaf link with account isolation
# (acceptance check #1 from docs/federation-nats-contract.md). HARD-FAILS if a broker
# does not boot. NOT the prod deploy (CODEX's half) — the acceptance smoke for it.
set -euo pipefail
NATS_BIN="${NATS_BIN:-$(command -v nats-server || echo /tmp/nats-server-test)}"
HUB_PORT=14501; LEAF_LISTEN=14511; LEAF_PORT=14502
HERE="$(cd "$(dirname "$0")/.." && pwd)"
[ -x "$NATS_BIN" ] || { echo "SKIP: no nats-server ($NATS_BIN)"; exit 0; }
cd "$HERE"

HUBCONF="$(mktemp /tmp/rmhub.XXXXXX.conf)"; LEAFCONF="$(mktemp /tmp/rmleaf.XXXXXX.conf)"
HUBLOG="$(mktemp /tmp/rmhub.XXXXXX.log)"; LEAFLOG="$(mktemp /tmp/rmleaf.XXXXXX.log)"

# Hub: federation accounts (from #43 renderer) + a leafnode listener.
FED_NATS_PORT="$HUB_PORT" node -e '
const { renderFederationNatsAccountsConfig } = require("./dist/src/index.js");
process.stdout.write(renderFederationNatsAccountsConfig({ orgs: ["aimindset","partner"], port: process.env.FED_NATS_PORT }));
' > "$HUBCONF"
printf '\nleafnodes { port: %s }\n' "$LEAF_LISTEN" >> "$HUBCONF"

# Leaf (partner edge): a local account whose leaf remote authenticates into the hub's
# ORG_PARTNER account (as the renderer-default partner user) -> edge clients ride that.
cat > "$LEAFCONF" <<EOF
port: $LEAF_PORT
leafnodes {
  remotes: [
    { urls: ["nats://partner:pw_partner@127.0.0.1:$LEAF_LISTEN"], account: "PARTNER_EDGE" }
  ]
}
accounts {
  PARTNER_EDGE { users: [{ user: "edge", password: "pw_edge" }] }
}
EOF

"$NATS_BIN" -c "$HUBCONF"  >"$HUBLOG"  2>&1 & HPID=$!
"$NATS_BIN" -c "$LEAFCONF" >"$LEAFLOG" 2>&1 & LPID=$!
cleanup(){ kill "$HPID" "$LPID" 2>/dev/null || true; rm -f "$HUBCONF" "$LEAFCONF" "$HUBLOG" "$LEAFLOG"; }
trap cleanup EXIT

for _ in $(seq 1 40); do
  kill -0 "$HPID" 2>/dev/null || { echo "FAIL: hub died"; cat "$HUBLOG"; exit 1; }
  kill -0 "$LPID" 2>/dev/null || { echo "FAIL: leaf died"; cat "$LEAFLOG"; exit 1; }
  if (exec 3<>/dev/tcp/127.0.0.1/$LEAF_PORT) 2>/dev/null; then exec 3>&- 3<&-; break; fi
  sleep 0.2
done
sleep 1.0  # leaf link establish

HUB_PORT=$HUB_PORT LEAF_PORT=$LEAF_PORT node -e '
const { connect, StringCodec } = require("nats");
const sc = StringCodec();
(async () => {
  const a = await connect({ servers: `nats://127.0.0.1:${process.env.HUB_PORT}`, user: "aimindset", pass: "pw_aimindset" });
  const b = await connect({ servers: `nats://127.0.0.1:${process.env.LEAF_PORT}`, user: "edge", pass: "pw_edge" });
  const sub = b.subscribe("fed.partner.msg.agent-codex");
  let got = null;
  (async () => { for await (const m of sub) { got = sc.decode(m.data); break; } })();
  await new Promise(r => setTimeout(r, 600));
  a.publish("fed.partner.msg.agent-codex", sc.encode("xorg-over-leaf"));
  await new Promise(r => setTimeout(r, 900));
  await a.drain(); await b.drain();
  console.log(got === "xorg-over-leaf" ? "REAL-MESH-LEAF-OK (acceptance #1: cross-org over leaf)" : `REAL-MESH-LEAF-FAIL got=${got}`);
  process.exit(got === "xorg-over-leaf" ? 0 : 1);
})().catch(e => { console.log("REAL-MESH-LEAF-ERR", e.message); process.exit(1); });
'
