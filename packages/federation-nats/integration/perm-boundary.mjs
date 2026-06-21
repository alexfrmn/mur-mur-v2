// Acceptance #2/#3 (docs/federation-nats-contract.md): with restrictUserPermissions,
// a federation user may publish ONLY into imported partner prefixes and subscribe ONLY
// on its own exported prefix. Boots a single-broker restricted accounts config and
// asserts that out-of-bounds publish/subscribe are rejected while in-bounds succeed.
import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { connect, StringCodec } from "nats";

const PORT = process.env.FED_NATS_PORT || "14601";
const NATS_URL = `nats://127.0.0.1:${PORT}`;
const sc = StringCodec();

function reachable(url) {
  const { hostname, port } = new URL(url);
  return new Promise((res) => {
    const s = net.connect({ host: hostname, port: Number(port) }, () => { s.destroy(); res(true); });
    s.on("error", () => res(false));
    s.setTimeout(1500, () => { s.destroy(); res(false); });
  });
}

// Capture connection status events (permission violations land here in nats.js).
function captureStatus(nc) {
  const events = [];
  (async () => { for await (const s of nc.status()) events.push(s); })().catch(() => {});
  return events;
}
// nats.js surfaces a denied op as: { type:"error", data:"PERMISSIONS_VIOLATION",
//   permissionContext:{ operation:"publish"|"subscription", subject } }
const violated = (events, operation, subject) =>
  events.some((e) =>
    e?.data === "PERMISSIONS_VIOLATION" &&
    e?.permissionContext?.operation === operation &&
    e?.permissionContext?.subject === subject);

test("acceptance #2/#3: restricted federation user pub/sub boundaries", async (t) => {
  if (!(await reachable(NATS_URL))) { t.skip(`no nats at ${NATS_URL}; run via run-perm-boundary.sh`); return; }

  // aimindset: publish.allow=[fed.partner.>], subscribe.allow=[fed.aimindset.>]
  const a = await connect({ servers: NATS_URL, user: "aimindset", pass: "pw_aimindset", name: "orgA" });
  const events = captureStatus(a);
  t.after(async () => { await a.drain(); });

  // ── positive controls (in-bounds) ──
  a.publish("fed.partner.msg.agent-codex", sc.encode("allowed-pub"));        // imported partner: OK
  const okSub = a.subscribe("fed.aimindset.msg.agent-jarvis");                // own export: OK
  await a.flush();

  // ── #2: publish OUTSIDE imported partner prefixes is denied ──
  a.publish("fed.intruder.msg.x", sc.encode("denied-pub"));
  a.publish("fed.aimindset.msg.self", sc.encode("denied-own-pub")); // own namespace is sub-only, not pub
  await a.flush();

  // ── #3: subscribe OUTSIDE own exported prefix is denied ──
  const badSub = a.subscribe("fed.partner.msg.agent-codex");
  await a.flush();
  await new Promise((r) => setTimeout(r, 400)); // let -ERR statuses arrive

  // #2: publish OUTSIDE imported partner prefixes -> denied
  assert.ok(violated(events, "publish", "fed.intruder.msg.x"), "#2 publish to non-imported prefix denied");
  assert.ok(violated(events, "publish", "fed.aimindset.msg.self"), "#2 publish to own (subscribe-only) namespace denied");
  // #3: subscribe OUTSIDE own exported prefix -> denied
  assert.ok(violated(events, "subscription", "fed.partner.msg.agent-codex"), "#3 subscribe outside own export denied");
  // positive controls: in-bounds ops are NOT violations
  assert.ok(!violated(events, "publish", "fed.partner.msg.agent-codex"), "in-bounds publish (imported partner) allowed");
  assert.ok(!violated(events, "subscription", "fed.aimindset.msg.agent-jarvis"), "in-bounds subscribe (own export) allowed");
  void okSub;
  void badSub;
});
