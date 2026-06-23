// Spike S0.2a/S0.2b smoke for the session-ownership lease (#80).
// Proves: single-owner, per-turn token bump, stale takeover, resurrect fence.
// Run: node scripts/lease-smoke.test.mjs
import { SessionLeaseStore } from "./lease.mjs";
import { rmSync } from "node:fs";

const DB = process.env.LEASE_SMOKE_DB || "/tmp/lease-smoke.db";
for (const ext of ["", "-wal", "-shm"]) rmSync(DB + ext, { force: true });

const store = new SessionLeaseStore(DB);
let pass = 0, fail = 0;
const ok = (name, cond) => { (cond ? pass++ : fail++); console.log(`${cond ? "PASS" : "FAIL"}  ${name}`); };

const C = "conv-1", M = "agent-codex-volt", A = "sessA", B = "sessB";
const TTL = 20000;
const t = 1000;

// 1. A claims a fresh channel -> wins, token 1
const r1 = store.claimOrSkip(C, M, A, TTL, t);
ok("A wins fresh claim (token=1)", r1.won && r1.token === 1);

// 2. B claims while A is live (not stale) -> loses, sees A as owner  [AC-4 / AC-1]
const r2 = store.claimOrSkip(C, M, B, TTL, t + 100);
ok("B loses while A live, owner=A", !r2.won && r2.ownerSessionId === A);

// 3. A heartbeats -> ownership kept, token unchanged
store.heartbeat(C, M, A, t + 200);
ok("token stable after heartbeat (=1)", store.isCurrentToken(C, M, 1));

// 4. A re-claims for a new turn -> wins, token bumps to 2 (per-turn fence)
const r4 = store.claimOrSkip(C, M, A, TTL, t + 300);
ok("A re-claim bumps token (=2)", r4.won && r4.token === 2);
ok("old turn token 1 fenced", !store.isCurrentToken(C, M, 1));
ok("current token is 2", store.isCurrentToken(C, M, 2));

// 5. A goes stale (now > heartbeat + ttl); B takes over -> wins, token 3  [AC-2]
const r5 = store.claimOrSkip(C, M, B, TTL, t + 300 + TTL + 1);
ok("B stale-takeover wins (owner=B, token=3)", r5.won && r5.ownerSessionId === B && r5.token === 3);

// 6. Resurrect race: A wakes and tries to send with its OLD token 2 -> fenced  [AC-3]
ok("resurrect A token-2 suppressed by fence", !store.isCurrentToken(C, M, 2));
ok("B token-3 is current", store.isCurrentToken(C, M, 3));

// 7. A's heartbeat after losing ownership is a no-op
ok("A heartbeat after loss = 0 rows", store.heartbeat(C, M, A, t + 99999) === 0);

// 8. session_presence registry round-trips
store.registerSession({ sessionId: A, agentId: "agent-codex-volt", threadId: "019ef606", pid: 12345, mode: "foreground", now: t });
ok("session presence heartbeat = 1 row", store.sessionHeartbeat(A, t + 1) === 1);

store.close();
console.log(`\n${pass} pass, ${fail} fail`);
process.exit(fail ? 1 : 0);
