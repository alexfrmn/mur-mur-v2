// Parity smoke: the @murmurv2/core (TS, built) SessionLeaseStore must behave identically to the
// runtime scripts/lease.mjs. Run after `npm run build`. Run: node scripts/lease-core-parity.test.mjs
import { SessionLeaseStore, createNativeLeaseGate, NATIVE_SESSION_PREFIX } from "@murmurv2/core";
import { rmSync } from "node:fs";

const DB = process.env.PARITY_DB || "/tmp/lease-core-parity.db";
for (const e of ["", "-wal", "-shm"]) rmSync(DB + e, { force: true });
const store = new SessionLeaseStore(DB);

let pass = 0, fail = 0;
const ok = (n, c) => { (c ? pass++ : fail++); console.log(`${c ? "PASS" : "FAIL"}  ${n}`); };

const C = "c", M = "agent", A = "sessA", B = "sessB", TTL = 20000;
const t = 1000;

const r1 = store.claimOrSkip(C, M, A, TTL, t);
ok("A wins fresh (token=1)", r1.won && r1.token === 1);
const r2 = store.claimOrSkip(C, M, B, TTL, t + 100);
ok("B loses while A live", !r2.won && r2.ownerSessionId === A);
const r4 = store.claimOrSkip(C, M, A, TTL, t + 300);
ok("A re-claim bumps token (=2)", r4.won && r4.token === 2);
ok("old turn token fenced", !store.isCurrentToken(C, M, 1));
ok("current token is 2", store.isCurrentToken(C, M, 2));
const r5 = store.claimOrSkip(C, M, B, TTL, t + 300 + TTL + 1);
ok("B stale-takeover (token=3)", r5.won && r5.token === 3);
ok("resurrect token-2 fenced", !store.isCurrentToken(C, M, 2));

store.claimOrSkip("cp", M, `${NATIVE_SESSION_PREFIX}${M}`, TTL, t);
const cp = store.claimOrSkip("cp", M, "fg", TTL, t, NATIVE_SESSION_PREFIX);
ok("real session preempts native owner", cp.won && store.getOwner("cp", M)?.ownerSessionId === "fg");

const gate = createNativeLeaseGate({ store, agentId: M, ttlMs: TTL, now: () => 9_000_000 });
store.registerSession({ sessionId: "fg2", agentId: M, mode: "foreground", now: 9_000_000 });
const d = await gate({ conversationId: "cz" });
ok("native gate defers on live presence", d.allow === false && d.reason === "live-interactive-session");
ok("NATIVE_SESSION_PREFIX exported", NATIVE_SESSION_PREFIX === "native:");

store.close();
console.log(`\n${pass} pass, ${fail} fail (core build parity)`);
process.exit(fail ? 1 : 0);
