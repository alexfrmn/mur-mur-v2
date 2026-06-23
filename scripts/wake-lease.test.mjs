// Spike #82 — native wake path is a lease-gated fallback.
// Proves: native claims on cold channel; a live chat session preempts the native fallback;
// native is muted while that session is live; native resumes after it goes stale;
// and WakeMonitor honours the gate (no inject when denied).
// Run: node scripts/wake-lease.test.mjs
import { SessionLeaseStore, createNativeLeaseGate } from "./lease.mjs";
import { WakeMonitor } from "./wake-monitor.mjs";
import { rmSync } from "node:fs";

const DB = process.env.WAKE_LEASE_DB || "/tmp/wake-lease.db";
for (const e of ["", "-wal", "-shm"]) rmSync(DB + e, { force: true });
const store = new SessionLeaseStore(DB);

let pass = 0, fail = 0;
const ok = (n, c) => { (c ? pass++ : fail++); console.log(`${c ? "PASS" : "FAIL"}  ${n}`); };

const AGENT = "agent-codex-volt", CONV = "conv-x", SLOT = AGENT;
let clock = 1000;
const now = () => clock;
const gate = createNativeLeaseGate({ store, agentId: AGENT, ttlMs: 20000, now });

// 1. cold channel -> native fallback claims and allows the wake
const d1 = await gate({ conversationId: CONV });
ok("native allows on cold channel", d1.allow === true);
ok("owner is native:<agent>", store.getOwner(CONV, SLOT)?.ownerSessionId === `native:${AGENT}`);

// 2. live foreground chat session preempts the native fallback
const fg = "fg-019ef606";
const c2 = store.claimOrSkip(CONV, SLOT, fg, 20000, clock, "native:");
ok("foreground preempts native fallback", c2.won && store.getOwner(CONV, SLOT)?.ownerSessionId === fg);

// 3. native wake is muted while the foreground session is live
clock += 100;
const d3 = await gate({ conversationId: CONV });
ok("native muted while foreground live", d3.allow === false && d3.ownerSessionId === fg);

// 4. foreground goes stale -> native fallback resumes
clock += 20001;
const d4 = await gate({ conversationId: CONV });
ok("native resumes after foreground stale", d4.allow === true);

// --- WakeMonitor honours the gate ---
const mkWM = (leaseGate) => {
  let injected = 0;
  const wm = new WakeMonitor({
    enabled: true,
    mode: "codex_app_server",
    peers: { [AGENT]: { mode: "codex_app_server" } },
    injector: async () => { injected++; },
    leaseGate,
    now,
    log: () => {},
  });
  return { wm, injected: () => injected };
};

clock += 1;
const A = mkWM(async () => ({ allow: false, ownerSessionId: "other", reason: "live-owner" }));
await A.wm.onInbound({ msgId: "m1", conversationId: CONV, from: AGENT, text: "hi", cursor: 1 });
ok("WakeMonitor mutes wake when gate denies", A.injected() === 0);

const B = mkWM(async () => ({ allow: true, token: 7 }));
await B.wm.onInbound({ msgId: "m2", conversationId: CONV, from: AGENT, text: "hi", cursor: 2 });
ok("WakeMonitor wakes when gate allows", B.injected() === 1);

store.close();
console.log(`\n${pass} pass, ${fail} fail`);
process.exit(fail ? 1 : 0);
