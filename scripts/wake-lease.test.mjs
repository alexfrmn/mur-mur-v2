// Spike #82 — native wake path is a presence-deferring, lease-gated fallback.
// Proves: native claims on a cold channel with no attended session; native DEFERS while a live
// interactive (foreground/mcp-channel) session_presence exists; native resumes once that presence
// goes stale; a real session preempts a native-owned channel; WakeMonitor honours the gate.
// Run: node scripts/wake-lease.test.mjs
import { SessionLeaseStore, createNativeLeaseGate } from "./lease.mjs";
import { WakeMonitor } from "./wake-monitor.mjs";
import { rmSync } from "node:fs";

const DB = process.env.WAKE_LEASE_DB || "/tmp/wake-lease.db";
for (const e of ["", "-wal", "-shm"]) rmSync(DB + e, { force: true });
const store = new SessionLeaseStore(DB);

let pass = 0, fail = 0;
const ok = (n, c) => { (c ? pass++ : fail++); console.log(`${c ? "PASS" : "FAIL"}  ${n}`); };

const AGENT = "agent-codex-volt", SLOT = AGENT;
let clock = 1000;
const now = () => clock;
const gate = createNativeLeaseGate({ store, agentId: AGENT, ttlMs: 20000, now });

// 1. cold channel, no attended session -> native fallback claims and allows the wake
const d1 = await gate({ conversationId: "conv-cold" });
ok("native allows on cold channel (no presence)", d1.allow === true);
ok("owner is native:<agent>", store.getOwner("conv-cold", SLOT)?.ownerSessionId === `native:${AGENT}`);

// 2. a live foreground session registers presence -> native defers (no competing wake)
store.registerSession({ sessionId: "fg-019ef606", agentId: AGENT, threadId: "019ef606", pid: 111, mode: "foreground", now: clock });
clock += 100;
const d2 = await gate({ conversationId: "conv-live" });
ok("native defers to live interactive presence", d2.allow === false && d2.reason === "live-interactive-session");

// 3. presence goes stale (no heartbeat past ttl) -> native fallback resumes
clock += 20001;
const d3 = await gate({ conversationId: "conv-resume" });
ok("native resumes after presence stale", d3.allow === true);

// 4. real session preempts a native-owned channel (belt-and-suspenders for presence gaps)
store.claimOrSkip("conv-p", SLOT, `native:${AGENT}`, 20000, clock); // native grabs it first
const cp = store.claimOrSkip("conv-p", SLOT, "fg-2", 20000, clock, "native:");
ok("real session preempts native owner", cp.won && store.getOwner("conv-p", SLOT)?.ownerSessionId === "fg-2");

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

const A = mkWM(async () => ({ allow: false, reason: "live-interactive-session" }));
await A.wm.onInbound({ msgId: "m1", conversationId: "conv-cold", from: AGENT, text: "hi", cursor: 1 });
ok("WakeMonitor mutes wake when gate denies", A.injected() === 0);

const B = mkWM(async () => ({ allow: true, token: 7 }));
await B.wm.onInbound({ msgId: "m2", conversationId: "conv-cold", from: AGENT, text: "hi", cursor: 2 });
ok("WakeMonitor wakes when gate allows", B.injected() === 1);

store.close();
console.log(`\n${pass} pass, ${fail} fail`);
process.exit(fail ? 1 : 0);
