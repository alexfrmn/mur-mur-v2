// Spike #85 (local) — cross-path coordination proof at the lease-contract level.
// One agent, one conversation, several competing delivery paths. Proves the property Alex wants:
// while a chat window is attended, exactly that window owns the channel; the native daemon wake,
// the coldstart fallback, and a second window all stand down; on crash, the fallback resumes.
// (Live NATS + real Codex sessions are the deploy-time acceptance; this proves the logic.)
// Run: node scripts/spike-coordination.test.mjs
import { SessionLeaseStore, createNativeLeaseGate } from "./lease.mjs";
import { rmSync } from "node:fs";

const DB = process.env.SPIKE_COORD_DB || "/tmp/spike-coord.db";
for (const e of ["", "-wal", "-shm"]) rmSync(DB + e, { force: true });
const store = new SessionLeaseStore(DB);

let pass = 0, fail = 0;
const ok = (n, c) => { (c ? pass++ : fail++); console.log(`${c ? "PASS" : "FAIL"}  ${n}`); };

const AGENT = "agent-codex-volt", CONV = "conv-85", SLOT = AGENT, TTL = 20000;
let clock = 1000;
const now = () => clock;
const nativeGate = createNativeLeaseGate({ store, agentId: AGENT, ttlMs: TTL, now });

// Window A (attended foreground) registers presence and claims, preempting any native fallback.
const A = "fg-019ef606";
store.registerSession({ sessionId: A, agentId: AGENT, threadId: "019ef606", pid: 111, mode: "foreground", now: clock });
const aClaim = store.claimOrSkip(CONV, SLOT, A, TTL, clock, "native:");
ok("window A owns the channel", aClaim.won);
const aToken = aClaim.token;

// An inbound message arrives. Every other path must stand down:
clock += 10;
// (a) native daemon wake gate -> defers to the attended session
ok("native daemon defers", (await nativeGate({ conversationId: CONV })).allow === false);
// (b) coldstart fallback claim (no preempt) -> loses to the live window
const cold = store.claimOrSkip(CONV, SLOT, `coldstart:222:msgX`, TTL, clock);
ok("coldstart fallback stands down", cold.won === false && cold.ownerSessionId === A);
// (c) a SECOND window of the same agent claims -> loses (cannot preempt a real session)
const B = "mcp-agent-codex-volt-907481";
const bClaim = store.claimOrSkip(CONV, SLOT, B, TTL, clock, "native:");
ok("second window stands down", bClaim.won === false && bClaim.ownerSessionId === A);
// (d) only window A's token is current -> only A's outbound passes the fence
ok("only window A passes the outbound fence", store.isCurrentToken(CONV, SLOT, aToken));

// Window A keeps the lease alive while attended (heartbeat, no token bump).
clock += 5000;
store.registerSession({ sessionId: A, agentId: AGENT, threadId: "019ef606", pid: 111, mode: "foreground", now: clock });
store.heartbeat(CONV, SLOT, A, clock);
ok("attended window keeps ownership (token stable)", store.isCurrentToken(CONV, SLOT, aToken));
ok("native still defers while attended", (await nativeGate({ conversationId: CONV })).allow === false);

// Window A crashes: presence + lease both go stale. After TTL the native fallback resumes.
clock += TTL + 1;
const resumed = await nativeGate({ conversationId: CONV });
ok("native fallback resumes after window crash", resumed.allow === true);
ok("native is now the owner (cold delivery)", store.getOwner(CONV, SLOT)?.ownerSessionId === `native:${AGENT}`);

store.close();
console.log(`\n${pass} pass, ${fail} fail`);
process.exit(fail ? 1 : 0);
