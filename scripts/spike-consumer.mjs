// Spike #85 multi-process rig — one simulated delivery session (e.g. an mcp-channel-server).
// Registers presence, then races to claim the channel and "emit". Mirrors the contract every
// real delivery path obeys: claim_or_skip BEFORE the side-effect, fence the outbound by token.
// argv: <leaseDb> <sessionId> <conversationId> <memberSlot> <agentId> <startAtMs>
import { SessionLeaseStore } from "./lease.mjs";

const [, , db, sessionId, conv, slot, agent, startAtMs] = process.argv;
const store = new SessionLeaseStore(db);
store.registerSession({ sessionId, agentId: agent, mode: "mcp-channel" });

// Barrier: spin until the shared wall-clock deadline so all processes hit claim together.
const startAt = Number(startAtMs);
while (Date.now() < startAt) { /* busy-wait to maximise contention */ }

const res = store.claimOrSkip(conv, slot, sessionId, 20000, Date.now(), "native:");
if (res.won && store.isCurrentToken(conv, slot, res.token)) {
  console.log(`EMIT ${sessionId} token=${res.token}`);
} else {
  console.log(`SUPPRESS ${sessionId} owner=${res.ownerSessionId ?? "?"}`);
}
store.close();
