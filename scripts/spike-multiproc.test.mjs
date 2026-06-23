// Spike #85 multi-process rig — REAL cross-process proof of "N sessions -> exactly 1 emits".
// Spawns N child processes that each register presence and race to claim the same channel on a
// shared lease.db (real SQLite WAL + CAS contention, not a logical simulation). Exactly one must
// EMIT; the rest must SUPPRESS. This is the deploy-free analogue of the channel.log 444/445 fix.
// Run: node scripts/spike-multiproc.test.mjs
import { spawn } from "node:child_process";
import { rmSync } from "node:fs";
import { SessionLeaseStore } from "./lease.mjs";

const DB = process.env.SPIKE_MP_DB || "/tmp/spike-mp.db";
for (const e of ["", "-wal", "-shm"]) rmSync(DB + e, { force: true });
new SessionLeaseStore(DB).close(); // materialise schema once

const N = Number(process.env.SPIKE_MP_N || 5);
const AGENT = "agent-codex-volt", CONV = "conv-mp", SLOT = AGENT;
const startAt = Date.now() + 400; // shared barrier deadline

const run = (i) =>
  new Promise((resolve) => {
    const p = spawn("node", ["scripts/spike-consumer.mjs", DB, `mcp-sess-${i}`, CONV, SLOT, AGENT, String(startAt)]);
    let out = "";
    p.stdout.on("data", (d) => (out += d));
    p.on("close", () => resolve(out.trim()));
  });

const lines = await Promise.all(Array.from({ length: N }, (_, i) => run(i)));
const emits = lines.filter((l) => l.startsWith("EMIT"));
const suppress = lines.filter((l) => l.startsWith("SUPPRESS"));

console.log(lines.join("\n"));
console.log(`\n${N} processes raced: ${emits.length} EMIT, ${suppress.length} SUPPRESS`);
const okOne = emits.length === 1;
const okRest = suppress.length === N - 1;
console.log(okOne ? "PASS  exactly one session emitted" : "FAIL  not exactly one emit");
console.log(okRest ? "PASS  all others suppressed" : "FAIL  suppress count wrong");
process.exit(okOne && okRest ? 0 : 1);
