import test from "node:test";
import assert from "node:assert/strict";
import {
  CandidateRegistry,
  isPresenceFrameV1,
  isSignedPresenceFrameV1,
  observeSignedPresence,
  stablePresencePayload,
} from "../dist/src/index.js";

const frame = (over = {}) => ({
  presenceVersion: "1.0",
  agentId: "agent-x",
  encryptionPublicKey: "enc-pub",
  signingPublicKey: "sig-pub",
  subject: "msg.agent-x",
  capabilities: ["chat", "files"],
  ttlMs: 60_000,
  ts: "2026-06-22T13:00:00.000Z",
  nonce: "n1",
  ...over,
});

const at = (iso) => Date.parse(iso);

// --- isPresenceFrameV1 -------------------------------------------------------

test("isPresenceFrameV1 accepts a well-formed frame", () => {
  assert.equal(isPresenceFrameV1(frame()), true);
});

test("isPresenceFrameV1 rejects malformed frames", () => {
  assert.equal(isPresenceFrameV1(null), false);
  assert.equal(isPresenceFrameV1({ ...frame(), presenceVersion: "2.0" }), false);
  assert.equal(isPresenceFrameV1({ ...frame(), agentId: "" }), false);
  assert.equal(isPresenceFrameV1({ ...frame(), ttlMs: 0 }), false);
  assert.equal(isPresenceFrameV1({ ...frame(), ttlMs: -5 }), false);
  assert.equal(isPresenceFrameV1({ ...frame(), ts: "not-a-date" }), false);
  assert.equal(isPresenceFrameV1({ ...frame(), capabilities: "chat" }), false);
  assert.equal(isPresenceFrameV1({ ...frame(), nonce: "" }), false);
});

// --- observe / registry build ------------------------------------------------

test("observe builds a candidate from a valid frame", () => {
  const reg = new CandidateRegistry();
  const now = at("2026-06-22T13:00:10.000Z");
  const c = reg.observe(frame(), now);
  assert.ok(c);
  assert.equal(c.agentId, "agent-x");
  assert.equal(c.subject, "msg.agent-x");
  assert.deepEqual(c.capabilities, ["chat", "files"]);
  assert.equal(reg.size(now), 1);
  assert.equal(reg.get("agent-x", now)?.encryptionPublicKey, "enc-pub");
});

test("observe rejects invalid or already-expired frames", () => {
  const reg = new CandidateRegistry();
  assert.equal(reg.observe({ bad: true }, at("2026-06-22T13:00:10.000Z")), null);
  // ttl already elapsed at observation time
  const late = at("2026-06-22T13:05:00.000Z"); // 5 min later, ttl 60s
  assert.equal(reg.observe(frame(), late), null);
  assert.equal(reg.size(late), 0);
});

// --- CRITICAL INVARIANT: candidates are never auto-trusted -------------------

test("a discovered candidate is NEVER auto-trusted", () => {
  const reg = new CandidateRegistry();
  const now = at("2026-06-22T13:00:10.000Z");
  const c = reg.observe(frame(), now);
  assert.equal(c.trusted, false);
  assert.equal(reg.get("agent-x", now).trusted, false);
  assert.equal(reg.list(now).every((x) => x.trusted === false), true);
});

// --- expiry ------------------------------------------------------------------

test("candidate expires after ts + ttlMs", () => {
  const reg = new CandidateRegistry();
  reg.observe(frame(), at("2026-06-22T13:00:05.000Z"));
  const beforeExpiry = at("2026-06-22T13:00:50.000Z"); // within 60s
  const afterExpiry = at("2026-06-22T13:01:01.000Z"); // past 60s
  assert.equal(reg.size(beforeExpiry), 1);
  assert.equal(reg.get("agent-x", afterExpiry), undefined);
  assert.equal(reg.list(afterExpiry).length, 0);
});

test("prune removes expired candidates", () => {
  const reg = new CandidateRegistry();
  reg.observe(frame(), at("2026-06-22T13:00:05.000Z"));
  reg.observe(frame({ agentId: "agent-y", nonce: "ny", ttlMs: 600_000 }), at("2026-06-22T13:00:05.000Z"));
  const afterFirstExpiry = at("2026-06-22T13:02:00.000Z");
  assert.equal(reg.prune(afterFirstExpiry), 1); // agent-x gone, agent-y stays
  assert.equal(reg.size(afterFirstExpiry), 1);
  assert.ok(reg.get("agent-y", afterFirstExpiry));
});

// --- idempotency / ordering --------------------------------------------------

test("prune drops expired dedupe markers too (bounded seenNonces)", () => {
  const reg = new CandidateRegistry();
  reg.observe(frame({ nonce: "a" }), at("2026-06-22T13:00:05.000Z")); // ttl 60s
  reg.observe(frame({ agentId: "agent-y", nonce: "b", ttlMs: 600_000 }), at("2026-06-22T13:00:05.000Z"));
  assert.equal(reg.nonceCount(), 2);
  reg.prune(at("2026-06-22T13:02:00.000Z")); // agent-x's 60s frame expired, agent-y's 600s not
  assert.equal(reg.nonceCount(), 1); // only the non-expired nonce marker remains
});

test("duplicate (agentId, nonce) announcement is idempotent", () => {
  const reg = new CandidateRegistry();
  const now = at("2026-06-22T13:00:10.000Z");
  const c1 = reg.observe(frame(), now);
  const c2 = reg.observe(frame(), now); // same nonce
  assert.equal(c2.agentId, c1.agentId);
  assert.equal(reg.size(now), 1);
});

test("a fresh announcement (new nonce) refreshes the candidate", () => {
  const reg = new CandidateRegistry();
  reg.observe(frame(), at("2026-06-22T13:00:05.000Z"));
  const c = reg.observe(
    frame({ nonce: "n2", ts: "2026-06-22T13:00:40.000Z", capabilities: ["chat", "files", "audio"] }),
    at("2026-06-22T13:00:41.000Z"),
  );
  assert.deepEqual(c.capabilities, ["chat", "files", "audio"]);
  assert.equal(c.lastSeen, at("2026-06-22T13:00:40.000Z"));
});

// --- signed presence (PR2) ---------------------------------------------------

test("stablePresencePayload is deterministic and excludes the signature", () => {
  const a = stablePresencePayload(frame());
  const b = stablePresencePayload({ ...frame() });
  assert.equal(a, b);
  assert.ok(!a.includes("signature"));
  assert.ok(a.includes("agent-x") && a.includes("n1"));
});

test("isSignedPresenceFrameV1 validates the wrapper", () => {
  assert.equal(isSignedPresenceFrameV1({ frame: frame(), signature: "sig" }), true);
  assert.equal(isSignedPresenceFrameV1({ frame: frame(), signature: "" }), false);
  assert.equal(isSignedPresenceFrameV1({ frame: { bad: 1 }, signature: "sig" }), false);
  assert.equal(isSignedPresenceFrameV1(null), false);
});

test("observeSignedPresence folds a valid-signature frame as an untrusted candidate", async () => {
  const reg = new CandidateRegistry();
  const now = at("2026-06-22T13:00:10.000Z");
  const verifyOk = async () => true;
  const c = await observeSignedPresence(reg, { frame: frame(), signature: "sig" }, verifyOk, now);
  assert.ok(c);
  assert.equal(c.agentId, "agent-x");
  assert.equal(c.trusted, false);
  assert.equal(reg.size(now), 1);
});

test("observeSignedPresence rejects a bad signature and a malformed wrapper", async () => {
  const reg = new CandidateRegistry();
  const now = at("2026-06-22T13:00:10.000Z");
  const verifyBad = async () => false;
  assert.equal(await observeSignedPresence(reg, { frame: frame(), signature: "sig" }, verifyBad, now), null);
  assert.equal(await observeSignedPresence(reg, { nope: true }, async () => true, now), null);
  assert.equal(reg.size(now), 0);
});

test("observeSignedPresence drops a frame when the verifier THROWS (malformed bytes)", async () => {
  const reg = new CandidateRegistry();
  const now = at("2026-06-22T13:00:10.000Z");
  // Real Ed25519 verifiers throw on bad-length/garbage signature or key bytes;
  // untrusted public frames must be dropped, not escalated to an unhandled rejection.
  const verifyThrows = async () => {
    throw new Error("signature of length 64 expected, got 9");
  };
  const r = await observeSignedPresence(reg, { frame: frame(), signature: "not-b64" }, verifyThrows, now);
  assert.equal(r, null);
  assert.equal(reg.size(now), 0);
});

test("observeSignedPresence verifies against the key the frame advertises", async () => {
  const reg = new CandidateRegistry();
  const now = at("2026-06-22T13:00:10.000Z");
  const seen = {};
  const verify = async (payload, signature, publicKey) => {
    seen.payload = payload; seen.publicKey = publicKey; return true;
  };
  await observeSignedPresence(reg, { frame: frame(), signature: "sig" }, verify, now);
  assert.equal(seen.publicKey, "sig-pub");
  assert.equal(seen.payload, stablePresencePayload(frame()));
});

test("an out-of-order (older) frame does not overwrite a fresher candidate", () => {
  const reg = new CandidateRegistry();
  reg.observe(frame({ nonce: "new", ts: "2026-06-22T13:00:40.000Z" }), at("2026-06-22T13:00:41.000Z"));
  const stale = reg.observe(
    frame({ nonce: "old", ts: "2026-06-22T13:00:10.000Z", subject: "msg.stale" }),
    at("2026-06-22T13:00:42.000Z"),
  );
  assert.equal(stale.lastSeen, at("2026-06-22T13:00:40.000Z")); // kept fresher
  assert.equal(reg.get("agent-x", at("2026-06-22T13:00:42.000Z")).subject, "msg.agent-x");
});
