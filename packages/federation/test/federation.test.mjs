import test from "node:test";
import assert from "node:assert/strict";
import { createKeyPair, createSigningKeyPair } from "../../security/dist/src/index.js";
import {
  canonicalRoster,
  formatAddress,
  isLocal,
  lookupAgentKeys,
  parseAddress,
  signRoster,
  verifyRoster,
} from "../dist/src/index.js";

// ─── Addressing ───

test("parseAddress: bare agentId resolves to local org (back-compat)", () => {
  assert.deepEqual(parseAddress("agent-jarvis", "aimindset"), {
    org: "aimindset",
    agentId: "agent-jarvis",
  });
});

test("parseAddress: org/agentId splits on the single slash", () => {
  assert.deepEqual(parseAddress("partner/agent-bob", "aimindset"), {
    org: "partner",
    agentId: "agent-bob",
  });
});

test("parseAddress: rejects nested slash + empty", () => {
  assert.throws(() => parseAddress("a/b/c", "local"), /nested slash/);
  assert.throws(() => parseAddress("", "local"), /empty address/);
});

test("formatAddress + isLocal round-trip", () => {
  const addr = parseAddress("partner/agent-bob", "aimindset");
  assert.equal(formatAddress(addr), "partner/agent-bob");
  assert.equal(isLocal(addr, "aimindset"), false);
  assert.equal(isLocal(parseAddress("agent-jarvis", "aimindset"), "aimindset"), true);
});

// ─── Signed roster ───

async function buildRoster(org = "aimindset") {
  const sign = await createSigningKeyPair(); // Ed25519 directory-signing key
  const jarvisEnc = await createKeyPair(); // X25519
  const jarvisSig = await createSigningKeyPair(); // Ed25519 (verify)
  const body = {
    org,
    version: 1,
    issuedAt: "2026-06-21T18:00:00.000Z",
    agents: {
      "agent-jarvis": {
        encryptPublicKey: jarvisEnc.publicKey,
        verifyPublicKey: jarvisSig.publicKey,
      },
    },
  };
  const roster = await signRoster(body, sign.privateKey, sign.publicKey);
  return { roster, sign, jarvisEnc, jarvisSig };
}

test("signRoster -> verifyRoster round-trips", async () => {
  const { roster, sign } = await buildRoster();
  assert.ok(roster.signature.length > 0);
  assert.equal(await verifyRoster(roster, sign.publicKey), true);
});

test("verifyRoster fails on tampered keys (sig covers the body)", async () => {
  const { roster, sign } = await buildRoster();
  const forged = await createKeyPair();
  const tampered = {
    ...roster,
    agents: {
      "agent-jarvis": {
        ...roster.agents["agent-jarvis"],
        encryptPublicKey: forged.publicKey, // swap in an attacker key
      },
    },
  };
  assert.equal(await verifyRoster(tampered, sign.publicKey), false);
});

test("verifyRoster fails when embedded key differs from pinned org key", async () => {
  const { roster, sign } = await buildRoster();
  const attacker = await createSigningKeyPair();
  assert.equal(await verifyRoster({ ...roster, signingPublicKey: attacker.publicKey }, sign.publicKey), false);
});

test("verifyRoster rejects a forged roster signed by the attacker's embedded key", async () => {
  const { sign, jarvisEnc, jarvisSig } = await buildRoster();
  const attacker = await createSigningKeyPair();
  const forged = await signRoster(
    {
      org: "aimindset",
      version: 2,
      issuedAt: "2026-06-21T18:05:00.000Z",
      agents: {
        "agent-jarvis": {
          encryptPublicKey: jarvisEnc.publicKey,
          verifyPublicKey: jarvisSig.publicKey,
        },
      },
    },
    attacker.privateKey,
    attacker.publicKey,
  );

  assert.equal(await verifyRoster(forged, sign.publicKey), false);
});

test("canonicalRoster is insertion-order independent", () => {
  const a = { org: "o", version: 1, issuedAt: "t", agents: { b: { encryptPublicKey: "1", verifyPublicKey: "2" }, a: { encryptPublicKey: "3", verifyPublicKey: "4" } } };
  const b = { org: "o", version: 1, issuedAt: "t", agents: { a: { encryptPublicKey: "3", verifyPublicKey: "4" }, b: { encryptPublicKey: "1", verifyPublicKey: "2" } } };
  assert.equal(canonicalRoster(a), canonicalRoster(b));
});

test("lookupAgentKeys returns keys / throws on miss", async () => {
  const { roster, jarvisEnc } = await buildRoster();
  assert.equal(lookupAgentKeys(roster, "agent-jarvis").encryptPublicKey, jarvisEnc.publicKey);
  assert.throws(() => lookupAgentKeys(roster, "ghost"), /not in roster/);
});

test("signRoster rejects non-positive version", async () => {
  const sign = await createSigningKeyPair();
  await assert.rejects(
    () => signRoster({ org: "o", version: 0, issuedAt: "t", agents: {} }, sign.privateKey, sign.publicKey),
    /version must be a positive integer/,
  );
});
