// Conformance suite for the Murmur V2 wire protocol (schemaVersion 1.0).
// Validates fixtures against the machine-readable JSON Schema (schema/protocol-v1.schema.json)
// AND asserts the schema and the runtime guards AGREE on every structural case — so the spec and
// the implementation can't silently drift, and a cross-language implementer can trust either as
// the contract. Covered wire types: EnvelopeV1, AckV1, PresenceFrameV1, SignedPresenceFrameV1,
// StreamStart/StreamChunk/StreamEnd (+ the discriminated StreamFrame union). No external validator
// dep: a tiny subset validator interprets the flat protocol $defs
// (type incl. boolean / required / const / enum / minLength / minItems / items / exclusiveMinimum / oneOf).
// The only runtime-only check is `format: date-time` validity, which Draft 2020-12 treats as an
// advisory annotation (not an assertion); the guards enforce it via Date.parse and it sits outside
// the agreement matrices (documented per-type). Everything else — including ttlMs > 0 and non-empty
// stream-chunk data — is enforced by BOTH the schema and the guards and lives in the matrices.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  isEnvelopeV1,
  isPresenceFrameV1,
  isSignedPresenceFrameV1,
  isStreamStart,
  isStreamChunk,
  isStreamEnd,
  isStreamFrame,
} from "../dist/src/index.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const schema = JSON.parse(readFileSync(path.join(here, "..", "schema", "protocol-v1.schema.json"), "utf8"));

function validate(def, value, p = "$") {
  if (def.$ref) {
    const name = def.$ref.replace("#/$defs/", "");
    return validate(schema.$defs[name], value, `${p}->${name}`);
  }
  if (def.oneOf) {
    // Discriminated union: valid iff EXACTLY one branch accepts (mirrors JSON Schema oneOf).
    const passing = def.oneOf.filter((sub) => validate(sub, value, p).length === 0).length;
    return passing === 1 ? [] : [`${p}: oneOf matched ${passing}`];
  }
  const errs = [];
  if ("const" in def) {
    if (value !== def.const) errs.push(`${p}: const`);
    return errs;
  }
  if ("enum" in def) {
    if (!def.enum.includes(value)) errs.push(`${p}: enum`);
    return errs;
  }
  switch (def.type) {
    case "object": {
      if (value === null || typeof value !== "object" || Array.isArray(value)) return [`${p}: object`];
      for (const req of def.required ?? []) if (!(req in value)) errs.push(`${p}.${req}: required`);
      for (const [k, sub] of Object.entries(def.properties ?? {})) {
        if (k in value) errs.push(...validate(sub, value[k], `${p}.${k}`));
      }
      // unknown properties are intentionally allowed (forward compatibility)
      break;
    }
    case "string":
      if (typeof value !== "string") errs.push(`${p}: string`);
      else if (def.minLength != null && value.length < def.minLength) errs.push(`${p}: minLength`);
      break;
    case "number":
      if (typeof value !== "number") errs.push(`${p}: number`);
      else if (def.exclusiveMinimum != null && value <= def.exclusiveMinimum) errs.push(`${p}: exclusiveMinimum`);
      break;
    case "boolean":
      if (typeof value !== "boolean") errs.push(`${p}: boolean`);
      break;
    case "array":
      if (!Array.isArray(value)) errs.push(`${p}: array`);
      else {
        if (def.minItems != null && value.length < def.minItems) errs.push(`${p}: minItems`);
        value.forEach((it, i) => errs.push(...validate(def.items, it, `${p}[${i}]`)));
      }
      break;
  }
  return errs;
}
const envelopeOk = (v) => validate(schema.$defs.EnvelopeV1, v).length === 0;
const ackOk = (v) => validate(schema.$defs.AckV1, v).length === 0;
const presenceOk = (v) => validate(schema.$defs.PresenceFrameV1, v).length === 0;
const signedPresenceOk = (v) => validate(schema.$defs.SignedPresenceFrameV1, v).length === 0;
const streamStartOk = (v) => validate(schema.$defs.StreamStart, v).length === 0;
const streamChunkOk = (v) => validate(schema.$defs.StreamChunk, v).length === 0;
const streamEndOk = (v) => validate(schema.$defs.StreamEnd, v).length === 0;
const streamFrameOk = (v) => validate(schema.$defs.StreamFrame, v).length === 0;
// Validate against the document ROOT (which $refs EnvelopeV1) — the entrypoint a
// third party uses when validating "against protocol-v1.schema.json".
const rootOk = (v) => validate(schema, v).length === 0;

const GOOD = Object.freeze({
  schemaVersion: "1.0",
  msgId: "m1",
  conversationId: "c1",
  senderAgentId: "agent-a",
  recipients: ["agent-b"],
  createdAt: "2026-06-21T00:00:00.000Z",
  payloadCiphertext: "ciphertext",
  payloadNonce: "nonce",
  signature: "sig",
});
const without = (key) => { const e = { ...GOOD }; delete e[key]; return e; };

test("schema bundle is a valid Draft 2020-12 $defs registry", () => {
  assert.equal(schema.$schema, "https://json-schema.org/draft/2020-12/schema");
  assert.ok(schema.$defs?.EnvelopeV1 && schema.$defs?.AckV1);
  assert.equal(schema.$defs.EnvelopeV1.properties.schemaVersion.const, "1.0");
});

test("schema ROOT validates an EnvelopeV1 and rejects non-envelopes", () => {
  // The root has a real validation target ($ref EnvelopeV1) — without it a validator
  // run against the file root would accept arbitrary input (the bug this guards).
  assert.equal(schema.$ref, "#/$defs/EnvelopeV1");
  assert.equal(rootOk(GOOD), true);
  assert.equal(rootOk({}), false);
  assert.equal(rootOk({ hello: "world" }), false);
  // an AckV1 is NOT an EnvelopeV1 — the root rejects it (acks validate via #/$defs/AckV1)
  assert.equal(rootOk({ msgId: "m", consumerId: "c", status: "ack", at: "2026-06-21T00:00:00Z" }), false);
});

test("a conformant EnvelopeV1 passes BOTH the schema and isEnvelopeV1", () => {
  assert.equal(envelopeOk(GOOD), true);
  assert.equal(isEnvelopeV1(GOOD), true);
});

test("schema and isEnvelopeV1 agree on every structural violation", () => {
  const bad = [
    ["schemaVersion const", { ...GOOD, schemaVersion: "2.0" }],
    ["missing msgId", without("msgId")],
    ["missing conversationId", without("conversationId")],
    ["empty senderAgentId", { ...GOOD, senderAgentId: "" }],
    ["empty recipients", { ...GOOD, recipients: [] }],
    ["non-string recipient", { ...GOOD, recipients: [1] }],
    ["empty-string recipient", { ...GOOD, recipients: [""] }],
    ["missing payloadCiphertext", without("payloadCiphertext")],
    ["missing payloadNonce", without("payloadNonce")],
    ["empty signature (unsigned)", { ...GOOD, signature: "" }],
  ];
  for (const [label, e] of bad) {
    assert.equal(envelopeOk(e), false, `schema must reject: ${label}`);
    assert.equal(isEnvelopeV1(e), false, `isEnvelopeV1 must reject: ${label}`);
  }
});

test("optional fields + forward-compatible unknown fields are accepted by both", () => {
  const e = { ...GOOD, ttlSeconds: 60, traceId: "t", sequence: 3, parentMsgId: "p", futureFieldV2: "x" };
  assert.equal(envelopeOk(e), true);
  assert.equal(isEnvelopeV1(e), true);
});

test("AckV1: required fields + status enum", () => {
  assert.equal(ackOk({ msgId: "m", consumerId: "c", status: "ack", at: "2026-06-21T00:00:00Z" }), true);
  assert.equal(ackOk({ msgId: "m", consumerId: "c", status: "nack", reason: "x", at: "2026-06-21T00:00:00Z" }), true);
  assert.equal(ackOk({ msgId: "m", consumerId: "c", status: "maybe", at: "2026-06-21T00:00:00Z" }), false);
  assert.equal(ackOk({ msgId: "m", status: "ack", at: "2026-06-21T00:00:00Z" }), false);
});

// Note: `createdAt` carries JSON-Schema `format: date-time` (advisory) and is enforced at
// runtime by isEnvelopeV1 via Date.parse — that one check lives in the runtime guard, not in
// this minimal validator, so date validity is intentionally not part of the agreement matrix.

// ---------------------------------------------------------------------------
// PresenceFrameV1 — discovery announcement (schema <-> isPresenceFrameV1)
// ---------------------------------------------------------------------------
const omit = (o, k) => { const c = { ...o }; delete c[k]; return c; };

const GOOD_PRESENCE = Object.freeze({
  presenceVersion: "1.0",
  agentId: "agent-a",
  encryptionPublicKey: "enc-pub",
  signingPublicKey: "sig-pub",
  subject: "msg.agent-a",
  capabilities: ["files", "audio"],
  ttlMs: 60000,
  ts: "2026-06-22T00:00:00.000Z",
  nonce: "n1",
});

test("a conformant PresenceFrameV1 passes BOTH the schema and isPresenceFrameV1", () => {
  assert.equal(presenceOk(GOOD_PRESENCE), true);
  assert.equal(isPresenceFrameV1(GOOD_PRESENCE), true);
  // empty capabilities + forward-compatible unknown fields accepted by both
  const e = { ...GOOD_PRESENCE, capabilities: [], futureFieldV2: "x" };
  assert.equal(presenceOk(e), true);
  assert.equal(isPresenceFrameV1(e), true);
});

test("schema and isPresenceFrameV1 agree on every structural violation", () => {
  const bad = [
    ["presenceVersion const", { ...GOOD_PRESENCE, presenceVersion: "2.0" }],
    ["missing agentId", omit(GOOD_PRESENCE, "agentId")],
    ["empty agentId", { ...GOOD_PRESENCE, agentId: "" }],
    ["empty encryptionPublicKey", { ...GOOD_PRESENCE, encryptionPublicKey: "" }],
    ["empty signingPublicKey", { ...GOOD_PRESENCE, signingPublicKey: "" }],
    ["empty subject", { ...GOOD_PRESENCE, subject: "" }],
    ["missing capabilities", omit(GOOD_PRESENCE, "capabilities")],
    ["capabilities not array", { ...GOOD_PRESENCE, capabilities: "files" }],
    ["capabilities non-string item", { ...GOOD_PRESENCE, capabilities: [1] }],
    ["missing ttlMs", omit(GOOD_PRESENCE, "ttlMs")],
    ["ttlMs not number", { ...GOOD_PRESENCE, ttlMs: "60000" }],
    ["ttlMs zero (exclusiveMinimum)", { ...GOOD_PRESENCE, ttlMs: 0 }],
    ["ttlMs negative", { ...GOOD_PRESENCE, ttlMs: -1 }],
    ["missing ts", omit(GOOD_PRESENCE, "ts")],
    ["missing nonce", omit(GOOD_PRESENCE, "nonce")],
    ["empty nonce", { ...GOOD_PRESENCE, nonce: "" }],
  ];
  for (const [label, e] of bad) {
    assert.equal(presenceOk(e), false, `schema must reject: ${label}`);
    assert.equal(isPresenceFrameV1(e), false, `isPresenceFrameV1 must reject: ${label}`);
  }
});

test("PresenceFrameV1: date-time validity is a runtime-only check (format is advisory in JSON Schema)", () => {
  // `ts` carries `format: date-time`, which Draft 2020-12 treats as an annotation, NOT an
  // assertion — a spec-compliant validator does not reject a malformed date. The runtime guard
  // (Date.parse) is stricter. (ttlMs > 0 IS enforced by the schema via exclusiveMinimum, so it
  // lives in the agreement matrix above, not here.)
  const e = { ...GOOD_PRESENCE, ts: "not-a-date" };
  assert.equal(presenceOk(e), true, "subset schema accepts (format is advisory)");
  assert.equal(isPresenceFrameV1(e), false, "runtime guard rejects via Date.parse");
});

// ---------------------------------------------------------------------------
// SignedPresenceFrameV1 — signed wrapper (schema <-> isSignedPresenceFrameV1)
// ---------------------------------------------------------------------------
const GOOD_SIGNED = Object.freeze({ frame: { ...GOOD_PRESENCE }, signature: "sig" });

test("SignedPresenceFrameV1: both accept a good frame and agree on violations", () => {
  assert.equal(signedPresenceOk(GOOD_SIGNED), true);
  assert.equal(isSignedPresenceFrameV1(GOOD_SIGNED), true);
  const bad = [
    ["missing signature", omit(GOOD_SIGNED, "signature")],
    ["empty signature", { ...GOOD_SIGNED, signature: "" }],
    ["missing frame", omit(GOOD_SIGNED, "frame")],
    ["frame not object", { ...GOOD_SIGNED, frame: "x" }],
    ["frame structurally invalid", { ...GOOD_SIGNED, frame: omit(GOOD_PRESENCE, "agentId") }],
  ];
  for (const [label, e] of bad) {
    assert.equal(signedPresenceOk(e), false, `schema must reject: ${label}`);
    assert.equal(isSignedPresenceFrameV1(e), false, `guard must reject: ${label}`);
  }
});

// ---------------------------------------------------------------------------
// Stream frames — StreamStart / StreamChunk / StreamEnd (schema <-> guards)
// ---------------------------------------------------------------------------
const GOOD_START = Object.freeze({ kind: "stream.start", streamId: "s1", chunkCount: 3, totalBytes: 100 });
const GOOD_CHUNK = Object.freeze({ kind: "stream.chunk", streamId: "s1", chunkIndex: 0, chunkCount: 3, data: "abc", isLast: false });
const GOOD_END = Object.freeze({ kind: "stream.end", streamId: "s1", chunkCount: 3, totalBytes: 100 });

test("StreamStart: schema and isStreamStart agree", () => {
  assert.equal(streamStartOk(GOOD_START), true);
  assert.equal(isStreamStart(GOOD_START), true);
  // optional fields accepted by both
  const withOpt = { ...GOOD_START, contentType: "text/plain", startedAt: "2026-06-22T00:00:00Z" };
  assert.equal(streamStartOk(withOpt), true);
  assert.equal(isStreamStart(withOpt), true);
  const bad = [
    ["wrong kind", { ...GOOD_START, kind: "stream.chunk" }],
    ["missing streamId", omit(GOOD_START, "streamId")],
    ["empty streamId", { ...GOOD_START, streamId: "" }],
    ["missing chunkCount", omit(GOOD_START, "chunkCount")],
    ["chunkCount not number", { ...GOOD_START, chunkCount: "3" }],
    ["missing totalBytes", omit(GOOD_START, "totalBytes")],
    ["contentType not string", { ...GOOD_START, contentType: 5 }],
  ];
  for (const [label, e] of bad) {
    assert.equal(streamStartOk(e), false, `schema must reject: ${label}`);
    assert.equal(isStreamStart(e), false, `isStreamStart must reject: ${label}`);
  }
});

test("StreamChunk: schema and isStreamChunk agree (incl. boolean isLast)", () => {
  assert.equal(streamChunkOk(GOOD_CHUNK), true);
  assert.equal(isStreamChunk(GOOD_CHUNK), true);
  const bad = [
    ["wrong kind", { ...GOOD_CHUNK, kind: "stream.end" }],
    ["missing streamId", omit(GOOD_CHUNK, "streamId")],
    ["empty streamId", { ...GOOD_CHUNK, streamId: "" }],
    ["missing chunkIndex", omit(GOOD_CHUNK, "chunkIndex")],
    ["chunkIndex not number", { ...GOOD_CHUNK, chunkIndex: "0" }],
    ["missing chunkCount", omit(GOOD_CHUNK, "chunkCount")],
    ["missing data", omit(GOOD_CHUNK, "data")],
    ["data not string", { ...GOOD_CHUNK, data: 5 }],
    // zero-byte data: the runtime (createStreamChunk + both reassemblers) throws
    // stream-chunk-data-required, so the contract rejects it — schema (minLength 1)
    // and guard (data.length > 0) agree.
    ["empty data", { ...GOOD_CHUNK, data: "" }],
    ["missing isLast", omit(GOOD_CHUNK, "isLast")],
    ["isLast not boolean", { ...GOOD_CHUNK, isLast: "false" }],
  ];
  for (const [label, e] of bad) {
    assert.equal(streamChunkOk(e), false, `schema must reject: ${label}`);
    assert.equal(isStreamChunk(e), false, `isStreamChunk must reject: ${label}`);
  }
});

test("StreamEnd: schema and isStreamEnd agree", () => {
  assert.equal(streamEndOk(GOOD_END), true);
  assert.equal(isStreamEnd(GOOD_END), true);
  const bad = [
    ["wrong kind", { ...GOOD_END, kind: "stream.start" }],
    ["missing streamId", omit(GOOD_END, "streamId")],
    ["empty streamId", { ...GOOD_END, streamId: "" }],
    ["missing chunkCount", omit(GOOD_END, "chunkCount")],
    ["chunkCount not number", { ...GOOD_END, chunkCount: "3" }],
    ["missing totalBytes", omit(GOOD_END, "totalBytes")],
  ];
  for (const [label, e] of bad) {
    assert.equal(streamEndOk(e), false, `schema must reject: ${label}`);
    assert.equal(isStreamEnd(e), false, `isStreamEnd must reject: ${label}`);
  }
});

test("StreamFrame union: oneOf schema and isStreamFrame agree on the discriminator", () => {
  for (const good of [GOOD_START, GOOD_CHUNK, GOOD_END]) {
    assert.equal(streamFrameOk(good), true, `oneOf must accept ${good.kind}`);
    assert.equal(isStreamFrame(good), true, `isStreamFrame must accept ${good.kind}`);
  }
  const notFrames = [
    ["unknown kind", { kind: "stream.bogus", streamId: "s1" }],
    ["empty object", {}],
    ["start missing required", omit(GOOD_START, "streamId")],
  ];
  for (const [label, e] of notFrames) {
    assert.equal(streamFrameOk(e), false, `oneOf must reject: ${label}`);
    assert.equal(isStreamFrame(e), false, `isStreamFrame must reject: ${label}`);
  }
});
