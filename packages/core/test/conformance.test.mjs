// Conformance suite for the Murmur V2 wire protocol (schemaVersion 1.0).
// Validates fixtures against the machine-readable JSON Schema (schema/protocol-v1.schema.json)
// AND asserts the schema and the runtime guard isEnvelopeV1 AGREE on every structural case —
// so the spec and the implementation can't silently drift. No external validator dep: a tiny
// subset validator interprets the flat protocol $defs (type/required/const/enum/minLength/minItems/items).
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isEnvelopeV1 } from "../dist/src/index.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const schema = JSON.parse(readFileSync(path.join(here, "..", "schema", "protocol-v1.schema.json"), "utf8"));

function validate(def, value, p = "$") {
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
