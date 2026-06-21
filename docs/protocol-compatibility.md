# Protocol Compatibility Matrix

Companion to [`protocol-v1.md`](protocol-v1.md) (prose) and the machine-readable
[`packages/core/schema/protocol-v1.schema.json`](../packages/core/schema/protocol-v1.schema.json)
(JSON Schema, Draft 2020-12). The schema is the canonical wire contract; the runtime
guard `isEnvelopeV1` in `@murmurv2/core` mirrors it, and the conformance suite
(`packages/core/test/conformance.test.mjs`) asserts the two cannot drift.

## Versions

| `schemaVersion` | Status  | Envelope | Ack    | Notes |
|-----------------|---------|----------|--------|-------|
| `1.0`           | current | `EnvelopeV1` | `AckV1` | Shipped in v2.x. Only accepted wire version. |

## Compatibility policy

- **Single wire version.** `schemaVersion` is a `const "1.0"`; envelopes with any
  other value are rejected (schema + `isEnvelopeV1`).
- **Forward-compatible reads.** Unknown top-level fields are **permitted and ignored**
  (no `additionalProperties: false`). A future minor MAY add optional fields without
  bumping `schemaVersion`; older consumers ignore them. Current optional fields:
  `ttlSeconds`, `traceId`, `sequence`, `parentMsgId`.
- **Breaking change ⇒ new version.** Removing/renaming a required field, changing a
  type, or tightening an enum bumps `schemaVersion` (e.g. `2.0`). Consumers gate on it.
- **Signature/crypto are out of band of the schema.** The schema validates *shape*;
  `signature` non-emptiness is required, but signature *verification* and payload
  decryption are runtime concerns (`@murmurv2/security`), not JSON-Schema constraints.
- **`createdAt`** carries `format: date-time` (advisory in JSON Schema) and is
  enforced at runtime by `isEnvelopeV1` via `Date.parse`.

## Required vs optional (EnvelopeV1)

| Field | Required | Type | Constraint |
|-------|----------|------|------------|
| `schemaVersion` | ✅ | string | `const "1.0"` |
| `msgId` | ✅ | string | non-empty |
| `conversationId` | ✅ | string | non-empty |
| `senderAgentId` | ✅ | string | non-empty |
| `recipients` | ✅ | string[] | ≥1, each non-empty |
| `createdAt` | ✅ | string | ISO-8601 date-time |
| `payloadCiphertext` | ✅ | string | non-empty |
| `payloadNonce` | ✅ | string | non-empty |
| `signature` | ✅ | string | non-empty (on the wire) |
| `ttlSeconds` | — | number | |
| `traceId` | — | string | |
| `sequence` | — | number | |
| `parentMsgId` | — | string | |

## AckV1

| Field | Required | Type | Constraint |
|-------|----------|------|------------|
| `msgId` | ✅ | string | non-empty |
| `consumerId` | ✅ | string | non-empty |
| `status` | ✅ | string | enum `ack` \| `nack` |
| `at` | ✅ | string | ISO-8601 date-time |
| `reason` | — | string | |

## Entrypoints

`protocol-v1.schema.json` is a single file with two validation targets:

| Validate | Entrypoint |
|----------|------------|
| an inbound **envelope** | the document **root** (it `$ref`s `#/$defs/EnvelopeV1`) — so validating against the file directly is correct |
| an **ack** | `#/$defs/AckV1` |

There is one canonical machine-readable schema (`packages/core/schema/protocol-v1.schema.json`);
no other EnvelopeV1/AckV1 JSON schemas exist in the repo.

## For third-party implementations

Validate inbound envelopes against `protocol-v1.schema.json` (the root) and reject on
failure; validate acks against its `#/$defs/AckV1`. The conformance suite is the
reference behaviour for accept/reject decisions; run it (or port its fixtures) to check
an independent implementation against this contract.
