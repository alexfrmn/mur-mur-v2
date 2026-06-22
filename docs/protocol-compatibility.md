# Protocol Compatibility Matrix

Companion to [`protocol-v1.md`](protocol-v1.md) (prose) and the machine-readable
[`packages/core/schema/protocol-v1.schema.json`](../packages/core/schema/protocol-v1.schema.json)
(JSON Schema, Draft 2020-12). The schema is the canonical wire contract; the runtime
guard `isEnvelopeV1` in `@murmurv2/core` mirrors it, and the conformance suite
(`packages/core/test/conformance.test.mjs`) asserts the two cannot drift.

## Versions

| `schemaVersion` | Status  | Envelope | Ack    | Notes |
|-----------------|---------|----------|--------|-------|
| `1.0`           | current | `EnvelopeV1` | `AckV1` | Shipped in v2.x. Only accepted wire version. Also covers the discovery (`PresenceFrameV1`, `SignedPresenceFrameV1`) and streaming (`StreamStart`/`StreamChunk`/`StreamEnd`) frames below. |

Only `EnvelopeV1` carries `schemaVersion` on the wire; the presence and stream frames
are versioned together with it (they ship and break as one protocol version) and are
discriminated structurally — `presenceVersion: "1.0"` for presence, `kind` for streams.

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
| `authToken` | — | string | non-empty if present; bearer (`MURMUR-AUTH:…`) |

**`authToken` is part of the signed payload.** When present it is appended to
`stableEnvelopePayload` in a fixed final position, so it cannot be stripped or swapped
without invalidating the signature. When absent, the signing payload is byte-identical
to envelopes from before the field existed (forward/backward compatible). Verification +
enforcement is a runtime concern (`@murmurv2/federation` `verifyAuthToken` /
`authorizeInbound`, gated by `MURMUR_ENFORCE_AUTH`), not a schema constraint.

## AckV1

| Field | Required | Type | Constraint |
|-------|----------|------|------------|
| `msgId` | ✅ | string | non-empty |
| `consumerId` | ✅ | string | non-empty |
| `status` | ✅ | string | enum `ack` \| `nack` |
| `at` | ✅ | string | ISO-8601 date-time |
| `reason` | — | string | |

## PresenceFrameV1 (discovery)

Public discovery metadata only — no secret. The signed wrapper proves integrity, not
identity; trust is an out-of-band operator promotion.

| Field | Required | Type | Constraint |
|-------|----------|------|------------|
| `presenceVersion` | ✅ | string | `const "1.0"` |
| `agentId` | ✅ | string | non-empty |
| `encryptionPublicKey` | ✅ | string | non-empty |
| `signingPublicKey` | ✅ | string | non-empty |
| `subject` | ✅ | string | non-empty |
| `capabilities` | ✅ | string[] | each a string (may be empty array) |
| `ttlMs` | ✅ | number | `> 0` (`exclusiveMinimum`) |
| `ts` | ✅ | string | ISO-8601 date-time (validity runtime-only) |
| `nonce` | ✅ | string | non-empty |

### SignedPresenceFrameV1

| Field | Required | Type | Constraint |
|-------|----------|------|------------|
| `frame` | ✅ | object | a valid `PresenceFrameV1` |
| `signature` | ✅ | string | non-empty (Ed25519 over the canonical frame) |

## Stream frames

Discriminated by `kind`. `StreamFrame` is the `oneOf` union of the three.

### StreamStart (`kind: "stream.start"`)

| Field | Required | Type | Constraint |
|-------|----------|------|------------|
| `kind` | ✅ | string | `const "stream.start"` |
| `streamId` | ✅ | string | non-empty |
| `chunkCount` | ✅ | number | declared total chunks |
| `totalBytes` | ✅ | number | declared total bytes |
| `contentType` | — | string | |
| `startedAt` | — | string | |

### StreamChunk (`kind: "stream.chunk"`)

| Field | Required | Type | Constraint |
|-------|----------|------|------------|
| `kind` | ✅ | string | `const "stream.chunk"` |
| `streamId` | ✅ | string | non-empty |
| `chunkIndex` | ✅ | number | |
| `chunkCount` | ✅ | number | |
| `data` | ✅ | string | **non-empty** (zero-byte chunk rejected: `stream-chunk-data-required`) |
| `isLast` | ✅ | boolean | |
| `sha256` | — | string | optional per-chunk integrity tag |

### StreamEnd (`kind: "stream.end"`)

| Field | Required | Type | Constraint |
|-------|----------|------|------------|
| `kind` | ✅ | string | `const "stream.end"` |
| `streamId` | ✅ | string | non-empty |
| `chunkCount` | ✅ | number | |
| `totalBytes` | ✅ | number | |
| `digest` | — | string | optional whole-stream integrity tag |
| `sha256` | — | string | optional whole-stream integrity tag |

## Entrypoints

`protocol-v1.schema.json` is a single file; validate each wire type against its target:

| Validate | Entrypoint |
|----------|------------|
| an inbound **envelope** | the document **root** (it `$ref`s `#/$defs/EnvelopeV1`) — so validating against the file directly is correct |
| an **ack** | `#/$defs/AckV1` |
| a **presence frame** | `#/$defs/PresenceFrameV1` |
| a **signed presence frame** | `#/$defs/SignedPresenceFrameV1` |
| a **stream frame** | `#/$defs/StreamFrame` (or a specific `#/$defs/StreamStart` \| `StreamChunk` \| `StreamEnd`) |

There is one canonical machine-readable schema (`packages/core/schema/protocol-v1.schema.json`);
no other protocol JSON schemas exist in the repo.

### Runtime-only checks (not assertable in JSON Schema)

The schema validates structural shape. A few checks live only in the runtime guards
and are intentionally **outside** the schema↔guard agreement matrices:

- **`createdAt` / `ts` date-time validity** — `format: date-time` is an advisory
  annotation in Draft 2020-12; the runtime guards enforce it via `Date.parse`
  (`isEnvelopeV1` for `EnvelopeV1.createdAt`, `isPresenceFrameV1` for `PresenceFrameV1.ts`).
- **`AckV1.at`** — generated as an ISO-8601 string by `createAck`, but there is **no
  `isAckV1` guard**: its `format: date-time` is advisory only (validator-dependent) with
  no runtime enforcement on read.
- **signature verification & payload decryption** — `@murmurv2/security`, not shape.
- **stream semantics** — `chunkIndex` bounds, `totalBytes` accounting, and
  `digest`/`sha256` matching are the reassembler's job, not the frame guards'.

## For third-party implementations

Validate inbound envelopes against `protocol-v1.schema.json` (the root) and reject on
failure; validate acks against its `#/$defs/AckV1`. The conformance suite is the
reference behaviour for accept/reject decisions; run it (or port its fixtures) to check
an independent implementation against this contract.
