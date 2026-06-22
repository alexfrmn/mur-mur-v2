# Protocol v1

Murmur V2's wire protocol, `schemaVersion` **1.0**. The canonical machine-readable
contract is [`packages/core/schema/protocol-v1.schema.json`](../packages/core/schema/protocol-v1.schema.json)
(JSON Schema, Draft 2020-12); the `@murmurv2/core` runtime guards mirror it, and the
[conformance suite](../packages/core/test/conformance.test.mjs) asserts the schema and
the guards cannot drift. Versioning and forward-compatibility rules live in
[`protocol-compatibility.md`](protocol-compatibility.md).

## Wire types

| Type | Purpose | Schema `$def` | Runtime guard |
|------|---------|---------------|---------------|
| `EnvelopeV1` | encrypted inbound message | document root (`#/$defs/EnvelopeV1`) | `isEnvelopeV1` |
| `AckV1` | delivery acknowledgement | `#/$defs/AckV1` | — |
| `PresenceFrameV1` | discovery announcement (public metadata) | `#/$defs/PresenceFrameV1` | `isPresenceFrameV1` |
| `SignedPresenceFrameV1` | Ed25519-signed presence | `#/$defs/SignedPresenceFrameV1` | `isSignedPresenceFrameV1` |
| `StreamStart` / `StreamChunk` / `StreamEnd` | chunked payload streaming | `#/$defs/Stream*` (+ `StreamFrame` union) | `isStreamStart` / `isStreamChunk` / `isStreamEnd` / `isStreamFrame` |

Envelope message payloads are encrypted on the wire; presence frames are intentionally
**public, signed cleartext** metadata (no secret). In all cases the schema validates
**shape**, while signature verification and payload decryption are runtime concerns
(`@murmurv2/security`).

## Envelope lifecycle
1. Producer builds `EnvelopeV1`
2. Producer signs envelope and encrypts payload
3. Publish to subject `msg.<conversationId>`
4. Consumer validates schema+signature
5. Consumer processes idempotently using `msgId`
6. Consumer emits ACK or NACK
7. Retry policy moves failed messages; terminal failures go to DLQ

An optional `authToken` (bearer `MURMUR-AUTH:…`) authorizes the sender. When present it
is part of the signed payload (cannot be stripped/swapped) and can be verified with
`@murmurv2/federation` `verifyAuthToken`; ingress enforcement (an `authorizeInbound`
helper gated by `MURMUR_ENFORCE_AUTH`) is forthcoming in auth/authz #47 PR-D. Absent on
un-authenticated envelopes, which sign byte-identically to before the field existed —
see [`protocol-compatibility.md`](protocol-compatibility.md).

## Delivery model
- at-least-once delivery
- idempotent consumers mandatory
- per-conversation sequence ordering target

## Discovery (presence)

Discovery never confers trust automatically — a presence frame proves only message
integrity, never that an `agentId` is who it claims. Trust is established out of band
by a deliberate operator promotion.

1. An agent announces a `PresenceFrameV1` — public keys, `subject`, `capabilities`,
   `ttlMs`, ISO-8601 `ts`, and a per-announcement `nonce` — on a discovery subject.
2. Announcements are wrapped as a `SignedPresenceFrameV1` (Ed25519 signature over the
   canonical frame). A listener verifies the signature against the key the frame
   advertises before folding it in (`announcePresence` / `subscribePresence` on the
   NATS broker; verification injected from `@murmurv2/security`).
3. Observers collect frames into a `CandidateRegistry` as **untrusted**
   `DiscoveryCandidate`s — deduped by (`agentId`, `nonce`), expiring at `ts + ttlMs`.
4. An operator (or an approved policy) introspects the roster with `queryCandidates`
   and explicitly promotes one with `promoteCandidate`, which returns the nested
   peer-config entry to wire into `peers[agentId]`. Promotion does not mutate the
   registry; the candidate stays untrusted until the operator applies the entry.

## Streaming

Large payloads are sent as an ordered sequence of stream frames sharing a `streamId`.

1. Producer opens with `StreamStart` — declared `chunkCount` and `totalBytes`
   (optional `contentType`, `startedAt`).
2. Producer emits ordered `StreamChunk`s — `chunkIndex`, **non-empty** `data`, an
   optional per-chunk `sha256`, and `isLast`. (A zero-byte chunk is rejected:
   `stream-chunk-data-required`.)
3. Producer closes with `StreamEnd` — optional whole-stream `digest` / `sha256`
   integrity tags.
4. Receiver reassembles idempotently by `streamId` (in-memory or SQLite-durable):
   duplicate chunks are no-ops, conflicting re-sends are rejected, integrity tags are
   verified, and completion yields the reassembled payload. Backpressure bounds the
   in-flight chunk and byte windows.

## Bridge mapping
- Murmur message -> EnvelopeV1
- OpenClaw session event -> EnvelopeV1
- Human channel events (Telegram etc.) -> EnvelopeV1
