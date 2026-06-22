# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Pending
- **Auth enforcement end-to-end** — the broker ingress hook + `authorizeInbound` exist; the daemon does not yet wire them (so `MURMUR_ENFORCE_AUTH` is not enforced end-to-end). Requires daemon roster/identity wiring + token provisioning.
- **npm republish @ 0.2.0** — `@murmurv2/core` (and dependents) export new API this release (`stableEnvelopePayload`, `EnvelopeV1.authToken`, stream guards, `authorizeInbound`, `InboundAuthorizer`); consumers importing them need a republished core (published `0.1.0` predates them).

## [2.3.0] - 2026-06-22

### Added

- **Agent discovery — complete.** Presence frames + candidate registry (ttl expiry, dedupe, out-of-order guard); signed presence with NATS `announcePresence`/`subscribePresence`; operator promote-flow (`queryCandidates` + `promoteCandidate`) returning the live nested peer-config entry. Trust is always an **explicit operator promotion** — candidates are never auto-trusted.
- **Message streaming — complete.** Stream frames (`stream.start`/`chunk`/`end`), UTF-8-safe chunking, in-memory + durable SQLite reassembly (out-of-order, idempotent, conflict-reject), backpressure (chunk + byte windows), sha256 per-chunk/whole-stream integrity, and an ACK-window.
- **Auth/authz enforcement mechanism** — `signAuthToken`/`verifyAuthToken` now carry a signed **`subject`** (actor); `EnvelopeV1` gains an optional, signed **`authToken`** (bearer `MURMUR-AUTH:…`, appended to the canonical payload only when present → byte-identical back-compat when absent); `@murmurv2/federation` `authorizeInbound` verifies it and binds `subject === senderAgentId`; `@murmurv2/broker-nats` enforces at ingress via an injected `InboundAuthorizer` hook behind `MURMUR_ENFORCE_AUTH` (default OFF, NACK `auth-rejected:<reason>`, never delivered). *Daemon end-to-end wiring pending (see Unreleased).*
- **Conformance suite — extended to every wire type.** `PresenceFrameV1`, `SignedPresenceFrameV1`, `StreamStart`/`StreamChunk`/`StreamEnd` (+ a discriminated `StreamFrame` `oneOf`) added to `protocol-v1.schema.json` and to schema↔runtime-guard agreement matrices; new structural guards `isStreamStart`/`isStreamChunk`/`isStreamEnd`/`isStreamFrame`.
- **Versioned protocol spec.** `docs/protocol-v1.md` (prose lifecycle for envelope, discovery, streaming) + `docs/protocol-compatibility.md` (field tables + per-type validation entrypoints + runtime-only-checks boundary) covering all wire types.

### Changed

- **`stableEnvelopePayload` centralized** into `@murmurv2/core` as the single canonical signing form (was byte-identically copy-pasted across 7 sites: mcp-server, daemon, bridge-a2a, shell-send, demos, agent-runner example, federation live test). Golden-locked by test.

### Fixed

- De-flaked the `mcp-request-reply` C2 long-poll-timeout test (real-timer boundary race → injectable fake clock).

### Validated

- **Real cross-host A2A.** A fresh Murmur agent deployed on Phoenix/agent-hq over the **published** `@murmurv2/*` packages connected to the live broker over Tailscale and exchanged **bidirectional** encrypt/verify/ACK messages with JARVIS — exercising the mesh across real hosts and network (closes the "real mesh deploy" mechanism gate; a second real partner *org* for federation remains an external gate).

## [2.2.0] - 2026-06-22

### Added

- **Published on npm.** All `@murmurv2/*` packages are public on the npm registry @ `0.1.0` (MIT), under the `murmurv2` org. Publish tooling: `scripts/prep-publish.mjs` (private→public, license, `publishConfig`, intra-workspace `file:`→`^0.1.0`, per-package `prepack` build guard, `files: dist/src + LICENSE`) and `scripts/publish-all.mjs` (root build → topological order → per-tarball assertion that `dist/src/index.{js,d.ts}` exist → publish). `@murmurv2/broker-ws` ships in the next release.
- **WebSocket transport adapter** — `@murmurv2/broker-ws`: relay server + broker client with envelope delivery, ACK correlation, dedupe, and invalid-envelope NACKs, reusing the core primitives. Browser/edge deployment examples pending.
- **Roster-backed auth tokens** — `@murmurv2/federation`: `signAuthToken`/`verifyAuthToken` issue Ed25519-signed tokens with audience, scopes, and `nbf`/`exp` windows; the issuer verify key is resolved from the verified roster (no embedded trust root).
- **`RosterStore`** — `@murmurv2/federation`: pinned-key trust + monotonic-version replay guard (rejects stale/downgraded rosters) + trust-epoch reset on key rotation.
- **Machine-readable protocol schema + conformance** — `@murmurv2/core/schema/protocol-v1.schema.json` (Draft 2020-12; root validates `EnvelopeV1`, `#/$defs/AckV1` for acks) + `docs/protocol-compatibility.md` matrix; the conformance suite asserts the schema and `isEnvelopeV1` agree on every accept/reject.
- **Federation live interop (in isolation)** — cross-org sealed+signed delivery proven over real NATS accounts and a leaf-node topology with least-privilege publish/subscribe boundaries (`packages/federation-nats/integration/`); a NATS accounts-config renderer generates each org's account contract.
- **ACP autonomy loop** — idempotent Murmur→ACP task producer + a gated send-boundary worker client.

### Changed

- README, file tree, and Roadmap synced to the real state, with honest scoping for in-isolation / mock-counterpart features.

## [2.1.0] - 2026-06-21

### Added

- **JetStream durability (opt-in).** Optional NATS JetStream durable consumers behind the existing broker/outbox interface — finite `max_deliver` (default 5) + `ack_wait` (default 30s), automatic repair of drifted consumers, retryable-failure `nak()` for broker redelivery, and poison-message terminal ACK. Default-OFF; enable with `MURMUR_JETSTREAM=1` or `config.jetstream.enabled`. The SQLite outbox remains the transactional source of truth.
- **JetStream advisory → DLQ.** `startJetStreamAdvisoryDlq` routes `MAX_DELIVERIES` / `MSG_TERMINATED` advisories to the outbox dead-letter sink.
- **Federation (cross-org).** New `@murmurv2/federation` — `org/agentId` addressing (bare id ⇒ local org) and an Ed25519-signed per-org key directory (roster) — and `@murmurv2/federation-nats` — NATS leaf-node / per-org account `fed.*` subject contract with subject-safe token encoding and account export/import isolation. Payload stays E2E-opaque across federation.
- **A2A bridge skeleton.** `@murmurv2/bridge-a2a` terminates the industry-standard A2A protocol (`@a2a-js/sdk`) and re-wraps tasks as internal Murmur E2E envelopes.
- **Native wake self-heal.** Codex app-server wake threads are re-seeded automatically when missing/stale; WS-over-UDS transport for the Codex app-server.

### Changed

- Wake/notify runtime no longer routes through OpenClaw or tmux persistent injection; native Claude/Codex wake plus Telegram notify are the supported paths.

### Security

- `verifyRoster` verifies a federation roster against a caller-pinned org key, not the roster's own embedded key — prevents an attacker from publishing a self-signed forged roster.

## [2.0.0] - 2026-06-20

### Added

- `murmur_request` send-and-wait tool for synchronous request/response over NATS.
- Mandatory WakeMonitor with deduplication, loop-breaker, audit-gate, and drain guards.
- WakeMonitor stateless and persistent wake modes.

### Fixed

- ACK routing now targets the original sender, not the consumer.
- Reconnect resilience defaults for long-running NATS clients.

### Changed

- Transport documentation now reflects core NATS plus SQLite outbox behavior without claiming JetStream durability.
- Security bump: `ws` upgraded to 8.21.0.

## [0.2.0] - 2026-03-26

### Added

- Deduplication by sender + conversationId + msgId with max 3 attempts before dead-letter ([109f27f])
- Bidirectional Murmur -- auto-reply OpenClaw responses via NATS ([2cc2d41])
- Observatory dashboard with 3D visualization ([58bf271])
- Bridge inbound Mur-Mur messages into OpenClaw sessions ([960b1d0])
- Operations guide covering queues, retry policy, and troubleshooting ([e302a83])

### Fixed

- Murmur resilience -- OpenClaw fallback + WAL busy_timeout ([5bd0e80])
- Rewrite on-receive-openclaw.mjs to use CLI instead of broken cron tool ([aaec353])
- Dead-letter on 400 responses + truncate Telegram messages over 4000 chars ([4324488])
- Bridge timeout increased to 120s, atomic claimDue, flush mutex ([a3d95ad])

### Changed

- NATS keepalive: 30s ping interval, infinite reconnect, named connections ([b67d64b])

## [0.1.0] - 2026-02-11

### Added

- Durable unified notify queue with quick init presets ([e7069d0])
- Invite-based peer setup -- 3 commands, zero JSON editing ([6a60294])

[Unreleased]: https://github.com/alexfrmn/mur-mur-v2/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/alexfrmn/mur-mur-v2/compare/v0.2.0...v2.0.0
[0.2.0]: https://github.com/alexfrmn/mur-mur-v2/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/alexfrmn/mur-mur-v2/releases/tag/v0.1.0
