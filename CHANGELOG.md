# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
