# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Wake/notify runtime no longer routes through OpenClaw or tmux persistent injection; native Claude/Codex wake plus Telegram notify are the supported paths.

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
