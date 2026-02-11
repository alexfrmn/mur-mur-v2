# mur-mur-v2

Reliable, secure multi-agent messaging core for OpenClaw-style agent systems.

## Goal
Build a **better Murmur-compatible architecture** with:
- guaranteed delivery (at-least-once + idempotency)
- strong security (service mTLS + app-level signed/encrypted envelopes)
- multi-agent + human-in-the-loop messaging
- observability and replay

## Architecture (v1)
- **Core bus:** NATS JetStream
- **Canonical envelope:** immutable JSON schema (`schema/envelope.v1.json`)
- **Bridges:** Murmur, OpenClaw sessions, Telegram
- **Policy engine:** capability-based routing/authz
- **State store:** Postgres for idempotency, checkpoints, cursors

## Repository Layout

```text
/docs                 ADRs + protocol docs
/schema               JSON schemas for envelope/ack/policy
/packages/core        ids, envelope validation, ordering, idempotency helpers
/packages/broker-nats producer/consumer/retry/DLQ scaffolding
/packages/bridge-*    adapters (murmur/openclaw/telegram)
/packages/security    signing/encryption/key helpers
/packages/observability metrics/tracing helpers
```

## Milestones

### Phase 0 — harden current flow
- [ ] real Murmur wake hook ingestion
- [ ] checkpoint store (`last_seen_message_id`)
- [ ] idempotent execute table

### Phase 1 — MVP bus
- [ ] envelope v1 schema + validation
- [ ] NATS streams + ACK/NACK + retry + DLQ
- [ ] Murmur bridge + OpenClaw bridge

### Phase 2 — security/HITL
- [ ] app-level E2E envelope crypto
- [ ] key rotation + revocation
- [ ] human approval queue for sensitive actions

### Phase 3 — production
- [ ] SLOs, load tests, chaos tests, DR runbooks

## Quick Start (dev)

```bash
# from repo root
npm install
npm run build
```

## Run local MVP demo (NATS + 2 agents)

Prerequisite: Docker/Compose installed.

```bash
npm run demo:up
npm run demo:consumer
# in second terminal
npm run demo:producer
```

Expected result: consumer prints received canonical envelope metadata (`msgId`, `conversationId`, `senderAgentId`).

Stop demo:

```bash
npm run demo:down
```

## Hand-off to Claude/JARVIS

Share this exact flow:
1. Clone `alexfrmn/mur-mur-v2`
2. `npm install`
3. `npm run demo:up`
4. Run consumer and producer scripts
5. Confirm envelope exchange over subject `msg.demo`

## Current status
- scaffold + ADR docs committed
- local broker demo added
- `@murmurv2/core`: envelope guards + ACK helper + in-memory idempotency + **file-persistent** dedupe/outbox stores
- `@murmurv2/broker-nats`: real connect/publish + subscribe-with-ack path with dedupe checks
- `@murmurv2/broker-nats`: outbox flush skeleton with retry/backoff + DLQ status transitions

Next: ACK correlation to mark outbox records `acked`, then SQLite/Postgres optimistic-locking store.
