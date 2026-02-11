# ADR-001: Use NATS JetStream as core message bus

## Status
Accepted

## Context
Current Murmur workflow is effective for ad-hoc agent coordination but lacks strict delivery semantics, replay, DLQ, and scalable fanout.

## Decision
Adopt NATS JetStream as messaging control-plane bus.

## Consequences
### Positive
- durable streams, replay, consumer cursors
- ack/nack/retry and dead-letter queue patterns
- low-latency and simple operations footprint

### Tradeoffs
- additional infra component to operate
- still need app-level E2E envelope layer for payload privacy
