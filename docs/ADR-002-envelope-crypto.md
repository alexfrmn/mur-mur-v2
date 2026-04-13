# ADR-002: Canonical signed/encrypted envelope

## Status
Accepted

## Context
Transport/protocol encryption alone is insufficient for multi-bridge routing and durable storage requirements.

## Decision
Use app-layer canonical envelope with:
- immutable metadata fields
- detached Ed25519 signature
- encrypted payload (target: X25519 + XChaCha20-Poly1305)

## Consequences
### Positive
- consistent security semantics across bridges
- tamper-evident messaging
- better auditability and replay validation

### Tradeoffs
- key lifecycle management complexity
- envelope validation and crypto overhead
