# Murmur Federation NATS Subject Contract

This document defines the first NATS-facing contract for issue #14. It is an
application-level contract and does not require restarting the shared broker for
unit smoke.

## Addressing

Federated recipients use `org/agentId`. Bare `agentId` addresses resolve to the
local org before routing.

Message subjects:

```text
fed.<org-token>.msg.<agent-token>
```

ACK subjects:

```text
fed.<org-token>.ack.<agent-token>
```

`org-token` and `agent-token` are subject-safe encodings of the address parts.
Plain `[A-Za-z0-9_-]+` values are emitted unchanged. Values containing `.`, a
reserved `_x` prefix, or other non-token characters are encoded as
`_x<base64url(utf8)>`. Address parts containing NATS wildcards, spaces, or `/`
are rejected before subject construction.

Examples:

```text
aimindset/agent-codex  -> fed.aimindset.msg.agent-codex
partner.org/agent.v1   -> fed._xcGFydG5lci5vcmc.msg._xYWdlbnQudjE
```

## Account Boundary

Each org account exports only its own prefix and imports partner prefixes:

```text
ORG_AIMINDSET exports fed.aimindset.>
ORG_AIMINDSET imports fed._xcGFydG5lci5vcmc.> from ORG__XCGFYDG5LCI5VCMC
```

This keeps federation routing separate from local mesh subjects and prevents a
partner account from publishing under another org's prefix when the broker is
configured with matching account exports/imports.

## E2E Boundary

Federation routing only chooses subjects. It does not decrypt, parse, rewrite,
or re-sign `EnvelopeV1.payloadCiphertext`, `payloadNonce`, or `signature`.
Roster/auth code must validate organization identity before a live bridge
accepts partner traffic.

## Smoke Harness

`@murmurv2/federation-nats` includes a pure smoke test that verifies:

- deterministic local and remote subject mapping;
- dotted org/agent IDs round-trip through subject-safe tokens;
- wildcard and slash injection are rejected;
- envelopes keep ciphertext fields unchanged through routing;
- account export/import prefixes match the subject contract.

Live NATS smoke should use this contract with isolated accounts or a dedicated
broker. Do not restart the shared production broker for this issue's smoke.
