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

For production/static NATS config, generate account blocks from the package API
instead of hand-maintaining exports/imports:

```js
import { renderFederationNatsAccountsConfig } from "@murmurv2/federation-nats";

process.stdout.write(renderFederationNatsAccountsConfig({
  orgs: ["aimindset", "partner.org"],
  usersByOrg: {
    aimindset: [{ user: "leaf-aimindset", password: process.env.AIMINDSET_LEAF_PASSWORD }],
    "partner.org": [{ user: "leaf-partner", password: process.env.PARTNER_LEAF_PASSWORD }],
  },
}));
```

The rendered config uses NATS service export/import for the `fed.<org>.>`
prefixes. Exports are partner-scoped by default:

```text
ORG_AIMINDSET {
  exports: [{ service: "fed.aimindset.>", accounts: ["ORG__XCGFYDG5LCI5VCMC"] }]
  imports: [{ service: { account: "ORG__XCGFYDG5LCI5VCMC", subject: "fed._xcGFydG5lci5vcmc.>" } }]
}
```

This matches the live interop finding: an importing account may publish to the
exporter's service subject, so one-way reply-less `fed.*` message delivery does
not require switching the subject contract to stream import/export.

## Real Mesh Leaf-Node Wiring

Deployment target for issue #14 is one federation account per org, connected by
leaf nodes or operator-mode accounts. Do not restart the shared production broker
for smoke tests; validate with isolated local brokers first.

Minimum real-mesh shape:

```text
local org account:
  account: ORG_<LOCAL>
  exports:
    - service: fed.<local-token>.>
      accounts: [ORG_<PARTNER>]
  imports:
    - service:
        account: ORG_<PARTNER>
        subject: fed.<partner-token>.>

leaf connection:
  bind the local leaf remote to ORG_<LOCAL>
  restrict leaf-user publish/subscribe permissions to fed.<local-token>.> and fed.<partner-token>.>
```

Before production wiring, run an operator/leaf-node smoke with the generated
account config and restricted leaf users. The acceptance check is:

1. org A can publish `fed.<partner>.msg.<agent>` and org B receives it.
2. org A cannot publish outside imported partner prefixes.
3. org B cannot subscribe outside its local exported prefix.
4. reply-less publish works without `allow_responses`; if request/reply is added
   later, configure response permissions explicitly for the responder user.

For operator/JWT deployments, keep the same service export/import subjects in
account JWTs. The static config renderer is the source-of-truth template, not a
separate contract.

## Production Trust Store

Roster `signingPublicKey` is metadata, not a trust root. Production federation
must pin partner org signing keys out-of-band, for example in deployment config
or an operator-managed trust store:

```json
{
  "partner.org": {
    "account": "ORG__XCGFYDG5LCI5VCMC",
    "rosterSigningPublicKey": "base64url-ed25519-public-key"
  }
}
```

Bridge/runtime code should reject rosters whose signature does not verify
against the pinned key and should enforce monotonic roster versions to prevent
replay. Version/replay state belongs in the bridge runtime, not in the NATS
subject contract.

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
