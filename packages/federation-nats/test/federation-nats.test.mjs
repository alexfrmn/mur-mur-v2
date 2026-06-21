import assert from "node:assert/strict";
import test from "node:test";
import {
  buildFederationAccountContract,
  buildFederationNatsAccountConfig,
  decodeFederationToken,
  encodeFederationToken,
  federationAckSubject,
  federationMessageSubject,
  parseFederationSubject,
  renderFederationNatsAccountsConfig,
  routeFederatedEnvelope,
} from "../dist/src/index.js";

const envelope = Object.freeze({
  schemaVersion: "1.0",
  msgId: "msg-fed-1",
  conversationId: "conv-fed",
  senderAgentId: "agent-jarvis",
  recipients: ["partner.org/agent.codex"],
  createdAt: "2026-06-21T18:30:00.000Z",
  payloadCiphertext: "opaque-ciphertext",
  payloadNonce: "opaque-nonce",
  signature: "opaque-signature",
});

test("maps local and remote addresses to deterministic federation subjects", () => {
  assert.equal(federationMessageSubject("agent-codex", "aimindset"), "fed.aimindset.msg.agent-codex");
  assert.equal(federationAckSubject("partner/agent-jarvis", "aimindset"), "fed.partner.ack.agent-jarvis");
});

test("round-trips dotted address parts without subject-token ambiguity", () => {
  const subject = federationMessageSubject("partner.org/agent.codex", "aimindset");
  assert.equal(subject, "fed._xcGFydG5lci5vcmc.msg._xYWdlbnQuY29kZXg");
  assert.deepEqual(parseFederationSubject(subject), {
    kind: "msg",
    address: {
      org: "partner.org",
      agentId: "agent.codex",
    },
  });
});

test("rejects wildcard and separator injection in address parts", () => {
  assert.throws(() => federationMessageSubject("partner/agent.*", "aimindset"), /invalid agentId/);
  assert.throws(() => federationMessageSubject("partner>/agent", "aimindset"), /invalid org/);
  assert.throws(() => federationMessageSubject("partner/agent/sub", "aimindset"), /nested slash/);
});

test("routes envelopes without inspecting or mutating ciphertext fields", () => {
  const route = routeFederatedEnvelope(envelope, "partner.org/agent.codex", "aimindset");
  assert.equal(route.subject, "fed._xcGFydG5lci5vcmc.msg._xYWdlbnQuY29kZXg");
  assert.equal(route.envelope, envelope);
  assert.equal(route.envelope.payloadCiphertext, "opaque-ciphertext");
  assert.equal(route.envelope.payloadNonce, "opaque-nonce");
  assert.equal(route.envelope.signature, "opaque-signature");
});

test("builds account export/import contract scoped to each org subject prefix", () => {
  assert.deepEqual(buildFederationAccountContract("aimindset", ["partner.org", "lab-2"]), {
    localOrg: "aimindset",
    localAccount: "ORG_AIMINDSET",
    exports: ["fed.aimindset.>"],
    imports: [
      {
        account: "ORG__XCGFYDG5LCI5VCMC",
        subject: "fed._xcGFydG5lci5vcmc.>",
      },
      {
        account: "ORG_LAB_2",
        subject: "fed.lab-2.>",
      },
    ],
  });
});

test("builds production-facing NATS account config with partner-scoped service exports", () => {
  assert.deepEqual(buildFederationNatsAccountConfig({
    localOrg: "aimindset",
    partnerOrgs: ["partner.org"],
    users: [{ user: "leaf-aimindset", password: "secret" }],
  }), {
    account: "ORG_AIMINDSET",
    users: [{ user: "leaf-aimindset", password: "secret" }],
    exports: [{
      service: "fed.aimindset.>",
      accounts: ["ORG__XCGFYDG5LCI5VCMC"],
    }],
    imports: [{
      service: {
        account: "ORG__XCGFYDG5LCI5VCMC",
        subject: "fed._xcGFydG5lci5vcmc.>",
      },
    }],
  });
});

test("renders static NATS accounts config from the federation contract", () => {
  const conf = renderFederationNatsAccountsConfig({
    orgs: ["aimindset", "partner.org"],
    port: 4223,
    usersByOrg: {
      aimindset: [{ user: "aimindset-leaf", password: "pw-a" }],
      "partner.org": [{ user: "partner-leaf", password: "pw-b" }],
    },
  });
  assert.match(conf, /^port: 4223\naccounts \{/);
  assert.match(conf, /ORG_AIMINDSET \{/);
  assert.match(conf, /\{ user: "aimindset-leaf", password: "pw-a" \}/);
  assert.match(conf, /\{ service: "fed\.aimindset\.>", accounts: \["ORG__XCGFYDG5LCI5VCMC"\] \}/);
  assert.match(conf, /\{ service: \{ account: "ORG__XCGFYDG5LCI5VCMC", subject: "fed\._xcGFydG5lci5vcmc\.>" \} \}/);
  assert.match(conf, /ORG__XCGFYDG5LCI5VCMC \{/);
});

test("keeps private exports non-public even when an org has no configured partners", () => {
  const conf = renderFederationNatsAccountsConfig({ orgs: ["aimindset"] });
  assert.match(conf, /\{ service: "fed\.aimindset\.>", accounts: \[\] \}/);
});

test("encoded federation tokens are reversible", () => {
  const token = encodeFederationToken("_xreserved.name");
  assert.equal(token, "_xX3hyZXNlcnZlZC5uYW1l");
  assert.equal(decodeFederationToken(token), "_xreserved.name");
});
