// @murmurv2/federation — cross-org addressing + signed peer/key directory.
// Federation E4 / track #14, JARVIS's half of the split.
//   - JARVIS (here): org/agent addressing API + signed key directory (roster).
//   - CODEX-VOLT:     NATS leaf-node / per-org account `fed.*` subject contract + smoke.
//
// Interlock: an address `org/agentId` selects the transport subject on Codex's side;
// before any cross-org encrypt/verify, `bridge-murmur` fetches the PARTNER org's
// roster (this module) and verifies its Ed25519 signature against a pinned org key.
// The roster is the ONLY net-new crypto surface federation adds (PRD E4, line 126).
//
// Joint back-compat invariant: a BARE `agentId` (no slash) ⇒ the LOCAL org — so
// today's single-org mesh keeps working untouched.
//
// STATUS: skeleton. Addressing + roster sign/verify/lookup are pure + unit-tested.
// Live cross-org fetch/transport (Codex's leaf-node contract) is the integration gate.

import { signEnvelope, verifyEnvelopeSignature } from "@murmurv2/security";

// ─────────────────────────── Addressing ───────────────────────────

export interface AgentAddress {
  /** Owning organization id (e.g. "aimindset"). */
  org: string;
  /** Agent id within the org (e.g. "agent-jarvis"). */
  agentId: string;
}

const TOKEN_RE = /^[A-Za-z0-9._-]+$/;

function validateToken(value: string, label: string): void {
  if (!value || !TOKEN_RE.test(value)) {
    throw new Error(`federation: invalid ${label}: ${JSON.stringify(value)}`);
  }
}

/**
 * Parse `org/agentId` or a bare `agentId`. A bare id resolves to `localOrg`
 * (back-compat invariant). Exactly one slash is allowed.
 */
export function parseAddress(raw: string, localOrg: string): AgentAddress {
  validateToken(localOrg, "localOrg");
  if (typeof raw !== "string" || raw.length === 0) {
    throw new Error("federation: empty address");
  }
  const slash = raw.indexOf("/");
  if (slash === -1) {
    validateToken(raw, "agentId");
    return { org: localOrg, agentId: raw };
  }
  const org = raw.slice(0, slash);
  const agentId = raw.slice(slash + 1);
  if (agentId.includes("/")) throw new Error(`federation: nested slash in address: ${raw}`);
  validateToken(org, "org");
  validateToken(agentId, "agentId");
  return { org, agentId };
}

/** Canonical `org/agentId` string. */
export function formatAddress(addr: AgentAddress): string {
  validateToken(addr.org, "org");
  validateToken(addr.agentId, "agentId");
  return `${addr.org}/${addr.agentId}`;
}

/** True when the address is served by the local org (no federation hop). */
export function isLocal(addr: AgentAddress, localOrg: string): boolean {
  return addr.org === localOrg;
}

// ──────────────────── Signed key directory (roster) ────────────────────

/** Public keys an agent advertises to peers. */
export interface AgentKeys {
  /** X25519 public key (base64) — peers seal E2E payloads to this. */
  encryptPublicKey: string;
  /** Ed25519 public key (base64) — peers verify this agent's envelope signatures. */
  verifyPublicKey: string;
}

/** The unsigned roster an org publishes for its agents. */
export interface RosterBody {
  org: string;
  /** Monotonic version; consumers reject a lower version than last seen. */
  version: number;
  /** ISO timestamp. NOTE: ok at runtime; offline tooling must inject the time. */
  issuedAt: string;
  /** agentId -> advertised keys. */
  agents: Record<string, AgentKeys>;
}

/** A roster plus the org's Ed25519 signature over its canonical form. */
export interface SignedRoster extends RosterBody {
  /** Ed25519 signature (base64) over canonicalRoster(body). */
  signature: string;
  /** The org's Ed25519 directory-signing public key (base64), pinned out-of-band. */
  signingPublicKey: string;
}

/**
 * Canonical signing input: fixed field order + agents sorted by id, so the bytes
 * are independent of object insertion order. MUST stay identical on sign + verify.
 */
export function canonicalRoster(body: RosterBody): string {
  const agents: Record<string, AgentKeys> = {};
  for (const id of Object.keys(body.agents).sort()) {
    const k = body.agents[id];
    agents[id] = { encryptPublicKey: k.encryptPublicKey, verifyPublicKey: k.verifyPublicKey };
  }
  return JSON.stringify({
    org: body.org,
    version: body.version,
    issuedAt: body.issuedAt,
    agents,
  });
}

/** Sign a roster body with the org's Ed25519 directory-signing keypair. */
export async function signRoster(
  body: RosterBody,
  signingPrivateKey: string,
  signingPublicKey: string,
): Promise<SignedRoster> {
  if (!Number.isInteger(body.version) || body.version < 1) {
    throw new Error("federation: roster version must be a positive integer");
  }
  const signature = await signEnvelope(canonicalRoster(body), signingPrivateKey);
  return { ...body, signature, signingPublicKey };
}

/**
 * Verify a signed roster against the caller's pinned/trusted org key.
 * The embedded signingPublicKey is transport metadata only; accepting it as the
 * trust root would let an attacker publish a forged roster signed by themselves.
 */
export async function verifyRoster(roster: SignedRoster, expectedSigningPublicKey: string): Promise<boolean> {
  if (!roster || !roster.signature || !roster.signingPublicKey || !expectedSigningPublicKey) return false;
  if (roster.signingPublicKey !== expectedSigningPublicKey) return false;
  const body: RosterBody = {
    org: roster.org,
    version: roster.version,
    issuedAt: roster.issuedAt,
    agents: roster.agents,
  };
  try {
    return await verifyEnvelopeSignature(canonicalRoster(body), roster.signature, expectedSigningPublicKey);
  } catch {
    return false;
  }
}

/** Look up an agent's advertised keys in a roster. Throws if absent. */
export function lookupAgentKeys(roster: SignedRoster, agentId: string): AgentKeys {
  const keys = roster.agents[agentId];
  if (!keys) throw new Error(`federation: agent not in roster ${roster.org}: ${agentId}`);
  return keys;
}

// ──────────────────── Roster store (runtime trust + replay guard) ────────────────────

export type RosterRejectReason = "org-not-pinned" | "signature-invalid" | "stale-or-replay";

export interface RosterOfferResult {
  accepted: boolean;
  reason?: RosterRejectReason;
}

/**
 * Holds the latest verified roster per org and enforces the production trust model
 * (the runtime/bridge-layer concern, deliberately NOT in the NATS subject contract):
 *   - org directory-signing keys are PINNED out-of-band; an embedded
 *     `signingPublicKey` is never the trust root.
 *   - a roster is accepted only if it verifies against the pinned key AND its version
 *     strictly exceeds the last accepted version for that org — so a replayed or
 *     downgraded roster (lower/equal version) is rejected.
 */
export class RosterStore {
  private readonly pinned: Map<string, string>;
  private readonly latest = new Map<string, SignedRoster>();

  constructor(pinnedOrgKeys: Record<string, string> | Map<string, string> = {}) {
    this.pinned =
      pinnedOrgKeys instanceof Map ? new Map(pinnedOrgKeys) : new Map(Object.entries(pinnedOrgKeys));
  }

  /** Pin (or re-pin) an org's directory-signing public key (out-of-band trust input). */
  pin(org: string, signingPublicKey: string): void {
    this.pinned.set(org, signingPublicKey);
  }

  /**
   * Offer a signed roster. Accepted only when it verifies against the pinned org key
   * and is strictly newer than the last accepted version (monotonic ⇒ no replay).
   */
  async offer(roster: SignedRoster): Promise<RosterOfferResult> {
    const pin = this.pinned.get(roster.org);
    if (!pin) return { accepted: false, reason: "org-not-pinned" };
    if (!(await verifyRoster(roster, pin))) return { accepted: false, reason: "signature-invalid" };
    const current = this.latest.get(roster.org);
    if (current && roster.version <= current.version) {
      return { accepted: false, reason: "stale-or-replay" };
    }
    this.latest.set(roster.org, roster);
    return { accepted: true };
  }

  /** The latest accepted roster for an org, if any. */
  current(org: string): SignedRoster | undefined {
    return this.latest.get(org);
  }

  /** Look up an agent's keys from the latest accepted roster. Throws if none accepted. */
  agentKeys(org: string, agentId: string): AgentKeys {
    const roster = this.latest.get(org);
    if (!roster) throw new Error(`federation: no accepted roster for org ${org}`);
    return lookupAgentKeys(roster, agentId);
  }
}
