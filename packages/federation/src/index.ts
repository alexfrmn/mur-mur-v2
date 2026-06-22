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

export type RosterRejectReason =
  | "org-not-pinned"
  | "signature-invalid"
  | "invalid-version"
  | "stale-or-replay";

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

  /**
   * Pin (or re-pin) an org's directory-signing public key (out-of-band trust input).
   * Rotating to a DIFFERENT key starts a fresh trust epoch: the org's previously
   * accepted roster (signed under the old key) is dropped, so a new roster under the
   * new key is accepted from any version rather than being blocked as stale-or-replay.
   * Re-pinning the same key is a no-op.
   */
  pin(org: string, signingPublicKey: string): void {
    const previous = this.pinned.get(org);
    this.pinned.set(org, signingPublicKey);
    if (previous !== undefined && previous !== signingPublicKey) {
      this.latest.delete(org);
    }
  }

  /**
   * Offer a signed roster. Accepted only when it (1) verifies against the pinned org
   * key, (2) carries a structurally valid version (positive integer — enforced here,
   * not trusted from the signer, since the store is the wire/runtime boundary), and
   * (3) is strictly newer than the last accepted version (monotonic ⇒ no replay).
   */
  async offer(roster: SignedRoster): Promise<RosterOfferResult> {
    const pin = this.pinned.get(roster.org);
    if (!pin) return { accepted: false, reason: "org-not-pinned" };
    if (!(await verifyRoster(roster, pin))) return { accepted: false, reason: "signature-invalid" };
    if (!Number.isInteger(roster.version) || roster.version < 1) {
      return { accepted: false, reason: "invalid-version" };
    }
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

// ──────────────────── Auth token (roster-backed authn/authz) ────────────────────

export interface AuthTokenClaims {
  /** Agent that signs the token; its Ed25519 verify key MUST come from RosterStore.
   *  For real authz this is an org-authority, NOT the caller (self-issued scope is
   *  meaningless). */
  issuer: AgentAddress;
  /** The authorized caller this token vouches for — the actor a verifier binds to the
   *  message sender (`subject === envelope.senderAgentId`). Without this an authority
   *  token granting `murmur:send` would not be bound to WHICH agent may send. */
  subject: AgentAddress;
  /** Intended recipient/service of the token. */
  audience: AgentAddress;
  /** Permission strings such as `murmur:send` or `murmur:reply`. */
  scopes: string[];
  /** ISO timestamp. Tokens are not accepted before this time. */
  issuedAt: string;
  /** ISO timestamp. Tokens expire at or after this time. */
  expiresAt: string;
  /** Optional caller-provided nonce/jti for replay caches outside this pure helper. */
  nonce?: string;
}

export interface SignedAuthToken extends AuthTokenClaims {
  /** Ed25519 signature over canonicalAuthTokenClaims(claims). */
  signature: string;
}

export type AuthTokenRejectReason =
  | "malformed"
  | "issuer-not-found"
  | "signature-invalid"
  | "not-yet-valid"
  | "expired"
  | "audience-mismatch"
  | "subject-mismatch"
  | "scope-missing";

export interface AuthTokenVerifyOptions {
  now?: Date | string | number;
  audience?: AgentAddress;
  /** Require the token's `subject` (actor) to equal this address — the caller binds it
   *  to the message sender, e.g. `requiredSubject = envelope.senderAgentId`. */
  requiredSubject?: AgentAddress;
  requiredScopes?: string[];
}

export interface AuthTokenVerifyResult {
  accepted: boolean;
  reason?: AuthTokenRejectReason;
}

export const AUTH_TOKEN_PREFIX = "MURMUR-AUTH:";

const SCOPE_RE = /^[A-Za-z0-9:._/-]+$/;

function validDateMs(value: string, label: string): number {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`federation: invalid ${label}`);
  }
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) throw new Error(`federation: invalid ${label}`);
  return ms;
}

function normalizeScopes(scopes: string[]): string[] {
  if (!Array.isArray(scopes) || scopes.length === 0) {
    throw new Error("federation: auth token must carry at least one scope");
  }
  const uniq = new Set<string>();
  for (const scope of scopes) {
    if (typeof scope !== "string" || !SCOPE_RE.test(scope)) {
      throw new Error(`federation: invalid auth scope: ${JSON.stringify(scope)}`);
    }
    uniq.add(scope);
  }
  return [...uniq].sort();
}

function normalizeAuthClaims(claims: AuthTokenClaims): AuthTokenClaims {
  if (!claims || typeof claims !== "object") throw new Error("federation: invalid auth token claims");
  const issuedAtMs = validDateMs(claims.issuedAt, "issuedAt");
  const expiresAtMs = validDateMs(claims.expiresAt, "expiresAt");
  if (expiresAtMs <= issuedAtMs) throw new Error("federation: expiresAt must be after issuedAt");
  const nonce = claims.nonce;
  if (nonce !== undefined && (typeof nonce !== "string" || nonce.length === 0)) {
    throw new Error("federation: invalid nonce");
  }
  return {
    issuer: parseAddress(formatAddress(claims.issuer), claims.issuer.org),
    subject: parseAddress(formatAddress(claims.subject), claims.subject.org),
    audience: parseAddress(formatAddress(claims.audience), claims.audience.org),
    scopes: normalizeScopes(claims.scopes),
    issuedAt: claims.issuedAt,
    expiresAt: claims.expiresAt,
    ...(nonce !== undefined ? { nonce } : {}),
  };
}

/**
 * Canonical auth-token signing input. Scopes are sorted/deduped so permission order
 * does not affect signatures; issuer/subject/audience are canonical `org/agentId`
 * strings. `subject` is signed too, so it can't be swapped to re-point an authority's
 * grant at a different actor.
 */
export function canonicalAuthTokenClaims(claims: AuthTokenClaims): string {
  const normalized = normalizeAuthClaims(claims);
  return JSON.stringify({
    issuer: formatAddress(normalized.issuer),
    subject: formatAddress(normalized.subject),
    audience: formatAddress(normalized.audience),
    scopes: normalized.scopes,
    issuedAt: normalized.issuedAt,
    expiresAt: normalized.expiresAt,
    ...(normalized.nonce !== undefined ? { nonce: normalized.nonce } : {}),
  });
}

/** Sign an auth token with the issuer agent's Ed25519 private key. */
export async function signAuthToken(
  claims: AuthTokenClaims,
  issuerSigningPrivateKey: string,
): Promise<SignedAuthToken> {
  const normalized = normalizeAuthClaims(claims);
  const signature = await signEnvelope(canonicalAuthTokenClaims(normalized), issuerSigningPrivateKey);
  return { ...normalized, signature };
}

function normalizeSignedAuthToken(token: SignedAuthToken): SignedAuthToken {
  const normalized = normalizeAuthClaims(token);
  if (typeof token.signature !== "string" || token.signature.length === 0) {
    throw new Error("federation: invalid auth token signature");
  }
  return { ...normalized, signature: token.signature };
}

/** Encode a signed auth token as a bearer-safe string. */
export function encodeAuthToken(token: SignedAuthToken): string {
  const normalized = normalizeSignedAuthToken(token);
  return `${AUTH_TOKEN_PREFIX}${Buffer.from(JSON.stringify(normalized), "utf8").toString("base64url")}`;
}

/** Decode a bearer-safe auth token string. Signature validity is checked by verifyAuthToken(). */
export function decodeAuthToken(encoded: string): SignedAuthToken {
  if (typeof encoded !== "string" || !encoded.startsWith(AUTH_TOKEN_PREFIX)) {
    throw new Error("federation: invalid auth token");
  }
  try {
    const raw = Buffer.from(encoded.slice(AUTH_TOKEN_PREFIX.length), "base64url").toString("utf8");
    return normalizeSignedAuthToken(JSON.parse(raw));
  } catch (err) {
    throw new Error("federation: invalid auth token", { cause: err });
  }
}

function optionTimeMs(now: Date | string | number | undefined): number {
  if (now === undefined) return Date.now();
  if (now instanceof Date) return now.getTime();
  if (typeof now === "number") return now;
  const ms = Date.parse(now);
  if (!Number.isFinite(ms)) throw new Error("federation: invalid now");
  return ms;
}

/**
 * Verify a signed auth token against RosterStore. The token contains no trust root:
 * the issuer's verify key is resolved from the latest accepted roster.
 */
export async function verifyAuthToken(
  token: SignedAuthToken,
  rosters: RosterStore,
  options: AuthTokenVerifyOptions = {},
): Promise<AuthTokenVerifyResult> {
  let normalized: AuthTokenClaims;
  let issuedAtMs: number;
  let expiresAtMs: number;
  let nowMs: number;
  try {
    normalized = normalizeAuthClaims(token);
    issuedAtMs = validDateMs(normalized.issuedAt, "issuedAt");
    expiresAtMs = validDateMs(normalized.expiresAt, "expiresAt");
    nowMs = optionTimeMs(options.now);
  } catch {
    return { accepted: false, reason: "malformed" };
  }

  let verifyPublicKey: string;
  try {
    verifyPublicKey = rosters.agentKeys(normalized.issuer.org, normalized.issuer.agentId).verifyPublicKey;
  } catch {
    return { accepted: false, reason: "issuer-not-found" };
  }

  let signatureOk = false;
  try {
    signatureOk =
      typeof token.signature === "string" &&
      (await verifyEnvelopeSignature(canonicalAuthTokenClaims(normalized), token.signature, verifyPublicKey));
  } catch {
    signatureOk = false;
  }
  if (!signatureOk) {
    return { accepted: false, reason: "signature-invalid" };
  }
  if (nowMs < issuedAtMs) return { accepted: false, reason: "not-yet-valid" };
  if (nowMs >= expiresAtMs) return { accepted: false, reason: "expired" };

  if (options.audience && formatAddress(options.audience) !== formatAddress(normalized.audience)) {
    return { accepted: false, reason: "audience-mismatch" };
  }

  if (options.requiredSubject && formatAddress(options.requiredSubject) !== formatAddress(normalized.subject)) {
    return { accepted: false, reason: "subject-mismatch" };
  }

  if (options.requiredScopes?.length) {
    const granted = new Set(normalized.scopes);
    for (const scope of normalizeScopes(options.requiredScopes)) {
      if (!granted.has(scope)) return { accepted: false, reason: "scope-missing" };
    }
  }

  return { accepted: true };
}
