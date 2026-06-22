/**
 * Agent discovery — presence frames + candidate registry (PR1: pure, in-memory).
 *
 * Goal (README): let peers find each other without a manual invite exchange.
 *
 * TRUST MODEL (hard rule): a presence frame only makes an agent a *candidate*.
 * Observing a presence NEVER auto-trusts or auto-adds a peer — promotion of a
 * candidate to a trusted peer is an explicit operator/policy action elsewhere.
 * (Lesson from live onboarding: adding a trusted peer widens the trust border, so
 * it must stay a deliberate operator step, not a side effect of an inbound frame.)
 *
 * A presence frame carries only PUBLIC metadata (public keys, subject,
 * capabilities). PR1 treats the frame as UNSIGNED data — there is no signature
 * field or verification here; frame signing + verification over NATS lands in
 * PR2. Even once signed, a valid signature would only prove announcement
 * integrity (not tampered in flight), NOT that the agentId is who it claims.
 * Identity authenticity is established out-of-band at promotion time.
 */

export interface PresenceFrameV1 {
  presenceVersion: "1.0";
  agentId: string;
  encryptionPublicKey: string;
  signingPublicKey: string;
  subject: string;
  capabilities: string[];
  /** Validity window in ms from `ts`; the candidate expires at ts + ttlMs. */
  ttlMs: number;
  /** ISO-8601 announcement timestamp. */
  ts: string;
  /** Random per-announcement value for dedupe. */
  nonce: string;
}

export const isPresenceFrameV1 = (v: unknown): v is PresenceFrameV1 => {
  if (!v || typeof v !== "object") return false;
  const o = v as Record<string, unknown>;
  return (
    o.presenceVersion === "1.0" &&
    typeof o.agentId === "string" && o.agentId.length > 0 &&
    typeof o.encryptionPublicKey === "string" && o.encryptionPublicKey.length > 0 &&
    typeof o.signingPublicKey === "string" && o.signingPublicKey.length > 0 &&
    typeof o.subject === "string" && o.subject.length > 0 &&
    Array.isArray(o.capabilities) && o.capabilities.every((c) => typeof c === "string") &&
    typeof o.ttlMs === "number" && Number.isFinite(o.ttlMs) && o.ttlMs > 0 &&
    typeof o.ts === "string" && !Number.isNaN(Date.parse(o.ts)) &&
    typeof o.nonce === "string" && o.nonce.length > 0
  );
};

/** A discovered, NOT-yet-trusted agent. `trusted` is always false here by design. */
export interface DiscoveryCandidate {
  agentId: string;
  encryptionPublicKey: string;
  signingPublicKey: string;
  subject: string;
  capabilities: string[];
  /** Epoch ms when this candidate expires (frame ts + ttlMs). */
  expiresAt: number;
  /** Epoch ms of the most recent presence observed. */
  lastSeen: number;
  /** Always false in PR1 — promotion to a trusted peer is an external operator action. */
  trusted: false;
}

/**
 * In-memory registry of discovered candidates. Pure: no I/O, no NATS, no auto-trust.
 * Feed it presence frames via `observe()`; read non-expired candidates via `list()`.
 */
export class CandidateRegistry {
  private readonly candidates = new Map<string, DiscoveryCandidate>();
  /**
   * Remembers (agentId, nonce) → expiry (epoch ms) of applied frames for
   * idempotency. Bounded: entries expire with their frame and are dropped by
   * `prune()`, so a long-running listener does not leak memory on every announce.
   */
  private readonly seenNonces = new Map<string, number>();

  /**
   * Observe a presence frame. Returns the candidate (created or refreshed), or null
   * if the frame is invalid, already expired at `now`, or a duplicate announcement.
   * A frame is NEVER auto-trusted: the returned candidate always has `trusted: false`.
   */
  observe(frame: unknown, now: number): DiscoveryCandidate | null {
    if (!isPresenceFrameV1(frame)) return null;

    const announcedAt = Date.parse(frame.ts);
    const expiresAt = announcedAt + frame.ttlMs;
    if (expiresAt <= now) return null; // already expired — ignore

    const dedupeKey = `${frame.agentId}:${frame.nonce}`;
    if (this.seenNonces.has(dedupeKey)) {
      // Duplicate announcement → idempotent: return current candidate unchanged.
      return this.candidates.get(frame.agentId) ?? null;
    }
    this.seenNonces.set(dedupeKey, expiresAt);

    const existing = this.candidates.get(frame.agentId);
    // Out-of-order/stale frame for a known agent → keep the fresher one.
    if (existing && announcedAt < existing.lastSeen) return existing;

    const candidate: DiscoveryCandidate = {
      agentId: frame.agentId,
      encryptionPublicKey: frame.encryptionPublicKey,
      signingPublicKey: frame.signingPublicKey,
      subject: frame.subject,
      capabilities: [...frame.capabilities],
      expiresAt,
      lastSeen: announcedAt,
      trusted: false,
    };
    this.candidates.set(frame.agentId, candidate);
    return candidate;
  }

  /** Get a candidate by agentId if present and not expired at `now`. */
  get(agentId: string, now: number): DiscoveryCandidate | undefined {
    const c = this.candidates.get(agentId);
    if (!c) return undefined;
    return c.expiresAt > now ? c : undefined;
  }

  /** All non-expired candidates at `now`. */
  list(now: number): DiscoveryCandidate[] {
    return [...this.candidates.values()].filter((c) => c.expiresAt > now);
  }

  /** Drop expired candidates AND expired dedupe markers. Returns candidates removed. */
  prune(now: number): number {
    let removed = 0;
    for (const [agentId, c] of this.candidates) {
      if (c.expiresAt <= now) {
        this.candidates.delete(agentId);
        removed += 1;
      }
    }
    // Bound the dedupe set: expired nonce markers cannot match a fresh frame.
    for (const [key, expiresAt] of this.seenNonces) {
      if (expiresAt <= now) this.seenNonces.delete(key);
    }
    return removed;
  }

  /** Count of remembered dedupe markers (test/observability hook). */
  nonceCount(): number {
    return this.seenNonces.size;
  }

  /** Count of non-expired candidates at `now`. */
  size(now: number): number {
    return this.list(now).length;
  }
}

/**
 * Canonical string a presence frame is signed over. Fixed field set + order so
 * both sides hash identical bytes regardless of object key order. Mirrors the
 * envelope's stable-payload approach. The `signature` field is NOT included.
 */
export const stablePresencePayload = (frame: PresenceFrameV1): string =>
  JSON.stringify({
    presenceVersion: frame.presenceVersion,
    agentId: frame.agentId,
    encryptionPublicKey: frame.encryptionPublicKey,
    signingPublicKey: frame.signingPublicKey,
    subject: frame.subject,
    capabilities: [...frame.capabilities],
    ttlMs: frame.ttlMs,
    ts: frame.ts,
    nonce: frame.nonce,
  });

/**
 * A presence frame plus an Ed25519 signature over `stablePresencePayload(frame)`.
 *
 * The signature proves the announcement was not tampered with in flight AND that
 * the announcer holds the private key for the `signingPublicKey` it advertises
 * (key-of-record consistency). It does NOT prove the `agentId` maps to a known,
 * trusted identity — that is still established out-of-band at operator promotion.
 * Verification itself lives in the transport/listener layer via @murmurv2/security.
 */
export interface SignedPresenceFrameV1 {
  frame: PresenceFrameV1;
  signature: string;
}

export const isSignedPresenceFrameV1 = (v: unknown): v is SignedPresenceFrameV1 => {
  if (!v || typeof v !== "object") return false;
  const o = v as Record<string, unknown>;
  return (
    typeof o.signature === "string" && o.signature.length > 0 &&
    isPresenceFrameV1(o.frame)
  );
};

/** Verifies an Ed25519 signature over a stable payload. Injected so core stays
 *  free of a crypto dependency (the listener passes @murmurv2/security's verify). */
export type PresenceVerifier = (
  payload: string,
  signature: string,
  publicKey: string,
) => Promise<boolean>;

/**
 * Verify a signed presence frame and, only if the signature checks out against
 * the key the frame advertises, fold it into the registry as a (still untrusted)
 * candidate. Returns the candidate, or null if the input is malformed or the
 * signature is invalid. Never trusts — `observe()` still yields `trusted: false`.
 */
export const observeSignedPresence = async (
  registry: CandidateRegistry,
  signed: unknown,
  verify: PresenceVerifier,
  now: number,
): Promise<DiscoveryCandidate | null> => {
  if (!isSignedPresenceFrameV1(signed)) return null;
  // Discovery frames are PUBLIC, untrusted traffic. A real Ed25519 verifier throws
  // on malformed signature/public-key bytes (wrong length, bad base64) rather than
  // returning false, and the wrapper guard only checks for non-empty strings — so a
  // hostile/garbage frame must be DROPPED here, never allowed to escalate into an
  // unhandled rejection that crashes the listen loop.
  let ok = false;
  try {
    ok = await verify(
      stablePresencePayload(signed.frame),
      signed.signature,
      signed.frame.signingPublicKey,
    );
  } catch {
    return null;
  }
  if (!ok) return null;
  return registry.observe(signed.frame, now);
};

/** Filter for a roster query over discovered candidates. */
export interface CandidateQuery {
  /** Only candidates advertising this capability. */
  capability?: string;
  /** Only candidates whose subject exactly matches. */
  subject?: string;
}

/**
 * Roster query: the non-expired candidates at `now` matching the filter. Read-only
 * — candidates remain untrusted; this is just discovery introspection (the surface
 * an operator/UI uses to decide who to promote).
 */
export const queryCandidates = (
  registry: CandidateRegistry,
  query: CandidateQuery,
  now: number,
): DiscoveryCandidate[] =>
  registry.list(now).filter(
    (c) =>
      (query.capability === undefined || c.capabilities.includes(query.capability)) &&
      (query.subject === undefined || c.subject === query.subject),
  );

/** A trusted-peer entry derived from a candidate — the shape a peer config expects. */
export interface PromotedPeer {
  agentId: string;
  encryptionPublicKey: string;
  signingPublicKey: string;
  subject: string;
  /** Epoch ms the operator promoted this candidate. */
  promotedAt: number;
}

/**
 * Promote a discovered candidate into a trusted-peer entry. This is the EXPLICIT
 * operator step the whole discovery design defers trust to: the caller (an
 * operator action or an approved policy) decides to promote, and this returns the
 * peer entry to add to the trusted peer set. It does NOT add the peer itself and
 * does NOT mutate the registry — wiring the entry into the live peer config /
 * daemon remains the caller's deliberate action.
 *
 * Returns null if there is no live (non-expired) candidate for `agentId`.
 */
export const promoteCandidate = (
  registry: CandidateRegistry,
  agentId: string,
  now: number,
): PromotedPeer | null => {
  const c = registry.get(agentId, now);
  if (!c) return null;
  return {
    agentId: c.agentId,
    encryptionPublicKey: c.encryptionPublicKey,
    signingPublicKey: c.signingPublicKey,
    subject: c.subject,
    promotedAt: now,
  };
};
