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
