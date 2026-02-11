import { connect, type NatsConnection, StringCodec, type Subscription } from "nats";
import {
  applyJitter,
  computeBackoffMs,
  createAck,
  type EnvelopeV1,
  isEnvelopeV1,
  type DedupeStore,
  type OutboxStore,
  type AckV1,
  type SecurityPolicy,
  validateEnvelopePolicy,
} from "@murmurv2/core";

export interface BrokerConfig {
  url: string;
  stream?: string;
  token?: string;
  connectMaxAttempts?: number;
  connectBaseBackoffMs?: number;
  connectJitterRatio?: number;
}

export type MessageHandler = (envelope: EnvelopeV1) => Promise<void>;

export class NatsBroker {
  private nc?: NatsConnection;
  private readonly sc = StringCodec();
  private readonly failedDeliveries = new Map<string, number>();

  constructor(private readonly config: BrokerConfig) {}

  async connect(): Promise<void> {
    if (this.nc) return;

    const maxAttempts = this.config.connectMaxAttempts ?? 5;
    const baseBackoffMs = this.config.connectBaseBackoffMs ?? 250;
    const jitterRatio = this.config.connectJitterRatio ?? 0.2;
    let lastErr: unknown;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        this.nc = await connect({
          servers: this.config.url,
          token: this.config.token,
        });
        return;
      } catch (err) {
        lastErr = err;
        if (attempt >= maxAttempts) break;
        const sleepMs = applyJitter(computeBackoffMs(attempt, baseBackoffMs), jitterRatio);
        await new Promise((resolve) => setTimeout(resolve, sleepMs));
      }
    }

    throw lastErr instanceof Error ? lastErr : new Error("nats-connect-failed");
  }

  async close(): Promise<void> {
    if (!this.nc) return;
    await this.nc.drain();
    this.nc = undefined;
  }

  async publish(subject: string, envelope: EnvelopeV1, policy?: SecurityPolicy): Promise<void> {
    const violations = validateEnvelopePolicy(envelope, policy);
    if (violations.length > 0) {
      throw new Error(`policy-rejected:${violations.join("|")}`);
    }
    await this.connect();
    this.nc!.publish(subject, this.sc.encode(JSON.stringify(envelope)));
  }

  async publishAck(subject: string, envelope: ReturnType<typeof createAck>): Promise<void> {
    await this.connect();
    this.nc!.publish(subject, this.sc.encode(JSON.stringify(envelope)));
  }

  async subscribeWithAck(params: {
    subject: string;
    consumerId: string;
    dedupe: DedupeStore;
    onMessage: MessageHandler;
    maxPoisonAttempts?: number;
  }): Promise<Subscription> {
    await this.connect();

    const sub = this.nc!.subscribe(params.subject);
    const ackSubject = `ack.${params.consumerId}`;

    (async () => {
      for await (const m of sub) {
        let msgId = "unknown";
        try {
          const decoded = JSON.parse(this.sc.decode(m.data));
          if (!isEnvelopeV1(decoded)) {
            await this.publishAck(ackSubject, createAck("unknown", params.consumerId, "nack", "invalid-envelope"));
            continue;
          }

          msgId = decoded.msgId;
          const isDup = await params.dedupe.seen(decoded.msgId, params.consumerId);
          if (isDup) {
            await this.publishAck(ackSubject, createAck(decoded.msgId, params.consumerId, "ack", "duplicate-ignored"));
            continue;
          }

          await params.onMessage(decoded);
          await params.dedupe.markSeen(decoded.msgId, params.consumerId);
          this.failedDeliveries.delete(`${params.consumerId}:${decoded.msgId}`);
          await this.publishAck(ackSubject, createAck(decoded.msgId, params.consumerId, "ack"));
        } catch (err) {
          const reason = err instanceof Error ? err.message : "handler-failed";
          const maxPoisonAttempts = params.maxPoisonAttempts ?? 3;
          const key = `${params.consumerId}:${msgId}`;
          const failures = (this.failedDeliveries.get(key) ?? 0) + 1;
          this.failedDeliveries.set(key, failures);
          if (msgId !== "unknown" && failures >= maxPoisonAttempts) {
            await params.dedupe.markSeen(msgId, params.consumerId);
            this.failedDeliveries.delete(key);
            await this.publishAck(ackSubject, createAck(msgId, params.consumerId, "nack", `poison-message:${reason}`));
            continue;
          }
          await this.publishAck(ackSubject, createAck(msgId, params.consumerId, "nack", reason));
        }
      }
    })().catch((err) => {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error("[NatsBroker.subscribeWithAck] loop crashed", { message: e.message, stack: e.stack });
    });

    return sub;
  }

  async startAckCorrelation(params: {
    outbox: OutboxStore;
    ackSubject: string;
  }): Promise<Subscription> {
    await this.connect();
    const sub = this.nc!.subscribe(params.ackSubject);

    (async () => {
      for await (const m of sub) {
        try {
          const decoded = JSON.parse(this.sc.decode(m.data)) as AckV1;
          if (typeof decoded.msgId !== "string" || decoded.msgId.length === 0) continue;

          if (decoded.status === "ack") {
            await params.outbox.markAcked(decoded.msgId);
            continue;
          }

          if (decoded.status === "nack") {
            await params.outbox.markFailed(
              decoded.msgId,
              decoded.reason ?? "nack",
              new Date().toISOString(),
            );
          }
        } catch (err) {
          const e = err instanceof Error ? err : new Error(String(err));
          console.error("[NatsBroker.startAckCorrelation] malformed ack frame", {
            message: e.message,
            stack: e.stack,
            raw: this.sc.decode(m.data),
          });
        }
      }
    })().catch((err) => {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error("[NatsBroker.startAckCorrelation] loop crashed", { message: e.message, stack: e.stack });
    });

    return sub;
  }

  /**
   * Basic outbox worker:
   * - picks due records
   * - publishes
   * - marks sent/failed/dlq
   * - ACK correlation handled via startAckCorrelation(...)
   */
  async flushOutbox(params: {
    outbox: OutboxStore;
    maxAttempts?: number;
    batchSize?: number;
    baseBackoffMs?: number;
    jitterRatio?: number;
    ackTimeoutMs?: number;
    policy?: SecurityPolicy;
  }): Promise<void> {
    const maxAttempts = params.maxAttempts ?? 5;
    if (params.ackTimeoutMs && params.outbox.requeueStaleSent) {
      await params.outbox.requeueStaleSent(params.ackTimeoutMs);
    }
    const due = await params.outbox.claimDue(params.batchSize ?? 50);

    for (const rec of due) {
      try {
        await this.publish(rec.subject, rec.envelope, params.policy);
        await params.outbox.markSent(rec.msgId);
      } catch (err) {
        const nextAttemptNum = rec.attempts + 1;
        const reason = err instanceof Error ? err.message : "publish-failed";

        if (reason.startsWith("policy-rejected:")) {
          await params.outbox.markDlq(rec.msgId, reason);
          continue;
        }

        if (nextAttemptNum >= maxAttempts) {
          await params.outbox.markDlq(rec.msgId, reason);
          continue;
        }

        const backoffMs = computeBackoffMs(nextAttemptNum, params.baseBackoffMs ?? 500);
        const withJitter = applyJitter(backoffMs, params.jitterRatio ?? 0.2);
        const nextAt = new Date(Date.now() + withJitter).toISOString();
        await params.outbox.markFailed(rec.msgId, reason, nextAt);
      }
    }
  }
}
