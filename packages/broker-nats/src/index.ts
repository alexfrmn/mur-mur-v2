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
}

export type MessageHandler = (envelope: EnvelopeV1) => Promise<void>;

export class NatsBroker {
  private nc?: NatsConnection;
  private readonly sc = StringCodec();
  private readonly failedDeliveries = new Map<string, number>();

  constructor(private readonly config: BrokerConfig) {}

  async connect(): Promise<void> {
    if (this.nc) return;
    this.nc = await connect({ servers: this.config.url });
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
    })().catch(() => undefined);

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
          if (decoded.status === "ack" && typeof decoded.msgId === "string") {
            await params.outbox.markAcked(decoded.msgId);
          }
        } catch {
          // ignore malformed ack frames
        }
      }
    })().catch(() => undefined);

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
