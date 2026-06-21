import {
  AckPolicy,
  connect,
  DeliverPolicy,
  type ConnectionOptions,
  type JetStreamClient,
  type JetStreamManager,
  type NatsConnection,
  StringCodec,
  type Subscription,
} from "nats";
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
  jetstream?: boolean;
  stream?: string;
  streamSubjects?: string[];
  jetstreamMaxDeliver?: number;
  jetstreamAckWaitMs?: number;
  token?: string;
  connectMaxAttempts?: number;
  connectBaseBackoffMs?: number;
  connectJitterRatio?: number;
  maxReconnectAttempts?: number;
  reconnectTimeWait?: number;
  reconnectJitter?: number;
  pingInterval?: number;
  maxPingOut?: number;
  waitOnFirstConnect?: boolean;
  onStatus?: (status: BrokerStatusEvent) => void;
}

export type MessageHandler = (envelope: EnvelopeV1) => Promise<void>;
export type BrokerSubscription = Subscription | { unsubscribe(): void | Promise<void> };

export interface BrokerStatusEvent {
  type: string;
  data?: unknown;
  reconnects: number;
}

export const buildNatsConnectionOptions = (config: BrokerConfig): ConnectionOptions => ({
  servers: config.url,
  token: config.token,
  maxReconnectAttempts: config.maxReconnectAttempts ?? -1,
  reconnectTimeWait: config.reconnectTimeWait ?? 2000,
  reconnectJitter: config.reconnectJitter ?? 500,
  pingInterval: config.pingInterval ?? 20000,
  maxPingOut: config.maxPingOut ?? 2,
  waitOnFirstConnect: config.waitOnFirstConnect ?? true,
});

export class NatsBroker {
  private nc?: NatsConnection;
  private js?: JetStreamClient;
  private jsm?: JetStreamManager;
  private readonly sc = StringCodec();
  private readonly failedDeliveries = new Map<string, number>();
  private reconnects = 0;
  private statusLoop?: Promise<void>;

  constructor(private readonly config: BrokerConfig) {}

  async connect(): Promise<void> {
    if (this.nc) {
      await this.ensureJetStream();
      return;
    }

    const maxAttempts = this.config.connectMaxAttempts ?? 5;
    const baseBackoffMs = this.config.connectBaseBackoffMs ?? 250;
    const jitterRatio = this.config.connectJitterRatio ?? 0.2;
    let lastErr: unknown;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        this.nc = await connect(buildNatsConnectionOptions(this.config));
        this.startStatusLoop(this.nc);
        await this.ensureJetStream();
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

  getReconnectCount(): number {
    return this.reconnects;
  }

  private jetStreamEnabled(): boolean {
    return this.config.jetstream === true || !!this.config.stream;
  }

  private streamName(): string {
    return this.config.stream ?? "MURMUR";
  }

  private streamSubjects(): string[] {
    return this.config.streamSubjects ?? ["msg.>", "ack.>"];
  }

  private jetStreamMaxDeliver(): number {
    const value = this.config.jetstreamMaxDeliver ?? 5;
    if (!Number.isFinite(value) || value < 1) {
      throw new Error("jetstream-max-deliver-invalid");
    }
    return Math.trunc(value);
  }

  private jetStreamAckWaitNanos(): number {
    const ackWaitMs = this.config.jetstreamAckWaitMs ?? 30000;
    if (!Number.isFinite(ackWaitMs) || ackWaitMs <= 0) {
      throw new Error("jetstream-ack-wait-invalid");
    }
    return Math.trunc(ackWaitMs * 1_000_000);
  }

  private buildJetStreamConsumerConfig(subject: string, durableName: string) {
    return {
      durable_name: durableName,
      name: durableName,
      filter_subject: subject,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      max_deliver: this.jetStreamMaxDeliver(),
      ack_wait: this.jetStreamAckWaitNanos(),
    };
  }

  private async ensureJetStream(): Promise<void> {
    if (!this.jetStreamEnabled() || !this.nc) return;
    if (this.js && this.jsm) return;

    this.jsm = await this.nc.jetstreamManager();
    const stream = this.streamName();
    const subjects = this.streamSubjects();

    try {
      const info = await this.jsm.streams.info(stream);
      const currentSubjects = new Set(info.config.subjects ?? []);
      const missingSubjects = subjects.filter((subject) => !currentSubjects.has(subject));
      if (missingSubjects.length > 0) {
        await this.jsm.streams.update(stream, {
          ...info.config,
          subjects: [...currentSubjects, ...missingSubjects],
        });
      }
    } catch {
      await this.jsm.streams.add({
        name: stream,
        subjects,
      });
    }

    this.js = this.nc.jetstream();
  }

  private startStatusLoop(nc: NatsConnection): void {
    if (this.statusLoop) return;
    this.statusLoop = (async () => {
      for await (const status of nc.status()) {
        if (status.type === "reconnect") this.reconnects += 1;
        if (status.type === "disconnect" || status.type === "reconnect" || status.type === "update") {
          const event = { type: status.type, data: status.data, reconnects: this.reconnects };
          this.config.onStatus?.(event);
          console.info("[NatsBroker.status]", event);
        }
      }
    })().catch((err) => {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error("[NatsBroker.status] loop crashed", { message: e.message, stack: e.stack });
    }).finally(() => {
      this.statusLoop = undefined;
    });
  }

  async publish(subject: string, envelope: EnvelopeV1, policy?: SecurityPolicy): Promise<void> {
    const violations = validateEnvelopePolicy(envelope, policy);
    if (violations.length > 0) {
      throw new Error(`policy-rejected:${violations.join("|")}`);
    }
    await this.connect();
    const payload = this.sc.encode(JSON.stringify(envelope));
    if (this.js) {
      await this.js.publish(subject, payload, { msgID: envelope.msgId });
      return;
    }
    this.nc!.publish(subject, payload);
  }

  async publishAck(subject: string, envelope: ReturnType<typeof createAck>): Promise<void> {
    await this.connect();
    const payload = this.sc.encode(JSON.stringify(envelope));
    if (this.js) {
      await this.js.publish(subject, payload, {
        msgID: `ack:${envelope.msgId}:${envelope.consumerId}:${envelope.status}`,
      });
      return;
    }
    this.nc!.publish(subject, payload);
  }

  private async processEnvelopeFrame(
    data: Uint8Array,
    params: {
      consumerId: string;
      dedupe: DedupeStore;
      onMessage: MessageHandler;
      maxPoisonAttempts?: number;
    },
  ): Promise<"ack" | "retry"> {
    let msgId = "unknown";
    let ackSubject = `ack.${params.consumerId}`;
    try {
      const decoded = JSON.parse(this.sc.decode(data));
      if (!isEnvelopeV1(decoded)) {
        await this.publishAck(ackSubject, createAck("unknown", params.consumerId, "nack", "invalid-envelope"));
        return "ack";
      }

      msgId = decoded.msgId;
      ackSubject = `ack.${decoded.senderAgentId}`;
      const isDup = await params.dedupe.seen(decoded.msgId, params.consumerId);
      if (isDup) {
        await this.publishAck(ackSubject, createAck(decoded.msgId, params.consumerId, "ack", "duplicate-ignored"));
        return "ack";
      }

      await params.onMessage(decoded);
      await params.dedupe.markSeen(decoded.msgId, params.consumerId);
      this.failedDeliveries.delete(`${params.consumerId}:${decoded.msgId}`);
      await this.publishAck(ackSubject, createAck(decoded.msgId, params.consumerId, "ack"));
      return "ack";
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
        return "ack";
      }
      await this.publishAck(ackSubject, createAck(msgId, params.consumerId, "nack", reason));
      return "retry";
    }
  }

  private async ensureJetStreamConsumer(subject: string, durableName: string): Promise<void> {
    if (!this.jsm) throw new Error("jetstream-manager-unavailable");
    const stream = this.streamName();
    const config = this.buildJetStreamConsumerConfig(subject, durableName);
    let info;
    try {
      info = await this.jsm.consumers.info(stream, durableName);
    } catch (err) {
      if (err instanceof Error && err.message.startsWith("jetstream-consumer-filter-mismatch:")) {
        throw err;
      }
      await this.jsm.consumers.add(stream, config);
      return;
    }

    const filterSubject = info.config.filter_subject;
    if (filterSubject && filterSubject !== subject) {
      throw new Error(`jetstream-consumer-filter-mismatch:${durableName}:${filterSubject}:${subject}`);
    }
    if (info.config.max_deliver !== config.max_deliver || info.config.ack_wait !== config.ack_wait) {
      await this.jsm.consumers.update(stream, durableName, {
        max_deliver: config.max_deliver,
        ack_wait: config.ack_wait,
      });
    }
  }

  private async consumeJetStream(
    subject: string,
    durableName: string,
    onMessage: (data: Uint8Array) => Promise<"ack" | "retry" | void>,
  ): Promise<BrokerSubscription> {
    await this.ensureJetStreamConsumer(subject, durableName);
    if (!this.js) throw new Error("jetstream-client-unavailable");

    const consumer = await this.js.consumers.get(this.streamName(), durableName);
    const messages = await consumer.consume();

    (async () => {
      for await (const m of messages) {
        try {
          const result = await onMessage(m.data);
          if (result === "retry") m.nak();
          else m.ack();
        } catch (err) {
          m.nak();
          const e = err instanceof Error ? err : new Error(String(err));
          console.error("[NatsBroker.consumeJetStream] message failed", {
            subject,
            durableName,
            message: e.message,
            stack: e.stack,
          });
        }
      }
    })().catch((err) => {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error("[NatsBroker.consumeJetStream] loop crashed", { subject, durableName, message: e.message, stack: e.stack });
    });

    return {
      unsubscribe: () => {
        void messages.close();
      },
    };
  }

  async subscribeWithAck(params: {
    subject: string;
    consumerId: string;
    dedupe: DedupeStore;
    onMessage: MessageHandler;
    maxPoisonAttempts?: number;
  }): Promise<BrokerSubscription> {
    await this.connect();

    if (this.js) {
      return this.consumeJetStream(params.subject, params.consumerId, (data) => this.processEnvelopeFrame(data, params));
    }

    const sub = this.nc!.subscribe(params.subject);

    (async () => {
      for await (const m of sub) {
        await this.processEnvelopeFrame(m.data, params);
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
    consumerId?: string;
  }): Promise<BrokerSubscription> {
    await this.connect();

    if (this.js) {
      const consumerId = params.consumerId ?? `${params.ackSubject.replaceAll(".", "-")}-consumer`;
      return this.consumeJetStream(params.ackSubject, consumerId, async (data) => {
        await this.processAckFrame(data, params.outbox);
      });
    }

    const sub = this.nc!.subscribe(params.ackSubject);

    (async () => {
      for await (const m of sub) {
        await this.processAckFrame(m.data, params.outbox);
      }
    })().catch((err) => {
      const e = err instanceof Error ? err : new Error(String(err));
      console.error("[NatsBroker.startAckCorrelation] loop crashed", { message: e.message, stack: e.stack });
    });

    return sub;
  }

  private async processAckFrame(data: Uint8Array, outbox: OutboxStore): Promise<void> {
    try {
      const decoded = JSON.parse(this.sc.decode(data)) as AckV1;
      if (typeof decoded.msgId !== "string" || decoded.msgId.length === 0) return;

      if (decoded.status === "ack") {
        await outbox.markAcked(decoded.msgId);
        return;
      }

      if (decoded.status === "nack") {
        await outbox.markFailed(
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
        raw: this.sc.decode(data),
      });
    }
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
