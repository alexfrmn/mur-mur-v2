import { connect, type NatsConnection, StringCodec, type Subscription } from "nats";
import { createAck, type EnvelopeV1, isEnvelopeV1, type DedupeStore } from "@murmurv2/core";

export interface BrokerConfig {
  url: string;
  stream?: string;
}

export type MessageHandler = (envelope: EnvelopeV1) => Promise<void>;

export class NatsBroker {
  private nc?: NatsConnection;
  private readonly sc = StringCodec();

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

  async publish(subject: string, envelope: EnvelopeV1): Promise<void> {
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
  }): Promise<Subscription> {
    await this.connect();

    const sub = this.nc!.subscribe(params.subject);
    const ackSubject = `ack.${params.consumerId}`;

    (async () => {
      for await (const m of sub) {
        try {
          const decoded = JSON.parse(this.sc.decode(m.data));
          if (!isEnvelopeV1(decoded)) {
            await this.publishAck(ackSubject, createAck("unknown", params.consumerId, "nack", "invalid-envelope"));
            continue;
          }

          const isDup = await params.dedupe.seen(decoded.msgId, params.consumerId);
          if (isDup) {
            await this.publishAck(ackSubject, createAck(decoded.msgId, params.consumerId, "ack", "duplicate-ignored"));
            continue;
          }

          await params.onMessage(decoded);
          await params.dedupe.markSeen(decoded.msgId, params.consumerId);
          await this.publishAck(ackSubject, createAck(decoded.msgId, params.consumerId, "ack"));
        } catch (err) {
          const reason = err instanceof Error ? err.message : "handler-failed";
          await this.publishAck(ackSubject, createAck("unknown", params.consumerId, "nack", reason));
        }
      }
    })().catch(() => undefined);

    return sub;
  }
}
