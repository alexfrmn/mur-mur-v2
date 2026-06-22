import { type ConnectionOptions, type Subscription } from "nats";
import { createAck, type EnvelopeV1, type DedupeStore, type OutboxStore, type SecurityPolicy } from "@murmurv2/core";
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
export type BrokerSubscription = Subscription | {
    unsubscribe(): void | Promise<void>;
};
export interface BrokerStatusEvent {
    type: string;
    data?: unknown;
    reconnects: number;
}
export declare const buildNatsConnectionOptions: (config: BrokerConfig) => ConnectionOptions;
export declare class NatsBroker {
    private readonly config;
    private nc?;
    private js?;
    private jsm?;
    private readonly sc;
    private readonly failedDeliveries;
    private reconnects;
    private statusLoop?;
    constructor(config: BrokerConfig);
    connect(): Promise<void>;
    close(): Promise<void>;
    getReconnectCount(): number;
    private jetStreamEnabled;
    private streamName;
    private streamSubjects;
    private jetStreamMaxDeliver;
    private jetStreamAckWaitNanos;
    private buildJetStreamConsumerConfig;
    private ensureJetStream;
    private startStatusLoop;
    publish(subject: string, envelope: EnvelopeV1, policy?: SecurityPolicy): Promise<void>;
    publishAck(subject: string, envelope: ReturnType<typeof createAck>): Promise<void>;
    private processEnvelopeFrame;
    private ensureJetStreamConsumer;
    private consumeJetStream;
    subscribeWithAck(params: {
        subject: string;
        consumerId: string;
        dedupe: DedupeStore;
        onMessage: MessageHandler;
        maxPoisonAttempts?: number;
    }): Promise<BrokerSubscription>;
    /**
     * Read-only real-time tap on a subject. Plain core subscription — no queue
     * group, no ACK publish, no dedupe, no JetStream consumer. A plain subscriber
     * still receives `js.publish`'d messages in real time, so this works whether
     * or not JetStream is enabled, and it never steals delivery from the durable
     * daemon consumer (which uses its own consumerId / queue semantics).
     *
     * Intended as a wake-signal source: `onEnvelope` receives the decoded
     * envelope METADATA only (conversationId, senderAgentId, msgId). It does NOT
     * decrypt the payload — decryption stays the daemon's responsibility. Malformed
     * frames are ignored (best-effort signal, not a delivery path).
     */
    subscribeRaw(subject: string, onEnvelope: (envelope: EnvelopeV1) => void): Promise<BrokerSubscription>;
    startAckCorrelation(params: {
        outbox: OutboxStore;
        ackSubject: string;
        consumerId?: string;
    }): Promise<BrokerSubscription>;
    private processAckFrame;
    startJetStreamAdvisoryDlq(params: {
        outbox: OutboxStore;
    }): Promise<BrokerSubscription>;
    private processJetStreamAdvisoryFrame;
    private jetStreamAdvisoryKind;
    private jetStreamAdvisoryReason;
    /**
     * Basic outbox worker:
     * - picks due records
     * - publishes
     * - marks sent/failed/dlq
     * - ACK correlation handled via startAckCorrelation(...)
     */
    flushOutbox(params: {
        outbox: OutboxStore;
        maxAttempts?: number;
        batchSize?: number;
        baseBackoffMs?: number;
        jitterRatio?: number;
        ackTimeoutMs?: number;
        policy?: SecurityPolicy;
    }): Promise<void>;
}
