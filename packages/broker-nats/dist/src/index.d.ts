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
