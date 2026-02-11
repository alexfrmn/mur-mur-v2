export type DeliveryMode = "at-least-once";

export interface EnvelopeV1 {
  schemaVersion: "1.0";
  msgId: string;
  conversationId: string;
  senderAgentId: string;
  recipients: string[];
  createdAt: string;
  ttlSeconds?: number;
  traceId?: string;
  sequence?: number;
  parentMsgId?: string;
  payloadCiphertext: string;
  payloadNonce: string;
  signature: string;
}

export interface AckV1 {
  msgId: string;
  consumerId: string;
  status: "ack" | "nack";
  reason?: string;
  at: string;
}

export interface DedupeStore {
  seen(msgId: string, consumerId: string): Promise<boolean>;
  markSeen(msgId: string, consumerId: string): Promise<void>;
}

export class InMemoryDedupeStore implements DedupeStore {
  private readonly keys = new Set<string>();

  async seen(msgId: string, consumerId: string): Promise<boolean> {
    return this.keys.has(`${consumerId}:${msgId}`);
  }

  async markSeen(msgId: string, consumerId: string): Promise<void> {
    this.keys.add(`${consumerId}:${msgId}`);
  }
}

export const isEnvelopeV1 = (v: unknown): v is EnvelopeV1 => {
  if (!v || typeof v !== "object") return false;
  const o = v as Record<string, unknown>;
  return (
    o.schemaVersion === "1.0" &&
    typeof o.msgId === "string" &&
    typeof o.conversationId === "string" &&
    typeof o.senderAgentId === "string" &&
    Array.isArray(o.recipients) &&
    typeof o.payloadCiphertext === "string"
  );
};

export const createAck = (
  msgId: string,
  consumerId: string,
  status: AckV1["status"],
  reason?: string,
): AckV1 => ({
  msgId,
  consumerId,
  status,
  reason,
  at: new Date().toISOString(),
});
