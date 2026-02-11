export type DeliveryMode = 'at-least-once';

export interface EnvelopeV1 {
  schemaVersion: '1.0';
  msgId: string;          // ULID
  conversationId: string;
  senderAgentId: string;
  recipients: string[];
  createdAt: string;      // ISO8601
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
  status: 'ack' | 'nack';
  reason?: string;
  at: string;
}

export const isEnvelopeV1 = (v: unknown): v is EnvelopeV1 => {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  return o.schemaVersion === '1.0' && typeof o.msgId === 'string' && typeof o.conversationId === 'string';
};
