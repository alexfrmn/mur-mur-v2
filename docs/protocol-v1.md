# Protocol v1 (draft)

## Envelope lifecycle
1. Producer builds `EnvelopeV1`
2. Producer signs envelope and encrypts payload
3. Publish to subject `msg.<conversationId>`
4. Consumer validates schema+signature
5. Consumer processes idempotently using `msgId`
6. Consumer emits ACK or NACK
7. Retry policy moves failed messages; terminal failures go to DLQ

## Delivery model
- at-least-once delivery
- idempotent consumers mandatory
- per-conversation sequence ordering target

## Bridge mapping
- Murmur message -> EnvelopeV1
- OpenClaw session event -> EnvelopeV1
- Human channel events (Telegram etc.) -> EnvelopeV1
