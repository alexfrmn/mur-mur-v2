import { connect, StringCodec } from "nats";
import { randomUUID } from "node:crypto";

const nc = await connect({ servers: process.env.NATS_URL || "nats://127.0.0.1:4222" });
const sc = StringCodec();
const subject = process.env.SUBJECT || "msg.demo";

const envelope = {
  schemaVersion: "1.0",
  msgId: randomUUID(),
  conversationId: process.env.CONVERSATION_ID || "demo-room",
  senderAgentId: process.env.SENDER || "agent-codex",
  recipients: [process.env.RECIPIENT || "agent-jarvis"],
  createdAt: new Date().toISOString(),
  payloadCiphertext: Buffer.from(
    process.env.MESSAGE || "ClawDigest is live from mur-mur-v2 demo"
  ).toString("base64"),
  payloadNonce: "demo-nonce",
  signature: "demo-signature",
};

await nc.publish(subject, sc.encode(JSON.stringify(envelope)));
console.log(`[producer] published to ${subject}`, {
  msgId: envelope.msgId,
  conversationId: envelope.conversationId,
});

await nc.drain();
