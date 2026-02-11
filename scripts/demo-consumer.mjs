import { connect, StringCodec } from "nats";

const nc = await connect({ servers: process.env.NATS_URL || "nats://127.0.0.1:4222" });
const sc = StringCodec();
const subject = process.env.SUBJECT || "msg.demo";

console.log(`[consumer] connected ${nc.getServer()}`);
console.log(`[consumer] listening on ${subject}`);

const sub = nc.subscribe(subject);
for await (const m of sub) {
  try {
    const txt = sc.decode(m.data);
    const json = JSON.parse(txt);
    console.log("[consumer] envelope", {
      msgId: json.msgId,
      conversationId: json.conversationId,
      senderAgentId: json.senderAgentId,
      createdAt: json.createdAt,
    });
  } catch {
    console.log("[consumer] raw", sc.decode(m.data));
  }
}
