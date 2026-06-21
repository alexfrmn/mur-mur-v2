// LIVE integration test for @murmurv2/bridge-a2a.
// Proves the full round-trip over REAL transports (no mocks of the hard parts):
//   A2A client --HTTP/JSON-RPC--> bridge --seal+NATS--> mock internal Murmur agent
//   --reply env on ack.<bridge>--> bridge --decrypt+correlate--> A2A reply to client.
// Requires a real nats-server (run via run-live.sh, default nats://127.0.0.1:14222).
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { connect, StringCodec } from "nats";
import {
  createKeyPair,
  createSigningKeyPair,
  decryptPayload,
  encryptPayload,
  signEnvelope,
} from "../../security/dist/src/index.js";
import { A2AMurmurBridge, extractText, stableEnvelopePayload } from "../dist/src/index.js";
import { ClientFactory } from "@a2a-js/sdk/client";
import { Role } from "@a2a-js/sdk";
import net from "node:net";

const NATS_URL = process.env.TEST_NATS_URL || "nats://127.0.0.1:14222";
const sc = StringCodec();
const ANSWER = "internal-agent-reply-42";

// Self-skip when no nats-server is reachable (file lives in integration/, outside
// node --test default discovery, but guard anyway so it never hard-fails in CI).
function natsReachable(url) {
  const { hostname, port } = new URL(url);
  return new Promise((resolve) => {
    const s = net.connect({ host: hostname, port: Number(port) }, () => { s.destroy(); resolve(true); });
    s.on("error", () => resolve(false));
    s.setTimeout(1500, () => { s.destroy(); resolve(false); });
  });
}

test("LIVE: A2A client -> bridge -> NATS mesh -> mock agent -> reply", async (t) => {
  if (!(await natsReachable(NATS_URL))) {
    t.skip(`no nats-server at ${NATS_URL}; run via integration/run-live.sh`);
    return;
  }
  const bridgeEnc = await createKeyPair();
  const bridgeSign = await createSigningKeyPair();
  const agentEnc = await createKeyPair();
  const agentSign = await createSigningKeyPair();

  const target = "agent-jarvis";
  const a2aPort = 14310;

  const cfg = {
    natsUrl: NATS_URL,
    agentId: "a2a-bridge",
    defaultTargetAgentId: target,
    a2aPort,
    signingPrivateKey: bridgeSign.privateKey,
    encryptionPrivateKey: bridgeEnc.privateKey,
    recipientPublicKeys: { [target]: agentEnc.publicKey },
    allowedExternalAgents: ["ext-1"],
    replyTimeoutMs: 10_000,
  };

  // ── mock internal Murmur agent over real NATS ──
  const nc = await connect({ servers: NATS_URL });
  const sub = nc.subscribe(`msg.${target}`);
  let receivedTaskText = null;
  const agentLoop = (async () => {
    for await (const m of sub) {
      const env = JSON.parse(sc.decode(m.data));
      // open the sealed task (proves bridge seal -> agent open works end-to-end)
      const plain = await decryptPayload(
        { ciphertext: env.payloadCiphertext, nonce: env.payloadNonce, senderPublicKey: bridgeEnc.publicKey },
        agentEnc.privateKey,
      );
      const task = JSON.parse(plain);
      receivedTaskText = task.text;
      // reply sealed back to the bridge, parentMsgId correlates to the original
      const replyEnc = await encryptPayload(
        JSON.stringify({ reply: ANSWER, echo: task.text }),
        bridgeEnc.publicKey,
        agentEnc.privateKey,
      );
      const unsigned = {
        schemaVersion: "1.0",
        msgId: randomUUID(),
        parentMsgId: env.msgId,
        conversationId: env.conversationId,
        senderAgentId: target,
        recipients: [env.senderAgentId],
        createdAt: new Date().toISOString(),
        payloadCiphertext: replyEnc.ciphertext,
        payloadNonce: replyEnc.nonce,
      };
      const signature = await signEnvelope(stableEnvelopePayload({ ...unsigned, signature: "" }), agentSign.privateKey);
      nc.publish(`ack.${cfg.agentId}`, sc.encode(JSON.stringify({ ...unsigned, signature })));
    }
  })();

  const bridge = new A2AMurmurBridge(cfg);
  await bridge.start();

  t.after(async () => {
    await bridge.stop();
    sub.unsubscribe();
    await agentLoop.catch(() => {});
    await nc.drain();
  });

  // ── real A2A client over HTTP ──
  const factory = new ClientFactory();
  const client = await factory.createFromUrl(`http://127.0.0.1:${a2aPort}`);

  const message = {
    messageId: randomUUID(),
    contextId: randomUUID(),
    taskId: "",
    role: Role.ROLE_USER,
    parts: [{ content: { $case: "text", value: "please do the thing" }, metadata: undefined, filename: "", mediaType: "text/plain" }],
    metadata: { externalAgentId: "ext-1" },
    extensions: [],
    referenceTaskIds: [],
  };

  const result = await client.sendMessage({ message });

  const replyText = extractText(result);
  assert.ok(receivedTaskText && receivedTaskText.includes("please do the thing"), `mock agent saw task text, got: ${receivedTaskText}`);
  assert.ok(replyText.includes(ANSWER), `A2A reply should carry internal answer, got: ${JSON.stringify(replyText)}`);
});
