import { setTimeout as sleep } from "node:timers/promises";
import { NatsBroker } from "@murmurv2/broker-nats";
import { SQLiteDedupeOutboxStore, validateEnvelopePolicy } from "@murmurv2/core";
import { decryptPayload, getCryptoProvider, verifyEnvelopeSignature } from "@murmurv2/security";
import { ensureDemoKeys, loadDemoConfig, policyFromConfig, stableEnvelopePayload } from "./demo-secure-common.mjs";

const cfg = loadDemoConfig();
const keys = await ensureDemoKeys(cfg.keysPath);

const broker = new NatsBroker({ url: cfg.natsUrl });
const dedupe = new SQLiteDedupeOutboxStore(cfg.dedupeDbPath);
const policy = policyFromConfig(cfg);

console.log("[consumer] secure listener starting", {
  subject: cfg.subject,
  consumerId: cfg.consumerId,
  cryptoProvider: getCryptoProvider().name,
});

let shouldExit = false;

try {
  await broker.subscribeWithAck({
    subject: cfg.subject,
    consumerId: cfg.consumerId,
    dedupe,
    onMessage: async (envelope) => {
      const violations = validateEnvelopePolicy(envelope, policy);
      if (violations.length > 0) {
        throw new Error(`policy-rejected:${violations.join("|")}`);
      }

      const validSignature = await verifyEnvelopeSignature(
        stableEnvelopePayload(envelope),
        envelope.signature,
        keys.sender.signing.publicKey,
      );
      if (!validSignature) {
        throw new Error("signature-invalid");
      }

      const plaintext = await decryptPayload(
        {
          ciphertext: envelope.payloadCiphertext,
          nonce: envelope.payloadNonce,
          senderPublicKey: keys.sender.encryption.publicKey,
        },
        keys.recipient.encryption.privateKey,
      );

      console.log("[consumer] verified + decrypted envelope", {
        msgId: envelope.msgId,
        conversationId: envelope.conversationId,
        senderAgentId: envelope.senderAgentId,
        text: plaintext,
      });

      if (cfg.exitAfterOne) {
        shouldExit = true;
      }
    },
  });

  while (true) {
    if (shouldExit) {
      await broker.close();
      process.exit(0);
    }
    await sleep(200);
  }
} catch (err) {
  console.error("[consumer] secure demo failed", {
    error: err instanceof Error ? err.message : String(err),
  });
  await broker.close().catch(() => undefined);
  process.exitCode = 1;
}
