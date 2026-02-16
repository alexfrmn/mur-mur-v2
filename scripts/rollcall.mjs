import { readFile } from 'fs/promises';
import { NatsBroker } from '@murmurv2/broker-nats';
import { encryptPayload, signEnvelope } from '@murmurv2/security';
import { randomUUID } from 'crypto';

const config = JSON.parse(await readFile('.data/agent-config.json', 'utf8'));
const { agentId, natsUrl, natsToken, peers, keys } = config;
const broker = new NatsBroker({ url: natsUrl, token: natsToken });
await broker.connect();

const targets = Object.keys(peers);
console.log('Sending PING to:', targets.join(', '));

for (const peerId of targets) {
  const peer = peers[peerId];
  const text = 'ROLL CALL PING from JARVIS @ ' + new Date().toISOString();
  
  const { ciphertext, nonce } = await encryptPayload(
    text,
    peer.encryption.publicKey,
    keys.encryption.privateKey
  );
  
  const msgId = randomUUID();
  const envelope = {
    schemaVersion: "1.0",
    msgId,
    conversationId: 'roll-call-' + Date.now(),
    senderAgentId: agentId,
    recipients: [peerId],
    createdAt: new Date().toISOString(),
    payloadCiphertext: ciphertext,
    payloadNonce: nonce,
  };
  
  const sigPayload = JSON.stringify({
    schemaVersion: envelope.schemaVersion,
    msgId: envelope.msgId,
    conversationId: envelope.conversationId,
    senderAgentId: envelope.senderAgentId,
    recipients: [...envelope.recipients],
    createdAt: envelope.createdAt,
    payloadCiphertext: envelope.payloadCiphertext,
    payloadNonce: envelope.payloadNonce,
  });
  
  envelope.signature = await signEnvelope(sigPayload, keys.signing.privateKey);
  
  await broker.publish('msg.' + peerId, envelope);
  console.log('  ✉️  ' + peerId + ' — sent (msgId: ' + msgId.slice(0,8) + ')');
}

await broker.close();
console.log('Done!');
