#!/usr/bin/env node
/**
 * send-task.mjs — Send encrypted task to a specific agent via Mur-Mur-V2
 * Usage: node scripts/send-task.mjs <agentId> <message>
 */
import { connect, StringCodec } from 'nats';
import { readFile } from 'fs/promises';
import { encryptPayload, signEnvelope } from '@murmurv2/security';
import { randomUUID } from 'crypto';
import { vaultGuardCheck } from './vault-guard.mjs';

const [,, targetAgent, ...msgParts] = process.argv;
const message = msgParts.join(' ');

if (!targetAgent || !message) {
  console.error('Usage: node scripts/send-task.mjs <agentId> <message>');
  console.error('Agents: agent-codex, codex2-agent-hq, glm-agent-hq, haiku-agent-hq');
  process.exit(1);
}

const sc = StringCodec();
const config = JSON.parse(await readFile('.data/agent-config.json', 'utf8'));
const peer = config.peers[targetAgent];

if (!peer) {
  console.error(`Unknown agent: ${targetAgent}. Available: ${Object.keys(config.peers).join(', ')}`);
  process.exit(1);
}

// Vault guard: warn if vault task going to non-vault agent
const guard = vaultGuardCheck(targetAgent, message, (level, msg, data) => {
  console.error(JSON.stringify({ ts: new Date().toISOString(), level, msg, ...data }));
});
if (guard.isVaultTask && !guard.allowed) {
  console.error(`\n⚠️  VAULT_GUARD: message contains vault keywords (${guard.keywords.join(', ')}) but ${targetAgent} has NO vault access.`);
  console.error(`   Consider using agent-codex-volt or agent-jarvis instead.\n`);
}

const { ciphertext, nonce } = await encryptPayload(message, peer.encryption.publicKey, config.keys.encryption.privateKey);
const envelope = {
  schemaVersion: '1.0',
  msgId: randomUUID(),
  conversationId: `task-${Date.now()}`,
  senderAgentId: config.agentId,
  recipients: [targetAgent],
  createdAt: new Date().toISOString(),
  payloadCiphertext: ciphertext,
  payloadNonce: nonce,
};

const sigPayload = JSON.stringify({
  schemaVersion: envelope.schemaVersion, msgId: envelope.msgId,
  conversationId: envelope.conversationId, senderAgentId: envelope.senderAgentId,
  recipients: [...envelope.recipients], createdAt: envelope.createdAt,
  payloadCiphertext: envelope.payloadCiphertext, payloadNonce: envelope.payloadNonce,
});
envelope.signature = await signEnvelope(sigPayload, config.keys.signing.privateKey);

const nc = await connect({ servers: config.natsUrl, token: config.natsToken });
nc.publish(`msg.${targetAgent}`, sc.encode(JSON.stringify(envelope)));
await nc.flush();
console.log(`✉️  ${targetAgent} ← ${message.slice(0, 80)}...`);
console.log(`   msgId: ${envelope.msgId}`);
await nc.close();
