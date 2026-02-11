export interface KeyPair {
  publicKey: string;
  privateKey: string;
}

export const signEnvelope = async (payload: string, privateKey: string): Promise<string> => {
  // TODO: Ed25519 sign
  return `${privateKey.slice(0, 8)}.${payload.length}`;
};

export const encryptPayload = async (plaintext: string, recipientPublicKey: string): Promise<{ciphertext: string; nonce: string}> => {
  // TODO: X25519 + XChaCha20-Poly1305
  return { ciphertext: Buffer.from(plaintext).toString('base64'), nonce: recipientPublicKey.slice(0, 12) };
};
