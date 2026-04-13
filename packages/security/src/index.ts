import { createHash, randomBytes } from "node:crypto";
import { ed25519, x25519 } from "@noble/curves/ed25519";
import { xchacha20poly1305 } from "@noble/ciphers/chacha";

export interface KeyPair {
  publicKey: string;
  privateKey: string;
}

export interface SigningKeyPair {
  publicKey: string;
  privateKey: string;
}

export interface E2EEncryptedPayload {
  ciphertext: string;
  nonce: string;
  senderPublicKey: string;
}

export interface CryptoProvider {
  readonly name: string;
  generateKeyPair(): Promise<KeyPair>;
  encrypt(plaintext: string, recipientPublicKey: string, senderPrivateKey: string): Promise<E2EEncryptedPayload>;
  decrypt(payload: E2EEncryptedPayload, recipientPrivateKey: string): Promise<string>;
  sign(payload: string, privateKey: string): Promise<string>;
  verify(payload: string, signature: string, publicKey: string): Promise<boolean>;
}

const b64 = (bytes: Uint8Array): string => Buffer.from(bytes).toString("base64");
const fromB64 = (value: string): Uint8Array => new Uint8Array(Buffer.from(value, "base64"));

const deriveSymmetricKey = (sharedSecret: Uint8Array): Uint8Array => {
  const digest = createHash("sha256").update(sharedSecret).digest();
  return new Uint8Array(digest);
};

export class NaClCryptoProvider implements CryptoProvider {
  readonly name = "nacl-x25519-xchacha20poly1305";

  async generateKeyPair(): Promise<KeyPair> {
    const privateKey = x25519.utils.randomPrivateKey();
    const publicKey = x25519.getPublicKey(privateKey);
    return { publicKey: b64(publicKey), privateKey: b64(privateKey) };
  }

  async encrypt(plaintext: string, recipientPublicKey: string, senderPrivateKey: string): Promise<E2EEncryptedPayload> {
    const senderPrivate = fromB64(senderPrivateKey);
    const senderPublic = x25519.getPublicKey(senderPrivate);
    const recipientPublic = fromB64(recipientPublicKey);
    const shared = x25519.getSharedSecret(senderPrivate, recipientPublic);
    const key = deriveSymmetricKey(shared);
    const nonce = randomBytes(24);
    const cipher = xchacha20poly1305(key, nonce);
    const ciphertext = cipher.encrypt(new TextEncoder().encode(plaintext));
    return {
      ciphertext: b64(ciphertext),
      nonce: b64(nonce),
      senderPublicKey: b64(senderPublic),
    };
  }

  async decrypt(payload: E2EEncryptedPayload, recipientPrivateKey: string): Promise<string> {
    const recipientPrivate = fromB64(recipientPrivateKey);
    const senderPublic = fromB64(payload.senderPublicKey);
    const shared = x25519.getSharedSecret(recipientPrivate, senderPublic);
    const key = deriveSymmetricKey(shared);
    const nonce = fromB64(payload.nonce);
    const ciphertext = fromB64(payload.ciphertext);
    const cipher = xchacha20poly1305(key, nonce);
    const plaintext = cipher.decrypt(ciphertext);
    return new TextDecoder().decode(plaintext);
  }

  async sign(payload: string, privateKey: string): Promise<string> {
    const msg = new TextEncoder().encode(payload);
    const key = fromB64(privateKey);
    const signature = ed25519.sign(msg, key);
    return b64(signature);
  }

  async verify(payload: string, signature: string, publicKey: string): Promise<boolean> {
    const msg = new TextEncoder().encode(payload);
    return ed25519.verify(fromB64(signature), msg, fromB64(publicKey));
  }
}

let activeProvider: CryptoProvider = new NaClCryptoProvider();

export const setCryptoProvider = (provider: CryptoProvider): void => {
  activeProvider = provider;
};

export const getCryptoProvider = (): CryptoProvider => activeProvider;

export const createKeyPair = async (): Promise<KeyPair> => activeProvider.generateKeyPair();

export const createSigningKeyPair = async (): Promise<SigningKeyPair> => {
  const privateKey = ed25519.utils.randomPrivateKey();
  const publicKey = ed25519.getPublicKey(privateKey);
  return { publicKey: b64(publicKey), privateKey: b64(privateKey) };
};

export const signEnvelope = async (payload: string, privateKey: string): Promise<string> => {
  return activeProvider.sign(payload, privateKey);
};

export const verifyEnvelopeSignature = async (payload: string, signature: string, publicKey: string): Promise<boolean> => {
  return activeProvider.verify(payload, signature, publicKey);
};

export const encryptPayload = async (
  plaintext: string,
  recipientPublicKey: string,
  senderPrivateKey: string,
): Promise<E2EEncryptedPayload> => {
  return activeProvider.encrypt(plaintext, recipientPublicKey, senderPrivateKey);
};

export const decryptPayload = async (
  payload: E2EEncryptedPayload,
  recipientPrivateKey: string,
): Promise<string> => {
  return activeProvider.decrypt(payload, recipientPrivateKey);
};

export interface MlsProvider {
  readonly name: string;
  createGroup(groupId: string, members: string[]): Promise<void>;
  encryptForGroup(groupId: string, plaintext: string): Promise<string>;
  decryptForGroup(groupId: string, ciphertext: string): Promise<string>;
}

class NoopMlsProvider implements MlsProvider {
  readonly name = "noop-mls";

  async createGroup(_groupId: string, _members: string[]): Promise<void> {
    throw new Error("mls-disabled");
  }

  async encryptForGroup(_groupId: string, _plaintext: string): Promise<string> {
    throw new Error("mls-disabled");
  }

  async decryptForGroup(_groupId: string, _ciphertext: string): Promise<string> {
    throw new Error("mls-disabled");
  }
}

let mlsProvider: MlsProvider = new NoopMlsProvider();

export const isMlsEnabled = (): boolean => process.env.MURMUR_ENABLE_MLS === "1";

export const setMlsProvider = (provider: MlsProvider): void => {
  mlsProvider = provider;
};

export const getMlsProvider = (): MlsProvider => mlsProvider;

export class MlsAdapterPlaceholder implements MlsProvider {
  readonly name = "mls-adapter-placeholder";

  async createGroup(_groupId: string, _members: string[]): Promise<void> {
    throw new Error("mls-adapter-not-configured");
  }

  async encryptForGroup(_groupId: string, _plaintext: string): Promise<string> {
    throw new Error("mls-adapter-not-configured");
  }

  async decryptForGroup(_groupId: string, _ciphertext: string): Promise<string> {
    throw new Error("mls-adapter-not-configured");
  }
}
