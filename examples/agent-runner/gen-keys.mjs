#!/usr/bin/env node
/**
 * Generate a fresh Murmur V2 keypair (encryption + signing) for a new agent.
 *
 *   node gen-keys.mjs
 *
 * Prints a `keys` block to paste into agent-config.json. Send ONLY the two
 * publicKey values to the mesh operator (Alex/JARVIS) so they can add you as a
 * peer. NEVER share or commit the privateKey values.
 */
import { createKeyPair, createSigningKeyPair } from "@murmurv2/security";

const encryption = await createKeyPair();
const signing = await createSigningKeyPair();

console.log(
  JSON.stringify(
    {
      keys: { encryption, signing },
      _share_with_operator: {
        "encryption.publicKey": encryption.publicKey,
        "signing.publicKey": signing.publicKey,
      },
    },
    null,
    2,
  ),
);

console.error("\n# Paste the `keys` block into agent-config.json.");
console.error("# Send ONLY _share_with_operator (the two publicKeys) to the mesh operator.");
console.error("# Keep privateKeys secret — never commit, never send over chat.");
