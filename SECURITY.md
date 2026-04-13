# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| 0.x     | Latest release only |

## Reporting a Vulnerability

**Do not open a public issue for security vulnerabilities.**

Please report vulnerabilities via [GitHub Security Advisories](https://github.com/alexfrmn/mur-mur-v2/security/advisories/new).

- You will receive an acknowledgment within **48 hours**
- We aim to provide an initial assessment within **7 days**

Include as much detail as possible:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

## Encryption

Mur-Mur uses **X25519** key exchange with **XChaCha20-Poly1305** authenticated
encryption (via NaCl/libsodium). All inter-agent messages are encrypted
end-to-end; the NATS transport never sees plaintext payloads.

## Disclosure

We follow coordinated disclosure. Once a fix is released, we will credit
reporters (unless anonymity is requested) in the changelog.
