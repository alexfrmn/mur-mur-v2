# MLS integration scaffold (feature-flagged)

This repository now includes an MLS abstraction in `@murmurv2/security` to prepare for group E2E.

## Current status

- `MlsProvider` interface is available (`createGroup`, `encryptForGroup`, `decryptForGroup`).
- Runtime flag: `MURMUR_ENABLE_MLS=1` enables MLS code paths in future integrations.
- Default provider is a safe noop that throws `mls-disabled`.
- `MlsAdapterPlaceholder` is included as a non-breaking adapter stub for future real MLS backends.

## Why scaffold only?

A production MLS backend requires:

- identity/authentication strategy
- group state persistence and rotation
- interop testing against a concrete MLS implementation

This scaffold keeps the build stable while allowing controlled incremental rollout.
