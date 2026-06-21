#!/usr/bin/env node
// One-shot publish-prep for the @murmurv2/* workspace packages: makes each package
// publishable to the public npm registry without changing its name. Idempotent.
//   - drop `private: true`
//   - set license: "MIT" + repository (with directory) + publishConfig.access=public
//   - add a short `description` (npm hygiene; stubs marked experimental)
//   - rewrite intra-workspace `@murmurv2/*` deps (file:../x or pinned) to `^<version>`
//   - `prepack: npm run build` guard so a publish/pack can never ship an empty dist
//   - files: ["dist/src", "LICENSE"] (+ "schema" when present) — ships compiled JS+d.ts,
//     drops dist/tsconfig.tsbuildinfo noise
import { readFileSync, writeFileSync, readdirSync, statSync, existsSync } from "node:fs";
import path from "node:path";

const REPO_URL = "git+https://github.com/alexfrmn/mur-mur-v2.git";
const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const pkgsDir = path.join(root, "packages");

const DESCRIPTIONS = {
  "@murmurv2/core": "Murmur V2 core — EnvelopeV1/AckV1 wire types, SQLite outbox + dedupe stores, machine-readable protocol schema.",
  "@murmurv2/security": "Murmur V2 crypto — X25519 + XChaCha20-Poly1305 + Ed25519 envelope encrypt/sign/verify (MLS scaffold).",
  "@murmurv2/broker-nats": "Murmur V2 NATS broker — core pub/sub with optional JetStream durability (finite redelivery + DLQ).",
  "@murmurv2/mcp-server": "Murmur V2 MCP server — 7 tools for agent-to-agent messaging over the Model Context Protocol.",
  "@murmurv2/federation": "Murmur V2 federation — org/agent addressing, Ed25519 signed roster + RosterStore, roster-backed auth tokens.",
  "@murmurv2/federation-nats": "Murmur V2 federation NATS contract — fed.* subjects + account-config renderer for cross-org leaf-node meshes.",
  "@murmurv2/bridge-a2a": "Murmur V2 to A2A protocol bridge (alpha) — terminates @a2a-js/sdk and re-wraps tasks as E2E Murmur envelopes.",
  "@murmurv2/bridge-telegram": "Murmur V2 Telegram notification adapter.",
  "@murmurv2/bridge-murmur": "Murmur V2 Murmur-to-Murmur bridge (experimental placeholder/stub).",
  "@murmurv2/bridge-openclaw": "Murmur V2 OpenClaw bridge (legacy/experimental).",
  "@murmurv2/observability": "Murmur V2 observability helpers (scaffold).",
};

for (const dir of readdirSync(pkgsDir)) {
  const file = path.join(pkgsDir, dir, "package.json");
  let pkg;
  try {
    if (!statSync(file).isFile()) continue;
    pkg = JSON.parse(readFileSync(file, "utf8"));
  } catch {
    continue;
  }
  if (!pkg.name) continue;

  delete pkg.private;
  pkg.license = "MIT";
  pkg.repository = { type: "git", url: REPO_URL, directory: `packages/${dir}` };
  pkg.publishConfig = { access: "public" };
  if (DESCRIPTIONS[pkg.name]) pkg.description = DESCRIPTIONS[pkg.name];

  const version = pkg.version || "0.1.0";
  for (const key of ["dependencies", "devDependencies", "peerDependencies"]) {
    const deps = pkg[key];
    if (!deps) continue;
    for (const name of Object.keys(deps)) {
      if (name.startsWith("@murmurv2/")) deps[name] = `^${version}`;
    }
  }

  // build guard: npm pack/publish always rebuilds dist first → never ship an empty tarball
  pkg.scripts = { ...(pkg.scripts || {}), prepack: "npm run build" };

  // ship only the compiled output + LICENSE (+ schema where present); excludes tsbuildinfo
  const files = ["dist/src", "LICENSE"];
  if (existsSync(path.join(pkgsDir, dir, "schema"))) files.splice(1, 0, "schema");
  pkg.files = files;

  writeFileSync(file, JSON.stringify(pkg, null, 2) + "\n");
  console.log(`prepped ${pkg.name}@${version}`);
}
