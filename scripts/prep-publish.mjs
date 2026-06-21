#!/usr/bin/env node
// One-shot publish-prep for the @murmurv2/* workspace packages: makes each package
// publishable to the public npm registry without changing its name. Idempotent.
//   - drop `private: true`
//   - set license: "MIT" + repository (with directory) + publishConfig.access=public
//   - rewrite intra-workspace `@murmurv2/*` deps (file:../x or pinned) to `^<version>`
//     so a consumer `npm install` resolves them from the registry, not a local path
//   - ensure `files` ships `dist` (and keeps anything already declared, e.g. core/schema)
import { readFileSync, writeFileSync, readdirSync, statSync } from "node:fs";
import path from "node:path";

const REPO_URL = "git+https://github.com/alexfrmn/mur-mur-v2.git";
const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const pkgsDir = path.join(root, "packages");

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

  const version = pkg.version || "0.1.0";
  for (const key of ["dependencies", "devDependencies", "peerDependencies"]) {
    const deps = pkg[key];
    if (!deps) continue;
    for (const name of Object.keys(deps)) {
      if (name.startsWith("@murmurv2/")) deps[name] = `^${version}`;
    }
  }

  if (!Array.isArray(pkg.files)) pkg.files = ["dist"];
  else if (!pkg.files.includes("dist")) pkg.files = ["dist", ...pkg.files];

  writeFileSync(file, JSON.stringify(pkg, null, 2) + "\n");
  console.log(`prepped ${pkg.name}@${version}`);
}
