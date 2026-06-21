#!/usr/bin/env node
// Publish all @murmurv2/* packages to npm in dependency (topological) order.
// Safety: root-build first, then for each package assert its packed tarball actually
// contains dist/src/index.js + dist/src/index.d.ts BEFORE publishing (npm versions are
// immutable — never publish an empty/broken tarball). Auth comes from ~/.npmrc; pass an
// OTP via `--otp=<code>` or NPM_OTP env if the token is not 2FA-bypass.
import { execFileSync } from "node:child_process";

// topo order (deps before dependents); independent leaves anywhere.
const ORDER = [
  "@murmurv2/security",
  "@murmurv2/core",
  "@murmurv2/observability",
  "@murmurv2/bridge-murmur",
  "@murmurv2/federation",
  "@murmurv2/broker-nats",
  "@murmurv2/bridge-openclaw",
  "@murmurv2/bridge-telegram",
  "@murmurv2/mcp-server",
  "@murmurv2/bridge-a2a",
  "@murmurv2/federation-nats",
];

const otpArg = (process.argv.find((a) => a.startsWith("--otp=")) || "").slice(6) || process.env.NPM_OTP || "";
const run = (args) => execFileSync("npm", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });

console.log("== root build ==");
run(["run", "build"]);

for (const name of ORDER) {
  // assert the tarball is non-empty (contains compiled entry) before the irreversible publish
  const dry = run(["pack", "--workspace", name, "--dry-run", "--json"]);
  const files = JSON.parse(dry)[0]?.files?.map((f) => f.path) ?? [];
  const hasJs = files.includes("dist/src/index.js");
  const hasDts = files.includes("dist/src/index.d.ts");
  if (!hasJs || !hasDts) {
    console.error(`ABORT ${name}: tarball missing dist/src/index.{js,d.ts} (${files.join(", ")})`);
    process.exit(1);
  }
  const args = ["publish", "--workspace", name, "--access", "public"];
  if (otpArg) args.push(`--otp=${otpArg}`);
  try {
    run(args);
    console.log(`OK   ${name}`);
  } catch (err) {
    const msg = (err.stderr || err.stdout || String(err)).split("\n").find((l) => /error/i.test(l)) || String(err);
    console.error(`FAIL ${name}: ${msg}`);
    process.exit(1);
  }
}
console.log("== all published ==");
