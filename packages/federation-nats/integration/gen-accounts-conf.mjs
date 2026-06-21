// Generate a nats-server accounts config from the federation account contract,
// so the LIVE two-org test runs against the SAME exports/imports the contract
// describes (no hand-divergence between contract and the server config).
// Maps each org's contract to NATS service export/import (cross-account one-way
// publish: an importing account may publish into the exporter's service subject).
import { buildFederationAccountContract } from "../dist/src/index.js";

const ORGS = (process.env.FED_ORGS || "aimindset,partner").split(",");
const PORT = process.env.FED_NATS_PORT || "14333";

const lines = [`port: ${PORT}`, "accounts {"];
for (const org of ORGS) {
  const partners = ORGS.filter((o) => o !== org);
  const c = buildFederationAccountContract(org, partners);
  lines.push(`  ${c.localAccount} {`);
  lines.push(`    users: [{ user: "${org}", password: "pw_${org}" }]`);
  lines.push(`    exports: [`);
  for (const ex of c.exports) lines.push(`      { service: "${ex}" }`);
  lines.push(`    ]`);
  lines.push(`    imports: [`);
  for (const im of c.imports) lines.push(`      { service: { account: "${im.account}", subject: "${im.subject}" } }`);
  lines.push(`    ]`);
  lines.push(`  }`);
}
lines.push("}");
process.stdout.write(lines.join("\n") + "\n");
