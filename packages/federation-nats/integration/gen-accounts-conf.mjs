// Generate a nats-server accounts config from the federation account contract,
// so the LIVE two-org test runs against the SAME exports/imports the contract
// describes (no hand-divergence between contract and the server config).
// Maps each org's contract to NATS service export/import (cross-account one-way
// publish: an importing account may publish into the exporter's service subject).
import { renderFederationNatsAccountsConfig } from "../dist/src/index.js";

const ORGS = (process.env.FED_ORGS || "aimindset,partner").split(",");
const PORT = process.env.FED_NATS_PORT || "14333";

process.stdout.write(renderFederationNatsAccountsConfig({ orgs: ORGS, port: PORT }));
