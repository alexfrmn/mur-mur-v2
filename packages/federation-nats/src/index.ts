import type { EnvelopeV1 } from "@murmurv2/core";
import { parseAddress, type AgentAddress } from "@murmurv2/federation";

export type FederationSubjectKind = "msg" | "ack";

export type FederationAddress = AgentAddress;

export interface FederatedRoute<TEnvelope extends EnvelopeV1 = EnvelopeV1> {
  subject: string;
  address: FederationAddress;
  envelope: TEnvelope;
}

export interface FederationImport {
  account: string;
  subject: string;
}

export interface FederationAccountContract {
  localOrg: string;
  localAccount: string;
  exports: string[];
  imports: FederationImport[];
}

const FEDERATION_PREFIX = "fed";
const ENCODED_PREFIX = "_x";
const RAW_TOKEN_RE = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;
const ENCODED_TOKEN_RE = /^_x[A-Za-z0-9_-]+$/;
const FORBIDDEN_ADDRESS_CHARS_RE = /[\s/*>]/;

const assertAddressPart = (name: string, value: string): string => {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name}-missing`);
  }
  if (FORBIDDEN_ADDRESS_CHARS_RE.test(value)) {
    throw new Error(`${name}-contains-nats-wildcard-or-separator`);
  }
  return value;
};

export const encodeFederationToken = (value: string): string => {
  assertAddressPart("federation-token", value);
  if (RAW_TOKEN_RE.test(value) && !value.startsWith(ENCODED_PREFIX)) return value;
  return `${ENCODED_PREFIX}${Buffer.from(value, "utf8").toString("base64url")}`;
};

export const decodeFederationToken = (token: string): string => {
  if (RAW_TOKEN_RE.test(token) && !token.startsWith(ENCODED_PREFIX)) return token;
  if (!ENCODED_TOKEN_RE.test(token)) throw new Error("federation-token-invalid");
  const decoded = Buffer.from(token.slice(ENCODED_PREFIX.length), "base64url").toString("utf8");
  return assertAddressPart("federation-token", decoded);
};

export const parseFederationAddress = (raw: string, localOrg: string): FederationAddress => {
  return parseAddress(raw, localOrg);
};

export const formatFederationAddress = (address: FederationAddress): string => {
  const org = assertAddressPart("org", address.org);
  const agentId = assertAddressPart("agent-id", address.agentId);
  return `${org}/${agentId}`;
};

export const federationSubject = (kind: FederationSubjectKind, address: FederationAddress): string => {
  if (kind !== "msg" && kind !== "ack") throw new Error("federation-subject-kind-invalid");
  const org = encodeFederationToken(address.org);
  const agentId = encodeFederationToken(address.agentId);
  return `${FEDERATION_PREFIX}.${org}.${kind}.${agentId}`;
};

export const parseFederationSubject = (subject: string): { kind: FederationSubjectKind; address: FederationAddress } => {
  const parts = subject.split(".");
  if (parts.length !== 4 || parts[0] !== FEDERATION_PREFIX) throw new Error("federation-subject-invalid");
  const kind = parts[2];
  if (kind !== "msg" && kind !== "ack") throw new Error("federation-subject-kind-invalid");
  return {
    kind,
    address: {
      org: decodeFederationToken(parts[1] ?? ""),
      agentId: decodeFederationToken(parts[3] ?? ""),
    },
  };
};

export const federationMessageSubject = (recipient: string | FederationAddress, localOrg: string): string => {
  const address = typeof recipient === "string" ? parseFederationAddress(recipient, localOrg) : recipient;
  return federationSubject("msg", address);
};

export const federationAckSubject = (recipient: string | FederationAddress, localOrg: string): string => {
  const address = typeof recipient === "string" ? parseFederationAddress(recipient, localOrg) : recipient;
  return federationSubject("ack", address);
};

export const routeFederatedEnvelope = <TEnvelope extends EnvelopeV1>(
  envelope: TEnvelope,
  recipient: string | FederationAddress,
  localOrg: string,
): FederatedRoute<TEnvelope> => {
  const address = typeof recipient === "string" ? parseFederationAddress(recipient, localOrg) : recipient;
  return {
    subject: federationSubject("msg", address),
    address,
    envelope,
  };
};

export const orgAccountName = (org: string): string => {
  const encodedOrg = encodeFederationToken(org);
  return `ORG_${encodedOrg.toUpperCase().replaceAll("-", "_")}`;
};

export const buildFederationAccountContract = (
  localOrg: string,
  partnerOrgs: string[] = [],
): FederationAccountContract => {
  const localOrgToken = encodeFederationToken(localOrg);
  return {
    localOrg,
    localAccount: orgAccountName(localOrg),
    exports: [`${FEDERATION_PREFIX}.${localOrgToken}.>`],
    imports: partnerOrgs.map((partnerOrg) => ({
      account: orgAccountName(partnerOrg),
      subject: `${FEDERATION_PREFIX}.${encodeFederationToken(partnerOrg)}.>`,
    })),
  };
};
