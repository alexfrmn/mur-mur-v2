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

export interface FederationNatsPermissions {
  publish: { allow: string[] };
  subscribe: { allow: string[] };
}

export interface FederationNatsUser {
  user: string;
  password: string;
  permissions?: FederationNatsPermissions;
}

export interface FederationNatsServiceExport {
  service: string;
  accounts?: string[];
}

export interface FederationNatsServiceImport {
  service: FederationImport;
}

export interface FederationNatsAccountConfig {
  account: string;
  users: FederationNatsUser[];
  exports: FederationNatsServiceExport[];
  imports: FederationNatsServiceImport[];
}

export interface BuildFederationNatsAccountConfigOptions {
  localOrg: string;
  partnerOrgs?: string[];
  users?: FederationNatsUser[];
  privateExports?: boolean;
  /**
   * Attach least-privilege pub/sub permissions to each user: publish only into
   * imported partner namespaces (`fed.<partner>.>`), subscribe only on this org's
   * own exported namespace (`fed.<self>.>`). Defense-in-depth for leaf users on top
   * of account isolation (acceptance #2/#3 in docs/federation-nats-contract.md).
   */
  restrictUserPermissions?: boolean;
}

export interface RenderFederationNatsAccountsConfigOptions {
  orgs: string[];
  port?: number | string;
  usersByOrg?: Record<string, FederationNatsUser[]>;
  privateExports?: boolean;
  restrictUserPermissions?: boolean;
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

const quoteNatsString = (value: string): string => JSON.stringify(value);

const federationPrefixWildcard = (org: string): string =>
  `${FEDERATION_PREFIX}.${encodeFederationToken(org)}.>`;

export const buildFederationNatsAccountConfig = ({
  localOrg,
  partnerOrgs = [],
  users = [{ user: localOrg, password: `pw_${localOrg}` }],
  privateExports = true,
  restrictUserPermissions = false,
}: BuildFederationNatsAccountConfigOptions): FederationNatsAccountConfig => {
  const contract = buildFederationAccountContract(localOrg, partnerOrgs);
  const partnerAccounts = partnerOrgs.map(orgAccountName);
  const permissions: FederationNatsPermissions | undefined = restrictUserPermissions
    ? {
        // Least privilege: send only into imported partner namespaces, receive only
        // on this org's own exported namespace.
        publish: { allow: partnerOrgs.map(federationPrefixWildcard) },
        subscribe: { allow: [federationPrefixWildcard(localOrg)] },
      }
    : undefined;
  return {
    account: contract.localAccount,
    users: permissions ? users.map((u) => ({ ...u, permissions })) : users,
    exports: contract.exports.map((service) => ({
      service,
      ...(privateExports ? { accounts: partnerAccounts } : {}),
    })),
    imports: contract.imports.map((service) => ({ service })),
  };
};

export const renderFederationNatsAccountsConfig = ({
  orgs,
  port = 14333,
  usersByOrg = {},
  privateExports = true,
  restrictUserPermissions = false,
}: RenderFederationNatsAccountsConfigOptions): string => {
  if (!Array.isArray(orgs) || orgs.length === 0) throw new Error("orgs-missing");
  const lines = [`port: ${port}`, "accounts {"];
  for (const org of orgs) {
    const partners = orgs.filter((o) => o !== org);
    const account = buildFederationNatsAccountConfig({
      localOrg: org,
      partnerOrgs: partners,
      users: usersByOrg[org] ?? [{ user: org, password: `pw_${org}` }],
      privateExports,
      restrictUserPermissions,
    });
    lines.push(`  ${account.account} {`);
    lines.push(`    users: [`);
    for (const user of account.users) {
      if (user.permissions) {
        const pub = user.permissions.publish.allow.map(quoteNatsString).join(", ");
        const sub = user.permissions.subscribe.allow.map(quoteNatsString).join(", ");
        lines.push(`      {`);
        lines.push(`        user: ${quoteNatsString(user.user)}, password: ${quoteNatsString(user.password)}`);
        lines.push(`        permissions: { publish: { allow: [${pub}] }, subscribe: { allow: [${sub}] } }`);
        lines.push(`      }`);
      } else {
        lines.push(`      { user: ${quoteNatsString(user.user)}, password: ${quoteNatsString(user.password)} }`);
      }
    }
    lines.push(`    ]`);
    lines.push(`    exports: [`);
    for (const ex of account.exports) {
      const accounts = ex.accounts ? `, accounts: [${ex.accounts.map(quoteNatsString).join(", ")}]` : "";
      lines.push(`      { service: ${quoteNatsString(ex.service)}${accounts} }`);
    }
    lines.push(`    ]`);
    lines.push(`    imports: [`);
    for (const im of account.imports) {
      lines.push(
        `      { service: { account: ${quoteNatsString(im.service.account)}, subject: ${quoteNatsString(im.service.subject)} } }`,
      );
    }
    lines.push(`    ]`);
    lines.push(`  }`);
  }
  lines.push("}");
  return `${lines.join("\n")}\n`;
};
