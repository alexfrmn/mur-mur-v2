import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";
import { createInterface } from "node:readline";
import {
  SQLiteDedupeOutboxStore,
  SQLiteMessageStore,
  type EnvelopeV1,
  type LocalMessageRecord,
} from "@murmurv2/core";
import { encryptPayload, signEnvelope } from "@murmurv2/security";

interface JsonRpcRequest {
  jsonrpc: "2.0";
  id?: string | number;
  method: string;
  params?: Record<string, unknown>;
}

interface AgentConfig {
  agentId: string;
  natsUrl: string;
  natsToken?: string;
  subject: string;
  dataDir: string;
  keys: {
    encryption: { publicKey: string; privateKey: string };
    signing: { publicKey: string; privateKey: string };
  };
  peers: Record<
    string,
    {
      encryption: { publicKey: string };
      signing: { publicKey: string };
      subject: string;
    }
  >;
}

// --- Load agent config (optional — gracefully degrade if missing) ---
const dataDir = process.env.DATA_DIR || ".data";
const configPath = path.join(dataDir, "agent-config.json");
const dbPath = process.env.MURMUR_STORE_PATH ?? path.join(dataDir, "murmur.db");

let agentConfig: AgentConfig | null = null;
try {
  agentConfig = JSON.parse(readFileSync(configPath, "utf8")) as AgentConfig;
} catch {
  // Agent config not found — send/inbox/peers tools will be unavailable
}

const store = new SQLiteMessageStore(dbPath);

// Outbox store — shared with daemon, only created if agent config exists
let outbox: SQLiteDedupeOutboxStore | null = null;
if (agentConfig) {
  outbox = new SQLiteDedupeOutboxStore(dbPath);
}

// --- Stable envelope payload for signing ---
const stableEnvelopePayload = (envelope: EnvelopeV1): string => {
  return JSON.stringify({
    schemaVersion: envelope.schemaVersion,
    msgId: envelope.msgId,
    conversationId: envelope.conversationId,
    senderAgentId: envelope.senderAgentId,
    recipients: [...envelope.recipients],
    createdAt: envelope.createdAt,
    payloadCiphertext: envelope.payloadCiphertext,
    payloadNonce: envelope.payloadNonce,
  });
};

// --- JSON-RPC helpers ---
const send = (payload: unknown): void => {
  process.stdout.write(`${JSON.stringify(payload)}\n`);
};

const ok = (id: string | number | undefined, result: unknown): void => {
  if (id === undefined) return;
  send({ jsonrpc: "2.0", id, result });
};

const fail = (id: string | number | undefined, message: string): void => {
  if (id === undefined) return;
  send({ jsonrpc: "2.0", id, error: { code: -32000, message } });
};

const asMessage = (r: LocalMessageRecord): Record<string, unknown> => ({
  id: r.id,
  conversationId: r.conversationId,
  msgId: r.msgId,
  direction: r.direction,
  sender: r.sender,
  text: r.text,
  createdAt: r.createdAt,
  transport: r.transport,
});

// --- Tool handlers ---
const handleTool = async (name: string, args: Record<string, unknown>): Promise<unknown> => {
  // === Original tools ===
  if (name === "send_message") {
    const text = String(args.text ?? "").trim();
    if (!text) throw new Error("text is required");
    const conversationId = String(args.conversationId ?? "local:default");
    const sender = String(args.sender ?? "mcp-client");
    const msgId = String(args.msgId ?? randomUUID());

    const row = await store.append({
      conversationId,
      msgId,
      direction: "outbound",
      sender,
      text,
      createdAt: new Date().toISOString(),
      transport: "mcp",
    });
    return { message: asMessage(row) };
  }

  if (name === "list_conversations") {
    const limit = Number(args.limit ?? 50);
    const conversations = await store.listConversations(Number.isFinite(limit) ? limit : 50);
    return { conversations };
  }

  if (name === "search_messages") {
    const query = String(args.query ?? "").trim();
    if (!query) throw new Error("query is required");
    const limit = Number(args.limit ?? 50);
    const messages = await store.searchMessages(query, Number.isFinite(limit) ? limit : 50);
    return { messages: messages.map(asMessage) };
  }

  // === New agent-to-agent tools (require agent config) ===

  if (name === "murmur_send") {
    if (!agentConfig || !outbox) throw new Error("agent config not loaded — run agent-config-init.mjs first");

    const to = String(args.to ?? "").trim();
    if (!to) throw new Error("'to' (recipient agent ID) is required");
    const text = String(args.text ?? "").trim();
    if (!text) throw new Error("'text' is required");

    const peer = agentConfig.peers[to];
    if (!peer) throw new Error(`unknown peer: ${to} — add to peers in agent-config.json`);

    const conversationId = String(args.conversationId ?? `dm:${agentConfig.agentId}:${to}`);
    const msgId = randomUUID();

    // Encrypt
    const encrypted = await encryptPayload(
      text,
      peer.encryption.publicKey,
      agentConfig.keys.encryption.privateKey,
    );

    // Build envelope
    const envelope: EnvelopeV1 = {
      schemaVersion: "1.0",
      msgId,
      conversationId,
      senderAgentId: agentConfig.agentId,
      recipients: [to],
      createdAt: new Date().toISOString(),
      payloadCiphertext: encrypted.ciphertext,
      payloadNonce: encrypted.nonce,
      signature: "",
    };

    // Sign
    envelope.signature = await signEnvelope(
      stableEnvelopePayload(envelope),
      agentConfig.keys.signing.privateKey,
    );

    // Enqueue to outbox — daemon will flush to NATS
    await outbox.enqueue(peer.subject, envelope);

    // Store outbound copy in message store
    await store.append({
      conversationId,
      msgId,
      direction: "outbound",
      sender: agentConfig.agentId,
      text,
      createdAt: envelope.createdAt,
      transport: "nats",
    });

    return { msgId, to, conversationId, status: "queued" };
  }

  if (name === "murmur_inbox") {
    if (!agentConfig) throw new Error("agent config not loaded — run agent-config-init.mjs first");

    const limit = Number(args.limit ?? 20);
    const effectiveLimit = Number.isFinite(limit) ? limit : 20;
    // Search inbound messages for this agent
    const messages = await store.searchMessages(agentConfig.agentId, effectiveLimit * 5);
    // Filter to only inbound messages
    const inbound = messages
      .filter((m) => m.direction === "inbound")
      .slice(0, effectiveLimit);
    return { messages: inbound.map(asMessage), count: inbound.length };
  }

  if (name === "murmur_request") {
    if (!agentConfig || !outbox) throw new Error("agent config not loaded — run agent-config-init.mjs first");

    const to = String(args.to ?? "").trim();
    if (!to) throw new Error("'to' (recipient agent ID) is required");
    const text = String(args.text ?? "").trim();
    if (!text) throw new Error("'text' is required");

    const peer = agentConfig.peers[to];
    if (!peer) throw new Error(`unknown peer: ${to} — add to peers in agent-config.json`);

    const timeoutMs = Number(args.timeout_ms ?? 300_000);
    const pollMs = Number(args.poll_interval_ms ?? 10_000);
    const conversationId = String(args.conversationId ?? `dm:${agentConfig.agentId}:${to}`);
    const msgId = randomUUID();
    const sentAt = new Date().toISOString();

    // Encrypt
    const encrypted = await encryptPayload(
      text,
      peer.encryption.publicKey,
      agentConfig.keys.encryption.privateKey,
    );

    // Build envelope
    const envelope: EnvelopeV1 = {
      schemaVersion: "1.0",
      msgId,
      conversationId,
      senderAgentId: agentConfig.agentId,
      recipients: [to],
      createdAt: sentAt,
      payloadCiphertext: encrypted.ciphertext,
      payloadNonce: encrypted.nonce,
      signature: "",
    };

    // Sign
    envelope.signature = await signEnvelope(
      stableEnvelopePayload(envelope),
      agentConfig.keys.signing.privateKey,
    );

    // Enqueue to outbox
    await outbox.enqueue(peer.subject, envelope);

    // Store outbound copy
    await store.append({
      conversationId,
      msgId,
      direction: "outbound",
      sender: agentConfig.agentId,
      text,
      createdAt: sentAt,
      transport: "nats",
    });

    // Poll for response
    const deadline = Date.now() + timeoutMs;
    let reply: LocalMessageRecord | null = null;

    while (Date.now() < deadline) {
      const inbound = await store.getInboundAfter(conversationId, sentAt, 1);
      if (inbound.length > 0) {
        reply = inbound[0];
        break;
      }
      await new Promise((r) => setTimeout(r, pollMs));
    }

    if (reply) {
      return {
        status: "received",
        msgId,
        conversationId,
        sentAt,
        reply: asMessage(reply),
      };
    }

    return {
      status: "timeout",
      msgId,
      conversationId,
      sentAt,
      timeout_ms: timeoutMs,
      hint: "Use murmur_inbox to check for late responses",
    };
  }

  if (name === "murmur_peers") {
    if (!agentConfig) throw new Error("agent config not loaded — run agent-config-init.mjs first");

    const peerList = Object.entries(agentConfig.peers).map(([id, p]) => ({
      agentId: id,
      subject: p.subject,
      hasEncryptionKey: !!p.encryption?.publicKey,
      hasSigningKey: !!p.signing?.publicKey,
    }));
    return { agentId: agentConfig.agentId, peers: peerList };
  }

  throw new Error(`unknown tool: ${name}`);
};

// --- Tool definitions ---
const tools = [
  {
    name: "send_message",
    description: "Store a local outbound message in the Murmur message store.",
    inputSchema: {
      type: "object",
      properties: {
        conversationId: { type: "string" },
        text: { type: "string" },
        sender: { type: "string" },
        msgId: { type: "string" },
      },
      required: ["text"],
    },
  },
  {
    name: "list_conversations",
    description: "List known conversations from local persisted message store.",
    inputSchema: {
      type: "object",
      properties: {
        limit: { type: "number" },
      },
    },
  },
  {
    name: "search_messages",
    description: "Search local stored messages by text/sender/conversation.",
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string" },
        limit: { type: "number" },
      },
      required: ["query"],
    },
  },
  {
    name: "murmur_send",
    description:
      "Send an encrypted, signed message to another agent via NATS. Message is queued in outbox and delivered by murmur-daemon.",
    inputSchema: {
      type: "object",
      properties: {
        to: { type: "string", description: "Recipient agent ID (must be in peers config)" },
        text: { type: "string", description: "Message text (will be encrypted)" },
        conversationId: { type: "string", description: "Optional conversation ID" },
      },
      required: ["to", "text"],
    },
  },
  {
    name: "murmur_request",
    description:
      "Send a message and wait for the reply. Combines murmur_send + automatic polling — the tool blocks until the peer responds or timeout is reached. Ideal for autonomous agent-to-agent conversations.",
    inputSchema: {
      type: "object",
      properties: {
        to: { type: "string", description: "Recipient agent ID (must be in peers config)" },
        text: { type: "string", description: "Message text (will be encrypted)" },
        conversationId: { type: "string", description: "Optional conversation ID" },
        timeout_ms: { type: "number", description: "Max wait time in ms (default: 300000 = 5 min)" },
        poll_interval_ms: { type: "number", description: "Poll interval in ms (default: 10000 = 10s)" },
      },
      required: ["to", "text"],
    },
  },
  {
    name: "murmur_inbox",
    description:
      "Read inbound messages received from other agents. Messages are decrypted and stored by murmur-daemon.",
    inputSchema: {
      type: "object",
      properties: {
        limit: { type: "number", description: "Max messages to return (default 20)" },
      },
    },
  },
  {
    name: "murmur_peers",
    description: "List known peer agents and their key status.",
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
];

// --- JSON-RPC stdio loop ---
const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });

rl.on("line", async (line) => {
  if (!line.trim()) return;

  let req: JsonRpcRequest;
  try {
    req = JSON.parse(line) as JsonRpcRequest;
  } catch {
    return;
  }

  try {
    if (req.method === "initialize") {
      ok(req.id, {
        protocolVersion: "2024-11-05",
        serverInfo: { name: "murmur-v2-mcp", version: "0.2.0" },
        capabilities: { tools: {} },
      });
      return;
    }

    if (req.method === "tools/list") {
      ok(req.id, { tools });
      return;
    }

    if (req.method === "tools/call") {
      const name = String(req.params?.name ?? "");
      const args = (req.params?.arguments as Record<string, unknown> | undefined) ?? {};
      const result = await handleTool(name, args);
      ok(req.id, { content: [{ type: "text", text: JSON.stringify(result) }] });
      return;
    }

    if (req.method === "notifications/initialized") return;

    fail(req.id, `unsupported method: ${req.method}`);
  } catch (err) {
    fail(req.id, err instanceof Error ? err.message : "request failed");
  }
});
