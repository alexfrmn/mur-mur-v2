import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";
import { createInterface } from "node:readline";
import {
  ChannelRosterStore,
  SQLiteDedupeOutboxStore,
  SQLiteMessageStore,
  stableEnvelopePayload,
  type EnvelopeV1,
  type LocalMessageRecord,
} from "@murmurv2/core";
import { encryptPayload, signEnvelope } from "@murmurv2/security";
import { NatsBroker, type BrokerSubscription } from "@murmurv2/broker-nats";
import { buildReplyMatcher, waitForReply } from "./request-reply.js";

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
const channelRosterPath = process.env.MURMUR_CHANNEL_ROSTER_PATH ?? path.join(dataDir, "channel-roster.db");

let agentConfig: AgentConfig | null = null;
try {
  agentConfig = JSON.parse(readFileSync(configPath, "utf8")) as AgentConfig;
} catch {
  // Agent config not found — send/inbox/peers tools will be unavailable
}

const store = new SQLiteMessageStore(dbPath);
const channelRoster = new ChannelRosterStore(channelRosterPath);

// Outbox store — shared with daemon, only created if agent config exists
let outbox: SQLiteDedupeOutboxStore | null = null;
if (agentConfig) {
  outbox = new SQLiteDedupeOutboxStore(dbPath);
}

// Lazy read-only NATS tap for wake-accelerated murmur_request. Optional — if NATS is
// unreachable we degrade gracefully to pure store polling. A failed connect does NOT
// disable acceleration forever: it's retried after a cooldown so a transient outage at
// startup doesn't permanently fall back to slow polling (per CODEX-VOLT review).
const WAKE_BROKER_RETRY_COOLDOWN_MS = 30_000;
let wakeBroker: NatsBroker | null = null;
let wakeBrokerNextRetry = 0;
const getWakeBroker = async (now: () => number = Date.now): Promise<NatsBroker | null> => {
  if (!agentConfig) return null;
  if (wakeBroker) return wakeBroker;
  if (now() < wakeBrokerNextRetry) return null; // in cooldown after a recent failure
  try {
    const broker = new NatsBroker({
      url: agentConfig.natsUrl,
      token: agentConfig.natsToken,
      jetstream: false,
    });
    await broker.connect();
    wakeBroker = broker;
  } catch {
    // graceful degrade — store polling still resolves the reply; retry after cooldown
    wakeBroker = null;
    wakeBrokerNextRetry = now() + WAKE_BROKER_RETRY_COOLDOWN_MS;
  }
  return wakeBroker;
};

// stableEnvelopePayload is the canonical signing form from @murmurv2/core
// (single source of truth shared by daemon / bridges / runner / demos).

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

  if (name === "channel_create") {
    const channelId = String(args.channelId ?? "").trim();
    if (!channelId) throw new Error("channelId is required");
    const conversationId = String(args.conversationId ?? "").trim();
    if (!conversationId) throw new Error("conversationId is required");
    const type = String(args.type ?? "").trim();
    if (!["dm", "group", "consult"].includes(type)) throw new Error("type must be one of: dm, group, consult");
    const members = Array.isArray(args.members) ? args.members as Array<Record<string, unknown>> : [];
    const channel = channelRoster.createChannel({
      channelId,
      conversationId,
      type: type as "dm" | "group" | "consult",
      metadata: typeof args.metadata === "object" && args.metadata && !Array.isArray(args.metadata) ? args.metadata as Record<string, unknown> : undefined,
      members: members.map((member) => ({
        memberId: String(member.memberId ?? "").trim(),
        memberSlot: member.memberSlot === undefined ? undefined : String(member.memberSlot),
        agentId: String(member.agentId ?? "").trim(),
        role: member.role === undefined ? undefined : String(member.role),
        personaId: member.personaId === undefined ? undefined : String(member.personaId),
        model: member.model === undefined ? undefined : String(member.model),
        baseInstructionsHash: member.baseInstructionsHash === undefined ? undefined : String(member.baseInstructionsHash),
        eligibility: typeof member.eligibility === "object" && member.eligibility && !Array.isArray(member.eligibility) ? member.eligibility as Record<string, unknown> : undefined,
        metadata: typeof member.metadata === "object" && member.metadata && !Array.isArray(member.metadata) ? member.metadata as Record<string, unknown> : undefined,
      })),
    });
    return { channel, members: channelRoster.listChannelMembers(channelId) };
  }

  if (name === "channel_list") {
    const conversationId = String(args.conversationId ?? "").trim();
    if (!conversationId) throw new Error("conversationId is required");
    return { channels: channelRoster.listChannelsForConversation(conversationId) };
  }

  if (name === "channel_members") {
    const channelId = String(args.channelId ?? "").trim();
    if (!channelId) throw new Error("channelId is required");
    return { members: channelRoster.listChannelMembers(channelId) };
  }

  if (name === "channel_evaluate_addressing") {
    const selfAgentId = String(args.selfAgentId ?? "").trim();
    if (!selfAgentId) throw new Error("selfAgentId is required");
    return {
      decision: channelRoster.evaluateAddressing({
        channelId: args.channelId === undefined ? undefined : String(args.channelId),
        selfAgentId,
        senderAgentId: args.senderAgentId === undefined ? undefined : String(args.senderAgentId),
        addresseeMemberId: args.addresseeMemberId === undefined ? undefined : String(args.addresseeMemberId),
        addresseeAgentId: args.addresseeAgentId === undefined ? undefined : String(args.addresseeAgentId),
      }),
    };
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

    // Wait for the reply. Store polling is the durable fallback and always runs;
    // an optional read-only NATS tap on our own subject accelerates the wait by
    // re-checking the store as soon as a matching envelope is observed. The tap is
    // signal-only — the daemon stays the source of truth for decrypt + persistence.
    const graceMs = Number(args.grace_ms ?? 250);
    const deadline = Date.now() + timeoutMs;
    const matchReply = buildReplyMatcher(conversationId, to);

    const broker = await getWakeBroker();
    // Holder object: the tap is attached inside a callback, so a plain `let` would be
    // narrowed to `null` by control-flow analysis. A mutable property keeps its type.
    const tap: { attach: Promise<void> | null; sub: BrokerSubscription | null } = {
      attach: null,
      sub: null,
    };
    let wokenBySignal = false;
    let onSignal: ((wake: () => void) => void) | undefined;
    if (broker) {
      onSignal = (wake) => {
        tap.attach = broker
          .subscribeRaw(agentConfig!.subject, (env) => {
            if (matchReply(env)) {
              wokenBySignal = true;
              wake();
            }
          })
          .then((sub) => {
            tap.sub = sub;
          })
          .catch(() => {
            /* tap failed to attach — store polling still resolves the reply */
          });
      };
    }

    let reply: LocalMessageRecord | null = null;
    try {
      reply = await waitForReply({
        checkStore: async () => {
          const inbound = await store.getInboundAfter(conversationId, sentAt, 1);
          return inbound.length > 0 ? inbound[0] : null;
        },
        pollMs,
        graceMs,
        deadline,
        onSignal,
      });
    } finally {
      if (tap.attach) await tap.attach;
      if (tap.sub) {
        try {
          await tap.sub.unsubscribe();
        } catch {
          /* ignore unsubscribe errors */
        }
      }
    }

    if (reply) {
      return {
        status: "received",
        msgId,
        conversationId,
        sentAt,
        reply: asMessage(reply),
        // Precise telemetry (per CODEX-VOLT review): tapAttached = the read-only NATS
        // tap was live for this wait; wokenBySignal = a matching envelope actually
        // short-circuited the poll (true acceleration, not merely "broker available").
        tapAttached: tap.sub !== null,
        wokenBySignal,
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
    name: "channel_create",
    description: "Create a typed Murmur channel roster entry with optional members.",
    inputSchema: {
      type: "object",
      properties: {
        channelId: { type: "string", description: "Stable channel/routing ID, distinct from conversationId" },
        conversationId: { type: "string", description: "Legacy history label associated with the channel" },
        type: { type: "string", enum: ["dm", "group", "consult"] },
        metadata: { type: "object" },
        members: {
          type: "array",
          items: {
            type: "object",
            properties: {
              memberId: { type: "string" },
              memberSlot: { type: "string" },
              agentId: { type: "string" },
              role: { type: "string" },
              personaId: { type: "string" },
              model: { type: "string" },
              baseInstructionsHash: { type: "string" },
              eligibility: { type: "object" },
              metadata: { type: "object" },
            },
            required: ["memberId", "agentId"],
          },
        },
      },
      required: ["channelId", "conversationId", "type"],
    },
  },
  {
    name: "channel_list",
    description: "List typed channels associated with a legacy conversationId.",
    inputSchema: {
      type: "object",
      properties: {
        conversationId: { type: "string" },
      },
      required: ["conversationId"],
    },
  },
  {
    name: "channel_members",
    description: "List active and historical members for a typed channel.",
    inputSchema: {
      type: "object",
      properties: {
        channelId: { type: "string" },
      },
      required: ["channelId"],
    },
  },
  {
    name: "channel_evaluate_addressing",
    description: "Evaluate channel membership/addressing into reject/append/wake decisions.",
    inputSchema: {
      type: "object",
      properties: {
        channelId: { type: "string" },
        selfAgentId: { type: "string" },
        senderAgentId: { type: "string" },
        addresseeMemberId: { type: "string" },
        addresseeAgentId: { type: "string" },
      },
      required: ["selfAgentId"],
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
      "Send a message and wait for the reply. Combines murmur_send with a durable store-poll, accelerated by a read-only NATS tap so the reply is returned as soon as it lands (falls back to pure polling when NATS is unavailable). The tool blocks until the peer responds or timeout is reached. Ideal for autonomous agent-to-agent conversations.",
    inputSchema: {
      type: "object",
      properties: {
        to: { type: "string", description: "Recipient agent ID (must be in peers config)" },
        text: { type: "string", description: "Message text (will be encrypted)" },
        conversationId: { type: "string", description: "Optional conversation ID" },
        timeout_ms: { type: "number", description: "Max wait time in ms (default: 300000 = 5 min)" },
        poll_interval_ms: { type: "number", description: "Store-poll fallback interval in ms (default: 10000 = 10s)" },
        grace_ms: { type: "number", description: "Delay after a wake signal before re-checking the store, to let the daemon persist (default: 250)" },
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
