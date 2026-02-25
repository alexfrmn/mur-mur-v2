#!/usr/bin/env node
/**
 * delegate-mcp.mjs — MCP server for delegating coding tasks to CODEX agents.
 *
 * Tools:
 *   - delegate_task: Send task to CODEX, wait for response
 *   - check_response: Check if a previously sent task got a response
 *   - list_agents: List available agents and their status
 *
 * Register in Claude Code:
 *   claude mcp add delegate-codex -- node /opt/lifecoach/mur-mur-v2/scripts/delegate-mcp.mjs
 */

import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { SQLiteDedupeOutboxStore, SQLiteMessageStore } from "@murmurv2/core";
import { encryptPayload, signEnvelope } from "@murmurv2/security";

const dataDir = process.env.DATA_DIR || ".data";
const config = JSON.parse(readFileSync(`${dataDir}/agent-config.json`, "utf8"));
const store = new SQLiteMessageStore(`${dataDir}/murmur.db`);
const outbox = new SQLiteDedupeOutboxStore(`${dataDir}/murmur.db`);

// Stable envelope for signing
const stableEnvelopePayload = (env) =>
  JSON.stringify({
    schemaVersion: env.schemaVersion,
    msgId: env.msgId,
    conversationId: env.conversationId,
    senderAgentId: env.senderAgentId,
    recipients: [...env.recipients],
    createdAt: env.createdAt,
    payloadCiphertext: env.payloadCiphertext,
    payloadNonce: env.payloadNonce,
  });

// Send encrypted message to peer
async function sendToPeer(to, text, conversationId) {
  const peer = config.peers[to];
  if (!peer) throw new Error(`Unknown peer: ${to}. Available: ${Object.keys(config.peers).join(", ")}`);

  const msgId = randomUUID();
  const createdAt = new Date().toISOString();

  const encrypted = await encryptPayload(text, peer.encryption.publicKey, config.keys.encryption.privateKey);

  const envelope = {
    schemaVersion: "1.0",
    msgId,
    conversationId,
    senderAgentId: config.agentId,
    recipients: [to],
    createdAt,
    payloadCiphertext: encrypted.ciphertext,
    payloadNonce: encrypted.nonce,
    signature: "",
  };

  envelope.signature = await signEnvelope(stableEnvelopePayload(envelope), config.keys.signing.privateKey);

  await outbox.enqueue(peer.subject, envelope);
  await store.append({ conversationId, msgId, direction: "outbound", sender: config.agentId, text, createdAt, transport: "nats" });

  return { msgId, conversationId, createdAt };
}

// Poll for response
async function pollResponse(conversationId, sentAfter, timeoutMs = 120000) {
  const deadline = Date.now() + timeoutMs;
  const pollInterval = 3000;

  while (Date.now() < deadline) {
    const messages = await store.searchMessages(conversationId, 50);
    const responses = messages.filter((m) => m.direction === "inbound" && new Date(m.createdAt) > new Date(sentAfter));

    if (responses.length > 0) {
      return { found: true, response: responses[responses.length - 1].text, respondedAt: responses[responses.length - 1].createdAt };
    }

    await new Promise((r) => setTimeout(r, pollInterval));
  }

  return { found: false, error: "timeout" };
}

// Tool handlers
async function handleTool(name, args) {
  if (name === "delegate_task") {
    const to = String(args.agent || "agent-codex");
    const task = String(args.task || "").trim();
    if (!task) throw new Error("task is required");

    const timeoutSec = Number(args.timeout_seconds || 120);
    const waitForResponse = args.wait !== false; // default true

    // Build task message
    let fullText = `[TASK FROM JARVIS]\n${task}`;

    // Attach file context if provided
    if (args.file_context) {
      fullText += `\n\n--- FILE CONTEXT ---\n${args.file_context}\n--- END FILE CONTEXT ---`;
    }

    fullText += `\n\n[INSTRUCTIONS]\n- Respond with the complete solution\n- Include code in markdown code blocks\n- Be concise but complete`;

    const conversationId = `task:${randomUUID().substring(0, 8)}`;
    const sent = await sendToPeer(to, fullText, conversationId);

    if (!waitForResponse) {
      return {
        status: "sent",
        agent: to,
        conversationId,
        msgId: sent.msgId,
        message: `Task sent to ${to}. Use check_response with conversationId to get the result later.`,
      };
    }

    // Wait for response
    const result = await pollResponse(conversationId, sent.createdAt, timeoutSec * 1000);

    if (result.found) {
      return { status: "completed", agent: to, conversationId, response: result.response };
    } else {
      return {
        status: "timeout",
        agent: to,
        conversationId,
        message: `No response after ${timeoutSec}s. Agent may be offline. Use check_response later.`,
      };
    }
  }

  if (name === "check_response") {
    const conversationId = String(args.conversation_id || "").trim();
    if (!conversationId) throw new Error("conversation_id is required");

    const messages = await store.searchMessages(conversationId, 50);
    const sorted = messages.sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));

    return {
      conversationId,
      messages: sorted.map((m) => ({
        direction: m.direction,
        sender: m.sender,
        text: m.text.substring(0, 5000),
        createdAt: m.createdAt,
      })),
      count: sorted.length,
    };
  }

  if (name === "list_agents") {
    const peers = Object.entries(config.peers).map(([id, p]) => ({
      agentId: id,
      hasKeys: !!(p.encryption?.publicKey && p.signing?.publicKey),
    }));

    // Check recent messages from each peer
    for (const peer of peers) {
      const msgs = await store.searchMessages(peer.agentId, 5);
      const inbound = msgs.filter((m) => m.direction === "inbound");
      peer.lastSeen = inbound.length > 0 ? inbound[0].createdAt : null;
    }

    return { myAgentId: config.agentId, peers };
  }

  throw new Error(`unknown tool: ${name}`);
}

// Tool definitions
const tools = [
  {
    name: "delegate_task",
    description:
      "Send a coding task to a CODEX agent via Murmur V2 (encrypted NATS). The agent processes the task using its own context and model (GPT-5.3). Returns the response without consuming your context window for the actual work. Default agent: agent-codex (CODEX-1, @ma_jarvis_codex_bot on GCP). Alternative: codex2-agent-hq (CODEX-2 on Agent-HQ).",
    inputSchema: {
      type: "object",
      properties: {
        agent: {
          type: "string",
          description: "Target agent ID. Options: agent-codex (CODEX-1/GCP), codex2-agent-hq (CODEX-2/Agent-HQ), glm-agent-hq, haiku-agent-hq",
          default: "agent-codex",
        },
        task: {
          type: "string",
          description: "Task description. Be specific: what to build, requirements, constraints.",
        },
        file_context: {
          type: "string",
          description: "Optional file contents to include as context for the task. Paste the relevant code here.",
        },
        timeout_seconds: {
          type: "number",
          description: "How long to wait for response (default: 120). Set higher for complex tasks.",
          default: 120,
        },
        wait: {
          type: "boolean",
          description: "Wait for response (true) or fire-and-forget (false). Default: true.",
          default: true,
        },
      },
      required: ["task"],
    },
  },
  {
    name: "check_response",
    description: "Check if a previously delegated task got a response. Use after delegate_task with wait=false.",
    inputSchema: {
      type: "object",
      properties: {
        conversation_id: {
          type: "string",
          description: "Conversation ID from delegate_task result.",
        },
      },
      required: ["conversation_id"],
    },
  },
  {
    name: "list_agents",
    description: "List available agents and their last seen timestamps.",
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
];

// JSON-RPC stdio server
const send = (payload) => process.stdout.write(`${JSON.stringify(payload)}\n`);
const ok = (id, result) => { if (id !== undefined) send({ jsonrpc: "2.0", id, result }); };
const fail = (id, message) => { if (id !== undefined) send({ jsonrpc: "2.0", id, error: { code: -32000, message } }); };

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });

rl.on("line", async (line) => {
  if (!line.trim()) return;
  let req;
  try { req = JSON.parse(line); } catch { return; }

  try {
    if (req.method === "initialize") {
      ok(req.id, {
        protocolVersion: "2024-11-05",
        serverInfo: { name: "delegate-codex", version: "1.0.0" },
        capabilities: { tools: {} },
      });
      return;
    }
    if (req.method === "tools/list") { ok(req.id, { tools }); return; }
    if (req.method === "tools/call") {
      const result = await handleTool(String(req.params?.name), (req.params?.arguments) || {});
      ok(req.id, { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] });
      return;
    }
    if (req.method === "notifications/initialized") return;
    fail(req.id, `unsupported method: ${req.method}`);
  } catch (err) {
    fail(req.id, err instanceof Error ? err.message : "request failed");
  }
});
