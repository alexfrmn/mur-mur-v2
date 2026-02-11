import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";
import { SQLiteMessageStore, type LocalMessageRecord } from "@murmurv2/core";

interface JsonRpcRequest {
  jsonrpc: "2.0";
  id?: string | number;
  method: string;
  params?: Record<string, unknown>;
}

const store = new SQLiteMessageStore(process.env.MURMUR_STORE_PATH ?? ".data/murmur.db");

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

const handleTool = async (name: string, args: Record<string, unknown>): Promise<unknown> => {
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

  throw new Error(`unknown tool: ${name}`);
};

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
];

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
        serverInfo: { name: "murmur-v2-mcp", version: "0.1.0" },
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
