import net from "node:net";

const DEFAULT_TIMEOUT_MS = 10000;

export const buildCodexTurnText = (payload) => {
  const lines = [
    "[MURMUR WAKE]",
    `from=${payload.from || "unknown"}`,
    `conversationId=${payload.conversationId || ""}`,
    `msgId=${payload.msgId || ""}`,
    "",
    payload.text || "",
  ];
  return lines.join("\n");
};

export const buildTurnStartRequest = ({ id = 1, threadId, text, metadata = {} }) => ({
  jsonrpc: "2.0",
  id,
  method: "turn/start",
  params: {
    threadId,
    input: [{ type: "text", text, text_elements: [] }],
    responsesapiClientMetadata: metadata,
  },
});

export class CodexAppServerClient {
  constructor({ socketPath, timeoutMs = DEFAULT_TIMEOUT_MS, connect = net.createConnection } = {}) {
    if (!socketPath) throw new Error("codex-app-server-socket-missing");
    this.socketPath = socketPath;
    this.timeoutMs = timeoutMs;
    this.connect = connect;
    this.nextId = 1;
  }

  request(method, params) {
    const id = this.nextId++;
    const request = { jsonrpc: "2.0", id, method, params };
    return this.send(request, id);
  }

  send(request, expectedId = request.id) {
    return new Promise((resolve, reject) => {
      let buffer = "";
      const socket = this.connect({ path: this.socketPath });
      const timer = setTimeout(() => {
        socket.destroy();
        reject(new Error(`codex-app-server-timeout:${this.socketPath}`));
      }, this.timeoutMs);

      const cleanup = () => clearTimeout(timer);
      socket.setEncoding("utf8");
      socket.on("connect", () => {
        socket.write(`${JSON.stringify(request)}\n`);
      });
      socket.on("data", (chunk) => {
        buffer += chunk;
        const lines = buffer.split(/\r?\n/);
        buffer = lines.pop() ?? "";
        for (const line of lines) {
          if (!line.trim()) continue;
          let message;
          try {
            message = JSON.parse(line);
          } catch {
            continue;
          }
          if (message.id !== expectedId) continue;
          cleanup();
          socket.end();
          if (message.error) {
            reject(new Error(`codex-app-server-error:${message.error.message || JSON.stringify(message.error)}`));
          } else {
            resolve(message.result);
          }
        }
      });
      socket.on("error", (err) => {
        cleanup();
        reject(new Error(`codex-app-server-connect-failed:${this.socketPath}:${err.message}`));
      });
      socket.on("close", () => {
        cleanup();
        if (!buffer.trim()) return;
        try {
          const message = JSON.parse(buffer);
          if (message.id === expectedId) {
            if (message.error) reject(new Error(`codex-app-server-error:${message.error.message || JSON.stringify(message.error)}`));
            else resolve(message.result);
          }
        } catch {
          reject(new Error(`codex-app-server-closed-with-partial-response:${this.socketPath}`));
        }
      });
    });
  }
}

export const createCodexAppServerInjector = ({ Client = CodexAppServerClient, log = () => {}, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) => {
  return async (payload, peer) => {
    const socketPath = peer?.socketPath || peer?.target;
    const threadId = peer?.threadId;
    if (!socketPath) throw new Error(`codex-app-server-socket-missing:${payload.from}`);
    if (!threadId) throw new Error(`codex-app-server-thread-missing:${payload.from}`);

    const client = new Client({ socketPath, timeoutMs });
    const text = buildCodexTurnText(payload);
    const result = await client.request("turn/start", {
      threadId,
      input: [{ type: "text", text, text_elements: [] }],
      responsesapiClientMetadata: {
        murmur_msg_id: payload.msgId || "",
        murmur_conversation_id: payload.conversationId || "",
        murmur_from: payload.from || "",
      },
    });
    log("info", "Codex app-server wake completed", { msgId: payload.msgId, threadId, socketPath });
    return result;
  };
};
