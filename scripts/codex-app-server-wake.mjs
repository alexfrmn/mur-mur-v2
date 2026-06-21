import WebSocket from "ws";

const DEFAULT_TIMEOUT_MS = 10000;
const INITIALIZE_TIMEOUT_MS = 10000;

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
  id,
  method: "turn/start",
  params: {
    threadId,
    input: [{ type: "text", text, text_elements: [] }],
    responsesapiClientMetadata: metadata,
  },
});

const buildThreadStartParams = () => ({
  model: null,
  modelProvider: null,
  cwd: null,
  runtimeWorkspaceRoots: null,
  approvalPolicy: null,
  approvalsReviewer: null,
  sandbox: null,
  permissions: null,
  config: null,
  serviceName: null,
  baseInstructions: null,
  developerInstructions: null,
  personality: null,
  ephemeral: false,
  sessionStartSource: null,
  threadSource: null,
  environments: null,
  dynamicTools: null,
  selectedCapabilityRoots: null,
  mockExperimentalField: null,
});

const buildInitializeRequest = (id) => ({
  id,
  method: "initialize",
  params: {
    clientInfo: {
      name: "murmur-codex-app-server-wake",
      title: "Murmur Codex App-Server Wake",
      version: "0.1.0",
    },
    capabilities: {
      experimentalApi: true,
      requestAttestation: false,
      optOutNotificationMethods: [
        "command/exec/outputDelta",
        "item/agentMessage/delta",
        "item/plan/delta",
        "item/fileChange/outputDelta",
        "item/reasoning/summaryTextDelta",
        "item/reasoning/textDelta",
      ],
    },
  },
});

export class CodexAppServerClient {
  constructor({ socketPath, timeoutMs = DEFAULT_TIMEOUT_MS, WebSocketImpl = WebSocket } = {}) {
    if (!socketPath) throw new Error("codex-app-server-socket-missing");
    this.socketPath = socketPath;
    this.timeoutMs = timeoutMs;
    this.WebSocketImpl = WebSocketImpl;
    this.nextId = 1;
  }

  request(method, params) {
    const id = this.nextId++;
    const request = { id, method, params };
    return this.send(request, id);
  }

  send(request, expectedId = request.id) {
    return new Promise((resolve, reject) => {
      let settled = false;
      let initialized = false;
      const initId = `init-${this.nextId++}`;
      const url = `ws+unix://${this.socketPath}:/`;
      const socket = new this.WebSocketImpl(url, {
        perMessageDeflate: false,
        handshakeTimeout: Math.min(this.timeoutMs, INITIALIZE_TIMEOUT_MS),
      });
      const timer = setTimeout(() => {
        finish(new Error(`codex-app-server-timeout:${this.socketPath}`));
      }, this.timeoutMs);

      const finish = (err, result) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        try {
          socket.close();
        } catch {
          // Ignore close races; the request already has a terminal result.
        }
        if (err) reject(err);
        else resolve(result);
      };

      const sendJson = (message) => socket.send(JSON.stringify(message));

      socket.on("open", () => sendJson(buildInitializeRequest(initId)));
      socket.on("message", (data) => {
        let message;
        try {
          message = JSON.parse(data.toString("utf8"));
        } catch {
          return;
        }

        if (message.id === initId) {
          if (message.error) {
            finish(new Error(`codex-app-server-initialize-error:${message.error.message || JSON.stringify(message.error)}`));
            return;
          }
          initialized = true;
          sendJson({ method: "initialized" });
          sendJson(request);
          return;
        }

        if (message.id !== expectedId) return;
        if (message.error) {
          finish(new Error(`codex-app-server-error:${message.error.message || JSON.stringify(message.error)}`));
        } else {
          finish(null, message.result);
        }
      });
      socket.on("error", (err) => {
        finish(new Error(`codex-app-server-connect-failed:${this.socketPath}:${err.message}`));
      });
      socket.on("close", () => {
        if (!settled) finish(new Error(`codex-app-server-closed-before-response:${this.socketPath}:${initialized ? "after-initialize" : "before-initialize"}`));
      });
    });
  }
}

export const createCodexAppServerInjector = ({ Client = CodexAppServerClient, log = () => {}, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) => {
  return async (payload, peer) => {
    const socketPath = peer?.socketPath || peer?.target;
    if (!socketPath) throw new Error(`codex-app-server-socket-missing:${payload.from}`);

    const client = new Client({ socketPath, timeoutMs });
    const text = buildCodexTurnText(payload);
    const startTurn = (threadId) => client.request("turn/start", {
      threadId,
      input: [{ type: "text", text, text_elements: [] }],
      responsesapiClientMetadata: {
        murmur_msg_id: payload.msgId || "",
        murmur_conversation_id: payload.conversationId || "",
        murmur_from: payload.from || "",
      },
    });

    let threadId = peer?.threadId;
    if (!threadId) {
      const started = await client.request("thread/start", buildThreadStartParams());
      threadId = started?.thread?.id;
      if (!threadId) throw new Error(`codex-app-server-thread-start-missing:${payload.from}`);
      peer.threadId = threadId;
      log("info", "Codex app-server wake thread seeded", { msgId: payload.msgId, threadId, socketPath });
    }

    let result;
    try {
      result = await startTurn(threadId);
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      if (!e.message.startsWith("codex-app-server-error:thread not found:")) throw e;
      const started = await client.request("thread/start", buildThreadStartParams());
      threadId = started?.thread?.id;
      if (!threadId) throw new Error(`codex-app-server-thread-start-missing:${payload.from}`);
      peer.threadId = threadId;
      log("info", "Codex app-server wake thread re-seeded", { msgId: payload.msgId, threadId, socketPath });
      result = await startTurn(threadId);
    }
    log("info", "Codex app-server wake completed", { msgId: payload.msgId, threadId, socketPath });
    return result;
  };
};
