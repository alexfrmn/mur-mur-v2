import { StringCodec, connect, type NatsConnection, type Subscription } from "nats";
import { isEnvelopeV1 } from "@murmurv2/core";

export interface OpenClawBridgeConfig {
  agentId: string;
  natsUrl: string;
  natsToken?: string;
  natsSubject?: string;

  openclawBaseUrl: string;
  openclawApiPath?: string;
  openclawApiToken?: string;
  openclawSessionKey?: string;
  openclawSessionLabel?: string;
  openclawAgentId?: string;

  maxRetries?: number;
  retryBaseMs?: number;
  requestTimeoutMs?: number;
  logger?: (level: "info" | "warn" | "error", message: string, meta?: Record<string, unknown>) => void;
}

type InboundPayload = {
  msgId: string;
  from: string;
  conversationId: string;
  text: string;
  raw: unknown;
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const sc = StringCodec();

const defaultLogger: OpenClawBridgeConfig["logger"] = (level, message, meta) => {
  const entry = { ts: new Date().toISOString(), level, message, ...(meta ?? {}) };
  console.log(JSON.stringify(entry));
};

const parseInbound = (rawJson: string): InboundPayload => {
  const parsed = JSON.parse(rawJson) as unknown;

  if (isEnvelopeV1(parsed)) {
    const text = Buffer.from(parsed.payloadCiphertext, "base64").toString("utf8");
    return {
      msgId: parsed.msgId,
      from: parsed.senderAgentId,
      conversationId: parsed.conversationId,
      text,
      raw: parsed,
    };
  }

  const obj = parsed as Record<string, unknown>;
  return {
    msgId: String(obj.msgId ?? `msg-${Date.now()}`),
    from: String(obj.from ?? obj.senderAgentId ?? "unknown"),
    conversationId: String(obj.conversationId ?? "unknown"),
    text: String(obj.text ?? rawJson),
    raw: parsed,
  };
};

const toOpenClawMessage = (input: InboundPayload): string => {
  return [
    "[MURMUR_INBOUND]",
    `from: ${input.from}`,
    `conversationId: ${input.conversationId}`,
    `msgId: ${input.msgId}`,
    "",
    input.text,
  ].join("\n");
};

export class OpenClawBridge {
  private nc?: NatsConnection;
  private sub?: Subscription;
  private running = false;

  constructor(private readonly config: OpenClawBridgeConfig) {
    if (!config.agentId) throw new Error("agentId is required");
    if (!config.natsUrl) throw new Error("natsUrl is required");
    if (!config.openclawBaseUrl) throw new Error("openclawBaseUrl is required");
  }

  private log(level: "info" | "warn" | "error", message: string, meta?: Record<string, unknown>) {
    (this.config.logger ?? defaultLogger)?.(level, message, meta);
  }

  private subject(): string {
    return this.config.natsSubject || `msg.${this.config.agentId}`;
  }

  private apiUrl(): string {
    const base = this.config.openclawBaseUrl.replace(/\/$/, "");
    const path = this.config.openclawApiPath || "/api/tools/sessions_send";
    return `${base}${path.startsWith("/") ? path : `/${path}`}`;
  }

  private async injectToOpenClaw(payload: InboundPayload): Promise<void> {
    const body: Record<string, unknown> = {
      message: toOpenClawMessage(payload),
      metadata: {
        source: "murmur-v2",
        msgId: payload.msgId,
        from: payload.from,
        conversationId: payload.conversationId,
      },
    };

    if (this.config.openclawSessionKey) body.sessionKey = this.config.openclawSessionKey;
    if (this.config.openclawSessionLabel) body.label = this.config.openclawSessionLabel;
    if (this.config.openclawAgentId) body.agentId = this.config.openclawAgentId;

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.config.requestTimeoutMs ?? 15_000);

    try {
      const headers: Record<string, string> = { "content-type": "application/json" };
      if (this.config.openclawApiToken) {
        headers.authorization = `Bearer ${this.config.openclawApiToken}`;
      }

      const res = await fetch(this.apiUrl(), {
        method: "POST",
        headers,
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`openclaw-http-${res.status}${text ? `: ${text.slice(0, 200)}` : ""}`);
      }
    } finally {
      clearTimeout(timeout);
    }
  }

  private async injectWithRetry(payload: InboundPayload): Promise<void> {
    const maxRetries = this.config.maxRetries ?? 3;
    const retryBaseMs = this.config.retryBaseMs ?? 500;

    let lastErr: unknown;
    for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
      try {
        await this.injectToOpenClaw(payload);
        return;
      } catch (err) {
        lastErr = err;
        if (attempt >= maxRetries) break;
        const backoff = retryBaseMs * Math.pow(2, attempt - 1);
        this.log("warn", "OpenClaw bridge retrying", {
          msgId: payload.msgId,
          attempt,
          maxRetries,
          backoffMs: backoff,
          error: err instanceof Error ? err.message : String(err),
        });
        await sleep(backoff);
      }
    }

    throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.nc = await connect({ servers: this.config.natsUrl, token: this.config.natsToken });
    this.sub = this.nc.subscribe(this.subject());
    this.running = true;

    this.log("info", "bridge-openclaw started", {
      subject: this.subject(),
      openclawUrl: this.apiUrl(),
    });

    (async () => {
      for await (const msg of this.sub!) {
        const raw = sc.decode(msg.data);
        try {
          const parsed = parseInbound(raw);
          await this.injectWithRetry(parsed);
          this.log("info", "message bridged", {
            subject: this.subject(),
            msgId: parsed.msgId,
            from: parsed.from,
          });
        } catch (err) {
          this.log("error", "bridge dispatch failed", {
            subject: this.subject(),
            error: err instanceof Error ? err.message : String(err),
            raw: raw.slice(0, 500),
          });
        }
      }
    })().catch((err) => {
      this.log("error", "subscription loop crashed", {
        error: err instanceof Error ? err.message : String(err),
      });
    });
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.sub) this.sub.unsubscribe();
    this.sub = undefined;
    if (this.nc) {
      await this.nc.drain();
      this.nc = undefined;
    }
    this.log("info", "bridge-openclaw stopped");
  }
}
