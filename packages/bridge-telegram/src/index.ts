import { randomUUID } from "node:crypto";
import { SQLiteMessageStore, type EnvelopeV1 } from "@murmurv2/core";
import type { CryptoProvider } from "@murmurv2/security";

const UNSIGNED_MARKER = "unsigned";

export interface TelegramBridgeConfig {
  botToken: string;
  defaultChatId: string;
  defaultTopicId?: string;
  senderAgentId?: string;
  recipientAgentId?: string;
  apiBase?: string;
  messageStorePath?: string;
  /**
   * Optional crypto provider used for envelope signing.
   * Consumers should treat envelopes as unsigned when payloadNonce/signature are both "unsigned".
   */
  cryptoProvider?: CryptoProvider;
  signingPrivateKey?: string;
}

interface TelegramUpdate {
  update_id: number;
  message?: {
    message_id: number;
    date: number;
    chat: { id: number | string };
    message_thread_id?: number;
    text?: string;
    from?: { id: number | string; username?: string };
  };
}

const validateTelegramBridgeConfig = (cfg: TelegramBridgeConfig): void => {
  const errs: string[] = [];
  if (!cfg.botToken || !cfg.botToken.trim()) errs.push("MURMUR_TELEGRAM_BOT_TOKEN is required");
  if (!cfg.defaultChatId || !cfg.defaultChatId.trim()) errs.push("MURMUR_TELEGRAM_CHAT_ID is required");
  if (!cfg.apiBase || !/^https?:\/\//.test(cfg.apiBase)) errs.push("apiBase must be an absolute http(s) URL");
  if (!cfg.senderAgentId || !cfg.senderAgentId.trim()) errs.push("senderAgentId must be non-empty");
  if (!cfg.recipientAgentId || !cfg.recipientAgentId.trim()) errs.push("recipientAgentId must be non-empty");
  if (errs.length > 0) throw new Error(`invalid-telegram-bridge-config: ${errs.join("; ")}`);
};

export const isEnvelopeSigned = (env: EnvelopeV1): boolean => {
  return env.payloadNonce !== UNSIGNED_MARKER && env.signature !== UNSIGNED_MARKER;
};

export class TelegramBridge {
  private readonly cfg: TelegramBridgeConfig;
  private readonly store: SQLiteMessageStore;

  constructor(config?: Partial<TelegramBridgeConfig>) {
    this.cfg = {
      botToken: config?.botToken ?? process.env.MURMUR_TELEGRAM_BOT_TOKEN ?? "",
      defaultChatId: config?.defaultChatId ?? process.env.MURMUR_TELEGRAM_CHAT_ID ?? "",
      defaultTopicId: config?.defaultTopicId ?? process.env.MURMUR_TELEGRAM_TOPIC_ID,
      senderAgentId: config?.senderAgentId ?? process.env.MURMUR_TELEGRAM_SENDER_AGENT_ID ?? "telegram-bridge",
      recipientAgentId: config?.recipientAgentId ?? process.env.MURMUR_TELEGRAM_RECIPIENT_AGENT_ID ?? "human",
      apiBase: config?.apiBase ?? "https://api.telegram.org",
      messageStorePath: config?.messageStorePath ?? process.env.MURMUR_STORE_PATH ?? ".data/murmur.db",
      cryptoProvider: config?.cryptoProvider,
      signingPrivateKey: config?.signingPrivateKey ?? process.env.MURMUR_TELEGRAM_SIGNING_PRIVATE_KEY,
    };
    validateTelegramBridgeConfig(this.cfg);
    this.store = new SQLiteMessageStore(this.cfg.messageStorePath);
  }

  private endpoint(method: string): string {
    if (!this.cfg.botToken) throw new Error("missing MURMUR_TELEGRAM_BOT_TOKEN");
    return `${this.cfg.apiBase}/bot${this.cfg.botToken}/${method}`;
  }

  private async signEnvelopePayload(payloadCiphertext: string): Promise<{ payloadNonce: string; signature: string }> {
    if (this.cfg.cryptoProvider && this.cfg.signingPrivateKey) {
      const signature = await this.cfg.cryptoProvider.sign(payloadCiphertext, this.cfg.signingPrivateKey);
      return { payloadNonce: "signed", signature };
    }
    return { payloadNonce: UNSIGNED_MARKER, signature: UNSIGNED_MARKER };
  }

  private async toInboundEnvelope(msg: TelegramUpdate["message"]): Promise<EnvelopeV1> {
    const text = msg?.text ?? "";
    const payloadCiphertext = Buffer.from(text, "utf8").toString("base64");
    const sender = msg?.from?.username ?? `telegram:${msg?.from?.id ?? "unknown"}`;
    const conversationId = `telegram:${msg?.chat.id}:${msg?.message_thread_id ?? "main"}`;
    const signatureFields = await this.signEnvelopePayload(payloadCiphertext);

    return {
      schemaVersion: "1.0",
      msgId: `telegram-${msg?.message_id}`,
      conversationId,
      senderAgentId: sender,
      recipients: [this.cfg.recipientAgentId ?? "human"],
      createdAt: new Date((msg?.date ?? 0) * 1000).toISOString(),
      payloadCiphertext,
      payloadNonce: signatureFields.payloadNonce,
      signature: signatureFields.signature,
    };
  }

  private async callTelegram<T>(method: string, payload: Record<string, unknown>): Promise<T> {
    const res = await fetch(this.endpoint(method), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`telegram-api-${method}-failed: ${res.status} ${body}`);
    }

    const parsed = (await res.json()) as { ok: boolean; result: T; description?: string };
    if (!parsed.ok) throw new Error(parsed.description ?? `telegram-api-${method}-not-ok`);
    return parsed.result;
  }

  async outbound(envelope: EnvelopeV1): Promise<{ chatId: string; messageId: number }> {
    if (!this.cfg.defaultChatId) throw new Error("missing MURMUR_TELEGRAM_CHAT_ID");

    const text = decodeEnvelopeText(envelope);
    const payload: Record<string, unknown> = {
      chat_id: this.cfg.defaultChatId,
      text,
      disable_web_page_preview: true,
    };
    if (this.cfg.defaultTopicId) payload.message_thread_id = Number(this.cfg.defaultTopicId);

    const sent = await this.callTelegram<{ message_id: number; chat: { id: number | string } }>("sendMessage", payload);

    await this.store.append({
      conversationId: envelope.conversationId,
      msgId: envelope.msgId,
      direction: "outbound",
      sender: envelope.senderAgentId,
      text,
      createdAt: new Date().toISOString(),
      transport: "telegram",
    });

    return { chatId: String(sent.chat.id), messageId: sent.message_id };
  }

  async inbound(params?: { offset?: number; limit?: number }): Promise<{ nextOffset: number; envelopes: EnvelopeV1[] }> {
    const updates = await this.callTelegram<TelegramUpdate[]>("getUpdates", {
      offset: params?.offset,
      limit: params?.limit ?? 50,
      timeout: 0,
      allowed_updates: ["message"],
    });

    let nextOffset = params?.offset ?? 0;
    const envelopes: EnvelopeV1[] = [];

    for (const update of updates) {
      nextOffset = Math.max(nextOffset, update.update_id + 1);
      const msg = update.message;
      if (!msg?.text) continue;
      if (String(msg.chat.id) !== this.cfg.defaultChatId) continue;
      if (this.cfg.defaultTopicId && String(msg.message_thread_id ?? "") !== this.cfg.defaultTopicId) continue;

      const envelope = await this.toInboundEnvelope(msg);

      await this.store.append({
        conversationId: envelope.conversationId,
        msgId: envelope.msgId,
        direction: "inbound",
        sender: envelope.senderAgentId,
        text: msg.text,
        createdAt: envelope.createdAt,
        transport: "telegram",
      });

      envelopes.push(envelope);
    }

    return { nextOffset, envelopes };
  }

  toOutboundEnvelope(input: { text: string; conversationId?: string; recipients?: string[] }): EnvelopeV1 {
    const payloadCiphertext = Buffer.from(input.text, "utf8").toString("base64");

    return {
      schemaVersion: "1.0",
      msgId: randomUUID(),
      conversationId: input.conversationId ?? `telegram:${this.cfg.defaultChatId}:${this.cfg.defaultTopicId ?? "main"}`,
      senderAgentId: this.cfg.senderAgentId ?? "telegram-bridge",
      recipients: input.recipients ?? [this.cfg.recipientAgentId ?? "human"],
      createdAt: new Date().toISOString(),
      payloadCiphertext,
      payloadNonce: UNSIGNED_MARKER,
      signature: UNSIGNED_MARKER,
    };
  }

  async toOutboundEnvelopeSigned(input: { text: string; conversationId?: string; recipients?: string[] }): Promise<EnvelopeV1> {
    const payloadCiphertext = Buffer.from(input.text, "utf8").toString("base64");
    const signatureFields = await this.signEnvelopePayload(payloadCiphertext);

    return {
      schemaVersion: "1.0",
      msgId: randomUUID(),
      conversationId: input.conversationId ?? `telegram:${this.cfg.defaultChatId}:${this.cfg.defaultTopicId ?? "main"}`,
      senderAgentId: this.cfg.senderAgentId ?? "telegram-bridge",
      recipients: input.recipients ?? [this.cfg.recipientAgentId ?? "human"],
      createdAt: new Date().toISOString(),
      payloadCiphertext,
      payloadNonce: signatureFields.payloadNonce,
      signature: signatureFields.signature,
    };
  }
}

export const decodeEnvelopeText = (envelope: EnvelopeV1): string => {
  try {
    return Buffer.from(envelope.payloadCiphertext, "base64").toString("utf8");
  } catch {
    return envelope.payloadCiphertext;
  }
};
