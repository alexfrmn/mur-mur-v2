import { randomUUID } from "node:crypto";
import { SQLiteMessageStore, type EnvelopeV1 } from "@murmurv2/core";

export interface TelegramBridgeConfig {
  botToken: string;
  defaultChatId: string;
  defaultTopicId?: string;
  senderAgentId?: string;
  recipientAgentId?: string;
  apiBase?: string;
  messageStorePath?: string;
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
    };
    this.store = new SQLiteMessageStore(this.cfg.messageStorePath);
  }

  private endpoint(method: string): string {
    if (!this.cfg.botToken) throw new Error("missing MURMUR_TELEGRAM_BOT_TOKEN");
    return `${this.cfg.apiBase}/bot${this.cfg.botToken}/${method}`;
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

      const sender = msg.from?.username ?? `telegram:${msg.from?.id ?? "unknown"}`;
      const conversationId = `telegram:${msg.chat.id}:${msg.message_thread_id ?? "main"}`;
      const envelope: EnvelopeV1 = {
        schemaVersion: "1.0",
        msgId: `telegram-${msg.message_id}`,
        conversationId,
        senderAgentId: sender,
        recipients: [this.cfg.recipientAgentId ?? "human"],
        createdAt: new Date(msg.date * 1000).toISOString(),
        payloadCiphertext: Buffer.from(msg.text, "utf8").toString("base64"),
        payloadNonce: "plain",
        signature: "telegram-bridge",
      };

      await this.store.append({
        conversationId,
        msgId: envelope.msgId,
        direction: "inbound",
        sender,
        text: msg.text,
        createdAt: envelope.createdAt,
        transport: "telegram",
      });

      envelopes.push(envelope);
    }

    return { nextOffset, envelopes };
  }

  toOutboundEnvelope(input: { text: string; conversationId?: string; recipients?: string[] }): EnvelopeV1 {
    return {
      schemaVersion: "1.0",
      msgId: randomUUID(),
      conversationId: input.conversationId ?? `telegram:${this.cfg.defaultChatId}:${this.cfg.defaultTopicId ?? "main"}`,
      senderAgentId: this.cfg.senderAgentId ?? "telegram-bridge",
      recipients: input.recipients ?? [this.cfg.recipientAgentId ?? "human"],
      createdAt: new Date().toISOString(),
      payloadCiphertext: Buffer.from(input.text, "utf8").toString("base64"),
      payloadNonce: "plain",
      signature: "telegram-bridge",
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
