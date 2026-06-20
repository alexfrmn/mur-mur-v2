import { execFile } from "node:child_process";

const ensureObject = (value) => (value && typeof value === "object" ? value : {});

export const normalizeWakeConfig = (config = {}) => {
  const wake = ensureObject(config.wake);
  const dedup = ensureObject(wake.dedup);
  return {
    enabled: wake.enabled !== false,
    mode: wake.mode || "stateless",
    dedup: {
      cooldownMs: Number.isFinite(Number(dedup.cooldownMs)) ? Number(dedup.cooldownMs) : 300000,
    },
  };
};

export const createShellHook = ({ command, timeoutMs = 10000, baseEnv = process.env, log = () => {} }) => {
  if (!command) return null;
  return (payload) => new Promise((resolve) => {
    const env = {
      ...baseEnv,
      MURMUR_FROM: payload.from,
      MURMUR_TEXT: payload.text,
      MURMUR_MSG_ID: payload.msgId,
      MURMUR_CONVERSATION_ID: payload.conversationId,
      ...(payload.env || {}),
    };
    execFile("sh", ["-c", command], { env, timeout: timeoutMs }, (err) => {
      if (err) log("warn", "wake hook failed", { error: err.message, msgId: payload.msgId });
      resolve();
    });
  });
};

export class WakeMonitor {
  constructor(options = {}) {
    const wakeConfig = normalizeWakeConfig({ wake: options });
    this.enabled = options.enabled ?? wakeConfig.enabled;
    this.mode = options.mode ?? wakeConfig.mode;
    this.cooldownMs = options.dedup?.cooldownMs ?? wakeConfig.dedup.cooldownMs;
    this.hook = options.hook || null;
    this.loadBacklogAfter = options.loadBacklogAfter || null;
    this.now = options.now || (() => Date.now());
    this.log = options.log || (() => {});
    this.seen = new Map();
    this.queue = [];
    this.queuedKeys = new Set();
    this.processing = false;
    this.cursor = Number.isFinite(Number(options.initialCursor)) ? Number(options.initialCursor) : 0;
  }

  async onInbound(payload) {
    if (!this.enabled || this.mode !== "stateless") return;
    this.enqueue(payload);
    await this.drain();
  }

  enqueue(payload) {
    if (!payload?.msgId) return;
    const key = this.keyFor(payload);
    if (this.queuedKeys.has(key)) return;
    this.queuedKeys.add(key);
    this.queue.push(payload);
  }

  async drain() {
    if (this.processing) return;
    this.processing = true;
    try {
      while (true) {
        while (this.queue.length > 0) {
          const payload = this.queue.shift();
          this.queuedKeys.delete(this.keyFor(payload));
          await this.processPayload(payload);
        }

        if (!this.loadBacklogAfter) break;
        const backlog = await this.loadBacklogAfter(this.cursor);
        if (!Array.isArray(backlog) || backlog.length === 0) break;
        for (const payload of backlog) this.enqueue(payload);
      }
    } finally {
      this.processing = false;
    }
  }

  async processPayload(payload) {
    const key = this.keyFor(payload);
    const now = this.now();
    const lastWakeAt = this.seen.get(key);
    if (lastWakeAt !== undefined && now - lastWakeAt < this.cooldownMs) {
      this.advanceCursor(payload);
      this.log("info", "WakeMonitor duplicate dropped", { msgId: payload.msgId, conversationId: payload.conversationId });
      return;
    }

    this.seen.set(key, now);
    try {
      if (this.hook) await this.hook(payload);
      this.log("info", "WakeMonitor hook completed", { msgId: payload.msgId, conversationId: payload.conversationId });
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      this.log("warn", "WakeMonitor hook error", { error: e.message, msgId: payload.msgId });
    } finally {
      this.advanceCursor(payload);
    }
  }

  advanceCursor(payload) {
    const cursor = Number(payload?.cursor);
    if (Number.isFinite(cursor) && cursor > this.cursor) this.cursor = cursor;
  }

  keyFor(payload) {
    return payload.msgId;
  }
}
