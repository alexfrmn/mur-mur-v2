import { execFile } from "node:child_process";

const ensureObject = (value) => (value && typeof value === "object" ? value : {});
const validMode = (mode) => mode === "persistent" || mode === "stateless" || mode === "codex_app_server";

export const normalizeWakeConfig = (config = {}) => {
  const wake = ensureObject(config.wake);
  const dedup = ensureObject(wake.dedup);
  const loopBreaker = ensureObject(wake.loopBreaker);
  const peers = Object.fromEntries(
    Object.entries(ensureObject(wake.peers)).map(([agentId, peer]) => {
      const value = ensureObject(peer);
      return [agentId, {
        mode: validMode(value.mode) ? value.mode : undefined,
        target: typeof value.target === "string" && value.target.trim() ? value.target.trim() : undefined,
        socketPath: typeof value.socketPath === "string" && value.socketPath.trim() ? value.socketPath.trim() : undefined,
        threadId: typeof value.threadId === "string" && value.threadId.trim() ? value.threadId.trim() : undefined,
      }];
    }),
  );
  return {
    enabled: wake.enabled !== false,
    mode: validMode(wake.mode) ? wake.mode : "stateless",
    peers,
    auditHook: typeof wake.auditHook === "string" && wake.auditHook.trim() ? wake.auditHook.trim() : null,
    dedup: {
      cooldownMs: Number.isFinite(Number(dedup.cooldownMs)) ? Number(dedup.cooldownMs) : 300000,
    },
    loopBreaker: {
      maxWakes: Number.isFinite(Number(loopBreaker.maxWakes)) ? Number(loopBreaker.maxWakes) : 5,
      windowMs: Number.isFinite(Number(loopBreaker.windowMs)) ? Number(loopBreaker.windowMs) : 60000,
    },
  };
};

export const createTmuxInjector = ({ exec = execFile, log = () => {} } = {}) => {
  const run = (args) => new Promise((resolve, reject) => {
    exec("tmux", args, (err) => err ? reject(err) : resolve());
  });
  return async (payload, peer) => {
    if (!peer?.target) throw new Error(`wake-persistent-target-missing:${payload.from}`);
    await run(["send-keys", "-t", peer.target, "-l", payload.text]);
    await run(["send-keys", "-t", peer.target, "Enter"]);
    log("info", "WakeMonitor persistent injected", { msgId: payload.msgId, target: peer.target });
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

export const createAuditShellHook = ({ command, timeoutMs = 10000, baseEnv = process.env, log = () => {} }) => {
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
    execFile("sh", ["-c", command], { env, timeout: timeoutMs }, (err, stdout) => {
      if (err) {
        log("warn", "wake audit hook failed", { error: err.message, msgId: payload.msgId });
        resolve("deny");
        return;
      }
      const verdict = String(stdout || "").split(/\r?\n/, 1)[0]?.trim();
      resolve(verdict === "deny" || verdict === "require_approval" || verdict === "allow" ? verdict : "deny");
    });
  });
};

export class WakeMonitor {
  constructor(options = {}) {
    const wakeConfig = normalizeWakeConfig({ wake: options });
    this.enabled = options.enabled ?? wakeConfig.enabled;
    this.mode = options.mode ?? wakeConfig.mode;
    this.peers = options.peers ?? wakeConfig.peers;
    this.cooldownMs = options.dedup?.cooldownMs ?? wakeConfig.dedup.cooldownMs;
    this.loopBreaker = {
      maxWakes: options.loopBreaker?.maxWakes ?? wakeConfig.loopBreaker.maxWakes,
      windowMs: options.loopBreaker?.windowMs ?? wakeConfig.loopBreaker.windowMs,
    };
    this.hook = options.hook || null;
    this.injector = options.injector || null;
    this.auditHook = options.auditHook || null;
    this.notify = options.notify || null;
    this.loadBacklogAfter = options.loadBacklogAfter || null;
    this.now = options.now || (() => Date.now());
    this.log = options.log || (() => {});
    this.seen = new Map();
    this.senderWindows = new Map();
    this.suspendedSenders = new Map();
    this.queue = [];
    this.queuedKeys = new Set();
    this.processing = false;
    this.cursor = Number.isFinite(Number(options.initialCursor)) ? Number(options.initialCursor) : 0;
  }

  async onInbound(payload) {
    if (!this.enabled) return;
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
    this.pruneSeen(now);
    const lastWakeAt = this.seen.get(key);
    if (lastWakeAt !== undefined && now - lastWakeAt < this.cooldownMs) {
      this.advanceCursor(payload);
      this.log("info", "WakeMonitor duplicate dropped", { msgId: payload.msgId, conversationId: payload.conversationId });
      return;
    }

    this.seen.set(key, now);
    if (await this.isLoopBreakerBlocked(payload, now)) {
      this.advanceCursor(payload);
      return;
    }

    const verdict = await this.audit(payload);
    if (verdict === "deny") {
      this.log("warn", "WakeMonitor audit denied wake", { msgId: payload.msgId, conversationId: payload.conversationId, from: payload.from });
      this.advanceCursor(payload);
      return;
    }
    if (verdict === "require_approval") {
      await this.notify?.(payload, "require_approval");
      this.log("warn", "WakeMonitor audit requires approval", { msgId: payload.msgId, conversationId: payload.conversationId, from: payload.from });
      this.advanceCursor(payload);
      return;
    }

    try {
      const peer = this.peerFor(payload);
      if (peer.mode === "persistent" || peer.mode === "codex_app_server") {
        if (!this.injector) throw new Error(`wake-persistent-injector-missing:${payload.from}`);
        await this.injector(payload, peer);
        this.log("info", "WakeMonitor persistent wake completed", { msgId: payload.msgId, conversationId: payload.conversationId, target: peer.target });
      } else {
        if (this.hook) await this.hook(payload);
        this.log("info", "WakeMonitor hook completed", { msgId: payload.msgId, conversationId: payload.conversationId });
      }
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      this.log("warn", "WakeMonitor hook error", { error: e.message, msgId: payload.msgId });
    } finally {
      this.advanceCursor(payload);
    }
  }

  pruneSeen(now = this.now()) {
    for (const [key, wokeAt] of this.seen.entries()) {
      if (now - wokeAt >= this.cooldownMs) this.seen.delete(key);
    }
  }

  async isLoopBreakerBlocked(payload, now) {
    const sender = payload.from || "unknown";
    const suspendedUntil = this.suspendedSenders.get(sender);
    if (suspendedUntil !== undefined) {
      if (now < suspendedUntil) {
        this.suspendedSenders.set(sender, now + this.loopBreaker.windowMs);
        await this.notify?.(payload, "loop-breaker");
        this.log("warn", "WakeMonitor loop-breaker suspended wake", { sender, msgId: payload.msgId, suspendedUntil: this.suspendedSenders.get(sender) });
        return true;
      }
      this.suspendedSenders.delete(sender);
    }

    const since = now - this.loopBreaker.windowMs;
    const window = (this.senderWindows.get(sender) || []).filter((ts) => ts > since);
    if (window.length >= this.loopBreaker.maxWakes) {
      this.suspendedSenders.set(sender, now + this.loopBreaker.windowMs);
      this.senderWindows.set(sender, window);
      await this.notify?.(payload, "loop-breaker");
      this.log("warn", "WakeMonitor loop-breaker tripped", { sender, count: window.length + 1, msgId: payload.msgId });
      return true;
    }

    window.push(now);
    this.senderWindows.set(sender, window);
    return false;
  }

  async audit(payload) {
    if (!this.auditHook) return "allow";
    const verdict = await this.auditHook(payload);
    return verdict === "allow" || verdict === "require_approval" || verdict === "deny" ? verdict : "deny";
  }

  advanceCursor(payload) {
    const cursor = Number(payload?.cursor);
    if (Number.isFinite(cursor) && cursor > this.cursor) this.cursor = cursor;
  }

  keyFor(payload) {
    return payload.msgId;
  }

  peerFor(payload) {
    const peer = ensureObject(this.peers?.[payload.from]);
    return {
      ...peer,
      mode: validMode(peer.mode) ? peer.mode : this.mode,
    };
  }
}
