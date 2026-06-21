// @murmurv2/bridge-a2a — A2A <-> Murmur bridge (v2.1 / track #4, E5).
//
// Trust boundary: terminates external A2A (JSON-RPC/HTTP + Agent Card discovery)
// and re-wraps each task as an internal Murmur E2E envelope on NATS. External
// agents speak the industry-standard A2A protocol; the internal mesh keeps its
// E2E encryption + signed envelopes + durable outbox untouched.
// Design: ../../Plans/Active "{murmur} {design} A2A to Murmur Bridge".
//
// Wired against @a2a-js/sdk@1.0.0-alpha.0:
//   - protobuf-style domain types (Part.content.$case='text', Role.ROLE_AGENT);
//   - server: DefaultRequestHandler + InMemoryTaskStore + an AgentExecutor;
//   - express: jsonRpcHandler + agentCardHandler + UserBuilder.noAuthentication.
//     (NOTE: the older A2AExpressApp().routes() surface is gone in alpha.)
//
// Crypto matches the mesh canonical form (stableEnvelopePayload, identical to
// scripts/murmur-daemon.mjs + packages/mcp-server) so internal agents verify the
// bridge's signatures. The bridge signs+seals as ITSELF; the external caller's id
// travels inside the encrypted payload, never as a spoofable senderAgentId.
//
// STATUS: skeleton. Crypto / correlation / allowlist / agent-card are wired and
// unit-tested. Live A2A-client interop (real remote agent over HTTP) is the next
// gate — opt-in package, intentionally NOT in the root tsc -b graph / core CI yet
// while @a2a-js/sdk is alpha (mirrors JetStream's default-OFF posture).

import { randomUUID } from "node:crypto";
import type { Server } from "node:http";
import express from "express";
import { StringCodec, connect, type NatsConnection, type Subscription } from "nats";
import { isEnvelopeV1, type AckV1, type EnvelopeV1 } from "@murmurv2/core";
import { decryptPayload, encryptPayload, signEnvelope } from "@murmurv2/security";
import {
  DefaultRequestHandler,
  InMemoryTaskStore,
  type AgentExecutor,
  type ExecutionEventBus,
  type RequestContext,
} from "@a2a-js/sdk/server";
import {
  UserBuilder,
  agentCardHandler,
  jsonRpcHandler,
} from "@a2a-js/sdk/server/express";
import { AgentCard, Message, Role } from "@a2a-js/sdk";

const sc = StringCodec();

export interface BridgeA2AConfig {
  /** NATS bus the internal mesh runs on. */
  natsUrl: string;
  natsToken?: string;
  /** This bridge's own agent id on the mesh (e.g. "a2a-bridge"). */
  agentId: string;
  /** Default internal recipient for inbound A2A tasks (e.g. "agent-jarvis"). */
  defaultTargetAgentId: string;
  /** HTTP port the A2A server listens on. */
  a2aPort: number;
  /** Ed25519 private key the bridge signs internal envelopes with. */
  signingPrivateKey: string;
  /** X25519 private key the bridge seals outbound / opens inbound replies with. */
  encryptionPrivateKey: string;
  /** Internal agentId -> X25519 public key. Used to seal to the target AND to open
   *  a reply (the sender pubkey is not carried in the envelope). */
  recipientPublicKeys: Record<string, string>;
  /** Allowlist of external A2A agent ids permitted to delegate tasks. */
  allowedExternalAgents: string[];
  /** ms to wait for the internal agent's reply before failing the A2A task. */
  replyTimeoutMs?: number;
}

export type Logger = (level: "info" | "warn" | "error", msg: string, meta?: unknown) => void;

/** What an A2A message/send is mapped into before sealing. */
export interface InboundTask {
  externalAgentId: string;
  conversationId: string;
  text: string;
}

/**
 * Mesh-canonical signing input. MUST stay byte-identical to
 * `scripts/murmur-daemon.mjs` + `packages/mcp-server` `stableEnvelopePayload`,
 * or internal agents reject the bridge's signature.
 * TODO(core): promote this single source of truth into @murmurv2/core (it is
 * currently duplicated across daemon / mcp-server / demos / this bridge).
 */
export function stableEnvelopePayload(envelope: EnvelopeV1): string {
  return JSON.stringify({
    schemaVersion: envelope.schemaVersion,
    msgId: envelope.msgId,
    conversationId: envelope.conversationId,
    senderAgentId: envelope.senderAgentId,
    recipients: [...envelope.recipients],
    createdAt: envelope.createdAt,
    payloadCiphertext: envelope.payloadCiphertext,
    payloadNonce: envelope.payloadNonce,
  });
}

type SealConfig = Pick<
  BridgeA2AConfig,
  "agentId" | "defaultTargetAgentId" | "signingPrivateKey" | "encryptionPrivateKey" | "recipientPublicKeys"
>;

/**
 * Pure: inbound A2A task -> signed + encrypted Murmur EnvelopeV1 sealed to the
 * target agent. External-caller provenance lives INSIDE the encrypted payload.
 */
export async function sealTaskEnvelope(task: InboundTask, cfg: SealConfig): Promise<EnvelopeV1> {
  const target = cfg.defaultTargetAgentId;
  const recipientKey = cfg.recipientPublicKeys[target];
  if (!recipientKey) throw new Error(`a2a-bridge: no recipient public key for ${target}`);

  const payload = JSON.stringify({
    intent: "task",
    source: "a2a",
    externalAgentId: task.externalAgentId,
    text: task.text,
  });
  const encrypted = await encryptPayload(payload, recipientKey, cfg.encryptionPrivateKey);

  const unsigned: Omit<EnvelopeV1, "signature"> = {
    schemaVersion: "1.0",
    msgId: randomUUID(),
    conversationId: task.conversationId,
    senderAgentId: cfg.agentId,
    recipients: [target],
    createdAt: new Date().toISOString(),
    payloadCiphertext: encrypted.ciphertext,
    payloadNonce: encrypted.nonce,
  };
  const signature = await signEnvelope(
    stableEnvelopePayload({ ...unsigned, signature: "" }),
    cfg.signingPrivateKey,
  );
  return { ...unsigned, signature };
}

type OpenConfig = Pick<BridgeA2AConfig, "encryptionPrivateKey" | "recipientPublicKeys">;

/**
 * Pure: open an internal agent's reply envelope -> decrypted plaintext reply.
 * The sender public key is not carried in the envelope (mesh convention); it is
 * resolved out-of-band from `recipientPublicKeys` by senderAgentId — the same key
 * directory federation #14 will formalize.
 */
export async function openReplyEnvelope(envelope: EnvelopeV1, cfg: OpenConfig): Promise<string> {
  const senderPublicKey = cfg.recipientPublicKeys[envelope.senderAgentId];
  if (!senderPublicKey) {
    throw new Error(`a2a-bridge: no public key to open reply from ${envelope.senderAgentId}`);
  }
  return decryptPayload(
    {
      ciphertext: envelope.payloadCiphertext,
      nonce: envelope.payloadNonce,
      senderPublicKey,
    },
    cfg.encryptionPrivateKey,
  );
}

/** Flatten an A2A Message's text parts into a single string. */
export function extractText(message: Message): string {
  const parts = message?.parts ?? [];
  const out: string[] = [];
  for (const part of parts) {
    const content = part.content;
    if (content && content.$case === "text") out.push(content.value);
  }
  return out.join("\n");
}

export class A2AMurmurBridge {
  private nc?: NatsConnection;
  private ackSub?: Subscription;
  private server?: Server;
  private running = false;
  /** original msgId -> resolver, correlates the internal reply back to the A2A task. */
  private readonly pending = new Map<string, (reply: string) => void>();

  constructor(
    private readonly config: BridgeA2AConfig,
    private readonly log: Logger = () => {},
  ) {
    if (!config.natsUrl) throw new Error("a2a-bridge: natsUrl is required");
    if (!config.agentId) throw new Error("a2a-bridge: agentId is required");
    if (!config.signingPrivateKey) throw new Error("a2a-bridge: signingPrivateKey is required");
    if (!config.encryptionPrivateKey) throw new Error("a2a-bridge: encryptionPrivateKey is required");
  }

  private ackSubject(): string {
    return `ack.${this.config.agentId}`;
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.nc = await connect({ servers: this.config.natsUrl, token: this.config.natsToken });

    // Internal replies/ACKs to this bridge land on ack.<agentId>; correlate by msgId.
    this.ackSub = this.nc.subscribe(this.ackSubject());
    void (async () => {
      for await (const m of this.ackSub!) {
        try {
          await this.handleInternalReply(sc.decode(m.data));
        } catch (err) {
          this.log("error", "ack handling failed", { error: errMsg(err) });
        }
      }
    })().catch((err) => this.log("error", "ack loop crashed", { error: errMsg(err) }));

    // A2A server (alpha API): DefaultRequestHandler drives an AgentExecutor that
    // bridges each inbound task into the mesh; express middleware exposes JSON-RPC
    // + the Agent Card for discovery.
    const requestHandler = new DefaultRequestHandler(
      this.agentCard(),
      new InMemoryTaskStore(),
      new MurmurAgentExecutor(this),
    );
    const app = express();
    app.use(express.json());
    app.use("/.well-known/agent-card.json", agentCardHandler({ agentCardProvider: requestHandler }));
    app.use(jsonRpcHandler({ requestHandler, userBuilder: UserBuilder.noAuthentication }));
    this.server = app.listen(this.config.a2aPort);

    this.running = true;
    this.log("info", "bridge-a2a started", {
      ackSubject: this.ackSubject(),
      a2aPort: this.config.a2aPort,
    });
  }

  /** A2A Agent Card served at /.well-known/agent-card.json (discovery). */
  agentCard(): AgentCard {
    // fromJSON fills the protobuf required defaults (supportedInterfaces, security*,
    // signatures, provider) so we only declare the fields we actually populate.
    return AgentCard.fromJSON({
      name: `murmur-bridge:${this.config.defaultTargetAgentId}`,
      description: "A2A bridge into a private Murmur (E2E/NATS) agent mesh.",
      version: "0.1.0",
      capabilities: { streaming: false }, // TODO(phase2): SSE streaming
      defaultInputModes: ["text/plain"],
      defaultOutputModes: ["text/plain"],
      skills: [
        {
          id: "delegate",
          name: "Delegate task to Murmur agent",
          description: "Forwards an A2A task to the internal agent and returns its reply.",
          tags: ["bridge", "murmur"],
        },
      ],
      // TODO(phase2): securitySchemes (OAuth2/API-key) enforced before dispatch.
    });
  }

  /**
   * Inbound A2A task -> internal Murmur envelope -> publish on msg.<target>.
   * Resolves with the internal agent's decrypted reply (mapped to an A2A reply
   * Message by the executor upstream).
   */
  async dispatchInboundTask(task: InboundTask): Promise<string> {
    if (!this.config.allowedExternalAgents.includes(task.externalAgentId)) {
      throw new Error(`a2a-bridge: external agent not allowlisted: ${task.externalAgentId}`);
    }
    if (!this.nc) throw new Error("a2a-bridge: not started");

    const target = this.config.defaultTargetAgentId;
    const envelope = await sealTaskEnvelope(task, this.config);

    const reply = new Promise<string>((resolve, reject) => {
      this.pending.set(envelope.msgId, resolve);
      const timer = setTimeout(() => {
        this.pending.delete(envelope.msgId);
        reject(new Error(`a2a-bridge: internal reply timeout for ${envelope.msgId}`));
      }, this.config.replyTimeoutMs ?? 120_000);
      // unref so a pending task never blocks shutdown
      (timer as unknown as { unref?: () => void }).unref?.();
    });

    this.nc.publish(`msg.${target}`, sc.encode(JSON.stringify(envelope)));
    this.log("info", "a2a task -> murmur", {
      msgId: envelope.msgId,
      target,
      from: task.externalAgentId,
    });
    return reply;
  }

  /** Internal reply (fresh EnvelopeV1 with parentMsgId) or AckV1 -> resolve A2A task. */
  private async handleInternalReply(raw: string): Promise<void> {
    const parsed: unknown = JSON.parse(raw);

    if (isEnvelopeV1(parsed)) {
      // The agent's answer references the original task via parentMsgId.
      const key = parsed.parentMsgId ?? parsed.conversationId;
      const resolve = key ? this.pending.get(key) : undefined;
      if (resolve && key) {
        this.pending.delete(key);
        try {
          resolve(await openReplyEnvelope(parsed, this.config));
        } catch (err) {
          resolve(`[a2a-bridge decrypt error] ${errMsg(err)}`);
        }
      }
      return;
    }

    const ack = parsed as AckV1;
    if (ack?.msgId && ack.status === "nack") {
      const resolve = this.pending.get(ack.msgId);
      if (resolve) {
        this.pending.delete(ack.msgId);
        resolve(`[murmur nack] ${ack.reason ?? "rejected"}`);
      }
    }
    // A positive AckV1 only confirms delivery; the real answer arrives as an envelope.
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.ackSub) {
      this.ackSub.unsubscribe();
      this.ackSub = undefined;
    }
    if (this.server) {
      await new Promise<void>((resolve) => this.server!.close(() => resolve()));
      this.server = undefined;
    }
    if (this.nc) {
      await this.nc.drain();
      this.nc = undefined;
    }
  }
}

/** Bridges the A2A executor contract onto the Murmur dispatch path. */
class MurmurAgentExecutor implements AgentExecutor {
  constructor(private readonly bridge: A2AMurmurBridge) {}

  execute = async (requestContext: RequestContext, eventBus: ExecutionEventBus): Promise<void> => {
    const text = extractText(requestContext.userMessage);
    const externalAgentId =
      asString(requestContext.userMessage.metadata?.["externalAgentId"]) ?? "a2a-anonymous";

    let replyText: string;
    try {
      replyText = await this.bridge.dispatchInboundTask({
        externalAgentId,
        conversationId: requestContext.contextId,
        text,
      });
    } catch (err) {
      replyText = `[a2a-bridge error] ${errMsg(err)}`;
    }

    eventBus.publish({ kind: "message", data: buildAgentMessage(replyText, requestContext) });
    eventBus.finished();
  };

  cancelTask = async (_taskId: string, eventBus: ExecutionEventBus): Promise<void> => {
    eventBus.finished();
  };
}

function buildAgentMessage(text: string, ctx: RequestContext): Message {
  return {
    messageId: randomUUID(),
    contextId: ctx.contextId,
    taskId: ctx.taskId,
    role: Role.ROLE_AGENT,
    parts: [
      {
        content: { $case: "text", value: text },
        metadata: undefined,
        filename: "",
        mediaType: "text/plain",
      },
    ],
    metadata: undefined,
    extensions: [],
    referenceTaskIds: [],
  };
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
