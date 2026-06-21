// @murmurv2/bridge-a2a — A2A <-> Murmur bridge (v2.1 / track #4, E5).
//
// Trust boundary: terminates external A2A (JSON-RPC/HTTP, OAuth, Agent Card) and
// re-wraps tasks as internal Murmur E2E envelopes on NATS. External agents speak
// the industry-standard A2A protocol; the internal mesh keeps E2E + durable outbox.
// Design: ../../Plans/Active "{murmur} {design} A2A to Murmur Bridge".
//
// STATUS: skeleton. Inbound A2A->Murmur path scaffolded. NOT built/tested yet.
// Next: `npm i @a2a-js/sdk express`, wire the SDK request handler, `tsc`, tests.

import { StringCodec, connect, type NatsConnection, type Subscription } from "nats";
import {
  isEnvelopeV1,
  type AckV1,
  type EnvelopeV1,
} from "@murmurv2/core";
import { encryptPayload, signEnvelope } from "@murmurv2/security";

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
  /** Map internal agentId -> X25519 public key, for sealing the payload. */
  recipientPublicKeys: Record<string, string>;
  /** Allowlist of external A2A agent ids permitted to delegate tasks. */
  allowedExternalAgents: string[];
  /** ms to wait for the internal agent's reply before failing the A2A task. */
  replyTimeoutMs?: number;
}

type Logger = (level: "info" | "warn" | "error", msg: string, meta?: unknown) => void;

/** What an A2A message/send is mapped into before sealing. */
interface InboundTask {
  externalAgentId: string;
  conversationId: string;
  text: string;
}

export class A2AMurmurBridge {
  private nc?: NatsConnection;
  private ackSub?: Subscription;
  private running = false;
  /** msgId -> resolver, correlates internal reply/ACK back to the waiting A2A task. */
  private readonly pending = new Map<string, (reply: string) => void>();

  constructor(
    private readonly config: BridgeA2AConfig,
    private readonly log: Logger = () => {},
  ) {
    if (!config.natsUrl) throw new Error("natsUrl is required");
    if (!config.agentId) throw new Error("agentId is required");
  }

  private ackSubject(): string {
    return `ack.${this.config.agentId}`;
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.nc = await connect({ servers: this.config.natsUrl, token: this.config.natsToken });

    // Internal replies/ACKs to this bridge land on ack.<agentId>; correlate by msgId.
    this.ackSub = this.nc.subscribe(this.ackSubject());
    (async () => {
      for await (const m of this.ackSub!) {
        try {
          this.handleInternalReply(sc.decode(m.data));
        } catch (err) {
          this.log("error", "ack handling failed", { error: errMsg(err) });
        }
      }
    })().catch((err) => this.log("error", "ack loop crashed", { error: errMsg(err) }));

    // TODO(a2a-sdk): start the A2A server here using @a2a-js/sdk + express:
    //   const handler = new DefaultRequestHandler(this.agentCard(), executor);
    //   express().use(new A2AExpressApp(handler).routes()).listen(this.config.a2aPort);
    // The executor's message/send handler calls this.dispatchInboundTask(...).
    // Verify exact SDK surface against a2aproject/a2a-js samples (v1.0.0-alpha.0).

    this.running = true;
    this.log("info", "bridge-a2a started", {
      ackSubject: this.ackSubject(),
      a2aPort: this.config.a2aPort,
    });
  }

  /** A2A Agent Card served at GET /.well-known/agent.json (discovery). */
  agentCard() {
    return {
      name: `murmur-bridge:${this.config.defaultTargetAgentId}`,
      description: "A2A bridge into a private Murmur (E2E/NATS) agent mesh.",
      version: "0.1.0",
      url: `http://0.0.0.0:${this.config.a2aPort}/`,
      capabilities: { streaming: false }, // TODO: SSE in phase 2
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
      // TODO: securitySchemes (OAuth2/API-key) — auth enforced before dispatch.
    };
  }

  /**
   * Inbound A2A task -> internal Murmur envelope -> publish on msg.<target>.
   * Resolves with the internal agent's reply (mapped to an A2A Artifact upstream).
   */
  async dispatchInboundTask(task: InboundTask): Promise<string> {
    if (!this.config.allowedExternalAgents.includes(task.externalAgentId)) {
      throw new Error(`a2a-bridge: external agent not allowlisted: ${task.externalAgentId}`);
    }
    if (!this.nc) throw new Error("bridge not started");

    const target = this.config.defaultTargetAgentId;
    const recipientKey = this.config.recipientPublicKeys[target];
    if (!recipientKey) throw new Error(`no recipient public key for ${target}`);

    const envelope = await this.sealEnvelope(task, target, recipientKey);

    const reply = new Promise<string>((resolve, reject) => {
      this.pending.set(envelope.msgId, resolve);
      const t = setTimeout(() => {
        this.pending.delete(envelope.msgId);
        reject(new Error(`a2a-bridge: internal reply timeout for ${envelope.msgId}`));
      }, this.config.replyTimeoutMs ?? 120_000);
      // unref so a pending task never blocks shutdown
      (t as unknown as { unref?: () => void }).unref?.();
    });

    this.nc.publish(`msg.${target}`, sc.encode(JSON.stringify(envelope)));
    this.log("info", "a2a task -> murmur", {
      msgId: envelope.msgId,
      target,
      from: task.externalAgentId,
    });
    return reply;
  }

  /** Build a signed+encrypted Murmur EnvelopeV1 from an inbound A2A task. */
  private async sealEnvelope(
    task: InboundTask,
    target: string,
    recipientPublicKey: string,
  ): Promise<EnvelopeV1> {
    // Provenance of the external caller travels in the (encrypted) payload, not as
    // a spoofable senderAgentId — the bridge signs as itself.
    const payload = JSON.stringify({
      intent: "task",
      source: "a2a",
      externalAgentId: task.externalAgentId,
      text: task.text,
    });
    const { ciphertext, nonce } = await encryptPayload(payload, recipientPublicKey);
    const msgId = cryptoRandomId();
    const createdAt = new Date().toISOString(); // NOTE: ok at runtime; see workflow Date caveat for offline tooling
    const unsigned: Omit<EnvelopeV1, "signature"> = {
      schemaVersion: "1.0",
      msgId,
      conversationId: task.conversationId,
      senderAgentId: this.config.agentId,
      recipients: [target],
      createdAt,
      payloadCiphertext: ciphertext,
      payloadNonce: nonce,
    };
    const signature = await signEnvelope(JSON.stringify(unsigned), this.config.signingPrivateKey);
    return { ...unsigned, signature };
  }

  /** Internal reply/ACK on ack.<agentId> -> resolve the waiting A2A task. */
  private handleInternalReply(raw: string): void {
    const parsed: unknown = JSON.parse(raw);
    // Replies may arrive as a fresh EnvelopeV1 (the agent's answer) or an AckV1.
    if (isEnvelopeV1(parsed)) {
      const target = parsed.parentMsgId ?? parsed.conversationId;
      const resolve = target ? this.pending.get(target) : undefined;
      if (resolve) {
        this.pending.delete(target!);
        // TODO: decrypt payload via bridge private key, extract reply text.
        resolve(parsed.payloadCiphertext); // placeholder until decrypt wired
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
    // a positive AckV1 just confirms delivery; the real answer comes as an envelope.
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.ackSub) this.ackSub.unsubscribe();
    this.ackSub = undefined;
    if (this.nc) {
      await this.nc.drain();
      this.nc = undefined;
    }
  }
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function cryptoRandomId(): string {
  // crypto.randomUUID is available on Node 18+; bridge runtime, not workflow sandbox.
  return globalThis.crypto.randomUUID();
}
