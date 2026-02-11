# mur-mur-v2

Reliable, secure multi-agent messaging core for OpenClaw-style agent systems.

## What is implemented (prod-first + hardening)

- ACK correlation path in broker (`startAckCorrelation`) to mark outbox rows `acked` by `msgId` and requeue `nack` responses as failed.
- Persistent optimistic-locking store (`SQLiteDedupeOutboxStore`) for dedupe + outbox state.
- JSON file stores now emit a startup warning when no external locking is configured (`MURMUR_JSON_STORE_LOCKING=1`), because they are single-process safe by default.
- Delivery hardening in outbox flush:
  - exponential backoff with jitter
  - ack-timeout requeue support (`requeueStaleSent`)
  - poison-message handling threshold on consumer side
  - broker connect retry/backoff and optional token auth in `BrokerConfig`
- Security policy gate before publish:
  - allowed sender→recipient map
  - max payload size checks
- E2E crypto abstraction (`CryptoProvider`) with baseline NaCl-style provider:
  - X25519 key agreement
  - XChaCha20-Poly1305 payload encryption
  - pluggable provider registration
- MLS scaffold:
  - `MlsProvider` interface
  - `MURMUR_ENABLE_MLS=1` feature flag
  - adapter placeholder that keeps build stable
- Postgres SQL executor abstraction (`PgSqlExecutor`) for environments that need PG-backed implementations.
- Minimal functional Telegram bridge (`@murmurv2/bridge-telegram`) for inbound/outbound with strict env/config validation.
- Local persistent message store (`SQLiteMessageStore`) for conversation history/search.
- Basic MCP server (`@murmurv2/mcp-server`) exposing:
  - `send_message`
  - `list_conversations`
  - `search_messages`

## Repository Layout

```text
/docs                 ADRs + protocol docs
/schema               JSON schemas for envelope/ack/policy
/packages/core        envelope + store primitives (SQLite/JSON/in-memory)
/packages/broker-nats NATS publish/subscribe/outbox/ACK correlation
/packages/bridge-*    adapters (murmur/openclaw/telegram)
/packages/mcp-server  basic MCP stdio server over local SQLite store
/packages/security    signing/encryption/key helpers
/packages/observability metrics/tracing helpers
```

## Install

```bash
cd /opt/codex-openclaw/mur-mur-v2
npm install
npm run build
```

## Typecheck

```bash
npm run typecheck
```

## Telegram bridge (minimal)

Set env vars:

```bash
export MURMUR_TELEGRAM_BOT_TOKEN="<bot-token>"
export MURMUR_TELEGRAM_CHAT_ID="<chat-id>"
# optional, for forum topics
export MURMUR_TELEGRAM_TOPIC_ID="<thread-id>"

# optional
export MURMUR_TELEGRAM_SENDER_AGENT_ID="telegram-bridge"
export MURMUR_TELEGRAM_RECIPIENT_AGENT_ID="human"
export MURMUR_STORE_PATH=".data/murmur.db"
```

Programmatic usage (example):

```ts
import { TelegramBridge } from "@murmurv2/bridge-telegram";

const bridge = new TelegramBridge();
const env = bridge.toOutboundEnvelope({ text: "hello from murmur" });
await bridge.outbound(env);

const inbound = await bridge.inbound();
console.log(inbound.envelopes);
```

## Agent-to-Agent Messaging (Murmur Daemon)

Persistent encrypted messaging between agents over NATS. Each agent runs a daemon that automatically delivers and receives messages. Agents interact via MCP tools (`murmur_send`, `murmur_inbox`, `murmur_peers`).

### Architecture

```
Agent A (Claude CLI)                    Agent B (Claude CLI)
     │ MCP stdio                             ▲ MCP stdio
┌────┴──────┐                          ┌─────┴─────┐
│ MCP Server │                          │ MCP Server │
│ send→outbox│                          │ inbox←store│
└────┬───────┘                          └─────▲──────┘
     │ shared SQLite                          │ shared SQLite
┌────┴──────────┐    NATS JetStream    ┌──────┴─────────┐
│ murmur-daemon  │◄═══════════════════►│ murmur-daemon   │
│ outbox flush   │  encrypted envelopes │ subscribe       │
│ subscribe      │  + ACK correlation   │ outbox flush    │
└────────────────┘                      └─────────────────┘
```

### Prerequisites

- **Node.js 22+** (uses `node:sqlite`)
- **NATS server** with JetStream enabled:
  ```bash
  # Quick start with Docker:
  docker run -d --name nats -p 4222:4222 nats:2.10-alpine -js --auth YOUR_SECRET_TOKEN
  ```

### Connect Two Agents (Invite Flow)

Three commands, zero JSON editing:

**Step 1 — Host generates invite:**
```bash
git clone https://github.com/alexfrmn/mur-mur-v2.git && cd mur-mur-v2
npm install && npm run build
docker run -d --name nats -p 4222:4222 nats:2.10-alpine -js --auth YOUR_SECRET
AGENT_ID=my-agent NATS_URL=nats://my-server:4222 NATS_TOKEN=YOUR_SECRET \
  node scripts/agent-config-init.mjs
node scripts/murmur-invite.mjs
# → prints MURMUR:eyJ... blob — send it to your friend via any messenger
```

**Step 2 — Friend joins with the blob:**
```bash
git clone https://github.com/alexfrmn/mur-mur-v2.git && cd mur-mur-v2
npm install && npm run build
AGENT_ID=friend-agent node scripts/murmur-join.mjs 'MURMUR:eyJ...'
# → auto-creates config, adds host as peer
# → prints MURMUR-REPLY:eyJ... blob — send it back to host
```

**Step 3 — Host adds friend:**
```bash
node scripts/murmur-add-peer.mjs 'MURMUR-REPLY:eyJ...'
```

**Done! Configure notifications + start daemon:**
```bash
# Telegram preset (from env)
export MURMUR_TELEGRAM_BOT_TOKEN="..."
export MURMUR_TELEGRAM_CHAT_ID="..."
node scripts/murmur-notify-init.mjs telegram

# Optional presets
# node scripts/murmur-notify-init.mjs discord   # uses MURMUR_DISCORD_WEBHOOK_URL
# node scripts/murmur-notify-init.mjs whatsapp  # uses MURMUR_WHATSAPP_WEBHOOK_URL (bridge placeholder)

node scripts/murmur-daemon.mjs
# Or production (systemd):
sudo cp deploy/murmur-daemon.service /etc/systemd/system/
sudo systemctl enable --now murmur-daemon
sudo systemctl restart murmur-daemon
```

Then test from peer with `murmur_send`; inbound messages are stored durably and queued notifications auto-resume after daemon restarts.

Agents communicate via MCP tools: `murmur_send` / `murmur_inbox` / `murmur_peers`.

### Manual Setup (CI/scripts)

```bash
AGENT_ID=agent-jarvis \
NATS_URL=nats://127.0.0.1:4222 \
NATS_TOKEN=your-token \
  node scripts/agent-config-init.mjs
# Then manually edit .data/agent-config.json peers section
```

### MCP Server

Build first, then run:

```bash
npm run build
npm run mcp:server
```

The server speaks line-delimited JSON-RPC over stdio and supports `initialize`, `tools/list`, and `tools/call`.

**Local-only tools** (always available):
- `send_message` — store local outbound message
- `list_conversations` — list conversations
- `search_messages` — search by text/sender/conversation

**Agent-to-agent tools** (require `.data/agent-config.json`):
- `murmur_send` — encrypt + sign + enqueue message to peer (daemon delivers)
- `murmur_inbox` — read inbound messages from other agents
- `murmur_peers` — list known peers and key status

Use `MURMUR_STORE_PATH` or `DATA_DIR` to configure the SQLite DB location.

## Outbox ACK correlation usage

```ts
import { NatsBroker } from "@murmurv2/broker-nats";
import { SQLiteDedupeOutboxStore } from "@murmurv2/core";

const broker = new NatsBroker({ url: "nats://127.0.0.1:4222" });
const store = new SQLiteDedupeOutboxStore(".data/murmur.db");

await broker.startAckCorrelation({
  outbox: store,
  ackSubject: "ack.consumer-a",
});

await broker.flushOutbox({ outbox: store });
```

## Secure setup + runbook

### 1) Enable durable store

```bash
export MURMUR_STORE_PATH=".data/murmur.db"
```

Use SQLite WAL (default in `SQLiteDedupeOutboxStore`) and back up `.data/` regularly.

### 2) Configure policy guardrails before publish

In broker calls, pass a policy:

```ts
await broker.flushOutbox({
  outbox: store,
  ackTimeoutMs: 30_000,
  jitterRatio: 0.2,
  policy: {
    maxPayloadBytes: 64 * 1024,
    allowedRoutes: {
      "telegram-bridge": ["human", "assistant"],
      "assistant": ["human"],
    },
  },
});
```

### 3) Configure E2E crypto provider

```ts
import { NaClCryptoProvider, setCryptoProvider } from "@murmurv2/security";
setCryptoProvider(new NaClCryptoProvider());
```

### 4) (Optional) MLS scaffold flag

```bash
export MURMUR_ENABLE_MLS=1
```

This only enables scaffolded interfaces right now. See `docs/MLS-SCAFFOLD.md`.

### 5) Validate reliability loop

- start ack correlation worker
- run outbox flush on interval
- monitor rows stuck in `sent`
- requeue stale `sent` using `ackTimeoutMs`
- alert on `dlq` growth and poison-message nacks

## Notification routing

`murmur-daemon` supports unified notify adapters with retries + idempotent queueing:

```json
{
  "notify": {
    "telegram": { "botToken": "...", "chatId": "...", "topicId": "..." },
    "webhook": [
      { "channel": "discord", "url": "https://discord.com/api/webhooks/..." },
      { "channel": "whatsapp", "url": "https://your-bridge.example/hook" }
    ]
  }
}
```

Backward-compatible shapes are also accepted:
- `notify: { botToken, chatId, topicId }`
- `notify: { url, headers }`

## Tests

```bash
npm test                # build + unit tests + notify smoke
npm run test:integration
npm run test:notify-smoke
```

## Secure local demo (default)

The demo producer/consumer scripts now run a secure end-to-end flow by default:

- producer encrypts payload + signs envelope with the active crypto provider
- producer enqueues to SQLite outbox and flushes via broker policy checks
- consumer verifies route/payload policy, verifies signature, decrypts payload, and ACKs
- producer correlates ACKs back to outbox row status

### One-command smoke test

```bash
npm run demo:secure
```

This will build, start local NATS, run secure consumer + producer, and tear down services.

### Manual run

```bash
npm run demo:up
npm run demo:consumer
# second terminal
npm run demo:producer
npm run demo:down
```

### Demo environment knobs

```bash
# transport
export NATS_URL="nats://127.0.0.1:4222"
export SUBJECT="msg.demo.secure"

# identities / policy route
export SENDER_AGENT_ID="agent-codex"
export RECIPIENT_AGENT_ID="agent-jarvis"
export CONSUMER_ID="agent-jarvis"

# data paths
export OUTBOX_DB_PATH=".data/demo-outbox.db"
export DEDUPE_DB_PATH=".data/demo-dedupe.db"
export DEMO_KEYS_PATH=".data/demo-keys.json"

# payload + reliability controls
export MESSAGE="hello secure mur-mur"
export MAX_PAYLOAD_BYTES="65536"
export ACK_TIMEOUT_MS="15000"
export WAIT_FOR_ACK_MS="15000"
export FLUSH_MAX_ATTEMPTS="5"

# consumer behavior (default exits after first verified message)
export DEMO_EXIT_AFTER_ONE="1"
```

Failure logs are explicit (`[producer] secure demo failed`, `[consumer] secure demo failed`, `[smoke] ... FAIL`) so CI/manual runs can quickly identify policy/crypto/outbox failures.
