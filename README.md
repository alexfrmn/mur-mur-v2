# mur-mur-v2

Reliable, secure multi-agent messaging core for OpenClaw-style agent systems.

## What is implemented (prod-first)

- ACK correlation path in broker (`startAckCorrelation`) to mark outbox rows `acked` by `msgId`.
- Persistent optimistic-locking store (`SQLiteDedupeOutboxStore`) for dedupe + outbox state.
- Postgres SQL executor abstraction (`PgSqlExecutor`) for environments that need PG-backed implementations.
- Minimal functional Telegram bridge (`@murmurv2/bridge-telegram`) for inbound/outbound with env config.
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

## Basic MCP server

Build first, then run:

```bash
npm run build
npm run mcp:server
```

The server speaks line-delimited JSON-RPC over stdio and supports `initialize`, `tools/list`, and `tools/call` for:

- `send_message`
- `list_conversations`
- `search_messages`

Use `MURMUR_STORE_PATH` to point to the SQLite DB file.

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

## Local demo (NATS)

```bash
npm run demo:up
npm run demo:consumer
# second terminal
npm run demo:producer
npm run demo:down
```
