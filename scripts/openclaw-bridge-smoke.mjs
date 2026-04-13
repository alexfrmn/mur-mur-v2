#!/usr/bin/env node
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { OpenClawBridgeQueue, flushOpenClawBridgeQueue } from "./openclaw-bridge.mjs";

const dir = mkdtempSync(path.join(tmpdir(), "murmur-openclaw-smoke-"));
const dbPath = path.join(dir, "murmur.db");
const outPath = path.join(dir, "bridge.log");
const helperPath = path.join(dir, "helper.mjs");

writeFileSync(helperPath, `#!/usr/bin/env node\nimport { appendFileSync } from 'node:fs';\nappendFileSync(process.env.BRIDGE_OUT_PATH, process.env.MURMUR_OPENCLAW_PAYLOAD_JSON + '\\n');\n`);

try {
  const queue = new OpenClawBridgeQueue(dbPath);
  const payload = {
    from: "agent-a",
    text: "hello openclaw",
    msgId: "bridge-smoke-1",
    conversationId: "conv-1",
    ts: new Date().toISOString(),
  };

  const targets = [{
    type: "openclaw",
    channel: "smoke",
    helperScript: helperPath,
    extraEnv: { BRIDGE_OUT_PATH: outPath },
  }];

  queue.enqueueMessage(payload, targets);
  queue.enqueueMessage(payload, targets);

  await flushOpenClawBridgeQueue({ queue, log: () => {}, limit: 10 });

  const lines = readFileSync(outPath, "utf8").trim().split("\n").filter(Boolean);
  assert.equal(lines.length, 1, "should dispatch exactly once for duplicated msgId");
  const bridged = JSON.parse(lines[0]);
  assert.equal(bridged.msgId, payload.msgId);
  assert.equal(queue.pendingCount(), 0);

  console.log("[openclaw-bridge-smoke] PASS");
} finally {
  rmSync(dir, { recursive: true, force: true });
}
