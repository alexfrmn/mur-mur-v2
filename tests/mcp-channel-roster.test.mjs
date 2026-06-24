import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";

function callMcp(proc, id, name, args) {
  proc.stdin.write(`${JSON.stringify({
    jsonrpc: "2.0",
    id,
    method: "tools/call",
    params: { name, arguments: args },
  })}\n`);
}

function nextResponse(proc) {
  return new Promise((resolve, reject) => {
    let buf = "";
    const onData = (chunk) => {
      buf += String(chunk);
      const idx = buf.indexOf("\n");
      if (idx < 0) return;
      proc.stdout.off("data", onData);
      const line = buf.slice(0, idx);
      try {
        resolve(JSON.parse(line));
      } catch (err) {
        reject(err);
      }
    };
    proc.stdout.on("data", onData);
  });
}

async function callTool(proc, id, name, args) {
  const responseP = nextResponse(proc);
  callMcp(proc, id, name, args);
  const response = await responseP;
  assert.equal(response.id, id);
  assert.ok(!response.error, response.error?.message);
  return JSON.parse(response.result.content[0].text);
}

test("MCP channel roster tools create/list/evaluate addressing", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "murmur-mcp-channel-"));
  const proc = spawn(process.execPath, ["packages/mcp-server/dist/src/index.js"], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      DATA_DIR: dir,
      MURMUR_STORE_PATH: path.join(dir, "murmur.db"),
      MURMUR_CHANNEL_ROSTER_PATH: path.join(dir, "channel-roster.db"),
    },
    stdio: ["pipe", "pipe", "pipe"],
  });
  try {
    const created = await callTool(proc, 1, "channel_create", {
      channelId: "chan:mcp:test",
      conversationId: "codex:task:mcp-test",
      type: "group",
      members: [
        { memberId: "codex", agentId: "agent-codex-volt", role: "implementer" },
        { memberId: "jarvis", agentId: "agent-jarvis", role: "reviewer" },
      ],
    });
    assert.equal(created.channel.channelId, "chan:mcp:test");
    assert.equal(created.members.length, 2);

    const listed = await callTool(proc, 2, "channel_list", { conversationId: "codex:task:mcp-test" });
    assert.equal(listed.channels.length, 1);
    assert.equal(listed.channels[0].channelId, "chan:mcp:test");

    const members = await callTool(proc, 3, "channel_members", { channelId: "chan:mcp:test" });
    assert.equal(members.members[0].memberId, "codex");

    const decision = await callTool(proc, 4, "channel_evaluate_addressing", {
      channelId: "chan:mcp:test",
      selfAgentId: "agent-jarvis",
      senderAgentId: "agent-codex-volt",
      addresseeMemberId: "codex",
    });
    assert.equal(decision.decision.reason, "observer-muted");
    assert.equal(decision.decision.allowAppend, true);
    assert.equal(decision.decision.allowWake, false);
  } finally {
    proc.kill();
    await rm(dir, { recursive: true, force: true });
  }
});
