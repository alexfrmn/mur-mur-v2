import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { buildChannelThreadStartBinding, ChannelRosterStore, SQLiteMessageStore } from "../packages/core/dist/src/index.js";

async function withRoster(fn) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "murmur-channels-"));
  try {
    const store = new ChannelRosterStore(path.join(dir, "channel-roster.db"));
    return await fn(store);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test("ChannelRosterStore creates typed channel roster with personality-ready member metadata", async () => {
  await withRoster(async (store) => {
    const channel = store.createChannel({
      channelId: "chan:research:alpha",
      conversationId: "codex:task:alpha",
      type: "group",
      createdAt: "2026-06-24T10:00:00.000Z",
      metadata: { topic: "phase-n" },
      members: [
        {
          memberId: "owner",
          memberSlot: "agent-codex-volt",
          agentId: "agent-codex-volt",
          role: "implementer",
          personaId: "codex-senior-engineer",
          model: "gpt-5",
          baseInstructionsHash: "sha256:codex",
          eligibility: { canAddress: true },
          metadata: { lane: "implementation" },
        },
        {
          memberId: "reviewer",
          memberSlot: "agent-jarvis",
          agentId: "agent-jarvis",
          role: "reviewer",
          personaId: "jarvis-ops",
          model: "claude",
        },
      ],
    });

    assert.deepEqual(channel, {
      channelId: "chan:research:alpha",
      conversationId: "codex:task:alpha",
      type: "group",
      createdAt: "2026-06-24T10:00:00.000Z",
      metadata: { topic: "phase-n" },
    });

    const members = store.listChannelMembers("chan:research:alpha");
    assert.equal(members.length, 2);
    assert.equal(members[0].memberId, "owner");
    assert.equal(members[0].memberSlot, "agent-codex-volt");
    assert.equal(members[0].personaId, "codex-senior-engineer");
    assert.equal(members[0].baseInstructionsHash, "sha256:codex");
    assert.deepEqual(members[0].eligibility, { canAddress: true });
    assert.deepEqual(members[0].metadata, { lane: "implementation" });
    assert.equal(store.isChannelMember("chan:research:alpha", "agent-codex-volt"), true);
    assert.equal(store.isChannelMember("chan:research:alpha", "agent-unknown"), false);
  });
});

test("channelId is separate from conversationId and legacy conversation listing still works", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "murmur-channels-legacy-"));
  try {
    const messages = new SQLiteMessageStore(path.join(dir, "murmur.db"));
    const roster = new ChannelRosterStore(path.join(dir, "channel-roster.db"));
    await messages.append({
      conversationId: "codex:task:legacy",
      msgId: "msg-1",
      direction: "inbound",
      sender: "agent-jarvis",
      text: "legacy message",
      createdAt: "2026-06-24T10:01:00.000Z",
      transport: "nats",
    });
    roster.createChannel({
      channelId: "chan:dm:jarvis-codex",
      conversationId: "codex:task:legacy",
      type: "dm",
      createdAt: "2026-06-24T10:02:00.000Z",
    });

    const conversations = await messages.listConversations();
    assert.equal(conversations.length, 1);
    assert.equal(conversations[0].conversationId, "codex:task:legacy");
    assert.equal(conversations[0].messageCount, 1);

    const channels = roster.listChannelsForConversation("codex:task:legacy");
    assert.equal(channels.length, 1);
    assert.equal(channels[0].channelId, "chan:dm:jarvis-codex");
    assert.equal(channels[0].conversationId, "codex:task:legacy");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("channel members can be rebound by memberId without changing legacy messages", async () => {
  await withRoster(async (store) => {
    store.createChannel({
      channelId: "chan:consult:qa",
      conversationId: "codex:task:qa",
      type: "consult",
      createdAt: "2026-06-24T10:03:00.000Z",
      members: [{ memberId: "reviewer", memberSlot: "qa", agentId: "agent-stas", role: "qa" }],
    });

    const rebound = store.upsertChannelMember("chan:consult:qa", {
      memberId: "reviewer",
      memberSlot: "ops",
      agentId: "agent-jarvis",
      role: "ops-review",
      personaId: "jarvis-readiness",
    });

    assert.equal(rebound.memberId, "reviewer");
    assert.equal(rebound.memberSlot, "ops");
    assert.equal(rebound.agentId, "agent-jarvis");
    assert.equal(rebound.personaId, "jarvis-readiness");
    assert.equal(store.isChannelMember("chan:consult:qa", "agent-stas"), false);
    assert.equal(store.isChannelMember("chan:consult:qa", "agent-jarvis"), true);
  });
});

test("closing a channel is idempotent and preserves roster rows", async () => {
  await withRoster(async (store) => {
    store.createChannel({
      channelId: "chan:closed",
      conversationId: "codex:task:closed",
      type: "group",
      createdAt: "2026-06-24T10:04:00.000Z",
      members: [{ memberId: "owner", memberSlot: "agent-codex-volt", agentId: "agent-codex-volt" }],
    });

    assert.equal(store.closeChannel("chan:closed", "2026-06-24T10:05:00.000Z"), true);
    assert.equal(store.closeChannel("chan:closed", "2026-06-24T10:06:00.000Z"), false);

    const channel = store.getChannel("chan:closed");
    assert.equal(channel.closedAt, "2026-06-24T10:05:00.000Z");
    const members = store.listChannelMembers("chan:closed");
    assert.equal(members.length, 1);
  });
});

test("addressing policy preserves legacy no-channel broadcast behavior", async () => {
  await withRoster(async (store) => {
    assert.deepEqual(store.evaluateAddressing({ selfAgentId: "agent-codex-volt" }), {
      allowAppend: true,
      allowWake: true,
      reject: false,
      reason: "legacy-no-channel",
    });
  });
});

test("addressing policy rejects channel traffic from or to non-members", async () => {
  await withRoster(async (store) => {
    store.createChannel({
      channelId: "chan:members-only",
      conversationId: "codex:task:members-only",
      type: "group",
      members: [
        { memberId: "codex", agentId: "agent-codex-volt" },
        { memberId: "jarvis", agentId: "agent-jarvis" },
      ],
    });

    assert.equal(store.evaluateAddressing({
      channelId: "chan:members-only",
      selfAgentId: "agent-outsider",
      senderAgentId: "agent-jarvis",
      addresseeAgentId: "agent-codex-volt",
    }).reason, "self-not-member");

    assert.equal(store.evaluateAddressing({
      channelId: "chan:members-only",
      selfAgentId: "agent-codex-volt",
      senderAgentId: "agent-outsider",
      addresseeAgentId: "agent-codex-volt",
    }).reason, "sender-not-member");

    assert.equal(store.evaluateAddressing({
      channelId: "chan:members-only",
      selfAgentId: "agent-codex-volt",
      senderAgentId: "agent-jarvis",
      addresseeAgentId: "agent-outsider",
    }).reason, "addressee-not-member");
  });
});

test("addressing policy wakes only the explicit addressee and mutes observers", async () => {
  await withRoster(async (store) => {
    store.createChannel({
      channelId: "chan:addressed",
      conversationId: "codex:task:addressed",
      type: "group",
      members: [
        { memberId: "codex", agentId: "agent-codex-volt", role: "implementer" },
        { memberId: "jarvis", agentId: "agent-jarvis", role: "reviewer" },
        { memberId: "stas", agentId: "agent-stas", role: "observer" },
      ],
    });

    const addressed = store.evaluateAddressing({
      channelId: "chan:addressed",
      selfAgentId: "agent-codex-volt",
      senderAgentId: "agent-jarvis",
      addresseeMemberId: "codex",
    });
    assert.equal(addressed.reason, "addressed-member");
    assert.equal(addressed.allowAppend, true);
    assert.equal(addressed.allowWake, true);
    assert.equal(addressed.reject, false);
    assert.equal(addressed.addresseeMember.agentId, "agent-codex-volt");

    const observer = store.evaluateAddressing({
      channelId: "chan:addressed",
      selfAgentId: "agent-stas",
      senderAgentId: "agent-jarvis",
      addresseeMemberId: "codex",
    });
    assert.equal(observer.reason, "observer-muted");
    assert.equal(observer.allowAppend, true);
    assert.equal(observer.allowWake, false);
    assert.equal(observer.reject, false);
  });
});

test("addressing policy treats channel messages without addressee as channel broadcast", async () => {
  await withRoster(async (store) => {
    store.createChannel({
      channelId: "chan:broadcast",
      conversationId: "codex:task:broadcast",
      type: "group",
      members: [{ memberId: "codex", agentId: "agent-codex-volt" }],
    });

    const decision = store.evaluateAddressing({
      channelId: "chan:broadcast",
      selfAgentId: "agent-codex-volt",
      senderAgentId: "agent-codex-volt",
    });
    assert.equal(decision.reason, "channel-broadcast");
    assert.equal(decision.allowAppend, true);
    assert.equal(decision.allowWake, true);
    assert.equal(decision.reject, false);
  });
});

test("thread start binding projects channel member persona/model/instructions without wake wiring", async () => {
  await withRoster(async (store) => {
    store.createChannel({
      channelId: "chan:persona",
      conversationId: "codex:task:persona",
      type: "dm",
      members: [{
        memberId: "codex-writer",
        memberSlot: "agent-codex-volt:writer",
        agentId: "agent-codex-volt",
        personaId: "codex-writer",
        model: "gpt-5",
        baseInstructionsHash: "sha256:writer-v1",
      }],
    });

    const member = store.getChannelMember("chan:persona", "codex-writer");
    const binding = buildChannelThreadStartBinding({
      member,
      baseInstructions: "Write concise engineering notes.",
    });

    assert.deepEqual(binding, {
      model: "gpt-5",
      personality: "codex-writer",
      baseInstructions: "Write concise engineering notes.",
      metadata: {
        murmur_channel_id: "chan:persona",
        murmur_member_id: "codex-writer",
        murmur_agent_id: "agent-codex-volt",
        murmur_member_slot: "agent-codex-volt:writer",
        murmur_persona_id: "codex-writer",
        murmur_model: "gpt-5",
        murmur_base_instructions_hash: "sha256:writer-v1",
      },
    });
  });
});

test("thread start binding remains null-safe for generic channel members", async () => {
  const binding = buildChannelThreadStartBinding({
    member: {
      channelId: "chan:generic",
      memberId: "codex",
      agentId: "agent-codex-volt",
      joinedAt: "2026-06-24T10:00:00.000Z",
      eligibility: {},
      metadata: {},
    },
  });

  assert.deepEqual(binding, {
    model: null,
    personality: null,
    baseInstructions: null,
    metadata: {
      murmur_channel_id: "chan:generic",
      murmur_member_id: "codex",
      murmur_agent_id: "agent-codex-volt",
    },
  });
});
