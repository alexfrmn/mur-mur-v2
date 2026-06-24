// Murmur Phase N / N1 — typed channel roster.
// This is intentionally separate from local_messages: conversationId remains a history label,
// while channelId is the stable routing/personality primitive for N2/N3.
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

export type ChannelType = "dm" | "group" | "consult";

export interface ChannelRecord {
  channelId: string;
  conversationId: string;
  type: ChannelType;
  createdAt: string;
  closedAt?: string;
  metadata: Record<string, unknown>;
}

export interface ChannelMemberRecord {
  channelId: string;
  memberId: string;
  memberSlot?: string;
  agentId: string;
  role?: string;
  personaId?: string;
  model?: string;
  baseInstructionsHash?: string;
  joinedAt: string;
  leftAt?: string;
  eligibility: Record<string, unknown>;
  metadata: Record<string, unknown>;
}

export interface ChannelMemberInput {
  memberId: string;
  memberSlot?: string;
  agentId: string;
  role?: string;
  personaId?: string;
  model?: string;
  baseInstructionsHash?: string;
  joinedAt?: string;
  leftAt?: string;
  eligibility?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
}

export interface CreateChannelInput {
  channelId: string;
  conversationId: string;
  type: ChannelType;
  createdAt?: string;
  closedAt?: string;
  metadata?: Record<string, unknown>;
  members?: ChannelMemberInput[];
}

export type ChannelAddressingReason =
  | "legacy-no-channel"
  | "channel-not-found"
  | "channel-closed"
  | "self-not-member"
  | "sender-not-member"
  | "channel-broadcast"
  | "addressee-not-member"
  | "addressed-member"
  | "observer-muted";

export interface ChannelAddressingInput {
  channelId?: string;
  selfAgentId: string;
  senderAgentId?: string;
  addresseeMemberId?: string;
  addresseeAgentId?: string;
}

export interface ChannelAddressingDecision {
  allowAppend: boolean;
  allowWake: boolean;
  reject: boolean;
  reason: ChannelAddressingReason;
  channel?: ChannelRecord;
  selfMember?: ChannelMemberRecord;
  senderMember?: ChannelMemberRecord;
  addresseeMember?: ChannelMemberRecord;
}

export interface ChannelThreadStartBindingInput {
  member: ChannelMemberRecord;
  baseInstructions?: string | null;
}

export interface ChannelThreadStartBinding {
  model: string | null;
  personality: string | null;
  baseInstructions: string | null;
  metadata: {
    murmur_channel_id: string;
    murmur_member_id: string;
    murmur_agent_id: string;
    murmur_member_slot?: string;
    murmur_persona_id?: string;
    murmur_model?: string;
    murmur_base_instructions_hash?: string;
  };
}

export const buildChannelThreadStartBinding = ({ member, baseInstructions = null }: ChannelThreadStartBindingInput): ChannelThreadStartBinding => ({
  model: member.model ?? null,
  personality: member.personaId ?? null,
  baseInstructions,
  metadata: {
    murmur_channel_id: member.channelId,
    murmur_member_id: member.memberId,
    murmur_agent_id: member.agentId,
    ...(member.memberSlot ? { murmur_member_slot: member.memberSlot } : {}),
    ...(member.personaId ? { murmur_persona_id: member.personaId } : {}),
    ...(member.model ? { murmur_model: member.model } : {}),
    ...(member.baseInstructionsHash ? { murmur_base_instructions_hash: member.baseInstructionsHash } : {}),
  },
});

const DDL = `
  PRAGMA journal_mode=WAL;
  PRAGMA busy_timeout=10000;
  PRAGMA foreign_keys=ON;

  CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('dm', 'group', 'consult')),
    created_at TEXT NOT NULL,
    closed_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
  );
  CREATE INDEX IF NOT EXISTS idx_channels_conversation ON channels(conversation_id);
  CREATE INDEX IF NOT EXISTS idx_channels_type ON channels(type);

  CREATE TABLE IF NOT EXISTS channel_members (
    channel_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    member_slot TEXT,
    agent_id TEXT NOT NULL,
    role TEXT,
    persona_id TEXT,
    model TEXT,
    base_instructions_hash TEXT,
    eligibility_json TEXT NOT NULL DEFAULT '{}',
    joined_at TEXT NOT NULL,
    left_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (channel_id, member_id),
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE
  );
  CREATE INDEX IF NOT EXISTS idx_channel_members_agent ON channel_members(agent_id);
  CREATE INDEX IF NOT EXISTS idx_channel_members_slot ON channel_members(channel_id, member_slot);
  CREATE INDEX IF NOT EXISTS idx_channel_members_persona ON channel_members(persona_id);
`;

const jsonObject = (value?: Record<string, unknown>): string => JSON.stringify(value ?? {});

const parseJsonObject = (raw: unknown): Record<string, unknown> => {
  if (typeof raw !== "string" || raw.length === 0) return {};
  try {
    const parsed = JSON.parse(raw) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
};

export class ChannelRosterStore {
  private readonly db: DatabaseSync;

  constructor(dbPath = ".data/channel-roster.db") {
    mkdirSync(dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec(DDL);
  }

  createChannel(input: CreateChannelInput): ChannelRecord {
    const createdAt = input.createdAt ?? new Date().toISOString();
    this.db.exec("BEGIN");
    try {
      this.db
        .prepare(
          `INSERT INTO channels (channel_id, conversation_id, type, created_at, closed_at, metadata_json)
           VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .run(input.channelId, input.conversationId, input.type, createdAt, input.closedAt ?? null, jsonObject(input.metadata));

      for (const member of input.members ?? []) {
        this.insertChannelMember(input.channelId, member, createdAt, false);
      }

      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }

    const channel = this.getChannel(input.channelId);
    if (!channel) throw new Error(`channel create failed: ${input.channelId}`);
    return channel;
  }

  getChannel(channelId: string): ChannelRecord | null {
    const row = this.db
      .prepare(
        `SELECT
           channel_id as channelId,
           conversation_id as conversationId,
           type,
           created_at as createdAt,
           closed_at as closedAt,
           metadata_json as metadataJson
         FROM channels
         WHERE channel_id = ?`,
      )
      .get(channelId) as
      | { channelId: string; conversationId: string; type: ChannelType; createdAt: string; closedAt?: string | null; metadataJson: string }
      | undefined;
    return row ? this.toChannelRecord(row) : null;
  }

  listChannelsForConversation(conversationId: string): ChannelRecord[] {
    const rows = this.db
      .prepare(
        `SELECT
           channel_id as channelId,
           conversation_id as conversationId,
           type,
           created_at as createdAt,
           closed_at as closedAt,
           metadata_json as metadataJson
         FROM channels
         WHERE conversation_id = ?
         ORDER BY created_at ASC`,
      )
      .all(conversationId) as Array<{ channelId: string; conversationId: string; type: ChannelType; createdAt: string; closedAt?: string | null; metadataJson: string }>;
    return rows.map((row) => this.toChannelRecord(row));
  }

  listChannelMembers(channelId: string): ChannelMemberRecord[] {
    const rows = this.db
      .prepare(
        `SELECT
           channel_id as channelId,
           member_id as memberId,
           member_slot as memberSlot,
           agent_id as agentId,
           role,
           persona_id as personaId,
           model,
           base_instructions_hash as baseInstructionsHash,
           eligibility_json as eligibilityJson,
           joined_at as joinedAt,
           left_at as leftAt,
           metadata_json as metadataJson
         FROM channel_members
         WHERE channel_id = ?
         ORDER BY member_id ASC`,
      )
      .all(channelId) as Array<{
      channelId: string;
      memberId: string;
      memberSlot?: string | null;
      agentId: string;
      role?: string | null;
      personaId?: string | null;
      model?: string | null;
      baseInstructionsHash?: string | null;
      eligibilityJson: string;
      joinedAt: string;
      leftAt?: string | null;
      metadataJson: string;
    }>;
    return rows.map((row) => this.toChannelMemberRecord(row));
  }

  getChannelMember(channelId: string, memberId: string): ChannelMemberRecord | null {
    const row = this.db
      .prepare(
        `SELECT
           channel_id as channelId,
           member_id as memberId,
           member_slot as memberSlot,
           agent_id as agentId,
           role,
           persona_id as personaId,
           model,
           base_instructions_hash as baseInstructionsHash,
           eligibility_json as eligibilityJson,
           joined_at as joinedAt,
           left_at as leftAt,
           metadata_json as metadataJson
         FROM channel_members
         WHERE channel_id = ? AND member_id = ?`,
      )
      .get(channelId, memberId) as
      | {
        channelId: string;
        memberId: string;
        memberSlot?: string | null;
        agentId: string;
        role?: string | null;
        personaId?: string | null;
        model?: string | null;
        baseInstructionsHash?: string | null;
        eligibilityJson: string;
        joinedAt: string;
        leftAt?: string | null;
        metadataJson: string;
      }
      | undefined;
    return row ? this.toChannelMemberRecord(row) : null;
  }

  findActiveMembersForAgent(agentId: string): ChannelMemberRecord[] {
    const rows = this.db
      .prepare(
        `SELECT
           channel_id as channelId,
           member_id as memberId,
           member_slot as memberSlot,
           agent_id as agentId,
           role,
           persona_id as personaId,
           model,
           base_instructions_hash as baseInstructionsHash,
           eligibility_json as eligibilityJson,
           joined_at as joinedAt,
           left_at as leftAt,
           metadata_json as metadataJson
         FROM channel_members
         WHERE agent_id = ? AND left_at IS NULL
         ORDER BY joined_at ASC`,
      )
      .all(agentId) as Array<{
      channelId: string;
      memberId: string;
      memberSlot?: string | null;
      agentId: string;
      role?: string | null;
      personaId?: string | null;
      model?: string | null;
      baseInstructionsHash?: string | null;
      eligibilityJson: string;
      joinedAt: string;
      leftAt?: string | null;
      metadataJson: string;
    }>;
    return rows.map((row) => this.toChannelMemberRecord(row));
  }

  findActiveChannelMemberForAgent(channelId: string, agentId: string): ChannelMemberRecord | null {
    const row = this.db
      .prepare(
        `SELECT
           channel_id as channelId,
           member_id as memberId,
           member_slot as memberSlot,
           agent_id as agentId,
           role,
           persona_id as personaId,
           model,
           base_instructions_hash as baseInstructionsHash,
           eligibility_json as eligibilityJson,
           joined_at as joinedAt,
           left_at as leftAt,
           metadata_json as metadataJson
         FROM channel_members
         WHERE channel_id = ? AND agent_id = ? AND left_at IS NULL
         ORDER BY joined_at ASC
         LIMIT 1`,
      )
      .get(channelId, agentId) as
      | {
        channelId: string;
        memberId: string;
        memberSlot?: string | null;
        agentId: string;
        role?: string | null;
        personaId?: string | null;
        model?: string | null;
        baseInstructionsHash?: string | null;
        eligibilityJson: string;
        joinedAt: string;
        leftAt?: string | null;
        metadataJson: string;
      }
      | undefined;
    return row ? this.toChannelMemberRecord(row) : null;
  }

  isChannelMember(channelId: string, agentId: string): boolean {
    const row = this.db
      .prepare(`SELECT 1 FROM channel_members WHERE channel_id = ? AND agent_id = ? AND left_at IS NULL LIMIT 1`)
      .get(channelId, agentId) as { "1": number } | undefined;
    return row !== undefined;
  }

  evaluateAddressing(input: ChannelAddressingInput): ChannelAddressingDecision {
    if (!input.channelId) {
      return { allowAppend: true, allowWake: true, reject: false, reason: "legacy-no-channel" };
    }

    const channel = this.getChannel(input.channelId);
    if (!channel) {
      return { allowAppend: false, allowWake: false, reject: true, reason: "channel-not-found" };
    }
    if (channel.closedAt) {
      return { allowAppend: false, allowWake: false, reject: true, reason: "channel-closed", channel };
    }

    const selfMember = this.findActiveChannelMemberForAgent(input.channelId, input.selfAgentId);
    if (!selfMember) {
      return { allowAppend: false, allowWake: false, reject: true, reason: "self-not-member", channel };
    }

    const senderMember = input.senderAgentId ? this.findActiveChannelMemberForAgent(input.channelId, input.senderAgentId) : undefined;
    if (input.senderAgentId && !senderMember) {
      return { allowAppend: false, allowWake: false, reject: true, reason: "sender-not-member", channel, selfMember };
    }

    if (!input.addresseeMemberId && !input.addresseeAgentId) {
      return { allowAppend: true, allowWake: true, reject: false, reason: "channel-broadcast", channel, selfMember, ...(senderMember ? { senderMember } : {}) };
    }

    const addresseeMember = input.addresseeMemberId
      ? this.getChannelMember(input.channelId, input.addresseeMemberId)
      : this.findActiveChannelMemberForAgent(input.channelId, input.addresseeAgentId ?? "");
    if (!addresseeMember || addresseeMember.leftAt) {
      return { allowAppend: false, allowWake: false, reject: true, reason: "addressee-not-member", channel, selfMember, ...(senderMember ? { senderMember } : {}) };
    }

    const isAddressed = addresseeMember.agentId === input.selfAgentId;
    return {
      allowAppend: true,
      allowWake: isAddressed,
      reject: false,
      reason: isAddressed ? "addressed-member" : "observer-muted",
      channel,
      selfMember,
      ...(senderMember ? { senderMember } : {}),
      addresseeMember,
    };
  }

  upsertChannelMember(channelId: string, member: ChannelMemberInput): ChannelMemberRecord {
    this.insertChannelMember(channelId, member, new Date().toISOString(), true);
    const found = this.getChannelMember(channelId, member.memberId);
    if (!found) throw new Error(`channel member upsert failed: ${channelId}/${member.memberId}`);
    return found;
  }

  closeChannel(channelId: string, closedAt = new Date().toISOString()): boolean {
    const result = this.db
      .prepare(`UPDATE channels SET closed_at = ? WHERE channel_id = ? AND closed_at IS NULL`)
      .run(closedAt, channelId);
    return result.changes > 0;
  }

  close(): void {
    this.db.close();
  }

  private insertChannelMember(channelId: string, member: ChannelMemberInput, defaultJoinedAt: string, upsert: boolean): void {
    const joinedAt = member.joinedAt ?? defaultJoinedAt;
    if (upsert) {
      this.db
        .prepare(
          `INSERT INTO channel_members
           (channel_id, member_id, member_slot, agent_id, role, persona_id, model, base_instructions_hash, eligibility_json, joined_at, left_at, metadata_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(channel_id, member_id) DO UPDATE SET
             member_slot = excluded.member_slot,
             agent_id = excluded.agent_id,
             role = excluded.role,
             persona_id = excluded.persona_id,
             model = excluded.model,
             base_instructions_hash = excluded.base_instructions_hash,
             eligibility_json = excluded.eligibility_json,
             left_at = excluded.left_at,
             metadata_json = excluded.metadata_json`,
        )
        .run(
          channelId,
          member.memberId,
          member.memberSlot ?? null,
          member.agentId,
          member.role ?? null,
          member.personaId ?? null,
          member.model ?? null,
          member.baseInstructionsHash ?? null,
          jsonObject(member.eligibility),
          joinedAt,
          member.leftAt ?? null,
          jsonObject(member.metadata),
        );
      return;
    }

    this.db
      .prepare(
        `INSERT INTO channel_members
         (channel_id, member_id, member_slot, agent_id, role, persona_id, model, base_instructions_hash, eligibility_json, joined_at, left_at, metadata_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        channelId,
        member.memberId,
        member.memberSlot ?? null,
        member.agentId,
        member.role ?? null,
        member.personaId ?? null,
        member.model ?? null,
        member.baseInstructionsHash ?? null,
        jsonObject(member.eligibility),
        joinedAt,
        member.leftAt ?? null,
        jsonObject(member.metadata),
      );
  }

  private toChannelRecord(row: { channelId: string; conversationId: string; type: ChannelType; createdAt: string; closedAt?: string | null; metadataJson: string }): ChannelRecord {
    return {
      channelId: row.channelId,
      conversationId: row.conversationId,
      type: row.type,
      createdAt: row.createdAt,
      ...(row.closedAt ? { closedAt: row.closedAt } : {}),
      metadata: parseJsonObject(row.metadataJson),
    };
  }

  private toChannelMemberRecord(row: {
    channelId: string;
    memberId: string;
    memberSlot?: string | null;
    agentId: string;
    role?: string | null;
    personaId?: string | null;
    model?: string | null;
    baseInstructionsHash?: string | null;
    eligibilityJson: string;
    joinedAt: string;
    leftAt?: string | null;
    metadataJson: string;
  }): ChannelMemberRecord {
    return {
      channelId: row.channelId,
      memberId: row.memberId,
      ...(row.memberSlot ? { memberSlot: row.memberSlot } : {}),
      agentId: row.agentId,
      ...(row.role ? { role: row.role } : {}),
      ...(row.personaId ? { personaId: row.personaId } : {}),
      ...(row.model ? { model: row.model } : {}),
      ...(row.baseInstructionsHash ? { baseInstructionsHash: row.baseInstructionsHash } : {}),
      joinedAt: row.joinedAt,
      ...(row.leftAt ? { leftAt: row.leftAt } : {}),
      eligibility: parseJsonObject(row.eligibilityJson),
      metadata: parseJsonObject(row.metadataJson),
    };
  }
}
