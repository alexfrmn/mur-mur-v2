/**
 * vault-guard.mjs — Murmur-level guard that warns when vault tasks
 * are sent to agents without vault access.
 *
 * This is a WARN-ONLY guard (does not block). Blocking could break
 * legitimate fallback flows, so we log warnings for human review.
 *
 * Vault-capable agents (on Main server with filesystem access):
 *   - agent-jarvis
 *   - agent-codex-volt
 *
 * Non-vault agents (remote servers, no vault filesystem):
 *   - agent-codex       (CODEX-1, GCP)
 *   - codex2-agent-hq   (CODEX-2, Agent-HQ)
 *   - glm-agent-hq      (GLM-Analyst, Agent-HQ)
 *   - glm-worker-agent-hq (GLM-Worker, Agent-HQ)
 *   - haiku-agent-hq    (HAIKU-QA, Agent-HQ)
 *
 * Reference: vault/Rules/Operations/agent-delegation.md
 */

// Agents that have vault filesystem access
const VAULT_AGENTS = new Set([
  "agent-jarvis",
  "agent-codex-volt",
]);

// Keywords that indicate a vault-related task
// Sorted by specificity: paths first, then general terms
const VAULT_KEYWORDS = [
  // Vault directory paths
  "00-Inbox/",
  "Everyday/",
  "People/",
  "Myself/",
  "Knowledge-Base/",
  "Research/",
  "IT/",
  "Plans/",
  "Rules/",
  "Agents-Space/",
  "Templates/",
  // Vault root
  "/opt/lifecoach/vault",
  // Obsidian-specific
  "obsidian",
  "[[",     // wiki-links
  "frontmatter",
  // File operations on .md
  ".md",
  // General vault terms
  "vault",
];

// Case-insensitive keyword check
const CASE_INSENSITIVE_KEYWORDS = new Set([
  "vault",
  "obsidian",
  "frontmatter",
]);

/**
 * Check if a message text contains vault-related keywords.
 * @param {string} text — message/task text
 * @returns {string[]} — list of matched keywords (empty = no match)
 */
function findVaultKeywords(text) {
  if (!text || typeof text !== "string") return [];

  const found = [];
  for (const kw of VAULT_KEYWORDS) {
    const check = CASE_INSENSITIVE_KEYWORDS.has(kw)
      ? text.toLowerCase().includes(kw.toLowerCase())
      : text.includes(kw);
    if (check) {
      found.push(kw);
    }
  }
  return found;
}

/**
 * Check outbound message and log warning if vault task goes to non-vault agent.
 * @param {string} targetAgentId — e.g. "agent-codex", "agent-codex-volt"
 * @param {string} text — message/task plaintext
 * @param {Function} logFn — logging function(level, msg, data)
 * @returns {{ isVaultTask: boolean, allowed: boolean, keywords: string[] }}
 */
export function vaultGuardCheck(targetAgentId, text, logFn) {
  const keywords = findVaultKeywords(text);

  if (keywords.length === 0) {
    return { isVaultTask: false, allowed: true, keywords: [] };
  }

  const allowed = VAULT_AGENTS.has(targetAgentId);

  if (!allowed && logFn) {
    logFn("warn", "VAULT_GUARD: vault task sent to non-vault agent", {
      target: targetAgentId,
      keywords_found: keywords,
      text_preview: text.substring(0, 200),
    });
  }

  return { isVaultTask: true, allowed, keywords };
}

export { VAULT_AGENTS, VAULT_KEYWORDS };
