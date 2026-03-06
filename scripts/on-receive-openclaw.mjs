#!/usr/bin/env node
/**
 * Mur-Mur -> OpenClaw bridge helper.
 *
 * DISABLED 2026-03-06: OpenClaw tool "cron" was removed.
 * Inbound Murmur messages are now handled via local_messages only.
 * This script is a no-op — messages already saved by murmur-daemon onMessage().
 *
 * To re-enable: replace with correct OpenClaw Gateway API endpoint.
 */

const text = process.env.MURMUR_TEXT || "";
const from = process.env.MURMUR_FROM || "unknown";
const msgId = process.env.MURMUR_MSG_ID || "unknown";

console.log(`[openclaw-helper] PASS-THROUGH: from=${from} msgId=${msgId} textLen=${text.length}`);
console.log("[openclaw-helper] Message already in local_messages, no OpenClaw dispatch needed");
process.exit(0);
