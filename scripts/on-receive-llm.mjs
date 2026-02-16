#!/usr/bin/env node
/**
 * on-receive-llm.mjs — LLM brain for Murmur V2 agent.
 * 
 * When the daemon receives a message, this script:
 * 1. Reads the incoming text from env vars
 * 2. Sends it to an LLM (OpenAI-compatible API)
 * 3. Sends the LLM response back via Murmur
 *
 * Environment (set by daemon):
 *   MURMUR_FROM          — sender agent ID
 *   MURMUR_TEXT          — decrypted message text
 *   MURMUR_MSG_ID        — message ID
 *   MURMUR_CONVERSATION_ID — conversation ID
 *
 * Configuration (set in shell or .env):
 *   LLM_API_KEY          — API key (OpenAI, Anthropic, OpenRouter, etc.)
 *   LLM_BASE_URL         — API base URL (default: https://api.openai.com/v1)
 *   LLM_MODEL            — Model name (default: gpt-4o)
 *   LLM_SYSTEM_PROMPT    — System prompt (optional, has default)
 *   DATA_DIR             — Murmur data dir (default: .data)
 */

import { readFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import https from "node:https";
import http from "node:http";

const execFileAsync = promisify(execFile);

const from = process.env.MURMUR_FROM || "unknown";
const text = process.env.MURMUR_TEXT || "";
const msgId = process.env.MURMUR_MSG_ID || "";
const conversationId = process.env.MURMUR_CONVERSATION_ID || "";

if (!text) {
  console.error("[llm] No text received, skipping");
  process.exit(0);
}

// LLM config
const apiKey = process.env.LLM_API_KEY || process.env.OPENAI_API_KEY || "";
const baseUrl = process.env.LLM_BASE_URL || "https://api.openai.com/v1";
const model = process.env.LLM_MODEL || "gpt-4o";
const systemPrompt = process.env.LLM_SYSTEM_PROMPT || `You are a helpful AI agent participating in a multi-agent mesh network (Murmur V2). You receive messages from other agents and respond thoughtfully. Be concise but helpful. You can discuss technical topics, brainstorm ideas, and collaborate on projects.

Current context:
- You are agent in the Murmur V2 network
- Messages are E2E encrypted (X25519 + XChaCha20-Poly1305)
- The network includes agents working on AI projects`;

if (!apiKey) {
  console.error("[llm] No API key found. Set LLM_API_KEY or OPENAI_API_KEY");
  // Send error reply
  await sendReply(from, `⚠️ LLM not configured — set LLM_API_KEY in environment. Message received but can't process: "${text.slice(0, 100)}..."`);
  process.exit(0);
}

console.log(`[llm] Processing message from ${from}: "${text.slice(0, 80)}..."`);

// Call LLM
try {
  const response = await callLLM(systemPrompt, `Message from ${from}:\n\n${text}`);
  console.log(`[llm] Got response (${response.length} chars)`);
  await sendReply(from, response);
  console.log(`[llm] Reply sent to ${from}`);
} catch (err) {
  console.error(`[llm] Error: ${err.message}`);
  await sendReply(from, `⚠️ LLM error: ${err.message}. Original message received: "${text.slice(0, 100)}..."`);
}

// === Functions ===

async function callLLM(system, user) {
  const url = new URL(`${baseUrl}/chat/completions`);
  const body = JSON.stringify({
    model,
    messages: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
    max_tokens: 2000,
    temperature: 0.7,
  });

  return new Promise((resolve, reject) => {
    const mod = url.protocol === "https:" ? https : http;
    const req = mod.request(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`,
      },
    }, (res) => {
      let data = "";
      res.on("data", (chunk) => data += chunk);
      res.on("end", () => {
        try {
          const json = JSON.parse(data);
          if (json.error) reject(new Error(json.error.message || JSON.stringify(json.error)));
          else resolve(json.choices?.[0]?.message?.content || "Empty response");
        } catch (e) {
          reject(new Error(`Parse error: ${data.slice(0, 200)}`));
        }
      });
    });
    req.on("error", reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error("Timeout 30s")); });
    req.write(body);
    req.end();
  });
}

async function sendReply(targetAgent, message) {
  const scriptDir = path.dirname(new URL(import.meta.url).pathname);
  const sendScript = path.join(scriptDir, "send-task.mjs");
  
  try {
    const { stdout, stderr } = await execFileAsync("node", [sendScript, targetAgent, message], {
      cwd: path.join(scriptDir, ".."),
      timeout: 15000,
      env: { ...process.env, DATA_DIR: process.env.DATA_DIR || ".data" },
    });
    if (stdout) console.log(`[llm] send-task: ${stdout.trim()}`);
    if (stderr) console.error(`[llm] send-task stderr: ${stderr.trim()}`);
  } catch (err) {
    console.error(`[llm] Failed to send reply: ${err.message}`);
  }
}
