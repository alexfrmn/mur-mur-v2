#!/usr/bin/env node
/**
 * sasha-task.mjs — Send a task directly to LLM as agent-sasha, 
 * then deliver the response back to agent-jarvis via Murmur.
 * 
 * This bypasses encryption entirely — it's a local proxy.
 * Usage: node scripts/sasha-task.mjs "task text"
 */
import https from "node:https";
import http from "node:http";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { readFile } from "node:fs/promises";

const execFileAsync = promisify(execFile);
const task = process.argv.slice(2).join(" ");

if (!task) {
  console.error("Usage: node scripts/sasha-task.mjs <task>");
  process.exit(1);
}

// LLM config — same as on-receive-llm.mjs
const apiKey = process.env.LLM_API_KEY || process.env.OPENAI_API_KEY || "";
const baseUrl = process.env.LLM_BASE_URL || "https://openrouter.ai/api/v1";
const model = process.env.LLM_MODEL || "anthropic/claude-3.5-sonnet";

if (!apiKey) {
  console.error("[sasha-task] No LLM_API_KEY set");
  process.exit(1);
}

const systemPrompt = `You are agent-sasha, an AI agent in the Murmur V2 mesh network. You work alongside agent-jarvis (the lead agent) and other agents on technical projects.

Your capabilities:
- Write clean, production-ready code
- Create web applications (HTML/CSS/JS)
- Design system architectures
- Research and analyze technical topics

When asked to create code or HTML files, output the COMPLETE file content. No shortcuts, no "..." placeholders.
Be thorough and detailed in your responses.`;

console.log(`[sasha-task] Sending task to LLM (${model})...`);
console.log(`[sasha-task] Task: ${task.slice(0, 100)}...`);

try {
  const response = await callLLM(systemPrompt, task);
  console.log(`[sasha-task] Got response: ${response.length} chars`);
  
  // Send response back via Murmur (as agent-jarvis sending to self with sasha's response)
  const replyText = `[agent-sasha LLM response]\n\n${response}`;
  
  // Inject into OpenClaw via bridge
  const config = JSON.parse(await readFile(".data/agent-config.json", "utf8"));
  const gatewayToken = config.gatewayToken || "";
  
  if (gatewayToken) {
    const body = JSON.stringify({
      tool: "cron",
      args: { action: "wake", text: `📨 [agent-sasha] ${response.slice(0, 3000)}`, mode: "now" },
    });
    
    const url = new URL("/tools/invoke", "http://127.0.0.1:18789");
    await new Promise((resolve, reject) => {
      const req = http.request(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Bearer ${gatewayToken}` },
      }, (res) => {
        let data = "";
        res.on("data", c => data += c);
        res.on("end", () => { console.log(`[sasha-task] Bridge: ${res.statusCode} ${data.slice(0,100)}`); resolve(); });
      });
      req.on("error", reject);
      req.write(body);
      req.end();
    });
  }
  
  // Also save response to file for retrieval
  const outPath = `.data/sasha-last-response.txt`;
  const { writeFile } = await import("node:fs/promises");
  await writeFile(outPath, response);
  console.log(`[sasha-task] Response saved to ${outPath}`);
  
} catch (err) {
  console.error(`[sasha-task] Error: ${err.message}`);
  process.exit(1);
}

async function callLLM(system, user) {
  const url = new URL(`${baseUrl}/chat/completions`);
  const body = JSON.stringify({
    model,
    messages: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
    max_tokens: 16000,
    temperature: 0.7,
  });

  return new Promise((resolve, reject) => {
    const mod = url.protocol === "https:" ? https : http;
    const req = mod.request(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${apiKey}` },
    }, (res) => {
      let data = "";
      res.on("data", c => data += c);
      res.on("end", () => {
        try {
          const json = JSON.parse(data);
          if (json.error) reject(new Error(json.error.message || JSON.stringify(json.error)));
          else resolve(json.choices?.[0]?.message?.content || "Empty response");
        } catch (e) { reject(new Error(`Parse error: ${data.slice(0, 200)}`)); }
      });
    });
    req.on("error", reject);
    req.setTimeout(120000, () => { req.destroy(); reject(new Error("Timeout 120s")); });
    req.write(body);
    req.end();
  });
}
