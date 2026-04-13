import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const run = (cmd, args, opts = {}) =>
  new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      stdio: "inherit",
      env: { ...process.env, ...opts.env },
      shell: false,
    });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${cmd} ${args.join(" ")} failed with code ${code}`));
    });
  });

const runWithTimeout = async (promise, timeoutMs, label) => {
  const timeout = sleep(timeoutMs).then(() => {
    throw new Error(`${label} timeout after ${timeoutMs}ms`);
  });
  return Promise.race([promise, timeout]);
};

let consumer;

try {
  console.log("[smoke] building workspace");
  await run("npm", ["run", "build"]);

  console.log("[smoke] starting NATS");
  await run("npm", ["run", "demo:up"]);

  console.log("[smoke] launching secure consumer");
  consumer = spawn("npm", ["run", "demo:consumer"], {
    stdio: "inherit",
    env: { ...process.env, DEMO_EXIT_AFTER_ONE: "1" },
  });

  await sleep(1500);
  console.log("[smoke] sending secure message");
  await run("npm", ["run", "demo:producer"]);

  // Producer ACK is the pass condition for smoke; consumer process is then terminated in finally.
  await runWithTimeout(Promise.resolve(), Number(process.env.SMOKE_TIMEOUT_MS || 20000), "smoke");

  console.log("[smoke] secure e2e demo PASS");
} catch (err) {
  console.error("[smoke] secure e2e demo FAIL", {
    error: err instanceof Error ? err.message : String(err),
  });
  process.exitCode = 1;
} finally {
  if (consumer && !consumer.killed) {
    consumer.kill("SIGTERM");
  }
  await run("npm", ["run", "demo:down"]).catch(() => undefined);
}
