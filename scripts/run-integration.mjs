import { spawnSync } from "node:child_process";

const run = (cmd, args, { allowFail = false } = {}) => {
  const res = spawnSync(cmd, args, { stdio: "inherit", env: process.env });
  if (res.status !== 0 && !allowFail) {
    throw new Error(`${cmd} ${args.join(" ")} failed with status ${res.status}`);
  }
  return res.status ?? 1;
};

const hasDocker = spawnSync("docker", ["--version"], { stdio: "ignore" }).status === 0;
if (!hasDocker) {
  console.log("[integration] docker unavailable; skipping integration tests");
  process.exit(0);
}

run("npm", ["run", "demo:up"]);
try {
  run("node", ["--test", "tests/*.integration.mjs"]);
} finally {
  run("npm", ["run", "demo:down"], { allowFail: true });
}
