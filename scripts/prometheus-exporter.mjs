#!/usr/bin/env node
import path from "node:path";
import { startPrometheusExporter } from "../packages/observability/dist/src/index.js";

const dataDir = process.env.DATA_DIR || ".data";
const dbPath = process.env.METRICS_DB_PATH || path.join(dataDir, "murmur.db");
const host = process.env.METRICS_HOST || "127.0.0.1";
const port = Number(process.env.METRICS_PORT || 9464);

const server = startPrometheusExporter({ dbPath, host, port });

server.on("listening", () => {
  console.log(JSON.stringify({
    level: "info",
    msg: "Prometheus exporter listening",
    host,
    port,
    dbPath,
    metricsPath: "/metrics",
    ts: new Date().toISOString(),
  }));
});

server.on("error", (error) => {
  console.error(JSON.stringify({
    level: "fatal",
    msg: "Prometheus exporter failed",
    error: error.message,
    ts: new Date().toISOString(),
  }));
  process.exit(1);
});
