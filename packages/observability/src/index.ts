import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import path from "node:path";
import Database from "better-sqlite3";

export interface PrometheusExporterOptions {
  dbPath?: string;
  host?: string;
  port?: number;
  now?: () => number;
}

export interface PrometheusSnapshot {
  generatedAt: string;
  outboxDepth: Record<string, number>;
  outboxOldestPendingAgeSeconds: number;
  localMessagesTotal: Record<string, number>;
  localMessagesLastHour: Record<string, number>;
  ackLatencyAvgSeconds: number;
  ackLatencyP95Seconds: number;
  errorRowsLastHour: number;
  retryRows: number;
  deadLetterRows: number;
}

const escapeLabel = (value: string): string => value.replaceAll("\\", "\\\\").replaceAll('"', '\\"').replaceAll("\n", "\\n");

const metricLine = (name: string, value: number, labels?: Record<string, string>): string => {
  if (!labels || Object.keys(labels).length === 0) return `${name} ${Number.isFinite(value) ? value : 0}`;
  const encoded = Object.entries(labels)
    .map(([key, label]) => `${key}="${escapeLabel(label)}"`)
    .join(",");
  return `${name}{${encoded}} ${Number.isFinite(value) ? value : 0}`;
};

const rowCount = (db: Database.Database, sql: string, params: unknown[] = []): number => {
  const row = db.prepare(sql).get(...params) as { count?: number } | undefined;
  return Number(row?.count ?? 0);
};

const percentile = (sortedValues: number[], p: number): number => {
  if (sortedValues.length === 0) return 0;
  const index = Math.min(sortedValues.length - 1, Math.max(0, Math.ceil(sortedValues.length * p) - 1));
  return sortedValues[index] ?? 0;
};

export const collectPrometheusSnapshot = (dbPath = ".data/murmur.db", now = () => Date.now()): PrometheusSnapshot => {
  const resolvedDbPath = path.resolve(dbPath);
  const db = new Database(resolvedDbPath, { readonly: true });
  try {
    const outboxRows = db.prepare(`SELECT status, COUNT(*) as count FROM outbox GROUP BY status`).all() as Array<{ status: string; count: number }>;
    const outboxDepth = Object.fromEntries(outboxRows.map((row) => [row.status, Number(row.count)]));

    const oldestPending = db.prepare(`
      SELECT MIN(created_at) AS created_at
      FROM outbox
      WHERE status IN ('pending', 'failed', 'sent')
    `).get() as { created_at?: string | null } | undefined;
    const oldestPendingAgeSeconds = oldestPending?.created_at
      ? Math.max(0, Math.round((now() - Date.parse(oldestPending.created_at)) / 1000))
      : 0;

    const messageTotals = db.prepare(`SELECT direction, COUNT(*) as count FROM local_messages GROUP BY direction`).all() as Array<{ direction: string; count: number }>;
    const localMessagesTotal = Object.fromEntries(messageTotals.map((row) => [row.direction, Number(row.count)]));

    const hourAgoIso = new Date(now() - 60 * 60 * 1000).toISOString();
    const messagesLastHour = db.prepare(`
      SELECT direction, COUNT(*) as count
      FROM local_messages
      WHERE created_at >= ?
      GROUP BY direction
    `).all(hourAgoIso) as Array<{ direction: string; count: number }>;
    const localMessagesLastHour = Object.fromEntries(messagesLastHour.map((row) => [row.direction, Number(row.count)]));

    const ackLatencyRows = db.prepare(`
      SELECT (julianday(updated_at) - julianday(created_at)) * 86400.0 AS latency_seconds
      FROM outbox
      WHERE status = 'acked'
        AND created_at IS NOT NULL
        AND updated_at IS NOT NULL
      ORDER BY latency_seconds ASC
    `).all() as Array<{ latency_seconds: number | null }>;
    const ackLatencies = ackLatencyRows
      .map((row) => Number(row.latency_seconds ?? 0))
      .filter((value) => Number.isFinite(value) && value >= 0)
      .sort((a, b) => a - b);
    const ackLatencyAvgSeconds = ackLatencies.length > 0
      ? ackLatencies.reduce((sum, value) => sum + value, 0) / ackLatencies.length
      : 0;
    const ackLatencyP95Seconds = percentile(ackLatencies, 0.95);

    const errorRowsLastHour = rowCount(
      db,
      `SELECT COUNT(*) as count FROM outbox WHERE status IN ('failed', 'dlq') AND updated_at >= ?`,
      [hourAgoIso],
    );
    const retryRows = rowCount(db, `SELECT COUNT(*) as count FROM outbox WHERE status = 'failed'`);
    const deadLetterRows = rowCount(db, `SELECT COUNT(*) as count FROM outbox WHERE status = 'dlq'`);

    return {
      generatedAt: new Date(now()).toISOString(),
      outboxDepth,
      outboxOldestPendingAgeSeconds: oldestPendingAgeSeconds,
      localMessagesTotal,
      localMessagesLastHour,
      ackLatencyAvgSeconds,
      ackLatencyP95Seconds,
      errorRowsLastHour,
      retryRows,
      deadLetterRows,
    };
  } finally {
    db.close();
  }
};

export const renderPrometheusMetrics = (snapshot: PrometheusSnapshot): string => {
  const lines: string[] = [
    "# HELP murmur_exporter_up Murmur Prometheus exporter health status.",
    "# TYPE murmur_exporter_up gauge",
    "murmur_exporter_up 1",
    "# HELP murmur_outbox_depth Current outbox rows by status.",
    "# TYPE murmur_outbox_depth gauge",
  ];

  for (const status of ["pending", "failed", "sent", "acked", "dlq"]) {
    lines.push(metricLine("murmur_outbox_depth", snapshot.outboxDepth[status] ?? 0, { status }));
  }

  lines.push(
    "# HELP murmur_outbox_oldest_pending_age_seconds Age of the oldest not-yet-acked outbox row.",
    "# TYPE murmur_outbox_oldest_pending_age_seconds gauge",
    metricLine("murmur_outbox_oldest_pending_age_seconds", snapshot.outboxOldestPendingAgeSeconds),
    "# HELP murmur_local_messages_total Stored local messages by direction.",
    "# TYPE murmur_local_messages_total gauge",
  );

  for (const direction of ["inbound", "outbound"]) {
    lines.push(metricLine("murmur_local_messages_total", snapshot.localMessagesTotal[direction] ?? 0, { direction }));
  }

  lines.push(
    "# HELP murmur_local_messages_last_hour Stored local messages in the last hour by direction.",
    "# TYPE murmur_local_messages_last_hour gauge",
  );

  for (const direction of ["inbound", "outbound"]) {
    lines.push(metricLine("murmur_local_messages_last_hour", snapshot.localMessagesLastHour[direction] ?? 0, { direction }));
  }

  lines.push(
    "# HELP murmur_ack_latency_avg_seconds Average time from enqueue to ack for acked messages.",
    "# TYPE murmur_ack_latency_avg_seconds gauge",
    metricLine("murmur_ack_latency_avg_seconds", snapshot.ackLatencyAvgSeconds),
    "# HELP murmur_ack_latency_p95_seconds P95 time from enqueue to ack for acked messages.",
    "# TYPE murmur_ack_latency_p95_seconds gauge",
    metricLine("murmur_ack_latency_p95_seconds", snapshot.ackLatencyP95Seconds),
    "# HELP murmur_outbox_errors_last_hour Failed or DLQ outbox transitions in the last hour.",
    "# TYPE murmur_outbox_errors_last_hour gauge",
    metricLine("murmur_outbox_errors_last_hour", snapshot.errorRowsLastHour),
    "# HELP murmur_outbox_retry_rows Current outbox rows waiting for retry.",
    "# TYPE murmur_outbox_retry_rows gauge",
    metricLine("murmur_outbox_retry_rows", snapshot.retryRows),
    "# HELP murmur_outbox_dead_letter_rows Current outbox rows in dead-letter queue.",
    "# TYPE murmur_outbox_dead_letter_rows gauge",
    metricLine("murmur_outbox_dead_letter_rows", snapshot.deadLetterRows),
    "# HELP murmur_metrics_generated_unixtime Unix timestamp when metrics were generated.",
    "# TYPE murmur_metrics_generated_unixtime gauge",
    metricLine("murmur_metrics_generated_unixtime", Math.floor(Date.parse(snapshot.generatedAt) / 1000)),
  );

  return `${lines.join("\n")}\n`;
};

export const createPrometheusHandler = (options: PrometheusExporterOptions = {}) => {
  const dbPath = options.dbPath ?? ".data/murmur.db";
  const now = options.now ?? (() => Date.now());

  return (_req: IncomingMessage, res: ServerResponse) => {
    try {
      const snapshot = collectPrometheusSnapshot(dbPath, now);
      const body = renderPrometheusMetrics(snapshot);
      res.writeHead(200, {
        "Content-Type": "text/plain; version=0.0.4; charset=utf-8",
        "Cache-Control": "no-store",
      });
      res.end(body);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
      res.end(`# exporter error\nmurmur_exporter_up 0\n# error ${message}\n`);
    }
  };
};

export const startPrometheusExporter = (options: PrometheusExporterOptions = {}) => {
  const host = options.host ?? process.env.METRICS_HOST ?? "127.0.0.1";
  const port = options.port ?? Number(process.env.METRICS_PORT ?? 9464);
  const server = createServer((req, res) => {
    if (!req.url || req.url === "/metrics") {
      return createPrometheusHandler(options)(req, res);
    }
    res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("Not found\n");
  });

  server.listen(port, host);
  return server;
};
