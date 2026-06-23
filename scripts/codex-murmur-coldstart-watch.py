#!/usr/bin/env python3
import argparse
import fcntl
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_MURMUR_ROOT = os.environ.get("MURMUR_ROOT", os.path.abspath(os.path.join(SCRIPT_DIR, "..")))
DEFAULT_DATA_DIR = os.environ.get("DATA_DIR", os.path.join(DEFAULT_MURMUR_ROOT, ".data"))
DEFAULT_STATE_DIR = os.environ.get(
    "XDG_STATE_HOME",
    os.path.join(os.path.expanduser("~"), ".local", "state"),
)
DEFAULT_DB = os.environ.get("MURMUR_STORE_PATH", os.path.join(DEFAULT_DATA_DIR, "murmur.db"))
DEFAULT_STATE = os.path.join(DEFAULT_STATE_DIR, "codex", "murmur-coldstart-watch.state")
DEFAULT_LOCK = os.path.join(DEFAULT_STATE_DIR, "codex", "murmur-coldstart-watch.lock")
DEFAULT_LOG = os.environ.get(
    "MURMUR_COLDSTART_LOG",
    os.path.join(DEFAULT_STATE_DIR, "codex", "murmur-coldstart-watch.log"),
)
DEFAULT_PROJECT = os.environ.get("CODEX_PROJECT", os.getcwd())
DEFAULT_RESPONDER = os.path.join(DEFAULT_MURMUR_ROOT, "scripts", "codex-murmur-one-shot-responder.mjs")
DEFAULT_CODEX_BIN = os.environ.get("CODEX_BIN", shutil.which("codex") or "codex")
DEFAULT_LEASE_DB = os.environ.get("MURMUR_LEASE_DB", os.path.join(DEFAULT_DATA_DIR, "lease.db"))
DEFAULT_LEASE_SCHEMA = os.path.join(DEFAULT_MURMUR_ROOT, "packages", "core", "lease-schema.sql")

CLAIM_SQL = """
INSERT INTO channel_owner
  (conversation_id, member_slot, owner_session_id, token, epoch, heartbeat_at, ttl_ms)
  VALUES (?, ?, ?, 1, 1, ?, ?)
ON CONFLICT(conversation_id, member_slot) DO UPDATE SET
  owner_session_id = excluded.owner_session_id,
  token            = channel_owner.token + 1,
  epoch            = channel_owner.epoch + (CASE WHEN channel_owner.owner_session_id <> excluded.owner_session_id THEN 1 ELSE 0 END),
  heartbeat_at     = excluded.heartbeat_at,
  ttl_ms           = excluded.ttl_ms
WHERE channel_owner.owner_session_id = excluded.owner_session_id
   OR (excluded.heartbeat_at - channel_owner.heartbeat_at) > channel_owner.ttl_ms
RETURNING owner_session_id AS ownerSessionId, token, epoch
"""

LEASE_DDL_FALLBACK = """
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=10000;
CREATE TABLE IF NOT EXISTS channel_owner (
  conversation_id   TEXT    NOT NULL,
  member_slot       TEXT    NOT NULL,
  owner_session_id  TEXT    NOT NULL,
  token             INTEGER NOT NULL,
  epoch             INTEGER NOT NULL,
  heartbeat_at      INTEGER NOT NULL,
  ttl_ms            INTEGER NOT NULL,
  PRIMARY KEY (conversation_id, member_slot)
);
CREATE TABLE IF NOT EXISTS session_presence (
  session_id    TEXT    PRIMARY KEY,
  agent_id      TEXT    NOT NULL,
  thread_id     TEXT,
  pid           INTEGER,
  mode          TEXT    NOT NULL,
  heartbeat_at  INTEGER NOT NULL,
  started_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_presence_agent ON session_presence(agent_id, mode);
"""

running = True


def stop(_signum, _frame):
    global running
    running = False


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def ensure_parent(path):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def log_event(path, event):
    ensure_parent(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": utc_now(), **event}, ensure_ascii=False, sort_keys=True) + "\n")


def connect_rw(db_path):
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_lease(conn, schema_path):
    if schema_path and os.path.exists(schema_path):
        with open(schema_path, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
    else:
        conn.executescript(LEASE_DDL_FALLBACK)


def lease_now_ms():
    return int(time.time() * 1000)


def register_session(args, session_id, anchor):
    now = lease_now_ms()
    ensure_parent(args.lease_db)
    with connect_rw(args.lease_db) as conn:
        ensure_lease(conn, args.lease_schema)
        conn.execute(
            """
            INSERT INTO session_presence (session_id, agent_id, thread_id, pid, mode, heartbeat_at, started_at)
            VALUES (?, ?, ?, ?, 'coldstart', ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
              thread_id = excluded.thread_id,
              pid = excluded.pid,
              mode = excluded.mode,
              heartbeat_at = excluded.heartbeat_at
            """,
            (session_id, args.agent_id, f"coldstart:{anchor['msg_id']}", os.getpid(), now, now),
        )
        conn.commit()


def get_owner(conn, conversation_id, member_slot):
    row = conn.execute(
        """
        SELECT owner_session_id AS ownerSessionId, token, epoch, heartbeat_at AS heartbeatAt, ttl_ms AS ttlMs
        FROM channel_owner WHERE conversation_id = ? AND member_slot = ?
        """,
        (conversation_id, member_slot),
    ).fetchone()
    if not row:
        return {"ownerSessionId": None, "token": None}
    return {"ownerSessionId": row["ownerSessionId"], "token": int(row["token"])}


def claim_or_skip(args, conversation_id, member_slot, session_id):
    now = lease_now_ms()
    ensure_parent(args.lease_db)
    with connect_rw(args.lease_db) as conn:
        ensure_lease(conn, args.lease_schema)
        row = conn.execute(
            CLAIM_SQL,
            (conversation_id, member_slot, session_id, now, args.lease_ttl_ms),
        ).fetchone()
        conn.commit()
        if row and row["ownerSessionId"] == session_id:
            return {"won": True, "token": int(row["token"]), "ownerSessionId": session_id}
        owner = get_owner(conn, conversation_id, member_slot)
        return {"won": False, **owner}


def is_current_token(args, conversation_id, member_slot, token):
    with connect_ro(args.lease_db) as conn:
        row = conn.execute(
            "SELECT token FROM channel_owner WHERE conversation_id = ? AND member_slot = ?",
            (conversation_id, member_slot),
        ).fetchone()
    return bool(row and int(row["token"]) == int(token))


def connect_ro(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_processed_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS codex_one_shot_processed (
          inbound_msg_id TEXT PRIMARY KEY,
          inbound_rowid INTEGER NOT NULL,
          conversation_id TEXT NOT NULL,
          reply_msg_id TEXT,
          status TEXT NOT NULL DEFAULT 'sent',
          processed_at TEXT NOT NULL,
          updated_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(codex_one_shot_processed)")}
    if "status" not in columns:
        conn.execute("ALTER TABLE codex_one_shot_processed ADD COLUMN status TEXT NOT NULL DEFAULT 'sent'")
    if "updated_at" not in columns:
        conn.execute("ALTER TABLE codex_one_shot_processed ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
    conn.commit()


def read_state(path, db_path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            value = f.read().strip()
        if value:
            return int(value)

    with connect_ro(db_path) as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(rowid), 0) AS max_rowid FROM local_messages WHERE direction='inbound'"
        ).fetchone()
    return int(row["max_rowid"] or 0)


def write_state(path, rowid):
    ensure_parent(path)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(int(rowid)))
    os.replace(tmp, path)


def fetch_new(db_path, after_rowid, limit, sender):
    with connect_ro(db_path) as conn:
        return conn.execute(
            """
            SELECT rowid, id, conversation_id, msg_id, direction, sender, text, created_at
            FROM local_messages
            WHERE direction='inbound'
              AND rowid > ?
              AND sender = ?
            ORDER BY rowid ASC
            LIMIT ?
            """,
            (after_rowid, sender, limit),
        ).fetchall()


def mark_processed(db_path, row, status, reply_msg_id=None):
    now = utc_now()
    with connect_rw(db_path) as conn:
        ensure_processed_table(conn)
        conn.execute(
            """
            INSERT INTO codex_one_shot_processed
              (inbound_msg_id, inbound_rowid, conversation_id, reply_msg_id, status, processed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(inbound_msg_id) DO UPDATE SET
              reply_msg_id=COALESCE(excluded.reply_msg_id, codex_one_shot_processed.reply_msg_id),
              status=excluded.status,
              updated_at=excluded.updated_at
            """,
            (row["msg_id"], row["rowid"], row["conversation_id"], reply_msg_id, status, now, now),
        )
        conn.commit()


def already_processed(db_path, msg_id):
    with connect_ro(db_path) as conn:
        row = conn.execute(
            "SELECT status FROM codex_one_shot_processed WHERE inbound_msg_id = ?",
            (msg_id,),
        ).fetchone()
    return row["status"] if row else None


def has_later_codex_outbound(db_path, row, agent_id):
    with connect_ro(db_path) as conn:
        found = conn.execute(
            """
            SELECT rowid, msg_id, created_at
            FROM local_messages
            WHERE direction='outbound'
              AND sender = ?
              AND conversation_id = ?
              AND rowid > ?
            ORDER BY rowid ASC
            LIMIT 1
            """,
            (agent_id, row["conversation_id"], row["rowid"]),
        ).fetchone()
    return found


def summarize(text, limit=180):
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    return (lines[0] if lines else "")[:limit]


def build_prompt(rows, agent_id):
    anchor = rows[-1]
    payload = "\n\n---\n\n".join(
        f"""Inbound message {index + 1}/{len(rows)}
- local_messages.rowid={row['rowid']}
- msg_id={row['msg_id']}
- sender={row['sender']}
- conversation_id={row['conversation_id']}
- created_at={row['created_at']}

{row['text']}"""
        for index, row in enumerate(rows)
    )
    return f"""You are {agent_id}, invoked by a cold-start Murmur watcher because an inbound message arrived while no persistent interactive Codex session can be assumed alive.

Handle the inbound batch autonomously, then produce the exact reply body that should be sent back to the sender.

Hard rules:
- Do not send Murmur yourself. The watcher will send your final answer through the one-shot responder.
- Do not claim live-session autonomous wake; this is cold-start spawn-on-inbound.
- Do not write to boevoy or restart the shared NATS broker.
- If you need code changes, use the dev-clone -> PR -> JARVIS review path unless the inbound explicitly says otherwise.
- If blocked, state the concrete blocker and the smallest next action.
- Write in Russian unless the inbound explicitly requires another language.
- Keep the final answer concise and operational; no markdown boilerplate unless useful.

Batch metadata:
- messages={len(rows)}
- reply_anchor_rowid={anchor['rowid']}
- reply_anchor_msg_id={anchor['msg_id']}
- conversation_id={anchor['conversation_id']}

Inbound batch:
{payload}
"""


def run_codex(args, rows):
    prompt = build_prompt(rows, args.agent_id)
    with tempfile.TemporaryDirectory(prefix="codex-coldstart.") as work:
        output_file = os.path.join(work, "last-message.txt")
        cmd = [
            args.codex_bin,
            "exec",
            "--cd",
            args.project,
            "--sandbox",
            args.sandbox,
            "--output-last-message",
            output_file,
            "-",
        ]
        env = os.environ.copy()
        env.pop("OPENAI_API_KEY", None)
        proc = subprocess.run(
            cmd,
            input=prompt,
            text=True,
            capture_output=True,
            timeout=args.codex_timeout,
            env=env,
            cwd=args.project,
        )
        if os.path.exists(output_file):
            reply = open(output_file, "r", encoding="utf-8").read().strip()
        else:
            reply = ""
        return {
            "returncode": proc.returncode,
            "stdout_tail": proc.stdout[-4000:],
            "stderr_tail": proc.stderr[-4000:],
            "reply": reply,
        }


def build_failure_reply(row, result):
    detail = (result.get("stderr_tail") or result.get("stdout_tail") or "").strip()[-1200:]
    return f"""[cold-start error]
source=codex-murmur-coldstart-watch
rowid={row['rowid']}
msg_id={row['msg_id']}
status=codex_exec_failed
returncode={result.get('returncode')}

{detail}"""


def send_reply(args, row, reply):
    cmd = [
        "node",
        args.responder,
        "--db",
        args.db,
        "--rowid",
        str(row["rowid"]),
        "--sender",
        args.sender,
        "--recipient",
        args.recipient,
        "--agent-id",
        args.agent_id,
        "--project",
        args.project,
        "--send-mode",
        args.send_mode,
        "--reply-text",
        reply,
    ]
    if args.murmur_root:
        cmd.extend(["--murmur-root", args.murmur_root])
    cmd.extend(
        [
            "--lease-db",
            args.lease_db,
            "--lease-member-slot",
            args.member_slot,
            "--lease-token",
            str(args.owner_token),
        ]
    )
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=args.send_timeout, cwd=args.project)
    return {
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-2000:],
        "stderr_tail": proc.stderr[-2000:],
    }


def mark_superseded(db_path, rows, anchor):
    for row in rows[:-1]:
        if row["msg_id"] != anchor["msg_id"] and not already_processed(db_path, row["msg_id"]):
            mark_processed(db_path, row, "superseded_by_batch", anchor["msg_id"])


def process_batch(args, rows):
    anchor = rows[-1]
    first = rows[0]
    status = already_processed(args.db, anchor["msg_id"])
    if status:
        return {"event": "skip_already_processed", "status": status}

    if args.settle_seconds > 0:
        time.sleep(args.settle_seconds)
        if not running:
            return {"event": "stopped_during_settle"}

    if args.dry_run:
        return {"event": "dry_run", "messages": len(rows), "summary": summarize(anchor["text"])}

    existing = has_later_codex_outbound(args.db, first, args.agent_id)
    if existing:
        for row in rows:
            mark_processed(args.db, row, "skipped_existing_outbound", existing["msg_id"])
        return {"event": "skip_existing_outbound", "outbound_rowid": existing["rowid"], "outbound_msg_id": existing["msg_id"]}

    session_id = f"coldstart:{os.getpid()}:{anchor['msg_id']}"
    register_session(args, session_id, anchor)
    claim = claim_or_skip(args, anchor["conversation_id"], args.member_slot, session_id)
    if not claim["won"]:
        return {
            "event": "skip_non_owner",
            "owner_session_id": claim.get("ownerSessionId"),
            "owner_token": claim.get("token"),
            "session_id": session_id,
        }
    args.owner_token = claim["token"]

    codex_result = run_codex(args, rows)
    reply = codex_result["reply"]
    if codex_result["returncode"] != 0 or not reply:
        reply = build_failure_reply(anchor, codex_result)

    existing = has_later_codex_outbound(args.db, first, args.agent_id)
    if existing:
        for row in rows:
            mark_processed(args.db, row, "skipped_existing_outbound", existing["msg_id"])
        return {"event": "skip_existing_outbound_after_codex", "outbound_rowid": existing["rowid"], "outbound_msg_id": existing["msg_id"]}

    if not is_current_token(args, anchor["conversation_id"], args.member_slot, args.owner_token):
        return {
            "event": "skip_stale_token_before_send",
            "owner_token": args.owner_token,
            "session_id": session_id,
        }

    send_result = send_reply(args, anchor, reply)
    if send_result["returncode"] == 0:
        mark_superseded(args.db, rows, anchor)
        return {
            "event": "sent",
            "messages": len(rows),
            "anchor_rowid": anchor["rowid"],
            "codex_returncode": codex_result["returncode"],
            "send": send_result["stdout_tail"].strip()[:500],
        }
    return {
        "event": "send_failed",
        "codex_returncode": codex_result["returncode"],
        "send_returncode": send_result["returncode"],
        "stderr": send_result["stderr_tail"],
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Cold-start Codex one-shot responder for Murmur inbound rows.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--lock", default=DEFAULT_LOCK)
    parser.add_argument("--log", default=DEFAULT_LOG)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--responder", default=DEFAULT_RESPONDER)
    parser.add_argument("--codex-bin", default=DEFAULT_CODEX_BIN)
    parser.add_argument("--murmur-root", default=DEFAULT_MURMUR_ROOT)
    parser.add_argument("--lease-db", default=DEFAULT_LEASE_DB)
    parser.add_argument("--lease-schema", default=DEFAULT_LEASE_SCHEMA)
    parser.add_argument("--lease-ttl-ms", type=int, default=20000)
    parser.add_argument("--sender", default="agent-jarvis")
    parser.add_argument("--recipient", default="agent-jarvis")
    parser.add_argument("--agent-id", default="agent-codex-volt")
    parser.add_argument("--member-slot", default="agent-codex-volt")
    parser.add_argument("--send-mode", choices=["print", "append-local", "murmur"], default="murmur")
    parser.add_argument("--sandbox", default="danger-full-access")
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--settle-seconds", type=float, default=30.0)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--codex-timeout", type=int, default=1800)
    parser.add_argument("--send-timeout", type=int, default=60)
    parser.add_argument("--catch-up", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--set-last-rowid", type=int)
    return parser.parse_args()


def main():
    args = parse_args()
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    ensure_parent(args.log)
    ensure_parent(args.lock)
    with connect_rw(args.db) as conn:
        ensure_processed_table(conn)

    if args.set_last_rowid is not None:
        write_state(args.state, args.set_last_rowid)
        log_event(args.log, {"event": "set_last_rowid", "rowid": args.set_last_rowid})
        return 0

    last_rowid = 0 if args.catch_up else read_state(args.state, args.db)
    write_state(args.state, last_rowid)
    log_event(args.log, {
        "event": "coldstart_watcher_started",
        "db": args.db,
        "state": args.state,
        "last_rowid": last_rowid,
        "interval": args.interval,
        "settle_seconds": args.settle_seconds,
        "send_mode": args.send_mode,
        "dry_run": args.dry_run,
    })

    with open(args.lock, "w", encoding="utf-8") as lock_file:
        while running:
            try:
                rows = fetch_new(args.db, last_rowid, args.limit, args.sender)
                if rows and running:
                    first = rows[0]
                    anchor = rows[-1]
                    event = {
                        "event": "inbound_batch_seen",
                        "messages": len(rows),
                        "first_rowid": int(first["rowid"]),
                        "anchor_rowid": int(anchor["rowid"]),
                        "anchor_msg_id": anchor["msg_id"],
                        "conversation_id": anchor["conversation_id"],
                        "summary": summarize(anchor["text"]),
                    }
                    log_event(args.log, event)
                    print(json.dumps({"ts": utc_now(), **event}, ensure_ascii=False, sort_keys=True), flush=True)

                    last_rowid = int(anchor["rowid"])
                    write_state(args.state, last_rowid)

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                    try:
                        result = process_batch(args, rows)
                        log_event(args.log, {"anchor_rowid": int(anchor["rowid"]), "anchor_msg_id": anchor["msg_id"], **result})
                        print(json.dumps({"ts": utc_now(), "anchor_rowid": int(anchor["rowid"]), "anchor_msg_id": anchor["msg_id"], **result}, ensure_ascii=False, sort_keys=True), flush=True)
                    finally:
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            except Exception as exc:
                log_event(args.log, {"event": "watcher_error", "error": repr(exc)})
                print(f"codex-murmur-coldstart-watch error: {exc}", file=sys.stderr, flush=True)
            if args.once:
                break
            time.sleep(args.interval)

    log_event(args.log, {"event": "coldstart_watcher_stopped", "last_rowid": last_rowid})
    return 0


if __name__ == "__main__":
    sys.exit(main())
