#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ACP = os.environ.get("ACP_BIN", "/opt/lifecoach/agent-control-plane/bin/acp")
PROJECT = os.environ.get("ACP_PROJECT", "/opt/lifecoach/vault")
STATE_ROOT = Path(os.environ.get("MURMUR_TO_ACP_STATE", "/var/lib/murmur-to-acp"))
LOG_PATH = Path(os.environ.get("MURMUR_TO_ACP_LOG", "/opt/lifecoach/logs/murmur-to-acp-producer.log"))
SELF_AGENT = os.environ.get("MURMUR_SELF_AGENT", "agent-codex-volt")
ACP_TIMEOUT_SECONDS = float(os.environ.get("MURMUR_TO_ACP_TIMEOUT_SECONDS", "3"))

TASK_INTENT_RE = re.compile(r"^\s*intent\s*=\s*task\s*$", re.IGNORECASE | re.MULTILINE)
SYNC_INTENT_RE = re.compile(r"^\s*intent\s*=\s*sync\s*$", re.IGNORECASE | re.MULTILINE)
ACP_PREFIX_RE = re.compile(r"^\s*#ACP(?:\s|$)", re.IGNORECASE)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(event: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"ts": utc_now(), **event}
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def should_process(sender: str, text: str) -> tuple[bool, str]:
    if not sender or sender == SELF_AGENT:
        return False, "self-or-empty-sender"
    if "source=codex-cli" in text:
        return False, "codex-cli-output"
    if "source=codex-murmur-autopilot" in text:
        return False, "autopilot-output"
    if "[CODEX AUTOPILOT]" in text or "[CODEX->AGENT]" in text:
        return False, "codex-output-marker"
    if SYNC_INTENT_RE.search(text):
        return False, "coord-sync"
    if TASK_INTENT_RE.search(text) or ACP_PREFIX_RE.search(text):
        return True, "explicit-task"
    return False, "missing-task-intent"


def one_line(text: str, limit: int = 100) -> str:
    for line in (text or "").splitlines():
        line = line.strip()
        if line:
            return line[:limit]
    return "Murmur task"


def task_body(sender: str, conversation_id: str, msg_id: str, rowid: str, created_at: str, text: str) -> str:
    packet = {
        "lane": "auto",
        "task_type": "analysis",
        "source": "murmur",
        "murmur": {
            "from": sender,
            "conversation_id": conversation_id,
            "msg_id": msg_id,
            "rowid": rowid,
            "created_at": created_at,
        },
        "reply": {
            "to": sender,
            "conversation_id": conversation_id,
            "send_boundary": "murmur-send-service",
            "client": "murmur-send-boundary.py",
            "command": (
                "python3 /opt/lifecoach/mur-mur-v2/scripts/murmur-send-boundary.py "
                f"--to {sender} --conversation-id {conversation_id} "
                "--kind final --source-task-id <ACP_TASK_ID> --summary '<summary>'"
            ),
        },
        "prompt": text,
        "requirements": ["create_or_get_task", "write_result", "write_proof_pack", "reply_via_send_boundary"],
        "coverage": ["create_or_get_task", "write_result", "write_proof_pack", "reply_via_send_boundary"],
    }
    return json.dumps(packet, ensure_ascii=False, indent=2)


def create_or_get_task(sender: str, conversation_id: str, msg_id: str, rowid: str, created_at: str, text: str) -> dict:
    title = f"Murmur {sender} {msg_id[:8]}: {one_line(text)}"
    body = task_body(sender, conversation_id, msg_id, rowid, created_at, text)
    proc = subprocess.run(
        [
            ACP,
            "create-or-get",
            "--source",
            "murmur",
            "--external-key",
            msg_id,
            "--title",
            title,
            "--body",
            body,
            "--repo-path",
            PROJECT,
            "--worker-kind",
            "codex-task",
        ],
        text=True,
        capture_output=True,
        timeout=ACP_TIMEOUT_SECONDS,
    )
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"acp create-or-get returned non-json rc={proc.returncode}: {proc.stdout[-500:]} {proc.stderr[-500:]}") from exc
    data["_returncode"] = proc.returncode
    if proc.returncode != 0 or not data.get("ok"):
        raise RuntimeError(f"acp create-or-get failed: {data}")
    return data


def process_env() -> dict:
    sender = os.environ.get("MURMUR_FROM", "")
    text = os.environ.get("MURMUR_TEXT", "")
    msg_id = os.environ.get("MURMUR_MSG_ID", "")
    conversation_id = os.environ.get("MURMUR_CONVERSATION_ID", "")
    rowid = os.environ.get("MURMUR_ROWID", "")
    created_at = os.environ.get("MURMUR_CREATED_AT", utc_now())

    if not msg_id:
        result = {"ok": True, "status": "ignored", "reason": "missing-msg-id"}
        log(result)
        return result

    allowed, reason = should_process(sender, text)
    if not allowed:
        result = {"ok": True, "status": "ignored", "reason": reason, "msg_id": msg_id, "sender": sender}
        log(result)
        return result

    seen_dir = STATE_ROOT / "seen"
    inbound_dir = STATE_ROOT / "inbound"
    seen_dir.mkdir(parents=True, exist_ok=True)
    inbound_dir.mkdir(parents=True, exist_ok=True)
    marker = seen_dir / msg_id
    try:
        marker.mkdir()
    except FileExistsError:
        result = {"ok": True, "status": "duplicate", "msg_id": msg_id}
        log(result)
        return result

    inbound = {
        "sender": sender,
        "conversation_id": conversation_id,
        "msg_id": msg_id,
        "rowid": rowid,
        "created_at": created_at,
        "text": text,
    }
    try:
        (inbound_dir / f"{msg_id}.json").write_text(json.dumps(inbound, ensure_ascii=False, indent=2), encoding="utf-8")
        task = create_or_get_task(sender, conversation_id, msg_id, rowid, created_at, text)
    except Exception:
        shutil.rmtree(marker, ignore_errors=True)
        raise

    result = {"ok": True, "status": "created" if task.get("created") else "existing", "msg_id": msg_id, "task_id": task["task_id"]}
    log(result)
    return result


def run_self_test() -> int:
    cases = [
        ("sync", "agent-jarvis", "intent=sync\nhello", False),
        ("codex", "agent-jarvis", "[CODEX->AGENT]\nintent=task", False),
        ("self", SELF_AGENT, "intent=task\nhello", False),
        ("coord", "agent-jarvis", "JARVIS -> CODEX hello", False),
        ("coord-arrow", "agent-jarvis", "JARVIS → CODEX hello", False),
        ("task", "agent-jarvis", "intent=task\nhello", True),
        ("task-meta", "agent-jarvis", "JARVIS discusses intent=task token", False),
        ("hash", "agent-jarvis", "#ACP\nhello", True),
        ("hash-meta", "agent-jarvis", "JARVIS discusses #ACP token", False),
    ]
    for name, sender, text, expected in cases:
        got, _reason = should_process(sender, text)
        print(f"{name}: got={got} expected={expected}")
        if got != expected:
            return 1

    body = task_body("agent-jarvis", "codex:task:test", "msg-1", "42", "2026-06-21T00:00:00Z", "intent=task\nhello")
    packet = json.loads(body)
    if packet["source"] != "murmur" or packet["murmur"]["msg_id"] != "msg-1":
        return 1
    if "reply_via_send_boundary" not in packet["requirements"]:
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return run_self_test()
    result = process_env()
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
