#!/usr/bin/env python3
from __future__ import annotations

import argparse
import grp
import json
import os
import re
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


SOCKET_PATH = Path(os.environ.get("MURMUR_SEND_SOCKET", "/run/murmur-send-service/codex-volt.sock"))
AUDIT_LOG = Path(os.environ.get("MURMUR_SEND_AUDIT", "/var/log/murmur-send-service.log"))
DATA_DIR = os.environ.get("MURMUR_DATA_DIR", "/opt/lifecoach/mur-mur-v2/.data-codex-volt")
SEND_SCRIPT = os.environ.get("MURMUR_SEND_SCRIPT", "/opt/lifecoach/mur-mur-v2/scripts/murmur-shell-send.mjs")
MURMUR_CWD = os.environ.get("MURMUR_CWD", "/opt/lifecoach/mur-mur-v2")

PEERS = {"agent-jarvis", "agent-codex-mac-kovalyaevo"}
KINDS = {"ack", "progress", "final", "blocked", "error"}
CONVERSATION_RE = re.compile(r"^[a-zA-Z0-9:_./-]{1,160}$")
SECRET_RE = re.compile(
    r"BEGIN PRIVATE KEY|OPENAI_API_KEY=|auth\.json|ghp_|github_pat_|AKIA[0-9A-Z]{16}|xox[abp]-|sk-[A-Za-z0-9_-]{20,}|-----BEGIN",
    re.IGNORECASE,
)
SOCKET_GROUP = os.environ.get("MURMUR_SEND_SOCKET_GROUP", "acpworkers")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def audit(event: dict) -> None:
    AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
    payload = {"ts": utc_now(), **event}
    with AUDIT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def validate_request(payload: dict) -> tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "payload-not-object"
    to = str(payload.get("to") or "")
    conversation_id = str(payload.get("conversation_id") or "")
    text = str(payload.get("text") or "")
    kind = str(payload.get("kind") or "")
    if to not in PEERS:
        return False, "bad-peer"
    if not CONVERSATION_RE.fullmatch(conversation_id):
        return False, "bad-conversation-id"
    if kind not in KINDS:
        return False, "bad-kind"
    encoded = text.encode("utf-8")
    if not encoded or len(encoded) > 16 * 1024:
        return False, "bad-text-size"
    if SECRET_RE.search(text):
        return False, "secret-marker"
    return True, "ok"


def send(payload: dict) -> dict:
    ok, reason = validate_request(payload)
    audit({"event": "request", "ok": ok, "reason": reason, "to": payload.get("to"), "kind": payload.get("kind"), "task_id": payload.get("source_task_id")})
    if not ok:
        return {"ok": False, "error": reason}

    env = os.environ.copy()
    env["DATA_DIR"] = DATA_DIR
    proc = subprocess.run(
        ["node", SEND_SCRIPT, "--to", payload["to"], "--conv", payload["conversation_id"], "--stdin"],
        input=payload["text"],
        text=True,
        capture_output=True,
        cwd=MURMUR_CWD,
        env=env,
        timeout=30,
    )
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        data = {"raw_stdout": proc.stdout[-1000:]}
    result = {"ok": proc.returncode == 0, "returncode": proc.returncode, "send": data, "stderr": proc.stderr[-1000:]}
    audit({"event": "result", "ok": result["ok"], "to": payload.get("to"), "kind": payload.get("kind"), "msg_id": data.get("msgId") if isinstance(data, dict) else None})
    return result


def handle_line(line: bytes) -> bytes:
    try:
        payload = json.loads(line.decode("utf-8"))
        result = send(payload)
    except Exception as exc:
        result = {"ok": False, "error": str(exc)}
    return (json.dumps(result, ensure_ascii=False) + "\n").encode("utf-8")


def serve() -> None:
    SOCKET_PATH.parent.mkdir(parents=True, exist_ok=True)
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(SOCKET_PATH))
        gid = grp.getgrnam(SOCKET_GROUP).gr_gid
        os.chown(SOCKET_PATH, 0, gid)
        os.chmod(SOCKET_PATH, 0o660)
        server.listen(20)
        audit({"event": "started", "socket": str(SOCKET_PATH)})
        while True:
            conn, _addr = server.accept()
            with conn:
                data = b""
                while not data.endswith(b"\n"):
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                    if len(data) > 20 * 1024:
                        break
                conn.sendall(handle_line(data))


def run_self_test() -> int:
    valid = {
        "to": "agent-jarvis",
        "conversation_id": "dm:agent-jarvis:agent-codex-volt",
        "text": "[CODEX->AGENT]\nsource=codex-cli\nproject=/opt/lifecoach/vault\nts=2026-06-20T00:00:00Z\nintent=sync\nsummary=test",
        "kind": "ack",
        "source_task_id": 1,
    }
    cases = [
        ("valid", valid, True),
        ("bad-peer", {**valid, "to": "agent-evil"}, False),
        ("bad-conv", {**valid, "conversation_id": "bad conv"}, False),
        ("bad-kind", {**valid, "kind": "other"}, False),
        ("secret", {**valid, "text": "OPENAI_API_KEY=secret"}, False),
        ("auth-json", {**valid, "text": "auth.json"}, False),
        ("too-large", {**valid, "text": "x" * (17 * 1024)}, False),
    ]
    for name, payload, expected in cases:
        got, reason = validate_request(payload)
        print(f"{name}: got={got} expected={expected} reason={reason}")
        if got != expected:
            return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return run_self_test()
    serve()
    return 0


if __name__ == "__main__":
    sys.exit(main())
