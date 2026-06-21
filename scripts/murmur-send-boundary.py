#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_SOCKET_PATH = Path(os.environ.get("MURMUR_SEND_SOCKET", "/run/murmur-send-service/codex-volt.sock"))
DEFAULT_PROJECT = os.environ.get("MURMUR_SEND_PROJECT", "/opt/lifecoach/vault")
DEFAULT_SOURCE = os.environ.get("MURMUR_SEND_SOURCE", "codex-acp-worker")
PEERS = {"agent-jarvis", "agent-codex-mac-kovalyaevo"}
KINDS = {"ack", "progress", "final", "blocked", "error"}
MAX_BODY_BYTES = 14 * 1024
CONVERSATION_RE = re.compile(r"^[a-zA-Z0-9:_./-]{1,160}$")
SECRET_RE = re.compile(
    r"BEGIN PRIVATE KEY|OPENAI_API_KEY=|auth\.json|ghp_|github_pat_|AKIA[0-9A-Z]{16}|xox[abp]-|sk-[A-Za-z0-9_-]{20,}|-----BEGIN",
    re.IGNORECASE,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_body(args: argparse.Namespace) -> str:
    if args.text is not None:
        return args.text
    return sys.stdin.read()


def build_message(*, kind: str, summary: str, body: str, source_task_id: str, project: str, source: str) -> str:
    lines = [
        "[CODEX->AGENT]",
        f"source={source}",
        f"project={project}",
        f"ts={utc_now()}",
        f"intent={kind}",
        f"summary={summary}",
    ]
    if source_task_id:
        lines.append(f"source_task_id={source_task_id}")
    lines.extend(["", "payload:", body.rstrip()])
    return "\n".join(lines).rstrip() + "\n"


def build_payload(args: argparse.Namespace) -> dict:
    body = read_body(args)
    if not body.strip():
        raise ValueError("empty-body")
    if len(body.encode("utf-8")) > MAX_BODY_BYTES:
        raise ValueError("body-too-large")
    if SECRET_RE.search(body):
        raise ValueError("secret-marker")
    if args.to not in PEERS:
        raise ValueError("bad-peer")
    if args.kind not in KINDS:
        raise ValueError("bad-kind")
    if not CONVERSATION_RE.fullmatch(args.conversation_id):
        raise ValueError("bad-conversation-id")
    if not args.summary.strip():
        raise ValueError("empty-summary")
    text = build_message(
        kind=args.kind,
        summary=args.summary.strip(),
        body=body,
        source_task_id=args.source_task_id or "",
        project=args.project,
        source=args.source,
    )
    return {
        "to": args.to,
        "conversation_id": args.conversation_id,
        "kind": args.kind,
        "source_task_id": args.source_task_id,
        "text": text,
    }


def send_to_socket(payload: dict, socket_path: Path, timeout_seconds: float) -> dict:
    line = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout_seconds)
        client.connect(str(socket_path))
        client.sendall(line)
        chunks = []
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
            if b"\n" in chunk:
                break
    raw = b"".join(chunks).decode("utf-8")
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"send-boundary-non-json-response:{raw[-500:]}") from exc
    if not result.get("ok"):
        raise RuntimeError(f"send-boundary-failed:{json.dumps(result, ensure_ascii=False, sort_keys=True)}")
    return result


def run_self_test() -> int:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--to",
            "agent-jarvis",
            "--conversation-id",
            "codex:task:test",
            "--kind",
            "final",
            "--source-task-id",
            "123",
            "--summary",
            "done",
            "--text",
            "proof-pack v0",
            "--dry-run",
        ]
    )
    payload = build_payload(args)
    if payload["to"] != "agent-jarvis" or payload["kind"] != "final":
        return 1
    if "source=codex-acp-worker" not in payload["text"]:
        return 1
    if "payload:\nproof-pack v0" not in payload["text"]:
        return 1
    print("WS5 send-boundary client self-test passed")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send ACP worker replies through the Murmur send boundary.")
    parser.add_argument("--to", default="")
    parser.add_argument("--conversation-id", default="")
    parser.add_argument("--kind", choices=sorted(KINDS))
    parser.add_argument("--source-task-id", default="")
    parser.add_argument("--summary", default="")
    parser.add_argument("--text")
    parser.add_argument("--socket", type=Path, default=DEFAULT_SOCKET_PATH)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.self_test:
        return run_self_test()
    try:
        payload = build_payload(args)
        if args.dry_run:
            print(json.dumps({"ok": True, "dry_run": True, "payload": payload}, ensure_ascii=False, indent=2))
            return 0
        result = send_to_socket(payload, args.socket, args.timeout)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
