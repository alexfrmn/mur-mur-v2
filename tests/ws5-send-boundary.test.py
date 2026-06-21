#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import contextlib
import io
import json
import socket
import tempfile
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


client = load("send_boundary", ROOT / "scripts" / "murmur-send-boundary.py")


def parser_args(*extra: str):
    parser = client.build_parser()
    return parser.parse_args(
        [
            "--to",
            "agent-jarvis",
            "--conversation-id",
            "codex:task:ws5",
            "--kind",
            "final",
            "--source-task-id",
            "321",
            "--summary",
            "ws5 done",
            "--text",
            "proof-pack v0",
            *extra,
        ]
    )


def assert_in(needle: str, haystack: str, label: str) -> None:
    if needle not in haystack:
        raise AssertionError(f"{label}: missing {needle!r}")


def test_payload_envelope() -> None:
    payload = client.build_payload(parser_args())
    assert payload["to"] == "agent-jarvis"
    assert payload["kind"] == "final"
    assert payload["source_task_id"] == "321"
    assert_in("[CODEX->AGENT]", payload["text"], "envelope")
    assert_in("source=codex-acp-worker", payload["text"], "source")
    assert_in("project=/opt/lifecoach/vault", payload["text"], "project")
    assert_in("intent=final", payload["text"], "intent")
    assert_in("summary=ws5 done", payload["text"], "summary")
    assert_in("payload:\nproof-pack v0", payload["text"], "body")


def test_client_validation() -> None:
    for extra, expected in [
        (("--to", "agent-evil"), "bad-peer"),
        (("--kind", "other"), None),
        (("--conversation-id", "bad conv"), "bad-conversation-id"),
        (("--summary", " "), "empty-summary"),
        (("--text", " "), "empty-body"),
        (("--text", "OPENAI_API_KEY=secret"), "secret-marker"),
    ]:
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                args = parser_args(*extra)
            client.build_payload(args)
        except SystemExit:
            if expected is None:
                continue
            raise
        except ValueError as exc:
            if expected is None or str(exc) != expected:
                raise AssertionError(f"expected {expected}, got {exc}") from exc
        else:
            raise AssertionError(f"expected validation failure {expected}")


def test_socket_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        sock_path = Path(tmp) / "boundary.sock"
        received: list[dict] = []

        def server() -> None:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as srv:
                srv.bind(str(sock_path))
                srv.listen(1)
                conn, _ = srv.accept()
                with conn:
                    data = b""
                    while not data.endswith(b"\n"):
                        data += conn.recv(65536)
                    received.append(json.loads(data.decode("utf-8")))
                    conn.sendall(json.dumps({"ok": True, "send": {"msgId": "msg-ws5"}}).encode("utf-8") + b"\n")

        thread = threading.Thread(target=server, daemon=True)
        thread.start()
        deadline = time.time() + 2
        while not sock_path.exists() and time.time() < deadline:
            time.sleep(0.01)
        if not sock_path.exists():
            raise AssertionError("socket server did not bind")
        result = client.send_to_socket(client.build_payload(parser_args()), sock_path, 2)
        thread.join(timeout=2)

        if thread.is_alive():
            raise AssertionError("socket server did not exit")
        if result["send"]["msgId"] != "msg-ws5":
            raise AssertionError(f"bad result {result}")
        if received[0]["conversation_id"] != "codex:task:ws5":
            raise AssertionError(f"bad request {received[0]}")


def main() -> int:
    test_payload_envelope()
    test_client_validation()
    test_socket_roundtrip()
    print("WS5 send-boundary tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
