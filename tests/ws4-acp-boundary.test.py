#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


producer = load("producer", ROOT / "scripts" / "murmur-to-acp-producer.py")
sender = load("sender", ROOT / "scripts" / "murmur-send-service.py")


def assert_eq(got, expected, label: str) -> None:
    if got != expected:
        raise AssertionError(f"{label}: got={got!r} expected={expected!r}")


def test_producer_filter() -> None:
    cases = [
        ("sync", "agent-jarvis", "intent=sync\nhello", False),
        ("codex-output", "agent-jarvis", "[CODEX->AGENT]\nintent=task", False),
        ("self", "agent-codex-volt", "intent=task\nhello", False),
        ("coord", "agent-jarvis", "JARVIS -> CODEX hello", False),
        ("coord-arrow", "agent-jarvis", "JARVIS → CODEX hello", False),
        ("task", "agent-jarvis", "intent=task\nhello", True),
        ("task-meta", "agent-jarvis", "JARVIS discusses intent=task token", False),
        ("hash", "agent-jarvis", "#ACP\nhello", True),
        ("hash-meta", "agent-jarvis", "JARVIS discusses #ACP token", False),
    ]
    for label, sender_name, text, expected in cases:
        got, _reason = producer.should_process(sender_name, text)
        assert_eq(got, expected, f"producer {label}")


def test_task_packet_contract() -> None:
    body = producer.task_body(
        "agent-jarvis",
        "codex:task:ws4",
        "msg-123",
        "899",
        "2026-06-21T20:42:08Z",
        "intent=task\nWrite result.",
    )
    packet = json.loads(body)
    assert_eq(packet["source"], "murmur", "packet source")
    assert_eq(packet["murmur"]["msg_id"], "msg-123", "packet msg_id")
    assert_eq(packet["reply"]["send_boundary"], "murmur-send-service", "send boundary")
    assert_eq(packet["reply"]["client"], "murmur-send-boundary.py", "send boundary client")
    assert "murmur-send-boundary.py" in packet["reply"]["command"]
    assert "write_proof_pack" in packet["requirements"]
    assert "reply_via_send_boundary" in packet["coverage"]


def test_marker_removed_when_acp_create_fails() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        producer.STATE_ROOT = Path(tmp)
        producer.LOG_PATH = Path(tmp) / "producer.log"
        original = producer.create_or_get_task
        try:
            producer.create_or_get_task = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
            env = {
                "MURMUR_FROM": "agent-jarvis",
                "MURMUR_TEXT": "intent=task\nhello",
                "MURMUR_MSG_ID": "msg-fail",
                "MURMUR_CONVERSATION_ID": "codex:task:ws4",
            }
            import os

            old = {key: os.environ.get(key) for key in env}
            os.environ.update(env)
            try:
                try:
                    producer.process_env()
                except RuntimeError:
                    pass
                else:
                    raise AssertionError("expected RuntimeError")
            finally:
                for key, value in old.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
        finally:
            producer.create_or_get_task = original
        assert not (Path(tmp) / "seen" / "msg-fail").exists()


def test_send_service_validation() -> None:
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
    for label, payload, expected in cases:
        got, _reason = sender.validate_request(payload)
        assert_eq(got, expected, f"sender {label}")


def main() -> int:
    test_producer_filter()
    test_task_packet_contract()
    test_marker_removed_when_acp_create_fails()
    test_send_service_validation()
    print("WS4 ACP boundary tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
