#!/usr/bin/env python3
"""Exercise Antigravity wakes through the notifier, ConversationService, and a stubbed agentapi CLI."""

from __future__ import annotations

import json
import os
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from buslib.adapters.antigravity import AntigravityAdapter, normalize_ls_address
from buslib.adapters.base import AdapterError

HERE = Path(__file__).resolve().parent
CONVERSATION = "d943d26f-c0ac-40a7-af9a-75b59338a3c7"
SECOND_CONVERSATION = "11111111-1111-1111-1111-111111111111"
MISSING_CONVERSATION = "00000000-0000-0000-0000-000000000000"
CSRF_SECRET = "probe-csrf-secret-value"
PRIVATE_TEXT = "private payload with shell syntax: $(must-not-run)"

STUB = """
import json
import os
import sys

args = sys.argv[1:]
if args == ['--help']:
    print('Usage: agentapi <command> [args]')
    print('  get-conversation-metadata <conversation_id>')
    print('  send-message [--title=<title>] <recipient_id> <content>')
    sys.exit(0)
if args[:1] == ['get-conversation-metadata']:
    conv = args[1] if len(args) > 1 else ''
    if conv == os.environ.get('AGY_PROBE_VALID_CONV'):
        print(json.dumps({'response': {'conversationMetadata': {'metadata': {'rootConversationId': conv}}}}))
        sys.exit(0)
    print(json.dumps({'response': {}, 'error': f'rpc error: code = NotFound desc = not_found: {conv}'}))
    sys.exit(1)
record = {
    'args': args,
    'ls_address': os.environ.get('ANTIGRAVITY_LS_ADDRESS'),
    'csrf_present': os.environ.get('ANTIGRAVITY_CSRF_TOKEN') == os.environ.get('AGY_PROBE_CSRF'),
    'stripped_conv': 'ANTIGRAVITY_CONVERSATION_ID' not in os.environ,
    'stripped_traj': 'ANTIGRAVITY_TRAJECTORY_ID' not in os.environ,
    'stripped_meta': 'ANTIGRAVITY_SOURCE_METADATA' not in os.environ,
}
with open(os.environ['AGY_PROBE_CALLS'], 'a', encoding='utf-8') as log:
    log.write(json.dumps(record) + '\\n')
print(json.dumps({'response': {'sendMessage': {'recipientId': args[-2] if len(args) >= 2 else ''}}}))
"""


class FakeConversationServer:
    def __init__(self) -> None:
        self.mode = "idle"
        self.requests: list[dict] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or "0")
                raw = self.rfile.read(length).decode("utf-8", errors="replace")
                body = json.loads(raw) if raw else {}
                token = self.headers.get("x-antigravity-csrf-token", "")
                outer.requests.append({"path": self.path, "body": body, "csrf_ok": token == CSRF_SECRET})
                if self.path != "/product.v1.ConversationService/GetConversation":
                    self._send(404, {"code": "unimplemented", "message": "unknown path"})
                    return
                if token != CSRF_SECRET:
                    self._send(401, {"code": "unauthenticated", "message": "invalid CSRF token"})
                    return
                if outer.mode == "unimplemented":
                    self._send(404, {"code": "unimplemented", "message": "method not implemented"})
                    return
                conv_id = body.get("conversationId")
                if conv_id not in {CONVERSATION, SECOND_CONVERSATION} or outer.mode == "not_found":
                    self._send(404, {"code": "not_found", "message": f"conversation {conv_id} not found"})
                    return
                state_map = {
                    "idle": "RUN_STATE_IDLE",
                    "running": "RUN_STATE_RUNNING",
                    "cancelling": "RUN_STATE_CANCELLING",
                }
                self._send(
                    200,
                    {
                        "id": conv_id,
                        "metadata": {"projectId": "proj-1", "rootConversationId": conv_id},
                        "annotations": {"title": "probe-conv", "archived": False},
                        "state": {
                            "summary": "probe",
                            "runState": state_map.get(outer.mode, "RUN_STATE_IDLE"),
                            "updateTime": "2026-11-28T00:00:00Z",
                        },
                    },
                )

            def _send(self, status: int, payload: dict) -> None:
                data = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.address = f"127.0.0.1:{self.httpd.server_address[1]}"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self) -> FakeConversationServer:
        self.thread.start()
        return self

    def __exit__(self, *_) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def probe_adapter_unit(server: FakeConversationServer) -> None:
    assert normalize_ls_address(f"http://{server.address}/") == server.address
    try:
        normalize_ls_address("https://localhost:1234/evil")
    except SystemExit as exc:
        assert "invalid Antigravity language server address" in str(exc)
    else:
        raise AssertionError("expected invalid address rejection")

    cli_ok = subprocess.CompletedProcess([], 0, "send-message\nget-conversation-metadata", "")
    ask_msg = {
        "to_agent": "receiver",
        "from_agent": "sender",
        "thread_id": "thr-1",
        "message_id": "msg-ask-1",
        "message_type": "ask",
        "payload": json.dumps({"text": PRIVATE_TEXT}),
    }
    sys_msg = {
        "to_agent": "receiver",
        "from_agent": "system",
        "thread_id": "thr-1",
        "message_id": "msg-sys-1",
        "message_type": "system",
        "payload": json.dumps({"text": PRIVATE_TEXT}),
    }
    digest_msg = {
        "to_agent": "receiver",
        "from_agent": "team-room",
        "thread_id": "team-room",
        "message_id": "group-digest-team-room-5-receiver",
        "message_type": "system",
        "payload": json.dumps({"text": PRIVATE_TEXT}),
    }

    with patch.dict(os.environ, {"ANTIGRAVITY_CSRF_TOKEN": CSRF_SECRET}, clear=False):
        with patch.object(AntigravityAdapter, "_run", return_value=cli_ok) as cli:
            adapter = AntigravityAdapter(CONVERSATION, sys.executable, str(HERE), server.address)

            # Idle + running both deliver pointer prompts without leaking payload text.
            for mode, msg in (("idle", ask_msg), ("running", sys_msg), ("idle", digest_msg)):
                cli.reset_mock()
                server.mode = mode
                adapter.inject(msg)
                assert cli.call_count == 1, cli.call_args_list
                call_args = cli.call_args[0][0]
                assert call_args[0] == "send-message" and call_args[2] == CONVERSATION, call_args
                prompt = call_args[3]
                assert msg["message_id"] in prompt and PRIVATE_TEXT not in prompt, prompt
                if msg is ask_msg:
                    assert "answer" in prompt and "--answer-to msg-ask-1" in prompt, prompt
                elif msg is sys_msg:
                    assert "--to system" not in prompt, prompt
                else:
                    assert "group messages" in prompt and "group read" in prompt, prompt

            # RUN_STATE_CANCELLING raises a busy AdapterError without calling send-message.
            cli.reset_mock()
            server.mode = "cancelling"
            try:
                adapter.inject(ask_msg)
            except AdapterError as exc:
                assert "busy" in str(exc).lower(), exc
            else:
                raise AssertionError("expected busy error for RUN_STATE_CANCELLING")
            assert cli.call_count == 0

            # 404 not_found raises AdapterError without calling send-message.
            server.mode = "not_found"
            try:
                adapter.inject(ask_msg)
            except AdapterError as exc:
                assert "not found" in str(exc).lower(), exc
            else:
                raise AssertionError("expected not found error")
            assert cli.call_count == 0

            # unimplemented falls back to `agentapi get-conversation-metadata` before send-message.
            server.mode = "unimplemented"
            adapter.inject(ask_msg)
            assert [c[0][0][0] for c in cli.call_args_list] == ["get-conversation-metadata", "send-message"]

            # Bad CSRF token fails auth before calling send-message.
            cli.reset_mock()
            server.mode = "idle"
            adapter.csrf_token = "wrong-token"
            try:
                adapter.inject(ask_msg)
            except AdapterError as exc:
                assert "authentication failed" in str(exc).lower(), exc
            else:
                raise AssertionError("expected authentication error")
            assert cli.call_count == 0


def run(root: Path, server: FakeConversationServer) -> None:
    server.mode = "idle"
    bus_home = root / "bus home"
    target_cwd = root / "target project"
    target_cwd.mkdir()
    relative_db = Path("custom bus db") / "messages.sqlite"
    calls_path = root / "calls.jsonl"
    fake_cli = root / "fake agentapi"
    fake_cli.write_text(f"#!{sys.executable}\n" + STUB)
    fake_cli.chmod(0o700)
    env = {
        **{
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("AGENT_BUS_", "ANTIGRAVITY_"))
        },
        "AGENT_BUS_HOME": str(bus_home),
        "AGENT_BUS_DB": str(relative_db),
        "AGENT_BUS_TOKEN_DIR": "custom tokens",
        "PYTHONPATH": str(HERE),
        "ANTIGRAVITY_LS_ADDRESS": server.address,
        "ANTIGRAVITY_CSRF_TOKEN": CSRF_SECRET,
        "ANTIGRAVITY_CONVERSATION_ID": "11111111-1111-1111-1111-111111111111",
        "ANTIGRAVITY_TRAJECTORY_ID": "22222222-2222-2222-2222-222222222222",
        "ANTIGRAVITY_SOURCE_METADATA": '{"tool":"caller"}',
        "AGY_PROBE_CALLS": str(calls_path),
        "AGY_PROBE_CSRF": CSRF_SECRET,
        "AGY_PROBE_VALID_CONV": CONVERSATION,
    }
    cli = [sys.executable, str(HERE / "bus.py")]

    def run_cli(*args: str, check: bool = True, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        merged = dict(env)
        if extra_env:
            for k, v in extra_env.items():
                if v is None:
                    merged.pop(k, None)
                else:
                    merged[k] = v
        return subprocess.run([*cli, *args], env=merged, cwd=root, capture_output=True, text=True, timeout=15, check=check)

    # Reject non-UUID session ID.
    bad = run_cli("up", "--agent-id", "receiver", "--adapter", "antigravity", "--session-id", "not-a-uuid", check=False)
    assert bad.returncode and "conversation UUID" in bad.stderr, bad.stderr

    # Reject missing CSRF token.
    no_csrf = run_cli(
        "up", "--agent-id", "receiver", "--adapter", "antigravity", "--session-id", CONVERSATION,
        "--cli", str(fake_cli), check=False, extra_env={"ANTIGRAVITY_CSRF_TOKEN": None},
    )
    assert no_csrf.returncode and "ANTIGRAVITY_CSRF_TOKEN" in no_csrf.stderr, no_csrf.stderr

    # Reject nonexistent conversation under --verify-session and leave no pid file.
    missing = run_cli(
        "up", "--agent-id", "receiver", "--adapter", "antigravity", "--session-id", MISSING_CONVERSATION,
        "--cli", str(fake_cli), "--verify-session", check=False,
    )
    assert missing.returncode and "not found" in missing.stderr.lower(), missing.stderr
    assert not (bus_home / "run" / "notifier-receiver.json").exists()

    run_cli("register", "--agent-id", "sender", "--agent-type", "command")
    start = [
        "up", "--agent-id", "receiver", "--agent-type", "antigravity", "--session-id", CONVERSATION,
        "--cli", str(fake_cli), "--antigravity-ls-address", server.address,
        "--cwd", target_cwd.name, "--verify-session", "--poll", "0.05", "--outbox-flush", "0",
    ]
    try:
        result = json.loads(run_cli(*start).stdout)
        assert result["ok"] and result["session_probe"]["run_state"] == "RUN_STATE_IDLE", result
        pid_file = Path(result["notifier"]["pid_file"])
        pid_text = pid_file.read_text()
        assert CSRF_SECRET not in pid_text, "CSRF token must never be written to pid file"
        notifier_log = Path(result["notifier"]["log"])

        # Idempotent up succeeds; mismatched session or LS address fails without --force.
        assert json.loads(run_cli(*start).stdout)["already_running"]
        diff_session = [SECOND_CONVERSATION if arg == CONVERSATION else arg for arg in start]
        mismatch = run_cli(*diff_session, check=False)
        assert mismatch.returncode and "different notifier binding" in mismatch.stderr, mismatch.stderr
        diff_addr = ["127.0.0.1:9" if arg == server.address else arg for arg in start if arg != "--verify-session"]
        mismatch_addr = run_cli(*diff_addr, check=False)
        assert mismatch_addr.returncode and "different notifier binding" in mismatch_addr.stderr, mismatch_addr.stderr

        # Duplicate notifier process is blocked by lock.
        duplicate = subprocess.run(
            [sys.executable, "-m", "buslib.notifier", "--agent-id", "receiver", "--adapter", "antigravity"],
            env=env, cwd=root, capture_output=True, text=True, timeout=5,
        )
        assert duplicate.returncode and "already running" in duplicate.stderr, duplicate.stderr

        # Liveness probe reports Antigravity runState without exposing CSRF token,
        # and can resolve antigravity_ls_address from the notifier pid file when env omits it.
        live = json.loads(
            run_cli("liveness", "--agent-id", "receiver", extra_env={"ANTIGRAVITY_LS_ADDRESS": None}).stdout
        )
        assert live["model_liveness"] == "idle" and live["run_state"] == "RUN_STATE_IDLE", live
        assert CSRF_SECRET not in json.dumps(live)
        server.mode = "running"
        live2 = json.loads(run_cli("liveness", "--agent-id", "receiver").stdout)
        assert live2["model_liveness"] == "running" and live2["run_state"] == "RUN_STATE_RUNNING", live2
        server.mode = "idle"

        # 1:1 ask delivery + idempotency + pointer-first verification.
        ask_args = ("ask", "--from", "sender", "--to", "receiver", "--text", PRIVATE_TEXT, "--idempotency-key", "agy-probe")
        first = json.loads(run_cli(*ask_args).stdout)["message_id"]
        again = json.loads(run_cli(*ask_args).stdout)["message_id"]
        assert first == again

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with closing(sqlite3.connect(root / relative_db)) as conn:
                status = conn.execute("SELECT status FROM messages WHERE message_id=?", (first,)).fetchone()[0]
            if status == "acked":
                break
            time.sleep(0.02)
        else:
            raise AssertionError(notifier_log.read_text())

        calls = [json.loads(line) for line in calls_path.read_text().splitlines()]
        assert len(calls) == 1, calls
        call = calls[0]
        assert call["csrf_present"] is True and call["ls_address"] == server.address, call
        assert call["stripped_conv"] and call["stripped_traj"] and call["stripped_meta"], call
        args = call["args"]
        assert args[:3] == ["send-message", "--title=agent-bus: sender", CONVERSATION], args
        prompt = args[3]
        assert first in prompt and PRIVATE_TEXT not in prompt, prompt
        read_command = shlex.split(prompt.splitlines()[1])
        msg = subprocess.run(read_command, cwd=target_cwd, capture_output=True, text=True, timeout=5, check=True)
        assert json.loads(json.loads(msg.stdout)["message"]["payload"])["text"] == PRIVATE_TEXT

        # Group digest delivery also works through AntigravityAdapter.
        run_cli("group", "create", "--from", "sender", "--group-id", "agy-room", "--topic", "demo", "--rate-seconds", "0")
        run_cli("group", "join", "--group-id", "agy-room", "--agent-id", "receiver")
        run_cli("group", "send", "--from", "sender", "--group-id", "agy-room", "--text", "hello antigravity group")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            calls = [json.loads(line) for line in calls_path.read_text().splitlines()]
            if len(calls) >= 2:
                break
            time.sleep(0.02)
        else:
            raise AssertionError(notifier_log.read_text())
        digest_prompt = calls[1]["args"][3]
        assert "group-digest-agy-room-1-receiver" in digest_prompt and "group messages" in digest_prompt, digest_prompt
        assert CSRF_SECRET not in notifier_log.read_text()
    finally:
        run_cli("down", "--agent-id", "receiver", "--force")


if __name__ == "__main__":
    with FakeConversationServer() as srv:
        probe_adapter_unit(srv)
        with tempfile.TemporaryDirectory(prefix="bus-antigravity-") as tmp:
            run(Path(tmp), srv)
    print("ANTIGRAVITY_WAKE_OK")
