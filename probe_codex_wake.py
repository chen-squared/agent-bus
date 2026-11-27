#!/usr/bin/env python3
"""Exercise Codex wakes through the notifier and a stubbed App Server."""

from __future__ import annotations

import json
import os
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from buslib.adapters.base import AdapterError
from buslib.adapters.codex import CodexAdapter

HERE = Path(__file__).resolve().parent
THREAD = "d943d26f-c0ac-40a7-af9a-75b59338a3c7"
REMOTE = "unix:///tmp/codex-probe.sock"
PRIVATE_TEXT = "private payload with shell syntax: $(must-not-run)"

STUB = """
import json
import os
import sys

args = sys.argv[1:]
if args == ['queue', '--help']:
    print('codex queue --resume')
    sys.exit(0)
with open(os.environ['CODEX_PROBE_CALLS'], 'a', encoding='utf-8') as log:
    log.write(json.dumps(args) + '\\n')
print('Warning: message was queued, but could not resume thread (probe)', file=sys.stderr)
"""


class Server:
    def __init__(self, turn, steer_response=None):
        self.requests = []
        self.replies = iter([
            {"id": 1, "result": {}},
            {"id": 2, "method": "thread/status/changed", "params": {}},
            {"id": 2, "result": {"data": [turn]}},
            steer_response,
        ])

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def send(self, line):
        self.requests.append(json.loads(line))

    def recv(self, *, timeout):
        assert timeout > 0
        reply = next(self.replies)
        if reply is None:
            raise ConnectionResetError("response lost")
        return json.dumps(reply)


def probe_steer() -> None:
    active = {"id": "turn-active", "status": "inProgress"}
    result = {"id": 3, "result": {"turnId": active["id"]}}
    no_active = {"id": 3, "error": {"code": -32600, "message": "no active turn to steer"}}
    changed = {"id": 3, "error": {"code": -32600, "message": "expected active turn id `turn-active` but found `new`"}}
    review = {"id": 3, "error": {
        "code": -32600, "message": "cannot steer a review turn",
        "data": {"codexErrorInfo": {"activeTurnNotSteerable": {"turnKind": "review"}}},
    }}
    message = {"to_agent": "receiver", "message_id": "probe-steer", "payload": PRIVATE_TEXT}
    cli_ok = subprocess.CompletedProcess([], 0, "codex queue --resume", "")
    turns = [
        (Server(active, result), False, False),
        (Server({"id": "old", "status": "completed"}), True, False),
        (Server(active, no_active), True, False),
        (Server(active, changed), True, False),
        (Server(active, review), True, False),
        (Server(active), False, True),
    ]
    with patch.object(CodexAdapter, "_run", return_value=cli_ok) as cli:
        adapter = CodexAdapter(THREAD, sys.executable, str(HERE), REMOTE)
        for server, queued, missing_ack in turns:
            cli.reset_mock()
            with patch("buslib.adapters.codex_rpc._connect", return_value=server):
                try:
                    adapter.inject(message)
                except AdapterError as exc:
                    assert missing_ack and "acknowledgement missing" in str(exc), exc
                else:
                    assert not missing_ack
            assert cli.call_count == int(queued), (server.requests, cli.call_args_list)
            assert [request["method"] for request in server.requests[:3]] == [
                "initialize", "initialized", "thread/turns/list",
            ]
            if len(server.requests) > 3:
                steer = server.requests[3]
                assert steer["method"] == "turn/steer"
                params = steer["params"]
                assert params["expectedTurnId"] == active["id"] and params["threadId"] == THREAD
                prompt = params["input"][0]["text"]
                assert message["message_id"] in prompt and PRIVATE_TEXT not in prompt, prompt


def run(root: Path) -> None:
    bus_home = root / "bus home"
    target_cwd = root / "target project"
    target_cwd.mkdir()
    relative_db = Path("custom bus db") / "messages.sqlite"
    calls_path = root / "calls.jsonl"
    fake_cli = root / "fake codex"
    fake_cli.write_text(f"#!{sys.executable}\n" + STUB)
    fake_cli.chmod(0o700)
    env = {
        **{key: value for key, value in os.environ.items() if not key.startswith("AGENT_BUS_")},
        "AGENT_BUS_HOME": str(bus_home),
        "AGENT_BUS_DB": str(relative_db),
        "AGENT_BUS_TOKEN_DIR": "custom tokens",
        "PYTHONPATH": str(HERE),
        "CODEX_PROBE_CALLS": str(calls_path),
    }
    cli = [sys.executable, str(HERE / "bus.py")]

    def run_cli(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run([*cli, *args], env=env, cwd=root, capture_output=True, text=True, timeout=12, check=check)

    bad = run_cli("up", "--agent-id", "receiver", "--adapter", "codex", "--session-id", "thread name", check=False)
    assert bad.returncode and "thread UUID" in bad.stderr, bad.stderr
    run_cli("register", "--agent-id", "sender", "--agent-type", "command")
    start = [
        "up", "--agent-id", "receiver", "--agent-type", "codex", "--session-id", THREAD,
        "--cli", str(fake_cli), "--codex-remote", REMOTE, "--cwd", target_cwd.name, "--poll", "0.05", "--outbox-flush", "0",
    ]
    try:
        result = json.loads(run_cli(*start).stdout)
        notifier_log = Path(result["notifier"]["log"])
        assert json.loads(run_cli(*start).stdout)["already_running"]
        different = ["11111111-1111-1111-1111-111111111111" if arg == THREAD else arg for arg in start]
        mismatch = run_cli(*different, check=False)
        assert mismatch.returncode and "different notifier binding" in mismatch.stderr, mismatch.stderr

        duplicate = subprocess.run(
            [sys.executable, "-m", "buslib.notifier", "--agent-id", "receiver"],
            env=env, cwd=root, capture_output=True, text=True, timeout=5,
        )
        assert duplicate.returncode and "already running" in duplicate.stderr, duplicate.stderr

        args = ("send", "--from", "sender", "--to", "receiver", "--text", PRIVATE_TEXT, "--idempotency-key", "wake-probe")
        first = json.loads(run_cli(*args).stdout)["message_id"]
        again = json.loads(run_cli(*args).stdout)["message_id"]
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
        args = calls[0]
        assert args[:5] == ["queue", "--thread", THREAD, "--resume", "--message"], args
        assert args[6:] == ["--remote", REMOTE], args
        prompt = args[5]
        assert first in prompt and PRIVATE_TEXT not in prompt, prompt
        read_command = shlex.split(prompt.splitlines()[1])
        msg = subprocess.run(read_command, cwd=target_cwd, capture_output=True, text=True, timeout=5, check=True)
        assert json.loads(json.loads(msg.stdout)["message"]["payload"])["text"] == PRIVATE_TEXT
        assert "could not resume" in notifier_log.read_text()
    finally:
        run_cli("down", "--agent-id", "receiver", "--force")


if __name__ == "__main__":
    probe_steer()
    with tempfile.TemporaryDirectory(prefix="bus-codex-") as tmp:
        run(Path(tmp))
    print("CODEX_WAKE_OK")
