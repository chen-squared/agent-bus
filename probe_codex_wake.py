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
import threading
import time
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from websockets.sync.server import unix_serve

from buslib.adapters.base import AdapterError, DeliveryUnknownError, TransientAdapterError
from buslib.adapters.codex import CodexAdapter

HERE = Path(__file__).resolve().parent
THREAD = "d943d26f-c0ac-40a7-af9a-75b59338a3c7"
REMOTE = "unix:///tmp/codex-probe.sock"
PRIVATE_TEXT = "private payload with shell syntax: $(must-not-run)"


class Server:
    def __init__(self, steps):
        self.requests = []
        self.replies = iter([("initialize", {"result": {}}), *steps])

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def send(self, line):
        self.requests.append(json.loads(line))

    def recv(self, *, timeout):
        assert timeout > 0
        method, reply = next(self.replies)
        request = self.requests[-1]
        assert request["method"] == method, self.requests
        if reply is None:
            raise ConnectionResetError("response lost")
        return json.dumps({"id": request["id"], **reply})


def thread_state(status):
    return {"thread": {"id": THREAD, "status": {"type": status}}}


def probe_delivery() -> None:
    active = {"id": "turn-active", "status": "inProgress"}
    read_active = ("thread/read", {"result": thread_state("active")})
    read_idle = ("thread/read", {"result": thread_state("idle")})
    read_unloaded = ("thread/read", {"result": thread_state("notLoaded")})
    active_history = ("thread/turns/list", {"result": {"data": [active]}})
    start = ("turn/start", {"result": {"turn": {"id": "turn-started"}}})
    steer = ("turn/steer", {"result": {"turnId": active["id"]}})
    no_active = {"error": {"code": -32600, "message": "no active turn to steer"}}
    changed = {"error": {"code": -32600, "message": "expected active turn id `turn-active` but found `new`"}}
    review = {"error": {
        "code": -32600, "message": "cannot steer a review turn",
        "data": {"codexErrorInfo": {"activeTurnNotSteerable": {"turnKind": "review"}}},
    }}
    missing_thread = {"error": {"code": -32602, "message": f"thread not found: {THREAD}"}}
    start_error = {"error": {"code": -32603, "message": "turn admission failed"}}
    message = {"to_agent": "receiver", "message_id": "probe-steer", "payload": PRIVATE_TEXT}
    cases = [
        ([read_active, active_history, steer], None),
        ([read_idle, start], None),
        ([read_unloaded, ("thread/resume", {"result": thread_state("idle")}), start], None),
        ([read_unloaded, ("thread/resume", {"result": thread_state("active")}), active_history, steer], None),
        ([read_active, ("thread/turns/list", {"result": {"data": []}})], TransientAdapterError),
        ([("thread/read", missing_thread)], AdapterError),
        ([read_active, active_history, ("turn/steer", no_active)], TransientAdapterError),
        ([read_active, active_history, ("turn/steer", changed)], TransientAdapterError),
        ([read_active, active_history, ("turn/steer", review)], TransientAdapterError),
        ([read_active, active_history, ("turn/steer", None)], DeliveryUnknownError),
        ([read_idle, ("turn/start", None)], DeliveryUnknownError),
        ([read_idle, ("turn/start", start_error)], DeliveryUnknownError),
    ]
    adapter = CodexAdapter(THREAD, str(HERE), REMOTE)
    client_message_ids = set()
    for steps, failure in cases:
        server = Server(steps)
        with patch("buslib.adapters.codex_rpc._connect", return_value=server):
            try:
                adapter.inject(message)
            except AdapterError as exc:
                assert type(exc) is failure, exc
            else:
                assert failure is None
        assert [request["method"] for request in server.requests] == [
            "initialize", "initialized", *(method for method, _ in steps),
        ]
        request = server.requests[-1]
        if request["method"] in ("turn/start", "turn/steer"):
            params = request["params"]
            assert params["threadId"] == THREAD and params["clientUserMessageId"]
            client_message_ids.add(params["clientUserMessageId"])
            assert params.get("expectedTurnId") == (active["id"] if request["method"] == "turn/steer" else None)
            prompt = params["input"][0]["text"]
            assert message["message_id"] in prompt and PRIVATE_TEXT not in prompt, prompt
    assert len(client_message_ids) == 1, client_message_ids


def run(root: Path) -> None:
    bus_home = root / "bus home"
    target_cwd = root / "target project"
    target_cwd.mkdir()
    relative_db = Path("custom bus db") / "messages.sqlite"
    socket_path = root / "app-server.sock"
    remote = f"unix://{socket_path}"
    starts = []
    env = {
        **{key: value for key, value in os.environ.items() if not key.startswith("AGENT_BUS_")},
        "AGENT_BUS_HOME": str(bus_home),
        "AGENT_BUS_DB": str(relative_db),
        "AGENT_BUS_TOKEN_DIR": "custom tokens",
        "PYTHONPATH": str(HERE),
    }
    cli = [sys.executable, str(HERE / "bus.py")]

    def run_cli(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run([*cli, *args], env=env, cwd=root, capture_output=True, text=True, timeout=12, check=check)

    def idle_app_server(socket):
        responses = {
            "initialize": {},
            "thread/read": thread_state("idle"),
            "turn/start": {"turn": {"id": "fresh-turn"}},
        }
        for raw in socket:
            request = json.loads(raw)
            if "id" in request:
                method = request["method"]
                if method == "turn/start":
                    starts.append(request["params"])
                socket.send(json.dumps({"id": request["id"], "result": responses[method]}))

    bad = run_cli("up", "--agent-id", "receiver", "--adapter", "codex", "--session-id", "thread name", check=False)
    assert bad.returncode and "thread UUID" in bad.stderr, bad.stderr
    run_cli("register", "--agent-id", "sender", "--agent-type", "command")
    start = [
        "up", "--agent-id", "receiver", "--agent-type", "codex", "--session-id", THREAD,
        "--codex-remote", remote, "--cwd", target_cwd.name, "--poll", "0.05", "--outbox-flush", "0",
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

        def wait_for_message(predicate):
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                with closing(sqlite3.connect(root / relative_db)) as conn:
                    row = conn.execute("SELECT status,retry_count,last_error FROM messages WHERE message_id=?", (first,)).fetchone()
                if row is not None and predicate(row):
                    return row
                time.sleep(0.02)
            raise AssertionError(notifier_log.read_text())

        # No reachable server means the bus keeps the input pending without queueing it in Codex.
        row = wait_for_message(lambda row: "Codex app server unavailable" in (row[2] or ""))
        assert row[:2] == ("queued", 0) and not starts, row

        with unix_serve(idle_app_server, str(socket_path)) as server:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                wait_for_message(lambda row: row[0] == "acked")
            finally:
                server.shutdown()
                worker.join(timeout=3)

        assert len(starts) == 1 and starts[0]["threadId"] == THREAD, starts
        prompt = starts[0]["input"][0]["text"]
        assert first in prompt and PRIVATE_TEXT not in prompt, prompt
        read_command = shlex.split(prompt.splitlines()[1])
        msg = subprocess.run(read_command, cwd=target_cwd, capture_output=True, text=True, timeout=5, check=True)
        assert json.loads(json.loads(msg.stdout)["message"]["payload"])["text"] == PRIVATE_TEXT
        assert f"accepted {first} via turn/start (fresh-turn)" in notifier_log.read_text()
    finally:
        run_cli("down", "--agent-id", "receiver", "--force")


if __name__ == "__main__":
    probe_delivery()
    with tempfile.TemporaryDirectory(prefix="bus-codex-", dir="/tmp") as tmp:
        run(Path(tmp))
    print("CODEX_WAKE_OK")
