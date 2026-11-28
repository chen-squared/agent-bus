#!/usr/bin/env python3
"""Exercise notifier transient-error backoff and dynamic OpenCode CLI re-resolution."""

from __future__ import annotations

import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from buslib.adapters.base import AdapterError, DeliveryUnknownError, TransientAdapterError, is_transient_error, native_send_error
from buslib.adapters.opencode import OpencodeAdapter
from buslib.agents import resolve_opencode_cli

HERE = Path(__file__).resolve().parent

FAKE_OPENCODE = """#!{python}
import json
import os
import sys
from pathlib import Path

args = sys.argv[1:]
if args[:2] == ["api", "session.get"]:
    print(json.dumps({"data": {"model": {"id": "fake"}, "outcome": "succeeded"}}))
    sys.exit(0)
if args[:2] == ["api", "session.prompt"]:
    state_file = Path(os.environ["PROBE_STATE_FILE"])
    calls_file = Path(os.environ["PROBE_CALLS_FILE"])
    mode = state_file.read_text().strip() if state_file.exists() else "ok"
    with calls_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"version": os.environ.get("PROBE_CLI_VER", "fixed"), "mode": mode, "args": args}) + "\\n")
    if mode == "offline":
        print("Error: connect ECONNREFUSED 127.0.0.1:4096", file=sys.stderr)
        sys.exit(1)
    if mode == "permanent":
        print("Error: session ses_test not found", file=sys.stderr)
        sys.exit(1)
    print(json.dumps({"ok": True}))
    sys.exit(0)
sys.exit(0)
"""


def probe_opencode_cli_upgrade() -> None:
    assert isinstance(native_send_error("Error: failed to connect to application socket"), TransientAdapterError)
    assert isinstance(native_send_error("connection reset while awaiting response"), DeliveryUnknownError)
    with tempfile.TemporaryDirectory(prefix="bus-oc-cli-") as tmp:
        fake_home = Path(tmp) / "home"
        cli_root = fake_home / "Library" / "Application Support" / "ai.opencode.desktop" / "cli"
        calls_file = Path(tmp) / "calls.jsonl"
        state_file = Path(tmp) / "state.txt"
        state_file.write_text("ok")

        def install_version(ver: str) -> Path:
            vdir = cli_root / f"{ver}-arm64"
            vdir.mkdir(parents=True, exist_ok=True)
            bin_path = vdir / "opencode-cli"
            body = FAKE_OPENCODE.replace("{python}", sys.executable).replace(
                'os.environ.get("PROBE_CLI_VER", "fixed")', repr(ver)
            )
            bin_path.write_text(body)
            bin_path.chmod(0o700)
            return bin_path

        v_old = install_version("2.9.0")
        env = {
            **os.environ,
            "HOME": str(fake_home),
            "PROBE_STATE_FILE": str(state_file),
            "PROBE_CALLS_FILE": str(calls_file),
        }
        env.pop("AGENT_BUS_CLI", None)

        msg = {
            "message_id": "m-1",
            "from_agent": "alice",
            "thread_id": "t-1",
            "payload": json.dumps({"text": "hello"}),
        }
        with patch.dict(os.environ, env, clear=True):
            assert resolve_opencode_cli() == str(v_old)
            adapter = OpencodeAdapter("ses_test", None, tmp)
            adapter.inject(msg)

            # Simulate app upgrade: 2.10.0 installed alongside 2.9.0, then 2.9.0 removed.
            # Lexicographic sort would wrongly pick 2.9.0 over 2.10.0; semver sort picks 2.10.0.
            v_new = install_version("2.10.0")
            assert resolve_opencode_cli() == str(v_new), resolve_opencode_cli()
            shutil.rmtree(v_old.parent)
            adapter.inject(msg)
            assert adapter.cli == str(v_new)

            # Transient offline error raises TransientAdapterError; permanent error raises plain AdapterError.
            state_file.write_text("offline")
            try:
                adapter.inject(msg)
            except TransientAdapterError as exc:
                assert is_transient_error(exc)
            else:
                raise AssertionError("expected TransientAdapterError for ECONNREFUSED")

            state_file.write_text("permanent")
            try:
                adapter.inject(msg)
            except AdapterError as exc:
                assert not isinstance(exc, TransientAdapterError) and not is_transient_error(exc), exc
            else:
                raise AssertionError("expected permanent AdapterError for session not found")

        calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
        assert [c["version"] for c in calls[:2]] == ["2.9.0", "2.10.0"], calls


def probe_notifier_restart_resilience() -> None:
    with tempfile.TemporaryDirectory(prefix="bus-resilience-") as tmp:
        root = Path(tmp)
        bus_home = root / "bus-home"
        fake_home = root / "user-home"
        cli_root = fake_home / "Library" / "Application Support" / "ai.opencode.desktop" / "cli"
        state_file = root / "state.txt"
        calls_file = root / "calls.jsonl"
        state_file.write_text("offline")

        def install_version(ver: str) -> Path:
            vdir = cli_root / f"{ver}-arm64"
            vdir.mkdir(parents=True, exist_ok=True)
            bin_path = vdir / "opencode-cli"
            body = FAKE_OPENCODE.replace("{python}", sys.executable).replace(
                'os.environ.get("PROBE_CLI_VER", "fixed")', repr(ver)
            )
            bin_path.write_text(body)
            bin_path.chmod(0o700)
            return bin_path

        v_old = install_version("2.4.2")
        env = {
            **{k: v for k, v in os.environ.items() if not k.startswith("AGENT_BUS_")},
            "HOME": str(fake_home),
            "AGENT_BUS_HOME": str(bus_home),
            "AGENT_BUS_TRANSIENT_BACKOFF_MAX": "0.2",
            "PYTHONPATH": str(HERE),
            "PROBE_STATE_FILE": str(state_file),
            "PROBE_CALLS_FILE": str(calls_file),
        }
        cli = [sys.executable, str(HERE / "bus.py")]

        def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
            return subprocess.run([*cli, *args], env=env, cwd=HERE, capture_output=True, text=True, timeout=15, check=True)

        run_cli("register", "--agent-id", "sender", "--agent-type", "command")
        try:
            up = json.loads(
                run_cli(
                    "up", "--agent-id", "oc-peer", "--adapter", "opencode", "--session-id", "ses_test",
                    "--poll", "0.05", "--heartbeat", "1", "--sweep", "60", "--outbox-flush", "0",
                ).stdout
            )
            log_path = Path(up["notifier"]["log"])
            db = bus_home / "bus.db"

            # Send two messages while OpenCode is offline/restarting.
            m1 = json.loads(run_cli("send", "--from", "sender", "--to", "oc-peer", "--text", "during restart 1").stdout)["message_id"]
            m2 = json.loads(run_cli("send", "--from", "sender", "--to", "oc-peer", "--text", "during restart 2").stdout)["message_id"]

            # Wait long enough for >5 poll ticks; neither message may be dead-lettered.
            deadline = time.monotonic() + 4.0
            while time.monotonic() < deadline:
                if calls_file.exists() and len(calls_file.read_text().splitlines()) >= 6:
                    break
                time.sleep(0.05)
            else:
                raise AssertionError(f"expected >=6 deferred offline attempts:\n{log_path.read_text()}")

            with closing(sqlite3.connect(db)) as conn:
                rows = dict(
                    conn.execute("SELECT message_id, status || ':' || retry_count FROM messages WHERE message_id IN (?,?)", (m1, m2)).fetchall()
                )
            assert rows == {m1: "queued:0", m2: "queued:0"}, (rows, log_path.read_text())

            # Now simulate OpenCode coming back online AFTER an upgrade to 2.10.0 (old 2.4.2 removed).
            install_version("2.10.0")
            shutil.rmtree(v_old.parent)
            state_file.write_text("ok")

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                with closing(sqlite3.connect(db)) as conn:
                    statuses = [
                        r[0] for r in conn.execute("SELECT status FROM messages WHERE message_id IN (?,?)", (m1, m2)).fetchall()
                    ]
                if statuses == ["acked", "acked"]:
                    break
                time.sleep(0.05)
            else:
                raise AssertionError(f"messages did not recover after restart+upgrade:\n{log_path.read_text()}")

            log_text = log_path.read_text()
            assert "target unavailable, deferring" in log_text, log_text
            assert "target recovered after" in log_text, log_text
            ok_calls = [json.loads(line) for line in calls_file.read_text().splitlines() if json.loads(line)["mode"] == "ok"]
            assert len(ok_calls) == 2 and all(c["version"] == "2.10.0" for c in ok_calls), ok_calls

            # Permanent failure still consumes retry_count and dead-letters at 5.
            state_file.write_text("permanent")
            m3 = json.loads(run_cli("send", "--from", "sender", "--to", "oc-peer", "--text", "bad session").stdout)["message_id"]
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                with closing(sqlite3.connect(db)) as conn:
                    row = conn.execute("SELECT status, retry_count FROM messages WHERE message_id=?", (m3,)).fetchone()
                if row and row[0] == "dead":
                    break
                time.sleep(0.05)
            else:
                raise AssertionError(f"permanent error did not dead-letter:\n{log_path.read_text()}")
            assert row == ("dead", 5), row
        finally:
            run_cli("down", "--agent-id", "oc-peer", "--force")


def probe_delivery_attempts() -> None:
    with tempfile.TemporaryDirectory(prefix="bus-attempts-") as tmp:
        root = Path(tmp)
        db = root / "bus.db"
        attempts = root / "crash-attempts"
        release = root / "release"
        script = root / "notify.py"
        script.write_text(
            "import os, sys, time\n"
            "from pathlib import Path\n"
            "thread = os.environ['AGENT_BUS_THREAD_ID']\n"
            "if thread in {'deferred', 'a-deferred'}:\n"
            "    sys.exit('service unavailable')\n"
            "if thread == 'c-permanent':\n"
            "    sys.exit('conversation not found')\n"
            "if thread == 'crash':\n"
            "    root = Path(__file__).parent\n"
            "    with (root / 'crash-attempts').open('a') as f:\n"
            "        print(os.environ['AGENT_BUS_MESSAGE_ID'], file=f)\n"
            "    deadline = time.monotonic() + 15\n"
            "    while not (root / 'release').exists() and time.monotonic() < deadline:\n"
            "        time.sleep(0.02)\n"
        )
        env = {
            **{k: v for k, v in os.environ.items() if not k.startswith("AGENT_BUS_")},
            "AGENT_BUS_HOME": tmp,
            "PROBE_PYTHON": sys.executable,
            "PROBE_NOTIFY": str(script),
        }
        cli = [sys.executable, str(HERE / "bus.py")]

        def run_cli(*args: str) -> dict:
            result = subprocess.run([*cli, *args], env=env, cwd=HERE, capture_output=True, text=True, timeout=15, check=True)
            return json.loads(result.stdout)

        def send(thread: str) -> str:
            return run_cli("send", "--from", "sender", "--to", "receiver", "--thread-id", thread, "--text", thread)["message_id"]

        def wait_for_message(message_id: str, status: str) -> tuple:
            deadline = time.monotonic() + 6
            while time.monotonic() < deadline:
                with closing(sqlite3.connect(db)) as conn:
                    row = conn.execute(
                        "SELECT status,retry_count,last_error FROM messages WHERE message_id=?", (message_id,)
                    ).fetchone()
                if row and row[0] == status:
                    return row
                time.sleep(0.03)
            raise AssertionError((message_id, status, (root / "logs/notifier-receiver.log").read_text()))

        for agent in ("sender", "receiver"):
            run_cli("register", "--agent-id", agent, "--agent-type", "command")
        for group in ("a-deferred", "b-healthy", "c-permanent"):
            run_cli("group", "create", "--from", "sender", "--group-id", group, "--topic", group)
            run_cli("group", "join", "--agent-id", "receiver", "--group-id", group)
            run_cli("group", "send", "--from", "sender", "--group-id", group, "--text", group)
        first = send("deferred")
        # More than one batch's worth of failed messages must let a fresh message through.
        for _ in range(25):
            send("deferred")
        healthy = send("healthy")
        start = (
            "up", "--agent-id", "receiver", "--adapter", "command", "--notify-cmd",
            'exec "$PROBE_PYTHON" "$PROBE_NOTIFY"', "--poll", "0.05", "--outbox-flush", "0",
        )
        try:
            pid = run_cli(*start)["notifier"]["pid"]
            wait_for_message(healthy, "acked")
            wait_for_message("group-digest-b-healthy-1-receiver", "acked")
            assert wait_for_message(first, "queued") == ("queued", 0, "service unavailable")
            with closing(sqlite3.connect(db)) as conn:
                state = conn.execute(
                    "SELECT last_digest_seq FROM group_read_state WHERE group_id='a-deferred' AND agent_id='receiver'"
                ).fetchone()
            assert state is None or state == (0,), state

            interrupted = send("crash")
            deadline = time.monotonic() + 6
            while not attempts.exists() or not attempts.read_text().splitlines():
                assert time.monotonic() < deadline, "notify command never started"
                time.sleep(0.03)
            os.kill(pid, signal.SIGKILL)
            release.touch()
            assert run_cli(*start, "--force")["notifier"]["pid"] != pid

            dead = wait_for_message(interrupted, "dead")
            assert dead[1] == 0 and "notifier stopped" in dead[2], dead
            wait_for_message(send("after-restart"), "acked")
            assert attempts.read_text().splitlines() == [interrupted]

            run_cli("dlq", "retry", "--agent-id", "receiver", "--message-id", interrupted)
            wait_for_message(interrupted, "acked")
            assert attempts.read_text().splitlines() == [interrupted, interrupted]

            digest_id = "group-digest-c-permanent-1-receiver"
            assert wait_for_message(digest_id, "dead") == ("dead", 5, "conversation not found")
            unread = run_cli("group", "messages", "--group-id", "c-permanent", "--agent-id", "receiver")
            assert [message["seq"] for message in unread["messages"]] == [1], unread
        finally:
            release.touch()
            run_cli("down", "--agent-id", "receiver", "--force")


if __name__ == "__main__":
    probe_opencode_cli_upgrade()
    probe_notifier_restart_resilience()
    probe_delivery_attempts()
    print("RESILIENCE_OK")
