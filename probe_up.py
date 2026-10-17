#!/usr/bin/env python3
"""Exercise the packaged one-command bootstrap in an isolated bus world.

This probe intentionally uses `python -m buslib.cli`, the same entry point the
console script wraps, so it covers registration, detached notifier startup,
pid/log bookkeeping, delivery, idempotent `up`, and `down`.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = Path(tempfile.mkdtemp(prefix="bus-up-"))
HOME = ROOT / "home"
INBOX = ROOT / "bob.inbox"
FAIL: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


def cli(*argv: str, timeout: float = 10) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["AGENT_BUS_HOME"] = str(HOME)
    env["PYTHONPATH"] = str(HERE)
    return subprocess.run(
        [sys.executable, "-m", "buslib.cli", *argv],
        cwd=HERE, env=env, capture_output=True, text=True, timeout=timeout,
    )


def jout(proc: subprocess.CompletedProcess[str]) -> dict:
    try:
        return json.loads(proc.stdout)
    except (json.JSONDecodeError, TypeError):
        return {}


def wait_for(predicate, timeout: float = 10.0, interval: float = 0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return False
    return True


def main() -> int:
    try:
        print("A init + up")
        proc = cli("init")
        check("init succeeds", proc.returncode == 0 and jout(proc).get("ok") is True, proc.stderr[:160])
        check("home is the isolated world", jout(proc).get("home") == str(HOME.resolve()), proc.stdout[:160])

        proc = cli(
            "up", "--agent-id", "alice", "--adapter", "command", "--agent-type", "command",
            "--notify-cmd", "true", "--poll", "0.2", "--heartbeat", "1",
            "--sweep", "60", "--outbox-flush", "0",
        )
        alice = jout(proc)
        check("alice up succeeds", proc.returncode == 0 and alice.get("ok") is True, proc.stderr[:200])
        check("alice notifier pid recorded", bool(alice.get("notifier", {}).get("pid")))
        token_file = alice.get("agent", {}).get("token_file")
        check("alice token created", bool(token_file) and Path(token_file).is_file())

        hook = f"printf '%s' \"$AGENT_BUS_TEXT\" >> {INBOX}"
        proc = cli(
            "up", "--agent-id", "bob", "--adapter", "command", "--agent-type", "command",
            "--notify-cmd", hook, "--poll", "0.2", "--heartbeat", "1",
            "--sweep", "60", "--outbox-flush", "0",
        )
        bob = jout(proc)
        check("bob up succeeds", proc.returncode == 0 and bob.get("ok") is True, proc.stderr[:200])

        print("B idempotent up")
        proc = cli("up", "--agent-id", "alice", "--adapter", "command", "--agent-type", "command")
        again = jout(proc)
        check("second up reports already_running", proc.returncode == 0 and again.get("already_running") is True, proc.stderr[:160])
        check("same pid is kept", again.get("notifier", {}).get("pid") == alice.get("notifier", {}).get("pid"))

        print("C delivery")
        proc = cli("send", "--from", "alice", "--to", "bob", "--text", "up probe hello")
        check("send succeeds", proc.returncode == 0 and jout(proc).get("ok") is True, proc.stderr[:160])
        delivered = wait_for(lambda: INBOX.is_file() and "up probe hello" in INBOX.read_text(), timeout=10)
        check("bob's command adapter received the text", bool(delivered))

        proc = cli("agents")
        agents = {a["agent_id"]: a for a in jout(proc).get("agents", [])}
        check("alice is active", agents.get("alice", {}).get("status") == "active")
        check("bob is active", agents.get("bob", {}).get("status") == "active")

        print("D down")
        for agent in ("alice", "bob"):
            proc = cli("down", "--agent-id", agent)
            check(f"{agent} down succeeds", proc.returncode == 0 and jout(proc).get("ok") is True, proc.stderr[:160])
        time.sleep(0.3)
        check("alice notifier is gone", not pid_alive(int(alice["notifier"]["pid"])))
        check("bob notifier is gone", not pid_alive(int(bob["notifier"]["pid"])))
        check("pid files removed", not (HOME / "run" / "notifier-alice.json").exists())

        print(f"\nRESULT: {'ALL PASS' if not FAIL else str(len(FAIL)) + ' FAILED'}")
        return 0 if not FAIL else 1
    finally:
        cli("down", "--agent-id", "alice")
        cli("down", "--agent-id", "bob")
        shutil.rmtree(ROOT, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
