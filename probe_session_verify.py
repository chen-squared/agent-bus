#!/usr/bin/env python3
"""Cover `agent-bus up --verify-session` with a deterministic fake opencode CLI."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = Path(tempfile.mkdtemp(prefix="bus-verify-"))
HOME = ROOT / "home"
FAKE = ROOT / "fake-opencode"
FAIL: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


def cli(*argv: str, outcome: str = "succeeded") -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["AGENT_BUS_HOME"] = str(HOME)
    env["PYTHONPATH"] = str(HERE)
    env["FAKE_OUTCOME"] = outcome
    return subprocess.run(
        [sys.executable, "-m", "buslib.cli", *argv],
        cwd=HERE, env=env, capture_output=True, text=True, timeout=20,
    )


def jout(proc: subprocess.CompletedProcess[str]) -> dict:
    try:
        return json.loads(proc.stdout)
    except (json.JSONDecodeError, TypeError):
        return {}


def main() -> int:
    alice: dict | None = None
    try:
        FAKE.write_text(
            "#!/bin/sh\n"
            'if [ "$1" = "api" ] && [ "$2" = "session.get" ]; then\n'
            '  if [ "$FAKE_OUTCOME" = "failed" ]; then\n'
            "    printf '%s' '{\"data\":{\"model\":{\"id\":\"fake-model\"},\"outcome\":\"failed\"}}'\n"
            "  else\n"
            "    printf '%s' '{\"data\":{\"model\":{\"id\":\"fake-model\"},\"outcome\":\"succeeded\"}}'\n"
            "  fi\n"
            "  exit 0\n"
            "fi\n"
            "exit 0\n"
        )
        FAKE.chmod(0o755)
        proc = cli("init")
        check("init succeeds", proc.returncode == 0, proc.stderr[:160])

        print("A strict failure is fatal before starting a notifier")
        proc = cli(
            "up", "--agent-id", "alice", "--adapter", "opencode", "--agent-type", "opencode",
            "--session-id", "ses_fake", "--cli", str(FAKE), "--verify-session", "--quiet",
            outcome="failed",
        )
        check("outcome=failed is rejected", proc.returncode != 0 and "outcome=failed" in proc.stderr, proc.stderr[:240])
        check("no notifier pid file was written", not (HOME / "run" / "notifier-alice.json").exists())

        print("B warning-only mode starts but reports the unhealthy session")
        proc = cli(
            "up", "--agent-id", "alice", "--adapter", "opencode", "--agent-type", "opencode",
            "--session-id", "ses_fake", "--cli", str(FAKE), "--quiet",
            "--poll", "0.2", "--heartbeat", "1", "--sweep", "60", "--outbox-flush", "0",
            outcome="failed",
        )
        warned = jout(proc)
        check("unverified start succeeds", proc.returncode == 0 and warned.get("ok") is True, proc.stderr[:200])
        check("warning mentions unhealthy outcome", "outcome=failed" in proc.stderr, proc.stderr[:200])
        check("session_probe is included", warned.get("session_probe", {}).get("outcome") == "failed")
        alice = warned
        cli("down", "--agent-id", "alice")

        print("C --verify-session accepts a healthy session")
        proc = cli(
            "up", "--agent-id", "alice", "--adapter", "opencode", "--agent-type", "opencode",
            "--session-id", "ses_fake", "--cli", str(FAKE), "--verify-session", "--quiet",
            "--poll", "0.2", "--heartbeat", "1", "--sweep", "60", "--outbox-flush", "0",
            outcome="succeeded",
        )
        healthy = jout(proc)
        check("healthy verify succeeds", proc.returncode == 0 and healthy.get("ok") is True, proc.stderr[:200])
        check("probe reports the model", healthy.get("session_probe", {}).get("model") == "fake-model")
        check("quiet output has a pid", bool(healthy.get("notifier", {}).get("pid")))
        check("quiet output omits full home info", "home" not in healthy)
        cli("down", "--agent-id", "alice")

        print("D non-opencode strict verification is rejected")
        proc = cli("up", "--agent-id", "alice", "--adapter", "command", "--agent-type", "command", "--verify-session")
        check("--verify-session rejects command adapter", proc.returncode != 0 and "only supported" in proc.stderr, proc.stderr[:160])

        print(f"\nRESULT: {'ALL PASS' if not FAIL else str(len(FAIL)) + ' FAILED'}")
        return 0 if not FAIL else 1
    finally:
        if alice is not None:
            cli("down", "--agent-id", "alice")
        shutil.rmtree(ROOT, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
