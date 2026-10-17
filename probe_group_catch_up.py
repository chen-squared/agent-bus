#!/usr/bin/env python3
"""Cover the group catch-up shortcut against the real paging/read clamps."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = Path(tempfile.mkdtemp(prefix="bus-catchup-"))
HOME = ROOT / "home"
FAIL: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


def cli(*argv: str, agent: str | None = None) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["AGENT_BUS_HOME"] = str(HOME)
    env["PYTHONPATH"] = str(HERE)
    if agent:
        env["AGENT_BUS_TOKEN_FILE"] = str(HOME / "tokens" / f"{agent}.token")
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
    try:
        check("init succeeds", cli("init").returncode == 0)
        check("alice registers", cli("register", "--agent-id", "alice", "--agent-type", "command").returncode == 0)
        check("bob registers", cli("register", "--agent-id", "bob", "--agent-type", "command").returncode == 0)
        proc = cli(
            "group", "create", "--from", "alice", "--group-id", "catch",
            "--rate-seconds", "0", "--ceiling-per-minute", "1000",
        )
        check("group creates", proc.returncode == 0, proc.stderr[:160])
        check("bob joins", cli("group", "join", "--group-id", "catch", "--agent-id", "bob").returncode == 0)

        for i in range(25):
            proc = cli("group", "send", "--from", "alice", "--group-id", "catch", "--text", f"m{i}")
            if proc.returncode != 0:
                check(f"alice send {i} succeeds", False, proc.stderr[:200])
                break
        else:
            check("alice sent 25 messages", True)

        print("history is readable before catch-up and does not move cursors")
        proc = cli("group", "history", "--group-id", "catch", "--agent-id", "bob", "--limit", "10", agent="bob")
        hist = jout(proc)
        seqs = [m.get("seq") for m in hist.get("messages", [])]
        check("history returns the newest page", proc.returncode == 0 and seqs == list(range(16, 26)), str(seqs))
        check("history reports older pages", hist.get("has_more") is True and hist.get("next_before_seq") == 16, str(hist)[:240])
        proc = cli("group", "history", "--group-id", "catch", "--agent-id", "bob", "--limit", "10", "--before-seq", "16", agent="bob")
        older = jout(proc)
        check("history pages backwards", [m.get("seq") for m in older.get("messages", [])] == list(range(6, 16)), str(older)[:240])
        proc = cli("group", "history", "--group-id", "catch", "--agent-id", "bob", "--limit", "100", agent="bob")
        full = jout(proc)
        check("history returns the whole store", full.get("count") == 25 and full.get("has_more") is False, str(full)[:240])
        check("history exposes senders", all(m.get("sender_agent") == "alice" for m in full.get("messages", [])), str(full)[:240])
        check("history zero limit is rejected", cli("group", "history", "--group-id", "catch", "--agent-id", "bob", "--limit", "0", agent="bob").returncode != 0)

        proc = cli("group", "catch-up", "--group-id", "catch", "--agent-id", "bob", "--limit", "10", agent="bob")
        out = jout(proc)
        check("catch-up succeeds", proc.returncode == 0 and out.get("ok") is True, proc.stderr[:240])
        check("paged through multiple pages", out.get("pages", 0) >= 3, str(out))
        check("counted all messages", out.get("messages_read") == 25, str(out))
        check("acked through latest", out.get("last_read_seq") == 25, str(out))
        check("pagination warnings are suppressed", "returned 10 of" not in proc.stderr, proc.stderr[:240])

        proc = cli("group", "send", "--from", "bob", "--group-id", "catch", "--text", "caught up", agent="bob")
        check("bob can send immediately after catch-up", proc.returncode == 0 and jout(proc).get("seq") == 26, proc.stderr[:200])

        proc = cli("group", "catch-up", "--group-id", "catch", "--agent-id", "bob", agent="bob")
        check("second catch-up is harmless", proc.returncode == 0 and jout(proc).get("latest_seq") == 26, proc.stderr[:160])
        check("zero page size is rejected", cli("group", "catch-up", "--group-id", "catch", "--agent-id", "bob", "--limit", "0", agent="bob").returncode != 0)

        print(f"\nRESULT: {'ALL PASS' if not FAIL else str(len(FAIL)) + ' FAILED'}")
        return 0 if not FAIL else 1
    finally:
        shutil.rmtree(ROOT, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
