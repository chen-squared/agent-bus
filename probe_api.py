#!/usr/bin/env python3
"""Smoke-test the embeddable `from buslib import Bus` surface."""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path

from buslib import Bus

ROOT = Path(tempfile.mkdtemp(prefix="bus-api-"))
INBOX = ROOT / "bob.inbox"
FAIL: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


def wait_for(predicate, timeout: float = 10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    return None


def main() -> int:
    alice: Bus | None = None
    bob: Bus | None = None
    try:
        home = str(ROOT / "home")
        alice = Bus("alice", home=home)
        print("A home + start")
        info = alice.home()
        check("home is isolated", info.get("home") == str(Path(home).resolve()), str(info.get("home")))
        started = alice.start(adapter="command", agent_type="command", notify_cmd="true")
        check("alice starts through Bus.start", started.get("ok") is True, str(started)[:200])

        bob = Bus("bob", home=home)
        hook = f"printf '%s' \"$AGENT_BUS_TEXT\" >> {INBOX}"
        started = bob.start(adapter="command", agent_type="command", notify_cmd=hook)
        check("bob starts through Bus.start", started.get("ok") is True, str(started)[:200])
        check("idempotent start keeps running", bob.start(adapter="command", agent_type="command").get("already_running") is True)

        print("B send + discovery")
        sent = alice.send("bob", "api hello")
        check("send returns ok", sent.get("ok") is True, str(sent)[:200])
        check("bob's hook received text", bool(wait_for(lambda: INBOX.is_file() and "api hello" in INBOX.read_text())))
        agents = alice.agents()
        statuses = {a["agent_id"]: a["status"] for a in agents.get("agents", [])}
        check("alice is active", statuses.get("alice") == "active", str(statuses))
        check("bob is active", statuses.get("bob") == "active", str(statuses))

        print("C groups through the API")
        check("group creates", alice.group_create("api-group", rate_seconds=0).get("ok") is True)
        check("bob joins", bob.group_join("api-group").get("ok") is True)
        for i in range(3):
            check(f"alice group send {i}", alice.group_send("api-group", f"g{i}").get("ok") is True)
        caught = bob.group_catch_up("api-group")
        check("catch-up drains the group", caught.get("messages_read") == 3 and caught.get("last_read_seq") == 3, str(caught))
        check("bob sends after catch-up", bob.group_send("api-group", "caught up").get("ok") is True)

        print("D stop")
        check("alice stops", alice.stop().get("ok") is True)
        check("bob stops", bob.stop().get("ok") is True)
        print(f"\nRESULT: {'ALL PASS' if not FAIL else str(len(FAIL)) + ' FAILED'}")
        return 0 if not FAIL else 1
    finally:
        for bus in (alice, bob):
            if bus is not None:
                try:
                    bus.stop()
                except Exception:
                    pass
        shutil.rmtree(ROOT, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
