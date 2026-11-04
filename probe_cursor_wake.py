#!/usr/bin/env python3
"""Cursor wake: idle lines, and a hook insert while a turn is running."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from buslib.cursor_wake import HOLD_S, Store, consume, drain_once, hook

FAIL: list[str] = []
NEW = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee 11111111-2222-3333-4444-555555555555 other@peer\n"
OLD = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee 11111111-2222-3333-4444-555555555555 dev71-opencode@dev71\n"


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="cursor-wake-"))
    store = Store("dev-cursor", root)
    store.inbox.write_text(OLD)
    store.seed()
    check("first watch consumes the inbox it found", store.pending(store.read_state()) == [])

    new_id = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
    store.inbox.write_text(OLD + NEW)
    base = 1_000_000.0
    store.write_state({**store.read_state(), "phase": "running", "phase_at": base})
    check("a live turn does not print", drain_once(store, now=base + 10) == [])
    stuck = drain_once(store, now=base + HOLD_S + 1)
    check("a stuck running phase still wakes", [row["id"] for row in stuck] == [new_id])
    check("the same pointer is not repeated inside the hold", drain_once(store, now=base + HOLD_S + 1) == [])
    again = drain_once(store, now=base + 2 * HOLD_S + 2)
    check("an unread pointer wakes again after the hold", [row["id"] for row in again] == [new_id])

    running = hook(store, {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-1",
        "generation_id": "g1",
        "tool_input": {"command": "python -m buslib.cursor_wake watch --agent-id dev-cursor"},
    })
    check("postToolUse names the unread pointer", new_id in running.get("additional_context", ""))
    check("a fresh heartbeat is quiet", drain_once(store) == [])
    check("another conversation gets nothing", hook(store, {"hook_event_name": "postToolUse", "conversation_id": "other", "generation_id": "g9", "tool_input": {"command": "echo hi"}}) == {})
    check("the same generation is not inserted twice", hook(store, {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-1",
        "generation_id": "g1",
        "tool_input": {"command": "echo later"},
    }) == {})

    stopped = hook(store, {"hook_event_name": "stop", "conversation_id": "conv-1", "status": "completed"})
    check("stop only marks the conversation idle", stopped == {} and store.read_state()["phase"] == "stopped")
    ended = hook(store, {"hook_event_name": "sessionEnd", "conversation_id": "conv-1"})
    check("sessionEnd sends nothing", ended == {})
    other = hook(store, {"hook_event_name": "sessionEnd", "session_id": "someone-else"})
    check("another session ending does not unbind this one", other == {} and store.read_state()["conversation_id"] == "conv-1")

    consume(store, [new_id])
    check("consume drops that pointer", store.pending(store.read_state()) == [])
    check("a consumed pointer does not wake", drain_once(store) == [])

    fresh_id = "cccccccc-bbbb-cccc-dddd-eeeeeeeeeeee"
    fresh = f"{fresh_id} 11111111-2222-3333-4444-555555555555 other@peer\n"
    store.inbox.write_text(store.inbox.read_text() + fresh)
    woken = drain_once(store)
    check("an idle watcher prints the new pointer", [row["id"] for row in woken] == [fresh_id])
    check("the wake line is not printed twice", drain_once(store) == [])

    if FAIL:
        raise SystemExit(f"{len(FAIL)} failed")
    print("CURSOR_WAKE_OK")


if __name__ == "__main__":
    main()
