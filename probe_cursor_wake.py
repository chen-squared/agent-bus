#!/usr/bin/env python3
"""Cursor wake: idle lines, and a hook insert while a turn is running."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from buslib.cursor_wake import HOLD_S, Store, _consume_ids, _is_watch_command, consume, drain_once, hook

FAIL: list[str] = []
NEW = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee 11111111-2222-3333-4444-555555555555 other@peer\n"
OLD = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee 11111111-2222-3333-4444-555555555555 dev71-opencode@dev71\n"


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="cursor-wake-"))
    iso_root = Path(tempfile.mkdtemp(prefix="cursor-wake-iso-"))
    try:
        _run(root, iso_root)
    finally:
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(iso_root, ignore_errors=True)


def _run(root: Path, iso_root: Path) -> None:
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

    # Multi-session isolation: global hooks fire for every Cursor session, so
    # each hook must only bind and consume commands targeting its own agent/dir.
    cursor_dir = iso_root / "dev-cursor"
    reviewer_dir = iso_root / "dev-reviewer"
    cursor_store = Store("dev-cursor", cursor_dir)
    reviewer_store = Store("dev-reviewer", reviewer_dir)
    cursor_store.seed()
    reviewer_store.seed()

    # Non-watch commands never bind either session.
    for cmd in (
        f"python -u -m buslib.cursor_wake pending --agent-id dev-reviewer --state-dir {reviewer_dir}",
        f"python -u -m buslib.cursor_wake hook --agent-id dev-cursor --state-dir {cursor_dir}",
        "pgrep -af cursor_wake",
        "cat buslib/cursor_wake.py",
        "python -c 'import buslib.cursor_wake'",
        f"python {Path.home() / '.local/share/dev-cursor-bus/wake.py'} --pending",
    ):
        ev = {
            "hook_event_name": "postToolUse",
            "conversation_id": "conv-noise",
            "generation_id": "g-noise",
            "tool_input": {"command": cmd},
        }
        hook(cursor_store, ev)
        hook(reviewer_store, ev)
    check("non-watch commands do not bind cursor_store", cursor_store.read_state()["conversation_id"] == "")
    check("non-watch commands do not bind reviewer_store", reviewer_store.read_state()["conversation_id"] == "")

    # Starting watch for dev-cursor binds ONLY dev-cursor, leaving dev-reviewer untouched.
    ev_cursor = {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-cursor",
        "generation_id": "gc1",
        "tool_input": {"command": f"/venv/bin/python -u -m buslib.cursor_wake watch --agent-id dev-cursor --state-dir {cursor_dir}"},
    }
    hook(cursor_store, ev_cursor)
    hook(reviewer_store, ev_cursor)
    check("watch binds matching dev-cursor store", cursor_store.read_state()["conversation_id"] == "conv-cursor")
    check("dev-cursor watch does not hijack dev-reviewer", reviewer_store.read_state()["conversation_id"] == "")

    # Starting watch for dev-reviewer via chained shell command binds ONLY dev-reviewer.
    ev_reviewer = {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-reviewer",
        "generation_id": "gr1",
        "tool_input": {
            "command": f"cd /tmp && bash -lc 'agent-bus cursor watch --agent-id=dev-reviewer --state-dir={reviewer_dir}'"
        },
    }
    hook(cursor_store, ev_reviewer)
    hook(reviewer_store, ev_reviewer)
    check("dev-reviewer watch binds dev-reviewer", reviewer_store.read_state()["conversation_id"] == "conv-reviewer")
    check("dev-reviewer watch does not overwrite dev-cursor", cursor_store.read_state()["conversation_id"] == "conv-cursor")

    # Wrong --state-dir for the same agent-id must not bind.
    other_dir = iso_root / "other-reviewer"
    ev_wrong_dir = {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-wrong-dir",
        "generation_id": "gw1",
        "tool_input": {"command": f"python -m buslib.cursor_wake watch --agent-id dev-reviewer --state-dir {other_dir}"},
    }
    hook(reviewer_store, ev_wrong_dir)
    check("mismatched state-dir does not rebind", reviewer_store.read_state()["conversation_id"] == "conv-reviewer")

    # Unrelated pending message for dev-reviewer is NOT injected into dev-cursor's turn.
    rev_msg = "dddddddd-bbbb-cccc-dddd-eeeeeeeeeeee"
    reviewer_store.inbox.write_text(f"{rev_msg} 11111111-2222-3333-4444-555555555555 dev-opencode\n")
    cross = hook(reviewer_store, {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-cursor",
        "generation_id": "gc2",
        "tool_input": {"command": "ls"},
    })
    check("dev-cursor turn does not receive dev-reviewer mail", cross == {})
    direct = hook(reviewer_store, {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-reviewer",
        "generation_id": "gr2",
        "tool_input": {"command": "ls"},
    })
    check("dev-reviewer turn receives its own mail", rev_msg in direct.get("additional_context", ""))

    # Consume filtering: chained send + consume only marks target store and ignores unrelated words.
    cur_msg = "eeeeeeee-bbbb-cccc-dddd-eeeeeeeeeeee"
    ev_consume = {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-cursor",
        "generation_id": "gc3",
        "tool_input": {
            "command": (
                f"python -m buslib.cli send --from dev-cursor --to dev-opencode --text ok --idempotency-key k1 "
                f"&& python -m buslib.cursor_wake consume --agent-id dev-cursor --state-dir {cursor_dir} {cur_msg}"
            )
        },
    }
    hook(cursor_store, ev_consume)
    hook(reviewer_store, ev_consume)
    check("matching consume marks cursor_store only", cur_msg in cursor_store.read_state()["consumed"])
    check("consume does not pollute cursor_store with send tokens", "k1" not in cursor_store.read_state()["consumed"] and "send" not in cursor_store.read_state()["consumed"])
    check("consume for dev-cursor does not touch dev-reviewer", cur_msg not in reviewer_store.read_state()["consumed"])

    # Legacy wake.py wrapper only matches dev-cursor.
    legacy_root = Path.home() / ".local" / "share" / "dev-cursor-bus"
    legacy_store = Store("dev-cursor", legacy_root)
    check("legacy wake.py matches dev-cursor at dev-cursor-bus", _is_watch_command(legacy_store, f"python {legacy_root / 'wake.py'}"))
    check("legacy wake.py --pending is not a watch command", not _is_watch_command(legacy_store, f"python {legacy_root / 'wake.py'} --pending"))
    check("legacy wake.py does not match dev-reviewer", not _is_watch_command(reviewer_store, f"python {legacy_root / 'wake.py'}"))
    check(
        "legacy wake.py --consume only targets dev-cursor",
        _consume_ids(legacy_store, f"python {legacy_root / 'wake.py'} --consume {cur_msg}") == [cur_msg]
        and _consume_ids(reviewer_store, f"python {legacy_root / 'wake.py'} --consume {cur_msg}") == [],
    )

    if FAIL:
        raise SystemExit(f"{len(FAIL)} failed")
    print("CURSOR_WAKE_OK")


if __name__ == "__main__":
    main()
