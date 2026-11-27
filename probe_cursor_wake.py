#!/usr/bin/env python3
"""Cursor wake: idle lines, and a hook insert while a turn is running."""

from __future__ import annotations

import fcntl
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from threading import Event
from unittest.mock import patch

from buslib.cursor_wake import HOLD_S, WAKE_PREFIX, Store, _is_watch_command, consume, default_state_dir, drain_once, hook

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
    store.initialize()
    old_id = OLD.split()[0]
    check("first watch keeps pointers already written", [row["id"] for row in store.pending(store.read_state())] == [old_id])
    consume(store, [old_id])

    new_id = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
    store.inbox.write_text(OLD + NEW + NEW)
    base = 1_000_000.0
    store.write_state({**store.read_state(), "phase": "running", "phase_at": base})
    check("a live turn does not print", drain_once(store, now=base + 10) == [])
    stuck = drain_once(store, now=base + HOLD_S + 1)
    check("a stuck running phase still wakes", [row["id"] for row in stuck] == [new_id])
    check("the same pointer is not repeated inside the hold", drain_once(store, now=base + HOLD_S + 1) == [])
    again = drain_once(store, now=base + 2 * HOLD_S + 2)
    check("an unread pointer wakes again after the hold", [row["id"] for row in again] == [new_id])
    consume(store, [new_id])

    hook_id = "hook-message"
    hook_pointer = f"{hook_id} thread other@peer\n"
    store.inbox.write_text(NEW + hook_pointer)
    running = hook(store, {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-1",
        "generation_id": "g1",
        "tool_input": {"command": f"python -m buslib.cursor_wake watch --agent-id dev-cursor --state-dir {root}"},
    })
    check("postToolUse names the unread pointer", hook_id in running.get("additional_context", ""))
    check("a fresh heartbeat is quiet", drain_once(store) == [])
    check("another conversation gets nothing", hook(store, {"hook_event_name": "postToolUse", "conversation_id": "other", "generation_id": "g9", "tool_input": {"command": "echo hi"}}) == {})
    check("the same generation is not inserted twice", hook(store, {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-1",
        "generation_id": "g1",
        "tool_input": {"command": "echo later"},
    }) == {})
    second_id = "new-in-the-same-turn"
    store.inbox.write_text(NEW + hook_pointer + f"{second_id} thread other@peer\n")
    new_notice = hook(store, {
        "hook_event_name": "postToolUse", "conversation_id": "conv-1", "generation_id": "g1",
    }).get("additional_context", "")
    check("a changing pending list inserts only new IDs", second_id in new_notice and hook_id not in new_notice)

    stopped = hook(store, {"hook_event_name": "stop", "conversation_id": "conv-1", "status": "completed"})
    check("stop only marks the conversation idle", stopped == {} and store.read_state()["phase"] == "stopped")
    ended = hook(store, {"hook_event_name": "sessionEnd", "conversation_id": "conv-1"})
    check("sessionEnd sends nothing", ended == {})
    other = hook(store, {"hook_event_name": "sessionEnd", "session_id": "someone-else"})
    check("another session ending does not unbind this one", other == {} and store.read_state()["conversation_id"] == "conv-1")
    check("idle watcher does not repeat pointers just supplied by the hook", drain_once(store) == [])

    consume(store, [hook_id, second_id])
    check("consume drops that pointer", store.pending(store.read_state()) == [])
    check("a consumed pointer does not wake", drain_once(store) == [])

    fresh_id = "cccccccc-bbbb-cccc-dddd-eeeeeeeeeeee"
    fresh = f"{fresh_id} 11111111-2222-3333-4444-555555555555 other@peer\n"
    long_agent = "a" * 64
    long_group = "b" * 64
    digest_id = f"group-digest-{long_group}-9223372036854775807-{long_agent}"
    store.inbox.write_text(store.inbox.read_text() + fresh + f"{digest_id} {long_group} {long_agent}@{long_group}\n")
    capture = StringIO()
    with redirect_stdout(capture):
        woken = drain_once(store)
    check("an idle watcher batches direct and full-length digest IDs", [row["id"] for row in woken] == [fresh_id, digest_id])
    check("the batch generates one wake line", capture.getvalue().count(WAKE_PREFIX) == 1)
    check("the wake line is not printed twice", drain_once(store) == [])
    check("the watcher and hook share notice state", hook(store, {"hook_event_name": "postToolUse", "conversation_id": "conv-1"}) == {})
    consume(store, [fresh_id, digest_id])
    check("consume accepts full-length digest IDs", store.pending(store.read_state()) == [])

    # Two watchers reading while the first is emitting used to deliver twice.
    race_store = Store("dev-cursor", iso_root / "watch-race")
    race_store.initialize()
    race_store.inbox.write_text(NEW)
    emitting = Event()
    release = Event()

    def slow_emit(_store, _rows):
        emitting.set()
        release.wait(2)

    with patch("buslib.cursor_wake._emit", side_effect=slow_emit) as emit, ThreadPoolExecutor(2) as pool:
        first = pool.submit(drain_once, race_store)
        check("first watcher enters emission", emitting.wait(2))
        with race_store.lock_path.open("a") as contender:
            try:
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                held_during_emission = True
            else:
                held_during_emission = False
        check("a watcher holds the state lock while emitting", held_during_emission)
        second = pool.submit(drain_once, race_store)
        release.set()
        first.result(2)
        second.result(2)
        check("concurrent watchers claim one wake", emit.call_count == 1)

    # Multi-session isolation: global hooks fire for every Cursor session, so
    # each hook must only bind and consume commands targeting its own agent/dir.
    cursor_dir = iso_root / "dev-cursor"
    reviewer_dir = iso_root / "dev-reviewer"
    cursor_store = Store("dev-cursor", cursor_dir)
    reviewer_store = Store("dev-reviewer", reviewer_dir)
    cursor_store.initialize()
    reviewer_store.initialize()

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
    implicit = "agent-bus cursor watch --agent-id dev-reviewer"
    check("omitted state-dir matches only the default store", not _is_watch_command(reviewer_store, implicit)
          and _is_watch_command(Store("dev-reviewer", default_state_dir("dev-reviewer")), implicit))
    env_watch = f"env AGENT_BUS_WAKE_HOME={reviewer_dir} bash -lc '{implicit}'"
    check("a nested watch inherits its state-dir override", _is_watch_command(reviewer_store, env_watch))

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

    # A hook can see a command without it ever having been executed.
    cur_msg = "eeeeeeee-bbbb-cccc-dddd-eeeeeeeeeeee"
    cursor_store.inbox.write_text(f"{cur_msg} thread sender\n")
    ev_consume = {
        "hook_event_name": "postToolUse",
        "conversation_id": "conv-cursor",
        "generation_id": "gc3",
        "tool_input": {
            "command": (
                "false "
                f"&& python -m buslib.cursor_wake consume --agent-id dev-cursor --state-dir {cursor_dir} {cur_msg}"
            )
        },
    }
    hook(cursor_store, ev_consume)
    hook(reviewer_store, ev_consume)
    check("a shell mention of consume leaves the pointer pending", cur_msg not in cursor_store.read_state()["consumed"])
    consume(cursor_store, [cur_msg])
    check("explicit consume updates only its target store", cur_msg in cursor_store.read_state()["consumed"]
          and cur_msg not in reviewer_store.read_state()["consumed"])

    # Legacy wake.py wrapper only matches dev-cursor.
    legacy_root = Path.home() / ".local" / "share" / "dev-cursor-bus"
    legacy_store = Store("dev-cursor", legacy_root)
    check("legacy wake.py matches dev-cursor at dev-cursor-bus", _is_watch_command(legacy_store, f"python {legacy_root / 'wake.py'}"))
    check("legacy wake.py --pending is not a watch command", not _is_watch_command(legacy_store, f"python {legacy_root / 'wake.py'} --pending"))
    check("legacy wake.py does not match dev-reviewer", not _is_watch_command(reviewer_store, f"python {legacy_root / 'wake.py'}"))
    check("legacy wake.py --consume does not bind", not _is_watch_command(legacy_store, f"python {legacy_root / 'wake.py'} --consume {cur_msg}"))

    if FAIL:
        raise SystemExit(f"{len(FAIL)} failed")
    print("CURSOR_WAKE_OK")


if __name__ == "__main__":
    main()
