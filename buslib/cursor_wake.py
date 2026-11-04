"""Cursor wake for an agent that has no session.prompt API.

The notifier appends one pointer line to inbox.txt.

- ``watch`` prints ``AGENT_LOOP_WAKE_agentbus`` for each unconsumed pointer.
  A turn that is still receiving hook events keeps it quiet. If those events
  stop, ``watch`` prints anyway after ``HOLD_S`` and repeats until ``consume``.
- ``hook`` runs for every Cursor session, and only the conversation that
  started ``watch`` gets mail. ``postToolUse`` inserts unread pointers into
  the turn already in progress and refreshes that heartbeat. ``stop`` and
  ``sessionEnd`` mark the conversation idle so ``watch`` can print.

Claude Code uses ``bus_wake.sh`` (a stop hook that exits 2). That protocol is
not this one. The pointer file is the shared piece.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import shlex
import sys
import time
from pathlib import Path

WAKE_PREFIX = "AGENT_LOOP_WAKE_agentbus "
ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,80}")
FROM_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9@_.-]{0,120}")
WATCH_MARKERS = ("cursor_wake", "cursor watch", "dev-cursor-bus/wake.py")
HOLD_S = 60.0


def default_state_dir(agent_id: str) -> Path:
    override = os.environ.get("AGENT_BUS_WAKE_HOME")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".local" / "share" / "agent-bus-cursor" / agent_id


class Store:
    def __init__(self, agent_id: str, state_dir: Path):
        self.agent_id = agent_id
        self.state_dir = state_dir
        self.inbox = state_dir / "inbox.txt"
        self.state_path = state_dir / "wake-state.json"
        self.lock_path = state_dir / "wake.lock"
        self.bus_cli = f"{sys.executable} -m buslib.cli"
        self.wake_cli = f"{sys.executable} -m buslib.cursor_wake"

    def lock(self):
        self.state_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.state_dir, 0o700)
        handle = open(self.lock_path, "a+")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return handle

    def read_state(self) -> dict:
        try:
            data = json.loads(self.state_path.read_text())
        except (OSError, json.JSONDecodeError):
            data = {}
        if not isinstance(data, dict):
            data = {}
        out = {
            "seeded": bool(data.get("seeded")),
            "conversation_id": data.get("conversation_id") if isinstance(data.get("conversation_id"), str) else "",
            "noticed_generation": data.get("noticed_generation") if isinstance(data.get("noticed_generation"), str) else "",
            "phase": data.get("phase") if data.get("phase") in {"running", "stopped"} else "stopped",
            "phase_at": float(data["phase_at"]) if isinstance(data.get("phase_at"), (int, float)) else 0.0,
        }
        consumed = data.get("consumed")
        out["consumed"] = [v for v in consumed if isinstance(v, str)] if isinstance(consumed, list) else []
        wake_at = {}
        raw_wake = data.get("wake_at")
        if isinstance(raw_wake, dict):
            for key, value in raw_wake.items():
                if isinstance(key, str) and isinstance(value, (int, float)):
                    wake_at[key] = float(value)
        out["wake_at"] = wake_at
        return out

    def write_state(self, data: dict) -> None:
        tmp = self.state_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2) + "\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.state_path)
        os.chmod(self.state_path, 0o600)

    def pointers(self) -> list[dict[str, str]]:
        try:
            text = self.inbox.read_text()
        except OSError:
            return []
        rows = []
        for line in text.splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            if ID_RE.fullmatch(parts[0]) is None or ID_RE.fullmatch(parts[1]) is None or FROM_RE.fullmatch(parts[2]) is None:
                continue
            rows.append({"id": parts[0], "thread": parts[1], "from": parts[2]})
        return rows

    def pending(self, data: dict) -> list[dict[str, str]]:
        done = set(data["consumed"])
        return [row for row in self.pointers() if row["id"] not in done]

    def seed(self) -> None:
        handle = self.lock()
        try:
            data = self.read_state()
            if data.get("seeded"):
                return
            data["consumed"] = [row["id"] for row in self.pointers()]
            data["seeded"] = True
            data["phase"] = "stopped"
            self.write_state(data)
        finally:
            handle.close()

    def prompt(self) -> str:
        state = shlex.quote(str(self.state_dir))
        return (
            f"A new agent-bus message arrived for {self.agent_id}. "
            f"Read every unconsumed pointer in {self.inbox} "
            "(message_id thread_id from_agent). For each one run "
            f"{self.bus_cli} message show --agent-id {self.agent_id} --message-id <id>. "
            "If the id starts with group-digest-, read it with "
            f"{self.bus_cli} group messages --group-id <thread_id> --agent-id {self.agent_id}. "
            f"Do the work and reply with {self.bus_cli} send --from {self.agent_id}. "
            f"Then run {self.wake_cli} consume --agent-id {self.agent_id} --state-dir {state} <id>. "
            "If this id is already being handled in this session, do not start a second copy."
        )

    def notice(self, rows: list[dict[str, str]]) -> str:
        state = shlex.quote(str(self.state_dir))
        lines = [f"agent-bus: unread message(s) for {self.agent_id}."]
        for row in rows[:5]:
            lines.append(f"- {row['id']} thread {row['thread']} from {row['from']}")
        if len(rows) > 5:
            lines.append(f"- {len(rows) - 5} more in {self.inbox}")
        lines.append(
            f"Read each with {self.bus_cli} message show --agent-id {self.agent_id} --message-id <id>, "
            f"reply with {self.bus_cli} send --from {self.agent_id}, then "
            f"{self.wake_cli} consume --agent-id {self.agent_id} --state-dir {state} <id>."
        )
        return "\n".join(lines)

    def notify_cmd(self) -> str:
        inbox = shlex.quote(str(self.inbox))
        return f"printf '%s %s %s\\n' '{{message_id}}' '{{thread_id}}' '{{from_agent}}' >> {inbox}"

    def log(self, text: str) -> None:
        try:
            with (self.state_dir / "hook.log").open("a") as handle:
                handle.write(time.strftime("%Y-%m-%dT%H:%M:%S%z ") + text + "\n")
        except OSError:
            return


def _emit(store: Store, row: dict[str, str]) -> None:
    sys.stdout.write(
        WAKE_PREFIX
        + json.dumps(
            {"prompt": store.prompt(), "client": store.agent_id, "id": row["id"], "from": row["from"]},
            ensure_ascii=False,
        )
        + "\n"
    )
    sys.stdout.flush()


def drain_once(store: Store, now: float | None = None) -> list[dict[str, str]]:
    """Print a wake line for each unconsumed pointer that is due.

    A turn with a recent hook event is left to ``postToolUse``. After
    ``HOLD_S`` without one, or once the turn is idle, print. Repeat until
    ``consume`` so a missed wake does not wait on a person.
    """
    now = time.time() if now is None else float(now)
    handle = store.lock()
    emitted: list[dict[str, str]] = []
    try:
        data = store.read_state()
        live = data.get("phase") == "running" and now - data["phase_at"] < HOLD_S
        rows = []
        if not live:
            wake_at = data["wake_at"]
            rows = [row for row in store.pending(data) if now - wake_at.get(row["id"], 0.0) >= HOLD_S]
    finally:
        handle.close()
    for row in rows:
        _emit(store, row)
    if not rows:
        return []
    handle = store.lock()
    try:
        data = store.read_state()
        for row in rows:
            data["wake_at"][row["id"]] = now
            emitted.append(row)
        store.write_state(data)
    finally:
        handle.close()
    return emitted


def watch(store: Store) -> None:
    store.seed()
    while True:
        try:
            drain_once(store)
        except Exception:
            pass
        time.sleep(0.5)


def _tool_command(event: dict) -> str:
    tool_input = event.get("tool_input")
    if isinstance(tool_input, str):
        try:
            tool_input = json.loads(tool_input)
        except json.JSONDecodeError:
            return tool_input
    if isinstance(tool_input, dict):
        command = tool_input.get("command")
        if isinstance(command, str):
            return command
    return ""


def _consume_ids(command: str) -> list[str]:
    try:
        tokens = shlex.split(command)
    except ValueError:
        return []
    if "--consume" not in tokens and "consume" not in tokens:
        return []
    ids: list[str] = []
    skip_next = False
    for token in tokens:
        if skip_next:
            skip_next = False
            continue
        if token in {"--agent-id", "--state-dir"}:
            skip_next = True
            continue
        if token.startswith("-") or token in {"consume", "watch", "hook", "cursor"}:
            continue
        if ID_RE.fullmatch(token):
            ids.append(token)
    return ids


def hook(store: Store, event: dict | None = None) -> dict:
    if event is None:
        try:
            event = json.loads(sys.stdin.read() or "{}")
        except json.JSONDecodeError:
            event = {}
    if not isinstance(event, dict):
        event = {}
    name = event.get("hook_event_name")
    conversation_id = event.get("conversation_id") or event.get("session_id") or ""
    if not isinstance(conversation_id, str):
        conversation_id = ""
    out: dict = {}
    handle = store.lock()
    try:
        data = store.read_state()
        command = _tool_command(event)
        changed = False
        if name == "postToolUse" and any(marker in command for marker in WATCH_MARKERS) and "--consume" not in command and conversation_id:
            if data.get("conversation_id") != conversation_id:
                data["conversation_id"] = conversation_id
                changed = True
                store.log(f"bind conversation={conversation_id}")
        for message_id in _consume_ids(command):
            if message_id not in data["consumed"]:
                data["consumed"].append(message_id)
                changed = True
        pending = store.pending(data)
        bound = bool(conversation_id) and data.get("conversation_id") == conversation_id
        if bound and name == "postToolUse":
            data["phase"] = "running"
            data["phase_at"] = time.time()
            changed = True
            if pending:
                generation = str(event.get("generation_id") or "")
                signature = generation + "|" + ",".join(row["id"] for row in pending)
                if signature != data.get("noticed_generation"):
                    data["noticed_generation"] = signature
                    out = {"additional_context": store.notice(pending)}
                    store.log(f"insert {','.join(row['id'] for row in pending)}")
        elif bound and name in {"stop", "sessionEnd"}:
            if data.get("phase") != "stopped":
                data["phase"] = "stopped"
                data["phase_at"] = time.time()
                changed = True
                if name == "sessionEnd":
                    store.log("sessionEnd idle")
        if changed:
            store.write_state(data)
    finally:
        handle.close()
    return out


def hook_main(store: Store) -> None:
    try:
        sys.stdout.write(json.dumps(hook(store), ensure_ascii=False))
    except Exception:
        sys.stdout.write("{}")


def consume(store: Store, ids: list[str]) -> None:
    handle = store.lock()
    try:
        data = store.read_state()
        for message_id in ids:
            if ID_RE.fullmatch(message_id) and message_id not in data["consumed"]:
                data["consumed"].append(message_id)
        store.write_state(data)
    finally:
        handle.close()


def _store_from_args(args: argparse.Namespace) -> Store:
    state_dir = Path(args.state_dir).expanduser() if args.state_dir else default_state_dir(args.agent_id)
    return Store(args.agent_id, state_dir)


def cmd_watch(args: argparse.Namespace) -> int:
    watch(_store_from_args(args))
    return 0


def cmd_hook(args: argparse.Namespace) -> int:
    hook_main(_store_from_args(args))
    return 0


def cmd_consume(args: argparse.Namespace) -> int:
    consume(_store_from_args(args), args.message_ids)
    print(json.dumps({"ok": True, "consumed": args.message_ids}))
    return 0


def cmd_pending(args: argparse.Namespace) -> int:
    store = _store_from_args(args)
    handle = store.lock()
    try:
        rows = store.pending(store.read_state())
    finally:
        handle.close()
    print(json.dumps({"ok": True, "pending": rows}))
    return 0


def cmd_notify_cmd(args: argparse.Namespace) -> int:
    print(_store_from_args(args).notify_cmd())
    return 0


def _add_common(command: argparse.ArgumentParser) -> None:
    command.add_argument("--agent-id", required=True)
    command.add_argument("--state-dir", help="pointer directory (default: ~/.local/share/agent-bus-cursor/<agent-id>)")


def add_parser(sub: argparse._SubParsersAction) -> None:
    parser = sub.add_parser("cursor", help="wake a Cursor conversation from the command adapter")
    commands = parser.add_subparsers(dest="cursor_command", required=True)
    watch_p = commands.add_parser("watch", help="print AGENT_LOOP_WAKE_agentbus while the conversation is idle")
    _add_common(watch_p)
    watch_p.set_defaults(func=cmd_watch)
    hook_p = commands.add_parser("hook", help="Cursor postToolUse, stop, and sessionEnd; reads the event on stdin")
    _add_common(hook_p)
    hook_p.set_defaults(func=cmd_hook)
    consume_p = commands.add_parser("consume", help="mark pointers read so they are not delivered again")
    _add_common(consume_p)
    consume_p.add_argument("message_ids", nargs="+")
    consume_p.set_defaults(func=cmd_consume)
    pending_p = commands.add_parser("pending", help="list unconsumed pointers")
    _add_common(pending_p)
    pending_p.set_defaults(func=cmd_pending)
    notify_p = commands.add_parser("notify-cmd", help="print the --notify-cmd value for agent-bus up")
    _add_common(notify_p)
    notify_p.set_defaults(func=cmd_notify_cmd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="buslib.cursor_wake", allow_abbrev=False)
    sub = parser.add_subparsers(dest="cursor_command", required=True)
    for name, func, ids in (
        ("watch", cmd_watch, False),
        ("hook", cmd_hook, False),
        ("consume", cmd_consume, True),
        ("pending", cmd_pending, False),
        ("notify-cmd", cmd_notify_cmd, False),
    ):
        command = sub.add_parser(name)
        command.add_argument("--agent-id", required=True)
        command.add_argument("--state-dir")
        if ids:
            command.add_argument("message_ids", nargs="+")
        command.set_defaults(func=func)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
