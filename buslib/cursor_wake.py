"""Cursor wake for an agent that has no session.prompt API.

The notifier appends one pointer line to inbox.txt.

- ``watch`` prints ``AGENT_LOOP_WAKE_agentbus`` to stdout for a batch of due
  pointers. Run it unbuffered (``python -u``) inside the target Cursor
  conversation with background output monitoring (``notify_on_output`` on
  ``^AGENT_LOOP_WAKE_agentbus``), and do NOT redirect stdout or detach with
  ``&``/``nohup``. A turn still receiving hook events keeps it quiet. If those
  events stop, ``watch`` retries after ``HOLD_S`` until ``consume``. The watcher
  and hook share notice timestamps for each message ID.
- ``hook`` runs for every Cursor session, and only the conversation that
  started ``watch`` for that exact ``--agent-id`` and effective state directory
  binds and gets mail. ``postToolUse`` inserts new pointers into
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
# Group digest IDs combine a prefix, two 64-character IDs and a SQLite seq.
MESSAGE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,161}")
FROM_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9@_.-]{0,128}")
HOLD_S = 60.0
NOTICE_LIMIT = 25

_ENV_ASSIGN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=.*")
_PYTHON_EXE_RE = re.compile(r"python(?:\d+(?:\.\d+)*)?")
_REDIR_OPS = {">", ">>", "<", "<<", "<<<", ">&", "<&", "<>", "&>", "&>>", ">|"}
_CONTROL_OPS = {";", "&&", "||", "|", "&", "\n", "(", ")", ";;"}
_CURSOR_SUBCOMMANDS = {"watch", "hook", "consume", "pending", "notify-cmd"}
_SHELL_NAMES = {"sh", "bash", "zsh", "dash"}


def default_state_dir(agent_id: str, override: str | None = None) -> Path:
    if override is None:
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
        self.bus_cli = shlex.join([sys.executable, "-m", "buslib.cli"])
        self.wake_cli = shlex.join([sys.executable, "-m", "buslib.cursor_wake"])

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
        legacy_notice = data.get("noticed_generation")
        if isinstance(legacy_notice, str):
            for message_id in legacy_notice.rpartition("|")[2].split(","):
                if MESSAGE_ID_RE.fullmatch(message_id):
                    wake_at[message_id] = max(wake_at.get(message_id, 0), out["phase_at"])
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
        rows = {}
        for line in text.splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            if MESSAGE_ID_RE.fullmatch(parts[0]) is None or ID_RE.fullmatch(parts[1]) is None or FROM_RE.fullmatch(parts[2]) is None:
                continue
            rows.setdefault(parts[0], {"id": parts[0], "thread": parts[1], "from": parts[2]})
        return list(rows.values())

    def pending(self, data: dict) -> list[dict[str, str]]:
        done = set(data["consumed"])
        return [row for row in self.pointers() if row["id"] not in done]

    def initialize(self) -> None:
        with self.lock():
            data = self.read_state()
            if not data["seeded"]:
                data["seeded"] = True
                self.write_state(data)

    def notice(self, rows: list[dict[str, str]]) -> str:
        state = shlex.quote(str(self.state_dir))
        lines = [f"agent-bus has unread messages for {self.agent_id}."]
        for row in rows:
            lines.append(f"- {row['id']} thread {row['thread']} from {row['from']}")
        lines.append(
            f"Read each with {self.bus_cli} message show --agent-id {self.agent_id} --message-id <id>. "
            f"For a group digest, run {self.bus_cli} group messages --group-id <thread> --agent-id {self.agent_id} "
            "and acknowledge through the served_through_seq of each page read. "
            f"Handle each ID once, reply if needed, then run "
            f"{self.wake_cli} consume --agent-id {self.agent_id} --state-dir {state} <id>."
        )
        return "\n".join(lines)

    def notify_cmd(self) -> str:
        # The command adapter expands format fields before handing it to bash.
        inbox = shlex.quote(str(self.inbox)).replace("{", "{{").replace("}", "}}")
        return f"printf '%s %s %s\\n' '{{message_id}}' '{{thread_id}}' '{{from_agent}}' >> {inbox}"

    def log(self, text: str) -> None:
        try:
            with (self.state_dir / "hook.log").open("a") as handle:
                handle.write(time.strftime("%Y-%m-%dT%H:%M:%S%z ") + text + "\n")
        except OSError:
            return


def _emit(store: Store, rows: list[dict[str, str]]) -> None:
    first = rows[0]
    sys.stdout.write(
        WAKE_PREFIX
        + json.dumps(
            {
                "prompt": store.notice(rows), "client": store.agent_id,
                "id": first["id"], "from": first["from"], "ids": [row["id"] for row in rows],
            },
            ensure_ascii=False,
        )
        + "\n"
    )
    sys.stdout.flush()


def drain_once(store: Store, now: float | None = None) -> list[dict[str, str]]:
    """Print one wake line for the unconsumed pointers that are due.

    A turn with a recent hook event is left to ``postToolUse``. After
    ``HOLD_S`` without one, or once the turn is idle, print. Repeat until
    ``consume`` so a missed wake does not wait on a person.
    """
    with store.lock():
        now = time.time() if now is None else float(now)
        data = store.read_state()
        if data["phase"] == "running" and now - data["phase_at"] < HOLD_S:
            return []
        wake_at = data["wake_at"]
        rows = [
            row for row in store.pending(data)
            if row["id"] not in wake_at or now - wake_at[row["id"]] >= HOLD_S
        ][:NOTICE_LIMIT]
        if rows:
            _emit(store, rows)
            wake_at.update({row["id"]: now for row in rows})
            store.write_state(data)
        return rows


def watch(store: Store) -> None:
    store.initialize()
    while True:
        try:
            drain_once(store)
        except BrokenPipeError:
            return
        except OSError as exc:
            store.log(f"watch could not read or write state: {exc}")
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


def _strip_line_continuations(command: str) -> str:
    out: list[str] = []
    in_single = False
    in_double = False
    i = 0
    n = len(command)
    while i < n:
        ch = command[i]
        if ch == "'" and not in_double:
            in_single = not in_single
            out.append(ch)
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch)
            i += 1
            continue
        if ch == "\\" and not in_single:
            if i + 1 < n and command[i + 1] == "\n":
                i += 2
                continue
            if i + 2 < n and command[i + 1] == "\r" and command[i + 2] == "\n":
                i += 3
                continue
            if i + 1 < n:
                out.append(ch)
                out.append(command[i + 1])
                i += 2
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def _split_shell_segments(command: str) -> list[list[str]]:
    if not command.strip():
        return []
    lex = shlex.shlex(_strip_line_continuations(command), posix=True, punctuation_chars="();<>|&\n")
    lex.whitespace_split = True
    lex.whitespace = " \t\r"
    try:
        tokens = list(lex)
    except ValueError:
        return []
    segments: list[list[str]] = []
    current: list[str] = []
    for tok in tokens:
        if tok in _CONTROL_OPS:
            if current:
                segments.append(current)
                current = []
        else:
            current.append(tok)
    if current:
        segments.append(current)
    return segments


def _strip_redirections(tokens: list[str]) -> list[str]:
    out: list[str] = []
    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if tok in {"0", "1", "2"} and i + 1 < n and tokens[i + 1] in _REDIR_OPS:
            i += 2
            if i < n and tokens[i] not in _REDIR_OPS:
                i += 1
            continue
        if tok in _REDIR_OPS:
            i += 1
            if i < n and tokens[i] not in _REDIR_OPS:
                i += 1
            continue
        out.append(tok)
        i += 1
    return out


def _unwrap_wrappers(tokens: list[str]) -> tuple[dict[str, str], list[str]]:
    env_vars: dict[str, str] = {}
    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if _ENV_ASSIGN_RE.fullmatch(tok):
            key, val = tok.split("=", 1)
            env_vars[key] = val
            i += 1
            continue
        name = Path(tok).name
        if name == "env":
            i += 1
            while i < n:
                t = tokens[i]
                if t in {"-u", "--unset", "-C", "--chdir", "-S", "--split-string"}:
                    i += 2
                    continue
                if t.startswith("-"):
                    i += 1
                    continue
                if _ENV_ASSIGN_RE.fullmatch(t):
                    key, val = t.split("=", 1)
                    env_vars[key] = val
                    i += 1
                    continue
                break
            continue
        if name in {"nohup", "setsid"}:
            i += 1
            continue
        if name == "exec":
            i += 1
            while i < n and tokens[i].startswith("-"):
                if tokens[i] == "-a":
                    i += 2
                else:
                    i += 1
            continue
        if name == "command":
            i += 1
            while i < n and tokens[i] in {"-p", "-v", "-V", "--"}:
                if tokens[i] == "--":
                    i += 1
                    break
                i += 1
            continue
        if name == "stdbuf":
            i += 1
            while i < n and tokens[i].startswith("-"):
                if tokens[i] in {"-i", "-o", "-e"}:
                    i += 2
                else:
                    i += 1
            continue
        break
    return env_vars, tokens[i:]


def _shell_inline_script(args: list[str]) -> str | None:
    saw_c = False
    i = 0
    n = len(args)
    while i < n:
        tok = args[i]
        if tok == "--":
            i += 1
            break
        if tok.startswith("-") and not tok.startswith("--"):
            if "c" in tok[1:]:
                saw_c = True
            i += 1
            continue
        if tok.startswith("--"):
            i += 1
            continue
        break
    if saw_c and i < n:
        return args[i]
    return None


def _is_legacy_wake_script(path_str: str) -> bool:
    normalized = path_str.replace("\\", "/")
    if normalized == "dev-cursor-bus/wake.py" or normalized.endswith("/dev-cursor-bus/wake.py"):
        return True
    try:
        target = Path.home() / ".local" / "share" / "dev-cursor-bus" / "wake.py"
        return Path(path_str).expanduser().resolve(strict=False) == target.resolve(strict=False)
    except OSError:
        return False


def _is_bus_cli_script(path_str: str) -> bool:
    return Path(path_str).name in {"bus.py", "cli.py"}


def _parse_agent_bus_cli(args: list[str], env_vars: dict[str, str]) -> tuple[str, list[str], dict[str, str]] | None:
    i = 0
    while i < len(args):
        tok = args[i]
        if tok == "--db":
            i += 2
            continue
        if tok.startswith("--db="):
            i += 1
            continue
        break
    if i + 1 < len(args) and args[i] == "cursor" and args[i + 1] in _CURSOR_SUBCOMMANDS:
        return args[i + 1], args[i + 2 :], env_vars
    return None


def _parse_legacy_wake(args: list[str], env_vars: dict[str, str]) -> tuple[str, list[str], dict[str, str]] | None:
    common = [
        "--agent-id",
        "dev-cursor",
        "--state-dir",
        str(Path.home() / ".local" / "share" / "dev-cursor-bus"),
    ]
    if not args:
        return "watch", common, env_vars
    if args[0] == "hook":
        return "hook", common, env_vars
    if args[0] == "--consume":
        return "consume", [*common, *args[1:]], env_vars
    if args[0] == "--pending":
        return "pending", common, env_vars
    if args[0] in _CURSOR_SUBCOMMANDS:
        return args[0], args[1:], env_vars
    return None


def _parse_python_module(module: str, rest: list[str], env_vars: dict[str, str]) -> tuple[str, list[str], dict[str, str]] | None:
    if module == "buslib.cursor_wake":
        if rest and rest[0] in _CURSOR_SUBCOMMANDS:
            return rest[0], rest[1:], env_vars
        return None
    if module == "buslib.cli":
        return _parse_agent_bus_cli(rest, env_vars)
    return None


def _parse_script_invocation(script_path: str, args: list[str], env_vars: dict[str, str]) -> tuple[str, list[str], dict[str, str]] | None:
    if _is_legacy_wake_script(script_path):
        return _parse_legacy_wake(args, env_vars)
    if Path(script_path).name == "cursor_wake.py":
        if args and args[0] in _CURSOR_SUBCOMMANDS:
            return args[0], args[1:], env_vars
        return None
    if _is_bus_cli_script(script_path):
        return _parse_agent_bus_cli(args, env_vars)
    return None


def _parse_cursor_invocation(env_vars: dict[str, str], tokens: list[str]) -> tuple[str, list[str], dict[str, str]] | None:
    if not tokens:
        return None
    exe_name = Path(tokens[0]).name
    if _PYTHON_EXE_RE.fullmatch(exe_name):
        i = 1
        n = len(tokens)
        while i < n:
            tok = tokens[i]
            if tok == "--":
                i += 1
                break
            if tok == "-c" or tok.startswith("-c"):
                return None
            if tok == "-m":
                if i + 1 >= n:
                    return None
                return _parse_python_module(tokens[i + 1], tokens[i + 2 :], env_vars)
            if tok.startswith("-m") and len(tok) > 2:
                return _parse_python_module(tok[2:], tokens[i + 1 :], env_vars)
            if tok.startswith("-") and not tok.startswith("--"):
                flags = tok[1:]
                if "c" in flags:
                    return None
                if "m" in flags:
                    idx = flags.index("m")
                    if idx + 1 < len(flags):
                        return _parse_python_module(flags[idx + 1 :], tokens[i + 1 :], env_vars)
                    if i + 1 >= n:
                        return None
                    return _parse_python_module(tokens[i + 1], tokens[i + 2 :], env_vars)
                if tok in {"-W", "-X"}:
                    i += 2
                    continue
                i += 1
                continue
            if tok == "--check-hash-based-pycs":
                i += 2
                continue
            if tok.startswith("--"):
                i += 1
                continue
            return _parse_script_invocation(tok, tokens[i + 1 :], env_vars)
        if i < n:
            return _parse_script_invocation(tokens[i], tokens[i + 1 :], env_vars)
        return None
    if exe_name in {"agent-bus", "run"} or _is_bus_cli_script(tokens[0]):
        return _parse_agent_bus_cli(tokens[1:], env_vars)
    if _is_legacy_wake_script(tokens[0]):
        return _parse_legacy_wake(tokens[1:], env_vars)
    if Path(tokens[0]).name == "cursor_wake.py":
        if len(tokens) >= 2 and tokens[1] in _CURSOR_SUBCOMMANDS:
            return tokens[1], tokens[2:], env_vars
    return None


def _iter_invocations(command: str, depth: int = 0, inherited_env: dict[str, str] | None = None):
    if depth > 2 or not command:
        return
    shell_env = dict(inherited_env or {})
    for seg in _split_shell_segments(command):
        clean = _strip_redirections(seg)
        env_vars, cmd = _unwrap_wrappers(clean)
        if not cmd:
            shell_env.update(env_vars)
            continue
        if cmd[0] == "export":
            for arg in cmd[1:]:
                if _ENV_ASSIGN_RE.fullmatch(arg):
                    key, val = arg.split("=", 1)
                    shell_env[key] = val
            continue
        merged_env = {**shell_env, **env_vars}
        if Path(cmd[0]).name in _SHELL_NAMES:
            inner = _shell_inline_script(cmd[1:])
            if inner is not None:
                yield from _iter_invocations(inner, depth + 1, merged_env)
            continue
        parsed = _parse_cursor_invocation(merged_env, cmd)
        if parsed is not None:
            yield parsed


def _matches_store(store: Store, sub_args: list[str], env_vars: dict[str, str]) -> bool:
    agent_id: str | None = None
    state_dir: str | None = None
    i = 0
    n = len(sub_args)
    while i < n:
        tok = sub_args[i]
        if tok == "--agent-id" and i + 1 < n:
            agent_id = sub_args[i + 1]
            i += 2
            continue
        if tok.startswith("--agent-id="):
            agent_id = tok.split("=", 1)[1]
            i += 1
            continue
        if tok == "--state-dir" and i + 1 < n:
            state_dir = sub_args[i + 1]
            i += 2
            continue
        if tok.startswith("--state-dir="):
            state_dir = tok.split("=", 1)[1]
            i += 1
            continue
        return False
    if agent_id != store.agent_id:
        return False
    cmd_dir = Path(state_dir).expanduser() if state_dir else default_state_dir(agent_id, env_vars.get("AGENT_BUS_WAKE_HOME"))
    try:
        return cmd_dir.resolve() == store.state_dir.resolve()
    except OSError:
        return False


def _is_watch_command(store: Store, command: str) -> bool:
    for subcmd, sub_args, env_vars in _iter_invocations(command):
        if subcmd == "watch" and _matches_store(store, sub_args, env_vars):
            return True
    return False


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
    with store.lock():
        data = store.read_state()
        command = _tool_command(event)
        changed = False
        if name == "postToolUse" and conversation_id and _is_watch_command(store, command):
            if data.get("conversation_id") != conversation_id:
                data["conversation_id"] = conversation_id
                changed = True
                store.log(f"bind conversation={conversation_id}")
        bound = bool(conversation_id) and data.get("conversation_id") == conversation_id
        if bound and name == "postToolUse":
            data["phase"] = "running"
            data["phase_at"] = time.time()
            changed = True
            # Only a first notice can be inserted into a running turn. The
            # watcher handles retries when the turn is idle or its events stop.
            wake_at = data["wake_at"]
            unseen = [row for row in store.pending(data) if row["id"] not in wake_at][:NOTICE_LIMIT]
            if unseen:
                wake_at.update({row["id"]: data["phase_at"] for row in unseen})
                out = {"additional_context": store.notice(unseen)}
                store.log(f"insert {','.join(row['id'] for row in unseen)}")
        elif bound and name in {"stop", "sessionEnd"}:
            if data.get("phase") != "stopped":
                data["phase"] = "stopped"
                data["phase_at"] = time.time()
                changed = True
                if name == "sessionEnd":
                    store.log("sessionEnd idle")
        if changed:
            store.write_state(data)
    return out


def hook_main(store: Store) -> None:
    try:
        sys.stdout.write(json.dumps(hook(store), ensure_ascii=False))
    except Exception as exc:
        store.log(f"hook could not process the event: {exc}")
        sys.stdout.write("{}")


def consume(store: Store, ids: list[str]) -> None:
    with store.lock():
        data = store.read_state()
        for message_id in ids:
            if MESSAGE_ID_RE.fullmatch(message_id) and message_id not in data["consumed"]:
                data["consumed"].append(message_id)
                data["wake_at"].pop(message_id, None)
        store.write_state(data)


def _store_from_args(args: argparse.Namespace) -> Store:
    if not ID_RE.fullmatch(args.agent_id):
        raise SystemExit("error: invalid agent id; use letters, digits, underscores, periods or hyphens")
    state_dir = Path(args.state_dir).expanduser() if args.state_dir else default_state_dir(args.agent_id)
    return Store(args.agent_id, state_dir.resolve())


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
    with store.lock():
        rows = store.pending(store.read_state())
    print(json.dumps({"ok": True, "pending": rows}))
    return 0


def cmd_notify_cmd(args: argparse.Namespace) -> int:
    store = _store_from_args(args)
    store.initialize()
    print(store.notify_cmd())
    return 0


def _add_common(command: argparse.ArgumentParser) -> None:
    command.add_argument("--agent-id", required=True)
    command.add_argument("--state-dir", help="pointer directory (default: ~/.local/share/agent-bus-cursor/<agent-id>)")


_WATCH_DESC = (
    "Print AGENT_LOOP_WAKE_agentbus lines to stdout while the conversation is idle. "
    "Launch inside the target Cursor conversation as a backgrounded Shell call "
    "(block_until_ms=0) with notify_on_output matching '^AGENT_LOOP_WAKE_agentbus'. "
    "Do NOT redirect stdout/stderr (> file, >/dev/null) or detach with & / nohup."
)


def add_parser(sub: argparse._SubParsersAction) -> None:
    parser = sub.add_parser("cursor", help="wake a Cursor conversation from the command adapter")
    commands = parser.add_subparsers(dest="cursor_command", required=True)
    watch_p = commands.add_parser(
        "watch",
        help="print AGENT_LOOP_WAKE_agentbus while the conversation is idle",
        description=_WATCH_DESC,
    )
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
        kwargs = {"description": _WATCH_DESC} if name == "watch" else {}
        command = sub.add_parser(name, **kwargs)
        command.add_argument("--agent-id", required=True)
        command.add_argument("--state-dir")
        if ids:
            command.add_argument("message_ids", nargs="+")
        command.set_defaults(func=func)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
