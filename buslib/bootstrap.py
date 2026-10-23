"""One-command agent bootstrap and local process management.

The rest of the bus is deliberately small and composable, but an agent that
just installed the package should not need to know the four-step dance
(register -> heartbeat -> notifier -> verify). `agent-bus up` does the local
equivalent of a service manager: register, start a detached notifier, write a
pid/log record, and optionally start the HTTP daemon.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from .agents import cmd_register, resolve_opencode_cli
from .config import BUS_HOME, ensure_home, home_info, json_dumps, log_dir, run_dir
from .db import connect
from .notifier import main as notifier_main
from .validation import validate_id


def _notifier_pid_path(agent_id: str) -> Path:
    return run_dir() / f"notifier-{agent_id}.json"


def _legacy_notifier_pid_path(agent_id: str) -> Path:
    # Processes started from the repository before the packaged entry point
    # existed wrote simple pid files here. Detect them so `up` is idempotent.
    return BUS_HOME / f".bus_agent_{agent_id}.pid"


def _daemon_pid_path(port: int) -> Path:
    return run_dir() / f"daemon-{port}.json"


def _legacy_daemon_pid_path(port: int) -> Path:
    return BUS_HOME / f".bus_daemon_{port}.pid"


def _read_pid(path: Path) -> int | None:
    try:
        raw = path.read_text().strip()
    except (FileNotFoundError, OSError):
        return None
    if not raw:
        return None
    try:
        value = json.loads(raw)
        if isinstance(value, dict):
            value = value.get("pid")
        return int(value) if value is not None else None
    except (json.JSONDecodeError, TypeError, ValueError):
        try:
            return int(raw.split()[0])
        except (ValueError, IndexError):
            return None


def _write_pid(path: Path, info: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.parent.chmod(0o700)
    except OSError:
        pass
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(json_dumps(info) + "\n")
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _remove_pid(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError, OSError):
        path.unlink()


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _process_command(pid: int) -> str:
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip()


def _looks_like_notifier(command: str, agent_id: str) -> bool:
    if not command:
        return False
    if re.search(rf"--agent-id(?:=|\s+){re.escape(agent_id)}(?:\s|$)", command) is None:
        return False
    return any(
        marker in command
        for marker in ("bus_agent.py", "buslib.notifier", "agent-bus-notifier")
    )


def _looks_like_daemon(command: str, port: int) -> bool:
    if not command:
        return False
    return (
        any(marker in command for marker in ("bus_daemon.py", "buslib.daemon", "agent-bus-daemon"))
        and re.search(rf"--port(?:=|\s+){int(port)}(?:\s|$)", command) is not None
    )


def _find_process(primary: Path, legacy: Path, predicate) -> dict[str, Any] | None:
    for path, managed in ((primary, True), (legacy, False)):
        pid = _read_pid(path)
        if pid is None:
            continue
        if not _pid_alive(pid):
            _remove_pid(path)
            continue
        command = _process_command(pid)
        if command and not predicate(command):
            # The pid was recycled by a different process. Do not touch it.
            _remove_pid(path)
            continue
        return {
            "pid": pid,
            "pid_file": str(path),
            "legacy": not managed,
            "verified": bool(command),
            "command": command,
        }
    return None


def _find_notifier(agent_id: str) -> dict[str, Any] | None:
    info = _find_process(
        _notifier_pid_path(agent_id),
        _legacy_notifier_pid_path(agent_id),
        lambda command: _looks_like_notifier(command, agent_id),
    )
    if info is not None:
        if info.get("legacy"):
            info.setdefault("log", str(BUS_HOME / f"bus_agent_{agent_id}.log"))
        else:
            info.setdefault("log", str(log_dir() / f"notifier-{agent_id}.log"))
    return info


def _find_daemon(port: int) -> dict[str, Any] | None:
    return _find_process(
        _daemon_pid_path(port),
        _legacy_daemon_pid_path(port),
        lambda command: _looks_like_daemon(command, port),
    )


def _stop_process(info: dict[str, Any], *, label: str, force: bool = False) -> dict[str, Any]:
    pid = int(info["pid"])
    if not _pid_alive(pid):
        _remove_pid(Path(info["pid_file"]))
        return {"stopped": False, "reason": "not running", "pid": pid}
    if not info.get("verified") and not force:
        raise SystemExit(
            f"error: refusing to stop unverified {label} pid {pid} "
            f"(could not read its command line); use --force if you are sure"
        )
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGTERM)
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if not _pid_alive(pid):
            break
        time.sleep(0.1)
    killed = False
    if _pid_alive(pid):
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
        killed = True
        for _ in range(20):
            if not _pid_alive(pid):
                break
            time.sleep(0.1)
    _remove_pid(Path(info["pid_file"]))
    return {"stopped": True, "pid": pid, "killed": killed}


def _log_tail(path: Path, lines: int = 20) -> str:
    try:
        text = path.read_text(errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(text[-lines:])


def _child_env(agent_id: str | None = None) -> dict[str, str]:
    env = dict(os.environ)
    # Pin the child to the same world the parent just initialized. Without
    # this, a service manager starting from another cwd could silently use a
    # different source checkout / home.
    env["AGENT_BUS_HOME"] = str(BUS_HOME)
    if agent_id:
        env.setdefault("AGENT_BUS_AGENT", agent_id)
    return env


def _launch(
    argv: list[str],
    *,
    log_path: Path,
    pid_path: Path,
    info: dict[str, Any],
    cwd: str,
    env: dict[str, str],
) -> dict[str, Any]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        log_path.parent.chmod(0o700)
    except OSError:
        pass
    fd = os.open(str(log_path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.chmod(log_path, 0o600)
    except OSError:
        pass
    try:
        proc = subprocess.Popen(
            argv,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=fd,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    except OSError as exc:
        raise SystemExit(f"error: could not start {argv[0]}: {exc}") from exc
    finally:
        os.close(fd)

    record = {
        **info,
        "pid": proc.pid,
        "pid_file": str(pid_path),
        "log": str(log_path),
        "command": " ".join(argv),
        "started_at": time.time(),
    }
    _write_pid(pid_path, record)
    # Give the process enough time to fail its token/session checks. A
    # notifier that exits immediately should make `up` fail, not print a pid.
    deadline = time.time() + 1.5
    while time.time() < deadline:
        rc = proc.poll()
        if rc is not None:
            _remove_pid(pid_path)
            tail = _log_tail(log_path)
            raise SystemExit(
                f"error: {info.get('kind', 'process')} exited immediately (rc={rc})"
                + (f"\n--- {log_path} ---\n{tail}" if tail else "")
            )
        time.sleep(0.1)
    return record


def _register(args: argparse.Namespace) -> dict[str, Any]:
    ns = argparse.Namespace(
        agent_id=args.agent_id,
        agent_type=args.agent_type,
        display_name=args.display_name,
        model_tier=args.model_tier,
        capabilities=args.capabilities,
        session_id=args.session_id,
        endpoint=args.endpoint or args.session_id or args.tmux_target,
        machine_id=args.machine_id,
        metadata=args.metadata,
        rotate_token=False,
    )
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        cmd_register(ns)
    try:
        return json.loads(out.getvalue())
    except json.JSONDecodeError:
        return {"ok": True, "agent_id": args.agent_id, "raw": out.getvalue()}


def _notifier_argv(args: argparse.Namespace) -> list[str]:
    argv = [
        sys.executable, "-m", "buslib.notifier",
        "--agent-id", args.agent_id,
        "--adapter", args.adapter,
    ]
    if args.session_id:
        argv += ["--session-id", args.session_id]
    if args.tmux_target:
        argv += ["--tmux-target", args.tmux_target]
    if args.notify_cmd:
        argv += ["--notify-cmd", args.notify_cmd]
    if args.cli:
        argv += ["--cli", args.cli]
    if args.cwd:
        argv += ["--cwd", args.cwd]
    if args.poll is not None:
        argv += ["--poll", str(args.poll)]
    if args.heartbeat is not None:
        argv += ["--heartbeat", str(args.heartbeat)]
    if args.sweep is not None:
        argv += ["--sweep", str(args.sweep)]
    if args.outbox_flush is not None:
        argv += ["--outbox-flush", str(args.outbox_flush)]
    if args.allow_unsafe_text:
        argv += ["--allow-unsafe-text"]
    return argv


def _daemon_argv(args: argparse.Namespace, port: int) -> list[str]:
    argv = [
        sys.executable, "-m", "buslib.daemon",
        "--port", str(port), "--bind", args.bind,
    ]
    if args.machine_id:
        argv += ["--machine-id", args.machine_id]
    return argv


def _daemon_healthy(port: int, timeout: float = 0.5) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=timeout) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError, urllib.error.HTTPError):
        return False


def _start_daemon(args: argparse.Namespace, cwd: str, env: dict[str, str]) -> dict[str, Any]:
    port = int(args.daemon_port)
    existing = _find_daemon(port)
    if existing is not None:
        return {**existing, "already_running": True, "healthy": _daemon_healthy(port)}
    if _daemon_healthy(port):
        raise SystemExit(
            f"error: a daemon is listening on 127.0.0.1:{port} but is not managed by this home; "
            "stop it first or choose --daemon-port"
        )
    log_path = log_dir() / f"daemon-{port}.log"
    record = _launch(
        _daemon_argv(args, port),
        log_path=log_path,
        pid_path=_daemon_pid_path(port),
        info={"kind": "daemon", "port": port, "bind": args.bind},
        cwd=cwd,
        env=env,
    )
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if _daemon_healthy(port):
            return {**record, "already_running": False, "healthy": True}
        time.sleep(0.1)
    _remove_pid(_daemon_pid_path(port))
    tail = _log_tail(log_path)
    raise SystemExit(
        f"error: daemon did not become healthy on 127.0.0.1:{port}"
        + (f"\n--- {log_path} ---\n{tail}" if tail else "")
    )


def _infer_agent_type(adapter: str) -> str:
    return {"opencode": "opencode", "tmux": "tmux", "command": "command"}.get(adapter, adapter)


def _probe_opencode_session(args: argparse.Namespace) -> dict[str, Any]:
    """Best-effort opencode session probe.

    `up` used to trust any session id and report the notifier active even when
    the underlying model turn had failed. That is a false liveness signal. The
    probe is adapter-specific (opencode semantics), so callers can make it
    strict with --verify-session or just consume the warning.
    """
    cli = args.cli or resolve_opencode_cli()
    try:
        result = subprocess.run(
            [cli, "api", "session.get", "--param", f"sessionID={args.session_id}"],
            capture_output=True, text=True, timeout=20,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"ok": False, "cli": cli, "probe_error": f"{type(exc).__name__}: {exc}"}
    if result.returncode != 0:
        return {
            "ok": False,
            "cli": cli,
            "probe_error": (result.stderr or result.stdout or "session.get failed").strip()[:500],
        }
    try:
        data = json.loads(result.stdout).get("data") or {}
    except (json.JSONDecodeError, AttributeError, TypeError):
        return {"ok": False, "cli": cli, "probe_error": "session.get returned non-JSON"}
    model = data.get("model")
    if isinstance(model, dict):
        model = model.get("id")
    return {
        "ok": True,
        "cli": cli,
        "model": model,
        "outcome": data.get("outcome"),
        "session_id": args.session_id,
    }


def _session_probe_failed(probe: dict[str, Any]) -> bool:
    return not probe.get("ok") or probe.get("outcome") == "failed"


def _up_payload(args: argparse.Namespace, payload: dict[str, Any]) -> dict[str, Any]:
    if not getattr(args, "quiet", False):
        return payload
    # Agents often stop after a large tool response. Keep the machine-readable
    # contract but drop the redundant home/command dump in --quiet mode.
    return {
        "ok": payload.get("ok", True),
        "already_running": payload.get("already_running", False),
        "agent_id": payload.get("agent_id") or args.agent_id,
        "session_probe": payload.get("session_probe"),
        "notifier": {
            k: (payload.get("notifier") or {}).get(k)
            for k in ("pid", "pid_file", "log")
        },
        "daemon": payload.get("daemon"),
    }


def cmd_up(args: argparse.Namespace) -> int:
    if not args.agent_id:
        raise SystemExit("error: --agent-id or AGENT_BUS_AGENT is required")
    ensure_home()
    validate_id("agent_id", args.agent_id)
    adapter = args.adapter or os.environ.get("AGENT_BUS_ADAPTER") or "opencode"
    if adapter not in {"opencode", "tmux", "command"}:
        raise SystemExit(f"error: unknown adapter {adapter!r}; expected opencode, tmux, or command")
    args.adapter = adapter
    args.agent_type = args.agent_type or _infer_agent_type(adapter)
    args.cwd = args.cwd or os.getcwd()
    if args.foreground and args.daemon:
        raise SystemExit("error: --foreground cannot be combined with --daemon")
    for flag, value, minimum in (
        ("--poll", args.poll, 0.01),
        ("--heartbeat", args.heartbeat, 0.01),
        ("--sweep", args.sweep, 0.01),
        ("--outbox-flush", args.outbox_flush, 0.0),
    ):
        if value is not None and value < minimum:
            raise SystemExit(f"error: {flag} must be >= {minimum}")
    if not (1 <= int(args.daemon_port) <= 65535):
        raise SystemExit("error: --daemon-port must be in 1..65535")

    if adapter == "opencode" and not args.session_id:
        # A previous registration may already hold the binding. Check before
        # failing so `up` remains idempotent after a restart.
        conn = connect()
        row = conn.execute(
            "SELECT current_session_id FROM agents WHERE agent_id=?", (args.agent_id,)
        ).fetchone()
        conn.close()
        if row is not None and row["current_session_id"]:
            args.session_id = row["current_session_id"]
        else:
            raise SystemExit(
                "error: --session-id is required for the opencode adapter on first registration"
            )
    if adapter == "tmux" and not args.tmux_target:
        raise SystemExit("error: --tmux-target is required for the tmux adapter")
    if args.verify_session and adapter != "opencode":
        raise SystemExit("error: --verify-session is only supported for the opencode adapter")

    session_probe: dict[str, Any] | None = None
    if adapter == "opencode":
        session_probe = _probe_opencode_session(args)
        if args.verify_session:
            if not session_probe.get("ok"):
                raise SystemExit(
                    f"error: opencode session verification failed: {session_probe.get('probe_error')}"
                )
            if session_probe.get("outcome") == "failed":
                raise SystemExit(
                    f"error: opencode session {args.session_id} reports outcome=failed "
                    f"(model={session_probe.get('model')}); use --force or a working session, "
                    "or omit --verify-session to start anyway"
                )
        elif _session_probe_failed(session_probe):
            detail = session_probe.get("probe_error") or f"outcome={session_probe.get('outcome')}"
            print(
                f"warning: opencode session probe for {args.session_id} is unhealthy: {detail} "
                "(use --verify-session to make this fatal)",
                file=sys.stderr,
            )

    env = _child_env(args.agent_id)
    env["AGENT_BUS_CWD"] = args.cwd
    existing = _find_notifier(args.agent_id)
    if existing is not None and not args.force:
        daemon = _start_daemon(args, args.cwd, env) if args.daemon else None
        info = home_info()
        print(json_dumps(_up_payload(args, {
            "ok": True,
            "already_running": True,
            "agent_id": args.agent_id,
            **info,
            "notifier": existing,
            "daemon": daemon,
            "session_probe": session_probe,
            "hint": "use `agent-bus up ... --force` to replace it",
        })))
        return 0
    # Authenticate/register before stopping a working notifier. A bad token or
    # missing session should fail without creating an offline window.
    registration = _register(args)
    if existing is not None:
        _stop_process(existing, label=f"notifier-{args.agent_id}", force=True)
        for path in (_notifier_pid_path(args.agent_id), _legacy_notifier_pid_path(args.agent_id)):
            _remove_pid(path)

    if args.foreground:
        record = {
            "kind": "notifier",
            "agent_id": args.agent_id,
            "adapter": adapter,
            "session_id": args.session_id,
            "cwd": args.cwd,
            "pid": os.getpid(),
            "pid_file": str(_notifier_pid_path(args.agent_id)),
            "log": "(foreground)",
            "command": " ".join(_notifier_argv(args)),
            "started_at": time.time(),
        }
        pid_path = Path(record["pid_file"])
        _write_pid(pid_path, record)
        old_argv = sys.argv
        try:
            sys.argv = ["agent-bus-notifier", *_notifier_argv(args)[3:]]
            return notifier_main()
        finally:
            sys.argv = old_argv
            _remove_pid(pid_path)

    notifier = _launch(
        _notifier_argv(args),
        log_path=log_dir() / f"notifier-{args.agent_id}.log",
        pid_path=_notifier_pid_path(args.agent_id),
        info={
            "kind": "notifier",
            "agent_id": args.agent_id,
            "adapter": adapter,
            "session_id": args.session_id,
            "cwd": args.cwd,
        },
        cwd=args.cwd,
        env=env,
    )

    daemon: dict[str, Any] | None = None
    if args.daemon:
        daemon = _start_daemon(args, args.cwd, env)

    print(json_dumps(_up_payload(args, {
        "ok": True,
        "already_running": False,
        "agent_id": args.agent_id,
        "agent": registration,
        **home_info(),
        "notifier": notifier,
        "daemon": daemon,
        "session_probe": session_probe,
    })))
    return 0


def cmd_down(args: argparse.Namespace) -> int:
    if not args.agent_id:
        raise SystemExit("error: --agent-id or AGENT_BUS_AGENT is required")
    validate_id("agent_id", args.agent_id)
    if args.daemon and not (1 <= int(args.daemon_port) <= 65535):
        raise SystemExit("error: --daemon-port must be in 1..65535")
    existing = _find_notifier(args.agent_id)
    notifier: dict[str, Any] | None = None
    if existing is not None:
        notifier = _stop_process(existing, label=f"notifier-{args.agent_id}", force=args.force)
    for path in (_notifier_pid_path(args.agent_id), _legacy_notifier_pid_path(args.agent_id)):
        _remove_pid(path)

    daemon: dict[str, Any] | None = None
    if args.daemon:
        port = int(args.daemon_port)
        info = _find_daemon(port)
        if info is not None:
            if not info.get("verified") and not args.force:
                raise SystemExit(
                    f"error: refusing to stop unverified daemon pid {info['pid']}; use --force"
                )
            pid = int(info["pid"])
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGTERM)
            deadline = time.time() + 5.0
            while time.time() < deadline and _pid_alive(pid):
                time.sleep(0.1)
            killed = False
            if _pid_alive(pid):
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
                killed = True
            _remove_pid(Path(info["pid_file"]))
            _remove_pid(_daemon_pid_path(port))
            _remove_pid(_legacy_daemon_pid_path(port))
            daemon = {"stopped": True, "pid": pid, "killed": killed}
        else:
            daemon = {"stopped": False, "reason": "not running"}

    print(json_dumps({
        "ok": True,
        "agent_id": args.agent_id,
        "notifier": notifier or {"stopped": False, "reason": "not running"},
        "daemon": daemon,
    }))
    return 0


def cmd_home(_args: argparse.Namespace) -> int:
    ensure_home()
    print(json_dumps({"ok": True, **home_info()}))
    return 0


def cmd_guide(_args: argparse.Namespace) -> int:
    info = home_info()
    print(f"""agent-bus is installed. State home: {info['home']} ({info['source']})
Database: {info['db']}

One-command start (registers the agent and starts a detached notifier):
  agent-bus up --agent-id <id> --agent-type opencode --session-id <ses_...>
  agent-bus up --agent-id <id> --agent-type opencode --session-id <ses_...> --verify-session
  agent-bus up --agent-id <id> --agent-type command --notify-cmd '<your hook>'
  agent-bus up --agent-id <id> --agent-type opencode --session-id <ses_...> --daemon

Check:
  agent-bus home
  agent-bus agents
  agent-bus status
  agent-bus liveness --agent-id <id>

Agent skill:
  agent-bus skill install          # ~/.claude/skills/agent-bus (copy + launcher)
  agent-bus skill show             # print the bundled SKILL.md
  agent-bus skill path             # show source and install paths

Talk:
  agent-bus send --from <id> --to <peer> --text "hello"
  agent-bus ask  --from <id> --to <peer> --text "question" --wait
  agent-bus inbox --agent-id <id>
  agent-bus ack --agent-id <id> --message-id <id>

`send` may report target_stale=true when the recipient has no recent
heartbeat/notifier; the message is still queued. Start a notifier or poll
`inbox`. Audit event lines go to stderr; stdout is the command result JSON.

Groups:
  agent-bus group create --from <id> --group-id <group> --topic "..."
  agent-bus group join --group-id <group> --agent-id <peer>
  agent-bus group send --from <id> --group-id <group> --text "hello"
  agent-bus group messages --group-id <group> --agent-id <id>
  agent-bus group history --group-id <group> --agent-id <id>
  agent-bus group read --group-id <group> --agent-id <id> --seq <n>
  agent-bus group catch-up --group-id <group> --agent-id <id>

Stop:
  agent-bus down --agent-id <id> [--daemon]

The notifier log is under {info['logs']}; the pid file is under {info['run']}.
Set AGENT_BUS_HOME to use a different bus world before starting.

Restart note: `agent-bus up --force` replaces the running notifier. If an
agent runs that from inside its own bus turn, it also kills the notifier that
may be delivering that turn; prefer restarting from an external shell.""")
    return 0


__all__ = ["cmd_down", "cmd_guide", "cmd_home", "cmd_up"]
