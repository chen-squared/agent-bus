"""Small embeddable Python client for agent-bus.

The CLI remains the source of truth (validation, auth, gates, JSON shape).
This class is a subprocess-backed convenience layer so code can do:

    from buslib import Bus
    bus = Bus("alice")
    bus.start(agent_type="opencode", session_id="ses_...", daemon=True)
    bus.send("bob", "hello")

Using the public CLI for each call keeps the library and the installed console
script behavior identical, including the one-command `up` bootstrap. Calls are
not high-frequency RPC; if you need thousands of sends per second, call the
`buslib` functions directly instead.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


class BusError(RuntimeError):
    """A CLI command returned nonzero; the message is its stderr/stdout."""


class Bus:
    def __init__(
        self,
        agent_id: str | None = None,
        *,
        home: str | os.PathLike[str] | None = None,
        db: str | os.PathLike[str] | None = None,
    ) -> None:
        self.agent_id = agent_id or os.environ.get("AGENT_BUS_AGENT")
        self._env = dict(os.environ)
        if home is not None:
            self._env["AGENT_BUS_HOME"] = str(Path(home).expanduser().resolve())
        if db is not None:
            self._env["AGENT_BUS_DB"] = str(Path(db).expanduser().resolve())
        if self.agent_id:
            self._env["AGENT_BUS_AGENT"] = self.agent_id

    def _call(self, *argv: Any, timeout: float = 60.0) -> dict[str, Any]:
        command = [sys.executable, "-m", "buslib.cli", *map(str, argv)]
        try:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=self._env,
            )
        except subprocess.TimeoutExpired as exc:
            raise BusError(f"agent-bus timed out after {timeout}s: {' '.join(command)}") from exc
        if proc.returncode != 0:
            raise BusError((proc.stderr or proc.stdout or f"agent-bus exited {proc.returncode}").strip())
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError:
            return {"ok": True, "output": proc.stdout}

    def _require_agent(self) -> str:
        if not self.agent_id:
            raise BusError("Bus(agent_id=...) or AGENT_BUS_AGENT is required for this operation")
        return self.agent_id

    # ---- lifecycle -----------------------------------------------------

    def init(self) -> dict[str, Any]:
        return self._call("init")

    def home(self) -> dict[str, Any]:
        return self._call("home")

    def register(
        self,
        agent_type: str,
        *,
        session_id: str | None = None,
        endpoint: str | None = None,
        display_name: str | None = None,
        model_tier: str | None = None,
        capabilities: str | None = None,
        metadata: str | None = None,
    ) -> dict[str, Any]:
        agent = self._require_agent()
        argv: list[Any] = ["register", "--agent-id", agent, "--agent-type", agent_type]
        if session_id:
            argv += ["--session-id", session_id]
        if endpoint:
            argv += ["--endpoint", endpoint]
        if display_name:
            argv += ["--display-name", display_name]
        if model_tier:
            argv += ["--model-tier", model_tier]
        if capabilities:
            argv += ["--capabilities", capabilities]
        if metadata:
            argv += ["--metadata", metadata]
        return self._call(*argv)

    def start(
        self,
        *,
        agent_type: str | None = None,
        adapter: str | None = None,
        session_id: str | None = None,
        tmux_target: str | None = None,
        notify_cmd: str | None = None,
        cwd: str | None = None,
        daemon: bool = False,
        daemon_port: int = 8780,
        display_name: str | None = None,
        model_tier: str | None = None,
        capabilities: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        agent = self._require_agent()
        if adapter is None:
            adapter = agent_type if agent_type in {"opencode", "tmux", "command"} else "opencode"
        argv: list[Any] = [
            "up", "--agent-id", agent, "--adapter", adapter,
            "--daemon-port", daemon_port,
        ]
        if agent_type:
            argv += ["--agent-type", agent_type]
        if session_id:
            argv += ["--session-id", session_id]
        if tmux_target:
            argv += ["--tmux-target", tmux_target]
        if notify_cmd:
            argv += ["--notify-cmd", notify_cmd]
        if cwd:
            argv += ["--cwd", cwd]
        if display_name:
            argv += ["--display-name", display_name]
        if model_tier:
            argv += ["--model-tier", model_tier]
        if capabilities:
            argv += ["--capabilities", capabilities]
        if daemon:
            argv += ["--daemon"]
        if force:
            argv += ["--force"]
        return self._call(*argv, timeout=30.0)

    def stop(self, *, daemon: bool = False, daemon_port: int = 8780) -> dict[str, Any]:
        agent = self._require_agent()
        argv: list[Any] = ["down", "--agent-id", agent, "--daemon-port", daemon_port]
        if daemon:
            argv += ["--daemon"]
        return self._call(*argv)

    # ---- messages ------------------------------------------------------

    def send(
        self,
        to: str,
        text: str,
        *,
        thread_id: str | None = None,
        urgency: str = "normal",
        ttl: int | None = None,
        idempotency_key: str | None = None,
        attachment: str | None = None,
    ) -> dict[str, Any]:
        agent = self._require_agent()
        argv: list[Any] = ["send", "--from", agent, "--to", to, "--text", text]
        if thread_id:
            argv += ["--thread-id", thread_id]
        if urgency != "normal":
            argv += ["--urgency", urgency]
        if ttl is not None:
            argv += ["--ttl", ttl]
        if idempotency_key:
            argv += ["--idempotency-key", idempotency_key]
        if attachment:
            argv += ["--attachment", attachment]
        return self._call(*argv)

    def ask(
        self,
        to: str,
        text: str,
        *,
        wait: bool = False,
        timeout: int = 120,
        urgency: str = "normal",
        thread_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        agent = self._require_agent()
        argv: list[Any] = [
            "ask", "--from", agent, "--to", to, "--text", text,
            "--timeout", timeout,
        ]
        if wait:
            argv += ["--wait"]
        if urgency != "normal":
            argv += ["--urgency", urgency]
        if thread_id:
            argv += ["--thread-id", thread_id]
        if idempotency_key:
            argv += ["--idempotency-key", idempotency_key]
        return self._call(*argv, timeout=max(60.0, timeout + 30.0))

    def answer(
        self,
        thread_id: str,
        text: str,
        *,
        to: str | None = None,
        ttl: int | None = None,
    ) -> dict[str, Any]:
        agent = self._require_agent()
        argv: list[Any] = ["answer", "--thread-id", thread_id, "--from", agent, "--text", text]
        if to:
            argv += ["--to", to]
        if ttl is not None:
            argv += ["--ttl", ttl]
        return self._call(*argv)

    def inbox(self, *, status: str = "undelivered", limit: int = 50) -> dict[str, Any]:
        agent = self._require_agent()
        return self._call("inbox", "--agent-id", agent, "--status", status, "--limit", limit)

    def ack(self, message_id: str) -> dict[str, Any]:
        agent = self._require_agent()
        return self._call("ack", "--agent-id", agent, "--message-id", message_id)

    def thread(self, thread_id: str) -> dict[str, Any]:
        agent = self._require_agent()
        return self._call("thread", thread_id, "--agent-id", agent)

    def message(self, message_id: str) -> dict[str, Any]:
        agent = self._require_agent()
        return self._call("message", "show", "--agent-id", agent, "--message-id", message_id)

    # ---- discovery / status -------------------------------------------

    def agents(self) -> dict[str, Any]:
        return self._call("agents")

    def find(self, *, capability: str | None = None, model_tier: str | None = None, active_only: bool = False) -> dict[str, Any]:
        argv: list[Any] = ["find"]
        if capability:
            argv += ["--capability", capability]
        if model_tier:
            argv += ["--model-tier", model_tier]
        if active_only:
            argv += ["--active-only"]
        return self._call(*argv)

    def liveness(self, agent_id: str | None = None) -> dict[str, Any]:
        return self._call("liveness", "--agent-id", agent_id or self._require_agent())

    def status(self) -> dict[str, Any]:
        return self._call("status")

    # ---- groups --------------------------------------------------------

    def group_create(self, group_id: str, *, topic: str | None = None, rate_seconds: float = 2, ceiling_per_minute: int = 120) -> dict[str, Any]:
        agent = self._require_agent()
        argv: list[Any] = [
            "group", "create", "--from", agent, "--group-id", group_id,
            "--rate-seconds", rate_seconds, "--ceiling-per-minute", ceiling_per_minute,
        ]
        if topic:
            argv += ["--topic", topic]
        return self._call(*argv)

    def group_join(self, group_id: str, *, rate_seconds: float | None = None) -> dict[str, Any]:
        agent = self._require_agent()
        argv: list[Any] = ["group", "join", "--group-id", group_id, "--agent-id", agent]
        if rate_seconds is not None:
            argv += ["--rate-seconds", rate_seconds]
        return self._call(*argv)

    def group_send(self, group_id: str, text: str) -> dict[str, Any]:
        agent = self._require_agent()
        return self._call("group", "send", "--from", agent, "--group-id", group_id, "--text", text)

    def group_messages(self, group_id: str, *, limit: int = 50) -> dict[str, Any]:
        agent = self._require_agent()
        return self._call("group", "messages", "--group-id", group_id, "--agent-id", agent, "--limit", limit)

    def group_read(self, group_id: str, *, seq: int | None = None, allow_gap: bool = False) -> dict[str, Any]:
        agent = self._require_agent()
        argv: list[Any] = ["group", "read", "--group-id", group_id, "--agent-id", agent]
        if seq is not None:
            argv += ["--seq", seq]
        if allow_gap:
            argv += ["--allow-gap"]
        return self._call(*argv)


__all__ = ["Bus", "BusError"]
