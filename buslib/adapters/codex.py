"""Deliver message pointers to an existing Codex thread."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

from ..config import BUS_HOME, TOKEN_DIR
from ..db import db_path
from .base import AdapterError, DeliveryUnknownError, native_send_error


def codex_thread_id(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise SystemExit("error: Codex --session-id must be a thread UUID") from None


class CodexAdapter:
    def __init__(self, session_id: str, cli_path: str | None, cwd: str, remote: str | None = None):
        self.session_id = codex_thread_id(session_id)
        self.cwd = cwd
        self.remote = remote
        cli = shutil.which(cli_path or "codex")
        if cli is None:
            raise SystemExit("error: Codex CLI not found; install it or pass --cli /path/to/codex")
        self.cli = os.path.abspath(cli)
        try:
            help_result = self._run(["queue", "--help"])
        except (OSError, subprocess.SubprocessError) as exc:
            raise SystemExit(f"error: could not check Codex CLI: {exc}") from exc
        if help_result.returncode or "--resume" not in help_result.stdout:
            raise SystemExit("error: this Codex CLI needs an update to support `codex queue --resume`")
        try:
            from .codex_rpc import steer
        except ModuleNotFoundError as exc:
            if not exc.name or exc.name.split(".")[0] != "websockets":
                raise
            raise SystemExit("error: install the agent-bus dependencies to enable Codex steering") from exc
        self.steer = steer

        # Codex's server can have a different cwd and environment from the notifier.
        source_cli = Path(__file__).resolve().parents[2] / "bus.py"
        entrypoint = [str(source_cli)] if source_cli.is_file() else ["-m", "buslib.cli"]
        token_file = os.environ.get("AGENT_BUS_TOKEN_FILE")
        token_dir = Path(os.environ.get("AGENT_BUS_TOKEN_DIR") or TOKEN_DIR).expanduser().resolve()
        bus_env = ["env"]
        if not token_file:
            bus_env += ["-u", "AGENT_BUS_TOKEN_FILE"]
        bus_env += [f"AGENT_BUS_HOME={BUS_HOME}", f"AGENT_BUS_TOKEN_DIR={token_dir}"]
        if token_file:
            bus_env.append(f"AGENT_BUS_TOKEN_FILE={Path(token_file).expanduser().resolve()}")
        self.bus_command = [*bus_env, sys.executable, *entrypoint, "--db", str(db_path())]

    def _run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [self.cli, *args], cwd=self.cwd, stdin=subprocess.DEVNULL,
            capture_output=True, text=True, timeout=30,
        )

    def inject(self, message: dict[str, Any]) -> None:
        message_id = message["message_id"]
        agent_id = message["to_agent"]
        read = shlex.join([*self.bus_command, "message", "show", "--agent-id", agent_id, "--message-id", message_id])
        prompt = f"An agent-bus message arrived for {agent_id}. Read message {message_id} with\n{read}"
        if message_id.startswith("group-digest-"):
            group = shlex.join([
                *self.bus_command, "group", "messages", "--group-id", message["thread_id"], "--agent-id", agent_id,
            ])
            prompt += (
                f"\nRead the unread group messages with\n{group}\n"
                "Use `group read --seq <served_through_seq>` for each page you read."
            )
        else:
            prompt += (
                "\nHandle this message once. Use `agent-bus answer` on its thread if it is an ask. "
                f"If you reply, use `--idempotency-key reply-{message_id}`."
            )
        turn_id = self.steer(self.session_id, message_id, prompt, remote=self.remote, cwd=self.cwd)
        if turn_id:
            print(f"[codex {agent_id}] steered {message_id} into turn {turn_id}", flush=True)
            return
        args = ["queue", "--thread", self.session_id, "--resume", "--message", prompt]
        if self.remote:
            args += ["--remote", self.remote]
        try:
            result = self._run(args)
        except subprocess.TimeoutExpired as exc:
            raise DeliveryUnknownError("Codex queue timed out waiting for acknowledgement") from exc
        except (OSError, subprocess.SubprocessError) as exc:
            raise AdapterError(f"Codex queue failed: {exc}") from exc
        if result.returncode:
            detail = (result.stderr or result.stdout or "Codex queue failed").strip()
            raise native_send_error(detail)
        for line in result.stderr.splitlines():
            if line.startswith("Warning: message was queued, but could not resume thread "):
                # The queue accepted it. Re-enqueueing would start duplicate work.
                print(f"[codex {agent_id}] {line}", file=sys.stderr, flush=True)
