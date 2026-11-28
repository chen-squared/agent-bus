"""Deliver message pointers to an existing Codex thread."""

from __future__ import annotations

import os
import shlex
import sys
import uuid
from pathlib import Path
from typing import Any

from ..config import BUS_HOME, TOKEN_DIR
from ..db import db_path


def codex_thread_id(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise SystemExit("error: Codex --session-id must be a thread UUID") from None


class CodexAdapter:
    def __init__(self, session_id: str, cwd: str, remote: str | None = None):
        self.session_id = codex_thread_id(session_id)
        self.cwd = cwd
        self.remote = remote
        try:
            from .codex_rpc import deliver
        except ModuleNotFoundError as exc:
            if not exc.name or exc.name.split(".")[0] != "websockets":
                raise
            raise SystemExit("error: install the agent-bus dependencies to enable Codex delivery") from exc
        self.deliver = deliver

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
        method, turn_id = self.deliver(self.session_id, message_id, prompt, remote=self.remote, cwd=self.cwd)
        if method == "turn/steer":
            print(f"[codex {agent_id}] steered {message_id} into turn {turn_id}", flush=True)
        else:
            print(f"[codex {agent_id}] accepted {message_id} via turn/start ({turn_id})", flush=True)
