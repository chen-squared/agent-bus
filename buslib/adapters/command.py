"""Run one shell hook for a message (pointer-first, text via env/stdin)."""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any

from .base import AdapterError


class CommandAdapter:
    def __init__(self, template: str, session_id: str, allow_unsafe_text: bool = False):
        self.template = template
        self.session_id = session_id
        # {text} is interpolated into a shell command. Even shlex.quote cannot
        # make that safe when the template itself wraps the placeholder in
        # quotes, and a remote peer controls the text. The supported path is
        # AGENT_BUS_TEXT / stdin; keep {text} behind an explicit opt-in.
        if "{text}" in template and not allow_unsafe_text:
            raise SystemExit(
                "error: notify template contains {text}, which is shell-injection-prone; "
                "use {message_id}/{thread_id}/{from_agent} plus $AGENT_BUS_TEXT or stdin, "
                "or set AGENT_BUS_ALLOW_UNSAFE_TEXT=1 to accept the risk"
            )

    def inject(self, message: dict[str, Any]) -> None:
        payload = json.loads(message["payload"])
        paths = message.get("_attachment_paths") or []
        text = payload.get("text", "")
        if paths:
            text += " Attachments: " + " ".join(paths)
        text = text.replace("\n", " ")
        cmd = self.template.format(
            message_id=message["message_id"],
            thread_id=message["thread_id"],
            from_agent=message["from_agent"],
            agent_id=message["to_agent"],
            session_id=self.session_id,
            text=text,
        )
        env = dict(os.environ)
        env.update({
            "AGENT_BUS_MESSAGE_ID": str(message["message_id"]),
            "AGENT_BUS_THREAD_ID": str(message["thread_id"]),
            "AGENT_BUS_FROM_AGENT": str(message["from_agent"]),
            "AGENT_BUS_TO_AGENT": str(message["to_agent"]),
            "AGENT_BUS_TEXT": text,
        })
        result = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True, timeout=120, env=env, input=text)
        if result.returncode != 0:
            raise AdapterError((result.stderr or result.stdout or "notify command failed").strip())
