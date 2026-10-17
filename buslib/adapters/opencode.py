"""Inject a message into an opencode session."""

from __future__ import annotations

import json
import subprocess
from typing import Any

from .base import AdapterError


class OpencodeAdapter:
    def __init__(self, session_id: str, cli: str, cwd: str):
        self.session_id = session_id
        self.cli = cli
        self.cwd = cwd

    def inject(self, message: dict[str, Any]) -> None:
        payload = json.loads(message["payload"])
        text = payload.get("text", "")
        paths = message.get("_attachment_paths") or []
        rendered = f"[bus:{message['message_id']} from:{message['from_agent']} thread:{message['thread_id']}] {text}"
        if paths:
            rendered += "\nAttachments:\n" + "\n".join(paths)
        body = json.dumps({"text": rendered}, ensure_ascii=False)
        result = subprocess.run(
            [self.cli, "api", "session.prompt", "--param", f"sessionID={self.session_id}", "-d", body],
            cwd=self.cwd,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            raise AdapterError((result.stderr or result.stdout or "opencode prompt failed").strip())
