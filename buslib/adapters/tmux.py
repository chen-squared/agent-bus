"""Inject a message into a tmux pane, as a final fallback."""

from __future__ import annotations

import json
import subprocess
import time
from typing import Any

from .base import AdapterError


class TmuxAdapter:
    def __init__(self, target: str):
        self.target = target

    def is_idle(self) -> bool:
        out = subprocess.run(
            ["tmux", "capture-pane", "-t", self.target, "-p"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if out.returncode != 0:
            return True
        first = out.stdout
        time.sleep(0.3)
        out2 = subprocess.run(
            ["tmux", "capture-pane", "-t", self.target, "-p"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return out2.returncode == 0 and out2.stdout == first

    def inject(self, message: dict[str, Any]) -> None:
        if not self.is_idle():
            raise AdapterError("target tmux pane busy")
        payload = json.loads(message["payload"])
        paths = message.get("_attachment_paths") or []
        text = f"[bus:{message['message_id']} from:{message['from_agent']} thread:{message['thread_id']}] {payload.get('text','')}"
        if paths:
            text += " Attachments: " + " ".join(paths)
        # send-keys -l delivers the string literally, and the trailing Enter
        # submits it: every embedded newline would execute as its own command
        # line in a shell pane. Flatten to a single line.
        text = text.replace("\r", " ").replace("\n", " ")
        subprocess.run(["tmux", "send-keys", "-t", self.target, "-l", text], check=True, timeout=10)
        time.sleep(0.1)
        subprocess.run(["tmux", "send-keys", "-t", self.target, "Enter"], check=True, timeout=10)
