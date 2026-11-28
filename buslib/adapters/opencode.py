"""Inject a message into an opencode session."""

from __future__ import annotations

import json
import subprocess
from typing import Any

from ..agents import resolve_opencode_cli
from .base import AdapterError, TransientAdapterError, is_transient_error


class OpencodeAdapter:
    def __init__(self, session_id: str, cli: str | None, cwd: str):
        self.session_id = session_id
        self.cli_override = cli
        self.cli = cli or resolve_opencode_cli()
        self.cwd = cwd

    def _current_cli(self) -> str:
        if self.cli_override:
            return self.cli_override
        resolved = resolve_opencode_cli()
        if resolved != self.cli:
            self.cli = resolved
        return self.cli

    def _run_prompt(self, cli: str, body: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [cli, "api", "session.prompt", "--param", f"sessionID={self.session_id}", "-d", body],
            cwd=self.cwd,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def inject(self, message: dict[str, Any]) -> None:
        payload = json.loads(message["payload"])
        text = payload.get("text", "")
        paths = message.get("_attachment_paths") or []
        rendered = f"[bus:{message['message_id']} from:{message['from_agent']} thread:{message['thread_id']}] {text}"
        if paths:
            rendered += "\nAttachments:\n" + "\n".join(paths)
        body = json.dumps({"text": rendered}, ensure_ascii=False)

        cli = self._current_cli()
        try:
            result = self._run_prompt(cli, body)
        except FileNotFoundError as exc:
            # OpenCode desktop can swap out its versioned cli/<ver>/ directory
            # mid-flight. Re-scan once before failing when using autodetection.
            if not self.cli_override:
                refreshed = resolve_opencode_cli()
                if refreshed != cli:
                    self.cli = refreshed
                    try:
                        result = self._run_prompt(refreshed, body)
                    except subprocess.TimeoutExpired as timeout_exc:
                        raise TransientAdapterError(f"opencode prompt timed out: {timeout_exc}") from timeout_exc
                    except OSError as retry_exc:
                        raise AdapterError(f"opencode CLI unavailable at {refreshed}: {retry_exc}") from retry_exc
                else:
                    raise AdapterError(f"opencode CLI not found at {cli}: {exc}") from exc
            else:
                raise AdapterError(f"opencode CLI not found at {cli}: {exc}") from exc
        except subprocess.TimeoutExpired as exc:
            raise TransientAdapterError(f"opencode prompt timed out: {exc}") from exc
        except OSError as exc:
            raise AdapterError(f"opencode prompt failed to launch: {exc}") from exc

        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "opencode prompt failed").strip()
            err = AdapterError(detail)
            if is_transient_error(err):
                raise TransientAdapterError(detail)
            raise err
