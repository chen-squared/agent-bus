"""Deliver message pointers to an existing Antigravity conversation."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from ..config import BUS_HOME, TOKEN_DIR
from ..db import db_path
from .base import AdapterError

_LS_ADDR_RE = re.compile(r"^(?:[A-Za-z0-9.-]+|\[[0-9A-Fa-f:]+\]):[0-9]{1,5}$")
_STRIP_ENV_VARS = (
    "ANTIGRAVITY_CONVERSATION_ID",
    "ANTIGRAVITY_TRAJECTORY_ID",
    "ANTIGRAVITY_SOURCE_METADATA",
)


def antigravity_conversation_id(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except (TypeError, ValueError):
        raise SystemExit("error: Antigravity --session-id must be a conversation UUID") from None


def normalize_ls_address(value: str | None = None) -> str:
    raw = (
        value
        if value is not None
        else os.environ.get("AGENT_BUS_ANTIGRAVITY_LS_ADDRESS") or os.environ.get("ANTIGRAVITY_LS_ADDRESS") or ""
    )
    addr = raw.strip()
    if not addr:
        return ""
    if addr.startswith("http://"):
        addr = addr[len("http://") :]
    addr = addr.rstrip("/")
    if not _LS_ADDR_RE.fullmatch(addr):
        raise SystemExit(
            f"error: invalid Antigravity language server address {raw!r}; expected host:port"
        )
    return addr


def resolve_csrf_token(override: str | None = None) -> str:
    raw = override if override is not None else os.environ.get("ANTIGRAVITY_CSRF_TOKEN") or ""
    return raw.strip()


def resolve_antigravity_cli(cli_path: str | None = None) -> list[str] | None:
    if cli_path:
        resolved = shutil.which(cli_path)
        if resolved is None:
            cand = Path(cli_path).expanduser()
            if cand.is_file() and os.access(cand, os.X_OK):
                resolved = str(cand)
        if resolved is None:
            return None
        abs_path = os.path.abspath(resolved)
        if Path(abs_path).name == "language_server":
            return [abs_path, "agentapi"]
        return [abs_path]

    found = shutil.which("agentapi")
    if found:
        return [os.path.abspath(found)]

    app_data = os.environ.get("ANTIGRAVITY_APP_DATA_DIR")
    candidates: list[Path] = []
    if app_data:
        candidates.append(Path(app_data).expanduser() / "bin" / "agentapi")
    candidates.append(Path.home() / ".gemini" / "antigravity" / "bin" / "agentapi")
    for cand in candidates:
        if cand.is_file() and os.access(cand, os.X_OK):
            return [str(cand.resolve())]

    ls_candidates: list[Path] = []
    if exe := os.environ.get("ANTIGRAVITY_AGENTAPI_EXE"):
        ls_candidates.append(Path(exe).expanduser())
    ls_candidates.append(Path("/Applications/Antigravity.app/Contents/Resources/bin/language_server"))
    for cand in ls_candidates:
        if cand.is_file() and os.access(cand, os.X_OK):
            return [str(cand.resolve()), "agentapi"]
    return None


def _agentapi_env(ls_address: str, csrf_token: str) -> dict[str, str]:
    env = dict(os.environ)
    for key in _STRIP_ENV_VARS:
        env.pop(key, None)
    env["ANTIGRAVITY_LS_ADDRESS"] = ls_address
    env["ANTIGRAVITY_CSRF_TOKEN"] = csrf_token
    return env


def _json_error(text: str) -> str | None:
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, str) and err.strip():
            return err.strip()
        if isinstance(err, dict):
            msg = err.get("message") or json.dumps(err, ensure_ascii=False)
            return str(msg).strip()
    return None


def get_conversation(
    conversation_id: str,
    *,
    ls_address: str,
    csrf_token: str,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Read conversation state from Antigravity's ConversationService."""
    url = f"http://{ls_address}/product.v1.ConversationService/GetConversation"
    body = json.dumps({"conversationId": conversation_id}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Connect-Protocol-Version": "1",
            "x-antigravity-csrf-token": csrf_token,
        },
    )
    # Bypass system/env HTTP proxies for direct local language server calls.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        err_body = ""
        try:
            err_body = exc.read().decode("utf-8", errors="replace")
        except OSError:
            pass
        code = ""
        message = f"HTTP {exc.code}"
        try:
            parsed = json.loads(err_body)
            if isinstance(parsed, dict):
                if isinstance(parsed.get("code"), str):
                    code = parsed["code"]
                if isinstance(parsed.get("message"), str) and parsed["message"].strip():
                    message = parsed["message"].strip()
        except json.JSONDecodeError:
            if err_body.strip():
                message = err_body.strip()[:300]
        if not code:
            if exc.code == 404:
                code = "not_found"
            elif exc.code in {401, 403}:
                code = "unauthenticated"
            else:
                code = f"http_{exc.code}"
        return {
            "ok": False,
            "http_status": exc.code,
            "code": code,
            "probe_error": message[:300],
            "ls_address": ls_address,
            "conversation_id": conversation_id,
        }
    except (OSError, urllib.error.URLError, TimeoutError, ValueError) as exc:
        return {
            "ok": False,
            "code": "unreachable",
            "probe_error": f"{type(exc).__name__}: {exc}"[:300],
            "ls_address": ls_address,
            "conversation_id": conversation_id,
        }

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {
            "ok": False,
            "code": "invalid_json",
            "probe_error": "GetConversation returned non-JSON",
            "ls_address": ls_address,
            "conversation_id": conversation_id,
        }
    if not isinstance(data, dict):
        return {
            "ok": False,
            "code": "invalid_json",
            "probe_error": "GetConversation returned unexpected payload",
            "ls_address": ls_address,
            "conversation_id": conversation_id,
        }
    state = data.get("state") if isinstance(data.get("state"), dict) else {}
    annotations = data.get("annotations") if isinstance(data.get("annotations"), dict) else {}
    metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    return {
        "ok": True,
        "conversation_id": data.get("id") or conversation_id,
        "run_state": state.get("runState"),
        "update_time": state.get("updateTime"),
        "archived": bool(annotations.get("archived")),
        "title": annotations.get("title") or state.get("summary") or "",
        "project_id": metadata.get("projectId"),
        "ls_address": ls_address,
    }


def probe_antigravity_session(
    session_id: str,
    *,
    ls_address: str | None = None,
    csrf_token: str | None = None,
    cli_path: str | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Best-effort Antigravity conversation probe (never exposes CSRF token)."""
    if not session_id:
        return {"ok": False, "code": "missing_session", "probe_error": "no conversation UUID registered"}
    try:
        addr = normalize_ls_address(ls_address)
    except SystemExit as exc:
        return {"ok": False, "code": "invalid_address", "probe_error": str(exc)}
    if not addr:
        return {"ok": False, "code": "missing_address", "probe_error": "ANTIGRAVITY_LS_ADDRESS is not set"}
    token = resolve_csrf_token(csrf_token)
    if not token:
        return {"ok": False, "code": "missing_token", "probe_error": "ANTIGRAVITY_CSRF_TOKEN is not set"}

    probe = get_conversation(session_id, ls_address=addr, csrf_token=token, timeout=timeout)
    if probe.get("ok") or probe.get("code") in {"not_found", "unauthenticated"}:
        return probe

    # Fall back to `agentapi get-conversation-metadata` if ConversationService
    # is unimplemented or unavailable on this server build.
    cli_prefix = resolve_antigravity_cli(cli_path)
    if cli_prefix is None:
        return probe
    try:
        res = subprocess.run(
            [*cli_prefix, "get-conversation-metadata", session_id],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=_agentapi_env(addr, token),
        )
    except (OSError, subprocess.SubprocessError):
        return probe
    err = _json_error(res.stdout)
    if res.returncode == 0 and not err:
        return {
            "ok": True,
            "conversation_id": session_id,
            "run_state": None,
            "update_time": None,
            "archived": False,
            "ls_address": addr,
            "source": "agentapi",
        }
    return {
        "ok": False,
        "code": "not_found" if err and "not_found" in err.lower() else probe.get("code", "probe_failed"),
        "probe_error": (err or res.stderr or res.stdout or probe.get("probe_error") or "probe failed").strip()[:300],
        "ls_address": addr,
        "conversation_id": session_id,
    }


class AntigravityAdapter:
    def __init__(
        self,
        session_id: str,
        cli_path: str | None,
        cwd: str,
        ls_address: str | None = None,
    ):
        self.session_id = antigravity_conversation_id(session_id)
        self.cwd = cwd
        self.ls_address = normalize_ls_address(ls_address)
        if not self.ls_address:
            raise SystemExit(
                "error: ANTIGRAVITY_LS_ADDRESS is required for the antigravity adapter "
                "(start from Antigravity or pass --antigravity-ls-address)"
            )
        self.csrf_token = resolve_csrf_token()
        if not self.csrf_token:
            raise SystemExit("error: ANTIGRAVITY_CSRF_TOKEN is required for the antigravity adapter")

        cli_prefix = resolve_antigravity_cli(cli_path)
        if cli_prefix is None:
            raise SystemExit(
                "error: Antigravity agentapi CLI not found; install Antigravity or pass --cli /path/to/agentapi"
            )
        self.cli_prefix = cli_prefix
        try:
            help_result = self._run(["--help"])
        except (OSError, subprocess.SubprocessError) as exc:
            raise SystemExit(f"error: could not check Antigravity agentapi CLI: {exc}") from exc
        help_text = (help_result.stdout or "") + "\n" + (help_result.stderr or "")
        if help_result.returncode or "send-message" not in help_text:
            raise SystemExit("error: Antigravity CLI does not support `agentapi send-message`")

        # The target Antigravity conversation can have a different cwd, PATH,
        # or AGENT_BUS_HOME from the notifier.
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

    def _run(self, args: list[str], *, timeout: float = 30.0) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [*self.cli_prefix, *args],
            cwd=self.cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=_agentapi_env(self.ls_address, self.csrf_token),
        )

    def _check_target(self) -> None:
        # `agentapi send-message` exits 0 even for deleted/unknown conversation
        # IDs. Verify target existence and runState before pushing so messages
        # are never silently acked into a nonexistent conversation.
        probe = get_conversation(
            self.session_id,
            ls_address=self.ls_address,
            csrf_token=self.csrf_token,
            timeout=10.0,
        )
        if probe.get("ok"):
            if probe.get("run_state") == "RUN_STATE_CANCELLING":
                raise AdapterError(f"Antigravity conversation {self.session_id} busy (RUN_STATE_CANCELLING)")
            return
        code = probe.get("code")
        if code == "not_found":
            raise AdapterError(f"Antigravity conversation {self.session_id} not found")
        if code == "unauthenticated":
            raise AdapterError(
                "Antigravity authentication failed; restart the notifier with a current ANTIGRAVITY_CSRF_TOKEN"
            )
        try:
            meta = self._run(["get-conversation-metadata", self.session_id], timeout=15.0)
        except (OSError, subprocess.SubprocessError) as exc:
            raise AdapterError(f"Antigravity conversation check failed: {exc}") from exc
        err = _json_error(meta.stdout)
        if meta.returncode != 0 or err:
            detail = err or (meta.stderr or meta.stdout or probe.get("probe_error") or "conversation check failed").strip()
            raise AdapterError(f"Antigravity conversation check failed: {detail}")

    def _format_prompt(self, message: dict[str, Any]) -> str:
        message_id = message["message_id"]
        agent_id = message["to_agent"]
        thread_id = message["thread_id"]
        from_agent = message["from_agent"]
        message_type = message.get("message_type") or "text"

        read = shlex.join([*self.bus_command, "message", "show", "--agent-id", agent_id, "--message-id", message_id])
        prompt = (
            f"An agent-bus message arrived for {agent_id} from {from_agent} (thread {thread_id}). "
            f"Read message {message_id} with:\n{read}"
        )
        if message_id.startswith("group-digest-"):
            group = shlex.join([
                *self.bus_command, "group", "messages", "--group-id", thread_id, "--agent-id", agent_id,
            ])
            prompt += (
                f"\nRead the unread group messages with:\n{group}\n"
                f"Use the same bus command with `group read --group-id {shlex.quote(thread_id)} "
                f"--agent-id {shlex.quote(agent_id)} --seq <served_through_seq>` for each page you read "
                f"(or `group catch-up`), and `group send --from {shlex.quote(agent_id)} "
                f"--group-id {shlex.quote(thread_id)} --text '...'` if a reply is needed."
            )
        elif message_type == "system" or from_agent == "system":
            prompt += "\nHandle this agent-bus system notice once."
        elif message_type == "ask":
            reply_cmd = shlex.join([
                *self.bus_command, "answer",
                "--thread-id", thread_id,
                "--from", agent_id,
                "--to", from_agent,
                "--answer-to", message_id,
                "--idempotency-key", f"reply-{message_id}",
                "--text", "<your reply>",
            ])
            prompt += (
                "\nHandle this ask once. Do NOT reply only in chat; send your answer over agent-bus with:\n"
                f"{reply_cmd}"
            )
        else:
            reply_cmd = shlex.join([
                *self.bus_command, "send",
                "--thread-id", thread_id,
                "--from", agent_id,
                "--to", from_agent,
                "--idempotency-key", f"reply-{message_id}",
                "--text", "<your reply>",
            ])
            prompt += (
                "\nHandle this message once. If a reply is needed, reply over agent-bus (not only in chat) with:\n"
                f"{reply_cmd}\n"
                "Use `answer` instead of `send` if answering an ask."
            )
        return prompt

    def inject(self, message: dict[str, Any]) -> None:
        self._check_target()
        prompt = self._format_prompt(message)
        title = f"agent-bus: {message['from_agent']}"
        try:
            result = self._run(["send-message", f"--title={title}", self.session_id, prompt])
        except (OSError, subprocess.SubprocessError) as exc:
            raise AdapterError(f"Antigravity send-message failed: {exc}") from exc
        err = _json_error(result.stdout)
        if result.returncode != 0 or err:
            detail = err or (result.stderr or result.stdout or "Antigravity send-message failed").strip()
            raise AdapterError(f"Antigravity send-message failed: {detail}")


__all__ = [
    "AntigravityAdapter",
    "antigravity_conversation_id",
    "get_conversation",
    "normalize_ls_address",
    "probe_antigravity_session",
    "resolve_antigravity_cli",
    "resolve_csrf_token",
]
