"""Open a saved chat in ChatGPT Desktop on the shared Codex App Server."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys

from .adapters.base import AdapterError
from .adapters.codex import codex_thread_id


APP_ID = "com.openai.codex"
BUNDLED_CODEX = "/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex"


def _run(argv: list[str], action: str, *, timeout: float) -> str:
    try:
        result = subprocess.run(argv, capture_output=True, text=True, check=True, timeout=timeout)
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or str(exc)).strip()
        raise SystemExit(f"error: {action}: {detail}") from None
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(f"error: {action}: {exc}") from None
    return result.stdout.strip()


def _require_desktop_closed() -> None:
    running = _run(
        ["/usr/bin/osascript", "-e", f'application id "{APP_ID}" is running'],
        "could not check ChatGPT Desktop. Run this from a macOS Terminal",
        timeout=5,
    )
    if running == "true":
        raise SystemExit(
            "error: ChatGPT Desktop is already running. Wait for active turns to finish, quit with Command-Q, then retry."
        )
    if running != "false":
        raise SystemExit("error: could not confirm ChatGPT Desktop is closed")


def cmd_codex_desktop(args: argparse.Namespace) -> int:
    if sys.platform != "darwin":
        raise SystemExit("error: agent-bus codex desktop requires macOS")
    session_id = args.session_id or os.environ.get("CODEX_THREAD_ID")
    if not session_id:
        raise SystemExit("error: pass --session-id with the existing Codex thread UUID")
    thread_id = codex_thread_id(session_id)
    try:
        from .adapters.codex_rpc import default_socket_path, wait_for_thread
    except ModuleNotFoundError as exc:
        if exc.name and exc.name.split(".")[0] == "websockets":
            raise SystemExit("error: install the agent-bus dependencies to enable Codex Desktop launch") from None
        raise

    socket_path = default_socket_path().absolute()
    # Desktop's ws+unix client uses the URL pathname as-is for the socket path.
    if any(ord(char) <= 32 or ord(char) >= 127 or char in ':"<>?#`{}' for char in str(socket_path)):
        raise SystemExit("error: the Codex socket path cannot be used as a Desktop URL. Check CODEX_HOME for spaces or URL delimiters.")
    _require_desktop_closed()
    codex = shutil.which(args.cli) if args.cli else shutil.which(BUNDLED_CODEX) or shutil.which("codex")
    if codex is None:
        raise SystemExit("error: Codex CLI unavailable. Pass --cli /path/to/codex.")

    _run([codex, "app-server", "daemon", "start"], "could not start the Codex App Server daemon", timeout=60)
    try:
        wait_for_thread(thread_id, socket_path)
    except AdapterError as exc:
        raise SystemExit(f"error: {exc}") from None
    _require_desktop_closed()

    argv = ["/usr/bin/open", "-b", APP_ID, "--env", f"CODEX_APP_SERVER_WS_URL=ws+unix://localhost{socket_path}:/rpc"]
    if os.environ.get("CODEX_HOME"):
        argv += ["--env", f"CODEX_HOME={socket_path.parents[1]}"]
    argv += ["-u", f"codex://threads/{thread_id}"]
    _run(argv, "could not open ChatGPT Desktop", timeout=10)
    print(f"Opening Codex thread {thread_id} on {socket_path}")
    return 0
