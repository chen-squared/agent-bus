"""Start or steer a Codex turn through the App Server."""

from __future__ import annotations

import json
import os
import time
import uuid
from itertools import count
from pathlib import Path
from typing import Any

from websockets.exceptions import WebSocketException
from websockets.sync.client import ClientConnection, connect, unix_connect

from .. import __version__
from .base import AdapterError, DeliveryUnknownError, TransientAdapterError


class _RequestRejected(RuntimeError):
    def __init__(self, error: dict[str, Any]):
        super().__init__(str(error.get("message", "request rejected")))
        self.code = error.get("code")
        data = error.get("data")
        self.info = data.get("codexErrorInfo") if isinstance(data, dict) else None

    def can_retry_steer(self) -> bool:
        # These results confirm that Codex did not add the input to the turn.
        if self.code == -32601 or (isinstance(self.info, dict) and "activeTurnNotSteerable" in self.info):
            return True
        message = str(self)
        return message in {"no active turn to steer", "active turn uses a different output schema"} or message.startswith(
            ("expected active turn id ", "thread not found: ", "Invalid request: unknown variant `turn/steer`"),
        )


def default_socket_path() -> Path:
    codex_home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex").expanduser()
    return codex_home / "app-server-control" / "app-server-control.sock"


def _connect(remote: str | None, cwd: str) -> ClientConnection:
    if remote and not remote.startswith("unix://"):
        return connect(remote, open_timeout=3, close_timeout=0.5)

    socket_path = remote.removeprefix("unix://") if remote else ""
    path = Path(cwd) / (Path(socket_path).expanduser() if socket_path else default_socket_path())
    return unix_connect(str(path.resolve()), open_timeout=2, close_timeout=0.5)


def _call(socket: ClientConnection, request_id: int, method: str, params: dict[str, Any]) -> dict[str, Any]:
    socket.send(json.dumps({"id": request_id, "method": method, "params": params}))
    deadline = time.monotonic() + 5
    while (remaining := deadline - time.monotonic()) > 0:
        reply = json.loads(socket.recv(timeout=remaining))
        if not isinstance(reply, dict) or "method" in reply or reply.get("id") != request_id:
            continue
        if isinstance(reply.get("error"), dict):
            raise _RequestRejected(reply["error"])
        if not isinstance(reply.get("result"), dict):
            raise ValueError(f"invalid {method} response")
        return reply["result"]
    raise TimeoutError(f"timed out waiting for {method}")


def _thread_status(result: dict[str, Any], thread_id: str) -> str:
    thread = result.get("thread")
    if not isinstance(thread, dict) or thread.get("id") != thread_id:
        raise ValueError("invalid Codex thread metadata")
    status = thread.get("status")
    if not isinstance(status, dict) or not isinstance(status.get("type"), str):
        raise ValueError("invalid Codex thread status")
    return status["type"]


def _initialize(socket: ClientConnection) -> None:
    _call(socket, 1, "initialize", {"clientInfo": {"name": "agent-bus", "version": __version__}})
    socket.send(json.dumps({"method": "initialized"}))


def wait_for_thread(thread_id: str, socket_path: Path) -> None:
    """Wait for the local App Server to serve metadata for an existing thread."""
    deadline = time.monotonic() + 10
    while True:
        try:
            with _connect(f"unix://{socket_path}", str(socket_path.parent)) as socket:
                _initialize(socket)
                read = _call(socket, 2, "thread/read", {"threadId": thread_id, "includeTurns": False})
                _thread_status(read, thread_id)
            return
        except _RequestRejected as exc:
            raise AdapterError(f"App Server could not read thread {thread_id}: {exc}. Check the UUID and CODEX_HOME.") from exc
        except (OSError, ValueError, WebSocketException) as exc:
            if time.monotonic() >= deadline:
                raise AdapterError(f"Codex App Server did not become ready at {socket_path}: {exc}") from exc
            time.sleep(0.2)


def deliver(thread_id: str, message_id: str, prompt: str, *, remote: str | None, cwd: str) -> tuple[str, str]:
    """Return the method and turn ID accepted by the App Server."""
    method = None
    request_ids = count(2)
    try:
        with _connect(remote, cwd) as socket:
            _initialize(socket)
            target = {"threadId": thread_id}
            read = _call(socket, next(request_ids), "thread/read", {**target, "includeTurns": False})
            status = _thread_status(read, thread_id)
            if status == "notLoaded":
                resumed = _call(socket, next(request_ids), "thread/resume", {**target, "excludeTurns": True})
                status = _thread_status(resumed, thread_id)

            params = {
                **target,
                "clientUserMessageId": str(uuid.uuid5(uuid.UUID(thread_id), f"agent-bus:{message_id}")),
                "input": [{"type": "text", "text": prompt, "text_elements": []}],
            }
            if status == "active":
                history = _call(socket, next(request_ids), "thread/turns/list", {**target, "limit": 1, "itemsView": "notLoaded"})
                turns = history.get("data")
                turn = turns[0] if isinstance(turns, list) and turns else None
                if not isinstance(turn, dict) or turn.get("status") != "inProgress":
                    raise TransientAdapterError("Codex active turn could not be confirmed")
                turn_id = turn.get("id")
                if not isinstance(turn_id, str) or not turn_id:
                    raise ValueError("missing active Codex turn ID")
                method = "turn/steer"
                params["expectedTurnId"] = turn_id
                result = _call(socket, next(request_ids), method, params)
                if result.get("turnId") != turn_id:
                    raise ValueError("Codex did not confirm the target turn ID")
            elif status == "idle":
                method = "turn/start"
                result = _call(socket, next(request_ids), method, params)
                turn = result.get("turn")
                turn_id = turn.get("id") if isinstance(turn, dict) else None
                if not isinstance(turn_id, str) or not turn_id:
                    raise ValueError("Codex did not confirm the turn start")
            else:
                raise TransientAdapterError(f"Codex thread is unavailable ({status})")
            return method, turn_id
    except _RequestRejected as exc:
        if method is None:
            if str(exc).startswith(("thread not found:", "no rollout found for thread id")):
                raise AdapterError(f"Codex thread not found: {exc}") from exc
            raise TransientAdapterError(f"Codex thread status unavailable: {exc}") from exc
        if method == "turn/start":
            if exc.code == -32601:
                raise TransientAdapterError(f"Codex app server does not support turn/start: {exc}") from exc
            # Some App Server versions can report an error after starting the turn.
            raise DeliveryUnknownError(f"Codex turn/start did not confirm delivery: {exc}") from exc
        if not exc.can_retry_steer():
            raise AdapterError(f"Codex steer rejected: {exc}") from exc
        raise TransientAdapterError(f"Codex turn did not accept steering yet: {exc}") from exc
    except (OSError, ValueError, WebSocketException) as exc:
        # After sending the request, losing the response can mean it already ran.
        if method:
            raise DeliveryUnknownError(f"Codex {method} acknowledgement missing: {exc}") from exc
        raise TransientAdapterError(f"Codex app server unavailable: {exc}") from exc
