"""Send a message pointer to an active Codex App Server turn."""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

from websockets.exceptions import WebSocketException
from websockets.sync.client import ClientConnection, connect, unix_connect

from .. import __version__
from .base import AdapterError, DeliveryUnknownError


class _RequestRejected(RuntimeError):
    def __init__(self, error: dict[str, Any]):
        super().__init__(str(error.get("message", "request rejected")))
        self.code = error.get("code")
        data = error.get("data")
        self.info = data.get("codexErrorInfo") if isinstance(data, dict) else None

    def can_queue(self) -> bool:
        # These results confirm that Codex did not add the input to the turn.
        if self.code == -32601 or (isinstance(self.info, dict) and "activeTurnNotSteerable" in self.info):
            return True
        message = str(self)
        return message in {"no active turn to steer", "active turn uses a different output schema"} or message.startswith(
            ("expected active turn id ", "thread not found: ", "Invalid request: unknown variant `turn/steer`"),
        )


def _connect(remote: str | None, cwd: str) -> ClientConnection:
    if remote and not remote.startswith("unix://"):
        return connect(remote, open_timeout=3, close_timeout=0.5)

    socket_path = remote.removeprefix("unix://") if remote else ""
    if not socket_path:
        codex_home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex").expanduser()
        socket_path = codex_home / "app-server-control" / "app-server-control.sock"
    path = Path(cwd) / Path(socket_path).expanduser()
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


def steer(thread_id: str, message_id: str, prompt: str, *, remote: str | None, cwd: str) -> str | None:
    """Return the turn ID if accepted, or None when it is safe to queue."""
    submitted = False
    try:
        with _connect(remote, cwd) as socket:
            _call(socket, 1, "initialize", {"clientInfo": {"name": "agent-bus", "version": __version__}})
            socket.send(json.dumps({"method": "initialized"}))
            history = _call(socket, 2, "thread/turns/list", {"threadId": thread_id, "limit": 1, "itemsView": "notLoaded"})
            turns = history.get("data")
            if not isinstance(turns, list) or not turns:
                return None
            turn = turns[0]
            if not isinstance(turn, dict) or turn.get("status") != "inProgress":
                return None
            turn_id = turn.get("id")
            if not isinstance(turn_id, str) or not turn_id:
                return None
            params = {
                "threadId": thread_id,
                "expectedTurnId": turn_id,
                "clientUserMessageId": str(uuid.uuid5(uuid.UUID(thread_id), f"agent-bus:{message_id}")),
                "input": [{"type": "text", "text": prompt, "text_elements": []}],
            }
            submitted = True
            result = _call(socket, 3, "turn/steer", params)
            if result.get("turnId") != turn_id:
                raise ValueError("Codex did not confirm the target turn ID")
            return turn_id
    except _RequestRejected as exc:
        if submitted and not exc.can_queue():
            raise AdapterError(f"Codex steer rejected: {exc}") from exc
    except (OSError, ValueError, WebSocketException) as exc:
        # After sending the request, losing the response can mean it already ran.
        if submitted:
            raise DeliveryUnknownError(f"Codex steer acknowledgement missing: {exc}") from exc
    return None
