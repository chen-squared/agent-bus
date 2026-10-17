"""HTTP server and route dispatch for the localhost daemon."""

from __future__ import annotations

import argparse
import json
import os
import socketserver
import sys
import time
from http.server import BaseHTTPRequestHandler
from typing import Any

from ..config import machine_id
from . import blobs, handoff
from .auth import authorized_peer, read_bearer


class DaemonHandler(BaseHTTPRequestHandler):
    machine_id_value: str = "unknown"

    def _send(self, code: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _authorized_peer(self, token: str):
        return authorized_peer(token)

    def _read_bearer(self) -> str:
        return read_bearer(self.headers)

    def do_HEAD(self) -> None:
        sha = blobs.blob_sha(self.path)
        if sha is None:
            self._send(404, {"ok": False, "error": "not found"})
            return
        if not self._authorized_peer(self._read_bearer()):
            self._send(403, {"ok": False, "error": "unknown peer or bad token"})
            return
        blobs.serve_head(self, sha)

    def do_GET(self) -> None:
        if self.path.split("?", 1)[0] == "/health":
            self._send(200, {"ok": True, "machine_id": self.machine_id_value, "schema_version": 1})
            return
        sha = blobs.blob_sha(self.path)
        if sha is not None:
            if not self._authorized_peer(self._read_bearer()):
                self._send(403, {"ok": False, "error": "unknown peer or bad token"})
                return
            blobs.serve_get(self, sha)
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self) -> None:
        if self.path.split("?", 1)[0] != "/bus/handoff":
            self._send(404, {"ok": False, "error": "not found"})
            return
        peer = self._authorized_peer(self._read_bearer())
        if peer is None:
            self._send(403, {"ok": False, "error": "unknown peer or bad token"})
            return
        handoff.handle(self, peer)

    def log_message(self, fmt: str, *args: Any) -> None:
        # Used to be `return`: failed auth attempts and handoffs were never
        # logged anywhere, the one place the system had no audit trail at all.
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [daemon] {self.address_string()} {fmt % args}", file=sys.stderr, flush=True)


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="agent-bus daemon", allow_abbrev=False)
    parser.add_argument("--machine-id", default=os.environ.get("AGENT_BUS_MACHINE"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("AGENT_BUS_DAEMON_PORT", "8780")))
    parser.add_argument("--bind", default=os.environ.get("AGENT_BUS_DAEMON_BIND", "127.0.0.1"))
    args = parser.parse_args()
    os.environ.setdefault("AGENT_BUS_MACHINE", args.machine_id or "")
    DaemonHandler.machine_id_value = machine_id()
    if args.machine_id:
        os.environ["AGENT_BUS_MACHINE"] = args.machine_id
        DaemonHandler.machine_id_value = machine_id()

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    if args.bind not in {"127.0.0.1", "localhost", "::1"}:
        # AGENT_BUS.md promises "bind all HTTP transports to 127.0.0.1".
        # Default does; make an override loud instead of silent.
        print(f"warning: binding to {args.bind} exposes the token-auth API to the network; front it with an SSH tunnel", file=sys.stderr, flush=True)
    with socketserver.ThreadingTCPServer((args.bind, args.port), DaemonHandler) as httpd:
        print(f"bus daemon machine={DaemonHandler.machine_id_value} listen={args.bind}:{args.port}", flush=True)
        try:
            while True:
                httpd.handle_request()
        except KeyboardInterrupt:
            pass
    return 0


__all__ = ["DaemonHandler", "main"]
