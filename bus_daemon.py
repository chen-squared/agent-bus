#!/usr/bin/env python3
"""Per-machine bus daemon for v1 handoff.

Runs a localhost HTTP API. Remote peers send handoffs here. This keeps the
SQLite database and notifier local to the machine. Reverse SSH tunnels are
expected to front this daemon when peers are on other hosts.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import socketserver
import sys
import time
import uuid
from http.server import BaseHTTPRequestHandler
from typing import Any

import bus

MAX_HANDOFF_BYTES = 16 * 1024 * 1024


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
        if not token:
            return None
        conn = bus.connect()
        # Constant-time compare; the old `==` over a full table scan was both
        # a timing side channel and duplicated inline in do_POST.
        for row in conn.execute("SELECT * FROM peers").fetchall():
            if row["auth_token"] and secrets.compare_digest(str(row["auth_token"]), token):
                conn.close()
                return row
        conn.close()
        return None

    def _read_bearer(self) -> str:
        auth = self.headers.get("Authorization", "")
        return auth[len("Bearer ") :].strip() if auth.lower().startswith("bearer ") else ""

    def _blob_sha(self) -> str | None:
        prefix = "/bus/blob/"
        path = self.path.split("?", 1)[0]
        if not path.startswith(prefix):
            return None
        sha = path[len(prefix):].strip("/")
        if not sha or "/" in sha or not bus.SHA256_RE.fullmatch(sha):
            return None
        return sha

    def do_HEAD(self) -> None:
        sha = self._blob_sha()
        if sha is None:
            self._send(404, {"ok": False, "error": "not found"})
            return
        auth = self._read_bearer()
        if not self._authorized_peer(auth):
            self._send(403, {"ok": False, "error": "unknown peer or bad token"})
            return
        p = bus.blob_path(sha)
        if not p.exists() or bus.sha256_file(p) != sha:
            self._send(404, {"ok": False, "error": "blob not found"})
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(p.stat().st_size))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self) -> None:
        if self.path.split("?", 1)[0] == "/health":
            self._send(200, {"ok": True, "machine_id": self.machine_id_value, "schema_version": 1})
            return
        sha = self._blob_sha()
        if sha is not None:
            auth = self._read_bearer()
            if not self._authorized_peer(auth):
                self._send(403, {"ok": False, "error": "unknown peer or bad token"})
                return
            p = bus.blob_path(sha)
            if not p.exists() or bus.sha256_file(p) != sha:
                self._send(404, {"ok": False, "error": "blob not found"})
                return
            size = p.stat().st_size
            range_header = self.headers.get("Range")
            start = 0
            end = size - 1
            if range_header and range_header.lower().startswith("bytes="):
                spec = range_header.split("=", 1)[1].strip()
                parts = spec.split("-", 1)
                try:
                    if parts[0]:
                        start = int(parts[0])
                    # Suffix range `bytes=-N` means the LAST N bytes; the old
                    # parse returned the FIRST N with a 206 and a wrong
                    # Content-Range, i.e. silently wrong data.
                    elif parts[1]:
                        start = max(0, size - int(parts[1]))
                    if parts[0] and parts[1]:
                        end = int(parts[1])
                except ValueError:
                    self._send(416, {"ok": False, "error": "invalid range"})
                    return
                if start < 0 or end >= size or start > end:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{size}")
                    self.end_headers()
                    return
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
                self.send_header("Content-Length", str(end - start + 1))
            else:
                self.send_response(200)
                self.send_header("Content-Length", str(size))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            with open(p, "rb") as f:
                f.seek(start)
                remaining = end - start + 1
                while remaining > 0:
                    chunk = f.read(min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self) -> None:
        if self.path.split("?", 1)[0] != "/bus/handoff":
            self._send(404, {"ok": False, "error": "not found"})
            return
        auth = self._read_bearer()
        # Content-Length used to be int()'d unguarded (non-numeric -> uncaught
        # ValueError outside the try) and read without a cap (memory DoS).
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
        except ValueError:
            self._send(400, {"ok": False, "error": "invalid Content-Length"})
            return
        if length < 0 or length > MAX_HANDOFF_BYTES:
            self._send(413, {"ok": False, "error": f"body too large (max {MAX_HANDOFF_BYTES} bytes)"})
            return
        try:
            envelope = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send(400, {"ok": False, "error": "invalid json"})
            return

        peer = self._authorized_peer(auth)
        if peer is None:
            self._send(403, {"ok": False, "error": "unknown peer or bad token"})
            return
        conn = bus.connect()
        if envelope.get("schema_version", 1) != 1:
            conn.close()
            self._send(400, {"ok": False, "error": "unsupported schema_version, upgrade peer"})
            return

        message = envelope.get("message")
        if not isinstance(message, dict):
            conn.close()
            self._send(400, {"ok": False, "error": "missing message"})
            return
        # Identity binding: the AUTHENTICATED peer names the sender. The
        # envelope's claimed from_agent/from_machine are attacker-supplied and
        # used to let a peer authenticated as machine-b insert a message as
        # `cat-jam`, spoofing a local agent.
        #
        # The claimed local part and every id that reaches a shell template or
        # a pointer file must pass the same closed charset as the CLI. Without
        # this a peer could send from_agent="x'; touch /tmp/pwn; echo '" and
        # have a command-adapter notifier execute it.
        claimed = str(message.get("from_agent", "unknown"))
        local_part = claimed.split("@", 1)[0] or "unknown"
        to_agent = message.get("to_agent")
        thread_id = message.get("thread_id") or None
        if not isinstance(to_agent, str) or not to_agent:
            conn.close()
            self._send(400, {"ok": False, "error": "missing or invalid to_agent"})
            return
        if thread_id is not None and not isinstance(thread_id, str):
            conn.close()
            self._send(400, {"ok": False, "error": "invalid thread_id"})
            return
        try:
            bus.validate_id("agent_id", local_part)
            bus.validate_id("agent_id", to_agent)
            bus.validate_id("thread_id", thread_id if thread_id else None)
        except SystemExit as exc:
            conn.close()
            self._send(400, {"ok": False, "error": str(exc)})
            return
        from_agent = f"{local_part}@{peer['peer_name']}"
        # message_id is generated locally: an attacker-chosen PRIMARY KEY let
        # a peer pre-empt or suppress a future local message id. Retry dedup
        # rides on the peer-namespaced idempotency_key instead.
        message_id = str(uuid.uuid4())
        peer_key = message.get("idempotency_key")
        if peer_key is not None:
            peer_key = str(peer_key)
            if len(peer_key) > 200 or any(ord(ch) < 32 for ch in peer_key):
                conn.close()
                self._send(400, {"ok": False, "error": "invalid idempotency_key"})
                return
        idem = bus.scoped_idempotency_key(peer["peer_name"], peer_key, namespace="remote") if peer_key else None
        if idem:
            existing = conn.execute("SELECT message_id FROM messages WHERE idempotency_key=?", (idem,)).fetchone()
            if existing is not None:
                conn.close()
                self._send(200, {"ok": True, "accepted": "duplicate", "receipt_id": existing["message_id"]})
                return
        attachments = message.get("attachments")
        if isinstance(attachments, str):
            try:
                attachments = json.loads(attachments or "[]")
            except (json.JSONDecodeError, TypeError):
                attachments = []
        if attachments is not None and not isinstance(attachments, list):
            conn.close()
            self._send(400, {"ok": False, "error": "attachments must be a list"})
            return
        if isinstance(attachments, list):
            for item in attachments:
                if isinstance(item, str):
                    # Local path strings are meaningful only inside the sending
                    # machine; accepting one lets a peer plant an arbitrary path
                    # in the recipient's attachment list.
                    conn.close()
                    self._send(400, {"ok": False, "error": "remote attachments must be blob manifests, not local paths"})
                    return
                if isinstance(item, dict):
                    item.pop("path", None)
                    if "sha256" not in item:
                        conn.close()
                        self._send(400, {"ok": False, "error": "remote attachment missing sha256"})
                        return
                    try:
                        bus.validate_sha256(item["sha256"])
                    except ValueError as exc:
                        conn.close()
                        self._send(400, {"ok": False, "error": str(exc)})
                        return
                    for key in ("origin", "name"):
                        if isinstance(item.get(key), str):
                            item[key] = os.path.basename(item[key])
            message["attachments"] = attachments
        correlation_id = message.get("correlation_id")
        if correlation_id is not None:
            correlation_id = str(correlation_id)
            if len(correlation_id) > 128 or any(ord(ch) < 32 for ch in correlation_id):
                conn.close()
                self._send(400, {"ok": False, "error": "invalid correlation_id"})
                return
        mtype = message.get("message_type", "text")
        expects = 1 if message.get("expects_response") or mtype == "ask" else 0
        now_ts = bus.now()
        try:
            response_deadline = message.get("response_deadline")
            if response_deadline is None and expects:
                response_deadline = now_ts + bus.NUDGE_FIRST_SECONDS
            response_deadline = float(response_deadline) if response_deadline is not None else None
            ttl = message.get("ttl_seconds")
            ttl = int(ttl) if ttl is not None else None
            expires_at = message.get("expires_at")
            expires_at = float(expires_at) if expires_at is not None else None
            created_at = float(message.get("created_at", now_ts))
        except (TypeError, ValueError):
            conn.close()
            self._send(400, {"ok": False, "error": "invalid numeric message field"})
            return
        try:
            with bus.tx(conn):
                if thread_id:
                    bus.ensure_thread(conn, thread_id, from_agent, [from_agent, to_agent])
                conn.execute(
                    """
                    INSERT INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,message_type,urgency,
                        payload,attachments,ttl_seconds,expires_at,expects_response,response_deadline,nudge_count,
                        correlation_id,retry_count,status,created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        message_id,
                        thread_id,
                        idem,
                        from_agent,
                        to_agent,
                        mtype,
                        message.get("urgency", "normal"),
                        bus.json_dumps(message.get("payload", {})) if not isinstance(message.get("payload"), str) else message.get("payload"),
                        bus.json_dumps(message.get("attachments", [])) if not isinstance(message.get("attachments"), str) else message.get("attachments"),
                        ttl,
                        expires_at,
                        expects,
                        response_deadline,
                        0,
                        correlation_id,
                        0,
                        "queued",
                        created_at,
                    ),
                )
        except Exception as exc:
            # Detail goes to the daemon's stderr, not to the remote caller:
            # the old response echoed SQLite schema/column internals.
            print(f"handoff insert failed peer={peer['peer_name']}: {exc}", file=sys.stderr, flush=True)
            conn.close()
            self._send(500, {"ok": False, "error": "insert failed"})
            return
        conn.close()
        self._send(202, {"ok": True, "accepted": "inserted", "receipt_id": message_id})

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
    DaemonHandler.machine_id_value = bus.machine_id()
    if args.machine_id:
        os.environ["AGENT_BUS_MACHINE"] = args.machine_id
        DaemonHandler.machine_id_value = bus.machine_id()

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


if __name__ == "__main__":
    raise SystemExit(main())