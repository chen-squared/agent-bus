#!/usr/bin/env python3
"""Process-level integration probe.

Starts the real daemon and notifier subprocesses, then verifies:
  - authenticated handoff -> notifier pointer injection -> ack
  - daemon blob GET / Range / HEAD / auth / traversal rejection
  - pointer-only hook does not execute message text
  - notifier background outbox flush sends a pending handoff
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

root = tempfile.mkdtemp(prefix="bus-process-")
DB = os.path.join(root, "world", "bus.db")
TOK = os.path.join(root, "world", "tokens")
BLOB = os.path.join(root, "world", "blobs")
AUDIT = os.path.join(root, "world", "audit.log")
INBOX = os.path.join(root, "inbox.txt")
CANARY = os.path.join(root, "pwned")
NOTIFIER_LOG = os.path.join(root, "notifier.log")
os.environ.update({
    "AGENT_BUS_DB": DB,
    "AGENT_BUS_TOKEN_DIR": TOK,
    "AGENT_BUS_AUDIT_LOG": AUDIT,
    "AGENT_BUS_BLOB_DIR": BLOB,
})
HERE = os.path.dirname(os.path.abspath(__file__))
FAIL: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


def bus_cli(*argv: str, token: str | None = None):
    env = dict(os.environ)
    if token:
        env["AGENT_BUS_TOKEN_FILE"] = token
    return subprocess.run(
        [sys.executable, os.path.join(HERE, "bus.py"), *argv],
        capture_output=True, text=True, env=env,
    )


def tokfile(agent: str) -> str:
    return os.path.join(TOK, f"{agent}.token")


def wait(predicate, timeout: float = 12.0, interval: float = 0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


def post_handoff(port: int, message: dict) -> tuple[int, dict]:
    body = json.dumps({"schema_version": 1, "message": message}).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/bus/handoff", data=body, method="POST",
        headers={"Content-Type": "application/json", "Authorization": "Bearer secret"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def http_get(port: int, path: str, token: str | None = "secret", method: str = "GET", headers: dict | None = None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, response.read(), dict(response.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)


class FlushServer(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": True, "receipt_id": "process-receipt"}).encode())

    def log_message(self, *args):
        pass


def main() -> int:
    for agent in ("alice", "bob"):
        rc = bus_cli("register", "--agent-id", agent, "--agent-type", "command")
        if rc.returncode != 0:
            check(f"register {agent}", False, rc.stderr[:200])
            return 1
    check("alice/bob registered", True)

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    daemon_port = s.getsockname()[1]
    s.close()
    rc = bus_cli("peer", "add", "--peer", "machine-b", "--base-url", f"http://127.0.0.1:{daemon_port}",
                 "--token", "secret", "--agent-id", "alice", token=tokfile("alice"))
    check("peer added", rc.returncode == 0, rc.stderr[:200])

    daemon = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "bus_daemon.py"), "--port", str(daemon_port), "--bind", "127.0.0.1"],
        env=dict(os.environ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    notifier = None
    flush_server = None
    try:
        ok = wait(lambda: _health(daemon_port), timeout=10)
        check("daemon health", bool(ok), str(ok))

        # Keep the hook pointer-only: text must never be interpolated.
        notify = f"printf '%s %s %s\\n' '{{message_id}}' '{{thread_id}}' '{{from_agent}}' >> {INBOX}"
        log_fh = open(NOTIFIER_LOG, "w")
        notifier = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "bus_agent.py"), "--agent-id", "alice",
             "--adapter", "command", "--session-id", "alice", "--notify-cmd", notify,
             "--poll", "0.2", "--heartbeat", "1", "--sweep", "1", "--outbox-flush", "0.2"],
            env=dict(os.environ), stdout=log_fh, stderr=subprocess.STDOUT,
        )
        check("notifier started", notifier.poll() is None)

        canary_text = f"$(touch {CANARY})"
        code, payload = post_handoff(daemon_port, {
            "from_agent": "bob", "to_agent": "alice", "thread_id": "process-thread",
            "message_type": "text", "payload": {"text": canary_text},
            "idempotency_key": "process-1",
        })
        check("handoff accepted", code == 202, str(payload)[:160])

        def inbox_line():
            try:
                return Path(INBOX).read_text() if Path(INBOX).exists() else ""
            except OSError:
                return ""

        line = wait(lambda: inbox_line() if "process-thread" in inbox_line() else None)
        check("notifier wrote the pointer", bool(line), (line or NOTIFIER_LOG)[:200])
        check("pointer carries namespaced sender", "bob@machine-b" in (line or ""), (line or "")[:200])
        check("message text was not executed", not Path(CANARY).exists())

        def acked():
            conn = sqlite3.connect(DB)
            try:
                row = conn.execute(
                    "SELECT status FROM messages WHERE thread_id='process-thread'"
                ).fetchone()
                return row[0] if row else None
            finally:
                conn.close()

        status = wait(lambda: acked() == "acked" and "acked")
        check("message acked", status == "acked", str(status))

        # Blob endpoint: authenticated GET, Range, HEAD, bad auth, traversal.
        blob_file = Path(root) / "blob.txt"
        blob_file.write_text("0123456789")
        rc = bus_cli("blob", "put", "--path", str(blob_file))
        manifest = json.loads(rc.stdout) if rc.returncode == 0 else {}
        sha = manifest.get("sha256")
        check("blob stored", bool(sha), rc.stderr[:200])
        if sha:
            code, body, _ = http_get(daemon_port, f"/bus/blob/{sha}")
            check("blob GET", code == 200 and body == b"0123456789", f"{code} {body[:20]!r}")
            code, body, _ = http_get(daemon_port, f"/bus/blob/{sha}", headers={"Range": "bytes=0-3"})
            check("blob Range", code == 206 and body == b"0123", f"{code} {body!r}")
            code, body, _ = http_get(daemon_port, f"/bus/blob/{sha}", method="HEAD")
            check("blob HEAD", code == 200, str(code))
            code, body, _ = http_get(daemon_port, f"/bus/blob/{sha}", token=None)
            check("blob auth required", code == 403, str(code))
            code, body, _ = http_get(daemon_port, "/bus/blob/..%2f..%2fetc%2fpasswd")
            check("blob traversal rejected", code == 404, str(code))

        # Background outbox flush: notifier should claim and send this row.
        flush_server = HTTPServer(("127.0.0.1", 0), FlushServer)
        threading.Thread(target=flush_server.serve_forever, daemon=True).start()
        rc = bus_cli("peer", "add", "--peer", "flush-peer",
                     "--base-url", f"http://127.0.0.1:{flush_server.server_address[1]}",
                     "--token", "x", "--agent-id", "alice", token=tokfile("alice"))
        check("flush peer added", rc.returncode == 0, rc.stderr[:200])
        now_ts = time.time()
        outbox_id = str(uuid.uuid4())
        conn = sqlite3.connect(DB)
        conn.execute(
            "INSERT INTO outbox (outbox_id,peer_name,message_id,from_agent,to_remote,payload,status,attempts,max_attempts,created_at,updated_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (outbox_id, "flush-peer", str(uuid.uuid4()), "alice", "bob",
             json.dumps({"message_id": str(uuid.uuid4()), "from_agent": "alice", "to_agent": "bob",
                         "payload": json.dumps({"text": "flush"}), "attachments": "[]",
                         "created_at": now_ts}),
             "pending", 0, 5, now_ts, now_ts),
        )
        conn.commit()
        conn.close()

        def outbox_status():
            conn = sqlite3.connect(DB)
            try:
                row = conn.execute("SELECT status FROM outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
                return row[0] if row else None
            finally:
                conn.close()

        status = wait(lambda: outbox_status() == "sent" and "sent")
        check("notifier auto-flushed outbox", status == "sent", str(status))

        return 0 if not FAIL else 1
    finally:
        if notifier is not None and notifier.poll() is None:
            notifier.terminate()
            try:
                notifier.wait(timeout=5)
            except subprocess.TimeoutExpired:
                notifier.kill()
        if flush_server is not None:
            flush_server.shutdown()
        daemon.terminate()
        try:
            daemon.wait(timeout=5)
        except subprocess.TimeoutExpired:
            daemon.kill()
        Path(CANARY).unlink(missing_ok=True)
        if FAIL and os.path.exists(NOTIFIER_LOG):
            print("\n--- notifier log tail ---")
            print("\n".join(Path(NOTIFIER_LOG).read_text().splitlines()[-20:]))


def _health(port: int):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
            return json.loads(response.read())
    except Exception:
        return None


if __name__ == "__main__":
    rc = main()
    print()
    if FAIL:
        print(f"RESULT: {len(FAIL)} FAILED: {FAIL}")
    else:
        print("RESULT: ALL PASS")
    raise SystemExit(rc)
