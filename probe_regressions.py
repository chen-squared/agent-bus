#!/usr/bin/env python3
"""Regression probe for the review fixes.

Run from the repository root:

    python3 probe_regressions.py

Uses a throwaway bus world (AGENT_BUS_DB / AGENT_BUS_TOKEN_DIR /
AGENT_BUS_AUDIT_LOG / AGENT_BUS_BLOB_DIR) and never the production one.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import stat
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

root = tempfile.mkdtemp(prefix="bus-reg-")
DB = os.path.join(root, "world", "bus.db")
TOK = os.path.join(root, "world", "tokens")
BLOB = os.path.join(root, "world", "blobs")
AUDIT = os.path.join(root, "world", "audit.log")
os.environ["AGENT_BUS_DB"] = DB
os.environ["AGENT_BUS_TOKEN_DIR"] = TOK
os.environ["AGENT_BUS_AUDIT_LOG"] = AUDIT
os.environ["AGENT_BUS_BLOB_DIR"] = BLOB

HERE = os.path.dirname(os.path.abspath(__file__))
FAIL: list[str] = []
sys.path.insert(0, HERE)

import bus  # noqa: E402
import bus_agent  # noqa: E402


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


def cli(*argv: str, tok: str | None = None, env_extra: dict[str, str] | None = None):
    env = dict(os.environ)
    if tok:
        env["AGENT_BUS_TOKEN_FILE"] = tok
    if env_extra:
        env.update(env_extra)
    p = subprocess.run(
        [sys.executable, os.path.join(HERE, "bus.py"), *argv],
        capture_output=True, text=True, env=env,
    )
    return p.returncode, p.stdout, p.stderr


def jout(out: str) -> dict:
    try:
        return json.loads(out)
    except (json.JSONDecodeError, TypeError):
        return {}


def tokfile(agent: str) -> str:
    return os.path.join(TOK, f"{agent}.token")


def wait_row(conn: sqlite3.Connection, sql: str, params: tuple, tries: int = 50):
    for _ in range(tries):
        row = conn.execute(sql, params).fetchone()
        if row is not None:
            return row
        time.sleep(0.1)
    return None


print("A parser / id / sha helpers")
check("digest id with a digit-ending agent parses", bus.parse_group_digest_id("group-digest-g-5-bob2", "bob2") == ("g", 5))
check("digest id with a hyphenated agent parses", bus.parse_group_digest_id("group-digest-g-5-muse-spark", "muse-spark") == ("g", 5))
try:
    bus.blob_path("../../evil")
    check("blob_path rejects traversal", False)
except ValueError:
    check("blob_path rejects traversal", True)
check("idempotency scopes differ by sender",
      bus.scoped_idempotency_key("alice", "K") != bus.scoped_idempotency_key("bob", "K"))
try:
    bus_agent.CommandAdapter("printf '%s' '{text}'", "s")
    check("CommandAdapter blocks {text}", False)
except SystemExit:
    check("CommandAdapter blocks {text}", True)

print("B register/login + idempotency scoping")
rc, out, err = cli("register", "--agent-id", "alice", "--agent-type", "command")
check("alice registered", rc == 0, err or out[:80])
rc, out, err = cli("register", "--agent-id", "bob", "--agent-type", "command")
check("bob registered", rc == 0, err or out[:80])
rc, out, err = cli("inbox", "--agent-id", "alice", "--limit", "-1", tok=tokfile("alice"))
check("negative --limit is rejected", rc != 0 and ">= 0" in err, err[:120])
rc, out, err = cli("send", "--from", "alice", "--to", "bob", "--text", "a", "--idempotency-key", "SAME", tok=tokfile("alice"))
a_id = jout(out).get("message_id")
rc, out, err = cli("send", "--from", "bob", "--to", "alice", "--text", "b", "--idempotency-key", "SAME", tok=tokfile("bob"))
b_id = jout(out).get("message_id")
check("two senders can reuse one key without collision", bool(a_id and b_id and a_id != b_id), f"{a_id} / {b_id}")
rc, out, err = cli("send", "--from", "alice", "--to", "bob", "--text", "a2", "--idempotency-key", "SAME", tok=tokfile("alice"))
check("same sender duplicate is reported", rc == 0 and jout(out).get("duplicate") is True and jout(out).get("message_id") == a_id, err or out[:120])

print("C attachment shape is shared by send and ask")
att = os.path.join(root, "att.txt")
Path(att).write_text("attachment body")
rc, out, err = cli("send", "--from", "alice", "--to", "bob", "--text", "with att", "--attachment", att, tok=tokfile("alice"))
check("send with attachment", rc == 0, err or out[:80])
rc, out, err = cli("ask", "--from", "alice", "--to", "bob", "--text", "ask att", "--attachment", att, tok=tokfile("alice"))
check("ask with attachment", rc == 0, err or out[:80])
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
shapes = []
for row in conn.execute("SELECT message_type,attachments FROM messages WHERE message_type IN ('text','ask') ORDER BY rowid"):
    parsed = json.loads(row["attachments"])
    if parsed:
        shapes.append((row["message_type"], parsed[0]))
check("send and ask both store manifest dicts with path",
      len(shapes) >= 2 and all(isinstance(v, dict) and v.get("sha256") and v.get("path") for _, v in shapes),
      str(shapes)[:220])

print("D local and cross-machine ask --wait correlation")
def run_ask_wait(thread_id: str, to: str):
    return subprocess.Popen(
        [sys.executable, os.path.join(HERE, "bus.py"), "ask", "--from", "alice", "--to", to,
         "--thread-id", thread_id, "--text", "question", "--wait", "--timeout", "10"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=dict(os.environ),
    )

local = run_ask_wait("wait-local", "bob")
ask_row = wait_row(conn, "SELECT message_id FROM messages WHERE thread_id='wait-local' AND message_type='ask'", ())
cli("answer", "--thread-id", "wait-local", "--from", "bob", "--to", "alice", "--text", "local answer", tok=tokfile("bob"))
out, err = local.communicate(timeout=15)
check("local ask --wait sees the answer", local.returncode == 0 and "local answer" in out, err[-200:] or out[:160])

cli("peer", "add", "--peer", "remote", "--base-url", "http://127.0.0.1:9", "--token", "x", "--agent-id", "alice")
remote = run_ask_wait("wait-remote", "bob@remote")
ask_row = wait_row(conn, "SELECT message_id FROM messages WHERE thread_id='wait-remote' AND message_type='ask'", ())
if ask_row:
    now = time.time()
    conn.execute(
        """INSERT INTO messages (message_id,thread_id,from_agent,to_agent,message_type,urgency,payload,attachments,
           ttl_seconds,expires_at,status,correlation_id,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (str(uuid.uuid4()), "wait-remote", "bob@remote", "alice", "answer", "normal",
         json.dumps({"text": "remote answer"}), "[]", 300, now + 300, "queued", ask_row["message_id"], now),
    )
    conn.commit()
out, err = remote.communicate(timeout=15)
check("cross-machine ask --wait matches correlation id", remote.returncode == 0 and "remote answer" in out, err[-200:] or out[:160])

print("E peer remove / group leave / prune")
cli("peer", "add", "--peer", "dead", "--base-url", "http://127.0.0.1:9", "--token", "x", "--agent-id", "alice")
conn.execute(
    "INSERT INTO outbox (outbox_id,peer_name,message_id,from_agent,to_remote,payload,status,attempts,max_attempts,created_at,updated_at)"
    " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
    ("o-dead", "dead", "m-dead", "alice", "bob", "{}", "pending", 0, 5, time.time(), time.time()),
)
conn.commit()
rc, out, err = cli("peer", "remove", "--peer", "dead", "--agent-id", "alice", tok=tokfile("alice"))
check("peer remove refuses to orphan outbox rows", rc != 0 and "outbox" in err, err[:160])
rc, out, err = cli("peer", "remove", "--peer", "dead", "--agent-id", "alice", "--force", tok=tokfile("alice"))
check("peer remove --force removes the peer and rows", rc == 0 and "removed_outbox" in out, err[:160] or out[:120])
rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "leave-g", "--rate-seconds", "0", tok=tokfile("alice"))
cli("group", "join", "--group-id", "leave-g", "--agent-id", "bob", tok=tokfile("bob"))
rc, out, err = cli("group", "leave", "--group-id", "leave-g", "--agent-id", "bob", tok=tokfile("bob"))
check("group leave reports left", rc == 0 and jout(out).get("status") == "left", err or out[:120])
rc, out, err = cli("group", "leave", "--group-id", "leave-g", "--agent-id", "bob", tok=tokfile("bob"))
check("group leave is idempotent", rc == 0 and jout(out).get("status") == "already_left", err or out[:120])

mid = jout(cli("send", "--from", "alice", "--to", "bob", "--text", "prune me", tok=tokfile("alice"))[1]).get("message_id")
cli("ack", "--agent-id", "bob", "--message-id", mid, tok=tokfile("bob"))
rc, out, err = cli("prune", "--agent-id", "alice", "--days", "0", "--dry-run", tok=tokfile("alice"))
check("prune dry-run reports acked message", rc == 0 and jout(out).get("messages", 0) >= 1, err or out[:160])
rc, out, err = cli("prune", "--agent-id", "alice", "--days", "0", tok=tokfile("alice"))
check("prune deletes terminal rows", rc == 0 and jout(out).get("messages", 0) >= 1, err or out[:160])
audit_tail = Path(AUDIT).read_text().strip().splitlines()[-1]
check("prune audit logs operator id, not token", '"agent_id":"alice"' in audit_tail and "token" not in audit_tail, audit_tail[:160])

print("F daemon ingress validation and correlation")
canary = os.path.join(root, "daemon-pwn")
Path(canary).unlink(missing_ok=True)
s = socket.socket()
s.bind(("127.0.0.1", 0))
port = s.getsockname()[1]
s.close()
cli("peer", "add", "--peer", "machine-b", "--base-url", f"http://127.0.0.1:{port}", "--token", "secret", "--agent-id", "alice")
daemon = subprocess.Popen(
    [sys.executable, os.path.join(HERE, "bus_daemon.py"), "--port", str(port), "--bind", "127.0.0.1"],
    env=dict(os.environ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)
try:
    for _ in range(50):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1).read()
            break
        except Exception:
            time.sleep(0.1)

    def handoff(msg: dict) -> tuple[int, dict]:
        body = json.dumps({"schema_version": 1, "message": msg}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/bus/handoff", data=body, method="POST",
            headers={"Content-Type": "application/json", "Authorization": "Bearer secret"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    code, payload = handoff({"from_agent": f"x'; touch {canary}; echo '", "to_agent": "alice", "payload": {"text": "x"}})
    check("daemon rejects shell metacharacters in from_agent", code == 400 and not Path(canary).exists(), str(payload)[:120])
    code, payload = handoff({"from_agent": "bob", "to_agent": "alice", "payload": {"text": "x"},
                             "attachments": ["/etc/passwd"]})
    check("daemon rejects local-path attachments", code == 400, str(payload)[:120])
    code, payload = handoff({"from_agent": "bob", "to_agent": "alice", "thread_id": "t-remote", "message_type": "ask",
                             "payload": {"text": "remote ask"}, "correlation_id": "corr-1", "expects_response": 1,
                             "idempotency_key": "K"})
    check("daemon accepts a valid handoff", code == 202, str(payload)[:120])
    code, payload = handoff({"from_agent": "bob", "to_agent": "alice", "thread_id": "t-remote", "message_type": "ask",
                             "payload": {"text": "remote ask"}, "correlation_id": "corr-1", "expects_response": 1,
                             "idempotency_key": "K"})
    check("daemon dedupes a retried handoff", code == 200 and payload.get("accepted") == "duplicate", str(payload)[:120])
    row = conn.execute("SELECT from_agent,correlation_id,expects_response FROM messages WHERE thread_id='t-remote'").fetchone()
    check("daemon namespaces sender and preserves correlation",
          row is not None and row["from_agent"] == "bob@machine-b" and row["correlation_id"] == "corr-1" and row["expects_response"] == 1,
          str(dict(row)) if row else "missing")
finally:
    daemon.terminate()
    daemon.wait(timeout=5)
    Path(canary).unlink(missing_ok=True)

print("G gateway abbreviation bypass")
gw = os.path.join(HERE, "bus_ssh_gateway.py")
for gid in ("allowed", "evil"):
    cli("group", "create", "--from", "alice", "--group-id", gid, "--rate-seconds", "0", tok=tokfile("alice"))
    cli("group", "join", "--group-id", gid, "--agent-id", "bob", tok=tokfile("bob"))


def gw_run(cmd: str, groups: str | None = None):
    env = dict(os.environ)
    env["SSH_ORIGINAL_COMMAND"] = cmd
    env.pop("AGENT_BUS_ALLOWED_GROUPS", None)
    argv = [sys.executable, gw, "--identity", "bob", "--db", DB, "--bus", os.path.join(HERE, "bus.py")]
    if groups:
        argv += ["--groups", groups]
    p = subprocess.run(argv, capture_output=True, text=True, env=env)
    return p.returncode, p.stdout, p.stderr


rc, out, err = gw_run("group send --group-id allowed --group-i evil --text pwned", groups="allowed")
check("abbreviated duplicate group flag is rejected", rc != 0, err[:160] or out[:120])
evil_count = conn.execute("SELECT COUNT(*) FROM group_messages WHERE group_id='evil'").fetchone()[0]
check("nothing landed in the non-allowlisted group", evil_count == 0, str(evil_count))
rc, out, err = gw_run("group send --group-id allowed --text ok", groups="allowed")
check("exact allowlisted command still works", rc == 0 and jout(out).get("ok") is True, err[:160] or out[:120])

print("H permissions")
for path, mode in ((DB, 0o600), (AUDIT, 0o600), (TOK, 0o700), (BLOB, 0o700)):
    if os.path.exists(path):
        actual = stat.S_IMODE(os.stat(path).st_mode)
        check(f"{os.path.basename(path)} mode {oct(mode)}", actual == mode, oct(actual))
    else:
        check(f"{os.path.basename(path)} exists", False, path)

print("I outbox auto-flush claim")
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": True, "receipt_id": "receipt-1"}).encode())

    def log_message(self, *args):
        pass


server = HTTPServer(("127.0.0.1", 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
cli("peer", "add", "--peer", "flush-peer", "--base-url", f"http://127.0.0.1:{server.server_address[1]}",
    "--token", "x", "--agent-id", "alice")
payload = {"message_id": str(uuid.uuid4()), "from_agent": "alice", "to_agent": "bob",
           "thread_id": str(uuid.uuid4()), "payload": json.dumps({"text": "flush"}), "attachments": "[]",
           "created_at": time.time()}
conn.execute(
    "INSERT INTO outbox (outbox_id,peer_name,message_id,from_agent,to_remote,payload,status,attempts,max_attempts,created_at,updated_at)"
    " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
    ("o-flush", "flush-peer", payload["message_id"], "alice", "bob", json.dumps(payload), "pending", 0, 5,
     time.time(), time.time()),
)
conn.commit()
sent, failed = bus.sweep_outbox(conn)
status = conn.execute("SELECT status,receipt FROM outbox WHERE outbox_id='o-flush'").fetchone()
check("sweep_outbox sends a pending row", sent >= 1 and status["status"] == "sent", f"sent={sent} failed={failed} row={dict(status)}")
server.shutdown()

conn.close()
print()
if FAIL:
    print(f"RESULT: {len(FAIL)} FAILED: {FAIL}")
    sys.exit(1)
print("RESULT: ALL PASS")
