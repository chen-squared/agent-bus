"""Authenticated handoff ingestion for the localhost daemon."""

from __future__ import annotations

import json
import os
import sys
import uuid
from typing import Any

from ..config import NUDGE_FIRST_SECONDS, json_dumps, now
from ..db import connect, scoped_idempotency_key, tx
from ..messaging import ensure_thread
from ..validation import validate_id, validate_sha256

MAX_HANDOFF_BYTES = 16 * 1024 * 1024


def handle(handler: Any, peer) -> None:
    # Content-Length used to be int()'d unguarded (non-numeric -> uncaught
    # ValueError outside the try) and read without a cap (memory DoS).
    try:
        length = int(handler.headers.get("Content-Length", "0") or "0")
    except ValueError:
        handler._send(400, {"ok": False, "error": "invalid Content-Length"})
        return
    if length < 0 or length > MAX_HANDOFF_BYTES:
        handler._send(413, {"ok": False, "error": f"body too large (max {MAX_HANDOFF_BYTES} bytes)"})
        return
    try:
        envelope = json.loads(handler.rfile.read(length).decode("utf-8")) if length else {}
    except (UnicodeDecodeError, json.JSONDecodeError):
        handler._send(400, {"ok": False, "error": "invalid json"})
        return

    conn = connect()
    if envelope.get("schema_version", 1) != 1:
        conn.close()
        handler._send(400, {"ok": False, "error": "unsupported schema_version, upgrade peer"})
        return

    message = envelope.get("message")
    if not isinstance(message, dict):
        conn.close()
        handler._send(400, {"ok": False, "error": "missing message"})
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
        handler._send(400, {"ok": False, "error": "missing or invalid to_agent"})
        return
    if thread_id is not None and not isinstance(thread_id, str):
        conn.close()
        handler._send(400, {"ok": False, "error": "invalid thread_id"})
        return
    try:
        validate_id("agent_id", local_part)
        validate_id("agent_id", to_agent)
        validate_id("thread_id", thread_id if thread_id else None)
    except SystemExit as exc:
        conn.close()
        handler._send(400, {"ok": False, "error": str(exc)})
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
            handler._send(400, {"ok": False, "error": "invalid idempotency_key"})
            return
    idem = scoped_idempotency_key(peer["peer_name"], peer_key, namespace="remote") if peer_key else None
    if idem:
        existing = conn.execute("SELECT message_id FROM messages WHERE idempotency_key=?", (idem,)).fetchone()
        if existing is not None:
            conn.close()
            handler._send(200, {"ok": True, "accepted": "duplicate", "receipt_id": existing["message_id"]})
            return
    attachments = message.get("attachments")
    if isinstance(attachments, str):
        try:
            attachments = json.loads(attachments or "[]")
        except (json.JSONDecodeError, TypeError):
            attachments = []
    if attachments is not None and not isinstance(attachments, list):
        conn.close()
        handler._send(400, {"ok": False, "error": "attachments must be a list"})
        return
    if isinstance(attachments, list):
        for item in attachments:
            if isinstance(item, str):
                # Local path strings are meaningful only inside the sending
                # machine; accepting one lets a peer plant an arbitrary path
                # in the recipient's attachment list.
                conn.close()
                handler._send(400, {"ok": False, "error": "remote attachments must be blob manifests, not local paths"})
                return
            if isinstance(item, dict):
                item.pop("path", None)
                if "sha256" not in item:
                    conn.close()
                    handler._send(400, {"ok": False, "error": "remote attachment missing sha256"})
                    return
                try:
                    validate_sha256(item["sha256"])
                except ValueError as exc:
                    conn.close()
                    handler._send(400, {"ok": False, "error": str(exc)})
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
            handler._send(400, {"ok": False, "error": "invalid correlation_id"})
            return
    mtype = message.get("message_type", "text")
    expects = 1 if message.get("expects_response") or mtype == "ask" else 0
    now_ts = now()
    try:
        response_deadline = message.get("response_deadline")
        if response_deadline is None and expects:
            response_deadline = now_ts + NUDGE_FIRST_SECONDS
        response_deadline = float(response_deadline) if response_deadline is not None else None
        ttl = message.get("ttl_seconds")
        ttl = int(ttl) if ttl is not None else None
        expires_at = message.get("expires_at")
        expires_at = float(expires_at) if expires_at is not None else None
        created_at = float(message.get("created_at", now_ts))
    except (TypeError, ValueError):
        conn.close()
        handler._send(400, {"ok": False, "error": "invalid numeric message field"})
        return
    try:
        with tx(conn):
            if thread_id:
                ensure_thread(conn, thread_id, from_agent, [from_agent, to_agent])
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
                    json_dumps(message.get("payload", {})) if not isinstance(message.get("payload"), str) else message.get("payload"),
                    json_dumps(message.get("attachments", [])) if not isinstance(message.get("attachments"), str) else message.get("attachments"),
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
        handler._send(500, {"ok": False, "error": "insert failed"})
        return
    conn.close()
    handler._send(202, {"ok": True, "accepted": "inserted", "receipt_id": message_id})


__all__ = ["MAX_HANDOFF_BYTES", "handle"]
