"""Peer configuration, outbox handoff, and bus status."""

from __future__ import annotations

import argparse
import json
import sqlite3
import urllib.request
from typing import Any

from .config import audit_log, home_info, json_dumps, machine_id, now, row_to_dict
from .db import connect, db_path, require_operator_auth, tx
from .validation import validate_id


def mask_peer(row: dict[str, Any]) -> dict[str, Any]:
    # peers.auth_token is stored plaintext; it must never be echoed by
    # `peer list` or `status`, and --token on argv already lands in ps.
    if "auth_token" in row:
        row["auth_token"] = "***" if row["auth_token"] else None
    return row


def cmd_peer_add(args: argparse.Namespace) -> int:
    validate_id("peer_name", args.peer)
    conn = connect()
    require_operator_auth(conn, getattr(args, "agent_id", None))
    try:
        conn.execute(
            """
            INSERT INTO peers(peer_name,host,base_url,auth_token,status,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(peer_name) DO UPDATE SET
                host=excluded.host,
                base_url=excluded.base_url,
                auth_token=excluded.auth_token,
                status='configured',
                updated_at=excluded.updated_at
            """,
            (
                args.peer,
                args.host,
                args.base_url.rstrip("/"),
                args.token,
                "configured",
                now(),
                now(),
            ),
        )
        conn.commit()
    except Exception as exc:
        conn.close()
        raise SystemExit(f"error: peer add failed: {exc}")
    print(json_dumps({"ok": True, "peer": args.peer, "base_url": args.base_url.rstrip("/")}))

    conn.close()
    return 0


def cmd_peer_list(args: argparse.Namespace) -> int:
    conn = connect()
    require_operator_auth(conn, getattr(args, "agent_id", None))
    rows = conn.execute("SELECT * FROM peers ORDER BY peer_name").fetchall()
    print(json_dumps({"ok": True, "peers": [mask_peer(row_to_dict(r)) for r in rows]}))
    conn.close()
    return 0


def cmd_peer_remove(args: argparse.Namespace) -> int:
    validate_id("peer_name", args.peer)
    conn = connect()
    operator = require_operator_auth(conn, getattr(args, "agent_id", None))
    if conn.execute("SELECT 1 FROM peers WHERE peer_name=?", (args.peer,)).fetchone() is None:
        conn.close()
        raise SystemExit(f"error: unknown peer {args.peer!r}")
    pending = conn.execute("SELECT COUNT(*) AS c FROM outbox WHERE peer_name=?", (args.peer,)).fetchone()["c"]
    if pending and not getattr(args, "force", False):
        conn.close()
        raise SystemExit(
            f"error: peer {args.peer!r} still has {pending} outbox row(s); "
            "flush/discard them first or pass --force to remove them with the peer"
        )
    try:
        with tx(conn):
            if pending:
                conn.execute("DELETE FROM outbox WHERE peer_name=?", (args.peer,))
            conn.execute("DELETE FROM peers WHERE peer_name=?", (args.peer,))
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise SystemExit(f"error: peer remove blocked by references: {exc}")
    audit_log("peer_remove", agent_id=operator, peer_name=args.peer, removed_outbox=pending)
    print(json_dumps({"ok": True, "removed": args.peer, "removed_outbox": pending}))
    conn.close()
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    conn = connect()
    agents = conn.execute("SELECT status,COUNT(*) c FROM agents GROUP BY status").fetchall()
    outbox = conn.execute("SELECT status,COUNT(*) c FROM outbox GROUP BY status").fetchall()
    dlq = conn.execute("SELECT COUNT(*) c FROM messages WHERE status IN ('dead','expired')").fetchone()
    peers = conn.execute("SELECT * FROM peers ORDER BY peer_name").fetchall()
    print(
        json_dumps(
            {
                "ok": True,
                "machine_id": machine_id(),
                "db": str(db_path()),
                "home": home_info(),
                "agents": {r["status"]: r["c"] for r in agents},
                "inactive_peers": [r["peer_name"] for r in peers if r["status"] not in {"ok", "configured"}],
                "outbox": {r["status"]: r["c"] for r in outbox},
                "dlq_depth": dlq["c"] if dlq else 0,
                "peers": [mask_peer(row_to_dict(r)) for r in peers],
            }
        )
    )
    conn.close()
    return 0


def cmd_outbox_list(args: argparse.Namespace) -> int:
    conn = connect()
    require_operator_auth(conn, getattr(args, "agent_id", None))
    rows = conn.execute(
        "SELECT * FROM outbox ORDER BY created_at DESC LIMIT ?", (args.limit,)
    ).fetchall()
    print(json_dumps({"ok": True, "count": len(rows), "outbox": [row_to_dict(r) for r in rows]}))
    conn.close()
    return 0


def _handoff_peer(peer, message_row: dict[str, Any], timeout: int = 10) -> tuple[bool, str | None]:
    url = peer["base_url"].rstrip("/") + "/bus/handoff"
    envelope = {
        "schema_version": 1,
        "from_machine": machine_id(),
        "to_machine": peer["peer_name"],
        "message": message_row,
    }
    data = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + peer["auth_token"]},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return False, str(exc)
    if not payload.get("ok"):
        return False, payload.get("error", "handoff rejected")
    return True, payload.get("receipt_id") or payload.get("receipt")


def sweep_outbox(conn: sqlite3.Connection, limit: int = 3, timeout: int = 3) -> tuple[int, int]:
    """Claim and retry a few outbox rows. Called by the notifier so
    store-and-forward does not depend on an operator remembering
    `agent-bus outbox flush`. Claims are a status transition under a write lock, so
    two notifiers cannot send the same row; a crashed claim is reclaimed after
    a lease. Remote dedupe makes the retry itself idempotent."""
    now_ts = now()
    claimed: list[sqlite3.Row] = []
    with tx(conn):
        conn.execute(
            "UPDATE outbox SET status='failed', updated_at=? WHERE status='sending' AND updated_at < ?",
            (now_ts, now_ts - 120),
        )
        candidates = conn.execute(
            """
            SELECT o.*, p.base_url, p.auth_token, p.peer_name AS pname FROM outbox o
            JOIN peers p ON o.peer_name=p.peer_name
            WHERE o.status IN ('pending','failed') AND o.attempts < o.max_attempts
            ORDER BY o.created_at LIMIT ?
            """,
            (limit,),
        ).fetchall()
        for row in candidates:
            cur = conn.execute(
                "UPDATE outbox SET status='sending', updated_at=? WHERE outbox_id=? AND status IN ('pending','failed')",
                (now_ts, row["outbox_id"]),
            )
            if cur.rowcount:
                claimed.append(row)
    sent = 0
    failed = 0
    for row in claimed:
        peer = {"peer_name": row["pname"], "base_url": row["base_url"], "auth_token": row["auth_token"]}
        try:
            message_row = json.loads(row["payload"])
            ok, receipt = _handoff_peer(peer, message_row, timeout=timeout)
        except Exception as exc:
            ok, receipt = False, f"{type(exc).__name__}: {exc}"
        if ok:
            conn.execute(
                "UPDATE outbox SET status='sent', attempts=attempts+1, receipt=?, last_error=NULL, updated_at=?, delivered_at=? WHERE outbox_id=?",
                (receipt, now(), now(), row["outbox_id"]),
            )
            sent += 1
        else:
            new_attempts = int(row["attempts"]) + 1
            status = "dead" if new_attempts >= int(row["max_attempts"]) else "failed"
            conn.execute(
                "UPDATE outbox SET status=?, attempts=?, last_error=?, updated_at=? WHERE outbox_id=?",
                (status, new_attempts, str(receipt)[:500], now(), row["outbox_id"]),
            )
            failed += 1
        conn.commit()
    return sent, failed


def cmd_outbox_flush(args: argparse.Namespace) -> int:
    conn = connect()
    require_operator_auth(conn, getattr(args, "agent_id", None))
    # outbox.max_attempts was declared and never read: a permanently dead peer
    # was retried forever, one 10s HTTP timeout at a time, with no LIMIT on the
    # backlog either. Rows past their attempt budget are given up on.
    rows = conn.execute(
        """
        SELECT o.*, p.base_url, p.auth_token, p.peer_name AS pname FROM outbox o
        JOIN peers p ON o.peer_name=p.peer_name
        WHERE o.status IN ('pending','failed') AND o.attempts < o.max_attempts
        ORDER BY o.created_at LIMIT 100
        """
    ).fetchall()
    given_up = conn.execute(
        "UPDATE outbox SET status='dead', updated_at=? WHERE status IN ('pending','failed') AND attempts>=max_attempts",
        (now(),),
    ).rowcount
    flushed = 0
    failed = 0
    for row in rows:
        peer = {
            "peer_name": row["pname"],
            "base_url": row["base_url"],
            "auth_token": row["auth_token"],
        }
        message_row = json.loads(row["payload"])
        ok, receipt = _handoff_peer(peer, message_row, timeout=args.timeout if args.timeout is not None else 10)
        if ok:
            conn.execute(
                "UPDATE outbox SET status='sent', attempts=attempts+1, receipt=?, updated_at=?, delivered_at=? WHERE outbox_id=?",
                (receipt, now(), now(), row["outbox_id"]),
            )
            flushed += 1
        else:
            conn.execute(
                "UPDATE outbox SET status='failed', attempts=attempts+1, last_error=?, updated_at=? WHERE outbox_id=?",
                (str(receipt)[:500], now(), row["outbox_id"]),
            )
            failed += 1
    print(json_dumps({"ok": True, "flushed": flushed, "failed": failed, "given_up": given_up}))
    conn.close()
    return 0 if failed == 0 else 1


__all__ = [
    "cmd_outbox_flush",
    "cmd_outbox_list",
    "cmd_peer_add",
    "cmd_peer_list",
    "cmd_peer_remove",
    "cmd_status",
    "mask_peer",
    "sweep_outbox",
]
