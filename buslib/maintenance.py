"""Expiry sweeps, ask nudges, prune, and dead-letter controls."""

from __future__ import annotations

import argparse
import sqlite3
import uuid
from typing import Any

from .agents import sweep_agents
from .config import NUDGE_STEP_SECONDS, audit_log, json_dumps, now, row_to_dict
from .db import connect, require_operator_auth, tx

def sweep_expired(conn: sqlite3.Connection) -> tuple[int, int]:
    ts = now()
    cur = conn.execute(
        "UPDATE messages SET status='expired' WHERE status IN ('queued','delivered') AND expires_at IS NOT NULL AND expires_at <= ?",
        (ts,),
    )
    expired = cur.rowcount
    cur = conn.execute(
        "UPDATE messages SET status='dead' WHERE status IN ('queued','delivered') AND retry_count >= 5 AND (expires_at IS NULL OR expires_at > ?)",
        (ts,),
    )
    dead = cur.rowcount
    conn.commit()
    return expired, dead


def cmd_prune(args: argparse.Namespace) -> int:
    """Retention tool. State otherwise grows forever: messages, delivery
    receipts, outbox history, and audit/log files. Group messages are kept
    because deleting seqs creates holes the cursors cannot distinguish from
    unread traffic."""
    conn = connect()
    operator = require_operator_auth(conn, getattr(args, "agent_id", None))
    cutoff = now() - args.days * 86400
    messages_n = conn.execute(
        "SELECT COUNT(*) AS c FROM messages WHERE status IN ('acked','expired','dead','stale') AND created_at < ?"
        " AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.message_id=messages.message_id AND o.status IN ('pending','failed','sending'))",
        (cutoff,),
    ).fetchone()["c"]
    outbox_n = conn.execute(
        "SELECT COUNT(*) AS c FROM outbox WHERE status IN ('sent','dead') AND updated_at < ?", (cutoff,)
    ).fetchone()["c"]
    orphan_deliveries = conn.execute(
        "SELECT COUNT(*) AS c FROM deliveries d WHERE NOT EXISTS (SELECT 1 FROM messages m WHERE m.message_id=d.message_id)"
    ).fetchone()["c"]
    old_deliveries = conn.execute(
        "SELECT COUNT(*) AS c FROM deliveries d WHERE d.delivered_at < ?"
        " AND EXISTS (SELECT 1 FROM messages m WHERE m.message_id=d.message_id AND m.status='acked')",
        (cutoff,),
    ).fetchone()["c"]
    if not args.dry_run:
        with tx(conn):
            conn.execute(
                "DELETE FROM messages WHERE status IN ('acked','expired','dead','stale') AND created_at < ?"
                " AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.message_id=messages.message_id AND o.status IN ('pending','failed','sending'))",
                (cutoff,),
            )
            conn.execute(
                "DELETE FROM deliveries WHERE NOT EXISTS (SELECT 1 FROM messages m WHERE m.message_id=deliveries.message_id)"
            )
            conn.execute(
                "DELETE FROM deliveries WHERE delivered_at < ?"
                " AND EXISTS (SELECT 1 FROM messages m WHERE m.message_id=deliveries.message_id AND m.status='acked')",
                (cutoff,),
            )
            conn.execute("DELETE FROM outbox WHERE status IN ('sent','dead') AND updated_at < ?", (cutoff,))
        conn.execute("PRAGMA optimize")
        audit_log("prune", agent_id=operator, days=args.days, messages=messages_n, outbox=outbox_n,
                  deliveries=orphan_deliveries + old_deliveries)
    print(json_dumps({
        "ok": True, "dry_run": bool(args.dry_run), "days": args.days,
        "messages": messages_n, "outbox": outbox_n, "orphan_deliveries": orphan_deliveries,
        "delivery_receipts": old_deliveries,
    }))
    conn.close()
    return 0


def cmd_sweep(_args: argparse.Namespace) -> int:
    conn = connect()
    stale_agents = sweep_agents(conn)
    nudged, stale = sweep_nudges(conn)
    expired, dead = sweep_expired(conn)
    print(json_dumps({"ok": True, "expired": expired, "dead": dead, "stale_agents": stale_agents, "nudged": nudged, "stale_asks": stale}))
    conn.close()
    return 0


def cmd_dlq_list(args: argparse.Namespace) -> int:
    conn = connect()
    # dlq list prints every agent's dead/expired payloads; retry and discard
    # mutate arbitrary message rows. All three now need a proven identity.
    require_operator_auth(conn, getattr(args, "agent_id", None))
    status_clause = "status IN ('dead','expired')"
    params: list[Any] = []
    if args.status:
        status_clause = "status=?"
        params.append(args.status)
    params_with_limit = params + [args.limit]
    rows = conn.execute(
        f"SELECT * FROM messages WHERE {status_clause} ORDER BY created_at DESC LIMIT ?",
        params_with_limit,
    ).fetchall()
    print(json_dumps({"ok": True, "count": len(rows), "messages": [row_to_dict(r) for r in rows]}))
    conn.close()
    return 0


def cmd_dlq_retry(args: argparse.Namespace) -> int:
    conn = connect()
    require_operator_auth(conn, getattr(args, "agent_id", None))
    row = conn.execute("SELECT status,expires_at,ttl_seconds FROM messages WHERE message_id=?", (args.message_id,)).fetchone()
    if row is None:
        conn.close()
        raise SystemExit(f"error: message {args.message_id} not found")
    if row["status"] not in {"dead", "expired"}:
        conn.close()
        raise SystemExit(f"error: message is {row['status']}, only dead/expired can retry")
    # Requeueing an expired message used to leave expires_at in the past, so
    # the next sweep_expired re-expired it immediately: the retry was a no-op.
    # A message that had no TTL stays TTL-free; one that did gets its ttl back.
    new_expires = None
    if row["expires_at"] is not None:
        ttl_back = row["ttl_seconds"] if row["ttl_seconds"] and row["ttl_seconds"] > 0 else 86400
        new_expires = now() + ttl_back
    conn.execute(
        "UPDATE messages SET status='queued', retry_count=0, last_error=NULL, expires_at=? WHERE message_id=?",
        (new_expires, args.message_id),
    )
    print(json_dumps({"ok": True, "message_id": args.message_id, "status": "queued", "expires_at": new_expires}))
    conn.close()
    return 0


def cmd_dlq_discard(args: argparse.Namespace) -> int:
    conn = connect()
    require_operator_auth(conn, getattr(args, "agent_id", None))
    row = conn.execute("SELECT status FROM messages WHERE message_id=?", (args.message_id,)).fetchone()
    if row is None:
        conn.close()
        raise SystemExit(f"error: message {args.message_id} not found")
    if row["status"] not in {"dead", "expired"}:
        conn.close()
        raise SystemExit(f"error: message is {row['status']}; ack or cancel it instead")
    conn.execute("DELETE FROM messages WHERE message_id=?", (args.message_id,))
    audit_log("dlq_discard", agent_id=getattr(args, "agent_id", None), message_id=args.message_id)
    print(json_dumps({"ok": True, "message_id": args.message_id, "discarded": True}))
    conn.close()
    return 0


def sweep_nudges(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (nudges, stale) counts for overdue asks without a linked answer.

    The whole read-then-insert runs in one transaction: every notifier used to
    run this sweep on every poll tick in autocommit, so two notifiers could
    both see nudge_count=0 and both insert a reminder. BEGIN IMMEDIATE makes
    the second sweep see the first one's writes."""
    ts = now()
    # Cheap read before taking the write lock: most polls have no due asks.
    due = conn.execute(
        """
        SELECT 1 FROM messages
        WHERE message_type='ask' AND expects_response=1
          AND status IN ('queued','delivered','acked')
          AND response_deadline IS NOT NULL AND response_deadline <= ?
          AND (expires_at IS NULL OR expires_at > ?)
        LIMIT 1
        """,
        (ts, ts),
    ).fetchone()
    if due is None:
        return 0, 0
    nudges = 0
    stale = 0
    with tx(conn):
        asks = conn.execute(
        """
        SELECT * FROM messages
        WHERE message_type='ask' AND expects_response=1
          AND status IN ('queued','delivered','acked')
          AND response_deadline IS NOT NULL
          AND response_deadline <= ?
          AND (expires_at IS NULL OR expires_at > ?)
        """,
        (ts, ts),
        ).fetchall()
        for ask in asks:
            ask_corr = ask["correlation_id"] or ask["message_id"]
            has_answer = conn.execute(
                "SELECT 1 FROM messages WHERE message_type='answer' AND "
                "(answer_to_message_id=? OR (correlation_id IS NOT NULL AND correlation_id=?)) LIMIT 1",
                (ask["message_id"], ask_corr),
            ).fetchone() is not None
            if has_answer:
                conn.execute("UPDATE messages SET expects_response=0 WHERE message_id=?", (ask["message_id"],))
                continue
            ncount = int(ask["nudge_count"] or 0)
            if ncount < 2:
                reminder_text = (
                    f"Reminder: unanswered ask {ask['message_id']} in thread {ask['thread_id']} "
                    f"is still pending. If handled elsewhere, send a pointer answer with "
                    f"bus answer --thread-id {ask['thread_id']} --from {ask['to_agent']} --text '...'. "
                    f"Otherwise reply now."
                )
                # system message to recipient
                system_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,message_type,urgency,
                        payload,attachments,ttl_seconds,expires_at,expects_response,response_deadline,nudge_count,status,created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        system_id,
                        ask["thread_id"],
                        None,
                        "system",
                        ask["to_agent"],
                        "system",
                        "normal",
                        json_dumps({"text": reminder_text}),
                        "[]",
                        86400,
                        ts + 86400,
                        0,
                        None,
                        0,
                        "queued",
                        ts,
                    ),
                )
                await_nudge_backoff = NUDGE_STEP_SECONDS
                conn.execute(
                    "UPDATE messages SET nudge_count=?, response_deadline=? WHERE message_id=?",
                    (ncount + 1, ts + await_nudge_backoff, ask["message_id"]),
                )
                nudges += 1
            else:
                notice_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,message_type,urgency,
                        payload,attachments,ttl_seconds,expires_at,expects_response,response_deadline,nudge_count,status,created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        notice_id,
                        ask["thread_id"],
                        None,
                        "system",
                        ask["from_agent"],
                        "system",
                        "normal",
                        json_dumps({"text": f"Ask {ask['message_id']} to {ask['to_agent']} is stale: no linked answer within nudge window."}),
                        "[]",
                        86400,
                        ts + 86400,
                        0,
                        None,
                        0,
                        "queued",
                        ts,
                    ),
                )
                conn.execute(
                    "UPDATE messages SET status='stale', expects_response=0, response_deadline=NULL, last_error=? WHERE message_id=?",
                    ("unanswered after nudges", ask["message_id"]),
                )
            stale += 1
    return nudges, stale
