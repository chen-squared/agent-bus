"""1:1 threads, sends, asks, answers, inbox, and acknowledgements."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import uuid
from typing import Any

from .agents import is_stale
from .blobs import prepare_attachments
from .config import NUDGE_FIRST_SECONDS, audit_log, json_dumps, now, row_to_dict
from .db import (
    connect, find_duplicate_message, require_agent, require_auth,
    scoped_idempotency_key, tx,
)
from .groups import _group_require_member
from .maintenance import sweep_expired, sweep_nudges
from .remote import _handoff_peer
from .validation import validate_id

def ensure_thread(conn: sqlite3.Connection, thread_id: str, created_by: str | None, participants: list[str]) -> None:
    # INSERT OR IGNORE: the old check-then-insert raced two processes that
    # both started the same fresh thread_id, and the loser died with an
    # uncaught IntegrityError traceback.
    conn.execute(
        "INSERT OR IGNORE INTO threads(thread_id,participants,created_by,created_at,status) VALUES (?,?,?,?,?)",
        (thread_id, json_dumps(participants), created_by, now(), "open"),
    )
    existing = conn.execute("SELECT participants FROM threads WHERE thread_id=?", (thread_id,)).fetchone()
    if participants and existing is not None:
        old = set(json.loads(existing["participants"] or "[]"))
        merged = sorted(old | set(participants))
        if merged != old:
            conn.execute("UPDATE threads SET participants=? WHERE thread_id=?", (json_dumps(merged), thread_id))


def parse_group_digest_id(message_id: str, agent_id: str) -> tuple[str, int | None]:
    """Parse `group-digest-<group>-<seq>-<agent>` without the greedy-regex
    ambiguity that broke agent ids ending in digits (e.g. `-q38`). The recipient
    id is known, so strip it first, then split the sequence off the end."""
    syn = message_id[len("group-digest-"):]
    suffix = f"-{agent_id}"
    base = syn[: -len(suffix)] if agent_id and syn.endswith(suffix) else syn
    group_id, sep, seq = base.rpartition("-")
    if sep and seq.isdigit():
        return group_id, int(seq)
    return base, None


def cmd_message_show(args: argparse.Namespace) -> int:
    conn = connect()
    require_auth(conn, args.agent_id)
    row = conn.execute("SELECT * FROM messages WHERE message_id=?", (args.message_id,)).fetchone()
    if row is None or ((args.message_id or "").startswith("group-digest-") and args.agent_id not in {row["from_agent"], row["to_agent"]}):

        # Synthetic digest ids are shared across recipients, so a persisted
        # row can only be owned by one agent. Fall back to reconstructing the
        # digest from the group itself for any active member.
        group_id, last_seq = parse_group_digest_id(args.message_id, args.agent_id)
        group = conn.execute("SELECT topic FROM groups WHERE group_id=?", (group_id,)).fetchone()
        if group is None:
            # Older ids may omit the seq; try the whole suffix as a group id.
            group_id, last_seq = args.message_id[len("group-digest-"):], None
            group = conn.execute("SELECT topic FROM groups WHERE group_id=?", (group_id,)).fetchone()
        if group is None:
            conn.close()
            raise SystemExit(f"error: unknown group {group_id!r}")
        _group_require_member(conn, group_id, args.agent_id)
        if last_seq is None:
            rows = conn.execute("SELECT seq,sender_agent,payload FROM group_messages WHERE group_id=? ORDER BY seq", (group_id,)).fetchall()
        else:
            rows = conn.execute("SELECT seq,sender_agent,payload FROM group_messages WHERE group_id=? AND seq<=? ORDER BY seq", (group_id, last_seq)).fetchall()
        total = len(rows)
        window = rows[-5:] if total > 5 else rows
        omitted = total - len(window)
        lines = [f"[group:{group_id} topic:{group['topic']}] reconstructed-digest={len(window)} of {total}"]
        for r in window:
            try:
                text = json.loads(r["payload"]).get("text", "")
            except (json.JSONDecodeError, TypeError):
                text = str(r["payload"])
            text = text.replace("\\n", " ").replace("\n", " ")[:240]
            lines.append(f"  seq {r['seq']} from {r['sender_agent']}: {text}")
        if omitted:
            lines.append(f"  ... +{omitted} more (full: bus group messages --group-id {group_id} --agent-id {args.agent_id})")
        digest = "\n".join(lines)
        conn.close()
        print(json_dumps({"ok": True, "agent_id": args.agent_id, "message": {"message_id": args.message_id, "from_agent": group_id, "to_agent": args.agent_id, "thread_id": group_id, "payload": json.dumps({"text": digest}, ensure_ascii=False), "status": "delivered"}}))
        return 0
    if row is None:
        conn.close()
        raise SystemExit(f"error: message {args.message_id} not found")
    from_agent = row["from_agent"]
    to_agent = row["to_agent"]
    if args.agent_id not in {from_agent, to_agent}:
        audit_log("message_read_denied", requester=args.agent_id, target_message=args.message_id, parties=f"{from_agent}->{to_agent}")
        conn.close()
        raise SystemExit(f"error: denied: {args.agent_id} is not a participant of {args.message_id}")
    audit_log("message_read_allowed", requester=args.agent_id, message_id=args.message_id, allowed=f"{from_agent}->{to_agent}")
    print(json_dumps({"ok": True, "agent_id": args.agent_id, "message": row_to_dict(row)}))
    conn.close()
    return 0


def cmd_thread(args: argparse.Namespace) -> int:
    conn = connect()
    require_auth(conn, args.agent_id)
    if not conn.execute("SELECT 1 FROM agents WHERE agent_id=?", (args.agent_id,)).fetchone():
        conn.close()
        raise SystemExit(f"error: unknown agent {args.agent_id}")
    thread = conn.execute("SELECT * FROM threads WHERE thread_id=?", (args.thread_id,)).fetchone()
    if thread is None:
        conn.close()
        raise SystemExit(f"error: unknown thread {args.thread_id}")
    participants = []
    try:
        participants = json.loads(thread["participants"] or "[]")
    except (json.JSONDecodeError, TypeError):
        participants = []
    # Group thread fallback: if thread participants are coarse and a group
    # owns the messages in this thread, use current group members on the home.
    if not participants:
        # (Old code queried group_messages.thread_id, a column that does not
        # exist, and discarded the result: an empty-participants thread crashed
        # with OperationalError instead of falling through to the involvement
        # check below.)
        pass
    if args.agent_id not in participants:
        # Allow access if this agent is the sender or original recipient of any
        # message in the thread; group members retain history while members,
        # and leaving members lose read on new messages but keep old history.
        involved = conn.execute(
            "SELECT 1 FROM messages WHERE thread_id=? AND (from_agent=? OR to_agent=?) LIMIT 1",
            (args.thread_id, args.agent_id, args.agent_id),
        ).fetchone()
        if involved is None:
            audit_log("thread_read_denied", requester=args.agent_id, thread_id=args.thread_id, participants=participants)
            conn.close()
            raise SystemExit(f"error: denied: {args.agent_id} is not a participant of {args.thread_id}")
    audit_log("thread_read_allowed", requester=args.agent_id, thread_id=args.thread_id)
    rows = conn.execute("SELECT * FROM messages WHERE thread_id=? ORDER BY created_at ASC, message_id ASC", (args.thread_id,)).fetchall()
    print(
        json_dumps(
            {
                "ok": True,
                "thread": row_to_dict(thread),
                "messages": [row_to_dict(r) for r in rows],
            }
        )
    )
    conn.close()
    return 0


def _build_payload(text: str, kind: str | None = None, metadata: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"text": text}
    if kind:
        payload["kind"] = kind
    if metadata:
        try:
            payload["metadata"] = json.loads(metadata)
        except json.JSONDecodeError:
            payload["metadata"] = {"note": metadata}
    return payload


def _insert_message(
    conn: sqlite3.Connection,
    *,
    message_id: str,
    thread_id: str,
    idempotency_key: str | None,
    from_agent: str,
    to_agent: str,
    message_type: str,
    urgency: str,
    payload: dict[str, Any],
    attachments: list[dict[str, Any]],
    ttl: int | None,
    expires: float | None,
    expects_response: int = 0,
    response_deadline: float | None = None,
    answer_to: str | None = None,
    correlation_id: str | None = None,
    created_at: float,
) -> None:
    conn.execute(
        """
        INSERT INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,message_type,urgency,
            payload,attachments,ttl_seconds,expires_at,expects_response,response_deadline,nudge_count,
            retry_count,status,answer_to_message_id,correlation_id,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            message_id, thread_id, idempotency_key, from_agent, to_agent,
            message_type, urgency, json_dumps(payload), json_dumps(attachments),
            ttl, expires, expects_response, response_deadline, 0, 0, "queued",
            answer_to, correlation_id, created_at,
        ),
    )


def _enqueue_outbox(
    conn: sqlite3.Connection, *, remote_peer: str, message_id: str,
    from_agent: str, to_agent: str, thread_id: str,
) -> str:
    outbox_id = str(uuid.uuid4())
    row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
    conn.execute(
        """
        INSERT INTO outbox (outbox_id,peer_name,message_id,from_agent,to_remote,thread_id,payload,status,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            outbox_id, remote_peer, message_id, from_agent, to_agent, thread_id,
            json_dumps(row_to_dict(row)), "pending", now(), now(),
        ),
    )
    return outbox_id


def _handoff_outbox_now(
    conn: sqlite3.Connection, *, outbox_id: str, remote_peer: str, message_id: str
) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
    peer = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
    ok, receipt = _handoff_peer(row_to_dict(peer), row_to_dict(row), timeout=10)
    conn.execute(
        "UPDATE outbox SET status=?, receipt=?, attempts=attempts+1, last_error=?, updated_at=?, delivered_at=? WHERE outbox_id=?",
        ("sent" if ok else "failed", receipt, None if ok else str(receipt)[:500], now(), now() if ok else None, outbox_id),
    )
    return {"outbox_id": outbox_id, "handoff": "ok" if ok else "failed", "receipt": receipt}


def cmd_send(args: argparse.Namespace) -> int:
    conn = connect()
    require_auth(conn, args.from_agent)
    remote_peer = None
    if "@" in args.to_agent:
        remote_to, remote_peer = args.to_agent.split("@", 1)
        validate_id("agent_id", remote_to)
        peer_row = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        if peer_row is None:
            conn.close()
            raise SystemExit(f"error: unknown peer {remote_peer!r}; run `bus peer add`")
        args.to_agent = remote_to
        target_stale = False
    else:
        validate_id("agent_id", args.to_agent)
        to_row = require_agent(conn, args.to_agent)
        target_stale = is_stale(to_row, now())
    # Make sure from can read token; auth done above.
    now_ts = now()
    if not (args.text or "").strip():
        conn.close()
        raise SystemExit("error: message text must not be empty")
    ttl = args.ttl
    # `if ttl` swallowed --ttl 0 into "no expiry"; an explicit 0 is explicit.
    expires = now_ts + ttl if ttl is not None else None
    thread_id = args.thread_id or str(uuid.uuid4())
    validate_id("thread_id", args.thread_id)
    ensure_thread(conn, thread_id, args.from_agent, [args.from_agent, args.to_agent])
    attachments = prepare_attachments(args.attachment, remote_peer is not None)
    text_bytes = len(args.text.encode("utf-8"))
    if text_bytes > 64 * 1024:
        conn.close()
        raise SystemExit("error: message text exceeds 64KB")
    soft_warning = "text exceeds 4KB soft cap; consider attachments" if text_bytes > 4 * 1024 else None
    if soft_warning:
        print(f"warning: {soft_warning}", file=sys.stderr)
    payload = _build_payload(args.text, metadata=args.metadata)
    found = find_duplicate_message(conn, args.from_agent, args.idempotency_key)
    if found is not None:
        conn.close()
        print(json_dumps({"ok": True, "duplicate": True, "message_id": found["message_id"], "thread_id": found["thread_id"]}))
        return 0
    message_id = str(uuid.uuid4())
    outbox_id: str | None = None
    message_type = args.message_type or "text"
    try:
        # One transaction: message row + its outbox handoff entry land
        # together, so a crash cannot leave a remote send with no outbox row.
        with tx(conn):
            _insert_message(
                conn,
                message_id=message_id,
                thread_id=thread_id,
                idempotency_key=scoped_idempotency_key(args.from_agent, args.idempotency_key),
                from_agent=args.from_agent,
                to_agent=args.to_agent,
                message_type=message_type,
                urgency=args.urgency or "normal",
                payload=payload,
                attachments=attachments,
                ttl=ttl,
                expires=expires,
                created_at=now_ts,
            )
            if remote_peer is not None:
                outbox_id = _enqueue_outbox(
                    conn, remote_peer=remote_peer, message_id=message_id,
                    from_agent=args.from_agent, to_agent=args.to_agent, thread_id=thread_id,
                )
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise SystemExit(f"error: insert failed: {exc}")
    audit_to = f"{args.to_agent}@{remote_peer}" if remote_peer else args.to_agent
    audit_log("send", from_agent=args.from_agent, to_agent=audit_to, message_id=message_id, thread_id=thread_id)
    remote_result = None
    if remote_peer is not None and outbox_id is not None:
        remote_result = _handoff_outbox_now(
            conn, outbox_id=outbox_id, remote_peer=remote_peer, message_id=message_id
        )
    result = {"ok": True, "message_id": message_id, "thread_id": thread_id, "to": args.to_agent, "type": message_type, "target_stale": target_stale}
    if soft_warning:
        result["warning"] = soft_warning
    if remote_result:
        result["remote"] = remote_result
    print(json_dumps(result))
    conn.close()
    return 0


def fetch_inbox(conn: sqlite3.Connection, agent_id: str, status: str | None, limit: int) -> list[sqlite3.Row]:
    require_agent(conn, agent_id)
    # Expiry is filtered by the query below; bulk status sweeps belong to
    # sweep_expired, not to every inbox read (which wrote on every poll tick).
    sql = "SELECT * FROM messages WHERE to_agent=?"
    params: list[Any] = [agent_id]
    if status == "undelivered":
        sql += " AND status IN ('queued','delivered') AND acked_at IS NULL"
    elif status == "all":
        pass  # "all" used to add `acked_at IS NULL`, silently hiding acked rows.
    elif status:
        sql += " AND status=?"
        params.append(status)
    else:
        sql += " AND acked_at IS NULL"
    sql += " AND (expires_at IS NULL OR expires_at > ?)"
    params.append(now())
    sql += " ORDER BY created_at ASC, message_id ASC LIMIT ?"
    params.append(limit)
    return conn.execute(sql, params).fetchall()


def cmd_inbox(args: argparse.Namespace) -> int:
    conn = connect()
    require_auth(conn, args.agent_id)
    rows = fetch_inbox(conn, args.agent_id, args.status, args.limit)
    print(json_dumps({"ok": True, "agent_id": args.agent_id, "count": len(rows), "messages": [row_to_dict(r) for r in rows]}))
    conn.close()
    return 0


def cmd_ack(args: argparse.Namespace) -> int:
    conn = connect()
    require_auth(conn, args.agent_id)
    require_agent(conn, args.agent_id)
    row = conn.execute(
        "SELECT * FROM messages WHERE message_id=? AND to_agent=?", (args.message_id, args.agent_id)
    ).fetchone()
    if row is None:
        conn.close()
        raise SystemExit(f"error: message {args.message_id} not found for {args.agent_id}")
    conn.execute(
        "UPDATE messages SET status='acked', acked_at=?, delivered_at=COALESCE(delivered_at,?) WHERE message_id=?",
        (now(), now(), args.message_id),
    )
    conn.commit()
    print(json_dumps({"ok": True, "message_id": args.message_id, "acked_at": now()}))
    conn.close()
    return 0


def cmd_ask(args: argparse.Namespace) -> int:
    conn = connect()
    require_auth(conn, args.from_agent)
    remote_peer = None
    if "@" in args.to_agent:
        remote_to, remote_peer = args.to_agent.split("@", 1)
        validate_id("agent_id", remote_to)
        peer_row = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        if peer_row is None:
            conn.close()
            raise SystemExit(f"error: unknown peer {remote_peer!r}")
        args.to_agent = remote_to
    else:
        validate_id("agent_id", args.to_agent)
        require_agent(conn, args.to_agent)
    now_ts = now()
    thread_id = args.thread_id or str(uuid.uuid4())
    validate_id("thread_id", args.thread_id)
    ensure_thread(conn, thread_id, args.from_agent, [args.from_agent, args.to_agent])
    attachments = prepare_attachments(args.attachment, remote_peer is not None)
    text_bytes = len(args.text.encode("utf-8"))
    if text_bytes > 64 * 1024:
        conn.close()
        print("error: message text exceeds 64KB", file=sys.stderr)
        raise SystemExit("error: message text exceeds 64KB")
    soft_warning = "text exceeds 4KB soft cap; consider attachments" if text_bytes > 4 * 1024 else None
    if soft_warning:
        print(f"warning: {soft_warning}", file=sys.stderr)
    ttl = args.ttl if args.ttl is not None else 300
    expires = now_ts + ttl
    expect_reply = bool(not getattr(args, "no_expect_reply", False))
    response_deadline = now_ts + NUDGE_FIRST_SECONDS if expect_reply else None
    found = find_duplicate_message(conn, args.from_agent, args.idempotency_key)
    if found is not None:
        conn.close()
        print(json_dumps({"ok": True, "duplicate": True, "message_id": found["message_id"], "thread_id": found["thread_id"]}))
        return 0
    message_id = str(uuid.uuid4())
    outbox_id: str | None = None
    payload = _build_payload(args.text, kind="ask", metadata=args.metadata)
    try:
        with tx(conn):
            _insert_message(
                conn,
                message_id=message_id,
                thread_id=thread_id,
                idempotency_key=scoped_idempotency_key(args.from_agent, args.idempotency_key),
                from_agent=args.from_agent,
                to_agent=args.to_agent,
                message_type="ask",
                urgency=args.urgency or "normal",
                payload=payload,
                attachments=attachments,
                ttl=ttl,
                expires=expires,
                expects_response=1 if expect_reply else 0,
                response_deadline=response_deadline,
                correlation_id=message_id,
                created_at=now_ts,
            )
            if remote_peer is not None:
                outbox_id = _enqueue_outbox(
                    conn, remote_peer=remote_peer, message_id=message_id,
                    from_agent=args.from_agent, to_agent=args.to_agent, thread_id=thread_id,
                )
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise SystemExit(f"error: insert failed: {exc}")
    audit_to = f"{args.to_agent}@{remote_peer}" if remote_peer else args.to_agent
    audit_log("send", from_agent=args.from_agent, to_agent=audit_to, message_id=message_id, thread_id=thread_id)
    remote_result = None
    if remote_peer is not None and outbox_id is not None:
        remote_result = _handoff_outbox_now(
            conn, outbox_id=outbox_id, remote_peer=remote_peer, message_id=message_id
        )
    result = {"ok": True, "message_id": message_id, "thread_id": thread_id, "to": args.to_agent, "type": "ask"}
    if soft_warning:
        result["warning"] = soft_warning
    if remote_result:
        result["remote"] = remote_result
    if not args.wait:
        print(json_dumps(result))
        conn.close()
        return 0
    timeout = args.timeout if args.timeout is not None else 120  # `or 120` swallowed --timeout 0
    deadline = time.time() + timeout
    answer_row = None
    stale = False
    last_wait_sweep = 0.0
    while time.time() < deadline:
        if time.time() - last_wait_sweep >= 2:
            sweep_nudges(conn)
            sweep_expired(conn)
            last_wait_sweep = time.time()
        status_row = conn.execute("SELECT status FROM messages WHERE message_id=?", (message_id,)).fetchone()
        if status_row and status_row["status"] == "stale":
            stale = True
            break
        # Correlation survives the daemon hop; the old query required
        # from_agent == the local recipient id, so an answer arriving as
        # bob@peer never matched. Keep the old answer_to fallback for rows
        # written before correlation_id existed.
        remote_from = f"{args.to_agent}@{remote_peer}" if remote_peer else ""
        rows = conn.execute(
            """
            SELECT * FROM messages
            WHERE thread_id=? AND message_type='answer' AND from_agent IN (?,?)
              AND (
                    correlation_id=?
                 OR (correlation_id IS NULL AND created_at >= ?
                     AND (answer_to_message_id=? OR answer_to_message_id IS NULL))
              )
            ORDER BY created_at ASC LIMIT 1
            """,
            (thread_id, args.to_agent, remote_from, message_id, now_ts, message_id),
        ).fetchall()
        if rows:
            answer_row = rows[0]
            break
        time.sleep(0.5)
    if stale:
        print(json_dumps({"ok": False, "stale": True, "message_id": message_id, "thread_id": thread_id, "hint": f"bus thread {thread_id}"}))
    elif answer_row is not None:
        print(json_dumps({"ok": True, "message_id": message_id, "thread_id": thread_id, "answer": row_to_dict(answer_row)}))
    else:
        print(json_dumps({"ok": True, "timeout": True, "message_id": message_id, "thread_id": thread_id, "hint": f"bus thread {thread_id}"}))
    conn.close()
    return 0


def cmd_answer(args: argparse.Namespace) -> int:
    validate_id("thread_id", args.thread_id)
    conn = connect()
    require_auth(conn, args.from_agent)
    thread = conn.execute("SELECT * FROM threads WHERE thread_id=?", (args.thread_id,)).fetchone()
    if thread is None:
        conn.close()
        raise SystemExit(f"error: unknown thread {args.thread_id}")
    to = args.to_agent
    if not to:
        # Prefer the most recent participant other than the answerer; fall
        # back to the thread starter only when no other participant exists.
        other = conn.execute(
            "SELECT from_agent FROM messages WHERE thread_id=? AND from_agent<>? ORDER BY created_at DESC, message_id DESC LIMIT 1",
            (args.thread_id, args.from_agent),
        ).fetchone()
        if other is not None:
            to = other["from_agent"]
        else:
            first = conn.execute(
                "SELECT from_agent FROM messages WHERE thread_id=? ORDER BY created_at ASC LIMIT 1", (args.thread_id,)
            ).fetchone()
            if first is None:
                conn.close()
                raise SystemExit("error: cannot derive recipient from empty thread")
            to = first["from_agent"]
    remote_peer = None
    if "@" in to:
        remote_to, remote_peer = to.split("@", 1)
        validate_id("agent_id", remote_to)
        peer_row = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        if peer_row is None:
            conn.close()
            raise SystemExit(f"error: unknown peer {remote_peer!r}")
        to = remote_to
    else:
        validate_id("agent_id", to)
        require_agent(conn, to)
    now_ts = now()
    ttl = args.ttl if args.ttl is not None else 300
    expires = now_ts + ttl
    found = find_duplicate_message(conn, args.from_agent, args.idempotency_key)
    if found is not None:
        conn.close()
        print(json_dumps({"ok": True, "duplicate": True, "message_id": found["message_id"], "thread_id": found["thread_id"]}))
        return 0
    message_id = str(uuid.uuid4())
    outbox_id: str | None = None
    payload = _build_payload(args.text, kind="answer", metadata=args.metadata)
    answer_to = args.answer_to
    ask_row = None
    if answer_to:
        ask_row = conn.execute(
            "SELECT message_id,correlation_id FROM messages WHERE message_id=?", (answer_to,)
        ).fetchone()
    else:
        ask_row = conn.execute(
            "SELECT message_id,correlation_id FROM messages WHERE thread_id=? AND message_type='ask' ORDER BY created_at DESC LIMIT 1",
            (args.thread_id,),
        ).fetchone()
        answer_to = ask_row["message_id"] if ask_row else None
    # correlation_id is the id the asker knows. A remote ask's local row has a
    # locally generated message_id, but preserves the sender's correlation id
    # through the outbox/handoff envelope, so `ask --wait` can still match.
    correlation_id = None
    if ask_row is not None:
        correlation_id = ask_row["correlation_id"] or ask_row["message_id"]
    elif answer_to:
        correlation_id = answer_to
    try:
        with tx(conn):
            _insert_message(
                conn,
                message_id=message_id,
                thread_id=args.thread_id,
                idempotency_key=scoped_idempotency_key(args.from_agent, args.idempotency_key),
                from_agent=args.from_agent,
                to_agent=to,
                message_type="answer",
                urgency=args.urgency or "normal",
                payload=payload,
                attachments=[],
                ttl=ttl,
                expires=expires,
                answer_to=answer_to,
                correlation_id=correlation_id,
                created_at=now_ts,
            )
            if remote_peer is not None:
                outbox_id = _enqueue_outbox(
                    conn, remote_peer=remote_peer, message_id=message_id,
                    from_agent=args.from_agent, to_agent=to, thread_id=args.thread_id,
                )
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise SystemExit(f"error: insert failed: {exc}")
    audit_to = f"{to}@{remote_peer}" if remote_peer else to
    audit_log("send", from_agent=args.from_agent, to_agent=audit_to, message_id=message_id, thread_id=args.thread_id)
    remote_result = None
    if remote_peer is not None and outbox_id is not None:
        remote_result = _handoff_outbox_now(
            conn, outbox_id=outbox_id, remote_peer=remote_peer, message_id=message_id
        )
    result = {"ok": True, "message_id": message_id, "thread_id": args.thread_id, "to": to, "type": "answer"}
    if remote_result:
        result["remote"] = remote_result
    print(json_dumps(result))
    conn.close()
    return 0
