"""Group chat commands and their four send gates."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sqlite3
import sys
import uuid

from .config import audit_log, json_dumps, machine_id, now, row_to_dict
from .db import connect, require_auth, tx
from .validation import validate_id


def _group_require_member(conn: sqlite3.Connection, group_id: str, agent_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM group_members WHERE group_id=? AND agent_id=?", (group_id, agent_id)).fetchone()
    if row is None:
        raise SystemExit(f"error: {agent_id} is not a member of {group_id}")
    if row["status"] != "active":
        raise SystemExit(f"error: {agent_id} is not active in {group_id}")
    return row


def cmd_group_create(args: argparse.Namespace) -> int:
    validate_id("group_id", args.group_id)
    conn = connect()
    if not args.owner:
        args.owner = args.from_agent if args.from_agent else None
    if not args.owner:
        conn.close()
        raise SystemExit("error: group create requires --owner or --from (a group without an owner cannot be closed)")
    require_auth(conn, args.owner)
    ceiling = args.ceiling_per_minute if args.ceiling_per_minute is not None else 120
    # The ceiling check is `recent >= ceiling`, so 0 blocks the very first send
    # forever. A group that cannot ever send is not useful; reject it at create
    # instead of letting the first member discover it via the send gate.
    if ceiling <= 0:
        conn.close()
        raise SystemExit("error: --ceiling-per-minute must be > 0; 0 would block every group send")
    now_ts = now()
    try:
        with tx(conn):
            conn.execute(
                """
                INSERT INTO groups (group_id,topic,home_machine,mode,max_members,rate_seconds,ceiling_per_minute,
                    backlog_threshold,status,created_by,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    args.group_id,
                    args.topic,
                    machine_id(),
                    args.mode or "catch-up-gated",
                    # `or` here swallowed explicit zeros: --backlog-threshold 0 stored 50,
                    # --ceiling-per-minute 0 stored 120, --max-members 0 stored 50. A flag
                    # that silently becomes a different number is worse than a rejected one.
                    (args.max_members if args.max_members is not None else 50),
                    (args.rate_seconds if args.rate_seconds is not None else 2),
                    ceiling,
                    (args.backlog_threshold if args.backlog_threshold is not None else 50),
                    "active",
                    args.owner,
                    now_ts,
                    now_ts,
                ),
            )
            conn.execute(
                "INSERT INTO group_members (group_id,agent_id,role,rate_seconds,joined_at,status) VALUES (?,?,?,?,?,?)",
                (args.group_id, args.owner, "owner", (args.rate_seconds if args.rate_seconds is not None else 2), now_ts, "active"),
            )
            conn.execute(
                "INSERT INTO group_read_state (group_id,agent_id,last_read_seq,updated_at) VALUES (?,?,?,?)",
                (args.group_id, args.owner, 0, now_ts),
            )
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise SystemExit(f"error: create group failed: {exc}")
    print(json_dumps({"ok": True, "group_id": args.group_id, "home_machine": machine_id()}))
    conn.close()
    return 0


def cmd_group_join(args: argparse.Namespace) -> int:
    validate_id("group_id", args.group_id)
    conn = connect()
    require_auth(conn, args.agent_id)
    group = conn.execute("SELECT * FROM groups WHERE group_id=?", (args.group_id,)).fetchone()
    if group is None:
        conn.close()
        raise SystemExit(f"error: unknown group {args.group_id}")
    if group["status"] != "active":
        conn.close()
        raise SystemExit(f"error: group {args.group_id} is {group['status']}")
    # --role used to be accepted here and then dropped on the floor: the INSERT
    # below writes "member" unconditionally, so `--role owner` landed a member and
    # reported ok. A silently ignored privilege flag is worse than a rejected one,
    # because the caller walks away believing it worked. Roles are assigned only at
    # group create (--owner); join can never grant or change one.
    if args.role is not None and args.role != "member":
        conn.close()
        raise SystemExit(
            f"error: --role is not settable on join (got {args.role!r}); "
            "every join lands as member, roles come only from group create --owner"
        )
    rate = (args.rate_seconds if args.rate_seconds is not None else (group["rate_seconds"] if group["rate_seconds"] is not None else 2))
    now_ts = now()
    # The capacity check and the two inserts are one transaction: the old
    # COUNT-then-INSERT allowed two concurrent joins to both pass the cap.
    with tx(conn):
        count = conn.execute("SELECT COUNT(*) AS c FROM group_members WHERE group_id=? AND status='active'", (args.group_id,)).fetchone()["c"]
        already = conn.execute("SELECT status FROM group_members WHERE group_id=? AND agent_id=?", (args.group_id, args.agent_id)).fetchone()
        if count >= group["max_members"] and (already is None or already["status"] != "active"):
            # No conn.close() here: the tx context manager rolls back first,
            # and closing inside would make the rollback raise and mask the error.
            raise SystemExit(f"error: group {args.group_id} is full")
        conn.execute(
            """
            INSERT INTO group_members (group_id,agent_id,role,rate_seconds,joined_at,status)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(group_id,agent_id) DO UPDATE SET
                status='active',
                rate_seconds=excluded.rate_seconds,
                joined_at=excluded.joined_at
            """,
            (args.group_id, args.agent_id, "member", rate, now_ts, "active"),
        )
        conn.execute(
            "INSERT OR IGNORE INTO group_read_state (group_id,agent_id,last_read_seq,updated_at) VALUES (?,?,?,?)",
            (args.group_id, args.agent_id, 0, now_ts),
        )
    audit_log("group_join", group_id=args.group_id, agent_id=args.agent_id)
    print(json_dumps({"ok": True, "group_id": args.group_id, "agent_id": args.agent_id}))
    conn.close()
    return 0


def cmd_group_leave(args: argparse.Namespace) -> int:
    validate_id("group_id", args.group_id)
    conn = connect()
    # Without this any same-user agent could leave on someone else's behalf,
    # which deletes that member's read_state row and resets their digest cursor
    # to zero -- more destructive than an unauthenticated join.
    require_auth(conn, args.agent_id)
    row = conn.execute(
        "SELECT status FROM group_members WHERE group_id=? AND agent_id=?", (args.group_id, args.agent_id)
    ).fetchone()
    if row is None:
        print(json_dumps({"ok": True, "group_id": args.group_id, "agent_id": args.agent_id, "status": "not_a_member"}))
        conn.close()
        return 0
    if row["status"] != "active":
        print(json_dumps({"ok": True, "group_id": args.group_id, "agent_id": args.agent_id, "status": "already_left"}))
        conn.close()
        return 0
    with tx(conn):
        conn.execute("UPDATE group_members SET status='left' WHERE group_id=? AND agent_id=?", (args.group_id, args.agent_id))
        conn.execute("DELETE FROM group_read_state WHERE group_id=? AND agent_id=?", (args.group_id, args.agent_id))
    audit_log("group_leave", group_id=args.group_id, agent_id=args.agent_id)
    print(json_dumps({"ok": True, "group_id": args.group_id, "agent_id": args.agent_id, "status": "left"}))
    conn.close()
    return 0


def cmd_group_close(args: argparse.Namespace) -> int:
    validate_id("group_id", args.group_id)
    conn = connect()
    require_auth(conn, args.agent_id)
    if conn.execute("SELECT 1 FROM groups WHERE group_id=?", (args.group_id,)).fetchone() is None:
        conn.close()
        raise SystemExit(f"error: unknown group {args.group_id}")
    member = conn.execute("SELECT role FROM group_members WHERE group_id=? AND agent_id=?", (args.group_id, args.agent_id)).fetchone()
    if member is None or member["role"] not in {"owner", "admin"}:
        group = conn.execute("SELECT created_by FROM groups WHERE group_id=?", (args.group_id,)).fetchone()
        if group is None or group["created_by"] != args.agent_id:
            conn.close()
            raise SystemExit("error: group close requires owner or admin")
    conn.execute("UPDATE groups SET status='closed', updated_at=? WHERE group_id=?", (now(), args.group_id))
    conn.commit()
    print(json_dumps({"ok": True, "group_id": args.group_id, "status": "closed"}))
    conn.close()
    return 0


def cmd_group_list(args: argparse.Namespace) -> int:
    conn = connect()
    if args.agent_id:
        rows = conn.execute(
            "SELECT g.* FROM groups g JOIN group_members gm ON g.group_id=gm.group_id WHERE gm.agent_id=? AND gm.status='active'",
            (args.agent_id,),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM groups ORDER BY group_id").fetchall()
    groups = []
    for row in rows:
        item = row_to_dict(row)
        item["member_count"] = conn.execute("SELECT COUNT(*) AS c FROM group_members WHERE group_id=? AND status='active'", (row["group_id"],)).fetchone()["c"]
        item["latest_seq"] = conn.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM group_messages WHERE group_id=?", (row["group_id"],)).fetchone()["seq"]
        groups.append(item)
    print(json_dumps({"ok": True, "count": len(groups), "groups": groups}))
    conn.close()
    return 0


def cmd_group_messages(args: argparse.Namespace) -> int:
    validate_id("group_id", args.group_id)
    conn = connect()
    if conn.execute("SELECT 1 FROM groups WHERE group_id=?", (args.group_id,)).fetchone() is None:
        conn.close()
        raise SystemExit(f"error: unknown group {args.group_id}")
    if args.agent_id:
        # Membership alone was guessable: group_id + agent_id are not secrets,
        # and this call both reads the member's entire unread history and moves
        # their last_served_seq cursor. It now requires the member's token.
        require_auth(conn, args.agent_id)
        _group_require_member(conn, args.group_id, args.agent_id)
        state = conn.execute("SELECT last_read_seq FROM group_read_state WHERE group_id=? AND agent_id=?", (args.group_id, args.agent_id)).fetchone()
        since = state["last_read_seq"] if state else 0
        # Explicit 0 means "counters only, no rows" -- same falsiness rule as the
        # other numeric group flags, where `or 50` rewrote a configured 0.
        limit = args.limit if args.limit is not None else 50
        if limit < 0:
            conn.close()
            raise SystemExit("error: --limit cannot be negative (SQLite reads LIMIT -1 as no limit)")
        # group_messages.expires_at was written but never enforced anywhere.
        live = "AND (expires_at IS NULL OR expires_at > ?)"
        ts = now()
        unread_total = conn.execute(
            f"SELECT COUNT(*) AS c FROM group_messages WHERE group_id=? AND seq>? {live}", (args.group_id, since, ts)
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM group_messages WHERE group_id=? AND seq>? {live} ORDER BY seq ASC LIMIT ?",
            (args.group_id, since, ts, limit),
        ).fetchall()
    else:
        conn.close()
        raise SystemExit("error: denied: --agent-id is required for group messages read")
    latest = conn.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM group_messages WHERE group_id=?", (args.group_id,)).fetchone()["seq"]
    # Record the high-water mark of what this call actually handed over. Without it
    # `group read --seq <latest>` acknowledges rows the caller was never shown: the
    # default --limit is 50 and the page is the OLDEST unread, so one capped call
    # plus one read silently marks the tail as seen. `group read` clamps against
    # this. The notifier digest path deliberately never writes it -- a digest is not
    # a receipt for a gap the caller chose to acknowledge.
    served_through = rows[-1]["seq"] if rows else 0
    if not rows and unread_total == 0:
        # Nothing live remains above `since`: any leftover seqs are expired and
        # gone, so acknowledging through latest is honest, not a gap.
        served_through = max(served_through, latest)
    if served_through:
        conn.execute(
            """
            INSERT INTO group_read_state (group_id,agent_id,last_served_seq,updated_at) VALUES (?,?,?,?)
            ON CONFLICT(group_id,agent_id) DO UPDATE SET
                last_served_seq=CASE WHEN excluded.last_served_seq>group_read_state.last_served_seq THEN excluded.last_served_seq ELSE group_read_state.last_served_seq END,
                updated_at=excluded.updated_at
            """,
            (args.group_id, args.agent_id, served_through, now()),
        )
    truncated = unread_total > len(rows)
    if truncated and not getattr(args, "quiet", False):
        span = f"seq {rows[0]['seq']}-{served_through}" if rows else "no rows at all"
        print(
            f"warning: returned {len(rows)} of {unread_total} unread ({span}); "
            f"{unread_total - len(rows)} newer unread not shown. Ack through {served_through}, call again for the "
            "rest, or raise --limit.",
            file=sys.stderr,
        )
    print(json_dumps({
        "ok": True, "group_id": args.group_id, "latest_seq": latest, "count": len(rows),
        "unread_total": unread_total, "truncated": truncated, "served_through_seq": served_through,
        "messages": [row_to_dict(r) for r in rows],
    }))
    conn.close()
    return 0


def cmd_group_history(args: argparse.Namespace) -> int:
    """Read stored group messages without moving any read cursor.

    `group messages` is intentionally an unread/digest view: after a catch-up
    it returns nothing, so a late joiner or an auditor has no CLI way to inspect
    sender order. The rows were always retained; expose them explicitly instead
    of forcing a sqlite query.
    """
    validate_id("group_id", args.group_id)
    validate_id("agent_id", args.agent_id)
    conn = connect()
    if conn.execute("SELECT 1 FROM groups WHERE group_id=?", (args.group_id,)).fetchone() is None:
        conn.close()
        raise SystemExit(f"error: unknown group {args.group_id}")
    require_auth(conn, args.agent_id)
    _group_require_member(conn, args.group_id, args.agent_id)
    limit = args.limit if args.limit is not None else 50
    if limit <= 0:
        conn.close()
        raise SystemExit("error: group history --limit must be > 0")
    latest = conn.execute(
        "SELECT COALESCE(MAX(seq),0) AS seq FROM group_messages WHERE group_id=?",
        (args.group_id,),
    ).fetchone()["seq"]
    before = args.before_seq if args.before_seq is not None else latest + 1
    rows = conn.execute(
        "SELECT * FROM group_messages WHERE group_id=? AND seq<? ORDER BY seq DESC LIMIT ?",
        (args.group_id, before, limit),
    ).fetchall()
    rows.reverse()
    oldest = rows[0]["seq"] if rows else None
    has_more = False
    if oldest is not None:
        has_more = conn.execute(
            "SELECT 1 FROM group_messages WHERE group_id=? AND seq<? LIMIT 1",
            (args.group_id, oldest),
        ).fetchone() is not None
    print(json_dumps({
        "ok": True,
        "group_id": args.group_id,
        "agent_id": args.agent_id,
        "latest_seq": latest,
        "before_seq": before,
        "count": len(rows),
        "has_more": has_more,
        "next_before_seq": oldest if has_more else None,
        "messages": [row_to_dict(r) for r in rows],
    }))
    conn.close()
    return 0


def cmd_group_read(args: argparse.Namespace) -> int:
    validate_id("group_id", args.group_id)
    conn = connect()
    # Same reasoning as group messages: this moves the member's last_read_seq,
    # i.e. it disarms the read gate on their behalf. Guessing group_id+agent_id
    # must not be enough.
    require_auth(conn, args.agent_id)
    _group_require_member(conn, args.group_id, args.agent_id)
    latest = conn.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM group_messages WHERE group_id=?", (args.group_id,)).fetchone()["seq"]
    requested = args.seq if args.seq is not None else latest
    if requested < 0 or requested > latest:
        conn.close()
        raise SystemExit(f"error: invalid seq {requested}, latest is {latest}")
    state = conn.execute(
        "SELECT last_read_seq,last_served_seq FROM group_read_state WHERE group_id=? AND agent_id=?",
        (args.group_id, args.agent_id),
    ).fetchone()
    cur = state["last_read_seq"] if state else 0
    served = state["last_served_seq"] if state else 0
    # Acknowledging a seq you were never served is the silent message-loss path:
    # `group messages` pages the OLDEST unread rows (default --limit 50) while
    # reporting `latest_seq`, so `read --seq <latest>` after one capped call burned
    # the tail. The cursor now stops at the highest seq actually handed over, and
    # the read gate keeps the caller honest about the rest.
    ceiling = max(cur, served)
    seq = requested
    clamped = requested > ceiling and not args.allow_gap
    if clamped:
        seq = ceiling
    elif requested > ceiling:
        print(
            f"warning: --allow-gap acks seq {ceiling + 1}-{requested} ({requested - ceiling} messages) that "
            "group messages never returned to you.",
            file=sys.stderr,
        )
    if clamped:
        print(
            f"warning: asked to ack seq {requested}, but group messages has only served you through {served}; "
            f"cursor stopped at {seq}. Call group messages again for the rest, or pass --allow-gap.",
            file=sys.stderr,
        )
    conn.execute(
        """
        INSERT INTO group_read_state (group_id,agent_id,last_read_seq,updated_at)
        VALUES (?,?,?,?)
        ON CONFLICT(group_id,agent_id) DO UPDATE SET
            last_read_seq=CASE WHEN excluded.last_read_seq>group_read_state.last_read_seq THEN excluded.last_read_seq ELSE group_read_state.last_read_seq END,
            updated_at=excluded.updated_at
        """,
        (args.group_id, args.agent_id, seq, now()),
    )
    conn.commit()
    out = {
        "ok": True, "group_id": args.group_id, "agent_id": args.agent_id, "last_read_seq": seq,
        "latest_seq": latest, "requested_seq": requested, "served_through_seq": served,
        "unacknowledged_seq_count": latest - seq,
    }
    if clamped:
        out["clamped"] = True
    if args.allow_gap and requested > ceiling:
        out["gap_acked"] = requested - ceiling
    print(json_dumps(out))
    conn.close()
    return 0


def _group_send_gates(
    conn: sqlite3.Connection, group: sqlite3.Row, agent_id: str, now_ts: float
) -> int:
    """Evaluate the read, rate, ceiling, and backpressure gates.

    Runs inside the caller's BEGIN IMMEDIATE transaction so every check and the
    subsequent seq assignment guard the same state. Returns the latest seq.
    """
    group_id = group["group_id"]
    latest = conn.execute(
        "SELECT COALESCE(MAX(seq),0) AS seq FROM group_messages WHERE group_id=?", (group_id,)
    ).fetchone()["seq"]
    read_state = conn.execute(
        "SELECT last_read_seq FROM group_read_state WHERE group_id=? AND agent_id=?", (group_id, agent_id)
    ).fetchone()
    last_read = read_state["last_read_seq"] if read_state else 0
    digest_state = conn.execute(
        "SELECT last_digest_seq FROM group_read_state WHERE group_id=? AND agent_id=?", (group_id, agent_id)
    ).fetchone()
    last_digest = digest_state["last_digest_seq"] if digest_state and digest_state["last_digest_seq"] is not None else 0
    if latest > 0 and last_read < latest:
        raise SystemExit(
            json_dumps(
                {
                    "error": "must confirm latest message first",
                    "latest_seq": latest,
                    "last_read_seq": last_read,
                    "last_digest_seq": last_digest,
                    "digest_health_hint": (
                        "caught-up messages may still be unprocessed by your notifier"
                        if last_digest < latest
                        else "digest path is healthy, only a manual read catch-up is missing"
                    ),
                }
            )
        )
    # Re-read the member row under the write lock: the pre-tx read could
    # be stale against a concurrent send by the same member.
    member = conn.execute(
        "SELECT * FROM group_members WHERE group_id=? AND agent_id=?", (group_id, agent_id)
    ).fetchone()
    if member is None or member["status"] != "active":
        raise SystemExit(f"error: {agent_id} is not active in {group_id}")
    rate = member["rate_seconds"] if member["rate_seconds"] is not None else (group["rate_seconds"] if group["rate_seconds"] is not None else 2)
    # now_ts is captured after BEGIN IMMEDIATE. Clamp a negative delta
    # (clock skew) to zero instead of reporting wait > rate.
    dt = max(0.0, now_ts - float(member["last_sent_at"])) if member["last_sent_at"] else 0.0
    if rate > 0 and member["last_sent_at"] and dt < rate:
        wait = round(rate - dt, 3)
        raise SystemExit(json_dumps({"error": "rate limited", "retry_after_seconds": wait}))
    # Same falsiness rule as cmd_group_create: a stored 0 means 0, not the default.
    ceiling = group["ceiling_per_minute"] if group["ceiling_per_minute"] is not None else 120
    recent = conn.execute(
        "SELECT COUNT(*) AS c FROM group_messages WHERE group_id=? AND created_at>?",
        (group_id, now_ts - 60),
    ).fetchone()["c"]
    if recent >= ceiling:
        raise SystemExit(json_dumps({"error": "group ceiling reached", "retry_after_seconds": 5}))
    backlog = conn.execute(
        """
        SELECT COUNT(*) AS c FROM group_read_state grs
        JOIN group_members gm ON gm.group_id=grs.group_id AND gm.agent_id=grs.agent_id
        WHERE grs.group_id=? AND gm.status='active' AND grs.last_read_seq < ?
        """,
        (group_id, latest),
    ).fetchone()["c"]
    if backlog > (group["backlog_threshold"] if group["backlog_threshold"] is not None else 50):
        raise SystemExit(json_dumps({"error": "group backpressure: too many unread participants"}))
    return latest


def cmd_group_send(args: argparse.Namespace) -> int:
    validate_id("group_id", args.group_id)
    conn = connect()
    require_auth(conn, args.from_agent)
    group = conn.execute("SELECT * FROM groups WHERE group_id=?", (args.group_id,)).fetchone()
    if group is None:
        conn.close()
        raise SystemExit(f"error: unknown group {args.group_id}")
    if group["status"] != "active":
        conn.close()
        raise SystemExit(f"error: group {args.group_id} is {group['status']}")
    _group_require_member(conn, args.group_id, args.from_agent)
    # Input validation runs BEFORE the gates: a blank message to a group you
    # have not caught up on used to report "must confirm latest message first"
    # instead of "text must not be empty".
    if not (args.text or "").strip():
        conn.close()
        raise SystemExit("error: group message text must not be empty")
    text_bytes = len(args.text.encode("utf-8"))
    if text_bytes > 64 * 1024:
        conn.close()
        raise SystemExit("error: message text exceeds 64KB")
    soft_warning = "text exceeds 4KB soft cap; consider attachments" if text_bytes > 4 * 1024 else None
    message_id = str(uuid.uuid4())
    payload = {"text": args.text}
    # Gates, seq assignment, insert and cursor bump are ONE transaction.
    # BEGIN IMMEDIATE serializes concurrent senders: the second sender reads a
    # fresh latest after the first commits, so the old MAX(seq)+1 TOCTOU (both
    # computed the same seq, the loser got `error: insert failed`) is gone,
    # and the ceiling/rate gates are evaluated under the same lock they guard.
    with tx(conn):
        # Capture time after BEGIN IMMEDIATE. Capturing it before the lock
        # made dt negative when another sender held the lock, which tripped
        # the rate gate even for a member that had not sent recently.
        now_ts = now()
        latest = _group_send_gates(conn, group, args.from_agent, now_ts)
        next_seq = latest + 1
        # `if args.ttl` swallowed --ttl 0; an explicit 0 is explicit.
        expires = now_ts + args.ttl if args.ttl is not None else None
        conn.execute(
            """
            INSERT INTO group_messages (message_id,group_id,seq,sender_agent,message_type,urgency,payload,attachments,expires_at,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                message_id,
                args.group_id,
                next_seq,
                args.from_agent,
                args.message_type or "text",
                args.urgency or "normal",
                json_dumps(payload),
                "[]",
                expires,
                now_ts,
            ),
        )
        conn.execute("UPDATE group_members SET last_sent_at=? WHERE group_id=? AND agent_id=?", (now_ts, args.group_id, args.from_agent))
        conn.execute(
            """
            INSERT INTO group_read_state (group_id,agent_id,last_read_seq,updated_at,last_digest_seq)
            VALUES (?,?,?,?,?)
            ON CONFLICT(group_id,agent_id) DO UPDATE SET
                last_read_seq=CASE WHEN excluded.last_read_seq>group_read_state.last_read_seq THEN excluded.last_read_seq ELSE group_read_state.last_read_seq END,
                updated_at=excluded.updated_at
            """,
            (args.group_id, args.from_agent, next_seq, now_ts, 0),
        )
    audit_log("group_send", group_id=args.group_id, from_agent=args.from_agent, message_id=message_id, seq=next_seq)
    result = {"ok": True, "group_id": args.group_id, "message_id": message_id, "seq": next_seq, "latest_seq": next_seq}
    if soft_warning:
        result["warning"] = soft_warning
    print(json_dumps(result))
    conn.close()
    return 0


def _captured_json(func, args: argparse.Namespace) -> dict:
    """Run an existing CLI handler and parse what it printed to stdout."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        func(args)
    try:
        return json.loads(buf.getvalue())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"error: group catch-up could not parse {func.__name__} output: {exc}") from exc


def cmd_group_catch_up(args: argparse.Namespace) -> int:
    """Drain unread group pages and ack what was actually served.

    The notifier digest deliberately advances only `last_digest_seq`. An agent
    that has digested everything still trips the read gate until it manually
    pages `group messages` and calls `group read`. This command is the safe
    shorthand: it uses the same clamps and auth as the two lower-level calls,
    so it cannot ack a gap it never saw.
    """
    validate_id("group_id", args.group_id)
    validate_id("agent_id", args.agent_id)
    limit = 200 if args.limit is None else args.limit
    if limit <= 0:
        raise SystemExit("error: group catch-up --limit must be > 0")
    pages = 0
    messages_read = 0
    latest = 0
    last_read = 0
    while True:
        page = _captured_json(
            cmd_group_messages,
            argparse.Namespace(
                group_id=args.group_id, agent_id=args.agent_id,
                limit=limit, quiet=True,
            ),
        )
        pages += 1
        latest = int(page.get("latest_seq") or 0)
        messages_read += int(page.get("count") or 0)
        served = int(page.get("served_through_seq") or 0)
        if page.get("count"):
            read = _captured_json(
                cmd_group_read,
                argparse.Namespace(
                    group_id=args.group_id, agent_id=args.agent_id,
                    seq=served, allow_gap=False,
                ),
            )
            last_read = int(read.get("last_read_seq") or 0)
        if not page.get("truncated"):
            break
        if pages >= 10000:
            raise SystemExit("error: group catch-up exceeded 10000 pages; refusing to loop")
    # The final read covers the "nothing live remains" case where group
    # messages reports served_through=latest but returned no rows.
    read = _captured_json(
        cmd_group_read,
        argparse.Namespace(
            group_id=args.group_id, agent_id=args.agent_id,
            seq=latest, allow_gap=False,
        ),
    )
    last_read = int(read.get("last_read_seq") or last_read)
    print(json_dumps({
        "ok": True,
        "group_id": args.group_id,
        "agent_id": args.agent_id,
        "pages": pages,
        "messages_read": messages_read,
        "latest_seq": latest,
        "last_read_seq": last_read,
        "read_through": last_read,
    }))
    return 0


def cmd_group_status(args: argparse.Namespace) -> int:
    validate_id("group_id", args.group_id)
    conn = connect()
    # group status prints the member list and every member's cursors; it used
    # to have no authorization at all. It is now member-only, like group
    # messages.
    require_auth(conn, args.agent_id)
    _group_require_member(conn, args.group_id, args.agent_id)
    group = conn.execute("SELECT * FROM groups WHERE group_id=?", (args.group_id,)).fetchone()
    if group is None:
        conn.close()
        raise SystemExit(f"error: unknown group {args.group_id}")
    members = conn.execute("SELECT * FROM group_members WHERE group_id=? ORDER BY agent_id", (args.group_id,)).fetchall()
    latest = conn.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM group_messages WHERE group_id=?", (args.group_id,)).fetchone()["seq"]
    states = conn.execute("SELECT * FROM group_read_state WHERE group_id=?", (args.group_id,)).fetchall()
    msg_count = conn.execute("SELECT COUNT(*) AS c FROM group_messages WHERE group_id=?", (args.group_id,)).fetchone()["c"]
    recent = conn.execute("SELECT COUNT(*) AS c FROM group_messages WHERE group_id=? AND created_at>?", (args.group_id, now() - 60)).fetchone()["c"]
    print(
        json_dumps(
            {
                "ok": True,
                "group": row_to_dict(group),
                "latest_seq": latest,
                "message_count": msg_count,
                "messages_last_minute": recent,
                "ceilings": {"rate_seconds": group["rate_seconds"], "per_minute": group["ceiling_per_minute"]},
                "members": [row_to_dict(r) for r in members],
                "read_state": [row_to_dict(r) for r in states],
            }
        )
    )
    conn.close()
    return 0


__all__ = [
    "cmd_group_catch_up",
    "cmd_group_close",
    "cmd_group_history",
    "cmd_group_create",
    "cmd_group_join",
    "cmd_group_leave",
    "cmd_group_list",
    "cmd_group_messages",
    "cmd_group_read",
    "cmd_group_send",
    "cmd_group_status",
]
