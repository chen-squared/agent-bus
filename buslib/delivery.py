"""Notifier-side delivery preparation: attachments and group digests."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from .blobs import blob_materialize
from .messaging import ensure_thread


def materialize_attachments(message: dict[str, Any], conn: sqlite3.Connection) -> list[str]:
    try:
        raw = json.loads(message.get("attachments") or "[]")
    except (json.JSONDecodeError, TypeError):
        raw = []
    if not isinstance(raw, list):
        raw = []
    if not raw:
        return []
    paths: list[str] = []
    source_peer_name = None
    if "@" in str(message.get("from_agent", "")):
        source_peer_name = str(message["from_agent"]).split("@", 1)[1]
    peer = None
    if source_peer_name:
        row = conn.execute("SELECT * FROM peers WHERE peer_name=?", (source_peer_name,)).fetchone()
        if row is not None:
            peer = {k: row[k] for k in row.keys()}
    for item in raw:
        if isinstance(item, str):
            paths.append(item)
        elif isinstance(item, dict):
            if item.get("path") and Path(item["path"]).exists():
                paths.append(str(item["path"]))
            elif "sha256" in item:
                p = blob_materialize(item, source_peer_name or "", peer)
                paths.append(str(p))
            elif item.get("origin"):
                paths.append(str(item["origin"]))
    message["_attachment_paths"] = paths
    return paths


def poll_group_digest(agent_id: str, conn: sqlite3.Connection) -> dict[str, Any] | None:
    groups = conn.execute(
        """
        SELECT gm.group_id, g.topic, COALESCE(grs.last_read_seq, 0) AS last_read_seq,
               COALESCE(grs.last_digest_seq, 0) AS last_digest_seq
        FROM group_members gm
        JOIN groups g ON g.group_id = gm.group_id
        LEFT JOIN group_read_state grs ON grs.group_id = gm.group_id AND grs.agent_id = gm.agent_id
        WHERE gm.agent_id = ? AND gm.status = 'active' AND g.status = 'active'
        ORDER BY gm.group_id
        """,
        (agent_id,),
    ).fetchall()
    for g in groups:
        latest = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS seq FROM group_messages WHERE group_id=?", (g["group_id"],)
        ).fetchone()["seq"]
        if latest <= g["last_digest_seq"]:
            continue
        first = g["last_digest_seq"] + 1
        # Scan wider than the render budget. Own messages are skipped below, and
        # if the window were only as wide as the budget a burst of my own posts
        # would hold it open and starve everyone else's messages out of view.
        scan_to = min(g["last_digest_seq"] + 40, latest)
        rows = conn.execute(
            "SELECT seq,sender_agent,payload FROM group_messages WHERE group_id=? AND seq>=? AND seq<=? AND (expires_at IS NULL OR expires_at > ?) ORDER BY seq",
            (g["group_id"], first, scan_to, time.time()),
        ).fetchall()
        # My own posts are already in my context, so digesting them back to me is
        # a wake for nothing.
        foreign = [r for r in rows if r["sender_agent"] != agent_id]
        if not foreign:
            # Nothing to say to me, but the cursor still has to slide or the
            # window never moves past my own messages. Advancing here is safe:
            # the no-advance-before-inject invariant protects a render that could
            # fail, and there is nothing to render.
            conn.execute(
                """
                INSERT INTO group_read_state (group_id,agent_id,last_digest_seq,updated_at)
                VALUES (?,?,?,?)
                ON CONFLICT(group_id,agent_id) DO UPDATE SET
                    last_digest_seq=excluded.last_digest_seq,
                    updated_at=excluded.updated_at
                """,
                (g["group_id"], agent_id, scan_to, time.time()),
            )
            conn.commit()
            continue
        take = foreign[:5]
        last = take[-1]["seq"]
        pending = conn.execute(
            "SELECT COUNT(*) AS c FROM group_messages WHERE group_id=? AND seq>? AND seq<=? AND sender_agent<>? AND (expires_at IS NULL OR expires_at > ?)",
            (g["group_id"], g["last_digest_seq"], latest, agent_id, time.time()),
        ).fetchone()["c"]
        lines = [f"[group:{g['group_id']} topic:{g['topic']}] seq={first}-{scan_to} digest={len(take)} of {pending} unread (own posts not shown)"]
        for r in take:
            try:
                text = json.loads(r["payload"]).get("text", "")
            except (json.JSONDecodeError, TypeError):
                text = str(r["payload"])
            text = text.replace("\\n", " ").replace("\n", " ")
            if len(text) > 500:
                text = text[:500] + "…+" + str(len(text) - 500) + " chars (full: agent-bus group messages --group-id " + g["group_id"] + " --agent-id " + agent_id + ")"
            lines.append(f"  seq {r['seq']} from {r['sender_agent']}: {text}")
        digest_text = "\n".join(lines)
        message = {
            # Keyed on the last seq actually rendered, not on latest. Two digest
            # rounds inside the same latest window would otherwise collide on
            # this id, and commit_group_digest's INSERT OR IGNORE silently drops
            # the second row: the text still reaches the adapter, but
            # `agent-bus message show` on that pointer returns not found.
            "message_id": f"group-digest-{g['group_id']}-{last}-{agent_id}",
            "from_agent": f"{g['group_id']}",
            # CommandAdapter reads message["to_agent"] for the agent_id
            # placeholder and for AGENT_BUS_TO_AGENT. Its absence raised
            # KeyError on every digest, so command-adapter agents received
            # no group messages at all.
            "to_agent": agent_id,
            "thread_id": g["group_id"],
            "payload": json.dumps({"text": digest_text}, ensure_ascii=False),
            "attachments": "[]",
            "_attachment_paths": [],
            "_group_id": g["group_id"],
            "_digest_seq": last,
        }
        # The cursor is NOT advanced here. Committing it before inject marked
        # unread messages as digested even when inject raised, which silently
        # dropped group messages. See commit_group_digest.
        return message
    return None


def commit_group_digest(conn: sqlite3.Connection, agent_id: str, message: dict[str, Any]) -> None:
    """Advance the digest cursor, but only once the adapter accepted the digest."""
    now_ts = time.time()
    payload = json.loads(message["payload"])
    ensure_thread(conn, message["thread_id"], agent_id, [agent_id])
    conn.execute(
        """
        INSERT OR IGNORE INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,
            message_type,urgency,payload,attachments,ttl_seconds,expires_at,status,delivered_at,acked_at,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            message["message_id"], message["thread_id"], None, agent_id,
            message["to_agent"], "system", "normal",
            json.dumps(payload, ensure_ascii=False), "[]", 86400,
            now_ts + 86400, "acked", now_ts, now_ts, now_ts,
        ),
    )
    # UPSERT, matching poll_group_digest's own-only branch: a bare UPDATE
    # matched 0 rows when no group_read_state row existed, the cursor never
    # moved, and the same digest re-injected every poll forever.
    conn.execute(
        """
        INSERT INTO group_read_state (group_id,agent_id,last_digest_seq,updated_at)
        VALUES (?,?,?,?)
        ON CONFLICT(group_id,agent_id) DO UPDATE SET
            last_digest_seq=excluded.last_digest_seq,
            updated_at=excluded.updated_at
        """,
        (message["_group_id"], agent_id, message["_digest_seq"], time.time()),
    )
    conn.commit()


__all__ = ["commit_group_digest", "materialize_attachments", "poll_group_digest"]
