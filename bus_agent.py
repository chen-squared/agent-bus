#!/usr/bin/env python3
"""Per-agent notifier for agent-bus v0.

Polls the shared SQLite bus for undelivered messages addressed to this agent.
On receipt it injects them into the native agent through an adapter, records
delivery for idempotency, then acks."
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any

import bus

DEFAULT_CLI = "/Users/chen/Library/Application Support/ai.opencode.desktop/cli/2.0.29-arm64/opencode-cli"
DEFAULT_ADAPTER = "opencode"


class AdapterError(RuntimeError):
    pass


class OpencodeAdapter:
    def __init__(self, session_id: str, cli: str, cwd: str):
        self.session_id = session_id
        self.cli = cli
        self.cwd = cwd

    def target(self) -> str:
        return self.session_id

    def inject(self, message: dict[str, Any]) -> None:
        payload = json.loads(message["payload"])
        text = payload.get("text", "")
        paths = message.get("_attachment_paths") or []
        rendered = f"[bus:{message['message_id']} from:{message['from_agent']} thread:{message['thread_id']}] {text}"
        if paths:
            rendered += "\\nAttachments:\\n" + "\\n".join(paths)
        body = json.dumps({"text": rendered}, ensure_ascii=False)
        result = subprocess.run(
            [self.cli, "api", "session.prompt", "--param", f"sessionID={self.session_id}", "-d", body],
            cwd=self.cwd,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            raise AdapterError((result.stderr or result.stdout or "opencode prompt failed").strip())


class TmuxAdapter:
    def __init__(self, target: str):
        self.target = target

    def target_id(self) -> str:
        return self.target

    def is_idle(self) -> bool:
        out = subprocess.run(
            ["tmux", "capture-pane", "-t", self.target, "-p"],
            capture_output=True,
            text=True,
        )
        if out.returncode != 0:
            return True
        first = out.stdout
        time.sleep(0.3)
        out2 = subprocess.run(
            ["tmux", "capture-pane", "-t", self.target, "-p"],
            capture_output=True,
            text=True,
        )
        return out2.returncode == 0 and out2.stdout == first

    def inject(self, message: dict[str, Any]) -> None:
        if not self.is_idle():
            raise AdapterError("target tmux pane busy")
        payload = json.loads(message["payload"])
        paths = message.get("_attachment_paths") or []
        text = f"[bus:{message['message_id']} from:{message['from_agent']} thread:{message['thread_id']}] {payload.get('text','')}"
        if paths:
            text += "\\nAttachments:\\n" + "\\n".join(paths)
        subprocess.run(["tmux", "send-keys", "-t", self.target, text], check=True)
        time.sleep(0.1)
        subprocess.run(["tmux", "send-keys", "-t", self.target, "Enter"], check=True)


class CommandAdapter:
    def __init__(self, template: str, session_id: str):
        self.template = template
        self.session_id = session_id

    def target_id(self) -> str:
        return self.session_id

    def inject(self, message: dict[str, Any]) -> None:
        payload = json.loads(message["payload"])
        import shlex

        paths = message.get("_attachment_paths") or []
        text = payload.get("text", "")
        if paths:
            text += " Attachments: " + " ".join(paths)
        text = text.replace("\n", " ")
        cmd = self.template.format(
            message_id=message["message_id"],
            thread_id=message["thread_id"],
            from_agent=message["from_agent"],
            agent_id=message["to_agent"],
            session_id=self.session_id,
            text=text,
        )
        env = dict(os.environ)
        env.update({
            "AGENT_BUS_MESSAGE_ID": str(message["message_id"]),
            "AGENT_BUS_THREAD_ID": str(message["thread_id"]),
            "AGENT_BUS_FROM_AGENT": str(message["from_agent"]),
            "AGENT_BUS_TO_AGENT": str(message["to_agent"]),
            "AGENT_BUS_TEXT": text,
        })
        result = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True, timeout=120, env=env, input=text)
        if result.returncode != 0:
            raise AdapterError((result.stderr or result.stdout or "notify command failed").strip())


def materialize_attachments(message: dict[str, Any], conn: sqlite3.Connection) -> list[str]:
    try:
        raw = json.loads(message.get("attachments") or "[]")
    except (json.JSONDecodeError, TypeError):
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
                p = bus.blob_materialize(item, source_peer_name or "", peer)
                paths.append(str(p))
            elif item.get("origin"):
                paths.append(str(item["origin"]))
    message["_attachment_paths"] = paths
    return paths


def log(msg: str, agent_id: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{agent_id}] {msg}"
    print(line, flush=True)
    try:
        path = Path(f"/Users/chen/Developer/agent-bus/bus_agent_{agent_id}.log")
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def agent_row(conn: sqlite3.Connection, agent_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM agents WHERE agent_id=?", (agent_id,)).fetchone()


def verify_token(conn: sqlite3.Connection, agent_id: str) -> None:
    row = agent_row(conn, agent_id)
    if row is None:
        raise SystemExit(f"error: agent {agent_id} not registered")
    token = bus.load_token(agent_id)
    if token is None or bus.sha256_text(token) != row["token_hash"]:
        raise SystemExit(f"error: invalid token for {agent_id}")


def already_delivered(conn: sqlite3.Connection, agent_id: str, message_id: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM deliveries WHERE agent_id=? AND message_id=?", (agent_id, message_id)
        ).fetchone()
        is not None
    )


def record_delivery(conn: sqlite3.Connection, agent_id: str, message_id: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO deliveries(agent_id,message_id,delivered_at) VALUES (?,?,?)",
        (agent_id, message_id, time.time()),
    )


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
        last = min(g["last_digest_seq"] + 5, latest)
        rows = conn.execute(
            "SELECT seq,sender_agent,payload FROM group_messages WHERE group_id=? AND seq>=? AND seq<=? ORDER BY seq",
            (g["group_id"], first, last),
        ).fetchall()
        lines = [f"[group:{g['group_id']} topic:{g['topic']}] seq={first}-{latest} digest={len(rows)} of {latest - g['last_digest_seq']} unread"]
        for r in rows:
            try:
                text = json.loads(r["payload"]).get("text", "")
            except (json.JSONDecodeError, TypeError):
                text = str(r["payload"])
            text = text.replace("\\n", " ")[:160]
            lines.append(f"  seq {r['seq']} from {r['sender_agent']}: {text}")
        digest_text = "\\n".join(lines)
        message = {
            "message_id": f"group-digest-{g['group_id']}-{latest}",
            "from_agent": f"{g['group_id']}",
            "thread_id": g["group_id"],
            "payload": json.dumps({"text": digest_text}, ensure_ascii=False),
            "attachments": "[]",
            "_attachment_paths": [],
            "_group_id": g["group_id"],
        }
        conn.execute(
            """
            UPDATE group_read_state
            SET last_digest_seq=?, updated_at=?
            WHERE group_id=? AND agent_id=?
            """,
            (latest, time.time(), g["group_id"], agent_id),
        )
        conn.commit()
        return message
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="agent-bus per-agent notifier")
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--adapter", default=os.environ.get("AGENT_BUS_ADAPTER", DEFAULT_ADAPTER))
    parser.add_argument("--session-id", default=os.environ.get("AGENT_BUS_SESSION"))
    parser.add_argument("--tmux-target", default=os.environ.get("TMUX_TARGET"))
    parser.add_argument("--notify-cmd", default=os.environ.get("AGENT_NOTIFY_CMD", "printf '{message_id} {thread_id} {from_agent}\\n'"))
    parser.add_argument("--cli", default=os.environ.get("AGENT_BUS_CLI", DEFAULT_CLI))
    parser.add_argument("--cwd", default=os.environ.get("AGENT_BUS_CWD", "/Users/chen/Developer/agent-bus"))
    parser.add_argument("--poll", type=float, default=float(os.environ.get("AGENT_BUS_POLL", "1.0")))
    parser.add_argument("--heartbeat", type=float, default=float(os.environ.get("AGENT_BUS_HEARTBEAT", "5.0")))
    args = parser.parse_args()

    agent_id = args.agent_id
    adapter_kind = args.adapter
    session_id = args.session_id

    conn = bus.connect()
    verify_token(conn, agent_id)
    row = agent_row(conn, agent_id)
    if session_id is None:
        session_id = row["current_session_id"]
    conn.close()

    # Build adapter lazily after session known.
    adapter: Any
    if adapter_kind == "opencode":
        if not session_id:
            raise SystemExit("error: --session-id or registered session required for opencode adapter")
        adapter = OpencodeAdapter(session_id, args.cli, args.cwd)
    elif adapter_kind == "tmux":
        if not args.tmux_target:
            raise SystemExit("error: --tmux-target required for tmux adapter")
        adapter = TmuxAdapter(args.tmux_target)
    elif adapter_kind == "command":
        if not args.notify_cmd:
            args.notify_cmd = "printf '{message_id} {thread_id} {from_agent}\\n'"
        adapter = CommandAdapter(args.notify_cmd, session_id or agent_id)
    else:
        raise SystemExit(f"error: unknown adapter {adapter_kind}")

    last_heartbeat = 0.0
    adapter_session = getattr(adapter, "session_id", getattr(adapter, "target", ""))
    log(f"started adapter={adapter_kind} session={adapter_session}", agent_id)

    while True:
        try:
            conn = bus.connect()
            verify_token(conn, agent_id)
            if time.time() - last_heartbeat >= args.heartbeat:
                bus.cmd_heartbeat(
                    argparse.Namespace(agent_id=agent_id, session_id=session_id, endpoint=session_id)
                )
                last_heartbeat = time.time()
            # sweep expired and stale agents/tasks
            bus.sweep_agents(conn)
            bus.sweep_nudges(conn)
            bus.sweep_expired(conn)

            rows = bus.fetch_inbox(conn if False else None, agent_id, "undelivered", 25) if False else None
            # direct query simpler
            msgs = conn.execute(
                """
                SELECT * FROM messages
                WHERE to_agent=? AND acked_at IS NULL
                  AND status IN ('queued','delivered')
                  AND (expires_at IS NULL OR expires_at > ?)
                ORDER BY created_at ASC, message_id ASC
                LIMIT 25
                """,
                (agent_id, time.time()),
            ).fetchall()

            # Optional group digest (one unseen digest per poll cycle).
            try:
                digest_message = poll_group_digest(agent_id, conn)
                if digest_message is not None:
                    try:
                        adapter.inject(digest_message)
                        log(f"group digest delivered for {digest_message.get('_group_id')}", agent_id)
                    except Exception as exc:
                        log(f"group digest failed: {exc}", agent_id)
            except Exception as exc:
                log(f"group digest poll error: {exc}", agent_id)

            for m in msgs:
                msg = bus.row_to_dict(m)
                if already_delivered(conn, agent_id, msg["message_id"]):
                    # Already injected earlier; just ack.
                    bus.cmd_ack(argparse.Namespace(agent_id=agent_id, message_id=msg["message_id"]))
                    log(f"already delivered, re-acked {msg['message_id']}", agent_id)
                    continue
                try:
                    materialize_attachments(msg, conn)
                    adapter.inject(msg)
                    record_delivery(conn, agent_id, msg["message_id"])
                    # refresh session binding (in case adapter target changed)
                    conn.execute(
                        "UPDATE messages SET status='delivered', delivered_at=COALESCE(delivered_at,?), retry_count=retry_count WHERE message_id=?",
                        (time.time(), msg["message_id"]),
                    )
                    conn.commit()
                    bus.cmd_ack(argparse.Namespace(agent_id=agent_id, message_id=msg["message_id"]))
                    log(f"delivered+acked {msg['message_id']} from {msg['from_agent']}", agent_id)
                except Exception as exc:
                    conn.execute(
                        "UPDATE messages SET retry_count=retry_count+1, last_error=? WHERE message_id=?",
                        (str(exc)[:500], msg["message_id"]),
                    )
                    conn.commit()
                    row2 = conn.execute("SELECT retry_count FROM messages WHERE message_id=?", (msg["message_id"],)).fetchone()
                    if row2 and row2["retry_count"] >= 5:
                        conn.execute("UPDATE messages SET status='dead' WHERE message_id=?", (msg["message_id"],))
                        conn.commit()
                        log(f"dead-letter {msg['message_id']}: {exc}", agent_id)
                    else:
                        log(f"delivery failed {msg['message_id']}: {exc}", agent_id)
            conn.close()
        except Exception as exc:
            log(f"poll error: {exc}", agent_id)
        time.sleep(args.poll)


if __name__ == "__main__":
    raise SystemExit(main())