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

def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


DEFAULT_CLI = bus.resolve_opencode_cli()  # version-pinned path broke every notifier when the app updated
DEFAULT_ADAPTER = "opencode"


class AdapterError(RuntimeError):
    pass


class OpencodeAdapter:
    def __init__(self, session_id: str, cli: str, cwd: str):
        self.session_id = session_id
        self.cli = cli
        self.cwd = cwd

    def inject(self, message: dict[str, Any]) -> None:
        payload = json.loads(message["payload"])
        text = payload.get("text", "")
        paths = message.get("_attachment_paths") or []
        rendered = f"[bus:{message['message_id']} from:{message['from_agent']} thread:{message['thread_id']}] {text}"
        if paths:
            rendered += "\nAttachments:\n" + "\n".join(paths)
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

    def is_idle(self) -> bool:
        out = subprocess.run(
            ["tmux", "capture-pane", "-t", self.target, "-p"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if out.returncode != 0:
            return True
        first = out.stdout
        time.sleep(0.3)
        out2 = subprocess.run(
            ["tmux", "capture-pane", "-t", self.target, "-p"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return out2.returncode == 0 and out2.stdout == first

    def inject(self, message: dict[str, Any]) -> None:
        if not self.is_idle():
            raise AdapterError("target tmux pane busy")
        payload = json.loads(message["payload"])
        paths = message.get("_attachment_paths") or []
        text = f"[bus:{message['message_id']} from:{message['from_agent']} thread:{message['thread_id']}] {payload.get('text','')}"
        if paths:
            text += " Attachments: " + " ".join(paths)
        # send-keys -l delivers the string literally, and the trailing Enter
        # submits it: every embedded newline would execute as its own command
        # line in a shell pane. Flatten to a single line.
        text = text.replace("\r", " ").replace("\n", " ")
        subprocess.run(["tmux", "send-keys", "-t", self.target, "-l", text], check=True, timeout=10)
        time.sleep(0.1)
        subprocess.run(["tmux", "send-keys", "-t", self.target, "Enter"], check=True, timeout=10)


class CommandAdapter:
    def __init__(self, template: str, session_id: str, allow_unsafe_text: bool = False):
        self.template = template
        self.session_id = session_id
        # {text} is interpolated into a shell command. Even shlex.quote cannot
        # make that safe when the template itself wraps the placeholder in
        # quotes, and a remote peer controls the text. The supported path is
        # AGENT_BUS_TEXT / stdin; keep {text} behind an explicit opt-in.
        if "{text}" in template and not allow_unsafe_text:
            raise SystemExit(
                "error: notify template contains {text}, which is shell-injection-prone; "
                "use {message_id}/{thread_id}/{from_agent} plus $AGENT_BUS_TEXT or stdin, "
                "or set AGENT_BUS_ALLOW_UNSAFE_TEXT=1 to accept the risk"
            )

    def inject(self, message: dict[str, Any]) -> None:
        payload = json.loads(message["payload"])
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
                p = bus.blob_materialize(item, source_peer_name or "", peer)
                paths.append(str(p))
            elif item.get("origin"):
                paths.append(str(item["origin"]))
    message["_attachment_paths"] = paths
    return paths


def log(msg: str, agent_id: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{agent_id}] {msg}"
    print(line, flush=True)


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
                text = text[:500] + "…+" + str(len(text) - 500) + " chars (full: bus group messages --group-id " + g["group_id"] + " --agent-id " + agent_id + ")"
            lines.append(f"  seq {r['seq']} from {r['sender_agent']}: {text}")
        digest_text = "\n".join(lines)
        message = {
            # Keyed on the last seq actually rendered, not on latest. Two digest
            # rounds inside the same latest window would otherwise collide on
            # this id, and commit_group_digest's INSERT OR IGNORE silently drops
            # the second row: the text still reaches the adapter, but
            # `bus message show` on that pointer returns not found.
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
    bus.ensure_thread(conn, message["thread_id"], agent_id, [agent_id])
    conn.execute(
        """
        INSERT OR IGNORE INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,
            message_type,urgency,payload,attachments,ttl_seconds,expires_at,status,delivered_at,acked_at,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            message["message_id"],
            message["thread_id"],
            None,
            agent_id,
            message["to_agent"],
            "system",
            "normal",
            json.dumps(payload, ensure_ascii=False),
            "[]",
            86400,
            now_ts + 86400,
            "acked",
            now_ts,
            now_ts,
            now_ts,
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


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="agent-bus per-agent notifier", allow_abbrev=False)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--adapter", default=os.environ.get("AGENT_BUS_ADAPTER", DEFAULT_ADAPTER))
    parser.add_argument("--session-id", default=os.environ.get("AGENT_BUS_SESSION"))
    parser.add_argument("--tmux-target", default=os.environ.get("TMUX_TARGET"))
    parser.add_argument("--notify-cmd", default=os.environ.get("AGENT_NOTIFY_CMD", "printf '{message_id} {thread_id} {from_agent}\\n'"))
    parser.add_argument(
        "--allow-unsafe-text",
        action="store_true",
        default=os.environ.get("AGENT_BUS_ALLOW_UNSAFE_TEXT") == "1",
        help="allow {text} interpolation into the shell notify template (unsafe)",
    )
    parser.add_argument("--cli", default=os.environ.get("AGENT_BUS_CLI", DEFAULT_CLI))
    parser.add_argument("--cwd", default=os.environ.get("AGENT_BUS_CWD", str(Path(__file__).resolve().parent)))
    parser.add_argument("--poll", type=float, default=_env_float("AGENT_BUS_POLL", 1.0))
    parser.add_argument("--heartbeat", type=float, default=_env_float("AGENT_BUS_HEARTBEAT", 5.0))
    parser.add_argument("--sweep", type=float, default=_env_float("AGENT_BUS_SWEEP_INTERVAL", 10.0))
    parser.add_argument("--outbox-flush", type=float, default=_env_float("AGENT_BUS_OUTBOX_FLUSH", 30.0))
    args = parser.parse_args()
    args.sweep = max(1.0, args.sweep)

    agent_id = args.agent_id
    bus.validate_id("agent_id", agent_id)
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
        adapter = CommandAdapter(args.notify_cmd, session_id or agent_id, args.allow_unsafe_text)
        if args.allow_unsafe_text and "{text}" in args.notify_cmd:
            log("warning: unsafe {text} interpolation is enabled for the notify command", agent_id)
    else:
        raise SystemExit(f"error: unknown adapter {adapter_kind}")

    last_heartbeat = 0.0
    last_sweep = 0.0
    last_outbox = 0.0
    adapter_session = getattr(adapter, "session_id", getattr(adapter, "target", ""))
    explicit_session = args.session_id is not None
    log(f"started adapter={adapter_kind} session={adapter_session}", agent_id)

    def safe_ack(message_id: str) -> None:
        # bus.cmd_ack raises SystemExit (e.g. message vanished mid-flight);
        # SystemExit is not caught by `except Exception`, so an unguarded ack
        # used to kill the whole notifier process.
        try:
            bus.cmd_ack(argparse.Namespace(agent_id=agent_id, message_id=message_id))
        except SystemExit as exc:
            log(f"ack failed for {message_id}: {exc}", agent_id)

    while True:
        conn = None
        try:
            conn = bus.connect()
            verify_token(conn, agent_id)
            if time.time() - last_heartbeat >= args.heartbeat:
                # Heartbeat WITHOUT rebinding when --session-id was not given
                # at start: passing the startup session every 5s used to
                # clobber a fresh `register --session-id` forever after.
                hb_session = session_id if explicit_session else None
                bus.cmd_heartbeat(
                    argparse.Namespace(agent_id=agent_id, session_id=hb_session, endpoint=hb_session)
                )
                last_heartbeat = time.time()
            # Sweeps write; running them on every 1s poll made every notifier
            # contend for SQLite's single writer. 10s is plenty for expiry and
            # ask reminders, and fetch_inbox already filters expired rows.
            if time.time() - last_sweep >= args.sweep:
                bus.sweep_agents(conn)
                bus.sweep_nudges(conn)
                bus.sweep_expired(conn)
                last_sweep = time.time()

            if args.outbox_flush > 0 and time.time() - last_outbox >= args.outbox_flush:
                sent, failed = bus.sweep_outbox(conn, limit=3, timeout=3)
                if sent or failed:
                    log(f"outbox flush: sent={sent} failed={failed}", agent_id)
                last_outbox = time.time()

            msgs = [bus.row_to_dict(r) for r in bus.fetch_inbox(conn, agent_id, "undelivered", 25)]

            # Optional group digest (one unseen digest per poll cycle).
            try:
                digest_message = poll_group_digest(agent_id, conn)
                if digest_message is not None:
                    try:
                        adapter.inject(digest_message)
                        commit_group_digest(conn, agent_id, digest_message)
                        log(f"group digest delivered for {digest_message.get('_group_id')}", agent_id)
                    except Exception as exc:
                        log(f"group digest failed: {exc}", agent_id)
            except Exception as exc:
                log(f"group digest poll error: {exc}", agent_id)

            for msg in msgs:
                if already_delivered(conn, agent_id, msg["message_id"]):
                    # Already injected earlier; just ack.
                    safe_ack(msg["message_id"])
                    log(f"already delivered, re-acked {msg['message_id']}", agent_id)
                    continue
                try:
                    materialize_attachments(msg, conn)
                    adapter.inject(msg)
                    record_delivery(conn, agent_id, msg["message_id"])
                    conn.execute(
                        "UPDATE messages SET status='delivered', delivered_at=COALESCE(delivered_at,?) WHERE message_id=?",
                        (time.time(), msg["message_id"]),
                    )
                    conn.commit()
                    safe_ack(msg["message_id"])
                    log(f"delivered+acked {msg['message_id']} from {msg['from_agent']}", agent_id)
                except AdapterError as exc:
                    if "busy" in str(exc).lower():
                        log(f"target busy, deferring {msg['message_id']}; retry_count untouched", agent_id)
                    else:
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
        except Exception as exc:
            log(f"poll error: {exc}", agent_id)
        finally:
            # conn.close() used to sit at the end of the try body: any
            # exception between connect and there leaked the connection.
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        time.sleep(args.poll)


if __name__ == "__main__":
    raise SystemExit(main())