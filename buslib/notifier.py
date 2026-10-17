#!/usr/bin/env python3
"""Per-agent notifier for agent-bus v0.

Polls the shared SQLite bus for undelivered messages addressed to this agent.
On receipt it injects them into the native agent through an adapter, records
delivery for idempotency, then acks.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path

from .adapters import Adapter, AdapterError, CommandAdapter, OpencodeAdapter, TmuxAdapter
from .agents import cmd_heartbeat, resolve_opencode_cli, sweep_agents
from .config import load_token, row_to_dict, sha256_text
from .db import connect
from .delivery import commit_group_digest, materialize_attachments, poll_group_digest
from .maintenance import sweep_expired, sweep_nudges
from .messaging import cmd_ack, fetch_inbox
from .remote import sweep_outbox
from .validation import validate_id


def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


DEFAULT_CLI = resolve_opencode_cli()  # version-pinned path broke every notifier when the app updated
DEFAULT_ADAPTER = "opencode"


def log(msg: str, agent_id: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{agent_id}] {msg}"
    print(line, flush=True)


def agent_row(conn: sqlite3.Connection, agent_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM agents WHERE agent_id=?", (agent_id,)).fetchone()


def verify_token(conn: sqlite3.Connection, agent_id: str) -> None:
    row = agent_row(conn, agent_id)
    if row is None:
        raise SystemExit(f"error: agent {agent_id} not registered")
    token = load_token(agent_id)
    if token is None or sha256_text(token) != row["token_hash"]:
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
    validate_id("agent_id", agent_id)
    adapter_kind = args.adapter
    session_id = args.session_id

    conn = connect()
    verify_token(conn, agent_id)
    row = agent_row(conn, agent_id)
    if session_id is None:
        session_id = row["current_session_id"]
    conn.close()

    # Build adapter lazily after session known.
    adapter: Adapter
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
        # cmd_ack raises SystemExit (e.g. message vanished mid-flight);
        # SystemExit is not caught by `except Exception`, so an unguarded ack
        # used to kill the whole notifier process.
        try:
            cmd_ack(argparse.Namespace(agent_id=agent_id, message_id=message_id, quiet=True))
        except SystemExit as exc:
            log(f"ack failed for {message_id}: {exc}", agent_id)

    while True:
        conn = None
        try:
            conn = connect()
            verify_token(conn, agent_id)
            if time.time() - last_heartbeat >= args.heartbeat:
                # Heartbeat WITHOUT rebinding when --session-id was not given
                # at start: passing the startup session every 5s used to
                # clobber a fresh `register --session-id` forever after.
                hb_session = session_id if explicit_session else None
                cmd_heartbeat(
                    argparse.Namespace(agent_id=agent_id, session_id=hb_session, endpoint=hb_session, quiet=True)
                )
                last_heartbeat = time.time()
            # Sweeps write; running them on every 1s poll made every notifier
            # contend for SQLite's single writer. 10s is plenty for expiry and
            # ask reminders, and fetch_inbox already filters expired rows.
            if time.time() - last_sweep >= args.sweep:
                sweep_agents(conn)
                sweep_nudges(conn)
                sweep_expired(conn)
                last_sweep = time.time()

            if args.outbox_flush > 0 and time.time() - last_outbox >= args.outbox_flush:
                sent, failed = sweep_outbox(conn, limit=3, timeout=3)
                if sent or failed:
                    log(f"outbox flush: sent={sent} failed={failed}", agent_id)
                last_outbox = time.time()

            msgs = [row_to_dict(r) for r in fetch_inbox(conn, agent_id, "undelivered", 25)]

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


__all__ = ["main"]


if __name__ == "__main__":
    raise SystemExit(main())
