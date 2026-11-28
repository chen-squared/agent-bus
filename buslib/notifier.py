#!/usr/bin/env python3
"""Per-agent notifier for agent-bus v0.

Polls the shared SQLite bus for undelivered messages addressed to this agent.
On receipt it injects them into the native agent through an adapter, records
delivery for idempotency, then acks.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing

from .adapters import (
    Adapter,
    AdapterError,
    AntigravityAdapter,
    CodexAdapter,
    CommandAdapter,
    DeliveryUnknownError,
    OpencodeAdapter,
    TmuxAdapter,
    TransientAdapterError,
    is_transient_error,
)
from .agents import cmd_heartbeat, resolve_opencode_cli, sweep_agents
from .config import BUS_HOME, load_token, row_to_dict, sha256_text
from .db import connect, db_path, tx
from .delivery import commit_group_digest, materialize_attachments, poll_group_digest
from .maintenance import sweep_expired, sweep_nudges
from .messaging import fetch_inbox
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


def _ack_delivery(conn: sqlite3.Connection, agent_id: str, message_id: str) -> None:
    now = time.time()
    conn.execute(
        "UPDATE messages SET status='acked', acked_at=?, delivered_at=COALESCE(delivered_at,?), last_error=NULL"
        " WHERE to_agent=? AND message_id=? AND status IN ('queued','delivered') AND acked_at IS NULL",
        (now, now, agent_id, message_id),
    )


def _clear_attempt(conn: sqlite3.Connection, agent_id: str, message_id: str) -> None:
    conn.execute("DELETE FROM delivery_attempts WHERE agent_id=? AND message_id=?", (agent_id, message_id))


def _claim_attempt(conn: sqlite3.Connection, agent_id: str, message: dict) -> bool:
    """Record the attempt before dispatch. A NULL retry time means it is in flight."""
    message_id = message["message_id"]
    group_id = message.get("_group_id")
    with tx(conn):
        if group_id is not None:
            member = conn.execute(
                "SELECT r.last_read_seq FROM group_members m JOIN groups g USING (group_id)"
                " LEFT JOIN group_read_state r ON r.group_id=m.group_id AND r.agent_id=m.agent_id"
                " WHERE m.agent_id=? AND m.group_id=? AND m.status='active' AND g.status='active'",
                (agent_id, group_id),
            ).fetchone()
            if member is None or (member["last_read_seq"] or 0) >= message["_digest_seq"]:
                return False
            # The digest ID can change while new group messages arrive.
            previous = conn.execute(
                "SELECT retry_count FROM delivery_attempts WHERE agent_id=? AND group_id=? AND retry_after IS NOT NULL",
                (agent_id, group_id),
            ).fetchone()
            conn.execute(
                "DELETE FROM delivery_attempts WHERE agent_id=? AND group_id=? AND retry_after IS NOT NULL",
                (agent_id, group_id),
            )
            conn.execute(
                "INSERT INTO delivery_attempts(agent_id,message_id,group_id,digest,retry_count) VALUES (?,?,?,?,?)",
                (agent_id, message_id, group_id, json.dumps(message), previous["retry_count"] if previous else 0),
            )
            return True
        now = time.time()
        return conn.execute(
            "INSERT INTO delivery_attempts(agent_id,message_id)"
            " SELECT ?,message_id FROM messages WHERE to_agent=? AND message_id=?"
            " AND status IN ('queued','delivered') AND acked_at IS NULL AND (expires_at IS NULL OR expires_at > ?)"
            " ON CONFLICT(agent_id,message_id) DO UPDATE SET retry_after=NULL"
            " WHERE delivery_attempts.retry_after <= ?",
            (agent_id, agent_id, message_id, now, now),
        ).rowcount == 1


def _record_uncertain(
    conn: sqlite3.Connection, agent_id: str, message_id: str, digest: dict | None, detail: str,
) -> None:
    error = f"delivery outcome unknown: {detail}"[:500]
    with tx(conn):
        if digest is None:
            conn.execute(
                "UPDATE messages SET status='dead', last_error=? WHERE to_agent=? AND message_id=?"
                " AND status IN ('queued','delivered') AND acked_at IS NULL",
                (error, agent_id, message_id),
            )
        else:
            retry_count = conn.execute(
                "SELECT retry_count FROM delivery_attempts WHERE agent_id=? AND message_id=?",
                (agent_id, message_id),
            ).fetchone()["retry_count"]
            commit_group_digest(conn, agent_id, digest, last_error=error, retry_count=retry_count)
        _clear_attempt(conn, agent_id, message_id)
    log(f"dead-letter {message_id}: {error}", agent_id)


def _recover_attempts(conn: sqlite3.Connection, agent_id: str) -> None:
    # Another process cannot be injecting for this agent while we hold its notifier lock.
    attempts = conn.execute(
        "SELECT message_id,digest FROM delivery_attempts WHERE agent_id=? AND retry_after IS NULL",
        (agent_id,),
    ).fetchall()
    for row in attempts:
        digest = json.loads(row["digest"]) if row["digest"] is not None else None
        _record_uncertain(conn, agent_id, row["message_id"], digest, "notifier stopped before recording acknowledgement")


def _record_group_failure(
    conn: sqlite3.Connection, agent_id: str, message: dict, error: str, *, retry_after: float, transient: bool,
) -> None:
    message_id = message["message_id"]
    with tx(conn):
        row = conn.execute(
            "SELECT retry_count FROM delivery_attempts WHERE agent_id=? AND message_id=?",
            (agent_id, message_id),
        ).fetchone()
        retry_count = row["retry_count"] + (0 if transient else 1)
        if retry_count >= 5:
            commit_group_digest(conn, agent_id, message, last_error=error, retry_count=retry_count)
            _clear_attempt(conn, agent_id, message_id)
            label = "dead-letter"
        else:
            conn.execute(
                "UPDATE delivery_attempts SET retry_after=?, retry_count=? WHERE agent_id=? AND message_id=?",
                (retry_after, retry_count, agent_id, message_id),
            )
            label = "delivery deferred"
    log(f"{label} {message_id}: {error}", agent_id)


def _record_message_failure(
    conn: sqlite3.Connection, agent_id: str, message: dict, error: str, retry_after: float | None,
) -> None:
    message_id = message["message_id"]
    with tx(conn):
        if retry_after is None:
            _clear_attempt(conn, agent_id, message_id)
            conn.execute(
                "UPDATE messages SET retry_count=retry_count+1, last_error=?,"
                " status=CASE WHEN retry_count+1 >= 5 THEN 'dead' ELSE status END"
                " WHERE to_agent=? AND message_id=? AND status IN ('queued','delivered') AND acked_at IS NULL",
                (error, agent_id, message_id),
            )
            row = conn.execute("SELECT status FROM messages WHERE message_id=?", (message_id,)).fetchone()
            label = "dead-letter" if row and row["status"] == "dead" else "delivery failed"
        else:
            conn.execute(
                "UPDATE delivery_attempts SET retry_after=? WHERE agent_id=? AND message_id=?",
                (retry_after, agent_id, message_id),
            )
            conn.execute(
                "UPDATE messages SET last_error=? WHERE to_agent=? AND message_id=?"
                " AND status IN ('queued','delivered') AND acked_at IS NULL",
                (error, agent_id, message_id),
            )
            label = "delivery deferred"
    log(f"{label} {message_id}: {error}", agent_id)


def main(*, on_start: Callable[[], None] | None = None) -> int:
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
    parser.add_argument("--cli", default=os.environ.get("AGENT_BUS_CLI"))
    parser.add_argument("--codex-remote", default=os.environ.get("AGENT_BUS_CODEX_REMOTE"))
    parser.add_argument("--antigravity-ls-address", default=None)
    parser.add_argument("--cwd", default=os.environ.get("AGENT_BUS_CWD", os.getcwd()), help="working directory for the adapter (default: current directory)")
    parser.add_argument("--poll", type=float, default=_env_float("AGENT_BUS_POLL", 1.0))
    parser.add_argument("--heartbeat", type=float, default=_env_float("AGENT_BUS_HEARTBEAT", 5.0))
    parser.add_argument("--sweep", type=float, default=_env_float("AGENT_BUS_SWEEP_INTERVAL", 10.0))
    parser.add_argument("--outbox-flush", type=float, default=_env_float("AGENT_BUS_OUTBOX_FLUSH", 30.0))
    parser.add_argument("--check-adapter", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.sweep = max(1.0, args.sweep)

    validate_id("agent_id", args.agent_id)
    if args.check_adapter:
        _build_adapter(args, args.session_id)
        return 0
    # The delivery receipt is written after injection, so parallel pollers
    # could both see the same pending row and inject it before either records it.
    db = db_path()
    db.parent.mkdir(parents=True, exist_ok=True)
    lock_name = f".agent-bus-notifier-{sha256_text(f'{db.name}/{args.agent_id}')}.lock"
    with (db.parent / lock_name).open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"error: a notifier for {args.agent_id} is already running on {db}") from None
        return _run(args, on_start)


def _build_adapter(args: argparse.Namespace, session_id: str | None) -> Adapter:
    adapter_kind = args.adapter
    if adapter_kind == "opencode":
        if not session_id:
            raise SystemExit("error: --session-id or registered session required for opencode adapter")
        return OpencodeAdapter(session_id, args.cli, args.cwd)
    if adapter_kind == "codex":
        if not session_id:
            raise SystemExit("error: --session-id or registered thread UUID required for codex adapter")
        return CodexAdapter(session_id, args.cwd, args.codex_remote)
    if adapter_kind == "antigravity":
        if not session_id:
            raise SystemExit("error: --session-id or registered conversation UUID required for antigravity adapter")
        return AntigravityAdapter(session_id, args.cli, args.cwd, args.antigravity_ls_address)
    if adapter_kind == "tmux":
        if not args.tmux_target:
            raise SystemExit("error: --tmux-target required for tmux adapter")
        return TmuxAdapter(args.tmux_target)
    if adapter_kind == "command":
        command = args.notify_cmd or "printf '{message_id} {thread_id} {from_agent}\\n'"
        return CommandAdapter(command, session_id or args.agent_id, args.allow_unsafe_text)
    raise SystemExit(f"error: unknown adapter {adapter_kind}")


def _run(args: argparse.Namespace, on_start: Callable[[], None] | None) -> int:
    agent_id = args.agent_id
    session_id = args.session_id
    with closing(connect()) as conn:
        verify_token(conn, agent_id)
        if session_id is None:
            session_id = agent_row(conn, agent_id)["current_session_id"]

    adapter = _build_adapter(args, session_id)
    if args.adapter == "command" and args.allow_unsafe_text and "{text}" in (args.notify_cmd or ""):
        log("warning: unsafe {text} interpolation is enabled for the notify command", agent_id)

    if on_start is not None:
        on_start()
    last_heartbeat = 0.0
    last_sweep = 0.0
    last_outbox = 0.0
    transient_streak = 0
    transient_backoff_until = 0.0
    max_transient_backoff = max(args.poll, _env_float("AGENT_BUS_TRANSIENT_BACKOFF_MAX", 15.0))
    adapter_session = getattr(adapter, "session_id", getattr(adapter, "target", ""))
    explicit_session = args.session_id is not None
    log(f"started adapter={args.adapter} session={adapter_session} home={BUS_HOME} db={db_path()}", agent_id)

    def defer_transient(
        conn: sqlite3.Connection,
        exc: TransientAdapterError,
        message: dict,
    ) -> None:
        nonlocal transient_streak, transient_backoff_until
        transient_streak += 1
        base = max(args.poll, 0.05)
        delay = min(max_transient_backoff, base * (2 ** min(transient_streak - 1, 6)))
        transient_backoff_until = time.monotonic() + delay
        err_text = str(exc)[:500]
        message_id = message["message_id"]
        with tx(conn):
            if "_group_id" in message:
                conn.execute(
                    "UPDATE delivery_attempts SET retry_after=? WHERE agent_id=? AND message_id=?",
                    (time.time() + delay, agent_id, message_id),
                )
            else:
                _clear_attempt(conn, agent_id, message_id)
                if err_text != message.get("last_error"):
                    conn.execute(
                        "UPDATE messages SET last_error=? WHERE to_agent=? AND message_id=?"
                        " AND status IN ('queued','delivered') AND acked_at IS NULL",
                        (err_text, agent_id, message_id),
                    )
        reason = "busy" if "busy" in err_text.lower() else "unavailable"
        log(
            f"target {reason}, deferring {message_id} for {delay:.2f}s; retry_count untouched: {err_text}",
            agent_id,
        )

    def mark_recovered() -> None:
        nonlocal transient_streak, transient_backoff_until
        if transient_streak > 0:
            log(f"target recovered after {transient_streak} deferred attempt(s)", agent_id)
            transient_streak = 0
            transient_backoff_until = 0.0

    def deliver(conn: sqlite3.Connection, message: dict) -> bool:
        message_id = message["message_id"]
        is_digest = "_group_id" in message
        if not is_digest:
            if already_delivered(conn, agent_id, message_id):
                with tx(conn):
                    _ack_delivery(conn, agent_id, message_id)
                    _clear_attempt(conn, agent_id, message_id)
                return True
            try:
                materialize_attachments(message, conn)
            except Exception as exc:
                _record_message_failure(conn, agent_id, message, str(exc)[:500], None)
                return True
        if not _claim_attempt(conn, agent_id, message):
            return True

        try:
            adapter.inject(message)
        except TransientAdapterError as exc:
            defer_transient(conn, exc, message)
            return False
        except DeliveryUnknownError as exc:
            _record_uncertain(conn, agent_id, message_id, message if is_digest else None, str(exc))
        except AdapterError as exc:
            transient = is_transient_error(exc)
            retry_after = time.time() + max(args.poll, 1.0)
            if is_digest:
                _record_group_failure(conn, agent_id, message, str(exc)[:500], retry_after=retry_after, transient=transient)
            else:
                _record_message_failure(conn, agent_id, message, str(exc)[:500], retry_after if transient else None)
        except Exception as exc:
            _record_uncertain(conn, agent_id, message_id, message if is_digest else None, str(exc))
        else:
            with tx(conn):
                if is_digest:
                    commit_group_digest(conn, agent_id, message)
                else:
                    record_delivery(conn, agent_id, message_id)
                    _ack_delivery(conn, agent_id, message_id)
                _clear_attempt(conn, agent_id, message_id)
            mark_recovered()
            label = f"group digest delivered for {message['_group_id']}" if is_digest else f"delivered+acked {message_id} from {message['from_agent']}"
            log(label, agent_id)
        return True

    while True:
        conn = None
        try:
            conn = connect()
            verify_token(conn, agent_id)
            _recover_attempts(conn, agent_id)
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

            if time.monotonic() >= transient_backoff_until:
                msgs = [row_to_dict(r) for r in fetch_inbox(conn, agent_id, "undelivered", 25, ready_only=True)]
                try:
                    digest_message = poll_group_digest(agent_id, conn)
                    if digest_message is not None:
                        msgs.insert(0, digest_message)
                except Exception as exc:
                    log(f"group digest poll error: {exc}", agent_id)
                for message in msgs:
                    if not deliver(conn, message):
                        break
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
