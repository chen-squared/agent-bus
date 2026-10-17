"""SQLite connection, schema migrations, transactions, and authentication."""

from __future__ import annotations

import argparse
import contextlib
import os
import sqlite3
from pathlib import Path

from .config import (
    DEFAULT_DB, chmod_private, ensure_home, home_info, json_dumps, load_token,
    machine_id, sha256_text,
)
from .validation import validate_id


def db_path() -> Path:
    return Path(os.environ.get("AGENT_BUS_DB", DEFAULT_DB)).expanduser().resolve()


def connect() -> sqlite3.Connection:
    p = db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    init_schema(conn)
    chmod_private(p)
    chmod_private(Path(str(p) + "-wal"))
    chmod_private(Path(str(p) + "-shm"))
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS agents (
            agent_id TEXT PRIMARY KEY,
            agent_type TEXT NOT NULL,
            display_name TEXT,
            model_tier TEXT,
            capabilities TEXT NOT NULL DEFAULT '[]',
            schema_version INTEGER NOT NULL DEFAULT 1,
            token_hash TEXT NOT NULL,
            current_session_id TEXT,
            endpoint TEXT,
            machine_id TEXT NOT NULL DEFAULT 'unknown',
            status TEXT NOT NULL DEFAULT 'registered',
            last_heartbeat REAL,
            metadata TEXT NOT NULL DEFAULT '{}',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS threads (
            thread_id TEXT PRIMARY KEY,
            topic TEXT,
            participants TEXT NOT NULL DEFAULT '[]',
            created_by TEXT,
            created_at REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'open'
        );
        CREATE TABLE IF NOT EXISTS messages (
            message_id TEXT PRIMARY KEY,
            thread_id TEXT,
            idempotency_key TEXT UNIQUE,
            from_agent TEXT NOT NULL,
            to_agent TEXT NOT NULL,
            message_type TEXT NOT NULL,
            urgency TEXT NOT NULL DEFAULT 'normal',
            payload TEXT NOT NULL,
            attachments TEXT NOT NULL DEFAULT '[]',
            ttl_seconds INTEGER,
            expires_at REAL,
            expects_response INTEGER NOT NULL DEFAULT 0,
            response_deadline REAL,
            nudge_count INTEGER NOT NULL DEFAULT 0,
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            status TEXT NOT NULL DEFAULT 'queued',
            delivered_at REAL,
            acked_at REAL,
            answer_to_message_id TEXT,
            correlation_id TEXT,
            created_at REAL NOT NULL,
            FOREIGN KEY(thread_id) REFERENCES threads(thread_id)
        );
        CREATE TABLE IF NOT EXISTS deliveries (
            agent_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            delivered_at REAL NOT NULL,
            PRIMARY KEY(agent_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS peers (
            peer_name TEXT PRIMARY KEY,
            host TEXT,
            base_url TEXT NOT NULL,
            auth_token TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'configured',
            last_handoff_at REAL,
            last_error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS groups (
            group_id TEXT PRIMARY KEY,
            topic TEXT,
            home_machine TEXT NOT NULL DEFAULT 'unknown',
            mode TEXT NOT NULL DEFAULT 'catch-up-gated',
            max_members INTEGER NOT NULL DEFAULT 50,
            rate_seconds REAL NOT NULL DEFAULT 2,
            ceiling_per_minute INTEGER NOT NULL DEFAULT 120,
            backlog_threshold INTEGER NOT NULL DEFAULT 50,
            status TEXT NOT NULL DEFAULT 'active',
            created_by TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS group_members (
            group_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'member',
            rate_seconds REAL NOT NULL DEFAULT 0,
            last_sent_at REAL,
            status TEXT NOT NULL DEFAULT 'active',
            joined_at REAL NOT NULL,
            PRIMARY KEY (group_id, agent_id)
        );
        CREATE TABLE IF NOT EXISTS group_messages (
            message_id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            sender_agent TEXT NOT NULL,
            message_type TEXT NOT NULL DEFAULT 'text',
            urgency TEXT NOT NULL DEFAULT 'normal',
            payload TEXT NOT NULL,
            attachments TEXT NOT NULL DEFAULT '[]',
            expires_at REAL,
            created_at REAL NOT NULL,
            UNIQUE (group_id, seq)
        );
        CREATE TABLE IF NOT EXISTS group_read_state (
            group_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            last_read_seq INTEGER NOT NULL DEFAULT 0,
            last_digest_seq INTEGER NOT NULL DEFAULT 0,
            last_served_seq INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL,
            PRIMARY KEY (group_id, agent_id)
        );
        CREATE TABLE IF NOT EXISTS outbox (
            outbox_id TEXT PRIMARY KEY,
            peer_name TEXT NOT NULL,
            message_id TEXT NOT NULL,
            from_agent TEXT NOT NULL,
            to_remote TEXT NOT NULL,
            thread_id TEXT,
            payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 5,
            last_error TEXT,
            receipt TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            delivered_at REAL,
            FOREIGN KEY(peer_name) REFERENCES peers(peer_name)
        );
        CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status,created_at);
        CREATE INDEX IF NOT EXISTS idx_messages_to_status ON messages(to_agent,status,created_at);
        CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id,created_at);
        CREATE INDEX IF NOT EXISTS idx_messages_expires ON messages(expires_at,status);
        """
    )

    # Forward-compatible column additions for older databases.
    def columns(table: str) -> set[str]:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    if "machine_id" not in columns("agents"):
        conn.execute("ALTER TABLE agents ADD COLUMN machine_id TEXT NOT NULL DEFAULT 'unknown'")
    # Only fill in unknown machine_ids, and only when any exist: this runs on
    # every connect(), and an unconditional UPDATE took a write lock on every
    # read-only command and every notifier poll tick.
    if conn.execute("SELECT 1 FROM agents WHERE machine_id IS NULL OR machine_id='unknown' OR machine_id=''").fetchone():
        conn.execute("UPDATE agents SET machine_id=? WHERE machine_id IS NULL OR machine_id='unknown' OR machine_id=''", (machine_id(),))
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='group_read_state'").fetchone():
        for col, ddl in (
            ("last_digest_seq", "ALTER TABLE group_read_state ADD COLUMN last_digest_seq INTEGER NOT NULL DEFAULT 0"),
            ("last_served_seq", "ALTER TABLE group_read_state ADD COLUMN last_served_seq INTEGER NOT NULL DEFAULT 0"),
        ):
            if col not in columns("group_read_state"):
                conn.execute(ddl)
    for col, ddl in (
        ("expects_response", "ALTER TABLE messages ADD COLUMN expects_response INTEGER NOT NULL DEFAULT 0"),
        ("response_deadline", "ALTER TABLE messages ADD COLUMN response_deadline REAL"),
        ("nudge_count", "ALTER TABLE messages ADD COLUMN nudge_count INTEGER NOT NULL DEFAULT 0"),
        ("correlation_id", "ALTER TABLE messages ADD COLUMN correlation_id TEXT"),
    ):
        if col not in columns("messages"):
            conn.execute(ddl)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_correlation ON messages(correlation_id,message_type)")
    conn.commit()


def scoped_idempotency_key(from_agent: str, key: str | None, namespace: str = "agent") -> str | None:
    """Idempotency keys are sender-scoped. The column is UNIQUE globally, so
    storing the raw key let agent B's retry collide with agent A's and silently
    return A's message id. Namespacing fixes that without a destructive table
    migration; the raw key is still checked for rows written by older versions."""
    if not key:
        return None
    if len(key) > 200 or any(ord(ch) < 32 for ch in key):
        raise SystemExit("error: --idempotency-key must be <=200 chars and contain no control characters")
    return f"{namespace}:{from_agent}:{key}"


def find_duplicate_message(conn: sqlite3.Connection, from_agent: str, key: str | None) -> sqlite3.Row | None:
    if not key:
        return None
    scoped = scoped_idempotency_key(from_agent, key)
    return conn.execute(
        "SELECT message_id,thread_id FROM messages WHERE from_agent=? AND idempotency_key IN (?,?) ORDER BY idempotency_key=? DESC LIMIT 1",
        (from_agent, scoped, key, scoped),
    ).fetchone()


@contextlib.contextmanager
def tx(conn: sqlite3.Connection):
    """Explicit transaction. connect() is autocommit (isolation_level=None),
    so the old conn.commit() calls were no-ops and every multi-step write
    (gate check -> seq -> insert -> cursor) was its own transaction.
    BEGIN IMMEDIATE takes the write lock up front, which is what makes the
    group seq assignment and the join cap race-free across processes."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


def require_agent(conn: sqlite3.Connection, agent_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM agents WHERE agent_id=?", (agent_id,)).fetchone()
    if row is None:
        raise SystemExit(f"error: unknown agent {agent_id!r}; run `bus register` first")
    return row


def require_auth(conn: sqlite3.Connection, agent_id: str) -> str:
    row = require_agent(conn, agent_id)
    token = load_token(agent_id)
    if token is None or sha256_text(token) != row["token_hash"]:
        raise SystemExit(f"error: missing or invalid token for agent {agent_id!r}")
    return token


def require_operator_auth(conn: sqlite3.Connection, agent_id: str | None) -> str:
    """Maintenance commands (dlq/peer/outbox/prune) read or mutate every
    agent's data, so they need a proven identity even though they act on behalf
    of the operator rather than one agent. Returns the operator's agent id
    (never its token)."""
    agent_id = agent_id or os.environ.get("AGENT_BUS_AGENT")
    if not agent_id:
        raise SystemExit("error: this command needs an operator identity: pass --agent-id or set AGENT_BUS_AGENT")
    validate_id("agent_id", agent_id)
    require_auth(conn, agent_id)
    return agent_id


def cmd_init(_args: argparse.Namespace) -> int:
    ensure_home()
    conn = connect()
    info = home_info()
    print(json_dumps({"ok": True, "path": str(db_path()), **info}))
    conn.close()
    return 0


__all__ = [
    "cmd_init",
    "connect",
    "db_path",
    "find_duplicate_message",
    "init_schema",
    "require_agent",
    "require_auth",
    "require_operator_auth",
    "scoped_idempotency_key",
    "tx",
]
