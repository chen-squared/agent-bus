#!/usr/bin/env python3
"""Generic agent-bus command line (v0).

Shared SQLite + token-authenticated CLI. No central daemon: each agent
runs its own notifier (`bus_agent.py`) which polls the shared database.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import platform
import secrets
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

BUS_HOME = Path(os.environ.get("AGENT_BUS_HOME", Path(__file__).resolve().parent)).expanduser().resolve()
DEFAULT_DB = str(BUS_HOME / "bus.db")
TOKEN_DIR = BUS_HOME / "tokens"


def chmod_private(path: Path, mode: int = 0o600) -> None:
    """Best-effort tightening for state files. The bus assumes a single local
    unix user; a world-readable DB/blob store silently breaks that assumption."""
    try:
        os.chmod(path, mode)
    except (FileNotFoundError, PermissionError, OSError):
        pass


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


def now() -> float:
    return time.time()


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

NUDGE_FIRST_SECONDS = max(1, int(os.getenv("AGENT_BUS_NUDGE_FIRST", "60") or "60"))
NUDGE_STEP_SECONDS = max(1, int(os.getenv("AGENT_BUS_NUDGE_STEP", "60") or "60"))


def token_path(agent_id: str) -> Path:
    override = os.environ.get("AGENT_BUS_TOKEN_FILE")
    if override:
        return Path(override).expanduser()
    # Directory-wide override, so a second project can run a fully separate bus
    # world (own DB via AGENT_BUS_DB, own token namespace here) without colliding
    # on agent_id or reading each other's tokens. TOKEN_DIR itself stays default.
    base = os.environ.get("AGENT_BUS_TOKEN_DIR")
    if base:
        p = Path(base).expanduser() / f"{agent_id}.token"
    else:
        p = (TOKEN_DIR / f"{agent_id}.token").expanduser()
    chmod_private(p.parent, 0o700)
    return p


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


AUDIT_LOG = Path(os.environ.get("AGENT_BUS_AUDIT_LOG", str(BUS_HOME / "bus_audit.log"))).expanduser()


def audit_log(event: str, **fields: Any) -> None:
    line = json_dumps({"time": now(), "event": event, **{k: v for k, v in fields.items() if v is not None}})
    print(line, file=sys.stderr)
    try:
        AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(AUDIT_LOG), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        chmod_private(AUDIT_LOG)
    except Exception:
        pass


def load_token(agent_id: str) -> str | None:
    p = token_path(agent_id)
    try:
        return p.read_text().strip()
    except FileNotFoundError:
        return None


def save_token(agent_id: str, token: str) -> Path:
    p = token_path(agent_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    chmod_private(p.parent, 0o700)
    # Create with the final mode directly: write_text+chmod leaves the file at
    # umask perms (0644) for a brief window.
    fd = os.open(str(p), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(token + "\n")
    os.chmod(p, 0o600)
    return p


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


def machine_id() -> str:
    return os.environ.get("AGENT_BUS_MACHINE") or platform.node() or "localhost"


def parse_capabilities(value: str | None) -> list[str]:
    if not value:
        return []
    value = value.strip()
    if value.startswith("["):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except json.JSONDecodeError:
            pass
    return [x.strip() for x in value.split(",") if x.strip()]


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


# Ids reach shells (command-adapter templates, tmux send-keys, inbox pointer
# files) and file names (tokens/<agent>.token). A shell metacharacter or a
# newline in any of them is an injection or a line-forgery, so ids are a
# closed charset, checked once at every entry point that accepts one.
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def validate_id(kind: str, value: str | None) -> None:
    if value is None:
        return
    if not ID_RE.fullmatch(value):
        raise SystemExit(
            f"error: invalid {kind} {value!r}; allowed: letters, digits, '_', '.', '-' (max 64, no shell metacharacters or newlines)"
        )


def validate_sha256(value: str | None) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ValueError(f"invalid sha256 {value!r}; expected 64 lowercase hex characters")
    return value


def nonneg_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def nonneg_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


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


def public_agent(item: dict[str, Any]) -> dict[str, Any]:
    """Agent rows are printed by `agents`/`find`, which are discovery commands
    with no auth. token_hash must never ride along."""
    item.pop("token_hash", None)
    return item


def blob_dir() -> Path:
    root = Path(os.environ.get("AGENT_BUS_BLOB_DIR", str(BUS_HOME / "blobs"))).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    chmod_private(root, 0o700)
    return root


def blob_path(sha256: str) -> Path:
    # A sha256 is a hex digest, never a path fragment. Validating here closes
    # traversal from remote attachments and from `blob get --sha256`.
    validate_sha256(sha256)
    root = blob_dir()
    return root / f"{sha256}.blob"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def blob_max_bytes() -> int:
    return int(os.environ.get("AGENT_BUS_BLOB_MAX_BYTES", str(10 * 1024 * 1024)))


def store_blob_from_path(path: Path) -> dict[str, Any]:
    p = Path(path).expanduser()
    if not p.is_file():
        raise SystemExit(f"error: file not found: {p}")
    size = p.stat().st_size
    if size > blob_max_bytes():
        raise SystemExit(f"error: file exceeds blob cap of {blob_max_bytes()} bytes")
    digest = sha256_file(p)
    destination = blob_path(digest)
    if not destination.exists():
        # Atomic-ish copy.
        tmp = destination.with_suffix(".tmp")
        with open(p, "rb") as src, open(tmp, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
        os.replace(tmp, destination)
        chmod_private(destination)
    return {"sha256": digest, "size": size, "name": p.name, "path": str(destination)}


def prepare_attachments(raw_paths: str | None, remote: bool) -> list[dict[str, Any]]:
    """One attachment shape for send/ask, local and remote. The old ask path
    stored bare strings while send stored manifests, so the two commands
    produced different rows for the same flag."""
    attached: list[dict[str, Any]] = []
    if not raw_paths:
        return attached
    cap = blob_max_bytes()
    for raw in raw_paths.split(","):
        raw = raw.strip()
        if not raw:
            continue
        p = Path(raw).expanduser()
        if not p.is_file():
            raise SystemExit(f"error: attachment not found: {raw}")
        if p.stat().st_size > cap:
            raise SystemExit(f"error: attachment too large (>{cap} bytes): {raw}")
        manifest = store_blob_from_path(p)
        item = {
            "sha256": manifest["sha256"],
            "size": manifest["size"],
            "name": manifest["name"],
            "origin": p.name,
        }
        if not remote:
            item["path"] = str(p.resolve())
        attached.append(item)
    return attached


def iter_referenced_blob_hashes() -> set[str]:
    conn = connect()
    hashes: set[str] = set()

    def collect(items: Any) -> None:
        if not isinstance(items, list):
            return
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("sha256"), str):
                hashes.add(item["sha256"])
            elif isinstance(item, dict):
                for value in item.values():
                    collect(value if isinstance(value, list) else [])

    for row in conn.execute("SELECT attachments FROM messages").fetchall():
        try:
            collect(json.loads(row["attachments"] or "[]"))
        except (json.JSONDecodeError, TypeError):
            pass
    for row in conn.execute("SELECT payload FROM outbox").fetchall():
        try:
            data = json.loads(row["payload"] or "{}")
        except (json.JSONDecodeError, TypeError):
            data = {}
        collect(data.get("attachments") if isinstance(data, dict) else [])
    conn.close()
    return hashes


def blob_materialize(att: dict[str, Any], source_peer_name: str, peer: dict[str, Any] | None) -> Path:
    sha = validate_sha256(att.get("sha256"))
    dest = blob_path(sha)
    if dest.exists():
        if sha256_file(dest) == sha:
            return dest
        dest.unlink(missing_ok=True)
    if peer is None:
        raise RuntimeError(f"no peer config for source {source_peer_name}")
    url = peer["base_url"].rstrip("/") + f"/bus/blob/{sha}"
    auth = peer["auth_token"]
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + auth}, method="GET")
    expected_size = att.get("size")
    if expected_size is not None and int(expected_size) > blob_max_bytes():
        raise RuntimeError("remote blob exceeds configured cap")
    tmp = dest.with_suffix(".tmp")
    total = 0
    digest = hashlib.sha256()
    with urllib.request.urlopen(req, timeout=60) as response:
        with open(tmp, "wb") as out:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                if total + len(chunk) > blob_max_bytes():
                    raise RuntimeError("remote blob exceeds configured cap")
                out.write(chunk)
                digest.update(chunk)
                total += len(chunk)
    if digest.hexdigest() != sha:
        tmp.unlink(missing_ok=True)
        raise RuntimeError("blob hash mismatch")
    os.replace(tmp, dest)
    return dest


def cmd_blob_put(args: argparse.Namespace) -> int:
    result = store_blob_from_path(Path(args.path))
    print(json_dumps({"ok": True, **result}))


def cmd_blob_get(args: argparse.Namespace) -> int:
    try:
        p = blob_path(args.sha256)
    except ValueError as exc:
        raise SystemExit(f"error: {exc}")
    if not p.exists():
        raise SystemExit(f"error: blob not found: {args.sha256}")
    digest = sha256_file(p)
    if digest != args.sha256:
        raise SystemExit("error: blob corrupt")
    out = Path(args.output).expanduser().resolve() if args.output else p
    if args.output:
        import shutil

        shutil.copyfile(p, out)
    print(json_dumps({"ok": True, "sha256": digest, "size": p.stat().st_size, "path": str(out)}))


def cmd_blob_list(_args: argparse.Namespace) -> int:
    root = blob_dir()
    root.mkdir(parents=True, exist_ok=True)
    entries = []
    for p in root.glob("*.blob"):
        sha = p.stem
        entries.append({"sha256": sha, "size": p.stat().st_size, "path": str(p), "valid": sha256_file(p) == sha})
    print(json_dumps({"ok": True, "count": len(entries), "blobs": entries}))


def cmd_blob_gc(args: argparse.Namespace) -> int:
    referenced = iter_referenced_blob_hashes()
    root = blob_dir()
    root.mkdir(parents=True, exist_ok=True)
    grace = args.grace_seconds if args.grace_seconds is not None else 86400  # `or 86400` swallowed --grace-seconds 0
    removed = 0
    kept = 0
    removed_bytes = 0
    for p in root.glob("*.blob"):
        sha = p.stem
        if sha in referenced:
            kept += 1
            continue
        age = now() - p.stat().st_mtime
        if age < grace and not args.force:
            kept += 1
            continue
        removed_bytes += p.stat().st_size
        p.unlink(missing_ok=True)
        removed += 1
    print(json_dumps({"ok": True, "removed": removed, "removed_bytes": removed_bytes, "kept": kept}))


def cmd_init(_args: argparse.Namespace) -> int:
    conn = connect()
    print(json_dumps({"ok": True, "db": str(db_path()), "path": str(db_path())}))
    conn.close()
    return 0


def cmd_register(args: argparse.Namespace) -> int:
    validate_id("agent_id", args.agent_id)
    conn = connect()
    preexisting = conn.execute(
        "SELECT token_hash FROM agents WHERE agent_id=?", (args.agent_id,)
    ).fetchone()
    token = load_token(args.agent_id)
    created_token = False
    if preexisting is not None:
        # Re-registering used to mint a fresh token unconditionally and
        # overwrite both tokens/<id>.token and the stored token_hash. Any
        # local process could then take over any agent id AND invalidate the
        # real holder's credential. A re-register must present the current
        # token; deliberate rotation is an explicit, also-authenticated flag.
        if token is None or sha256_text(token) != preexisting["token_hash"]:
            conn.close()
            raise SystemExit(
                f"error: agent {args.agent_id!r} already exists; re-registering requires its current token "
                f"(expected at {token_path(args.agent_id)}). Use --rotate-token to intentionally replace it."
            )
        if args.rotate_token:
            token = secrets.token_urlsafe(32)
            save_token(args.agent_id, token)
            created_token = True
    elif token is None:
        token = secrets.token_urlsafe(32)
        save_token(args.agent_id, token)
        created_token = True
    token_hash = sha256_text(token)
    capabilities = parse_capabilities(args.capabilities)
    metadata = {}
    if args.metadata:
        try:
            metadata = json.loads(args.metadata)
        except json.JSONDecodeError:
            metadata = {"note": args.metadata}
    metadata.setdefault("capabilities_schema_version", 1)
    now_ts = now()
    conn.execute(
        """
        INSERT INTO agents (agent_id,agent_type,display_name,model_tier,capabilities,schema_version,
            token_hash,current_session_id,endpoint,machine_id,status,last_heartbeat,metadata,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(agent_id) DO UPDATE SET
            agent_type=excluded.agent_type,
            display_name=COALESCE(excluded.display_name,agents.display_name),
            model_tier=COALESCE(excluded.model_tier,agents.model_tier),
            capabilities=CASE WHEN ? THEN excluded.capabilities ELSE agents.capabilities END,
            schema_version=excluded.schema_version,
            token_hash=excluded.token_hash,
            current_session_id=COALESCE(excluded.current_session_id,agents.current_session_id),
            endpoint=COALESCE(excluded.endpoint,agents.endpoint),
            machine_id=COALESCE(excluded.machine_id,agents.machine_id),
            status=excluded.status,
            metadata=excluded.metadata,
            updated_at=excluded.updated_at
        """,
        (
            args.agent_id,
            args.agent_type,
            args.display_name,
            args.model_tier,
            json_dumps(capabilities),
            1,
            token_hash,
            args.session_id,
            args.endpoint,
            args.machine_id or machine_id(),
            "registered",
            None,
            json_dumps(metadata),
            now_ts,
            now_ts,
            bool(args.capabilities),
        ),
    )
    conn.commit()
    print(
        json_dumps(
            {
                "ok": True,
                "agent_id": args.agent_id,
                "token_file": str(token_path(args.agent_id)),
                "created_token": created_token,
            }
        )
    )
    conn.close()
    return 0


def cmd_heartbeat(args: argparse.Namespace) -> int:
    conn = connect()
    require_auth(conn, args.agent_id)
    require_agent(conn, args.agent_id)
    updates = {
        "last_heartbeat": now(),
        "status": "active",
        "updated_at": now(),
    }
    sql = "UPDATE agents SET last_heartbeat=?, status=?, updated_at=?"
    params: list[Any] = [updates["last_heartbeat"], updates["status"], updates["updated_at"]]
    if args.session_id is not None:
        sql += ", current_session_id=?"
        params.append(args.session_id)
    if args.endpoint is not None:
        sql += ", endpoint=?"
        params.append(args.endpoint)
    sql += " WHERE agent_id=?"
    params.append(args.agent_id)
    conn.execute(sql, params)
    conn.commit()
    print(json_dumps({"ok": True, "agent_id": args.agent_id, "heartbeat": updates["last_heartbeat"]}))
    conn.close()
    return 0


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
            latest = conn.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM group_messages WHERE group_id=?", (group_id,)).fetchone()["seq"]
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
    payload = {"text": args.text}
    if args.metadata:
        try:
            payload["metadata"] = json.loads(args.metadata)
        except json.JSONDecodeError:
            payload["metadata"] = {"note": args.metadata}
    found = find_duplicate_message(conn, args.from_agent, args.idempotency_key)
    if found is not None:
        conn.close()
        print(json_dumps({"ok": True, "duplicate": True, "message_id": found["message_id"], "thread_id": found["thread_id"]}))
        return 0
    message_id = str(uuid.uuid4())
    outbox_id = str(uuid.uuid4())
    attached = attachments
    message_type = args.message_type or "text"
    try:
        # One transaction: message row + its outbox handoff entry land
        # together, so a crash cannot leave a remote send with no outbox row.
        with tx(conn):
            conn.execute(
                """
                INSERT INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,message_type,urgency,
                    payload,attachments,ttl_seconds,expires_at,retry_count,status,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    message_id,
                    thread_id,
                    scoped_idempotency_key(args.from_agent, args.idempotency_key),
                    args.from_agent,
                    args.to_agent,
                    message_type,
                    args.urgency or "normal",
                    json_dumps(payload),
                    json_dumps(attached),
                    ttl,
                    expires,
                    0,
                    "queued",
                    now_ts,
                ),
            )
            if remote_peer is not None:
                row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
                conn.execute(
                    """
                    INSERT INTO outbox (outbox_id,peer_name,message_id,from_agent,to_remote,thread_id,payload,status,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        outbox_id,
                        remote_peer,
                        message_id,
                        args.from_agent,
                        args.to_agent,
                        thread_id,
                        json_dumps(row_to_dict(row)),
                        "pending",
                        now(),
                        now(),
                    ),
                )
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise SystemExit(f"error: insert failed: {exc}")
    audit_to = f"{args.to_agent}@{remote_peer}" if remote_peer else args.to_agent
    audit_log("send", from_agent=args.from_agent, to_agent=audit_to, message_id=message_id, thread_id=thread_id)
    remote_result = None
    if remote_peer is not None:
        row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
        peer = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        ok, receipt = _handoff_peer(row_to_dict(peer), row_to_dict(row), timeout=10)
        new_status = "sent" if ok else "failed"
        conn.execute(
            "UPDATE outbox SET status=?, receipt=?, attempts=attempts+1, last_error=?, updated_at=?, delivered_at=? WHERE outbox_id=?",
            (new_status, receipt, None if ok else str(receipt)[:500], now(), now() if ok else None, outbox_id),
        )
        remote_result = {"outbox_id": outbox_id, "handoff": "ok" if ok else "failed", "receipt": receipt}
    payload = {"ok": True, "message_id": message_id, "thread_id": thread_id, "to": args.to_agent, "type": message_type, "target_stale": target_stale}
    if soft_warning:
        payload["warning"] = soft_warning
    if remote_result:
        payload["remote"] = remote_result
    print(json_dumps(payload))
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


def cmd_agents(args: argparse.Namespace) -> int:
    conn = connect()
    rows = conn.execute("SELECT * FROM agents ORDER BY agent_id").fetchall()
    ts = now()
    items = []
    for r in rows:
        item = public_agent(row_to_dict(r))
        item["stale"] = is_stale(r, ts)
        item["heartbeat_age_seconds"] = (ts - float(r["last_heartbeat"])) if r["last_heartbeat"] else None
        items.append(item)
    print(json_dumps({"ok": True, "agents": items}))
    conn.close()
    return 0


def resolve_opencode_cli() -> str:
    """Pick the opencode CLI without hardcoding a version: a pinned
    `.../cli/2.0.29-arm64/opencode-cli` kept working silently broken the day
    the app updated to 2.0.31 and every notifier failed its next inject."""
    env = os.environ.get("AGENT_BUS_CLI")
    if env:
        return env
    base = Path.home() / "Library" / "Application Support" / "ai.opencode.desktop" / "cli"
    if base.is_dir():
        for cand in sorted(base.glob("*/opencode-cli"), reverse=True):
            if cand.exists() and os.access(cand, os.X_OK):
                return str(cand)
    return "opencode"  # fall back to whatever is on PATH


OPENCODE_CLI = resolve_opencode_cli()


def _opencode_outcome(session_id: str) -> dict[str, Any]:
    """Adapter-specific probe, NOT a bus signal.

    The bus acks when the adapter's inject returns, which for opencode means the
    server took the prompt; the model turn happens after that. opencode itself
    records the turn outcome, so ask it. Returns {} plus probe_error on any failure
    so the caller reports unknown rather than guessing.
    """
    if not session_id:
        return {"probe_error": "no session id registered"}
    try:
        p = subprocess.run(
            [OPENCODE_CLI, "api", "session.get", "--param", f"sessionID={session_id}"],
            capture_output=True, text=True, timeout=20,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"probe_error": f"{type(exc).__name__}: {exc}"}
    if p.returncode != 0:
        return {"probe_error": (p.stderr or p.stdout or "session.get failed").strip()[:300]}
    try:
        data = json.loads(p.stdout).get("data") or {}
    except (json.JSONDecodeError, AttributeError, TypeError):
        return {"probe_error": "session.get returned non-JSON"}
    return {
        "outcome": data.get("outcome"),
        "time_idle_ms": (data.get("time") or {}).get("idle"),
        "turn_model": (data.get("model") or {}).get("id"),
    }


def cmd_liveness(args: argparse.Namespace) -> int:
    """Report what the bus can and cannot prove about an agent being awake.

    Heartbeat proves the notifier process is alive, never that a model answered.
    Only opencode exposes a turn outcome, and only by asking opencode directly;
    every other adapter reports unknown and falls back to round-trip evidence.
    """
    conn = connect()
    row = conn.execute("SELECT * FROM agents WHERE agent_id=?", (args.agent_id,)).fetchone()
    if row is None:
        conn.close()
        raise SystemExit(f"error: agent {args.agent_id} not registered")
    ts = now()
    out: dict[str, Any] = {
        "ok": True,
        "agent_id": args.agent_id,
        "agent_type": row["agent_type"],
        "bus_status": row["status"],
        "notifier_heartbeat_age_seconds": (ts - float(row["last_heartbeat"])) if row["last_heartbeat"] else None,
        "notifier_heartbeat_stale": is_stale(row, ts),
    }
    # Adapter-agnostic evidence: when did this agent last actually produce a
    # message? Digest rows are injected INTO the agent with from_agent == the agent
    # itself, so they must be excluded or a dead agent looks like it just spoke.
    spoke = conn.execute(
        "SELECT MAX(created_at) AS t FROM messages WHERE from_agent=? AND from_agent<>to_agent"
        " AND message_type<>'system'",
        (args.agent_id,),
    ).fetchone()["t"]
    gspoke = conn.execute(
        "SELECT MAX(created_at) AS t FROM group_messages WHERE sender_agent=?", (args.agent_id,)
    ).fetchone()["t"]
    last_spoke = max([v for v in (spoke, gspoke) if v is not None], default=None)
    out["last_spoke_age_seconds"] = (ts - last_spoke) if last_spoke is not None else None
    out["open_asks"] = conn.execute(
        "SELECT COUNT(*) AS c FROM messages WHERE to_agent=? AND message_type='ask'"
        " AND status IN ('queued','delivered')",
        (args.agent_id,),
    ).fetchone()["c"]
    if row["agent_type"] == "opencode":
        probe = _opencode_outcome(row["endpoint"] or row["current_session_id"] or "")
        out["model_liveness"] = probe.get("outcome") or "unknown"
        out["liveness_source"] = "opencode session.get outcome (opencode's semantics, not the bus's)"
        if probe.get("probe_error"):
            out["probe_error"] = probe["probe_error"]
        for k in ("time_idle_ms", "turn_model"):
            if probe.get(k) is not None:
                out[k] = probe[k]
    else:
        out["model_liveness"] = "unknown"
        out["liveness_source"] = "none"
        out["liveness_note"] = (
            f"agent_type={row['agent_type']}: no adapter-level model probe exists. The bus only sees the "
            "adapter's exit code, so a hook that exits 0 without waking a model is indistinguishable from "
            "success. Use last_spoke_age_seconds and open_asks here, or a round-trip `ask --wait` that goes "
            "stale, as the liveness signal."
        )
    print(json_dumps(out))
    conn.close()
    return 0


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
    outbox_id = str(uuid.uuid4())
    payload = {"text": args.text, "kind": "ask"}
    if args.metadata:
        try:
            payload["metadata"] = json.loads(args.metadata)
        except json.JSONDecodeError:
            payload["metadata"] = {"note": args.metadata}
    try:
        with tx(conn):
            conn.execute(
                """
                INSERT INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,message_type,urgency,
                    payload,attachments,ttl_seconds,expires_at,expects_response,response_deadline,nudge_count,retry_count,status,correlation_id,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    message_id,
                    thread_id,
                    scoped_idempotency_key(args.from_agent, args.idempotency_key),
                    args.from_agent,
                    args.to_agent,
                    "ask",
                    args.urgency or "normal",
                    json_dumps(payload),
                    json_dumps(attachments),
                    ttl,
                    expires,
                    1 if expect_reply else 0,
                    response_deadline,
                    0,
                    0,
                    "queued",
                    message_id,
                    now_ts,
                ),
            )
            if remote_peer is not None:
                row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
                conn.execute(
                    """
                    INSERT INTO outbox (outbox_id,peer_name,message_id,from_agent,to_remote,thread_id,payload,status,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        outbox_id,
                        remote_peer,
                        message_id,
                        args.from_agent,
                        args.to_agent,
                        thread_id,
                        json_dumps(row_to_dict(row)),
                        "pending",
                        now(),
                        now(),
                    ),
                )
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise SystemExit(f"error: insert failed: {exc}")
    audit_to = f"{args.to_agent}@{remote_peer}" if remote_peer else args.to_agent
    audit_log("send", from_agent=args.from_agent, to_agent=audit_to, message_id=message_id, thread_id=thread_id)
    remote_result = None
    if remote_peer is not None:
        row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
        peer = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        ok, receipt = _handoff_peer(row_to_dict(peer), row_to_dict(row), timeout=10)
        conn.execute(
            "UPDATE outbox SET status=?, receipt=?, attempts=attempts+1, last_error=?, updated_at=?, delivered_at=? WHERE outbox_id=?",
            ("sent" if ok else "failed", receipt, None if ok else str(receipt)[:500], now(), now() if ok else None, outbox_id),
        )
        remote_result = {"outbox_id": outbox_id, "handoff": "ok" if ok else "failed", "receipt": receipt}
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
    outbox_id = str(uuid.uuid4())
    payload = {"text": args.text, "kind": "answer"}
    if args.metadata:
        try:
            payload["metadata"] = json.loads(args.metadata)
        except json.JSONDecodeError:
            payload["metadata"] = {"note": args.metadata}
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
            conn.execute(
                """
                INSERT INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,message_type,urgency,
                    payload,attachments,ttl_seconds,expires_at,retry_count,status,answer_to_message_id,correlation_id,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    message_id,
                    args.thread_id,
                    scoped_idempotency_key(args.from_agent, args.idempotency_key),
                    args.from_agent,
                    to,
                    "answer",
                    args.urgency or "normal",
                    json_dumps(payload),
                    "[]",
                    ttl,
                    expires,
                    0,
                    "queued",
                    answer_to,
                    correlation_id,
                    now_ts,
                ),
            )
            if remote_peer is not None:
                row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
                conn.execute(
                    """
                    INSERT INTO outbox (outbox_id,peer_name,message_id,from_agent,to_remote,thread_id,payload,status,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        outbox_id,
                        remote_peer,
                        message_id,
                        args.from_agent,
                        to,
                        args.thread_id,
                        json_dumps(row_to_dict(row)),
                        "pending",
                        now(),
                        now(),
                    ),
                )
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise SystemExit(f"error: insert failed: {exc}")
    audit_to = f"{to}@{remote_peer}" if remote_peer else to
    audit_log("send", from_agent=args.from_agent, to_agent=audit_to, message_id=message_id, thread_id=args.thread_id)
    remote_result = None
    if remote_peer is not None:
        row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
        peer = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        ok, receipt = _handoff_peer(row_to_dict(peer), row_to_dict(row), timeout=10)
        new_status = "sent" if ok else "failed"
        conn.execute(
            "UPDATE outbox SET status=?, receipt=?, attempts=attempts+1, last_error=?, updated_at=?, delivered_at=? WHERE outbox_id=?",
            (new_status, receipt, None if ok else str(receipt)[:500], now(), now() if ok else None, outbox_id),
        )
        remote_result = {"outbox_id": outbox_id, "handoff": "ok" if ok else "failed", "receipt": receipt}
    payload = {"ok": True, "message_id": message_id, "thread_id": args.thread_id, "to": to, "type": "answer"}
    if remote_result:
        payload["remote"] = remote_result
    print(json_dumps(payload))
    conn.close()
    return 0


def capabilities_info() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "model_tiers": ["weak", "medium", "strong", "specialist"],
        "message_types": ["text", "ask", "answer", "system"],
        "urgency": ["low", "normal", "high", "urgent"],
        "capability_examples": [
            "code-edit",
            "terminal",
            "file-read",
            "file-write",
            "browser",
            "search",
            "model:strong-reasoning",
            "language:python",
        ],
        "attachment_cap_bytes": 10 * 1024 * 1024,
        "text_cap_bytes": 64 * 1024,
    }


def cmd_capabilities(_args: argparse.Namespace) -> int:
    print(json_dumps(capabilities_info()))
    return 0


HEARTBEAT_EXPIRY = float(os.environ.get("AGENT_BUS_HEARTBEAT_EXPIRY", "30"))


def is_stale(row: sqlite3.Row | None, now_ts: float | None = None) -> bool:
    if row is None:
        return True
    ts = now_ts or now()
    last = row["last_heartbeat"]
    if last is None:
        return True
    return (ts - float(last)) > HEARTBEAT_EXPIRY


def sweep_agents(conn: sqlite3.Connection) -> int:
    cutoff = now() - HEARTBEAT_EXPIRY
    cur = conn.execute(
        "UPDATE agents SET status='stale',updated_at=? WHERE last_heartbeat IS NOT NULL AND last_heartbeat < ? AND status='active'",
        (now(), cutoff),
    )
    conn.commit()
    return cur.rowcount


def cmd_find(args: argparse.Namespace) -> int:
    conn = connect()
    rows = conn.execute("SELECT * FROM agents ORDER BY agent_id").fetchall()
    result = []
    ts = now()
    for r in rows:
        if args.model_tier and r["model_tier"] != args.model_tier:
            continue
        caps = json.loads(r["capabilities"] or "[]")
        if args.capability and args.capability not in caps:
            continue
        if args.active_only and is_stale(r, ts):
            continue
        item = public_agent(row_to_dict(r))
        item["stale"] = is_stale(r, ts)
        item["heartbeat_age_seconds"] = (ts - float(r["last_heartbeat"])) if r["last_heartbeat"] else None
        result.append(item)
    print(json_dumps({"ok": True, "count": len(result), "agents": result}))
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
    ts = now()
    print(
        json_dumps(
            {
                "ok": True,
                "machine_id": machine_id(),
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
    `bus outbox flush`. Claims are a status transition under a write lock, so
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
                    (args.ceiling_per_minute if args.ceiling_per_minute is not None else 120),
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
    if truncated:
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
    member = _group_require_member(conn, args.group_id, args.from_agent)
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
        latest = conn.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM group_messages WHERE group_id=?", (args.group_id,)).fetchone()["seq"]
        read_state = conn.execute("SELECT last_read_seq FROM group_read_state WHERE group_id=? AND agent_id=?", (args.group_id, args.from_agent)).fetchone()
        last_read = read_state["last_read_seq"] if read_state else 0
        digest_state = conn.execute(
            "SELECT last_digest_seq FROM group_read_state WHERE group_id=? AND agent_id=?",
            (args.group_id, args.from_agent),
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
        member = conn.execute("SELECT * FROM group_members WHERE group_id=? AND agent_id=?", (args.group_id, args.from_agent)).fetchone()
        rate = (member["rate_seconds"] if member["rate_seconds"] is not None else (group["rate_seconds"] if group["rate_seconds"] is not None else 2))
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
            (args.group_id, now_ts - 60),
        ).fetchone()["c"]
        if recent >= ceiling:
            raise SystemExit(json_dumps({"error": "group ceiling reached", "retry_after_seconds": 5}))
        backlog = conn.execute(
            """
            SELECT COUNT(*) AS c FROM group_read_state grs
            JOIN group_members gm ON gm.group_id=grs.group_id AND gm.agent_id=grs.agent_id
            WHERE grs.group_id=? AND gm.status='active' AND grs.last_read_seq < ?
            """,
            (args.group_id, latest),
        ).fetchone()["c"]
        if backlog > (group["backlog_threshold"] if group["backlog_threshold"] is not None else 50):
            raise SystemExit(json_dumps({"error": "group backpressure: too many unread participants"}))
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
                deadline = ask["response_deadline"] if ask["response_deadline"] else ts
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


class BusArgumentParser(argparse.ArgumentParser):
    """Prefix abbreviations are a security issue here: the SSH gateway
    validates exact --group-id spellings, but argparse would accept --group-i
    and let a later duplicate win. Disable them everywhere."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)


def main() -> int:
    os.umask(0o077)
    parser = BusArgumentParser(description="agent-bus generic message bus (v0)")
    parser.add_argument("--db", default=None, help="override database path")
    sub = parser.add_subparsers(dest="command", required=True, parser_class=BusArgumentParser)

    p = sub.add_parser("init", help="initialize database")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("register", help="register or update agent")
    p.add_argument("--agent-id", required=True)
    p.add_argument("--agent-type", required=True, help="opencode|claude-code|cursor|...")
    p.add_argument("--display-name")
    p.add_argument("--model-tier")
    p.add_argument("--capabilities", help="comma list or JSON list")
    p.add_argument("--session-id")
    p.add_argument("--endpoint")
    p.add_argument("--machine-id")
    p.add_argument("--metadata")
    p.add_argument("--rotate-token", action="store_true", help="intentionally replace an existing agent's token (requires the current token)")
    p.set_defaults(func=cmd_register)

    p = sub.add_parser("heartbeat", help="refresh agent liveness and session")
    p.add_argument("--agent-id", required=True)
    p.add_argument("--session-id")
    p.add_argument("--endpoint")
    p.set_defaults(func=cmd_heartbeat)

    p = sub.add_parser("send", help="send a message or ask-ish text")
    p.add_argument("--from", dest="from_agent", default=os.environ.get("AGENT_BUS_AGENT"))
    p.add_argument("--to", dest="to_agent", required=True)
    p.add_argument("--text", required=True)
    p.add_argument("--thread-id")
    p.add_argument("--message-type", default="text")
    p.add_argument("--urgency", default="normal")
    p.add_argument("--ttl", type=int)
    p.add_argument("--idempotency-key")
    p.add_argument("--attachment", help="comma-separated file paths")
    p.add_argument("--metadata")
    p.set_defaults(func=cmd_send)

    p = sub.add_parser("inbox", help="list undelivered messages for agent")
    p.add_argument("--agent-id", required=True)
    p.add_argument("--status", default="undelivered", choices=["undelivered", "queued", "delivered", "acked", "all"])
    p.add_argument("--limit", type=nonneg_int, default=50)
    p.set_defaults(func=cmd_inbox)

    p = sub.add_parser("ack", help="acknowledge delivered message")
    p.add_argument("--agent-id", required=True)
    p.add_argument("--message-id", required=True)
    p.set_defaults(func=cmd_ack)

    p = sub.add_parser("thread", help="show thread")
    p.add_argument("thread_id")
    p.add_argument("--agent-id", required=True)
    p.set_defaults(func=cmd_thread)

    p = sub.add_parser("message", help="read a single message with ownership check")
    m = p.add_subparsers(dest="message_command", required=True, parser_class=BusArgumentParser)
    ms = m.add_parser("show", help="show one message")
    ms.add_argument("--agent-id", required=True)
    ms.add_argument("--message-id", required=True)
    ms.set_defaults(func=cmd_message_show)

    p = sub.add_parser("agents", help="list agents")
    p.set_defaults(func=cmd_agents)

    p = sub.add_parser("liveness", help="what the bus can and cannot prove about one agent being awake")
    p.add_argument("--agent-id", required=True)
    p.set_defaults(func=cmd_liveness)

    p = sub.add_parser("sweep", help="expire TTL messages and dead-letter retries")
    p.set_defaults(func=cmd_sweep)

    p = sub.add_parser("prune", help="delete old terminal messages/outbox rows (retention)")
    p.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    p.add_argument("--days", type=nonneg_int, default=30)
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_prune)

    p = sub.add_parser("ask", help="send an ask and optionally wait for answer")
    p.add_argument("--from", dest="from_agent", default=os.environ.get("AGENT_BUS_AGENT"))
    p.add_argument("--to", dest="to_agent", required=True)
    p.add_argument("--text", required=True)
    p.add_argument("--thread-id")
    p.add_argument("--urgency", default="normal")
    p.add_argument("--ttl", type=int, default=300)
    p.add_argument("--timeout", type=nonneg_int, default=120)
    p.add_argument("--idempotency-key")
    p.add_argument("--attachment", help="comma-separated file paths")
    p.add_argument("--metadata")
    p.add_argument("--wait", action="store_true")
    p.add_argument("--no-expect-reply", action="store_true", help="do not monitor/remind for a response")
    p.set_defaults(func=cmd_ask)

    p = sub.add_parser("answer", help="answer an ask thread")
    p.add_argument("--thread-id", required=True)
    p.add_argument("--from", dest="from_agent", default=os.environ.get("AGENT_BUS_AGENT"))
    p.add_argument("--to", dest="to_agent")
    p.add_argument("--text", required=True)
    p.add_argument("--answer-to")
    p.add_argument("--urgency", default="normal")
    p.add_argument("--ttl", type=int, default=300)
    p.add_argument("--idempotency-key")
    p.add_argument("--metadata")
    p.set_defaults(func=cmd_answer)

    p = sub.add_parser("capabilities", help="show capability/tier vocabulary")
    p.set_defaults(func=cmd_capabilities)

    p = sub.add_parser("peer", help="manage remote peers")
    peer_sub = p.add_subparsers(dest="peer_command", required=True, parser_class=BusArgumentParser)
    pa = peer_sub.add_parser("add", help="add or update peer")
    pa.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    pa.add_argument("--peer", required=True)
    pa.add_argument("--host")
    pa.add_argument("--base-url", required=True)
    pa.add_argument("--token", required=True)
    pa.set_defaults(func=cmd_peer_add)
    pl = peer_sub.add_parser("list", help="list peers")
    pl.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    pl.set_defaults(func=cmd_peer_list)
    pr = peer_sub.add_parser("remove", help="remove peer")
    pr.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    pr.add_argument("--peer", required=True)
    pr.add_argument("--force", action="store_true", help="also delete this peer's outbox rows")
    pr.set_defaults(func=cmd_peer_remove)

    p = sub.add_parser("status", help="show bus health depths")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("group", help="group chat commands (V3)")
    g = p.add_subparsers(dest="group_command", required=True, parser_class=BusArgumentParser)
    gc = g.add_parser("create", help="create group")
    gc.add_argument("--from", dest="from_agent", default=os.environ.get("AGENT_BUS_AGENT"))
    gc.add_argument("--group-id", required=True)
    gc.add_argument("--topic")
    gc.add_argument("--owner")
    gc.add_argument("--max-members", type=nonneg_int, default=50)
    gc.add_argument("--rate-seconds", type=nonneg_float, default=2)
    gc.add_argument("--ceiling-per-minute", type=nonneg_int, default=120)
    gc.add_argument("--backlog-threshold", type=nonneg_int, default=50)
    gc.add_argument("--mode", default="catch-up-gated")
    gc.set_defaults(func=cmd_group_create)
    gj = g.add_parser("join", help="join group")
    gj.add_argument("--group-id", required=True)
    gj.add_argument("--agent-id", required=True)
    gj.add_argument("--role", default=None, help="not settable on join: every join lands as member; roles come only from group create --owner")
    gj.add_argument("--rate-seconds", type=nonneg_float)
    gj.set_defaults(func=cmd_group_join)
    gl = g.add_parser("leave", help="leave group")
    gl.add_argument("--group-id", required=True)
    gl.add_argument("--agent-id", required=True)
    gl.set_defaults(func=cmd_group_leave)
    gc = g.add_parser("close", help="close group (owner only)")
    gc.add_argument("--group-id", required=True)
    gc.add_argument("--agent-id", required=True)
    gc.set_defaults(func=cmd_group_close)
    glist = g.add_parser("list", help="list groups")
    glist.add_argument("--agent-id")
    glist.set_defaults(func=cmd_group_list)
    gm = g.add_parser("messages", help="list or read new group messages")
    gm.add_argument("--group-id", required=True)
    gm.add_argument("--agent-id")
    gm.add_argument("--limit", type=nonneg_int, default=50, help="oldest unread rows to return; 0 means counters only")
    gm.set_defaults(func=cmd_group_messages)
    gr = g.add_parser("read", help="ack seqs as read (clamped to what group messages actually served)")
    gr.add_argument("--group-id", required=True)
    gr.add_argument("--agent-id", required=True)
    gr.add_argument("--seq", type=nonneg_int, help="default latest; clamped to last_served_seq unless --allow-gap")
    gr.add_argument("--allow-gap", action="store_true", help="ack seqs that were never returned by group messages")
    gr.set_defaults(func=cmd_group_read)
    gs = g.add_parser("send", help="send a group message")
    gs.add_argument("--from", dest="from_agent", required=True)
    gs.add_argument("--group-id", required=True)
    gs.add_argument("--text", required=True)
    gs.add_argument("--message-type", default="text")
    gs.add_argument("--urgency", default="normal")
    gs.add_argument("--ttl", type=int)
    gs.set_defaults(func=cmd_group_send)
    gst = g.add_parser("status", help="show group stats (authed members only)")
    gst.add_argument("--group-id", required=True)
    gst.add_argument("--agent-id", required=True)
    gst.set_defaults(func=cmd_group_status)

    p = sub.add_parser("blob", help="content-addressed blob store (for long attachments)")
    blob_sub = p.add_subparsers(dest="blob_command", required=True, parser_class=BusArgumentParser)
    bp = blob_sub.add_parser("put", help="store a file in blob store")
    bp.add_argument("--path", required=True)
    bp.set_defaults(func=cmd_blob_put)
    bg = blob_sub.add_parser("get", help="copy a blob out of the store")
    bg.add_argument("--sha256", required=True)
    bg.add_argument("--output")
    bg.set_defaults(func=cmd_blob_get)
    bl = blob_sub.add_parser("list", help="list blobs")
    bl.set_defaults(func=cmd_blob_list)
    bgc = blob_sub.add_parser("gc", help="garbage collect unreferenced blobs")
    bgc.add_argument("--grace-seconds", type=nonneg_int, default=86400)
    bgc.add_argument("--force", action="store_true")
    bgc.set_defaults(func=cmd_blob_gc)

    p = sub.add_parser("outbox", help="cross-machine delivery queue")
    out_sub = p.add_subparsers(dest="outbox_command", required=True, parser_class=BusArgumentParser)
    ol = out_sub.add_parser("list", help="list outbox entries")
    ol.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    ol.add_argument("--limit", type=nonneg_int, default=50)
    ol.set_defaults(func=cmd_outbox_list)
    of = out_sub.add_parser("flush", help="retry pending handoffs")
    of.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    of.add_argument("--timeout", type=nonneg_int, default=10)
    of.set_defaults(func=cmd_outbox_flush)

    p = sub.add_parser("find", help="find agents by capability or model tier")
    p.add_argument("--capability")
    p.add_argument("--model-tier")
    p.add_argument("--active-only", action="store_true")
    p.set_defaults(func=cmd_find)

    p = sub.add_parser("dlq", help="dead-letter queue controls")
    dlq_sub = p.add_subparsers(dest="dlq_command", required=True, parser_class=BusArgumentParser)
    lp = dlq_sub.add_parser("list", help="list dead/expired messages")
    lp.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    lp.add_argument("--status", choices=["dead", "expired"])
    lp.add_argument("--limit", type=nonneg_int, default=50)
    lp.set_defaults(func=cmd_dlq_list)
    rp = dlq_sub.add_parser("retry", help="requeue dead/expired message")
    rp.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    rp.add_argument("--message-id", required=True)
    rp.set_defaults(func=cmd_dlq_retry)
    dp = dlq_sub.add_parser("discard", help="discard dead/expired message")
    dp.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    dp.add_argument("--message-id", required=True)
    dp.set_defaults(func=cmd_dlq_discard)

    args = parser.parse_args()
    if args.db:
        os.environ["AGENT_BUS_DB"] = args.db
    if getattr(args, "from_agent", None) is None and args.command in {"send", "ask", "answer"}:
        raise SystemExit("error: --from/AGENT_BUS_AGENT is required")
    # Every identity that can reach a token path or a shell template gets the
    # same closed charset, not only the register command.
    for attr in ("agent_id", "from_agent", "owner"):
        value = getattr(args, attr, None)
        if value:
            validate_id("agent_id", value)
    # (Old code did a throwaway connect() here to "ensure init"; every command
    # handler connects anyway, so this just ran the whole schema migration
    # twice per invocation.)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
