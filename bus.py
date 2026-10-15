#!/usr/bin/env python3
"""Generic agent-bus command line (v0).

Shared SQLite + token-authenticated CLI. No central daemon: each agent
runs its own notifier (`bus_agent.py`) which polls the shared database.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import platform
import secrets
import sqlite3
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

DEFAULT_DB = "/Users/chen/Developer/agent-bus/bus.db"
TOKEN_DIR = Path("/Users/chen/Developer/agent-bus/tokens")


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
    conn.execute("UPDATE agents SET machine_id=COALESCE(NULLIF(machine_id,'unknown'),?)", (machine_id(),))
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='group_read_state'").fetchone():
        if "last_digest_seq" not in columns("group_read_state"):
            conn.execute("ALTER TABLE group_read_state ADD COLUMN last_digest_seq INTEGER NOT NULL DEFAULT 0")
    for col, ddl in (
        ("expects_response", "ALTER TABLE messages ADD COLUMN expects_response INTEGER NOT NULL DEFAULT 0"),
        ("response_deadline", "ALTER TABLE messages ADD COLUMN response_deadline REAL"),
        ("nudge_count", "ALTER TABLE messages ADD COLUMN nudge_count INTEGER NOT NULL DEFAULT 0"),
    ):
        if col not in columns("messages"):
            conn.execute(ddl)
    conn.commit()


def now() -> float:
    return time.time()

NUDGE_FIRST_SECONDS = max(1, int(os.getenv("AGENT_BUS_NUDGE_FIRST", "60") or "60"))
NUDGE_STEP_SECONDS = max(1, int(os.getenv("AGENT_BUS_NUDGE_STEP", "60") or "60"))


def token_path(agent_id: str) -> Path:
    override = os.environ.get("AGENT_BUS_TOKEN_FILE")
    if override:
        return Path(override).expanduser()
    return (TOKEN_DIR / f"{agent_id}.token").expanduser()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


AUDIT_LOG = Path("/Users/chen/Developer/agent-bus/bus_audit.log")


def audit_log(event: str, **fields: Any) -> None:
    line = json_dumps({"time": now(), "event": event, **{k: v for k, v in fields.items() if v is not None}})
    print(line, file=sys.stderr)
    try:
        with open(AUDIT_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
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
    p.write_text(token + "\n")
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


def participants_arg(value: str | None) -> list[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def blob_dir() -> Path:
    return Path(os.environ.get("AGENT_BUS_BLOB_DIR", "/Users/chen/Developer/agent-bus/blobs")).expanduser().resolve()


def blob_path(sha256: str) -> Path:
    root = blob_dir()
    root.mkdir(parents=True, exist_ok=True)
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
    return {"sha256": digest, "size": size, "name": p.name, "path": str(destination)}


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
    sha = att["sha256"]
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
    p = blob_path(args.sha256)
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
    grace = args.grace_seconds or 86400
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
    conn = connect()
    preexisting = conn.execute(
        "SELECT token_hash FROM agents WHERE agent_id=?", (args.agent_id,)
    ).fetchone()
    token = None
    if preexisting is None:
        token = load_token(args.agent_id)
    if token is None:
        token = secrets.token_urlsafe(32)
        save_token(args.agent_id, token)
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
                "created_token": token is not None,
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
    existing = conn.execute("SELECT 1 FROM threads WHERE thread_id=?", (thread_id,)).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO threads(thread_id,participants,created_by,created_at,status) VALUES (?,?,?,?,?)",
            (thread_id, json_dumps(participants), created_by, now(), "open"),
        )
    elif participants:
        row = conn.execute("SELECT participants FROM threads WHERE thread_id=?", (thread_id,)).fetchone()
        old = set(json.loads(row["participants"] or "[]"))
        merged = sorted(old | set(participants))
        conn.execute("UPDATE threads SET participants=? WHERE thread_id=?", (json_dumps(merged), thread_id))


def cmd_message_show(args: argparse.Namespace) -> int:
    conn = connect()
    require_auth(conn, args.agent_id)
    row = conn.execute("SELECT * FROM messages WHERE message_id=?", (args.message_id,)).fetchone()
    if row is None or ((args.message_id or "").startswith("group-digest-") and args.agent_id not in {row["from_agent"], row["to_agent"]}):

        # Synthetic digest ids are shared across recipients, so a persisted
        # row can only be owned by one agent. Fall back to reconstructing the
        # digest from the group itself for any active member.
        syn = args.message_id[len("group-digest-"):]
        m = re.match(r"^(?P<group>.+)-(?P<seq>\d+)(?:-(?P<agent>.+))?$", syn)
        if m:
            group_id = m.group("group")
            last_seq = int(m.group("seq"))
        else:
            group_id, last_seq = syn, None
        _group_require_member(conn, group_id, args.agent_id)
        if conn.execute("SELECT 1 FROM groups WHERE group_id=?", (group_id,)).fetchone() is None:
            conn.close()
            raise SystemExit(f"error: unknown group {group_id!r}")
        group = conn.execute("SELECT topic FROM groups WHERE group_id=?", (group_id,)).fetchone()
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
        group_rows = {
            r["group_id"]
            for r in conn.execute("SELECT DISTINCT group_id FROM group_messages WHERE thread_id=? OR thread_id IS NULL", ()).fetchall()
        }
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
        peer_row = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        if peer_row is None:
            conn.close()
            raise SystemExit(f"error: unknown peer {remote_peer!r}; run `bus peer add`")
        args.to_agent = remote_to
        target_stale = False
    else:
        to_row = require_agent(conn, args.to_agent)
        target_stale = is_stale(to_row, now())
    # Make sure from can read token; auth done above.
    now_ts = now()
    if not (args.text or "").strip():
        conn.close()
        raise SystemExit("error: message text must not be empty")
    ttl = args.ttl
    expires = now_ts + ttl if ttl else None
    thread_id = args.thread_id or str(uuid.uuid4())
    ensure_thread(conn, thread_id, args.from_agent, [args.from_agent, args.to_agent])
    attachments = []
    if args.attachment:
        for raw in args.attachment.split(","):
            raw = raw.strip()
            if not raw:
                continue
            p = Path(raw).expanduser()
            if not p.exists():
                raise SystemExit(f"error: attachment not found: {raw}")
            if p.stat().st_size > 10 * 1024 * 1024:
                raise SystemExit(f"error: attachment too large (>10MB): {raw}")
            if remote_peer is not None:
                manifest = store_blob_from_path(p)
                manifest = {
                    "sha256": manifest["sha256"],
                    "size": manifest["size"],
                    "name": manifest["name"],
                    "origin": p.name,
                }
                attachments.append(manifest)
            else:
                manifest = store_blob_from_path(p)
                manifest = {
                    "sha256": manifest["sha256"],
                    "size": manifest["size"],
                    "name": manifest["name"],
                    "origin": p.name,
                    "path": str(p.resolve()),
                }
                attachments.append(manifest)
    text_bytes = len(args.text.encode("utf-8"))
    if text_bytes > 64 * 1024:
        print("error: message text exceeds 64KB", file=sys.stderr)
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
    if args.idempotency_key:
        found = conn.execute(
            "SELECT message_id,thread_id FROM messages WHERE idempotency_key=?", (args.idempotency_key,)
        ).fetchone()
        if found is not None:
            conn.close()
            print(json_dumps({"ok": True, "duplicate": True, "message_id": found["message_id"], "thread_id": found["thread_id"]}))
            return 0
    message_id = str(uuid.uuid4())
    attached = attachments
    message_type = args.message_type or "text"
    if message_type == "ask":
        message_type = "ask"
    try:
        conn.execute(
            """
            INSERT INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,message_type,urgency,
                payload,attachments,ttl_seconds,expires_at,retry_count,status,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                message_id,
                thread_id,
                args.idempotency_key,
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
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        conn.close()
        raise SystemExit(f"error: insert failed: {exc}")
    conn.commit()
    audit_log("send", from_agent=args.from_agent, to_agent=args.to_agent, message_id=message_id, thread_id=thread_id)
    remote_result = None
    if remote_peer is not None:
        row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
        outbox_id = str(uuid.uuid4())
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
        conn.commit()
        peer = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        ok, receipt = _handoff_peer(row_to_dict(peer), row_to_dict(row), timeout=10)
        new_status = "sent" if ok else "failed"
        conn.execute(
            "UPDATE outbox SET status=?, receipt=?, attempts=attempts+1, last_error=?, updated_at=?, delivered_at=? WHERE outbox_id=?",
            (new_status, receipt, None if ok else str(receipt)[:500], now(), now() if ok else None, outbox_id),
        )
        conn.commit()
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
    sweep_expired(conn)
    sql = "SELECT * FROM messages WHERE to_agent=?"
    params: list[Any] = [agent_id]
    if status == "undelivered":
        sql += " AND status IN ('queued','delivered') AND acked_at IS NULL"
    elif status and status != "all":
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
        item = row_to_dict(r)
        item["stale"] = is_stale(r, ts)
        item["heartbeat_age_seconds"] = (ts - float(r["last_heartbeat"])) if r["last_heartbeat"] else None
        items.append(item)
    print(json_dumps({"ok": True, "agents": items}))
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


def cmd_sweep(_args: argparse.Namespace) -> int:
    conn = connect()
    stale_agents = sweep_agents(conn)
    nudged, stale = sweep_nudges(conn)
    expired, dead = sweep_expired(conn)
    print(json_dumps({"ok": True, "expired": expired, "dead": dead, "stale_agents": stale_agents, "nudged": nudged, "stale_asks": stale}))
    conn.close()
    return 0


def cmd_ask(args: argparse.Namespace) -> int:
    ns = argparse.Namespace(
        from_agent=args.from_agent,
        to_agent=args.to_agent,
        text=args.text,
        thread_id=args.thread_id,
        message_type="ask",
        urgency=args.urgency or "normal",
        ttl=args.ttl if args.ttl is not None else 300,
        idempotency_key=args.idempotency_key,
        attachment=args.attachment,
        metadata=args.metadata,
    )
    # Reuse send logic by calling internal
    conn = connect()
    require_auth(conn, args.from_agent)
    remote_peer = None
    if "@" in args.to_agent:
        remote_to, remote_peer = args.to_agent.split("@", 1)
        peer_row = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        if peer_row is None:
            conn.close()
            raise SystemExit(f"error: unknown peer {remote_peer!r}")
        args.to_agent = remote_to
    else:
        require_agent(conn, args.to_agent)
    now_ts = now()
    thread_id = args.thread_id or str(uuid.uuid4())
    ensure_thread(conn, thread_id, args.from_agent, [args.from_agent, args.to_agent])
    attachments = []
    if args.attachment:
        for raw in args.attachment.split(","):
            raw = raw.strip()
            if raw:
                p = Path(raw).expanduser()
                if not p.exists():
                    conn.close()
                    raise SystemExit(f"error: attachment not found: {raw}")
                if remote_peer is not None:
                    manifest = store_blob_from_path(p)
                    manifest = {
                        "sha256": manifest["sha256"],
                        "size": manifest["size"],
                        "name": manifest["name"],
                        "origin": p.name,
                    }
                    attachments.append(manifest)
                else:
                    attachments.append(str(p.resolve()))
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
    message_id = str(uuid.uuid4())
    payload = {"text": args.text, "kind": "ask"}
    if args.metadata:
        try:
            payload["metadata"] = json.loads(args.metadata)
        except json.JSONDecodeError:
            payload["metadata"] = {"note": args.metadata}
    conn.execute(
        """
        INSERT INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,message_type,urgency,
            payload,attachments,ttl_seconds,expires_at,expects_response,response_deadline,nudge_count,retry_count,status,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            message_id,
            thread_id,
            args.idempotency_key,
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
            now_ts,
        ),
    )
    conn.commit()
    audit_log("send", from_agent=args.from_agent, to_agent=args.to_agent, message_id=message_id, thread_id=thread_id)
    remote_result = None
    if remote_peer is not None:
        row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
        outbox_id = str(uuid.uuid4())
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
        conn.commit()
        peer = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        ok, receipt = _handoff_peer(row_to_dict(peer), row_to_dict(row), timeout=10)
        conn.execute(
            "UPDATE outbox SET status=?, receipt=?, attempts=attempts+1, last_error=?, updated_at=?, delivered_at=? WHERE outbox_id=?",
            ("sent" if ok else "failed", receipt, None if ok else str(receipt)[:500], now(), now() if ok else None, outbox_id),
        )
        conn.commit()
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
    timeout = args.timeout or 120
    deadline = time.time() + timeout
    answer_row = None
    stale = False
    while time.time() < deadline:
        sweep_nudges(conn)
        sweep_expired(conn)
        status_row = conn.execute("SELECT status FROM messages WHERE message_id=?", (message_id,)).fetchone()
        if status_row and status_row["status"] == "stale":
            stale = True
            break
        rows = conn.execute(
            """
            SELECT * FROM messages
            WHERE thread_id=? AND message_type='answer' AND from_agent=? AND created_at >= ?
            ORDER BY created_at ASC LIMIT 1
            """,
            (thread_id, args.to_agent, now_ts),
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
        peer_row = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        if peer_row is None:
            conn.close()
            raise SystemExit(f"error: unknown peer {remote_peer!r}")
        to = remote_to
    else:
        require_agent(conn, to)
    now_ts = now()
    ttl = args.ttl if args.ttl is not None else 300
    expires = now_ts + ttl
    message_id = str(uuid.uuid4())
    payload = {"text": args.text, "kind": "answer"}
    if args.metadata:
        try:
            payload["metadata"] = json.loads(args.metadata)
        except json.JSONDecodeError:
            payload["metadata"] = {"note": args.metadata}
    answer_to = args.answer_to
    if not answer_to:
        row = conn.execute(
            "SELECT message_id FROM messages WHERE thread_id=? AND message_type='ask' ORDER BY created_at DESC LIMIT 1",
            (args.thread_id,),
        ).fetchone()
        answer_to = row["message_id"] if row else None
    conn.execute(
        """
        INSERT INTO messages (message_id,thread_id,idempotency_key,from_agent,to_agent,message_type,urgency,
            payload,attachments,ttl_seconds,expires_at,retry_count,status,answer_to_message_id,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            message_id,
            args.thread_id,
            args.idempotency_key,
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
            now_ts,
        ),
    )
    conn.commit()
    audit_log("send", from_agent=args.from_agent, to_agent=args.to_agent, message_id=message_id, thread_id=args.thread_id)
    remote_result = None
    if remote_peer is not None:
        row = conn.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
        outbox_id = str(uuid.uuid4())
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
        conn.commit()
        peer = conn.execute("SELECT * FROM peers WHERE peer_name=?", (remote_peer,)).fetchone()
        ok, receipt = _handoff_peer(row_to_dict(peer), row_to_dict(row), timeout=10)
        new_status = "sent" if ok else "failed"
        conn.execute(
            "UPDATE outbox SET status=?, receipt=?, attempts=attempts+1, last_error=?, updated_at=?, delivered_at=? WHERE outbox_id=?",
            (new_status, receipt, None if ok else str(receipt)[:500], now(), now() if ok else None, outbox_id),
        )
        conn.commit()
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
    if row["status"] in {"dead", "stale"}:
        # active agents can revive; stale marker itself counts stale.
        pass
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
        item = row_to_dict(r)
        item["stale"] = is_stale(r, ts)
        item["heartbeat_age_seconds"] = (ts - float(r["last_heartbeat"])) if r["last_heartbeat"] else None
        result.append(item)
    print(json_dumps({"ok": True, "count": len(result), "agents": result}))
    conn.close()
    return 0


def cmd_dlq_list(args: argparse.Namespace) -> int:
    conn = connect()
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
    row = conn.execute("SELECT status FROM messages WHERE message_id=?", (args.message_id,)).fetchone()
    if row is None:
        conn.close()
        raise SystemExit(f"error: message {args.message_id} not found")
    if row["status"] not in {"dead", "expired"}:
        conn.close()
        raise SystemExit(f"error: message is {row['status']}, only dead/expired can retry")
    conn.execute(
        "UPDATE messages SET status='queued', retry_count=0, last_error=NULL WHERE message_id=?",
        (args.message_id,),
    )
    conn.commit()
    print(json_dumps({"ok": True, "message_id": args.message_id, "status": "queued"}))
    conn.close()
    return 0


def cmd_dlq_discard(args: argparse.Namespace) -> int:
    conn = connect()
    row = conn.execute("SELECT status FROM messages WHERE message_id=?", (args.message_id,)).fetchone()
    if row is None:
        conn.close()
        raise SystemExit(f"error: message {args.message_id} not found")
    if row["status"] not in {"dead", "expired"}:
        conn.close()
        raise SystemExit(f"error: message is {row['status']}; ack or cancel it instead")
    conn.execute("DELETE FROM messages WHERE message_id=?", (args.message_id,))
    conn.commit()
    print(json_dumps({"ok": True, "message_id": args.message_id, "discarded": True}))
    conn.close()
    return 0


def cmd_peer_add(args: argparse.Namespace) -> int:
    conn = connect()
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


def cmd_peer_list(_args: argparse.Namespace) -> int:
    conn = connect()
    rows = conn.execute("SELECT * FROM peers ORDER BY peer_name").fetchall()
    print(json_dumps({"ok": True, "peers": [row_to_dict(r) for r in rows]}))
    conn.close()
    return 0


def cmd_peer_remove(args: argparse.Namespace) -> int:
    conn = connect()
    conn.execute("DELETE FROM peers WHERE peer_name=?", (args.peer,))
    conn.commit()
    print(json_dumps({"ok": True, "removed": args.peer}))
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
                "peers": [row_to_dict(r) for r in peers],
            }
        )
    )
    conn.close()
    return 0


def cmd_outbox_list(args: argparse.Namespace) -> int:
    conn = connect()
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


def cmd_outbox_flush(args: argparse.Namespace) -> int:
    conn = connect()
    rows = conn.execute(
        "SELECT o.*, p.base_url, p.auth_token, p.peer_name AS pname FROM outbox o JOIN peers p ON o.peer_name=p.peer_name WHERE o.status IN ('pending','failed') ORDER BY o.created_at"
    ).fetchall()
    flushed = 0
    failed = 0
    for row in rows:
        peer = {
            "peer_name": row["pname"],
            "base_url": row["base_url"],
            "auth_token": row["auth_token"],
        }
        message_row = json.loads(row["payload"])
        ok, receipt = _handoff_peer(peer, message_row, timeout=args.timeout or 10)
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
    conn.commit()
    print(json_dumps({"ok": True, "flushed": flushed, "failed": failed}))
    conn.close()
    return 0


def _group_require_member(conn: sqlite3.Connection, group_id: str, agent_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM group_members WHERE group_id=? AND agent_id=?", (group_id, agent_id)).fetchone()
    if row is None:
        raise SystemExit(f"error: {agent_id} is not a member of {group_id}")
    if row["status"] != "active":
        raise SystemExit(f"error: {agent_id} is not active in {group_id}")
    return row


def cmd_group_create(args: argparse.Namespace) -> int:
    conn = connect()
    if not args.owner:
        args.owner = args.from_agent if args.from_agent else None
    now_ts = now()
    try:
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
                args.max_members or 50,
                args.rate_seconds or 2,
                args.ceiling_per_minute or 120,
                args.backlog_threshold or 50,
                "active",
                args.owner,
                now_ts,
                now_ts,
            ),
        )
        if args.owner:
            conn.execute(
                "INSERT INTO group_members (group_id,agent_id,role,rate_seconds,joined_at,status) VALUES (?,?,?,?,?,?)",
                (args.group_id, args.owner, "owner", args.rate_seconds or 2, now_ts, "active"),
            )
            conn.execute(
                "INSERT INTO group_read_state (group_id,agent_id,last_read_seq,updated_at) VALUES (?,?,?,?)",
                (args.group_id, args.owner, 0, now_ts),
            )
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise SystemExit(f"error: create group failed: {exc}")
    conn.commit()
    print(json_dumps({"ok": True, "group_id": args.group_id, "home_machine": machine_id()}))
    conn.close()
    return 0


def cmd_group_join(args: argparse.Namespace) -> int:
    conn = connect()
    group = conn.execute("SELECT * FROM groups WHERE group_id=?", (args.group_id,)).fetchone()
    if group is None:
        conn.close()
        raise SystemExit(f"error: unknown group {args.group_id}")
    count = conn.execute("SELECT COUNT(*) AS c FROM group_members WHERE group_id=? AND status='active'", (args.group_id,)).fetchone()["c"]
    if count >= group["max_members"]:
        conn.close()
        raise SystemExit(f"error: group {args.group_id} is full")
    rate = args.rate_seconds if args.rate_seconds is not None else group["rate_seconds"]
    now_ts = now()
    conn.execute(
        """
        INSERT INTO group_members (group_id,agent_id,role,rate_seconds,joined_at,status)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(group_id,agent_id) DO UPDATE SET
            role=excluded.role,
            rate_seconds=excluded.rate_seconds,
            joined_at=excluded.joined_at,
            status='active'
        """,
        (args.group_id, args.agent_id, args.role or "member", rate, now_ts, "active"),
    )
    conn.execute(
        "INSERT OR IGNORE INTO group_read_state (group_id,agent_id,last_read_seq,updated_at) VALUES (?,?,?,?)",
        (args.group_id, args.agent_id, 0, now_ts),
    )
    conn.commit()
    print(json_dumps({"ok": True, "group_id": args.group_id, "agent_id": args.agent_id}))
    conn.close()
    return 0


def cmd_group_leave(args: argparse.Namespace) -> int:
    conn = connect()
    conn.execute("UPDATE group_members SET status='left' WHERE group_id=? AND agent_id=?", (args.group_id, args.agent_id))
    conn.execute("DELETE FROM group_read_state WHERE group_id=? AND agent_id=?", (args.group_id, args.agent_id))
    conn.commit()
    print(json_dumps({"ok": True, "group_id": args.group_id, "agent_id": args.agent_id, "status": "left"}))
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
    conn = connect()
    if conn.execute("SELECT 1 FROM groups WHERE group_id=?", (args.group_id,)).fetchone() is None:
        conn.close()
        raise SystemExit(f"error: unknown group {args.group_id}")
    if args.agent_id:
        _group_require_member(conn, args.group_id, args.agent_id)
        state = conn.execute("SELECT last_read_seq FROM group_read_state WHERE group_id=? AND agent_id=?", (args.group_id, args.agent_id)).fetchone()
        since = state["last_read_seq"] if state else 0
        limit = args.limit or 50
        rows = conn.execute(
            "SELECT * FROM group_messages WHERE group_id=? AND seq>? ORDER BY seq ASC LIMIT ?",
            (args.group_id, since, limit),
        ).fetchall()
    else:
        conn.close()
        raise SystemExit("error: denied: --agent-id is required for group messages read")
    latest = conn.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM group_messages WHERE group_id=?", (args.group_id,)).fetchone()["seq"]
    print(json_dumps({"ok": True, "group_id": args.group_id, "latest_seq": latest, "count": len(rows), "messages": [row_to_dict(r) for r in rows]}))
    conn.close()
    return 0


def cmd_group_read(args: argparse.Namespace) -> int:
    conn = connect()
    _group_require_member(conn, args.group_id, args.agent_id)
    latest = conn.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM group_messages WHERE group_id=?", (args.group_id,)).fetchone()["seq"]
    seq = args.seq if args.seq is not None else latest
    if seq < 0 or seq > latest:
        conn.close()
        raise SystemExit(f"error: invalid seq {seq}, latest is {latest}")
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
    print(json_dumps({"ok": True, "group_id": args.group_id, "agent_id": args.agent_id, "last_read_seq": seq, "latest_seq": latest}))
    conn.close()
    return 0


def cmd_group_send(args: argparse.Namespace) -> int:
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
        conn.close()
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
    if not (args.text or "").strip():
        conn.close()
        raise SystemExit("error: group message text must not be empty")
    text_bytes = len(args.text.encode("utf-8"))
    if text_bytes > 64 * 1024:
        conn.close()
        raise SystemExit("error: message text exceeds 64KB")
    if text_bytes > 1024 * 1024:
        pass
    rate = member["rate_seconds"] or group["rate_seconds"] or 2
    if member["last_sent_at"] and (now_ts - float(member["last_sent_at"])) < rate:
        wait = round(rate - (now_ts - float(member["last_sent_at"])), 3)
        conn.close()
        raise SystemExit(json_dumps({"error": "rate limited", "retry_after_seconds": wait}))
    ceiling = group["ceiling_per_minute"] or 120
    recent = conn.execute(
        "SELECT COUNT(*) AS c FROM group_messages WHERE group_id=? AND created_at>?",
        (args.group_id, now_ts - 60),
    ).fetchone()["c"]
    if recent >= ceiling:
        conn.close()
        raise SystemExit(json_dumps({"error": "group ceiling reached", "retry_after_seconds": 5}))
    backlog = conn.execute(
        """
        SELECT COUNT(*) AS c FROM group_read_state grs
        JOIN group_members gm ON gm.group_id=grs.group_id AND gm.agent_id=grs.agent_id
        WHERE grs.group_id=? AND gm.status='active' AND grs.last_read_seq < ?
        """,
        (args.group_id, latest),
    ).fetchone()["c"]
    if backlog > (group["backlog_threshold"] or 50):
        conn.close()
        raise SystemExit(json_dumps({"error": "group backpressure: too many unread participants"}))
    if group["max_members"] and False:
        pass
    next_seq = latest + 1
    expires = now_ts + args.ttl if args.ttl else None
    message_id = str(uuid.uuid4())
    payload = {"text": args.text}
    soft_warning = "text exceeds 4KB soft cap; consider attachments" if text_bytes > 4 * 1024 else None
    try:
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
    except sqlite3.IntegrityError as exc:
        conn.close()
        raise SystemExit(f"error: insert failed: {exc}")
    conn.commit()
    result = {"ok": True, "group_id": args.group_id, "message_id": message_id, "seq": next_seq, "latest_seq": next_seq}
    if soft_warning:
        result["warning"] = soft_warning
    print(json_dumps(result))
    conn.close()
    return 0


def cmd_group_status(args: argparse.Namespace) -> int:
    conn = connect()
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
    """Return (nudges, stale) counts for overdue asks without a linked answer."""
    ts = now()
    nudges = 0
    stale = 0
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
        has_answer = conn.execute(
            "SELECT 1 FROM messages WHERE answer_to_message_id=? LIMIT 1",
            (ask["message_id"],),
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
    conn.commit()
    return nudges, stale


def main() -> int:
    parser = argparse.ArgumentParser(description="agent-bus generic message bus (v0)")
    parser.add_argument("--db", default=None, help="override database path")
    sub = parser.add_subparsers(dest="command", required=True)

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
    p.add_argument("--limit", type=int, default=50)
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
    m = p.add_subparsers(dest="message_command", required=True)
    ms = m.add_parser("show", help="show one message")
    ms.add_argument("--agent-id", required=True)
    ms.add_argument("--message-id", required=True)
    ms.set_defaults(func=cmd_message_show)

    p = sub.add_parser("agents", help="list agents")
    p.set_defaults(func=cmd_agents)

    p = sub.add_parser("sweep", help="expire TTL messages and dead-letter retries")
    p.set_defaults(func=cmd_sweep)

    p = sub.add_parser("ask", help="send an ask and optionally wait for answer")
    p.add_argument("--from", dest="from_agent", default=os.environ.get("AGENT_BUS_AGENT"))
    p.add_argument("--to", dest="to_agent", required=True)
    p.add_argument("--text", required=True)
    p.add_argument("--thread-id")
    p.add_argument("--urgency", default="normal")
    p.add_argument("--ttl", type=int, default=300)
    p.add_argument("--timeout", type=int, default=120)
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
    peer_sub = p.add_subparsers(dest="peer_command", required=True)
    pa = peer_sub.add_parser("add", help="add or update peer")
    pa.add_argument("--peer", required=True)
    pa.add_argument("--host")
    pa.add_argument("--base-url", required=True)
    pa.add_argument("--token", required=True)
    pa.set_defaults(func=cmd_peer_add)
    pl = peer_sub.add_parser("list", help="list peers")
    pl.set_defaults(func=cmd_peer_list)
    pr = peer_sub.add_parser("remove", help="remove peer")
    pr.add_argument("--peer", required=True)
    pr.set_defaults(func=cmd_peer_remove)

    p = sub.add_parser("status", help="show bus health depths")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("group", help="group chat commands (V3)")
    g = p.add_subparsers(dest="group_command", required=True)
    gc = g.add_parser("create", help="create group")
    gc.add_argument("--from", dest="from_agent", default=os.environ.get("AGENT_BUS_AGENT"))
    gc.add_argument("--group-id", required=True)
    gc.add_argument("--topic")
    gc.add_argument("--owner")
    gc.add_argument("--max-members", type=int, default=50)
    gc.add_argument("--rate-seconds", type=float, default=2)
    gc.add_argument("--ceiling-per-minute", type=int, default=120)
    gc.add_argument("--backlog-threshold", type=int, default=50)
    gc.add_argument("--mode", default="catch-up-gated")
    gc.set_defaults(func=cmd_group_create)
    gj = g.add_parser("join", help="join group")
    gj.add_argument("--group-id", required=True)
    gj.add_argument("--agent-id", required=True)
    gj.add_argument("--role", default="member")
    gj.add_argument("--rate-seconds", type=float)
    gj.set_defaults(func=cmd_group_join)
    gl = g.add_parser("leave", help="leave group")
    gl.add_argument("--group-id", required=True)
    gl.add_argument("--agent-id", required=True)
    gl.set_defaults(func=cmd_group_leave)
    glist = g.add_parser("list", help="list groups")
    glist.add_argument("--agent-id")
    glist.set_defaults(func=cmd_group_list)
    gm = g.add_parser("messages", help="list or read new group messages")
    gm.add_argument("--group-id", required=True)
    gm.add_argument("--agent-id")
    gm.add_argument("--limit", type=int, default=50)
    gm.set_defaults(func=cmd_group_messages)
    gr = g.add_parser("read", help="ack latest seq as read")
    gr.add_argument("--group-id", required=True)
    gr.add_argument("--agent-id", required=True)
    gr.add_argument("--seq", type=int)
    gr.set_defaults(func=cmd_group_read)
    gs = g.add_parser("send", help="send a group message")
    gs.add_argument("--from", dest="from_agent", required=True)
    gs.add_argument("--group-id", required=True)
    gs.add_argument("--text", required=True)
    gs.add_argument("--message-type", default="text")
    gs.add_argument("--urgency", default="normal")
    gs.add_argument("--ttl", type=int)
    gs.set_defaults(func=cmd_group_send)
    gst = g.add_parser("status", help="show group stats")
    gst.add_argument("--group-id", required=True)
    gst.set_defaults(func=cmd_group_status)

    p = sub.add_parser("blob", help="content-addressed blob store (for long attachments)")
    blob_sub = p.add_subparsers(dest="blob_command", required=True)
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
    bgc.add_argument("--grace-seconds", type=int, default=86400)
    bgc.add_argument("--force", action="store_true")
    bgc.set_defaults(func=cmd_blob_gc)

    p = sub.add_parser("outbox", help="cross-machine delivery queue")
    out_sub = p.add_subparsers(dest="outbox_command", required=True)
    ol = out_sub.add_parser("list", help="list outbox entries")
    ol.add_argument("--limit", type=int, default=50)
    ol.set_defaults(func=cmd_outbox_list)
    of = out_sub.add_parser("flush", help="retry pending handoffs")
    of.add_argument("--timeout", type=int, default=10)
    of.set_defaults(func=cmd_outbox_flush)

    p = sub.add_parser("find", help="find agents by capability or model tier")
    p.add_argument("--capability")
    p.add_argument("--model-tier")
    p.add_argument("--active-only", action="store_true")
    p.set_defaults(func=cmd_find)

    p = sub.add_parser("dlq", help="dead-letter queue controls")
    dlq_sub = p.add_subparsers(dest="dlq_command", required=True)
    lp = dlq_sub.add_parser("list", help="list dead/expired messages")
    lp.add_argument("--status", choices=["dead", "expired"])
    lp.add_argument("--limit", type=int, default=50)
    lp.set_defaults(func=cmd_dlq_list)
    rp = dlq_sub.add_parser("retry", help="requeue dead/expired message")
    rp.add_argument("--message-id", required=True)
    rp.set_defaults(func=cmd_dlq_retry)
    dp = dlq_sub.add_parser("discard", help="discard dead/expired message")
    dp.add_argument("--message-id", required=True)
    dp.set_defaults(func=cmd_dlq_discard)

    args = parser.parse_args()
    if args.db:
        os.environ["AGENT_BUS_DB"] = args.db
    if getattr(args, "from_agent", None) is None and args.command in {"send", "ask", "answer"}:
        raise SystemExit("error: --from/AGENT_BUS_AGENT is required")
    # Auth checks need db
    if args.command not in {"init"}:
        # ensure init
        try:
            connect().close()
        except Exception as exc:
            raise SystemExit(f"error: database unavailable: {exc}")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
