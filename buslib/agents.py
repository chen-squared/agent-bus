"""Agent registration, discovery, liveness, and heartbeats."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

from .config import (
    json_dumps, load_token, machine_id, now, parse_capabilities, public_agent,
    row_to_dict, save_token, sha256_text, token_path,
)
from .db import connect, require_agent, require_auth
from .validation import validate_id

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


OPENCODE_CLI = resolve_opencode_cli()
