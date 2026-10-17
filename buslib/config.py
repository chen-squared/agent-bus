"""Paths, constants, tokens, audit logging, and small serialization helpers."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any


BUS_HOME = Path(os.environ.get("AGENT_BUS_HOME", Path(__file__).resolve().parent.parent)).expanduser().resolve()


DEFAULT_DB = str(BUS_HOME / "bus.db")


TOKEN_DIR = BUS_HOME / "tokens"


def chmod_private(path: Path, mode: int = 0o600) -> None:
    """Best-effort tightening for state files. The bus assumes a single local
    unix user; a world-readable DB/blob store silently breaks that assumption."""
    try:
        os.chmod(path, mode)
    except (FileNotFoundError, PermissionError, OSError):
        pass


def now() -> float:
    return time.time()


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


def public_agent(item: dict[str, Any]) -> dict[str, Any]:
    """Agent rows are printed by `agents`/`find`, which are discovery commands
    with no auth. token_hash must never ride along."""
    item.pop("token_hash", None)
    return item


__all__ = [
    "AUDIT_LOG",
    "BUS_HOME",
    "DEFAULT_DB",
    "NUDGE_FIRST_SECONDS",
    "NUDGE_STEP_SECONDS",
    "TOKEN_DIR",
    "audit_log",
    "chmod_private",
    "json_dumps",
    "load_token",
    "machine_id",
    "now",
    "parse_capabilities",
    "public_agent",
    "row_to_dict",
    "save_token",
    "sha256_text",
    "token_path",
]
