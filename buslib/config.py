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


def _source_checkout() -> Path | None:
    """True only for an editable/source checkout.

    A wheel install has no pyproject.toml next to the package. Keeping state in
    the source checkout preserves the existing development/live bus, while a
    normal `pip install agent-bus` gets a per-user state directory instead of
    writing inside site-packages.
    """
    root = Path(__file__).resolve().parent.parent
    if (root / "pyproject.toml").is_file() and (root / "buslib").is_dir():
        return root
    return None


def _user_data_dir() -> Path:
    if sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    elif os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
    else:
        base = Path(os.environ.get("XDG_DATA_HOME") or (Path.home() / ".local" / "share"))
    return (base / "agent-bus").expanduser().resolve()


def resolve_home() -> tuple[Path, str]:
    """Return (state directory, human-readable source).

    AGENT_BUS_HOME always wins. A source checkout keeps state beside the code
    for compatibility; an installed package uses the platform user-data
    directory. There is deliberately no cwd fallback: the bus is meant to be
    one stable world per machine/user, not a different bus per shell.
    """
    env_home = os.environ.get("AGENT_BUS_HOME")
    if env_home:
        return Path(env_home).expanduser().resolve(), "env:AGENT_BUS_HOME"
    source = _source_checkout()
    if source is not None:
        return source, "source-checkout"
    return _user_data_dir(), "user-data"


BUS_HOME, HOME_SOURCE = resolve_home()


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


def blob_dir_path() -> Path:
    return Path(os.environ.get("AGENT_BUS_BLOB_DIR", str(BUS_HOME / "blobs"))).expanduser().resolve()


def run_dir() -> Path:
    return Path(os.environ.get("AGENT_BUS_RUN_DIR", str(BUS_HOME / "run"))).expanduser().resolve()


def log_dir() -> Path:
    return Path(os.environ.get("AGENT_BUS_LOG_DIR", str(BUS_HOME / "logs"))).expanduser().resolve()


def ensure_home() -> Path:
    """Create the state directories private to the current unix user."""
    BUS_HOME.mkdir(parents=True, exist_ok=True)
    chmod_private(BUS_HOME, 0o700)
    token_dir = Path(os.environ.get("AGENT_BUS_TOKEN_DIR", str(TOKEN_DIR))).expanduser()
    for directory in (token_dir, blob_dir_path(), run_dir(), log_dir()):
        directory.mkdir(parents=True, exist_ok=True)
        chmod_private(directory, 0o700)
    return BUS_HOME


def home_info() -> dict[str, Any]:
    return {
        "home": str(BUS_HOME),
        "source": HOME_SOURCE,
        "db": str(Path(os.environ.get("AGENT_BUS_DB", DEFAULT_DB)).expanduser().resolve()),
        "tokens": str(Path(os.environ.get("AGENT_BUS_TOKEN_DIR", str(TOKEN_DIR))).expanduser().resolve()),
        "blobs": str(blob_dir_path()),
        "audit_log": str(Path(os.environ.get("AGENT_BUS_AUDIT_LOG", str(AUDIT_LOG))).expanduser().resolve()),
        "run": str(run_dir()),
        "logs": str(log_dir()),
    }


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
    "HOME_SOURCE",
    "NUDGE_FIRST_SECONDS",
    "NUDGE_STEP_SECONDS",
    "TOKEN_DIR",
    "audit_log",
    "blob_dir_path",
    "chmod_private",
    "ensure_home",
    "home_info",
    "json_dumps",
    "load_token",
    "log_dir",
    "machine_id",
    "now",
    "parse_capabilities",
    "public_agent",
    "resolve_home",
    "row_to_dict",
    "run_dir",
    "save_token",
    "sha256_text",
    "token_path",
]
