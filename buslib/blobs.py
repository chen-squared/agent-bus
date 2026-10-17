"""Content-addressed attachment storage and blob commands."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import urllib.request
from pathlib import Path
from typing import Any

from .config import BUS_HOME, chmod_private, json_dumps, now
from .db import connect
from .validation import validate_sha256


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
