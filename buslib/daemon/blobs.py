"""Blob HEAD/GET routes for the localhost daemon."""

from __future__ import annotations

from typing import Any

from ..blobs import blob_path, sha256_file
from ..validation import SHA256_RE


def blob_sha(path: str) -> str | None:
    prefix = "/bus/blob/"
    path = path.split("?", 1)[0]
    if not path.startswith(prefix):
        return None
    sha = path[len(prefix):].strip("/")
    if not sha or "/" in sha or not SHA256_RE.fullmatch(sha):
        return None
    return sha


def serve_head(handler: Any, sha: str) -> None:
    p = blob_path(sha)
    if not p.exists() or sha256_file(p) != sha:
        handler._send(404, {"ok": False, "error": "blob not found"})
        return
    handler.send_response(200)
    handler.send_header("Content-Type", "application/octet-stream")
    handler.send_header("Content-Length", str(p.stat().st_size))
    handler.send_header("Accept-Ranges", "bytes")
    handler.end_headers()


def serve_get(handler: Any, sha: str) -> None:
    p = blob_path(sha)
    if not p.exists() or sha256_file(p) != sha:
        handler._send(404, {"ok": False, "error": "blob not found"})
        return
    size = p.stat().st_size
    range_header = handler.headers.get("Range")
    start = 0
    end = size - 1
    if range_header and range_header.lower().startswith("bytes="):
        spec = range_header.split("=", 1)[1].strip()
        parts = spec.split("-", 1)
        try:
            if parts[0]:
                start = int(parts[0])
            # Suffix range `bytes=-N` means the LAST N bytes; the old
            # parse returned the FIRST N with a 206 and a wrong
            # Content-Range, i.e. silently wrong data.
            elif parts[1]:
                start = max(0, size - int(parts[1]))
            if parts[0] and parts[1]:
                end = int(parts[1])
        except ValueError:
            handler._send(416, {"ok": False, "error": "invalid range"})
            return
        if start < 0 or end >= size or start > end:
            handler.send_response(416)
            handler.send_header("Content-Range", f"bytes */{size}")
            handler.end_headers()
            return
        handler.send_response(206)
        handler.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        handler.send_header("Content-Length", str(end - start + 1))
    else:
        handler.send_response(200)
        handler.send_header("Content-Length", str(size))
    handler.send_header("Content-Type", "application/octet-stream")
    handler.send_header("Accept-Ranges", "bytes")
    handler.end_headers()
    with open(p, "rb") as f:
        f.seek(start)
        remaining = end - start + 1
        while remaining > 0:
            chunk = f.read(min(1024 * 1024, remaining))
            if not chunk:
                break
            handler.wfile.write(chunk)
            remaining -= len(chunk)


__all__ = ["blob_sha", "serve_get", "serve_head"]
