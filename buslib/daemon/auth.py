"""Peer authentication for the localhost daemon."""

from __future__ import annotations

import secrets
import sqlite3

from ..db import connect


def read_bearer(headers) -> str:
    auth = headers.get("Authorization", "")
    return auth[len("Bearer ") :].strip() if auth.lower().startswith("bearer ") else ""


def authorized_peer(token: str) -> sqlite3.Row | None:
    if not token:
        return None
    conn = connect()
    # Constant-time compare; the old `==` over a full table scan was both
    # a timing side channel and duplicated inline in do_POST.
    try:
        for row in conn.execute("SELECT * FROM peers").fetchall():
            if row["auth_token"] and secrets.compare_digest(str(row["auth_token"]), token):
                return row
        return None
    finally:
        conn.close()


__all__ = ["authorized_peer", "read_bearer"]
