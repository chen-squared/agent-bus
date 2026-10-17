"""Closed-charset and numeric argument validation."""

from __future__ import annotations

import argparse
import re


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


__all__ = [
    "ID_RE",
    "SHA256_RE",
    "nonneg_float",
    "nonneg_int",
    "validate_id",
    "validate_sha256",
]
