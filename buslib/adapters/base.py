"""Adapter protocol and shared errors."""

from __future__ import annotations

from typing import Any, Protocol

_TRANSIENT_MARKERS = (
    "busy",
    "connection refused",
    "econnrefused",
    "failed to connect",
    "unable to connect",
    "cannot connect",
    "could not connect",
    "connection reset",
    "econnreset",
    "broken pipe",
    "socket hang up",
    "temporarily unavailable",
    "service unavailable",
    "timed out",
    "etimedout",
    "unreachable",
    "server is not running",
    "daemon is not running",
)


class AdapterError(RuntimeError):
    pass


class TransientAdapterError(AdapterError):
    """Target is temporarily busy, offline, or restarting; defer without consuming retry budget."""


def is_transient_error(exc: BaseException) -> bool:
    if isinstance(exc, TransientAdapterError):
        return True
    if not isinstance(exc, AdapterError):
        return False
    lowered = str(exc).lower()
    return any(marker in lowered for marker in _TRANSIENT_MARKERS)


class Adapter(Protocol):
    def inject(self, message: dict[str, Any]) -> None:
        ...


__all__ = ["Adapter", "AdapterError", "TransientAdapterError", "is_transient_error"]
