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

# These failures can occur after the receiver has already enqueued the input.
_UNKNOWN_OUTCOME_MARKERS = (
    "timed out", "timeout", "etimedout", "connection reset", "econnreset",
    "broken pipe", "socket hang up", "unexpected eof", "connection closed",
)
_CONNECT_FAILURE_MARKERS = (
    "connection refused", "econnrefused", "server is not running", "daemon is not running",
    "failed to connect", "unable to connect", "cannot connect", "could not connect",
    "network is unreachable", "host is unreachable", "no route to host",
)


class AdapterError(RuntimeError):
    pass


class TransientAdapterError(AdapterError):
    """Target is temporarily unavailable and did not accept the input."""


class DeliveryUnknownError(AdapterError):
    """The adapter cannot confirm whether the target accepted the input."""


def native_send_error(detail: str) -> AdapterError:
    """Interpret errors from a native send command after it has been launched."""
    lowered = detail.lower()
    if any(marker in lowered for marker in _UNKNOWN_OUTCOME_MARKERS):
        return DeliveryUnknownError(detail)
    if any(marker in lowered for marker in _CONNECT_FAILURE_MARKERS):
        return TransientAdapterError(detail)
    return AdapterError(detail)


def is_transient_error(exc: BaseException) -> bool:
    if isinstance(exc, TransientAdapterError):
        return True
    if not isinstance(exc, AdapterError) or isinstance(exc, DeliveryUnknownError):
        return False
    lowered = str(exc).lower()
    return any(marker in lowered for marker in _TRANSIENT_MARKERS)


class Adapter(Protocol):
    def inject(self, message: dict[str, Any]) -> None:
        ...


__all__ = ["Adapter", "AdapterError", "DeliveryUnknownError", "TransientAdapterError", "is_transient_error", "native_send_error"]
