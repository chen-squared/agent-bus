"""Adapter protocol and shared errors."""

from __future__ import annotations

from typing import Any, Protocol


class AdapterError(RuntimeError):
    pass


class Adapter(Protocol):
    def inject(self, message: dict[str, Any]) -> None:
        ...
