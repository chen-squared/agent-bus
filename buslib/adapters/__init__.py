"""Delivery adapters for the per-agent notifier."""

from .antigravity import AntigravityAdapter
from .base import Adapter, AdapterError, DeliveryUnknownError, TransientAdapterError, is_transient_error
from .codex import CodexAdapter
from .command import CommandAdapter
from .opencode import OpencodeAdapter
from .tmux import TmuxAdapter

__all__ = [
    "Adapter",
    "AdapterError",
    "AntigravityAdapter",
    "CodexAdapter",
    "CommandAdapter",
    "DeliveryUnknownError",
    "OpencodeAdapter",
    "TmuxAdapter",
    "TransientAdapterError",
    "is_transient_error",
]
