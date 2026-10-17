"""Localhost daemon for authenticated cross-machine handoff and blob serving."""

from .server import DaemonHandler, main

__all__ = ["DaemonHandler", "main"]
