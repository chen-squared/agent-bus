#!/usr/bin/env python3
"""Generic agent-bus command line (v0).

Shared SQLite + token-authenticated CLI. No central daemon: each agent
runs its own notifier (`bus_agent.py`) which polls the shared database.

This module is the stable entry point and compatibility surface. The
implementation lives in `buslib/`; names are re-exported here because
`bus_agent.py`, `bus_daemon.py`, and the probes import `bus` directly.
"""

from __future__ import annotations

from buslib import Bus, BusError, __version__  # noqa: F401
from buslib.config import *  # noqa: F401,F403
from buslib.validation import *  # noqa: F401,F403
from buslib.db import *  # noqa: F401,F403
from buslib.blobs import *  # noqa: F401,F403
from buslib.groups import *  # noqa: F401,F403
from buslib.remote import *  # noqa: F401,F403
from buslib.agents import *  # noqa: F401,F403
from buslib.maintenance import *  # noqa: F401,F403
from buslib.messaging import *  # noqa: F401,F403
from buslib.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
