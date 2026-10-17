#!/usr/bin/env python3
"""Per-machine bus daemon entry point.

The implementation lives in `buslib.daemon`; this file stays as the stable
script invocation used by existing tunnels and process managers.
"""

from __future__ import annotations

from buslib.daemon import DaemonHandler, main

__all__ = ["DaemonHandler", "main"]

if __name__ == "__main__":
    raise SystemExit(main())
