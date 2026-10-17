#!/usr/bin/env python3
"""Per-agent notifier entry point.

The implementation lives in `buslib.notifier`; this file stays as the stable
script invocation used by existing process managers and pid files.
"""

from __future__ import annotations

from buslib.notifier import main

__all__ = ["main"]


if __name__ == "__main__":
    raise SystemExit(main())
