"""Allow `python -m buslib.daemon` to start the HTTP daemon."""

from __future__ import annotations

from .server import main

if __name__ == "__main__":
    raise SystemExit(main())
