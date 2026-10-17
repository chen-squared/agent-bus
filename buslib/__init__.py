"""Agent-bus implementation package.

The supported public surfaces are the `agent-bus` console script and the small
`Bus` client below. Internal modules are intentionally importable for embedding
and testing, but the CLI remains the source of truth for auth and validation.
"""

from .api import Bus, BusError

__version__ = "0.1.0"

__all__ = ["Bus", "BusError", "__version__"]
