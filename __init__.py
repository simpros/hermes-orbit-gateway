"""Orbit chat platform plugin for Hermes Agent.

The plugin loader imports this directory as a package and calls :func:`register`;
the adapter itself lives in ``adapter.py`` and touches no core Hermes code.
"""

try:  # normal path: imported as a package by the plugin loader
    from .adapter import register
except ImportError:  # pragma: no cover - imported as a top-level module (tooling)
    from adapter import register  # type: ignore[no-redef]

__all__ = ["register"]
