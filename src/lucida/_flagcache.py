"""Flag definition cache provider: lets many workers share one definitions fetch (Redis, KV, files...).

Implement the four methods; `Lucida(..., flag_definition_cache=provider)`. Before each poll the client asks
`should_fetch_flag_definitions()`: when False it adopts `flag_definitions()` instead of calling the server (the
worker holding the lock does the fetch and calls back through `on_flag_definitions_received`). A provider that
raises is treated as "fetch yourself". No Redis dependency here; see the README for a Redis example.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Protocol

from ._eval import Flag


class FlagDefinitionCacheProvider(Protocol):
    def flag_definitions(self) -> list[Flag] | None:
        """Cached definitions, or None when nothing is cached."""

    def should_fetch_flag_definitions(self) -> bool:
        """True when this process should call the server (e.g. it won the fetch lock)."""

    def on_flag_definitions_received(self, definitions: list[Flag]) -> None:
        """Store freshly fetched definitions for the other workers."""

    def shutdown(self) -> None:
        """Release locks/connections."""


class InMemoryFlagDefinitionCache:
    """Reference implementation: process-local, fetches again once `ttl` seconds have passed."""

    def __init__(self, ttl: float = 30.0) -> None:
        self._ttl, self._at = ttl, 0.0
        self._defs: list[Flag] | None = None
        self._lock = threading.Lock()

    def flag_definitions(self) -> list[Flag] | None:
        with self._lock:
            return self._defs

    def should_fetch_flag_definitions(self) -> bool:
        with self._lock:
            return self._defs is None or time.monotonic() - self._at >= self._ttl

    def on_flag_definitions_received(self, definitions: list[Flag]) -> None:
        with self._lock:
            self._defs, self._at = definitions, time.monotonic()

    def shutdown(self) -> None:
        return None


def adopt_cached(provider: Any) -> list[Flag] | None:
    """Definitions to use instead of fetching, or None when this process must fetch."""
    if provider is None:
        return None
    try:
        return None if provider.should_fetch_flag_definitions() else provider.flag_definitions()
    except Exception:  # noqa: BLE001
        return None


def publish_cached(provider: Any, flags: list[Flag]) -> None:
    if provider is None:
        return
    try:
        provider.on_flag_definitions_received(flags)
    except Exception:  # noqa: BLE001
        pass
