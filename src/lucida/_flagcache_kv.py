"""Optional shared definitions cache over a user-supplied Redis-style client (no redis dependency).

`RedisFlagDefinitionCache(client)` takes anything with `get(key)` and `set(key, value, ex=seconds, nx=bool)`: redis-py,
valkey, fakeredis, or a small adapter around another key-value store. The worker that wins the `nx` lock fetches and
publishes; the others read the stored copy.
"""

from __future__ import annotations

import json
from typing import Any, Protocol

from ._eval import Flag


class RedisLike(Protocol):
    def get(self, name: str) -> str | bytes | None: ...

    def set(self, name: str, value: str, *, ex: int | None = None, nx: bool = False) -> Any: ...


class RedisFlagDefinitionCache:
    """`FlagDefinitionCacheProvider` backed by a Redis-style client."""

    def __init__(self, client: RedisLike, *, prefix: str = "lucida:flags", ttl: int = 300, lock_ttl: int = 30) -> None:
        self._r, self._prefix, self._ttl, self._lock_ttl = client, prefix, ttl, lock_ttl

    def flag_definitions(self) -> list[Flag] | None:
        raw = self._r.get(f"{self._prefix}:defs")
        if not raw:
            return None
        try:
            v = json.loads(raw)
        except ValueError:
            return None
        return v if isinstance(v, list) else None

    def should_fetch_flag_definitions(self) -> bool:
        return bool(self._r.set(f"{self._prefix}:lock", "1", ex=self._lock_ttl, nx=True))

    def on_flag_definitions_received(self, definitions: list[Flag]) -> None:
        self._r.set(f"{self._prefix}:defs", json.dumps(definitions), ex=self._ttl)

    def shutdown(self) -> None:
        return None
