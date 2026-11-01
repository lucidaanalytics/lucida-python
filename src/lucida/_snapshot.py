"""Evaluate-once flag snapshots, `$feature/<key>` event properties and the per-request device scope."""

from __future__ import annotations

import contextlib
import contextvars
import threading
from collections.abc import Iterable, Iterator
from typing import Any

FlagValue = bool | str

_device: contextvars.ContextVar[str | None] = contextvars.ContextVar("lucida_device_id", default=None)


@contextlib.contextmanager
def device_scope(device_id: str) -> Iterator[None]:
    """Within the block, flag calls bucket device-aggregated flags on `device_id` (thread and asyncio safe)."""
    token = _device.set(device_id)
    try:
        yield
    finally:
        _device.reset(token)


def scoped_device_id() -> str | None:
    return _device.get()


def flag_event_props(props: dict[str, Any] | None, values: dict[str, FlagValue]) -> dict[str, Any]:
    """`$feature/<key>` per flag plus sorted `$active_feature_flags`; caller properties win."""
    if not values:
        return dict(props or {})
    out: dict[str, Any] = {f"$feature/{k}": v for k, v in values.items()}
    out["$active_feature_flags"] = sorted(k for k, v in values.items() if v is not False)
    out.update(props or {})
    return out


class FlagSnapshot:
    """Flags evaluated once for one distinct id. Reads never call the network or emit exposure events."""

    def __init__(self, values: dict[str, FlagValue], payloads: dict[str, Any] | None = None) -> None:
        self.values = dict(values)
        self.payloads = dict(payloads or {})
        self._accessed: set[str] = set()
        self._lock = threading.Lock()

    def get_flag(self, key: str) -> FlagValue | None:
        with self._lock:
            self._accessed.add(key)
        return self.values.get(key)

    def is_enabled(self, key: str) -> bool:
        v = self.get_flag(key)
        return v is not None and v is not False

    def get_payload(self, key: str) -> Any:
        return self.payloads.get(key)

    def keys(self) -> list[str]:
        return sorted(self.values)

    def only(self, keys: Iterable[str]) -> FlagSnapshot:
        ks = list(keys)
        return FlagSnapshot({k: self.values[k] for k in ks if k in self.values}, {k: self.payloads[k] for k in ks if k in self.payloads})

    def only_accessed(self) -> FlagSnapshot:
        with self._lock:
            ks = list(self._accessed)
        return self.only(ks)

    def event_props(self, props: dict[str, Any] | None = None) -> dict[str, Any]:
        return flag_event_props(props, self.values)
