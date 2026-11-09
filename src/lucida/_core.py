"""Pure helpers shared by the sync and asyncio clients: wire events, encoding, retry policy."""

from __future__ import annotations

import gzip
import json
import os
import random
import time
from collections import OrderedDict
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, TypedDict

VERSION = "0.1.0"
LIB = "lucida-python"
MIN_GZIP_BYTES = 1024
MAX_BATCH = 1000
DEFAULT_HOST = "https://capture.lucidace.com"

Props = Mapping[str, Any]
WireEvent = dict[str, Any]


class ImportEvent(TypedDict, total=False):
    """One historical event for `import_events`. `distinct_id` and `event` are required."""

    distinct_id: str
    event: str
    properties: Props
    timestamp: datetime | str
    uuid: str
    set: Props
    set_once: Props


class LucidaError(Exception):
    """Raised by `import_events` when the server rejects a chunk."""


def uuid7() -> str:
    ms = int(time.time() * 1000)
    b = bytearray(ms.to_bytes(6, "big") + os.urandom(10))
    b[6] = 0x70 | (b[6] & 0x0F)
    b[8] = 0x80 | (b[8] & 0x3F)
    h = b.hex()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


def iso(t: datetime | str | None) -> str:
    if isinstance(t, str):
        return t
    t = t or datetime.now(timezone.utc)
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def make_event(
    distinct_id: str,
    event: str,
    properties: Props | None = None,
    *,
    groups: Mapping[str, str] | None = None,
    timestamp: datetime | str | None = None,
    uuid: str | None = None,
    set: Props | None = None,  # noqa: A002
    set_once: Props | None = None,
) -> WireEvent:
    e: WireEvent = {
        "uuid": uuid or uuid7(),
        "event": event,
        "distinct_id": distinct_id,
        "timestamp": iso(timestamp),
        "properties": {**(properties or {}), "$lib": LIB, "$lib_version": VERSION},
    }
    if set:
        e["$set"] = dict(set)
    if set_once:
        e["$set_once"] = dict(set_once)
    if groups:
        e["$groups"] = dict(groups)
    return e


def import_wire(e: ImportEvent) -> WireEvent:
    return make_event(
        e["distinct_id"],
        e["event"],
        e.get("properties"),
        timestamp=e.get("timestamp"),
        uuid=e.get("uuid"),
        set=e.get("set"),
        set_once=e.get("set_once"),
    )


def encode(url: str, payload: dict[str, Any]) -> tuple[str, bytes, dict[str, str]]:
    """JSON body; gzip above 1 KB flagged with `?compression=gzip`."""
    body = json.dumps(payload, separators=(",", ":")).encode()
    if len(body) >= MIN_GZIP_BYTES:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}compression=gzip", gzip.compress(body), {"Content-Type": "application/json"}
    return url, body, {"Content-Type": "application/json"}


def batch_payload(api_key: str | None, batch: list[WireEvent]) -> dict[str, Any]:
    p: dict[str, Any] = {"batch": batch, "sent_at": iso(None)}
    if api_key:
        p["api_key"] = api_key
    return p


def is_retryable(status: int) -> bool:
    return status in (0, 408, 429) or status >= 500


def parse_retry_after(v: str | None) -> float | None:
    try:
        return min(max(float(v), 0.0), 60.0) if v else None
    except ValueError:
        return None


def retry_delay(attempt: int, retry_after: float | None = None, base: float = 1.0, cap: float = 30.0) -> float:
    """Exponential backoff with jitter (50%..100% of the capped step); Retry-After wins if longer."""
    d: float = min(cap, base * 2**attempt) * (0.5 + 0.5 * random.random())  # noqa: S311
    return d if retry_after is None else max(d, retry_after)


class LruSet:
    """Bounded set: inserting past `max` evicts the least recently seen key."""

    def __init__(self, max_size: int) -> None:
        self._m: OrderedDict[str, None] = OrderedDict()
        self._max = max_size

    def add(self, key: str) -> bool:
        """True when the key was new."""
        had = self._m.pop(key, 0) is None
        self._m[key] = None
        if len(self._m) > self._max:
            self._m.popitem(last=False)
        return not had

    def __len__(self) -> int:
        return len(self._m)
