"""Offline test mode: fixture definitions, an in-memory capture sink and a transport that refuses the network."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from typing import Any

import httpx

from ._core import WireEvent
from ._eval import Flag

Fixture = str | os.PathLike[str] | Mapping[str, Any] | Sequence[Flag]
"""A file written by `lucida flags fixture`, a `{"flags": [...]}` body, or a list of definitions."""


def load_fixture(src: Fixture) -> list[Flag]:
    body: Any = src
    if isinstance(src, str | os.PathLike):
        with open(src, encoding="utf-8") as f:
            body = json.load(f)
    flags = body.get("flags") if isinstance(body, Mapping) else body
    if not isinstance(flags, Sequence) or isinstance(flags, str):
        raise ValueError('test_mode fixture must be {"flags": [...]} or a list of flag definitions')
    return [dict(f) for f in flags]


def no_network(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("network disabled in test_mode", request=request)


class Sink:
    """Stands in for the send queue: events are kept in `events`, nothing is sent."""

    def __init__(self) -> None:
        self.events: list[WireEvent] = []
        self.dropped = 0

    def add(self, e: WireEvent) -> None:
        self.events.append(e)

    def start(self) -> None:
        return None

    def flush(self, timeout: float | None = None) -> bool:
        return True

    def shutdown(self, timeout: float | None = 10.0) -> None:
        return None


class AsyncSink(Sink):
    async def flush(self, timeout: float | None = None) -> bool:  # type: ignore[override]
        return True

    async def shutdown(self, timeout: float | None = 10.0) -> None:  # type: ignore[override]
        return None
