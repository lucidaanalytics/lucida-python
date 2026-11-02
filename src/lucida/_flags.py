"""Flag state shared by both clients: definitions, local resolution, remote parsing, exposure dedupe."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ._core import LruSet, Props
from ._eval import REQUIRES_SERVER, Flag, FlagResult, evaluate_all
from ._snapshot import scoped_device_id

FlagValue = bool | str


@dataclass
class FlagOptions:
    """Per-call inputs. `only_evaluate_locally` never calls the server; `send_exposure` is get_feature_flag only."""

    person_properties: Props | None = None
    groups: Mapping[str, str] | None = None
    group_properties: Mapping[str, Props] | None = None
    only_evaluate_locally: bool = False
    send_exposure: bool = True
    app_contexts: list[str] | None = None
    device_id: str | None = None
    # Pre-identify id of this person; the gateway links it so flags keep their bucket after identify.
    anon_distinct_id: str | None = None

    def unit(self) -> str | None:
        return self.device_id or scoped_device_id()


@dataclass
class Resolved:
    """One flag read: value, payload, variant, raw Lucida reason, condition index, request id (also `FlagDetails`)."""

    value: FlagValue
    payload: Any = None
    locally: bool = True
    request_id: str | None = None
    variant: str | None = None
    reason: str = ""
    condition_index: int = -1

    @property
    def evaluated_locally(self) -> bool:
        return self.locally

    @property
    def is_fallback(self) -> bool:
        return self.reason == FALLBACK_REASON


FlagDetails = Resolved
FALLBACK_REASON = "fallback"


def _local(r: FlagResult) -> Resolved:
    return Resolved(r.value, r.payload, True, None, r.variant, r.reason, r.condition_index)


@dataclass
class FlagState:
    defs: list[Flag] | None = None
    etag: str | None = None
    app_contexts: list[str] | None = None
    fallbacks: dict[str, FlagValue] = field(default_factory=dict)
    on_change: list[Callable[[], None]] = field(default_factory=list)
    on_stale: list[Callable[[], None]] = field(default_factory=list)
    _seen: LruSet = field(default_factory=lambda: LruSet(10_000))
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def local(self, distinct_id: str, o: FlagOptions) -> dict[str, FlagResult] | None:
        defs = self.defs
        if defs is None:
            return None
        ctx: dict[str, Any] = {
            "distinct_id": distinct_id,
            "groups": o.groups or {},
            "person_properties": o.person_properties or {},
            "group_properties": o.group_properties or {},
            "app_contexts": o.app_contexts or self.app_contexts or [],
        }
        if o.unit():
            ctx["device_id"] = o.unit()
        return {k: r for k, r in evaluate_all(defs, ctx).items() if r.reason != "app_context_excluded"}

    def local_one(self, key: str, distinct_id: str, o: FlagOptions) -> Resolved | None:
        r = (self.local(distinct_id, o) or {}).get(key)
        if r is None or r.reason == REQUIRES_SERVER:
            return None
        return _local(r)

    def fallback(self, key: str) -> Resolved | None:
        """The init-time fallback for `key` (reason "fallback"), used when evaluation yields nothing."""
        return Resolved(self.fallbacks[key], None, True, None, None, FALLBACK_REASON) if key in self.fallbacks else None

    def notify(self, hooks: list[Callable[[], None]]) -> None:
        for h in list(hooks):
            try:
                h()
            except Exception:  # noqa: BLE001 - a listener never breaks the reload loop
                pass

    def local_all(self, distinct_id: str, o: FlagOptions) -> tuple[dict[str, Resolved], bool]:
        """Decided flags, and whether anything is left for the server."""
        loc = self.local(distinct_id, o)
        out = {k: _local(r) for k, r in (loc or {}).items() if r.reason != REQUIRES_SERVER}
        return out, loc is None or any(r.reason == REQUIRES_SERVER for r in loc.values())

    def first_seen(self, distinct_id: str, key: str, value: FlagValue) -> bool:
        with self._lock:
            return self._seen.add(f"{distinct_id}\0{key}\0{value}")


def remote_body(
    api_key: str, distinct_id: str, o: FlagOptions, keys: list[str] | None, app_contexts: list[str] | None = None
) -> dict[str, Any]:
    b: dict[str, Any] = {
        "api_key": api_key,
        "distinct_id": distinct_id,
        "groups": dict(o.groups or {}),
        "person_properties": dict(o.person_properties or {}),
        "group_properties": {k: dict(v) for k, v in (o.group_properties or {}).items()},
    }
    if keys:
        b["flag_keys"] = keys
    if o.app_contexts or app_contexts:
        b["app_contexts"] = list(o.app_contexts or app_contexts or [])
    if o.unit():
        b["device_id"] = o.unit()
    if o.anon_distinct_id:
        b["anon_distinct_id"] = o.anon_distinct_id
    return b


def parse_remote(j: Any) -> dict[str, Resolved]:
    out: dict[str, Resolved] = {}
    if not isinstance(j, dict):
        return out
    rid = j.get("request_id")
    for k, f in (j.get("flags") or {}).items():
        v = f.get("value")
        if v is None:
            v = f.get("variant") or bool(f.get("enabled"))
        out[k] = Resolved(v, f.get("payload"), False, rid, f.get("variant") or None, f.get("reason") or "", f.get("condition_index", -1))
    return out


def exposure_events(key: str, r: Resolved) -> list[tuple[str, dict[str, Any]]]:
    props: dict[str, Any] = {"$feature_flag": key, "$feature_flag_response": r.value, "locally_evaluated": r.locally}
    if r.request_id:
        props["$feature_flag_request_id"] = r.request_id
    return [("$feature_flag_called", props), ("$exposure", dict(props))]
