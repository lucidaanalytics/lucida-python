"""Event assembly shared by the sync and asyncio clients: context merge, super properties, personless, geoip, before_send."""

from __future__ import annotations

import hashlib
import json
import threading
import uuid as uuidlib
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from typing import Any

from ._context import get_context
from ._core import ImportEvent, LruSet, Props, WireEvent, import_wire, make_event

IDENTITY_DEDUPE_SIZE = 1000

BeforeSend = Callable[[WireEvent], "WireEvent | None"]
OnError = Callable[[Exception, "list[WireEvent]"], None]


def notify(on_error: OnError | None, err: Exception, events: list[WireEvent]) -> None:
    """Call the user's error callback; a raising callback must never break delivery."""
    if on_error is None:
        return
    try:
        on_error(err, events)
    except Exception:  # noqa: BLE001
        pass


class EventBuilder:
    def __init__(
        self,
        *,
        before_send: BeforeSend | None = None,
        on_error: OnError | None = None,
        disable_geoip: bool = False,
        is_server: bool = False,
    ) -> None:
        self.before_send, self.on_error = before_send, on_error
        self.disable_geoip = disable_geoip
        self.super_props: dict[str, Any] = {}
        self._is_server = is_server
        self._identities = LruSet(IDENTITY_DEDUPE_SIZE)
        self._identities_lock = threading.Lock()
        self._identity_keys: dict[str, str] = {}  # event uuid -> dedupe key, until the batch is dropped

    def identity_uuid(self, *parts: Any) -> str | None:
        """The uuid to send an identify/group event under the first time this exact call (id + props) is seen, else None.

        The call stays deduped while its event is queued or delivered; `on_dropped` frees it when the batch is dropped.
        """
        key = hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()
        with self._identities_lock:
            if not self._identities.add(key):
                return None
            uid = str(uuidlib.uuid4())
            self._identity_keys[uid] = key
            if len(self._identity_keys) > IDENTITY_DEDUPE_SIZE:
                del self._identity_keys[next(iter(self._identity_keys))]
            return uid

    def on_dropped(self, err: Exception, events: list[WireEvent]) -> None:
        """Queue/sender error hook: free the identity keys of undelivered events, then call the user's `on_error`."""
        with self._identities_lock:
            for e in events:
                key = self._identity_keys.pop(str(e.get("uuid")), None)
                if key is not None:
                    self._identities.discard(key)
        notify(self.on_error, err, events)

    def register(self, properties: Props) -> None:
        """Client-wide default properties; per-event and context properties override them."""
        self.super_props.update(properties)

    def unregister(self, key: str) -> None:
        self.super_props.pop(key, None)

    def import_wire(self, events: Iterable[ImportEvent], raw: bool = False) -> list[WireEvent]:
        """Wire events for a historical import: default properties and `before_send` apply unless `raw`."""
        if raw:
            return [import_wire(e) for e in events]
        built = (
            self.build(
                e["distinct_id"], e["event"], e.get("properties"), timestamp=e.get("timestamp"),
                uuid=e.get("uuid"), set=e.get("set"), set_once=e.get("set_once"),
            )
            for e in events
        )
        return [w for w in built if w is not None]

    def build(
        self,
        distinct_id: str | None,
        event: str,
        properties: Props | None = None,
        *,
        groups: Mapping[str, str] | None = None,
        timestamp: datetime | str | None = None,
        uuid: str | None = None,
        set: Props | None = None,  # noqa: A002
        set_once: Props | None = None,
        process_person_profile: bool = True,
        disable_geoip: bool | None = None,
    ) -> WireEvent | None:
        """The wire event, or None when `before_send` dropped it. Explicit arguments beat the context."""
        ctx = get_context()
        distinct_id = distinct_id or (ctx.distinct_id if ctx else None)
        if not distinct_id:
            if process_person_profile:
                raise ValueError("distinct_id is required (pass one, or use a context with identify_context)")
            distinct_id = str(uuidlib.uuid4())
        props: dict[str, Any] = dict(self.super_props)
        if self._is_server:
            props["$is_server"] = True
        if ctx:
            props.update(ctx.properties)
            if ctx.session_id:
                props["$session_id"] = ctx.session_id
        props.update(properties or {})
        if not process_person_profile:
            props["$process_person_profile"] = False
            set = set_once = None  # noqa: A001 - ignored by the worker on personless events
        if self.disable_geoip if disable_geoip is None else disable_geoip:
            props["$geoip_disable"] = True
        e = make_event(
            distinct_id, event, props, groups=groups, timestamp=timestamp, uuid=uuid, set=set, set_once=set_once
        )
        if self.before_send is None:
            return e
        try:
            return self.before_send(e)
        except Exception as err:  # noqa: BLE001
            notify(self.on_error, err, [e])
            return e  # a raising hook keeps the original event
