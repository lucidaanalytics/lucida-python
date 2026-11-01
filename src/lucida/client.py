"""Synchronous server SDK. Every call names its distinct_id; there is no ambient identity."""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx

from ._build import BeforeSend, EventBuilder, OnError
from ._core import (
    DEFAULT_HOST,
    MAX_BATCH,
    ImportEvent,
    LruSet,
    LucidaError,
    Props,
    batch_payload,
    encode,
    import_wire,
    parse_retry_after,
    retry_delay,
)
from ._flags import (
    FlagOptions,
    FlagState,
    FlagValue,
    Resolved,
    exposure_events,
    parse_remote,
    remote_body,
)
from ._flagcache import FlagDefinitionCacheProvider, adopt_cached, publish_cached
from ._queue import DirectSender, Result, SyncQueue
from ._snapshot import FlagSnapshot, flag_event_props
from ._stream import SseEvents
from ._testmode import Fixture, Sink, load_fixture, no_network

log = logging.getLogger("lucida")


def setup_debug(debug: bool) -> None:
    if debug:
        log.setLevel(logging.DEBUG)
        if not log.handlers:
            log.addHandler(logging.StreamHandler())


class Lucida:
    """`Lucida("lc_pk_...", secret_key="lc_sk_...")`. Use as a context manager or call `shutdown()`."""

    def __init__(
        self,
        api_key: str,
        *,
        host: str = DEFAULT_HOST,
        flush_at: int = 20,
        flush_interval: float = 5.0,
        max_queue_size: int = 1000,
        max_retries: int = 5,
        secret_key: str | None = None,
        flag_poll_interval: float = 30.0,
        flag_stream: bool = True,
        stream_backoff: float = 1.0,
        timeout: float = 10.0,
        disabled: bool = False,
        app_contexts: list[str] | None = None,
        debug: bool = False,
        http_client: httpx.Client | None = None,
        test_mode: Fixture | None = None,
        max_batch_size: int = MAX_BATCH,
        before_send: BeforeSend | None = None,
        on_error: OnError | None = None,
        sync_mode: bool = False,
        disable_geoip: bool = False,
        flag_timeout: float | None = None,
        flag_dedup_cache_size: int = 10_000,
        flag_definition_cache: FlagDefinitionCacheProvider | None = None,
        default_properties: Props | None = None,
    ) -> None:
        """`max_batch_size`: events per request (<= 1000), independent of `flush_at`. `before_send(event)` edits or
        drops (return None) each wire event; a raising hook keeps the original. `on_error(exc, events)` fires when a
        batch is dropped. `sync_mode`: post each event on the caller's thread, no queue. `disable_geoip`: send
        `$geoip_disable` on every event. `flag_timeout`: seconds for flag requests (default `timeout`). `default_properties`: super properties on every
        event (same as `register`)."""
        setup_debug(debug)
        self._api_key, self._secret = api_key, secret_key
        self._host = host.rstrip("/")
        self._disabled = disabled
        self._test = test_mode is not None
        self._http = http_client or httpx.Client(
            timeout=timeout, transport=httpx.MockTransport(no_network) if self._test else None
        )
        self._own_http = http_client is None
        self._flags = FlagState(_seen=LruSet(flag_dedup_cache_size), app_contexts=app_contexts)
        self._flag_timeout = flag_timeout
        self._cache = flag_definition_cache
        self._builder = EventBuilder(before_send=before_send, on_error=on_error, disable_geoip=disable_geoip)
        if default_properties:
            self._builder.register(default_properties)
        self._direct: DirectSender | None = None
        self.sink: Sink | None = None
        """test_mode only: `sink.events` holds every captured event (including exposures), nothing is sent."""
        self._stop = threading.Event()
        self._queue: SyncQueue | None = None
        self._poller: threading.Thread | None = None
        self._streaming = False
        self._stream_backoff = stream_backoff
        self.ready = threading.Event()
        """Set after the first definitions fetch attempt (immediately when local evaluation is off)."""
        if test_mode is not None:
            self._flags.defs = load_fixture(test_mode)
            self.sink = Sink()
            self._queue = self.sink  # type: ignore[assignment]
            self.ready.set()
            return
        if disabled:
            self.ready.set()
            return
        self._direct = DirectSender(self._post_batch, on_error=on_error)
        self._queue = self._direct if sync_mode else SyncQueue(  # type: ignore[assignment]
            self._post_batch,
            flush_at=flush_at,
            interval=flush_interval,
            max_size=max_queue_size,
            max_retries=max_retries,
            log=log.debug,
            batch_size=max_batch_size,
            on_error=on_error,
        )
        if secret_key:
            self._poller = threading.Thread(
                target=self._poll, args=(flag_poll_interval,), name="lucida-flags", daemon=True
            )
            self._poller.start()
            if flag_stream:
                threading.Thread(target=self._stream, name="lucida-flag-stream", daemon=True).start()
        else:
            self.ready.set()

    # events ---------------------------------------------------------------------------------

    @property
    def dropped_events(self) -> int:
        """Events discarded because the queue was full or the server kept failing."""
        return self._queue.dropped if self._queue else 0

    def capture(
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
        send_feature_flags: bool = False,
        flags: FlagSnapshot | None = None,
        process_person_profile: bool = True,
        disable_geoip: bool | None = None,
    ) -> None:
        """Queue one event. `distinct_id=None` falls back to the active context; with
        `process_person_profile=False` and no id a random UUID is used (personless event).
        `flags` attaches a snapshot's `$feature/*` values; `send_feature_flags` evaluates the distinct id's flags
        first (locally when definitions are loaded, else one POST /v1/flags)."""
        if flags is not None:
            properties = flags.event_props(dict(properties or {}))
        elif send_feature_flags and distinct_id:
            properties = flag_event_props(dict(properties or {}), self._capture_flags(distinct_id, groups))
        if self._queue:
            e = self._builder.build(
                distinct_id, event, properties, groups=groups, timestamp=timestamp, uuid=uuid, set=set,
                set_once=set_once, process_person_profile=process_person_profile, disable_geoip=disable_geoip,
            )
            if e is not None:
                self._queue.add(e)

    def _capture_flags(self, distinct_id: str, groups: Mapping[str, str] | None) -> dict[str, FlagValue]:
        o = FlagOptions(groups=groups)
        res, undecided = self._flags.local_all(distinct_id, o)
        if undecided and not self._disabled:
            for k, r in self._remote(distinct_id, o, None).items():
                res.setdefault(k, r)
        return {k: r.value for k, r in res.items()}

    def capture_immediate(self, distinct_id: str | None, event: str, properties: Props | None = None, **kw: Any) -> str | None:
        """Deliver one event now on the caller's thread (serverless); returns its uuid, or None when dropped/failed."""
        if not self._queue:
            return None
        e = self._builder.build(distinct_id, event, properties, **kw)
        if e is None:
            return None
        if self.sink is not None:
            self.sink.add(e)
            return str(e["uuid"])
        return str(e["uuid"]) if self._direct and self._direct.send(e) else None

    def register(self, properties: Props) -> None:
        """Super properties: added to every event (context and per-event properties win)."""
        self._builder.register(properties)

    def unregister(self, key: str) -> None:
        self._builder.unregister(key)

    def identify(self, distinct_id: str | None, properties: Props | None = None, properties_set_once: Props | None = None) -> None:
        """`$identify`; an identical call (same id and properties) already sent in this process is skipped."""
        if self._builder.first_identity("identify", distinct_id, properties, properties_set_once):
            self.capture(distinct_id, "$identify", set=properties, set_once=properties_set_once)

    def set_person_properties(self, distinct_id: str | None, properties: Props) -> None:
        """Set person properties via a `$set` event (not deduped)."""
        self.capture(distinct_id, "$set", set=properties)

    def set_person_properties_once(self, distinct_id: str | None, properties: Props) -> None:
        """Set person properties only where unset, via a `$set` event with `$set_once`."""
        self.capture(distinct_id, "$set", set_once=properties)

    def alias(self, distinct_id: str | None, alias: str) -> None:
        self.capture(distinct_id, "$create_alias", {"alias": alias, "distinct_id": distinct_id or ""})

    def group_identify(
        self, group_type: str, group_key: str, properties: Props | None = None, distinct_id: str | None = None
    ) -> None:
        if not self._builder.first_identity("group", group_type, group_key, distinct_id, properties):
            return
        self.capture(
            distinct_id or f"${group_type}_{group_key}",
            "$groupidentify",
            {"$group_type": group_type, "$group_key": group_key, "$group_set": dict(properties or {})},
        )

    def flush(self, timeout: float | None = None) -> bool:
        """Send everything queued; False if `timeout` elapsed first."""
        return self._queue.flush(timeout) if self._queue else True

    def shutdown(self, timeout: float = 10.0) -> None:
        """Stop polling, flush queued events, stop accepting new ones."""
        self._stop.set()
        if self._cache:
            try:
                self._cache.shutdown()
            except Exception:  # noqa: BLE001
                pass
        if self._queue:
            self._queue.shutdown(timeout)
        if self._own_http:
            self._http.close()

    def __enter__(self) -> Lucida:
        return self

    def __exit__(self, *exc: object) -> None:
        self.shutdown()

    def _post(
        self, url: str, payload: dict[str, Any], bearer: str | None = None, timeout: float | None = None
    ) -> httpx.Response | None:
        url, body, headers = encode(url, payload)
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        try:
            return self._http.post(
                url, content=body, headers=headers, timeout=httpx.USE_CLIENT_DEFAULT if timeout is None else timeout
            )
        except httpx.HTTPError as e:
            log.debug("request failed: %s", e)
            return None

    def _post_batch(self, batch: list[dict[str, Any]]) -> Result:
        r = self._post(f"{self._host}/v1/batch", batch_payload(self._api_key, batch))
        return (0, None) if r is None else (r.status_code, parse_retry_after(r.headers.get("retry-after")))

    def import_events(self, events: Iterable[ImportEvent]) -> int:
        """Historical import via POST /v1/s2s/batch (secret key, timestamps kept), chunks of 1000.

        Raises `LucidaError` on the first rejected chunk; returns the number of events accepted.
        """
        if not self._secret:
            raise LucidaError("import_events requires secret_key")
        if self._disabled:
            return 0
        wire = [import_wire(e) for e in events]
        if self.sink is not None:
            for w in wire:
                self.sink.add(w)
            return len(wire)
        sent = 0
        for i in range(0, len(wire), MAX_BATCH):
            chunk = wire[i : i + MAX_BATCH]
            r = self._post(f"{self._host}/v1/s2s/batch", batch_payload(None, chunk), self._secret)
            if r is None or not r.is_success:
                raise LucidaError(f"import failed: status {r.status_code if r else 0} after {sent} events")
            sent += len(chunk)
        return sent

    # flags ----------------------------------------------------------------------------------

    def _poll(self, interval: float) -> None:
        while True:
            if not self._streaming:
                self.reload_feature_flags()
            self.ready.set()
            if self._stop.wait(interval):
                return

    def _stream(self) -> None:
        """Reload definitions on every SSE `definitions` event; reconnect with backoff, polling covers gaps."""
        attempt = 0
        while not self._stop.is_set():
            try:
                with self._http.stream(
                    "GET",
                    f"{self._host}/v1/flags/stream",
                    headers={"Authorization": f"Bearer {self._secret}", "Accept": "text/event-stream"},
                    timeout=httpx.Timeout(10.0, read=None),
                ) as r:
                    r.raise_for_status()
                    self._streaming, attempt = True, 0
                    sse = SseEvents()
                    for line in r.iter_lines():
                        if sse.feed(line) == "definitions":
                            self.reload_feature_flags()
            except httpx.HTTPError as e:
                log.debug("flag stream failed: %s", e)
            self._streaming = False
            if self._stop.wait(retry_delay(attempt, base=self._stream_backoff)):
                return
            attempt += 1

    def reload_feature_flags(self) -> None:
        """Fetch definitions now (ETag aware); keeps the previous ones on any failure."""
        if not self._secret or self._disabled:
            return
        if (cached := adopt_cached(self._cache)) is not None:
            self._flags.defs = cached
            return
        headers = {"Authorization": f"Bearer {self._secret}"}
        if self._flags.etag:
            headers["If-None-Match"] = self._flags.etag
        try:
            r = self._http.get(
                f"{self._host}/v1/flags/definitions",
                headers=headers,
                timeout=httpx.USE_CLIENT_DEFAULT if self._flag_timeout is None else self._flag_timeout,
            )
            if r.status_code == 304:
                return
            r.raise_for_status()
            flags = r.json()["flags"]
            if not isinstance(flags, list):
                raise ValueError("bad definitions body")
            self._flags.defs, self._flags.etag = flags, r.headers.get("etag")
            publish_cached(self._cache, flags)
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as e:
            log.debug("flag definitions poll failed: %s", e)

    def _remote(self, distinct_id: str, o: FlagOptions, keys: list[str] | None) -> dict[str, Resolved]:
        r = self._post(f"{self._host}/v1/flags", remote_body(self._api_key, distinct_id, o, keys, self._flags.app_contexts), timeout=self._flag_timeout)
        try:
            if r is not None and r.is_success:
                return parse_remote(r.json())
        except ValueError:
            pass
        log.debug("remote flags failed")
        return {}

    def _resolve(self, key: str, distinct_id: str, o: FlagOptions) -> Resolved | None:
        r = self._flags.local_one(key, distinct_id, o)
        if r or o.only_evaluate_locally:
            return r
        return self._remote(distinct_id, o, [key]).get(key)

    def get_feature_flag(
        self,
        key: str,
        distinct_id: str,
        *,
        person_properties: Props | None = None,
        groups: Mapping[str, str] | None = None,
        group_properties: Mapping[str, Props] | None = None,
        only_evaluate_locally: bool = False,
        send_exposure: bool = True,
        default: FlagValue | None = None,
        device_id: str | None = None,
    ) -> FlagValue | None:
        """Flag value (False, True or a variant key), or None when unknown / undecidable locally."""
        if self._disabled:
            return None
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally, send_exposure, device_id=device_id)
        r = self._resolve(key, distinct_id, o)
        if r and send_exposure and self._flags.first_seen(distinct_id, key, r.value):
            for name, props in exposure_events(key, r):
                self.capture(distinct_id, name, props)
        return r.value if r else default

    def is_feature_enabled(self, key: str, distinct_id: str, **kw: Any) -> bool | None:
        v = self.get_feature_flag(key, distinct_id, **kw)
        return None if v is None else v is not False

    def get_feature_flag_payload(
        self, key: str, distinct_id: str, *, person_properties: Props | None = None,
        groups: Mapping[str, str] | None = None, group_properties: Mapping[str, Props] | None = None,
        only_evaluate_locally: bool = False,
    ) -> Any:
        if self._disabled:
            return None
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally)
        r = self._resolve(key, distinct_id, o)
        return r.payload if r else None

    def _all(self, distinct_id: str, o: FlagOptions) -> dict[str, Resolved]:
        res, undecided = self._flags.local_all(distinct_id, o)
        if undecided and not o.only_evaluate_locally:
            for k, r in self._remote(distinct_id, o, None).items():
                res.setdefault(k, r)
        return res

    def get_all_flags(
        self, distinct_id: str, *, person_properties: Props | None = None, groups: Mapping[str, str] | None = None,
        group_properties: Mapping[str, Props] | None = None, only_evaluate_locally: bool = False,
    ) -> dict[str, FlagValue]:
        if self._disabled:
            return {}
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally)
        return {k: r.value for k, r in self._all(distinct_id, o).items()}

    def evaluate_flags(
        self, distinct_id: str, *, person_properties: Props | None = None, groups: Mapping[str, str] | None = None,
        group_properties: Mapping[str, Props] | None = None, only_evaluate_locally: bool = False,
    ) -> FlagSnapshot:
        """Evaluate every flag once; read the snapshot freely and pass it to `capture(..., flags=snapshot)`."""
        if self._disabled:
            return FlagSnapshot({})
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally)
        res = self._all(distinct_id, o)
        return FlagSnapshot({k: r.value for k, r in res.items()}, {k: r.payload for k, r in res.items() if r.payload is not None})

    def get_all_flags_and_payloads(
        self, distinct_id: str, *, person_properties: Props | None = None, groups: Mapping[str, str] | None = None,
        group_properties: Mapping[str, Props] | None = None, only_evaluate_locally: bool = False,
    ) -> tuple[dict[str, FlagValue], dict[str, Any]]:
        """(values, payloads); payloads holds only flags that have one."""
        if self._disabled:
            return {}, {}
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally)
        res = self._all(distinct_id, o)
        return {k: r.value for k, r in res.items()}, {k: r.payload for k, r in res.items() if r.payload is not None}

    def get_remote_config_payload(self, key: str) -> Any:
        """Payload of a remote-config flag, decrypted when it was stored encrypted. Needs `secret_key`
        (the server only opens encrypted payloads for secret keys). None when unknown or on failure."""
        if not self._secret:
            raise LucidaError("get_remote_config_payload requires secret_key")
        if self._disabled:
            return None
        try:
            r = self._http.get(
                f"{self._host}/v1/flags/{quote(key, safe='')}/remote-config",
                headers={"Authorization": f"Bearer {self._secret}"},
                timeout=httpx.USE_CLIENT_DEFAULT if self._flag_timeout is None else self._flag_timeout,
            )
            if r.is_success:  # 404: unknown key or not a remote-config flag
                return r.json().get("payload")
        except (httpx.HTTPError, ValueError, AttributeError) as e:
            log.debug("remote config fetch failed: %s", e)
        return None
