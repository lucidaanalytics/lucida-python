"""asyncio server SDK (httpx.AsyncClient). Same behaviour as the sync client."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx

from ._ai_config import AiConfig, ai_config
from ._agent import AgentConversation, start_conversation
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
    FlagDetails,
    FlagOptions,
    FlagState,
    FlagValue,
    Resolved,
    exposure_events,
    parse_remote,
    remote_body,
)
from ._flagcache import FlagDefinitionCacheProvider, adopt_cached, publish_cached
from ._queue import AsyncDirectSender, AsyncQueue, Result
from ._snapshot import FlagSnapshot, flag_event_props
from ._stream import SseEvents
from ._testmode import AsyncSink, Fixture, Sink, load_fixture, no_network
from .client import log, setup_debug


class AsyncLucida:
    """`async with AsyncLucida("lc_pk_...", secret_key="lc_sk_...") as lc:`; or `await lc.start()` / `await lc.shutdown()`.

    `capture` and friends are plain (non-blocking) calls; flag reads are coroutines.
    """

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
        http_client: httpx.AsyncClient | None = None,
        test_mode: Fixture | None = None,
        max_batch_size: int = MAX_BATCH,
        before_send: BeforeSend | None = None,
        on_error: OnError | None = None,
        disable_geoip: bool = False,
        flag_timeout: float | None = None,
        flag_dedup_cache_size: int = 10_000,
        flag_definition_cache: FlagDefinitionCacheProvider | None = None,
        default_properties: Props | None = None,
        capture_text: bool = False,
        fallbacks: Mapping[str, FlagValue] | None = None,
    ) -> None:
        """Same options as `Lucida` except `sync_mode` (use `await capture_immediate(...)`)."""
        setup_debug(debug)
        self._api_key, self._secret = api_key, secret_key
        self._host = host.rstrip("/")
        self._disabled = disabled
        self._http = http_client or httpx.AsyncClient(
            timeout=timeout, transport=httpx.MockTransport(no_network) if test_mode is not None else None
        )
        self._own_http = http_client is None
        self._flags = FlagState(_seen=LruSet(flag_dedup_cache_size), app_contexts=app_contexts, fallbacks=dict(fallbacks or {}))
        self._flag_timeout = flag_timeout
        self._cache = flag_definition_cache
        self._builder = EventBuilder(before_send=before_send, on_error=on_error, disable_geoip=disable_geoip)
        if default_properties:
            self._builder.register(default_properties)
        self._capture_text = capture_text
        self._direct = AsyncDirectSender(self._post_batch, on_error=on_error)
        self.sink: Sink | None = None
        """test_mode only: `sink.events` holds every captured event (including exposures), nothing is sent."""
        self._poll_interval = flag_poll_interval
        self._poller: asyncio.Task[None] | None = None
        self._streamer: asyncio.Task[None] | None = None
        self._streaming = False
        self._flag_stream = flag_stream
        self._stream_backoff = stream_backoff
        self._stop = asyncio.Event()
        self._queue = (
            None
            if disabled or test_mode is not None
            else AsyncQueue(
                self._post_batch,
                flush_at=flush_at,
                interval=flush_interval,
                max_size=max_queue_size,
                max_retries=max_retries,
                log=log.debug,
                batch_size=max_batch_size,
                on_error=on_error,
            )
        )
        if test_mode is not None:
            self._flags.defs = load_fixture(test_mode)
            self.sink = AsyncSink()
            self._queue = self.sink  # type: ignore[assignment]

    async def start(self) -> None:
        """Start the flusher and (with a secret key) the definitions poller; waits for the first fetch."""
        if self._disabled or self.sink is not None or self._poller is not None:
            return
        if self._queue:
            self._queue.start()
        if self._secret:
            await self.reload_feature_flags()
            self._poller = asyncio.get_running_loop().create_task(self._poll())
            if self._flag_stream:
                self._streamer = asyncio.get_running_loop().create_task(self._stream())

    async def __aenter__(self) -> AsyncLucida:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.shutdown()

    @property
    def dropped_events(self) -> int:
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
        """Queue one event (non-blocking). `distinct_id=None` uses the active context; see `Lucida.capture`.
        `flags` attaches a snapshot's `$feature/*` values; `send_feature_flags` evaluates the distinct id's flags
        first (locally from loaded definitions only; use `await evaluate_flags()` otherwise)."""
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
        return {k: r.value for k, r in res.items()}

    async def capture_immediate(
        self, distinct_id: str | None, event: str, properties: Props | None = None, **kw: Any
    ) -> str | None:
        """Await delivery of one event (one attempt); returns its uuid, or None when dropped/failed."""
        if not self._queue:
            return None
        e = self._builder.build(distinct_id, event, properties, **kw)
        if e is None:
            return None
        if self.sink is not None:
            self.sink.add(e)
            return str(e["uuid"])
        return str(e["uuid"]) if await self._direct.send(e) else None

    def ai_conversation(self, distinct_id: str | None, **opts: str | None) -> AgentConversation:
        """Agent analytics: emits `$ai_conversation_started` and returns the handle (`message`, `feedback`, `end`,
        `outcome`). Options: `conversation_id`, `agent_name`, `agent_version`, `trace_id`, `topic`, `use_case`.
        Text is sent (redacted) only with `capture_text=True` on the client."""
        return start_conversation(self.capture, distinct_id, self._capture_text, **opts)

    def register(self, properties: Props) -> None:
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

    async def flush(self, timeout: float | None = None) -> bool:
        return await self._queue.flush(timeout) if self._queue else True

    async def shutdown(self, timeout: float = 10.0) -> None:
        self._stop.set()
        tasks = [t for t in (self._poller, self._streamer) if t]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._cache:
            try:
                self._cache.shutdown()
            except Exception:  # noqa: BLE001
                pass
        if self._queue:
            await self._queue.shutdown(timeout)
        if self._own_http:
            await self._http.aclose()

    async def _post(
        self, url: str, payload: dict[str, Any], bearer: str | None = None, timeout: float | None = None
    ) -> httpx.Response | None:
        url, body, headers = encode(url, payload)
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        try:
            return await self._http.post(
                url, content=body, headers=headers, timeout=httpx.USE_CLIENT_DEFAULT if timeout is None else timeout
            )
        except httpx.HTTPError as e:
            log.debug("request failed: %s", e)
            return None

    async def _post_batch(self, batch: list[dict[str, Any]]) -> Result:
        r = await self._post(f"{self._host}/v1/batch", batch_payload(self._api_key, batch))
        return (0, None) if r is None else (r.status_code, parse_retry_after(r.headers.get("retry-after")))

    async def import_events(self, events: Iterable[ImportEvent]) -> int:
        """Historical import via POST /v1/s2s/batch (secret key, timestamps kept), chunks of 1000."""
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
            r = await self._post(f"{self._host}/v1/s2s/batch", batch_payload(None, chunk), self._secret)
            if r is None or not r.is_success:
                raise LucidaError(f"import failed: status {r.status_code if r else 0} after {sent} events")
            sent += len(chunk)
        return sent

    # flags ----------------------------------------------------------------------------------

    async def _poll(self) -> None:
        while True:
            await asyncio.sleep(self._poll_interval)
            if not self._streaming:
                await self.reload_feature_flags()

    async def _stream(self) -> None:
        """Reload definitions on every SSE `definitions` event; reconnect with backoff, polling covers gaps."""
        attempt = 0
        while True:
            try:
                async with self._http.stream(
                    "GET",
                    f"{self._host}/v1/flags/stream",
                    headers={"Authorization": f"Bearer {self._secret}", "Accept": "text/event-stream"},
                    timeout=httpx.Timeout(10.0, read=None),
                ) as r:
                    r.raise_for_status()
                    self._streaming, attempt = True, 0
                    sse = SseEvents()
                    async for line in r.aiter_lines():
                        if sse.feed(line) == "definitions":
                            await self.reload_feature_flags()
            except httpx.HTTPError as e:
                log.debug("flag stream failed: %s", e)
            if self._streaming:
                self._flags.notify(self._flags.on_stale)
            self._streaming = False
            await asyncio.sleep(retry_delay(attempt, base=self._stream_backoff))
            attempt += 1

    async def reload_feature_flags(self) -> None:
        if not self._secret or self._disabled:
            return
        if (cached := adopt_cached(self._cache)) is not None:
            self._flags.defs = cached
            self._flags.notify(self._flags.on_change)
            return
        headers = {"Authorization": f"Bearer {self._secret}"}
        if self._flags.etag:
            headers["If-None-Match"] = self._flags.etag
        try:
            r = await self._http.get(
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
            self._flags.notify(self._flags.on_change)
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as e:
            log.debug("flag definitions poll failed: %s", e)
            self._flags.notify(self._flags.on_stale)

    async def _remote(self, distinct_id: str, o: FlagOptions, keys: list[str] | None) -> dict[str, Resolved]:
        r = await self._post(
            f"{self._host}/v1/flags", remote_body(self._api_key, distinct_id, o, keys, self._flags.app_contexts), timeout=self._flag_timeout
        )
        try:
            if r is not None and r.is_success:
                return parse_remote(r.json())
        except ValueError:
            pass
        log.debug("remote flags failed")
        return {}

    async def _resolve(self, key: str, distinct_id: str, o: FlagOptions) -> Resolved | None:
        r = self._flags.local_one(key, distinct_id, o)
        if r or o.only_evaluate_locally:
            return r or self._flags.fallback(key)
        return (await self._remote(distinct_id, o, [key])).get(key) or self._flags.fallback(key)

    async def get_feature_flag(
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
        if self._disabled:
            return None
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally, send_exposure, device_id=device_id)
        r = await self._resolve(key, distinct_id, o)
        if r and not r.is_fallback and send_exposure and self._flags.first_seen(distinct_id, key, r.value):
            for name, props in exposure_events(key, r):
                self.capture(distinct_id, name, props)
        return r.value if r else default

    def on_flags_change(self, callback: Callable[[], None]) -> None:
        """Call `callback()` whenever definitions are (re)loaded with a change (first load included)."""
        self._flags.on_change.append(callback)

    def on_flags_stale(self, callback: Callable[[], None]) -> None:
        """Call `callback()` when a definitions reload fails or the flag stream drops."""
        self._flags.on_stale.append(callback)

    @property
    def local_evaluation(self) -> bool:
        """True when definitions are polled (a secret key is set and the client is enabled)."""
        return bool(self._secret) and not self._disabled

    @property
    def definitions_loaded(self) -> bool:
        return self._flags.defs is not None

    async def get_feature_flag_details(
        self, key: str, distinct_id: str, *, person_properties: Props | None = None,
        groups: Mapping[str, str] | None = None, group_properties: Mapping[str, Props] | None = None,
        only_evaluate_locally: bool = False, send_exposure: bool = True, device_id: str | None = None,
    ) -> FlagDetails | None:
        """Value, payload, variant, raw Lucida reason ("fallback" for an init-time fallback), condition index,
        request id and `evaluated_locally`; None when unknown and no fallback. Exposure like `get_feature_flag`."""
        if self._disabled:
            return None
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally, send_exposure, device_id=device_id)
        r = await self._resolve(key, distinct_id, o)
        if r and not r.is_fallback and send_exposure and self._flags.first_seen(distinct_id, key, r.value):
            for name, props in exposure_events(key, r):
                self.capture(distinct_id, name, props)
        return r

    async def is_feature_enabled(self, key: str, distinct_id: str, **kw: Any) -> bool | None:
        v = await self.get_feature_flag(key, distinct_id, **kw)
        return None if v is None else v is not False

    async def get_feature_flag_payload(
        self, key: str, distinct_id: str, *, person_properties: Props | None = None,
        groups: Mapping[str, str] | None = None, group_properties: Mapping[str, Props] | None = None,
        only_evaluate_locally: bool = False,
    ) -> Any:
        if self._disabled:
            return None
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally)
        r = await self._resolve(key, distinct_id, o)
        return r.payload if r else None

    async def get_ai_config(
        self, key: str, distinct_id: str, fallback: AiConfig, *, variables: dict[str, str] | None = None,
        person_properties: Props | None = None, groups: Mapping[str, str] | None = None,
        group_properties: Mapping[str, Props] | None = None, only_evaluate_locally: bool = False,
        send_exposure: bool = True,
    ) -> AiConfig:
        """The AI config variation served to ``distinct_id`` (completion or agent mode), or ``fallback``."""
        if self._disabled:
            return ai_config(None, None, fallback, variables)
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally, send_exposure)
        r = await self._resolve(key, distinct_id, o)
        if r and not r.is_fallback and send_exposure and self._flags.first_seen(distinct_id, key, r.value):
            for name, props in exposure_events(key, r):
                self.capture(distinct_id, name, props)
        variant = r.value if r and isinstance(r.value, str) else None
        return ai_config(variant, r.payload if r else None, fallback, variables)

    async def _all(self, distinct_id: str, o: FlagOptions) -> dict[str, Resolved]:
        res, undecided = self._flags.local_all(distinct_id, o)
        if undecided and not o.only_evaluate_locally:
            for k, r in (await self._remote(distinct_id, o, None)).items():
                res.setdefault(k, r)
        return res

    async def get_all_flags(
        self, distinct_id: str, *, person_properties: Props | None = None, groups: Mapping[str, str] | None = None,
        group_properties: Mapping[str, Props] | None = None, only_evaluate_locally: bool = False,
    ) -> dict[str, FlagValue]:
        if self._disabled:
            return {}
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally)
        return {k: r.value for k, r in (await self._all(distinct_id, o)).items()}

    async def evaluate_flags(
        self, distinct_id: str, *, person_properties: Props | None = None, groups: Mapping[str, str] | None = None,
        group_properties: Mapping[str, Props] | None = None, only_evaluate_locally: bool = False,
    ) -> FlagSnapshot:
        """Evaluate every flag once; read the snapshot freely and pass it to `capture(..., flags=snapshot)`."""
        if self._disabled:
            return FlagSnapshot({})
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally)
        res = await self._all(distinct_id, o)
        return FlagSnapshot({k: r.value for k, r in res.items()}, {k: r.payload for k, r in res.items() if r.payload is not None})

    async def get_all_flags_and_payloads(
        self, distinct_id: str, *, person_properties: Props | None = None, groups: Mapping[str, str] | None = None,
        group_properties: Mapping[str, Props] | None = None, only_evaluate_locally: bool = False,
    ) -> tuple[dict[str, FlagValue], dict[str, Any]]:
        if self._disabled:
            return {}, {}
        o = FlagOptions(person_properties, groups, group_properties, only_evaluate_locally)
        res = await self._all(distinct_id, o)
        return {k: r.value for k, r in res.items()}, {k: r.payload for k, r in res.items() if r.payload is not None}

    async def get_remote_config_payload(self, key: str) -> Any:
        """See `Lucida.get_remote_config_payload`."""
        if not self._secret:
            raise LucidaError("get_remote_config_payload requires secret_key")
        if self._disabled:
            return None
        try:
            r = await self._http.get(
                f"{self._host}/v1/flags/{quote(key, safe='')}/remote-config",
                headers={"Authorization": f"Bearer {self._secret}"},
                timeout=httpx.USE_CLIENT_DEFAULT if self._flag_timeout is None else self._flag_timeout,
            )
            if r.is_success:  # 404: unknown key or not a remote-config flag
                return r.json().get("payload")
        except (httpx.HTTPError, ValueError, AttributeError) as e:
            log.debug("remote config fetch failed: %s", e)
        return None
