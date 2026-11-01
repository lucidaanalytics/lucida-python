"""Bounded batching queues with a background flusher: a thread (sync) and an asyncio task."""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable

from ._build import OnError, notify
from ._core import MAX_BATCH, LucidaError, WireEvent, is_retryable, retry_delay

Result = tuple[int, float | None]  # (HTTP status or 0 on network error, Retry-After seconds)


class SyncQueue:
    """Flush at `flush_at` events or `interval` seconds after the first queued event."""

    def __init__(
        self,
        post: Callable[[list[WireEvent]], Result],
        *,
        flush_at: int,
        interval: float,
        max_size: int,
        max_retries: int,
        log: Callable[..., None],
        batch_size: int = MAX_BATCH,
        on_error: OnError | None = None,
    ) -> None:
        self._batch_size, self._on_error = max(1, min(batch_size, MAX_BATCH)), on_error
        self._post, self._flush_at, self._interval = post, flush_at, interval
        self._max_size, self._max_retries, self._log = max_size, max_retries, log
        self._items: deque[WireEvent] = deque()
        self._cv = threading.Condition()
        self._closed = self._abort = self._busy = self._flush_req = False
        self.dropped = 0
        self._thread = threading.Thread(target=self._run, name="lucida-flusher", daemon=True)
        self._thread.start()

    def add(self, e: WireEvent) -> None:
        with self._cv:
            if self._closed:
                return
            self._items.append(e)
            if len(self._items) > self._max_size:
                self._items.popleft()
                self.dropped += 1
            if len(self._items) in (1, self._flush_at):
                self._cv.notify_all()

    def _run(self) -> None:
        first_at: float | None = None
        while True:
            with self._cv:
                while True:
                    n = len(self._items)
                    if n == 0 and self._closed:
                        return
                    if n and (n >= self._flush_at or self._flush_req or self._closed):
                        break
                    if n:
                        first_at = first_at or time.monotonic()
                        remaining = first_at + self._interval - time.monotonic()
                        if remaining <= 0:
                            break
                        self._cv.wait(remaining)
                    else:
                        first_at = None
                        self._cv.wait()
                batch = [self._items.popleft() for _ in range(min(self._batch_size, len(self._items)))]
                self._busy = True
            first_at = None
            self._send(batch)
            with self._cv:
                self._busy = False
                if not self._items:
                    self._flush_req = False
                self._cv.notify_all()

    def _send(self, batch: list[WireEvent]) -> None:
        attempt = 0
        while True:
            status, retry_after = self._post(batch)
            if 200 <= status < 300:
                return
            if not is_retryable(status) or attempt >= self._max_retries or self._abort:
                self.dropped += len(batch)
                self._log("dropping batch", len(batch), "status", status)
                notify(self._on_error, LucidaError(f"dropped {len(batch)} events: status {status}"), batch)
                return
            delay = retry_delay(attempt, retry_after)
            attempt += 1
            self._log("retry in", round(delay, 2), "s")
            with self._cv:
                self._cv.wait_for(lambda: self._abort, timeout=delay)

    def flush(self, timeout: float | None = None) -> bool:
        """Block until everything queued has been sent or dropped. False on timeout."""
        with self._cv:
            if self._items or self._busy:
                self._flush_req = True  # not when idle: it would defeat batching for the next event
            self._cv.notify_all()
            return self._cv.wait_for(lambda: not self._items and not self._busy, timeout)

    def shutdown(self, timeout: float | None = 10.0) -> None:
        self.flush(timeout)
        with self._cv:
            self._closed = self._abort = True
            self.dropped += len(self._items)
            self._items.clear()
            self._cv.notify_all()
        self._thread.join(timeout)

    @property
    def size(self) -> int:
        return len(self._items)


class AsyncQueue:
    """asyncio twin of `SyncQueue`; the flusher task starts on first use inside a running loop."""

    def __init__(
        self,
        post: Callable[[list[WireEvent]], Awaitable[Result]],
        *,
        flush_at: int,
        interval: float,
        max_size: int,
        max_retries: int,
        log: Callable[..., None],
        batch_size: int = MAX_BATCH,
        on_error: OnError | None = None,
    ) -> None:
        self._batch_size, self._on_error = max(1, min(batch_size, MAX_BATCH)), on_error
        self._post, self._flush_at, self._interval = post, flush_at, interval
        self._max_size, self._max_retries, self._log = max_size, max_retries, log
        self._items: deque[WireEvent] = deque()
        self._closed = self._busy = self._flush_req = False
        self.dropped = 0
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._abort = asyncio.Event()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    def add(self, e: WireEvent) -> None:
        if self._closed:
            return
        self._items.append(e)
        if len(self._items) > self._max_size:
            self._items.popleft()
            self.dropped += 1
        try:
            self.start()
        except RuntimeError:
            return  # no running loop yet: the next flush()/start() picks the events up
        if len(self._items) in (1, self._flush_at):
            self._wake.set()

    async def _sleep(self, seconds: float) -> None:
        self._wake.clear()
        try:
            await asyncio.wait_for(self._wake.wait(), seconds)
        except asyncio.TimeoutError:
            pass

    async def _run(self) -> None:
        first_at: float | None = None
        loop = asyncio.get_running_loop()
        while True:
            n = len(self._items)
            if n == 0:
                if self._closed:
                    return
                first_at = None
                await self._sleep(3600)
                continue
            if not (n >= self._flush_at or self._flush_req or self._closed):
                first_at = first_at or loop.time()
                remaining = first_at + self._interval - loop.time()
                if remaining > 0:
                    await self._sleep(remaining)
                    continue
            batch = [self._items.popleft() for _ in range(min(self._batch_size, n))]
            self._busy, first_at = True, None
            try:
                await self._send(batch)
            finally:
                self._busy = False
                if not self._items:
                    self._flush_req = False

    async def _send(self, batch: list[WireEvent]) -> None:
        attempt = 0
        while True:
            status, retry_after = await self._post(batch)
            if 200 <= status < 300:
                return
            if not is_retryable(status) or attempt >= self._max_retries or self._abort.is_set():
                self.dropped += len(batch)
                self._log("dropping batch", len(batch), "status", status)
                notify(self._on_error, LucidaError(f"dropped {len(batch)} events: status {status}"), batch)
                return
            delay = retry_delay(attempt, retry_after)
            attempt += 1
            self._log("retry in", round(delay, 2), "s")
            try:
                await asyncio.wait_for(self._abort.wait(), delay)
            except asyncio.TimeoutError:
                pass

    async def flush(self, timeout: float | None = None) -> bool:
        """Wait until everything queued has been sent or dropped. False on timeout."""
        if self._items or self._busy:
            self._flush_req = True
            self.start()
            self._wake.set()
        deadline = None if timeout is None else time.monotonic() + timeout
        while self._items or self._busy:
            if deadline is not None and time.monotonic() > deadline:
                return False
            await asyncio.sleep(0.005)
        return True

    async def shutdown(self, timeout: float | None = 10.0) -> None:
        await self.flush(timeout)
        self._closed = True
        self._abort.set()
        self.dropped += len(self._items)
        self._items.clear()
        self._wake.set()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)

    @property
    def size(self) -> int:
        return len(self._items)


class DirectSender:
    """`sync_mode` / `capture_immediate`: each event is posted on the caller's thread, no queue. Retryable failures
    are retried up to `attempts` times total with the shared backoff (small base so a request is not held long)."""

    def __init__(
        self, post: Callable[[list[WireEvent]], Result], *, on_error: OnError | None = None,
        attempts: int = 3, retry_base: float = 0.2, sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._post, self._on_error = post, on_error
        self._attempts, self._base, self._sleep = attempts, retry_base, sleep
        self.dropped = 0

    def send(self, e: WireEvent) -> bool:
        status = 0
        for attempt in range(self._attempts):
            status, retry_after = self._post([e])
            if 200 <= status < 300:
                return True
            if not is_retryable(status) or attempt == self._attempts - 1:
                break
            self._sleep(retry_delay(attempt, retry_after, base=self._base, cap=2.0))
        self.dropped += 1
        notify(self._on_error, LucidaError(f"event not delivered: status {status}"), [e])
        return False

    def add(self, e: WireEvent) -> None:
        self.send(e)

    def start(self) -> None:
        return None

    def flush(self, timeout: float | None = None) -> bool:
        return True

    def shutdown(self, timeout: float | None = 10.0) -> None:
        return None


class AsyncDirectSender:
    """asyncio `capture_immediate`: awaits one event's delivery with the same bounded retry as `DirectSender`."""

    def __init__(
        self, post: Callable[[list[WireEvent]], Awaitable[Result]], *, on_error: OnError | None = None,
        attempts: int = 3, retry_base: float = 0.2, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._post, self._on_error = post, on_error
        self._attempts, self._base, self._sleep = attempts, retry_base, sleep
        self.dropped = 0

    async def send(self, e: WireEvent) -> bool:
        status = 0
        for attempt in range(self._attempts):
            status, retry_after = await self._post([e])
            if 200 <= status < 300:
                return True
            if not is_retryable(status) or attempt == self._attempts - 1:
                break
            await self._sleep(retry_delay(attempt, retry_after, base=self._base, cap=2.0))
        self.dropped += 1
        notify(self._on_error, LucidaError(f"event not delivered: status {status}"), [e])
        return False
