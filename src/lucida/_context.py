"""Ambient request context on contextvars (works across threads' own contexts and asyncio tasks).

Explicit capture arguments always win over context values. Context properties are merged under the
event's own properties; `distinct_id` and `$session_id` fill in only when the call gave none.
"""

from __future__ import annotations

import contextvars
import functools
import inspect
import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, TypeVar, overload

F = TypeVar("F", bound=Callable[..., Any])

DISTINCT_ID_HEADER = "X-Lucida-Distinct-Id"
SESSION_ID_HEADER = "X-Lucida-Session-Id"
_MAX_HEADER = 1000
_CTRL = re.compile(r"[\x00-\x1f\x7f]")


@dataclass
class Context:
    distinct_id: str | None = None
    session_id: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)


_current: contextvars.ContextVar[Context | None] = contextvars.ContextVar("lucida_context", default=None)


def get_context() -> Context | None:
    """The innermost active context, or None."""
    return _current.get()


@contextmanager
def new_context(
    *,
    fresh: bool = False,
    distinct_id: str | None = None,
    session_id: str | None = None,
    properties: Mapping[str, Any] | None = None,
) -> Iterator[Context]:
    """Open a context. Nested contexts inherit the parent's values unless `fresh=True`; changes never leak out."""
    parent = None if fresh else _current.get()
    ctx = Context(
        distinct_id if distinct_id is not None else (parent.distinct_id if parent else None),
        session_id if session_id is not None else (parent.session_id if parent else None),
        {**(parent.properties if parent else {}), **(properties or {})},
    )
    token = _current.set(ctx)
    try:
        yield ctx
    finally:
        _current.reset(token)


@overload
def scoped(fn: F) -> F: ...
@overload
def scoped(*, fresh: bool = ...) -> Callable[[F], F]: ...
def scoped(fn: F | None = None, *, fresh: bool = False) -> Any:
    """Decorator: run the function (sync or async) in its own context. `@scoped` or `@scoped(fresh=True)`."""

    def wrap(f: F) -> F:
        if inspect.iscoroutinefunction(f):

            @functools.wraps(f)
            async def aw(*a: Any, **k: Any) -> Any:
                with new_context(fresh=fresh):
                    return await f(*a, **k)

            return aw  # type: ignore[return-value]

        @functools.wraps(f)
        def sw(*a: Any, **k: Any) -> Any:
            with new_context(fresh=fresh):
                return f(*a, **k)

        return sw  # type: ignore[return-value]

    return wrap(fn) if fn is not None else wrap


def tag(key: str, value: Any) -> None:
    """Add a property to the current context (no-op outside one)."""
    if (c := _current.get()) is not None:
        c.properties[key] = value


def identify_context(distinct_id: str) -> None:
    if (c := _current.get()) is not None:
        c.distinct_id = distinct_id


def set_context_session(session_id: str) -> None:
    if (c := _current.get()) is not None:
        c.session_id = session_id


def clean_header(v: str | None) -> str | None:
    """Strip ASCII control chars, trim, cap at 1000 chars; empty -> None."""
    if not v:
        return None
    return _CTRL.sub("", v).strip()[:_MAX_HEADER] or None


def read_tracing_headers(get: Callable[[str], str | None]) -> tuple[str | None, str | None]:
    """(distinct_id, session_id) from the browser SDK's tracing headers. `get` looks a header up by name.

    These headers are client-controlled: use them to join events to a browser session, never for authentication.
    """
    return clean_header(get(DISTINCT_ID_HEADER)), clean_header(get(SESSION_ID_HEADER))


def request_properties(
    *, url: str | None, method: str | None, path: str | None, user_agent: str | None, ip: str | None
) -> dict[str, Any]:
    """The request props middleware stamps on the context (absent values are left out)."""
    raw = {"$current_url": url, "$request_method": method, "$request_path": path, "$user_agent": user_agent, "$ip": ip}
    return {k: v for k, v in raw.items() if v}


def client_ip(remote_addr: str | None, forwarded_for: str | None, trust_proxy: bool) -> str | None:
    """First X-Forwarded-For entry only when `trust_proxy`; else the socket peer."""
    if trust_proxy and forwarded_for:
        first = forwarded_for.split(",")[0].strip()
        if first:
            return first
    return remote_addr


def request_context_args(
    get_header: Callable[[str], str | None],
    *,
    method: str | None,
    url: str | None,
    path: str | None,
    remote_addr: str | None,
    trust_proxy: bool = False,
    capture_tracing_headers: bool = True,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Keyword arguments for `new_context(fresh=True, **args)`: tracing-header ids plus the stamped request props."""
    did, sid = read_tracing_headers(get_header) if capture_tracing_headers else (None, None)
    props = request_properties(
        url=url,
        method=method,
        path=path,
        user_agent=clean_header(get_header("User-Agent")),
        ip=client_ip(remote_addr, get_header("X-Forwarded-For"), trust_proxy),
    )
    return {"distinct_id": did, "session_id": sid, "properties": {**props, **(extra or {})}}
