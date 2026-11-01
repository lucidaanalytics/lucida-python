"""Pure ASGI middleware (FastAPI, Starlette, Litestar): one fresh context per HTTP request.

    app.add_middleware(LucidaASGIMiddleware)               # Starlette / FastAPI
    app = LucidaASGIMiddleware(app, trust_proxy=True)      # any ASGI app

Inside handlers, `client.capture(None, "event")` picks up the distinct id and session id from the browser SDK's
tracing headers (never use them for auth) and every event carries `$current_url`, `$request_method`,
`$request_path`, `$user_agent` and `$ip`. Call `identify_context(user_id)` once you know the user.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from typing import Any
from urllib.parse import quote

from ._context import new_context, request_context_args

Scope = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[MutableMapping[str, Any]]]
Send = Callable[[MutableMapping[str, Any]], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


class LucidaASGIMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        request_filter: Callable[[Scope], bool] | None = None,
        extra_properties: Mapping[str, Any] | None = None,
        capture_tracing_headers: bool = True,
        trust_proxy: bool = False,
    ) -> None:
        """`request_filter(scope)` returning False skips the context for that request."""
        self.app, self._filter, self._extra = app, request_filter, dict(extra_properties or {})
        self._tracing, self._trust = capture_tracing_headers, trust_proxy

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or (self._filter and not self._filter(scope)):
            await self.app(scope, receive, send)
            return
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        host = headers.get("host", "")
        path = scope.get("root_path", "") + scope.get("path", "")
        qs = scope.get("query_string", b"").decode("latin-1")
        url = f"{scope.get('scheme', 'http')}://{host}{quote(path)}" + (f"?{qs}" if qs else "")
        client = scope.get("client")
        args = request_context_args(
            lambda name: headers.get(name.lower()),
            method=scope.get("method"),
            url=url if host else None,
            path=path,
            remote_addr=client[0] if client else None,
            trust_proxy=self._trust,
            capture_tracing_headers=self._tracing,
            extra=self._extra,
        )
        with new_context(fresh=True, **args):
            await self.app(scope, receive, send)
