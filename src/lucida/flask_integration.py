"""Flask integration: a fresh Lucida context per request (flask is imported lazily; it is not a dependency).

    lucida_flask = LucidaFlask(app, trust_proxy=True)       # or LucidaFlask().init_app(app)

Handlers can then call `client.capture(None, "event")` and `identify_context(user_id)`; see `asgi.py` for what is stamped.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from typing import Any

from ._context import Context, new_context, request_context_args

_KEY = "_lucida_context"


class LucidaFlask:
    def __init__(
        self,
        app: Any = None,
        *,
        request_filter: Callable[[Any], bool] | None = None,
        extra_properties: Mapping[str, Any] | None = None,
        capture_tracing_headers: bool = True,
        trust_proxy: bool = False,
    ) -> None:
        self._filter, self._extra = request_filter, dict(extra_properties or {})
        self._tracing, self._trust = capture_tracing_headers, trust_proxy
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Any) -> LucidaFlask:
        from flask import g, request  # type: ignore[import-not-found,unused-ignore]  # lazy: Flask is optional

        def before() -> None:
            if self._filter and not self._filter(request):
                return
            args = request_context_args(
                lambda name: request.headers.get(name),
                method=request.method,
                url=request.url,
                path=request.path,
                remote_addr=request.remote_addr,
                trust_proxy=self._trust,
                capture_tracing_headers=self._tracing,
                extra=self._extra,
            )
            cm: AbstractContextManager[Context] = new_context(fresh=True, **args)
            cm.__enter__()
            setattr(g, _KEY, cm)

        def after(_exc: BaseException | None) -> None:
            cm = getattr(g, _KEY, None)
            if cm is not None:
                setattr(g, _KEY, None)
                cm.__exit__(None, None, None)

        app.before_request(before)
        app.teardown_request(after)
        return self
