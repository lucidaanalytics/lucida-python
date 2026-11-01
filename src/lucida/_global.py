"""Module-level facade: `lucida.api_key = "..."; lucida.capture(...)`. The client is built on first use."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from .client import Lucida

# Module attributes of `lucida` that are forwarded to the client constructor when set (not None).
OPTIONS = (
    "host", "secret_key", "debug", "disabled", "flush_at", "flush_interval", "max_batch_size",
    "before_send", "on_error", "sync_mode", "disable_geoip", "flag_timeout", "timeout", "default_properties",
)  # fmt: skip

_lock = threading.Lock()
_client: Lucida | None = None


def default_client() -> Lucida:
    """The process-wide client; reads `lucida.api_key` and the other settings once, on first call."""
    global _client
    import lucida as pkg

    with _lock:
        if _client is None:
            if not pkg.api_key:
                raise RuntimeError("set lucida.api_key before calling lucida.capture() and friends")
            _client = Lucida(pkg.api_key, **{k: v for k in OPTIONS if (v := getattr(pkg, k, None)) is not None})
        return _client


def delegate(name: str) -> Callable[..., Any]:
    def call(*args: Any, **kwargs: Any) -> Any:
        return getattr(default_client(), name)(*args, **kwargs)

    call.__name__ = name
    call.__doc__ = f"`Lucida.{name}` on the module-level client."
    return call


def shutdown(timeout: float = 10.0) -> None:
    """Flush and stop the module-level client (if one was built); a later call builds a fresh one."""
    global _client
    with _lock:
        c, _client = _client, None
    if c is not None:
        c.shutdown(timeout)
