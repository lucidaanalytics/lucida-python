from __future__ import annotations

from typing import Any

import httpx
from conftest import HOST, FakeServer

from lucida import Lucida


def mk(srv: FakeServer, **kw: Any) -> Lucida:
    kw.setdefault("flush_interval", 60.0)
    return Lucida("lc_pk_x", host=HOST, http_client=httpx.Client(transport=srv.transport()), **kw)


def test_missing_flag_uses_fallback_without_exposure() -> None:
    srv = FakeServer()
    lc = mk(srv, fallbacks={"dark": True, "theme": "blue"})
    d = lc.get_feature_flag_details("dark", "u")
    assert d is not None and (d.value, d.reason, d.evaluated_locally) == (True, "fallback", True)
    assert lc.get_feature_flag("theme", "u") == "blue"
    assert lc.get_feature_flag("other", "u", default="x") == "x"  # no fallback: old behaviour
    lc.flush(5)
    assert srv.events() == []
    lc.shutdown()


def test_network_failure_uses_fallback_per_call_default_only_without_one() -> None:
    srv = FakeServer()
    srv.respond = lambda r: httpx.Response(500) if r.url.path == "/v1/flags" else None
    lc = mk(srv, fallbacks={"dark": True})
    assert lc.get_feature_flag("dark", "u", default=False) is True
    assert lc.get_feature_flag("nope", "u", default=False) is False
    lc.shutdown()


def test_served_flag_keeps_server_value_and_reason() -> None:
    srv = FakeServer()
    srv.remote_flags = {"dark": {"enabled": True, "value": "b", "variant": "b", "reason": "condition_match", "condition_index": 2}}
    lc = mk(srv, fallbacks={"dark": False})
    d = lc.get_feature_flag_details("dark", "u")
    assert d is not None
    assert (d.value, d.variant, d.reason, d.condition_index, d.request_id, d.evaluated_locally) == ("b", "b", "condition_match", 2, "r1", False)
    lc.flush(5)
    assert [e["event"] for e in srv.events()] == ["$feature_flag_called", "$exposure"]
    lc.shutdown()


def test_change_and_stale_hooks() -> None:
    srv = FakeServer()
    srv.flag_defs = []
    lc = mk(srv, secret_key="lc_sk_x", flag_stream=False)
    lc.ready.wait(5)
    seen: list[str] = []
    lc.on_flags_change(lambda: seen.append("change"))
    lc.on_flags_stale(lambda: seen.append("stale"))
    srv.etag = '"v2"'
    lc.reload_feature_flags()
    srv.respond = lambda r: httpx.Response(500)
    lc.reload_feature_flags()
    assert seen == ["change", "stale"]
    lc.shutdown()
