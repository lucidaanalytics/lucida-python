from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
from conftest import HOST, FakeServer

import lucida
from lucida import (
    AsyncLucida,
    InMemoryFlagDefinitionCache,
    Lucida,
    LucidaError,
    get_context,
    identify_context,
    new_context,
    read_tracing_headers,
    scoped,
    set_context_session,
    tag,
)
from lucida.asgi import LucidaASGIMiddleware
from test_client import FLAGS


def mk(srv: FakeServer, **kw: Any) -> Lucida:
    kw.setdefault("flush_interval", 60.0)
    return Lucida("lc_pk_x", host=HOST, http_client=httpx.Client(transport=srv.transport()), **kw)


def sent(srv: FakeServer, lc: Lucida) -> list[dict[str, Any]]:
    assert lc.flush(5)
    return srv.events()


def test_context_merge_nesting_and_explicit_wins() -> None:
    srv = FakeServer()
    lc = mk(srv)
    with new_context(distinct_id="u1", properties={"a": 1, "b": 1}):
        tag("c", 3)
        identify_context("u2")
        set_context_session("s1")
        with new_context():  # inherits
            lc.capture(None, "inner", {"b": 2})
        with new_context(fresh=True):  # does not inherit
            assert get_context() is not None and get_context().distinct_id is None  # type: ignore[union-attr]
        lc.capture("explicit", "outer")
    assert get_context() is None
    inner, outer = sent(srv, lc)
    assert inner["distinct_id"] == "u2" and inner["properties"]["$session_id"] == "s1"
    assert (inner["properties"]["a"], inner["properties"]["b"], inner["properties"]["c"]) == (1, 2, 3)
    assert outer["distinct_id"] == "explicit"  # explicit arg beats context
    lc.shutdown()


def test_child_changes_do_not_leak() -> None:
    with new_context(distinct_id="p"):
        with new_context():
            identify_context("child")
            tag("k", 1)
        c = get_context()
        assert c is not None and c.distinct_id == "p" and "k" not in c.properties


def test_scoped_sync_and_async() -> None:
    @scoped
    def f() -> None:
        identify_context("x")
        assert get_context() is not None

    @scoped(fresh=True)
    async def g() -> str | None:
        identify_context("y")
        return get_context().distinct_id  # type: ignore[union-attr]

    f()
    assert get_context() is None
    assert asyncio.run(g()) == "y"
    assert get_context() is None


def test_missing_distinct_id_raises_and_personless_generates() -> None:
    srv = FakeServer()
    lc = mk(srv)
    with pytest.raises(ValueError):
        lc.capture(None, "e")
    lc.capture(None, "e", {"k": 1}, process_person_profile=False, set={"a": 1})
    (e,) = sent(srv, lc)
    assert e["properties"]["$process_person_profile"] is False
    assert len(e["distinct_id"]) == 36 and "$set" not in e
    lc.shutdown()


def test_tracing_header_helper_sanitizes() -> None:
    h = {"X-Lucida-Distinct-Id": "  ab\x00c\n" + "z" * 2000, "X-Lucida-Session-Id": "   "}
    did, sid = read_tracing_headers(h.get)
    assert did is not None and did.startswith("abc") and len(did) == 1000 and sid is None


def test_before_send_drop_edit_and_raise() -> None:
    srv = FakeServer()
    errs: list[Exception] = []

    def hook(e: dict[str, Any]) -> dict[str, Any] | None:
        if e["event"] == "drop":
            return None
        if e["event"] == "boom":
            raise RuntimeError("x")
        e["properties"]["edited"] = True
        return e

    lc = mk(srv, before_send=hook, on_error=lambda err, evs: errs.append(err))
    for n in ("drop", "boom", "ok"):
        lc.capture("u", n)
    evs = sent(srv, lc)
    assert [e["event"] for e in evs] == ["boom", "ok"]
    assert "edited" not in evs[0]["properties"] and evs[1]["properties"]["edited"] is True
    assert len(errs) == 1
    lc.shutdown()


def test_on_error_batch_dropped_and_max_batch_size() -> None:
    srv = FakeServer()
    srv.respond = lambda r: httpx.Response(400) if r.url.path == "/v1/batch" else None
    errs: list[tuple[Exception, int]] = []
    lc = mk(srv, flush_at=10, max_batch_size=2, on_error=lambda e, evs: errs.append((e, len(evs))))
    for i in range(5):
        lc.capture("u", f"e{i}")
    assert lc.flush(5)
    assert [n for _, n in errs] == [2, 2, 1]
    assert lc.dropped_events == 5
    lc.shutdown()


def test_capture_immediate_and_sync_mode() -> None:
    srv = FakeServer()
    lc = mk(srv)
    uid = lc.capture_immediate("u", "now")
    assert uid and len(srv.events()) == 1  # delivered before returning
    srv.respond = lambda r: httpx.Response(500)
    assert lc.capture_immediate("u", "fail") is None
    lc.shutdown()
    srv2 = FakeServer()
    lc2 = mk(srv2, sync_mode=True)
    lc2.capture("u", "a")
    assert len(srv2.events()) == 1
    lc2.shutdown()


def test_disable_geoip_and_super_properties() -> None:
    srv = FakeServer()
    lc = mk(srv)
    lc.register({"app": "x", "k": "default"})
    lc.capture("u", "a", {"k": "mine"}, disable_geoip=True)
    lc.unregister("app")
    lc.capture("u", "b")
    a, b = sent(srv, lc)
    assert a["properties"]["$geoip_disable"] is True and a["properties"]["app"] == "x" and a["properties"]["k"] == "mine"
    assert "$geoip_disable" not in b["properties"] and "app" not in b["properties"]
    lc.shutdown()
    srv2 = FakeServer()
    lc2 = mk(srv2, disable_geoip=True)
    lc2.capture("u", "c")
    lc2.capture("u", "d", disable_geoip=False)
    c, d = sent(srv2, lc2)
    assert c["properties"]["$geoip_disable"] is True and "$geoip_disable" not in d["properties"]
    lc2.shutdown()


def test_flag_timeout_dedup_cache_and_remote_config() -> None:
    srv = FakeServer()
    srv.remote_configs = {"cfg": {"a": 1}, "a/b": [2]}
    lc = mk(srv, secret_key="lc_sk_x", flag_timeout=1.5, flag_stream=False, flag_dedup_cache_size=1)
    assert lc.get_remote_config_payload("cfg") == {"a": 1}
    req = srv.to("/v1/flags/cfg/remote-config")[-1]
    assert req.method == "GET" and req.extensions["timeout"]["read"] == 1.5
    assert req.headers["authorization"] == "Bearer lc_sk_x"
    assert lc.get_remote_config_payload("a/b") == [2]
    assert srv.to("/v1/flags/a/b/remote-config")[-1].url.raw_path == b"/v1/flags/a%2Fb/remote-config"
    assert lc.get_remote_config_payload("nope") is None
    lc.shutdown()
    with pytest.raises(LucidaError):
        mk(FakeServer()).get_remote_config_payload("cfg")


def test_definition_cache_provider() -> None:
    cache = InMemoryFlagDefinitionCache(ttl=60)
    srv = FakeServer()
    srv.flag_defs = FLAGS
    a = mk(srv, secret_key="lc_sk_x", flag_stream=False, flag_definition_cache=cache)
    assert a.ready.wait(5)
    assert len(srv.to("/v1/flags/definitions")) == 1 and cache.flag_definitions() == FLAGS
    b = mk(srv, secret_key="lc_sk_x", flag_stream=False, flag_definition_cache=cache)
    assert b.ready.wait(5)
    assert len(srv.to("/v1/flags/definitions")) == 1  # adopted from the cache, no second fetch
    assert b.get_all_flags("u", only_evaluate_locally=True)["on"] is True
    a.shutdown()
    b.shutdown()


def test_all_flags_and_payloads() -> None:
    srv = FakeServer()
    srv.remote_flags = {"p": {"enabled": True, "value": True, "payload": 7}, "q": {"enabled": True, "value": True}}
    lc = mk(srv)
    vals, pay = lc.get_all_flags_and_payloads("u")
    assert vals == {"p": True, "q": True} and pay == {"p": 7}
    lc.shutdown()


def test_global_facade() -> None:
    with pytest.raises(RuntimeError):
        lucida.capture("u", "e")
    lucida.api_key, lucida.host, lucida.disabled = "lc_pk_x", HOST, True
    try:
        assert lucida.get_all_flags("u") == {}  # built from module settings (disabled: no network)
        lucida.shutdown()
    finally:
        lucida.api_key = lucida.host = lucida.disabled = None


async def test_async_context_hooks() -> None:
    srv = FakeServer()
    lc = AsyncLucida("lc_pk_x", host=HOST, http_client=httpx.AsyncClient(transport=srv.transport()), flush_interval=60.0)
    async with lc:
        with new_context(distinct_id="a1", session_id="s"):
            lc.capture(None, "ctx")
            assert await lc.capture_immediate(None, "imm")
        lc.capture(None, "anon", process_person_profile=False)
    evs = srv.events()
    assert {e["event"] for e in evs} == {"ctx", "imm", "anon"}
    assert all(e["properties"].get("$session_id") == "s" for e in evs if e["event"] != "anon")


async def test_asgi_middleware_context() -> None:
    seen: dict[str, Any] = {}

    async def app(scope: Any, receive: Any, send: Any) -> None:
        c = get_context()
        seen["c"] = c
        if c is None:
            return
        seen.update(did=c.distinct_id, sid=c.session_id, props=dict(c.properties))

    scope = {
        "type": "http", "method": "POST", "scheme": "https", "path": "/p", "query_string": b"a=1",
        "client": ("9.9.9.9", 1),
        "headers": [(b"host", b"x.dev"), (b"user-agent", b"UA"), (b"x-lucida-distinct-id", b"d1"),
                    (b"x-lucida-session-id", b"s1"), (b"x-forwarded-for", b"1.2.3.4, 5.6.7.8")],
    }  # fmt: skip
    await LucidaASGIMiddleware(app, extra_properties={"svc": "api"})(scope, None, None)  # type: ignore[arg-type]
    assert (seen["did"], seen["sid"]) == ("d1", "s1")
    assert seen["props"] == {
        "$current_url": "https://x.dev/p?a=1", "$request_method": "POST", "$request_path": "/p",
        "$user_agent": "UA", "$ip": "9.9.9.9", "svc": "api",
    }  # fmt: skip
    await LucidaASGIMiddleware(app, trust_proxy=True, capture_tracing_headers=False)(scope, None, None)  # type: ignore[arg-type]
    assert seen["did"] is None and seen["props"]["$ip"] == "1.2.3.4"
    await LucidaASGIMiddleware(app, request_filter=lambda s: False)(scope, None, None)  # type: ignore[arg-type]
    assert seen["c"] is None  # filtered: the app ran without a context


def test_flask_integration_with_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys
    import types

    from lucida.flask_integration import LucidaFlask

    class H(dict):  # type: ignore[type-arg]
        def get(self, k: str, d: Any = None) -> Any:
            return super().get(k.lower(), d)

    g = types.SimpleNamespace()
    req = types.SimpleNamespace(
        headers=H({"user-agent": "UA", "x-lucida-distinct-id": "d9"}), method="GET", url="http://h/x", path="/x",
        remote_addr="1.1.1.1",
    )  # fmt: skip
    monkeypatch.setitem(sys.modules, "flask", types.SimpleNamespace(g=g, request=req))

    class App:
        def before_request(self, f: Any) -> None:
            self.before = f

        def teardown_request(self, f: Any) -> None:
            self.after = f

    app = App()
    LucidaFlask(app)
    app.before()
    c = get_context()
    assert c is not None and c.distinct_id == "d9" and c.properties["$request_path"] == "/x"
    app.after(None)
    assert get_context() is None
