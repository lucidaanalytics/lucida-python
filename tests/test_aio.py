from __future__ import annotations

from typing import Any

import httpx
from conftest import HOST, FakeServer, body

from lucida import AsyncLucida, LucidaError

import pytest

from test_client import FLAGS


def mk(srv: FakeServer, **kw: Any) -> AsyncLucida:
    kw.setdefault("flush_interval", 60.0)
    return AsyncLucida("lc_pk_x", host=HOST, http_client=httpx.AsyncClient(transport=srv.transport()), **kw)


async def test_batch_gzip_and_shutdown() -> None:
    srv = FakeServer()
    async with mk(srv) as lc:
        lc.capture("u", "big", {"blob": "x" * 3000})
        lc.identify("u", {"a": 1})
    (r,) = srv.to("/v1/batch")
    assert r.url.params["compression"] == "gzip"
    assert [e["event"] for e in body(r)["batch"]] == ["big", "$identify"]


async def test_flush_at_and_interval() -> None:
    srv = FakeServer()
    async with mk(srv, flush_at=2) as lc:
        lc.capture("u", "a")
        lc.capture("u", "b")
        assert await lc.flush(5)
        assert len(srv.to("/v1/batch")) == 1
    srv2 = FakeServer()
    async with mk(srv2, flush_interval=0.05) as lc:
        lc.capture("u", "a")
        for _ in range(300):
            if srv2.requests:
                break
            await lc.flush(0.01)
        assert len(srv2.events()) == 1


async def test_retry_after_and_drop_oldest() -> None:
    srv = FakeServer()
    codes = iter([503, 200])
    srv.respond = lambda r: httpx.Response(next(codes), headers={"Retry-After": "0"}) if r.url.path == "/v1/batch" else None
    async with mk(srv, max_queue_size=2, flush_at=100) as lc:
        for i in range(3):
            lc.capture(f"u{i}", "e")
        assert lc.dropped_events == 1
        assert await lc.flush(10)
    assert len(srv.to("/v1/batch")) == 2
    assert [e["distinct_id"] for e in srv.events()] == ["u1", "u2", "u1", "u2"]


async def test_flags_local_remote_and_dedupe() -> None:
    srv = FakeServer()
    srv.flag_defs = FLAGS
    srv.remote_flags = {"cohorted": {"enabled": True, "value": True}}
    async with mk(srv, secret_key="lc_sk_x") as lc:
        assert await lc.get_feature_flag("pro", "u", person_properties={"plan": "pro"}) is True
        assert await lc.get_feature_flag("pro", "u", person_properties={"plan": "pro"}) is True
        assert await lc.is_feature_enabled("cohorted", "u") is True
        assert await lc.get_feature_flag("cohorted", "u", only_evaluate_locally=True) is None
        assert await lc.get_all_flags("u") == {"on": True, "pro": False, "cohorted": True}
        assert await lc.get_feature_flag_payload("on", "u") is None
        await lc.reload_feature_flags()
        assert srv.to("/v1/flags/definitions")[-1].headers["if-none-match"] == '"v1"'
    called = [e for e in srv.events() if e["event"] == "$feature_flag_called"]
    assert [e["properties"]["$feature_flag"] for e in called] == ["pro", "cohorted"]


async def test_import_and_disabled() -> None:
    srv = FakeServer()
    async with mk(srv, secret_key="lc_sk_x") as lc:
        assert await lc.import_events([{"distinct_id": "u", "event": "old"}]) == 1
        srv.respond = lambda r: httpx.Response(500)
        with pytest.raises(LucidaError):
            await lc.import_events([{"distinct_id": "u", "event": "old"}])
    srv2 = FakeServer()
    async with mk(srv2, disabled=True) as lc:
        lc.capture("u", "e")
        assert await lc.get_feature_flag("f", "u") is None
    assert srv2.requests == []
