from __future__ import annotations

import time
from typing import Any

import httpx
from conftest import HOST, FakeServer, body

from lucida import ImportEvent, Lucida, LucidaError
from lucida import _queue as q
from lucida._core import LruSet, retry_delay

import pytest

FLAGS: list[dict[str, Any]] = [
    {"key": "on", "type": "boolean", "active": True, "salt": "s", "aggregation": {"kind": "person"},
     "conditions": [{"rollout_pct": 100}]},
    {"key": "pro", "type": "boolean", "active": True, "salt": "s", "aggregation": {"kind": "person"},
     "conditions": [{"rollout_pct": 100, "filters": [{"type": "person", "key": "plan", "op": "eq", "value": "pro"}]}]},
    {"key": "cohorted", "type": "boolean", "active": True, "salt": "s", "aggregation": {"kind": "person"},
     "conditions": [{"rollout_pct": 100, "filters": [{"type": "cohort", "key": "c1", "op": "in", "value": 1}]}]},
]


def mk(srv: FakeServer, **kw: Any) -> Lucida:
    kw.setdefault("flush_interval", 60.0)
    return Lucida("lc_pk_x", host=HOST, http_client=httpx.Client(transport=srv.transport()), **kw)


def test_flushes_at_batch_size() -> None:
    srv = FakeServer()
    lc = mk(srv, flush_at=3)
    for i in range(3):
        lc.capture(f"u{i}", "e")
    assert lc.flush(5)
    assert len(srv.events()) == 3
    lc.shutdown()


def test_flushes_on_interval() -> None:
    srv = FakeServer()
    lc = mk(srv, flush_interval=0.05)
    lc.capture("u", "e")
    deadline = time.time() + 3
    while not srv.requests and time.time() < deadline:
        time.sleep(0.01)
    assert len(srv.events()) == 1
    lc.shutdown()


def test_gzip_above_1kb() -> None:
    srv = FakeServer()
    lc = mk(srv)
    lc.capture("u", "big", {"blob": "x" * 3000})
    lc.capture("u", "small")
    lc.flush(5)
    lc.shutdown()
    (r,) = srv.to("/v1/batch")
    assert r.url.params["compression"] == "gzip"
    assert len(body(r)["batch"]) == 2


def test_retry_honours_retry_after_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float | None] = []

    def fast(attempt: int, retry_after: float | None = None) -> float:
        delays.append(retry_after)
        return 0.01

    monkeypatch.setattr(q, "retry_delay", fast)
    srv = FakeServer()
    codes = iter([429, 503, 200])
    srv.respond = lambda r: (
        httpx.Response(next(codes), headers={"Retry-After": "2"}) if r.url.path == "/v1/batch" else None
    )
    lc = mk(srv)
    lc.capture("u", "e")
    assert lc.flush(5)
    lc.shutdown()
    assert delays == [2.0, 2.0]
    assert len(srv.to("/v1/batch")) == 3
    assert lc.dropped_events == 0


def test_retry_delay_is_jittered_and_floored_by_retry_after() -> None:
    assert all(0.5 <= retry_delay(0) <= 1.0 for _ in range(50))
    assert retry_delay(0, 7.0) == 7.0
    assert retry_delay(10) <= 30.0


def test_non_retryable_drops_and_counts() -> None:
    srv = FakeServer()
    srv.respond = lambda r: httpx.Response(400)
    lc = mk(srv)
    lc.capture("u", "e")
    lc.flush(5)
    assert len(srv.to("/v1/batch")) == 1
    assert lc.dropped_events == 1
    lc.shutdown()


def test_bounded_queue_drops_oldest() -> None:
    srv = FakeServer()
    lc = mk(srv, max_queue_size=3, flush_at=100)
    for i in range(5):
        lc.capture(f"u{i}", "e")
    assert lc.dropped_events == 2
    lc.flush(5)
    assert [e["distinct_id"] for e in srv.events()] == ["u2", "u3", "u4"]
    lc.shutdown()


def test_disabled_does_nothing() -> None:
    srv = FakeServer()
    lc = mk(srv, disabled=True, secret_key="lc_sk_x")
    lc.capture("u", "e")
    assert lc.get_feature_flag("on", "u") is None
    assert lc.get_all_flags("u") == {}
    assert lc.import_events([]) == 0
    lc.shutdown()
    assert srv.requests == []


def test_shutdown_flushes_and_stops_accepting() -> None:
    srv = FakeServer()
    lc = mk(srv)
    lc.capture("u", "e")
    lc.shutdown()
    lc.capture("u", "late")
    assert [e["event"] for e in srv.events()] == ["e"]


def test_local_flags_etag_poll_and_exposure_dedupe() -> None:
    srv = FakeServer()
    srv.flag_defs = FLAGS
    lc = mk(srv, secret_key="lc_sk_x")
    assert lc.ready.wait(5)
    assert lc.get_feature_flag("pro", "u1", person_properties={"plan": "pro"}) is True
    assert lc.get_feature_flag("pro", "u1", person_properties={"plan": "pro"}) is True
    assert lc.get_feature_flag("pro", "u1", person_properties={"plan": "free"}) is False
    assert lc.is_feature_enabled("on", "u1") is True
    lc.flush(5)
    names = [(e["event"], e["properties"]["$feature_flag_response"]) for e in srv.events()]
    assert names == [("$feature_flag_called", True), ("$exposure", True), ("$feature_flag_called", False),
                     ("$exposure", False), ("$feature_flag_called", True), ("$exposure", True)]
    assert srv.to("/v1/flags") == []
    assert srv.events()[0]["properties"]["locally_evaluated"] is True
    d = srv.to("/v1/flags/definitions")[0]
    assert d.headers["authorization"] == "Bearer lc_sk_x"
    lc.reload_feature_flags()
    assert srv.to("/v1/flags/definitions")[-1].headers["if-none-match"] == '"v1"'
    srv.flag_defs = []
    srv.etag = '"v2"'
    lc.reload_feature_flags()
    assert lc.get_all_flags("u1", only_evaluate_locally=True) == {}
    lc.shutdown()


def test_remote_fallback_on_requires_server_and_unknown() -> None:
    srv = FakeServer()
    srv.flag_defs = FLAGS
    srv.remote_flags = {"cohorted": {"enabled": True, "value": True, "reason": "x"},
                        "other": {"enabled": True, "variant": "b", "value": "b", "payload": {"k": 1}}}
    lc = mk(srv, secret_key="lc_sk_x")
    lc.ready.wait(5)
    assert lc.get_feature_flag("cohorted", "u1", send_exposure=False) is True
    assert lc.get_feature_flag("other", "u1", send_exposure=False) == "b"
    assert lc.get_feature_flag_payload("other", "u1") == {"k": 1}
    assert lc.get_feature_flag("cohorted", "u1", only_evaluate_locally=True) is None
    sent = srv.bodies("/v1/flags")[0]
    assert sent["api_key"] == "lc_pk_x" and sent["flag_keys"] == ["cohorted"] and sent["distinct_id"] == "u1"
    all_ = lc.get_all_flags("u1", person_properties={"plan": "pro"})
    assert all_ == {"on": True, "pro": True, "cohorted": True, "other": "b"}
    lc.flush(5)
    called = [e for e in srv.events() if e["event"] == "$feature_flag_called"]
    assert called == [] or all(e["properties"]["$feature_flag_request_id"] == "r1" for e in called)
    lc.shutdown()


def test_remote_only_without_secret_key() -> None:
    srv = FakeServer()
    srv.remote_flags = {"f": {"enabled": True, "value": True}}
    lc = mk(srv)
    assert lc.get_feature_flag("f", "u") is True
    assert srv.to("/v1/flags/definitions") == []
    lc.flush(5)
    assert srv.events()[0]["properties"]["locally_evaluated"] is False
    lc.shutdown()


def test_import_events_chunks_and_bearer() -> None:
    srv = FakeServer()
    lc = mk(srv, secret_key="lc_sk_x")
    evs: list[ImportEvent] = [{"distinct_id": f"u{i}", "event": "old", "timestamp": "2020-01-01T00:00:00Z"} for i in range(1500)]
    assert lc.import_events(evs) == 1500
    rs = srv.to("/v1/s2s/batch")
    assert [len(body(r)["batch"]) for r in rs] == [1000, 500]
    assert rs[0].headers["authorization"] == "Bearer lc_sk_x"
    assert "api_key" not in body(rs[0])
    assert body(rs[0])["batch"][0]["timestamp"] == "2020-01-01T00:00:00Z"
    srv.respond = lambda r: httpx.Response(400)
    with pytest.raises(LucidaError):
        lc.import_events(evs[:1])
    with pytest.raises(LucidaError):
        mk(srv).import_events(evs[:1])
    lc.shutdown()


def test_lru_set_evicts_least_recent() -> None:
    s = LruSet(2)
    assert s.add("a") and s.add("b") and not s.add("a") and s.add("c")
    assert len(s) == 2 and s.add("b")
