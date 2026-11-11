from __future__ import annotations

from typing import Any

import httpx
from conftest import HOST, FakeServer

from lucida import Lucida
from lucida import _queue as q


def mk(srv: FakeServer, **kw: Any) -> Lucida:
    kw.setdefault("flush_interval", 60.0)
    return Lucida("lc_pk_x", host=HOST, http_client=httpx.Client(transport=srv.transport()), **kw)


def test_set_person_properties_events() -> None:
    srv = FakeServer()
    lc = mk(srv)
    lc.set_person_properties("u", {"plan": "pro"})
    lc.set_person_properties_once("u", {"first": "x"})
    lc.shutdown()
    a, b = srv.events()
    assert a["event"] == "$set" and a["$set"] == {"plan": "pro"}
    assert b["event"] == "$set" and b["$set_once"] == {"first": "x"}


def test_identify_and_group_dedupe() -> None:
    srv = FakeServer()
    lc = mk(srv)
    for _ in range(3):
        lc.identify("u", {"a": 1})
        lc.group_identify("org", "acme", {"n": 1})
    lc.identify("u", {"a": 2})
    lc.shutdown()
    assert [e["event"] for e in srv.events()] == ["$identify", "$groupidentify", "$identify"]


def test_identify_retried_after_dropped_batch() -> None:
    srv = FakeServer()
    calls: list[int] = []

    def respond(r: httpx.Request) -> httpx.Response | None:
        calls.append(1)
        return httpx.Response(400) if len(calls) == 1 else None

    srv.respond = respond
    errors: list[object] = []
    lc = mk(srv, on_error=lambda e, evs: errors.append(e))
    lc.identify("u", {"a": 1})
    lc.group_identify("org", "acme", {"n": 1})
    lc.flush()
    assert len(errors) == 1
    lc.identify("u", {"a": 1})
    lc.group_identify("org", "acme", {"n": 1})
    lc.shutdown()
    # the rejected batch is recorded by the fake too; without the fix the repeat would never be sent
    assert [e["event"] for e in srv.events()] == ["$identify", "$groupidentify"] * 2


def test_import_events_applies_before_send_and_defaults_unless_raw() -> None:
    srv = FakeServer()
    lc = mk(
        srv,
        secret_key="lc_sk_x",
        default_properties={"env": "prod"},
        before_send=lambda e: None if e["event"] == "drop" else e,
    )
    assert lc.import_events([{"distinct_id": "u", "event": "keep"}, {"distinct_id": "u", "event": "drop"}]) == 1
    assert lc.import_events([{"distinct_id": "u", "event": "plain"}], raw=True) == 1
    keep, plain = srv.events("/v1/s2s/batch")
    assert keep["properties"]["env"] == "prod" and "env" not in plain["properties"]
    lc.shutdown()


def test_capture_immediate_retries_then_succeeds() -> None:
    srv = FakeServer()
    calls: list[int] = []

    def respond(r: httpx.Request) -> httpx.Response | None:
        calls.append(1)
        return httpx.Response(503) if len(calls) < 3 else None

    srv.respond = respond
    lc = mk(srv, sync_mode=True)
    lc._direct._sleep = lambda s: None  # type: ignore[union-attr]
    assert lc.capture_immediate("u", "e") is not None
    assert len(calls) == 3
    lc.shutdown()


def test_direct_sender_gives_up_after_cap_and_not_on_4xx() -> None:
    seen: list[int] = []
    errors: list[Exception] = []
    def reply(status: int) -> tuple[int, None]:
        seen.append(1)
        return (status, None)

    s = q.DirectSender(lambda b: reply(500), on_error=lambda e, ev: errors.append(e), sleep=lambda d: None)
    assert not s.send({"uuid": "1"})
    assert len(seen) == 3 and len(errors) == 1
    seen.clear()
    s2 = q.DirectSender(lambda b: reply(400), sleep=lambda d: None)
    assert not s2.send({"uuid": "2"})
    assert len(seen) == 1


def test_default_properties_are_super_properties() -> None:
    srv = FakeServer()
    lc = mk(srv, default_properties={"app": "a"})
    lc.capture("u", "e")
    lc.shutdown()
    assert srv.events()[0]["properties"]["app"] == "a"
