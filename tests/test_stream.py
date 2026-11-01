from __future__ import annotations

import time
from collections.abc import Callable, Iterator

import httpx

from lucida import Lucida
from lucida._stream import SseEvents

from conftest import HOST, FakeServer

ON = {"key": "f", "type": "boolean", "active": True, "salt": "s", "aggregation": {"kind": "person"},
      "conditions": [{"rollout_pct": 100}]}


def wait_for(cond: Callable[[], bool], timeout: float = 3.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return
        time.sleep(0.01)
    raise AssertionError("timeout")


def test_sse_events_parser() -> None:
    p = SseEvents()
    out = [p.feed(x) for x in [": keepalive", "", "event: definitions", 'data: {"etag":"a"}', "", "data: x", ""]]
    assert out == [None, None, None, None, "definitions", None, "message"]


def test_push_reloads_and_disconnect_falls_back_to_polling() -> None:
    fake = FakeServer()
    gate = {"push": False, "end": False}

    def events() -> Iterator[bytes]:
        yield b'event: definitions\ndata: {"etag":"v0"}\n\n'
        while not gate["push"]:
            time.sleep(0.01)
        yield b'event: definitions\ndata: {"etag":"v1"}\n\n'
        while not gate["end"]:
            time.sleep(0.01)

    streams = {"n": 0}

    def respond(req: httpx.Request) -> httpx.Response | None:
        if req.url.path != "/v1/flags/stream":
            return None
        streams["n"] += 1
        if streams["n"] == 1:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=events())
        return httpx.Response(503)

    fake.respond = respond
    fake.flag_defs = []
    c = Lucida("lc_pk_x", host=HOST, secret_key="lc_sk_x", flag_poll_interval=0.05, stream_backoff=0.01,
               http_client=httpx.Client(transport=fake.transport()))
    assert c.ready.wait(2)
    wait_for(lambda: streams["n"] >= 1)
    assert not c.is_feature_enabled("f", "u1", only_evaluate_locally=True)
    fake.flag_defs, fake.etag = [ON], '"v2"'
    gate["push"] = True
    wait_for(lambda: c.is_feature_enabled("f", "u1", only_evaluate_locally=True) is True)
    # stream ends and reconnects keep failing: polling must pick up the next change
    gate["end"] = True
    wait_for(lambda: streams["n"] >= 3)
    fake.flag_defs, fake.etag = [], '"v3"'
    wait_for(lambda: c.is_feature_enabled("f", "u1", only_evaluate_locally=True) is not True)
    c.shutdown()
