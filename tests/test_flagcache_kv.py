from __future__ import annotations

from typing import Any

import httpx
from conftest import HOST, FakeServer
from test_client import FLAGS

from lucida import Lucida, RedisFlagDefinitionCache


class FakeRedis:
    """The slice of redis-py the cache uses; `nx` replies None when the key exists, like Redis."""

    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self.ttls: dict[str, int | None] = {}

    def get(self, name: str) -> str | None:
        return self.data.get(name)

    def set(self, name: str, value: str, *, ex: int | None = None, nx: bool = False) -> Any:
        if nx and name in self.data:
            return None
        self.data[name], self.ttls[name] = value, ex
        return True


def test_lock_winner_publishes_and_loser_reads() -> None:
    r = FakeRedis()
    a = RedisFlagDefinitionCache(r, prefix="t", ttl=60, lock_ttl=5)
    b = RedisFlagDefinitionCache(r, prefix="t")
    assert a.should_fetch_flag_definitions() is True
    assert b.should_fetch_flag_definitions() is False
    assert b.flag_definitions() is None  # nothing published yet: the loser fetches itself
    a.on_flag_definitions_received(FLAGS)
    assert b.flag_definitions() == FLAGS
    assert r.ttls == {"t:lock": 5, "t:defs": 60}


def test_corrupt_copy_reads_as_nothing_cached() -> None:
    r = FakeRedis()
    r.data["lucida:flags:defs"] = "{oops"
    assert RedisFlagDefinitionCache(r).flag_definitions() is None
    r.data["lucida:flags:defs"] = '{"not": "a list"}'
    assert RedisFlagDefinitionCache(r).flag_definitions() is None


def test_two_workers_share_one_fetch() -> None:
    r = FakeRedis()
    srv = FakeServer()
    srv.flag_defs = FLAGS

    def mk() -> Lucida:
        return Lucida(
            "lc_pk_x", host=HOST, http_client=httpx.Client(transport=srv.transport()), flush_interval=60.0,
            secret_key="lc_sk_x", flag_stream=False, flag_definition_cache=RedisFlagDefinitionCache(r),
        )

    a = mk()
    assert a.ready.wait(5)
    b = mk()
    assert b.ready.wait(5)
    assert len(srv.to("/v1/flags/definitions")) == 1
    assert b.get_all_flags("u", only_evaluate_locally=True)["on"] is True
    a.shutdown()
    b.shutdown()
