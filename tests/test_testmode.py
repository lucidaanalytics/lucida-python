from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest

from lucida import AsyncLucida, Lucida

PRO = {
    "key": "pro", "type": "boolean", "active": True, "salt": "s", "aggregation": {"kind": "person"},
    "conditions": [{"rollout_pct": 100, "filters": [{"type": "person", "key": "plan", "op": "eq", "value": "pro"}]}],
}
ON = {"key": "on", "type": "boolean", "active": True, "salt": "s", "aggregation": {"kind": "person"}, "conditions": [{"rollout_pct": 100}]}


def fixture(tmp_path: Path) -> Path:
    f = tmp_path / "flags.json"
    f.write_text(json.dumps({"flags": [ON, PRO]}))
    return f


def test_fixture_file_offline_and_sink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a: object, **k: object) -> None:
        raise AssertionError("network used")

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", boom)
    with Lucida("lc_pk_x", secret_key="lc_sk_x", test_mode=fixture(tmp_path)) as lc:
        assert lc.ready.is_set()
        assert lc.get_feature_flag("on", "u1") is True
        assert lc.is_feature_enabled("pro", "u1", person_properties={"plan": "pro"}) is True
        assert lc.is_feature_enabled("pro", "u1", person_properties={"plan": "free"}) is False
        assert lc.get_feature_flag("missing", "u1") is None  # no remote fallback
        lc.get_feature_flag("on", "u1")  # exposure dedupes per (user, flag, value)
        lc.capture("u1", "signup", {"plan": "pro"})
        assert lc.import_events([{"distinct_id": "u2", "event": "old"}]) == 1
        assert lc.sink is not None
        names = [e["event"] for e in lc.sink.events]
        assert names.count("$exposure") == 3  # on=True, pro=True, pro=False
        assert names[-2:] == ["signup", "old"]
        assert next(e for e in lc.sink.events if e["event"] == "signup")["properties"]["plan"] == "pro"
        assert lc.get_all_flags("u1", person_properties={"plan": "pro"}) == {"on": True, "pro": True}


def test_accepts_list_and_rejects_bad_fixture() -> None:
    lc = Lucida("lc_pk_x", test_mode=[ON])
    assert lc.get_feature_flag("on", "u1", send_exposure=False) is True
    assert lc.sink is not None and lc.sink.events == []
    with pytest.raises(ValueError):
        Lucida("lc_pk_x", test_mode={"nope": 1})


def test_async(tmp_path: Path) -> None:
    async def run() -> None:
        async with AsyncLucida("lc_pk_x", secret_key="lc_sk_x", test_mode=fixture(tmp_path)) as lc:
            assert await lc.get_feature_flag("on", "u1") is True
            assert await lc.get_feature_flag("missing", "u1") is None
            lc.capture("u1", "signup")
            assert await lc.flush() is True
            assert lc.sink is not None
            assert [e["event"] for e in lc.sink.events] == ["$feature_flag_called", "$exposure", "signup"]

    asyncio.run(run())
