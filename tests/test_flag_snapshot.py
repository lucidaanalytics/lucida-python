from __future__ import annotations

import asyncio

from lucida import AsyncLucida, Lucida, device_scope, flag_event_props


def _on(key: str, *ctxs: str, pct: int = 100, agg: str = "person") -> dict[str, object]:
    return {"key": key, "type": "boolean", "active": True, "salt": "s", "aggregation": {"kind": agg},
            "app_contexts": list(ctxs), "conditions": [{"rollout_pct": pct}]}


FLAGS = [_on("a"), _on("b"), _on("off", pct=0), _on("web", "web"), _on("dev", agg="device")]


def test_capture_send_feature_flags_and_snapshot() -> None:
    lc = Lucida("lc_pk_x", secret_key="lc_sk_x", test_mode=FLAGS, app_contexts=["api"])
    lc.capture("u", "buy", {"$feature/a": "mine"}, send_feature_flags=True)
    snap = lc.evaluate_flags("u")
    assert snap.is_enabled("a") and snap.keys()[0] == "a" and "web" not in snap.keys()
    lc.capture("u", "e1", flags=snap.only_accessed())
    assert lc.sink is not None
    p0, p1 = lc.sink.events[0]["properties"], lc.sink.events[1]["properties"]
    assert p0["$feature/a"] == "mine" and p0["$feature/off"] is False and "$feature/web" not in p0
    assert p0["$active_feature_flags"] == ["a", "b"]  # dev has no device id
    assert p1["$feature/a"] is True and "$feature/b" not in p1
    assert all(e["event"] != "$feature_flag_called" for e in lc.sink.events)


def test_default_and_device_scope() -> None:
    lc = Lucida("lc_pk_x", secret_key="lc_sk_x", test_mode=FLAGS)
    assert lc.get_feature_flag("missing", "u", default="control") == "control"
    assert lc.get_feature_flag("dev", "u", send_exposure=False) is False
    with device_scope("d1"):
        assert lc.get_feature_flag("dev", "u", send_exposure=False) is True
    assert lc.get_feature_flag("dev", "u", device_id="d2", send_exposure=False) is True


def test_flag_event_props_caller_wins() -> None:
    assert flag_event_props({"x": 1}, {}) == {"x": 1}
    assert flag_event_props({"$active_feature_flags": []}, {"a": "v"})["$active_feature_flags"] == []


def test_async_evaluate_flags() -> None:
    async def run() -> None:
        async with AsyncLucida("lc_pk_x", secret_key="lc_sk_x", test_mode=FLAGS) as lc:
            snap = await lc.evaluate_flags("u")
            assert snap.only(["a", "zz"]).values == {"a": True}
            lc.capture("u", "e", send_feature_flags=True)
            assert lc.sink is not None and lc.sink.events[-1]["properties"]["$feature/b"] is True

    asyncio.run(run())
