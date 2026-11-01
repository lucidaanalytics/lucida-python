"""Shared fixtures: testdata/flags vectors (evaluator) and testdata/events wire fixtures (client)."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from conftest import HOST, FakeServer, load

from lucida import Lucida
from lucida._eval import evaluate_all, hash01

CTX_FIELDS = {
    "distinct_id": ["distinct_id", "bucketing_key"],
    "groups": ["groups"],
    "person_properties": ["person_properties"],
    "group_properties": ["group_properties"],
    "device_properties": ["device_properties"],
    "geo_properties": ["geo_properties"],
    "app_version": ["app_version"],
}


def supplied(vec: dict[str, Any]) -> dict[str, Any]:
    """Only the context fields named by `needs`: anything else must not influence the result."""
    ctx: dict[str, Any] = vec["context"]
    if "needs" not in vec:
        return ctx
    keep = {f for n in vec["needs"] for f in CTX_FIELDS.get(n, [])}
    return {k: v for k, v in ctx.items() if k in keep}


def check_file(name: str) -> int:
    d = load(f"flags/{name}")
    for h in d["hash_vectors"]:
        assert hash01(h["input"]) == h["hash"], h["input"]
    for v in d["vectors"]:
        r = evaluate_all(d["flags"], supplied(v))[v["flag"]]
        e = v["expected"]
        got = (r.value, r.variant, r.payload, r.reason, r.condition_index)
        want = (e["value"], e.get("variant"), e.get("payload"), e["reason"], e["condition_index"])
        assert got == want, f"{v['name']}: {got} != {want}"
    return len(d["vectors"])


def test_vectors_basic() -> None:
    assert check_file("vectors_basic.json") > 20


def test_vectors_g10() -> None:
    assert check_file("vectors_g10.json") > 15


def test_vectors_sticky() -> None:
    assert check_file("vectors_sticky.json") >= 19


def test_vectors_ops() -> None:
    assert check_file("vectors_ops.json") >= 110


def test_vectors_generated() -> None:
    assert check_file("vectors.json") == 10_000


def call(lc: Lucida, c: dict[str, Any]) -> None:
    m = c["method"]
    if m == "capture":
        lc.capture(
            c["distinct_id"], c["event"], c.get("properties"), groups=c.get("groups"),
            timestamp=c.get("timestamp"), uuid=c.get("uuid"), set=c.get("set"), set_once=c.get("set_once"),
        )
    elif m == "identify":
        lc.identify(c["distinct_id"], c.get("properties"), c.get("properties_set_once"))
    elif m == "alias":
        lc.alias(c["distinct_id"], c["alias"])
    else:
        lc.group_identify(c["group_type"], c["group_key"], c.get("properties"), c.get("distinct_id"))


@pytest.mark.parametrize("case", load("events/server_sdk.json")["cases"], ids=lambda c: str(c["name"]))
def test_event_fixtures(case: dict[str, Any]) -> None:
    srv = FakeServer()
    with Lucida("lc_pk_x", host=HOST, http_client=httpx.Client(transport=srv.transport())) as lc:
        call(lc, case["call"])
        lc.flush()
    assert srv.bodies("/v1/batch")[0]["api_key"] == "lc_pk_x"
    (ev,) = srv.events()
    props = dict(ev["properties"])
    assert props.pop("$lib").startswith("lucida-")
    assert props.pop("$lib_version")
    ev = {**ev, "properties": props}
    want = dict(case["expected"])
    for k in ("uuid", "timestamp"):
        if want[k] == "*":
            assert ev[k]
            want[k] = ev[k]
    assert ev == want
