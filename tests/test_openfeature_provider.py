"""Stdlib-only tests: the provider module has no imports from the rest of the package, so it loads by path
(importing `lucida` itself needs httpx)."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

def _testdata(rel: str) -> Path:
    """$LUCIDA_TESTDATA, else the nearest testdata/ above this file (monorepo and mirror layouts both work)."""
    if os.environ.get("LUCIDA_TESTDATA"):
        return Path(os.environ["LUCIDA_TESTDATA"]) / rel
    return next(p / "testdata" / rel for p in Path(__file__).resolve().parents if (p / "testdata" / rel).exists())


_SRC = Path(__file__).resolve().parents[1] / "src" / "lucida" / "openfeature_provider.py"
_spec = importlib.util.spec_from_file_location("lucida_openfeature_provider", _SRC)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = mod  # dataclasses resolve string annotations through sys.modules
_spec.loader.exec_module(mod)


@dataclass
class Ctx:
    targeting_key: str | None = "u1"
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class Det:
    value: Any
    payload: Any = None
    variant: str | None = None
    reason: str = "condition_match"
    condition_index: int = 0
    request_id: str | None = None
    evaluated_locally: bool = True


class FakeFlags:
    """`values` flags resolve with reason condition_match; `fallbacks` resolve with reason fallback."""

    def __init__(self, values: dict[str, Any], payloads: dict[str, Any] | None = None, fallbacks: dict[str, Any] | None = None) -> None:
        self.values, self.payloads, self.fallbacks = values, payloads or {}, fallbacks or {}
        self.calls: list[tuple[str, str, Any]] = []
        self.kw: dict[str, Any] = {}
        self.captured: list[tuple[Any, ...]] = []
        self.change: list[Any] = []
        self.stale: list[Any] = []

    def on_flags_change(self, cb: Any) -> None:
        self.change.append(cb)

    def on_flags_stale(self, cb: Any) -> None:
        self.stale.append(cb)

    def capture(self, distinct_id: Any, event: str, properties: Any = None, **kw: Any) -> None:
        self.captured.append((distinct_id, event, properties, kw))

    def get_feature_flag_details(self, key: str, distinct_id: str, *, person_properties: Any = None, **kw: Any) -> Any:
        self.calls.append((key, distinct_id, person_properties))
        self.kw = kw
        if key in self.values:
            v = self.values[key]
            return Det(v, self.payloads.get(key), v if isinstance(v, str) else None, request_id="r1", evaluated_locally=False)
        if key in self.fallbacks:
            return Det(self.fallbacks[key], reason="fallback", condition_index=-1)
        return None


class ProviderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.flags = FakeFlags(
            {"on": True, "off": False, "multi": "blue", "limit": True, "ratio": True, "cfg": True, "bad": True, "flag-b": True},
            {"limit": 5, "ratio": 0.25, "cfg": {"a": 1}, "bad": "text", "flag-b": True},
        )
        self.p = mod.LucidaProvider(self.flags)

    def test_metadata(self) -> None:
        self.assertEqual(self.p.get_metadata().name, "lucida")
        self.assertEqual(self.p.get_provider_hooks(), [])

    def test_boolean_and_string(self) -> None:
        self.assertIs(self.p.resolve_boolean_details("on", False, Ctx()).value, True)
        self.assertIs(self.p.resolve_boolean_details("off", True, Ctx()).value, False)
        d = self.p.resolve_boolean_details("multi", False, Ctx())  # any variant is "on"
        self.assertIs(d.value, True)
        self.assertEqual(d.variant, "blue")
        s = self.p.resolve_string_details("multi", "x", Ctx())
        self.assertEqual((s.value, s.variant, s.reason), ("blue", "blue", mod.Reason.SPLIT))

    def test_targeting_key_is_distinct_id_and_attributes_are_person_properties(self) -> None:
        self.p.resolve_boolean_details("on", False, Ctx("user-9", {"plan": "pro"}))
        self.assertEqual(self.flags.calls[-1], ("on", "user-9", {"plan": "pro"}))

    def test_reason_table_matches_shared_json(self) -> None:
        table = json.loads(_testdata("openfeature/reasons.json").read_text())
        for raw, row in table["reasons"].items():
            self.assertEqual(mod.map_reason(raw, True).value, row["reason"], raw)
            if "variant_reason" in row:
                self.assertEqual(mod.map_reason(raw, "blue").value, row["variant_reason"], raw)
        self.assertEqual(mod.map_reason("brand_new", True).value, "UNKNOWN")

    def test_flag_metadata(self) -> None:
        d = self.p.resolve_boolean_details("on", False, Ctx())
        self.assertEqual(d.flag_metadata, {"lucidaReason": "condition_match", "evaluatedLocally": False, "conditionIndex": 0, "requestId": "r1"})

    def test_fallback_keeps_value_without_error(self) -> None:
        p = mod.LucidaProvider(FakeFlags({}, fallbacks={"dark": True, "cap": 7}))
        d = p.resolve_boolean_details("dark", False, Ctx())
        self.assertEqual((d.value, d.reason, d.error_code), (True, mod.Reason.DEFAULT, None))
        self.assertEqual(d.flag_metadata["lucidaReason"], "fallback")
        self.assertNotIn("conditionIndex", d.flag_metadata)
        self.assertEqual(p.resolve_integer_details("cap", 0, Ctx()).value, 7)

    def test_groups_and_group_properties(self) -> None:
        self.p.resolve_boolean_details("on", False, Ctx("u", {"plan": "pro", "groups": {"org": "o1"}, "groupProperties": {"org": {"tier": "x"}}}))
        self.assertEqual(self.flags.calls[-1], ("on", "u", {"plan": "pro"}))
        self.assertEqual(self.flags.kw, {"groups": {"org": "o1"}, "group_properties": {"org": {"tier": "x"}}})

    def test_track_captures(self) -> None:
        @dataclass
        class Details:
            value: float | None = None
            attributes: dict[str, Any] = field(default_factory=dict)

        self.p.track("purchase", Ctx("u9"), Details(12.5, {"sku": "a"}))
        self.assertEqual(self.flags.captured[-1][:3], ("u9", "purchase", {"sku": "a", "$value": 12.5}))
        with self.assertRaises(mod.TargetingKeyMissingError):
            self.p.track("purchase", Ctx(None), None)

    def test_events_on_change_and_stale(self) -> None:
        self.p.initialize(None)
        self.flags.change[0]()
        self.flags.stale[0]()
        self.assertEqual([e[0] for e in self.p.emitted], ["PROVIDER_CONFIGURATION_CHANGED", "PROVIDER_STALE"])

    def test_first_load_failure_is_error_then_ready(self) -> None:
        f = FakeFlags({})
        f.local_evaluation, f.definitions_loaded = True, False  # type: ignore[attr-defined]
        p = mod.LucidaProvider(f)
        with self.assertRaises(mod.GeneralError):
            p.initialize(None)
        f.stale[0]()  # not ready yet: no STALE
        f.change[0]()
        self.assertEqual([e[0] for e in p.emitted], ["PROVIDER_READY"])

    def test_missing_targeting_key(self) -> None:
        for bad in (Ctx(None), Ctx(""), None):
            with self.assertRaises(mod.TargetingKeyMissingError):
                self.p.resolve_boolean_details("on", False, bad)

    def test_unknown_flag(self) -> None:
        with self.assertRaises(mod.FlagNotFoundError):
            self.p.resolve_boolean_details("nope", False, Ctx())

    def test_type_mismatch(self) -> None:
        with self.assertRaises(mod.TypeMismatchError):
            self.p.resolve_string_details("on", "x", Ctx())  # boolean flag is not a string variant
        with self.assertRaises(mod.TypeMismatchError):
            self.p.resolve_integer_details("bad", 0, Ctx())
        with self.assertRaises(mod.TypeMismatchError):
            self.p.resolve_integer_details("flag-b", 0, Ctx())  # a bool payload is not an integer
        with self.assertRaises(mod.TypeMismatchError):
            self.p.resolve_integer_details("ratio", 0, Ctx())  # 0.25 is not integral
        with self.assertRaises(mod.TypeMismatchError):
            self.p.resolve_object_details("limit", {}, Ctx())

    def test_payload_values(self) -> None:
        self.assertEqual(self.p.resolve_integer_details("limit", 0, Ctx()).value, 5)
        self.assertEqual(self.p.resolve_float_details("ratio", 0.0, Ctx()).value, 0.25)
        self.assertEqual(self.p.resolve_float_details("limit", 0.0, Ctx()).value, 5.0)
        self.assertEqual(self.p.resolve_object_details("cfg", {}, Ctx()).value, {"a": 1})


try:
    from openfeature import api as of_api
    from openfeature.evaluation_context import EvaluationContext
    from openfeature.exception import ErrorCode
except ImportError:  # pragma: no cover - the stdlib run above still covers the provider
    of_api = None


@unittest.skipIf(of_api is None, "openfeature-sdk not installed")
class RealClientTest(unittest.TestCase):
    """Through the real OpenFeature client (`pip install openfeature-sdk`): errors become defaults with codes."""

    def test_through_the_openfeature_client(self) -> None:
        flags = FakeFlags({"on": True, "multi": "blue", "limit": True}, {"limit": 5})
        of_api.set_provider(mod.LucidaProvider(flags))
        c = of_api.get_client()
        ctx = EvaluationContext("user-1", {"plan": "pro"})
        self.assertIs(c.get_boolean_value("on", False, ctx), True)
        self.assertEqual(flags.calls[-1], ("on", "user-1", {"plan": "pro"}))
        d = c.get_string_details("multi", "x", ctx)
        self.assertEqual((d.value, d.variant), ("blue", "blue"))
        self.assertEqual(c.get_integer_value("limit", 0, ctx), 5)
        missing = c.get_boolean_details("nope", False, ctx)
        self.assertEqual((missing.value, missing.error_code), (False, ErrorCode.FLAG_NOT_FOUND))
        mismatch = c.get_string_details("on", "dflt", ctx)
        self.assertEqual((mismatch.value, mismatch.error_code), ("dflt", ErrorCode.TYPE_MISMATCH))
        nokey = c.get_boolean_details("on", False, EvaluationContext(None, {}))
        self.assertEqual(nokey.error_code, ErrorCode.TARGETING_KEY_MISSING)
        of_api.clear_providers()


if __name__ == "__main__":
    unittest.main()
