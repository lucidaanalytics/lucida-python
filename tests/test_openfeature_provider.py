"""Stdlib-only tests: the provider module has no imports from the rest of the package, so it loads by path
(importing `lucida` itself needs httpx)."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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


class FakeFlags:
    def __init__(self, values: dict[str, Any], payloads: dict[str, Any] | None = None) -> None:
        self.values, self.payloads = values, payloads or {}
        self.calls: list[tuple[str, str, Any]] = []

    def get_feature_flag(self, key: str, distinct_id: str, *, person_properties: Any = None) -> Any:
        self.calls.append((key, distinct_id, person_properties))
        return self.values.get(key)

    def get_feature_flag_payload(self, key: str, distinct_id: str, *, person_properties: Any = None) -> Any:
        return self.payloads.get(key)


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
        self.assertEqual((s.value, s.variant, s.reason), ("blue", "blue", mod.Reason.TARGETING_MATCH))

    def test_targeting_key_is_distinct_id_and_attributes_are_person_properties(self) -> None:
        self.p.resolve_boolean_details("on", False, Ctx("user-9", {"plan": "pro"}))
        self.assertEqual(self.flags.calls[-1], ("on", "user-9", {"plan": "pro"}))

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
