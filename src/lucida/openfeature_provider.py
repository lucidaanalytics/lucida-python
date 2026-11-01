"""OpenFeature server provider over the Lucida Python SDK (LC-DX-22).

    from openfeature import api
    from lucida import Lucida
    from lucida.openfeature_provider import LucidaProvider

    api.set_provider(LucidaProvider(Lucida("lc_pk_...", secret_key="lc_sk_...")))
    api.get_client().get_boolean_value("new-checkout", False, EvaluationContext("user-1", {"plan": "pro"}))

The targeting key is the Lucida distinct id; every other context attribute is a person property.
Booleans and strings come from the flag value (a multivariate flag is "on" for any variant, its variant key
is the string value); integers, floats and objects come from the flag payload.

It uses `openfeature-sdk` (Apache-2.0, `pip install openfeature-sdk`) when installed and otherwise falls back to
tiny stand-ins with the same names, so this module and its tests need nothing beyond the standard library.
It wraps the synchronous client; the object only needs `get_feature_flag` and `get_feature_flag_payload`.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Protocol

try:  # pragma: no cover - which branch runs depends on the environment
    from openfeature.exception import FlagNotFoundError, TargetingKeyMissingError, TypeMismatchError
    from openfeature.flag_evaluation import FlagResolutionDetails, Reason
    from openfeature.provider import AbstractProvider, Metadata
except ImportError:  # pragma: no cover
    from enum import Enum

    class Reason(str, Enum):  # type: ignore[no-redef]
        TARGETING_MATCH = "TARGETING_MATCH"
        ERROR = "ERROR"

    @dataclass
    class FlagResolutionDetails:  # type: ignore[no-redef]
        value: Any
        error_code: Any = None
        error_message: str | None = None
        reason: Any = None
        variant: str | None = None
        flag_metadata: dict[str, Any] = field(default_factory=dict)

    @dataclass
    class Metadata:  # type: ignore[no-redef]
        name: str

    class AbstractProvider:  # type: ignore[no-redef]
        pass

    class _OpenFeatureError(Exception):
        code = "GENERAL"

        def __init__(self, error_message: str | None = None) -> None:
            super().__init__(error_message)
            self.error_message = error_message
            self.error_code = self.code

    class FlagNotFoundError(_OpenFeatureError):  # type: ignore[no-redef]
        code = "FLAG_NOT_FOUND"

    class TypeMismatchError(_OpenFeatureError):  # type: ignore[no-redef]
        code = "TYPE_MISMATCH"

    class TargetingKeyMissingError(_OpenFeatureError):  # type: ignore[no-redef]
        code = "TARGETING_KEY_MISSING"


class Flags(Protocol):
    """The slice of `lucida.Lucida` the provider needs."""

    def get_feature_flag(self, key: str, distinct_id: str, *, person_properties: Any = None) -> Any: ...

    def get_feature_flag_payload(self, key: str, distinct_id: str, *, person_properties: Any = None) -> Any: ...


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)  # bool is an int in Python


class LucidaProvider(AbstractProvider):  # type: ignore[misc]
    def __init__(self, flags: Flags) -> None:
        super().__init__()
        self._flags = flags

    def get_metadata(self) -> Any:
        return Metadata(name="lucida")

    def get_provider_hooks(self) -> list[Any]:
        return []

    def _resolve(self, key: str, ctx: Any) -> tuple[Any, str, dict[str, Any]]:
        distinct_id = getattr(ctx, "targeting_key", None)
        if not distinct_id:
            raise TargetingKeyMissingError("targeting_key (distinct id) is required")
        props = dict(getattr(ctx, "attributes", None) or {})
        value = self._flags.get_feature_flag(key, distinct_id, person_properties=props)
        if value is None:  # unknown, or undecidable locally
            raise FlagNotFoundError(f"flag {key} not found")
        return value, distinct_id, props

    def _details(self, value: Any, variant: str | None = None) -> Any:
        return FlagResolutionDetails(value=value, reason=Reason.TARGETING_MATCH, variant=variant)

    def _payload(self, key: str, ctx: Any) -> Any:
        _, distinct_id, props = self._resolve(key, ctx)
        return self._flags.get_feature_flag_payload(key, distinct_id, person_properties=props)

    def resolve_boolean_details(self, flag_key: str, default_value: bool, evaluation_context: Any = None) -> Any:
        value, _, _ = self._resolve(flag_key, evaluation_context)
        if isinstance(value, bool):
            return self._details(value)
        if isinstance(value, str):
            return self._details(True, value)
        raise TypeMismatchError(f"flag {flag_key} is not a boolean")

    def resolve_string_details(self, flag_key: str, default_value: str, evaluation_context: Any = None) -> Any:
        value, _, _ = self._resolve(flag_key, evaluation_context)
        if not isinstance(value, str):
            raise TypeMismatchError(f"flag {flag_key} is not a string variant")
        return self._details(value, value)

    def resolve_integer_details(self, flag_key: str, default_value: int, evaluation_context: Any = None) -> Any:
        p = self._payload(flag_key, evaluation_context)
        if not _is_number(p) or (isinstance(p, float) and (not math.isfinite(p) or p != math.trunc(p))):
            raise TypeMismatchError(f"flag {flag_key} has no integer payload")
        return self._details(int(p))

    def resolve_float_details(self, flag_key: str, default_value: float, evaluation_context: Any = None) -> Any:
        p = self._payload(flag_key, evaluation_context)
        if not _is_number(p):
            raise TypeMismatchError(f"flag {flag_key} has no numeric payload")
        return self._details(float(p))

    def resolve_object_details(self, flag_key: str, default_value: Any, evaluation_context: Any = None) -> Any:
        p = self._payload(flag_key, evaluation_context)
        if not isinstance(p, (dict, list)):
            raise TypeMismatchError(f"flag {flag_key} has no object payload")
        return self._details(p)
