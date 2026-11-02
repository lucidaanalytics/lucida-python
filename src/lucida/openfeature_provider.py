"""OpenFeature server provider over the Lucida Python SDK (LC-DX-22).

    from openfeature import api
    from lucida import Lucida
    from lucida.openfeature_provider import LucidaProvider

    api.set_provider(LucidaProvider(Lucida("lc_pk_...", secret_key="lc_sk_...")))
    api.get_client().get_boolean_value("new-checkout", False, EvaluationContext("user-1", {"plan": "pro"}))

The targeting key is the Lucida distinct id; the `groups` attribute (group type -> key) and `groupProperties`
(group type -> properties) feed Lucida groups and every other attribute is a person property.
Booleans and strings come from the flag value (a multivariate flag is "on" for any variant, its variant key
is the string value); integers, floats and objects come from the flag payload.
The raw Lucida reason maps through testdata/openfeature/reasons.json (kept as `REASONS` below) and rides in
flag_metadata (`lucidaReason`, `conditionIndex`, `requestId`, `evaluatedLocally`). A client `fallbacks` value
resolves with reason DEFAULT, no error code and `lucidaReason` "fallback". `track` maps to `capture`.
Provider events: READY after the first load, CONFIGURATION_CHANGED on a reload, STALE when a reload fails or the
stream drops, ERROR when the first load fails (needs a client with `on_flags_change` / `on_flags_stale`).

It uses `openfeature-sdk` (Apache-2.0, `pip install openfeature-sdk`, 0.8 API) when installed and otherwise falls
back to tiny stand-ins with the same names, so this module and its tests need nothing beyond the standard library.
It wraps the synchronous client; the object needs `get_feature_flag_details` and `capture`.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Protocol

try:  # pragma: no cover - which branch runs depends on the environment
    from openfeature.event import ProviderEventDetails
    from openfeature.exception import FlagNotFoundError, GeneralError, TargetingKeyMissingError, TypeMismatchError
    from openfeature.flag_evaluation import FlagResolutionDetails, Reason
    from openfeature.provider import AbstractProvider, Metadata
except ImportError:  # pragma: no cover
    from enum import Enum

    class Reason(str, Enum):  # type: ignore[no-redef]
        TARGETING_MATCH = "TARGETING_MATCH"
        SPLIT = "SPLIT"
        CACHED = "CACHED"
        DEFAULT = "DEFAULT"
        DISABLED = "DISABLED"
        STATIC = "STATIC"
        ERROR = "ERROR"
        UNKNOWN = "UNKNOWN"

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

    @dataclass
    class ProviderEventDetails:  # type: ignore[no-redef]
        message: str | None = None
        flags_changed: list[str] | None = None

    class AbstractProvider:  # type: ignore[no-redef]
        def __init__(self) -> None:
            self.emitted: list[tuple[str, Any]] = []

        def emit_provider_ready(self, details: Any) -> None:
            self.emitted.append(("PROVIDER_READY", details))

        def emit_provider_configuration_changed(self, details: Any) -> None:
            self.emitted.append(("PROVIDER_CONFIGURATION_CHANGED", details))

        def emit_provider_stale(self, details: Any) -> None:
            self.emitted.append(("PROVIDER_STALE", details))

        def emit_provider_error(self, details: Any) -> None:
            self.emitted.append(("PROVIDER_ERROR", details))

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

    class GeneralError(_OpenFeatureError):  # type: ignore[no-redef]
        code = "GENERAL"


# Lucida reason -> OpenFeature reason name; mirrors testdata/openfeature/reasons.json (a test compares them).
REASONS: dict[str, str] = {
    "condition_match": "TARGETING_MATCH", "sticky_bucket": "CACHED", "no_condition_match": "DEFAULT",
    "out_of_rollout": "DEFAULT", "holdout": "DEFAULT", "namespace_excluded": "DEFAULT", "no_bucketing_key": "DEFAULT",
    "prerequisite_failed": "DEFAULT", "prerequisite_missing": "DEFAULT", "prerequisite_cycle": "ERROR",
    "inactive": "DISABLED", "kill_switch": "DISABLED", "app_context_excluded": "DISABLED", "preview": "STATIC",
    "user_preview": "STATIC", "fallback": "DEFAULT", "": "UNKNOWN",
}  # fmt: skip


def map_reason(raw: str, value: Any) -> Any:
    """OpenFeature reason for a raw Lucida reason; a matched multivariate (string) value is a SPLIT."""
    if raw == "condition_match" and isinstance(value, str):
        return Reason.SPLIT
    return Reason[REASONS.get(raw or "", "UNKNOWN")]


class Flags(Protocol):
    """The slice of `lucida.Lucida` the provider needs."""

    def get_feature_flag_details(self, key: str, distinct_id: str, **kw: Any) -> Any: ...

    def capture(self, distinct_id: str | None, event: str, properties: Any = None, **kw: Any) -> None: ...


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)  # bool is an int in Python


def _split_context(ctx: Any) -> tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]:
    """(distinct id, person properties, groups, group properties) from an OpenFeature context."""
    distinct_id = getattr(ctx, "targeting_key", None)
    if not distinct_id:
        raise TargetingKeyMissingError("targeting_key (distinct id) is required")
    attrs = dict(getattr(ctx, "attributes", None) or {})
    groups, gprops = attrs.pop("groups", None), attrs.pop("groupProperties", None)
    return distinct_id, attrs, dict(groups) if isinstance(groups, dict) else {}, dict(gprops) if isinstance(gprops, dict) else {}


class LucidaProvider(AbstractProvider):  # type: ignore[misc]
    def __init__(self, flags: Flags, ready_timeout: float = 10.0) -> None:
        super().__init__()
        self._flags = flags
        self._ready_timeout = ready_timeout
        self._state = "init"  # init -> ready | error
        if hasattr(flags, "on_flags_change"):
            flags.on_flags_change(self._changed)
            flags.on_flags_stale(self._stale)

    def get_metadata(self) -> Any:
        return Metadata(name="lucida")

    def get_provider_hooks(self) -> list[Any]:
        return []

    def initialize(self, evaluation_context: Any = None) -> None:
        """Wait for the first definitions load; raising makes OpenFeature report PROVIDER_ERROR."""
        ev = getattr(self._flags, "ready", None)
        if ev is not None:
            ev.wait(self._ready_timeout)
        if getattr(self._flags, "local_evaluation", False) and not self._flags.definitions_loaded:
            self._state = "error"
            raise GeneralError("first flag definitions load failed")
        self._state = "ready"

    def _changed(self) -> None:
        was, self._state = self._state, "ready"
        if was == "error":
            self.emit_provider_ready(ProviderEventDetails(message="flag definitions loaded"))
        elif was == "ready":
            self.emit_provider_configuration_changed(ProviderEventDetails(message="flag definitions changed"))

    def _stale(self) -> None:
        if self._state == "ready":
            self.emit_provider_stale(ProviderEventDetails(message="flag definitions reload failed"))

    def track(self, tracking_event_name: str, evaluation_context: Any = None, tracking_event_details: Any = None) -> None:
        """OpenFeature tracking: a Lucida event named `tracking_event_name` for the targeting key; the details'
        attributes become properties and its numeric value is `$value`."""
        distinct_id, _, groups, _ = _split_context(evaluation_context)
        props = dict(getattr(tracking_event_details, "attributes", None) or {})
        value = getattr(tracking_event_details, "value", None)
        if value is not None:
            props["$value"] = value
        self._flags.capture(distinct_id, tracking_event_name, props, groups=groups or None)

    def _resolve(self, key: str, ctx: Any) -> Any:
        distinct_id, props, groups, gprops = _split_context(ctx)
        d = self._flags.get_feature_flag_details(
            key, distinct_id, person_properties=props, groups=groups or None, group_properties=gprops or None
        )
        if d is None:  # unknown, or undecidable locally, and no fallback
            raise FlagNotFoundError(f"flag {key} not found")
        return d

    @staticmethod
    def _details(d: Any, value: Any, variant: str | None = None) -> Any:
        meta: dict[str, Any] = {"lucidaReason": d.reason, "evaluatedLocally": bool(d.evaluated_locally)}
        if d.condition_index is not None and d.condition_index >= 0:
            meta["conditionIndex"] = d.condition_index
        if d.request_id:
            meta["requestId"] = d.request_id
        return FlagResolutionDetails(
            value=value, reason=map_reason(d.reason, d.value), variant=variant or d.variant, flag_metadata=meta
        )

    def _payload(self, key: str, ctx: Any) -> tuple[Any, Any]:
        d = self._resolve(key, ctx)
        return d, (d.value if d.reason == "fallback" else d.payload)  # a fallback is the value itself

    def resolve_boolean_details(self, flag_key: str, default_value: bool, evaluation_context: Any = None) -> Any:
        d = self._resolve(flag_key, evaluation_context)
        if isinstance(d.value, bool):
            return self._details(d, d.value)
        if isinstance(d.value, str):
            return self._details(d, True, d.value)
        raise TypeMismatchError(f"flag {flag_key} is not a boolean")

    def resolve_string_details(self, flag_key: str, default_value: str, evaluation_context: Any = None) -> Any:
        d = self._resolve(flag_key, evaluation_context)
        if not isinstance(d.value, str):
            raise TypeMismatchError(f"flag {flag_key} is not a string variant")
        return self._details(d, d.value, d.value)

    def resolve_integer_details(self, flag_key: str, default_value: int, evaluation_context: Any = None) -> Any:
        d, p = self._payload(flag_key, evaluation_context)
        if not _is_number(p) or (isinstance(p, float) and (not math.isfinite(p) or p != math.trunc(p))):
            raise TypeMismatchError(f"flag {flag_key} has no integer payload")
        return self._details(d, int(p))

    def resolve_float_details(self, flag_key: str, default_value: float, evaluation_context: Any = None) -> Any:
        d, p = self._payload(flag_key, evaluation_context)
        if not _is_number(p):
            raise TypeMismatchError(f"flag {flag_key} has no numeric payload")
        return self._details(d, float(p))

    def resolve_object_details(self, flag_key: str, default_value: Any, evaluation_context: Any = None) -> Any:
        d, p = self._payload(flag_key, evaluation_context)
        if not isinstance(p, (dict, list)):
            raise TypeMismatchError(f"flag {flag_key} has no object payload")
        return self._details(d, p)
