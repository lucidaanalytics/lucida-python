"""Local flag evaluator; behaviour pinned by testdata/flags vectors (see testdata/flags/README.md)."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ._ops_v2 import match_v2, parse_now

Flag = dict[str, Any]
Context = dict[str, Any]

REQUIRES_SERVER = "requires_server"
_UNIT = 4503599627370496  # 16**13


@dataclass(frozen=True)
class FlagResult:
    value: bool | str
    reason: str
    condition_index: int = -1
    variant: str | None = None
    payload: Any = None


def hash01(text: str) -> float:
    return int(hashlib.sha256(text.encode()).hexdigest()[:13], 16) / _UNIT


def _off(reason: str, idx: int = -1) -> FlagResult:
    return FlagResult(False, reason, idx)


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _to_float(v: Any) -> float | None:
    if _is_num(v):
        return float(v)
    if isinstance(v, str) and "_" not in v:
        try:
            return float(v.strip())
        except ValueError:
            return None
    return None


def _equal(a: Any, b: Any) -> bool:
    if _is_num(a) or _is_num(b):
        return _is_num(a) and _is_num(b) and float(a) == float(b)
    return bool(a == b)


def _semver(v: Any) -> list[int] | None:
    if not isinstance(v, str):
        return None
    s = v.strip().removeprefix("v")
    m = re.search(r"[-+]", s)
    if m:
        s = s[: m.start()]
    parts = s.split(".")
    if not all(p.isascii() and p.isdigit() for p in parts):
        return None
    return [int(p) for p in parts]


def _cmp_semver(a: Any, b: Any) -> int | None:
    pa, pb = _semver(a), _semver(b)
    if pa is None or pb is None:
        return None
    n = max(len(pa), len(pb))
    pa, pb = pa + [0] * (n - len(pa)), pb + [0] * (n - len(pb))
    return (pa > pb) - (pa < pb)


def _regex(pattern: str, s: str) -> bool | None:
    try:
        return re.search(pattern, s) is not None
    except re.error:
        return None


def _match(op: str, prop: Any, present: bool, want: Any, semver: bool, now: datetime | None = None) -> bool:  # noqa: PLR0911, PLR0912
    is_set = present and prop is not None
    if op == "is_set":
        return is_set
    if op == "is_not_set":
        return not is_set
    if not is_set:
        return False
    if op in ("eq", "neq"):
        if semver:
            c = _cmp_semver(prop, want)
            return c is not None and (c == 0) == (op == "eq")
        return _equal(prop, want) == (op == "eq")
    if op in ("contains", "ncontains", "regex", "nregex"):
        if not isinstance(prop, str) or not isinstance(want, str):
            return False
        if op in ("contains", "ncontains"):
            return (want in prop) == (op == "contains")
        r = _regex(want, prop)
        return r is not None and r == (op == "regex")
    if op in ("gt", "gte", "lt", "lte"):
        if semver:
            c = _cmp_semver(prop, want)
        else:
            x, y = _to_float(prop), _to_float(want)
            c = None if x is None or y is None else (x > y) - (x < y)
        if c is None:
            return False
        return {"gt": c > 0, "gte": c >= 0, "lt": c < 0, "lte": c <= 0}[op]
    if op in ("in", "not_in"):
        if not isinstance(want, list):
            return False
        return any(_equal(prop, w) for w in want) == (op == "in")
    return match_v2(op, prop, want, now or parse_now(None), _to_float, _semver, _cmp_semver)


def _filter_matches(fl: dict[str, Any], c: Context, resolved: dict[str, FlagResult]) -> bool:
    t = fl.get("type")
    want = fl.get("value")
    now = parse_now(c.get("now"))
    if t == "app_version":
        v = c.get("app_version") or ""
        return _match(fl.get("op", ""), v, v != "", want, True, now)
    if t == "flag":
        r = resolved.get(fl.get("key", ""))
        return r is not None and _match(fl.get("op", ""), r.value, True, want, False, now)
    if t == "person":
        props = c.get("person_properties")
    elif t == "group":
        props = (c.get("group_properties") or {}).get(fl.get("group_type", ""))
    elif t == "device":
        props = c.get("device_properties")
    elif t == "geo":
        props = c.get("geo_properties")
    else:
        return False
    props = props or {}
    key = fl.get("key", "")
    return _match(fl.get("op", ""), props.get(key), key in props, want, False, now)


def _value_matches(got: Any, want: Any) -> bool:
    if isinstance(want, bool):
        return got == want if isinstance(got, bool) else want
    return bool(got == want)


def _bucketing_key(f: Flag, c: Context) -> str:
    return _unit_key(f.get("aggregation") or {}, c)


def _unit_key(agg: dict[str, Any], c: Context) -> str:
    if agg.get("kind") == "group":
        return str((c.get("groups") or {}).get(agg.get("group_type", ""), ""))
    if agg.get("kind") == "device":
        return str(c.get("device_id") or "")
    return str(c.get("bucketing_key") or c.get("distinct_id") or "")


def _pick_variant(f: Flag, cond: dict[str, Any], key: str) -> dict[str, Any]:
    variants: list[dict[str, Any]] = f["variants"]
    override = cond.get("variant_override")
    if override:
        for v in variants:
            if v["key"] == override:
                return v
    h = hash01(f"{f['key']}.{f.get('salt', '')}.variant.{key}") * 100
    cum = 0.0
    for v in variants:
        cum += v.get("rollout_pct", 0)
        if h < cum:
            return v
    return variants[-1]  # percentages summed < 100: last variant takes the remainder


def _sticky(f: Flag, cond: dict[str, Any], i: int, c: Context) -> FlagResult | None:
    """A stored value of a sticky flag decides once a condition's filters passed (LC-EXT-29)."""
    stored = (c.get("sticky_assignments") or {}).get(f["key"]) if f.get("sticky") else None
    if stored is None:
        return None
    variants: list[dict[str, Any]] = f.get("variants") or []
    if f.get("type") == "multivariate" and variants:
        if any(v["key"] == cond.get("variant_override") for v in variants):
            return None  # the override wins
        for v in variants:
            if isinstance(stored, str) and v["key"] == stored:
                return FlagResult(v["key"], "sticky_bucket", i, v["key"], v.get("payload"))
        return None
    if stored is not True:
        return None
    return FlagResult(True, "sticky_bucket", i, None, f.get("payload") if f.get("type") == "json" else None)


def _app_context_allowed(f: Flag, c: Context) -> bool:
    want, have = f.get("app_contexts") or [], c.get("app_contexts") or []
    return not want or not have or any(x in have for x in want)


def evaluate(f: Flag, c: Context, resolved: dict[str, FlagResult]) -> FlagResult:  # noqa: PLR0911, PLR0912
    if not _app_context_allowed(f, c):
        return _off("app_context_excluded")
    if not f.get("active"):
        return _off("inactive")
    if f.get("kill_switch"):
        return _off("kill_switch")
    server = False
    for p in f.get("prerequisites") or []:
        r = resolved.get(p["flag"])
        if r is None:
            return _off("prerequisite_missing")
        if r.reason == REQUIRES_SERVER:
            server = True  # a definite failure elsewhere still wins
        elif r.reason == "prerequisite_cycle" or not _value_matches(r.value, p.get("value")):
            return _off("prerequisite_failed")
    if server:
        return _off(REQUIRES_SERVER)
    key = _bucketing_key(f, c)
    conds = f.get("conditions") or []
    per_condition = any(cond.get("aggregation") for cond in conds)
    if not key and (not per_condition or f.get("holdout") or f.get("namespace")):
        return _off("no_bucketing_key")
    ho = f.get("holdout")
    if ho and hash01(f"holdout.{ho['salt']}.{key}") * 100 < ho["percentage"]:
        return _off("holdout")
    ns = f.get("namespace")
    if ns and ns["total"] > 0:
        b = int(hash01(f"namespace.{ns['salt']}.{key}") * ns["total"])
        if b < ns["start"] or b >= ns["end"]:
            return _off("namespace_excluded")
    skipped = 0
    for i, cond in enumerate(conds):
        ckey = _unit_key(cond["aggregation"], c) if cond.get("aggregation") else key
        if not ckey:
            skipped += 1
            continue
        filters = cond.get("filters") or []

        def undecidable(fl: dict[str, Any]) -> bool:  # cohort, or a flag filter on a requires_server flag
            if fl.get("type") == "flag":
                r = resolved.get(fl.get("key", ""))
                return r is not None and r.reason == REQUIRES_SERVER
            return fl.get("type") == "cohort"

        if not all(_filter_matches(fl, c, resolved) for fl in filters if not undecidable(fl)):
            continue
        if any(undecidable(fl) for fl in filters):
            return _off(REQUIRES_SERVER, i)
        sticky = None if cond.get("aggregation") else _sticky(f, cond, i, c)  # the store keys on the flag-level unit
        if sticky:
            return sticky
        if hash01(f"{f['key']}.{f.get('salt', '')}.{ckey}") * 100 >= cond.get("rollout_pct", 0):
            if f.get("early_exit"):
                return _off("out_of_rollout", i)
            continue
        if f.get("type") == "multivariate" and f.get("variants"):
            v = _pick_variant(f, cond, ckey)
            return FlagResult(v["key"], "condition_match", i, v["key"], v.get("payload"))
        payload = f.get("payload") if f.get("type") == "json" else None
        return FlagResult(True, "condition_match", i, None, payload)
    return _off("no_bucketing_key" if conds and skipped == len(conds) else "no_condition_match")


def _deps(f: Flag) -> list[str]:
    d = [p["flag"] for p in f.get("prerequisites") or []]
    for cond in f.get("conditions") or []:
        d += [fl["key"] for fl in cond.get("filters") or [] if fl.get("type") == "flag"]
    return d


def evaluate_all(flags: list[Flag], c: Context) -> dict[str, FlagResult]:
    """Evaluate every flag, resolving prerequisites and `flag` filters depth-first."""
    by_key = {f["key"]: f for f in flags}
    out: dict[str, FlagResult] = {}
    state: dict[str, int] = {}  # 1 = visiting, 2 = done

    def visit(key: str) -> None:
        f = by_key.get(key)
        if f is None or key in state:
            return
        state[key] = 1
        cyc = False
        for dep in _deps(f):
            visit(dep)
            if state.get(dep) == 1 or (dep in out and out[dep].reason == "prerequisite_cycle"):
                cyc = True
        out[key] = _off("prerequisite_cycle") if cyc else evaluate(f, c, out)
        state[key] = 2

    for f in flags:
        visit(f["key"])
    return out
