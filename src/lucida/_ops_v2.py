"""Spec v2 operators (testdata/flags/README.md, "Spec v2"); port of go/internal/flageval/ops_v2.go."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

_REL = re.compile(r"^-?(\d{1,6})([hdwmy])$")
_UNIT = {"h": 3600, "d": 86400, "w": 7 * 86400, "m": 30 * 86400, "y": 365 * 86400}
_RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

V2_OPS = frozenset(
    "icontains not_icontains starts_with not_starts_with ends_with not_ends_with between not_between "
    "is_date_before is_date_after semver_eq semver_neq semver_gt semver_gte semver_lt semver_lte "
    "semver_tilde semver_caret semver_wildcard".split()
)


def _num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def relative_seconds(s: str) -> int | None:
    m = _REL.match(s.strip())
    return int(m.group(1)) * _UNIT[m.group(2)] if m else None


def parse_now(s: Any) -> datetime:
    if isinstance(s, str) and _RFC3339.match(s):
        return _iso(s)
    return datetime.now(timezone.utc)


def _iso(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    if "." in s:  # Python < 3.11 accepts at most 6 fraction digits
        head, rest = s.split(".", 1)
        digits = re.match(r"\d+", rest)
        frac = digits.group(0) if digits else ""
        s = f"{head}.{(frac + '000000')[:6]}{rest[len(frac):]}"
    return datetime.fromisoformat(s)


def _instant(v: Any, now: datetime, rel: bool) -> datetime | None:
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return datetime.fromtimestamp(v, timezone.utc)
    if not isinstance(v, str):
        return None
    s = v.strip()
    if rel:
        secs = relative_seconds(s)
        if secs is not None:
            return now - timedelta(seconds=secs)
    try:
        if _RFC3339.match(s):
            return _iso(s)
        if _DATE.match(s):
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return None


def semver_range(op: str, want: Any, parse: Callable[[Any], list[int] | None]) -> tuple[list[int], list[int]] | None:
    if not isinstance(want, str):
        return None
    parts = want.strip().removeprefix("v").split(".")
    if op == "semver_wildcard":
        if parts[-1] not in ("*", "x", "X"):
            return None
        if len(parts) == 1:
            return [0], [1 << 30]
        lo = parse(".".join(parts[:-1]))
        if lo is None:
            return None
        return lo, lo[:-1] + [lo[-1] + 1]
    p = parse(want)
    if p is None:
        return None
    p = p + [0] * (3 - len(p))
    a, b, c = p[0], p[1], p[2]
    if op == "semver_tilde" and len(parts) == 1:
        hi = [a + 1, 0, 0]
    elif op == "semver_tilde" or (a == 0 and b > 0):
        hi = [a, b + 1, 0]
    elif a > 0:
        hi = [a + 1, 0, 0]
    else:
        hi = [0, 0, c + 1]
    return p, hi


def _cmp_parts(a: list[int], b: list[int]) -> int:
    n = max(len(a), len(b))
    a, b = a + [0] * (n - len(a)), b + [0] * (n - len(b))
    return (a > b) - (a < b)


def match_v2(  # noqa: PLR0911
    op: str,
    prop: Any,
    want: Any,
    now: datetime,
    to_float: Callable[[Any], float | None],
    parse: Callable[[Any], list[int] | None],
    cmp_semver: Callable[[Any, Any], int | None],
) -> bool:
    if op in ("icontains", "not_icontains", "starts_with", "not_starts_with", "ends_with", "not_ends_with"):
        if not isinstance(prop, str) or not isinstance(want, str):
            return False
        if op.endswith("icontains"):
            return (want.lower() in prop.lower()) == (op == "icontains")
        if op.endswith("starts_with"):
            return prop.startswith(want) == (op == "starts_with")
        return prop.endswith(want) == (op == "ends_with")
    if op in ("between", "not_between"):
        x = to_float(prop)
        if not isinstance(want, list) or len(want) != 2 or x is None or not all(_num(w) for w in want):
            return False
        return bool((want[0] <= x <= want[1]) == (op == "between"))
    if op in ("is_date_before", "is_date_after"):
        p, w = _instant(prop, now, False), _instant(want, now, True)
        if p is None or w is None:
            return False
        return p < w if op == "is_date_before" else p > w
    if op.startswith("semver_") and op not in ("semver_tilde", "semver_caret", "semver_wildcard"):
        c = cmp_semver(prop, want)
        if c is None:
            return False
        return {"eq": c == 0, "neq": c != 0, "gt": c > 0, "gte": c >= 0, "lt": c < 0, "lte": c <= 0}.get(op[7:], False)
    if op in ("semver_tilde", "semver_caret", "semver_wildcard"):
        v, r = parse(prop), semver_range(op, want, parse)
        return v is not None and r is not None and _cmp_parts(v, r[0]) >= 0 and _cmp_parts(v, r[1]) < 0
    return False
