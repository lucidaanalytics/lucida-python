"""Agent analytics (P-AGENT): conversation events for in-product AI agents and chatbots.

Product side only: no tokens, latency, cost or model names (Watchtower owns LLM observability). The only
Watchtower touchpoint is the optional `$ai_trace_id`. Helpers match packages/sdk-core/src/agent.ts and the
vectors in testdata/agent-analytics/.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from typing import Any

Props = dict[str, Any]
Emit = Callable[[str, Props], None]

ROLES = ("user", "assistant")
OUTCOMES = ("resolved", "unresolved", "escalated", "abandoned", "error")
FEEDBACK_KINDS = ("thumbs", "rating", "comment")
MAX_TEXT, MAX_COMMENT = 4000, 2000

_TOKEN = re.compile(r"[^\W_]+")
_TRACE = re.compile(r"[0-9a-f]{32}")
_PUNCT = re.compile(r"[.,!?;:]")
_WS = re.compile(r"\s+")
_FNV_OFFSET, _FNV_PRIME, _MASK = 0xCBF29CE484222325, 0x100000001B3, (1 << 64) - 1

NEG = (
    "wrong", "useless", "not helpful", "unhelpful", "doesn't work", "does not work", "didn't help",
    "did not help", "not what i asked", "that's not it", "stupid", "terrible", "awful", "frustrating",
    "frustrated", "annoying", "worst", "hate", "incorrect", "nonsense", "confusing", "makes no sense",
    "real person", "human agent", "talk to a human", "speak to a human", "wtf", "ugh",
)  # fmt: skip
POS = ("thanks", "thank you", "great", "perfect", "awesome", "helpful", "that worked", "works now", "solved", "exactly")


def _fnv1a64(token: str) -> int:
    h = _FNV_OFFSET
    for b in token.encode("utf-8"):
        h = ((h ^ b) * _FNV_PRIME) & _MASK
    return h


def text_hash(text: str) -> str | None:
    """simhash64 of the lowercased letter/digit tokens (>= 2 code points), 16 hex chars; None without tokens."""
    tokens = [t for t in _TOKEN.findall(text.lower()) if len(t) >= 2]
    if not tokens:
        return None
    v = [0] * 64
    for t in tokens:
        h = _fnv1a64(t)
        for i in range(64):
            v[i] += 1 if (h >> i) & 1 else -1
    out = sum(1 << i for i in range(64) if v[i] > 0)
    return f"{out:016x}"


def sentiment(text: str) -> int:
    """-1 / 0 / 1 from the shared phrase lexicon (testdata/agent-analytics/lexicon.json)."""
    norm = _PUNCT.sub(" ", text.lower().replace("’", "'"))
    padded = " " + _WS.sub(" ", norm).strip() + " "
    neg = 0
    for p in NEG:
        if f" {p} " in padded:
            neg += 1
            padded = padded.replace(f" {p} ", " ")
    pos = sum(1 for p in POS if f" {p} " in padded)
    return -1 if neg > pos else 1 if pos > neg else 0


def _digits(s: str) -> int:
    return sum(1 for c in s if "0" <= c <= "9")


def _luhn(s: str) -> bool:
    d = [ord(c) - 48 for c in s if "0" <= c <= "9"]
    if not 13 <= len(d) <= 19:
        return False
    total = 0
    for i, x in enumerate(reversed(d)):
        total += (x * 2 - 9 if x > 4 else x * 2) if i % 2 == 1 else x
    return total % 10 == 0


# Same detectors, order and replacement as crates/lc-replay/src/mask.rs (the worker re-applies it).
_RULES: tuple[tuple[re.Pattern[str], Callable[[str], bool]], ...] = (
    (re.compile(r"\b\d(?:[ -]?\d){12,18}\b"), _luhn),
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}"), lambda _: True),
    (re.compile(r"\+?\d[\d\s().-]{7,}\d"), lambda s: 10 <= _digits(s) <= 15),
)


def stars(s: str) -> str:
    return "".join(c if c.isspace() else "*" for c in s)


def redact(text: str) -> str | None:
    """Masked copy (cards, emails, phones -> `*`, whitespace kept); None when nothing matched."""
    cur = text
    for rx, ok in _RULES:

        def mask(m: re.Match[str], ok: Callable[[str], bool] = ok) -> str:
            return stars(m[0]) if ok(m[0]) else m[0]

        cur = rx.sub(mask, cur)
    return None if cur == text else cur


def trace_id(v: object) -> str | None:
    """A W3C trace id (32 lowercase hex) or None."""
    if not isinstance(v, str):
        return None
    t = v.lower()
    return t if _TRACE.fullmatch(t) else None


def outcome(v: object) -> str | None:
    return v if isinstance(v, str) and v in OUTCOMES else None


def _label(v: object, n: int) -> str | None:
    return v[:n] if isinstance(v, str) and v else None


def _text_props(key: str, text: str, cap: int) -> Props:
    masked = redact(text[:cap])
    return {key: masked if masked is not None else text[:cap], **({"$ai_text_redacted": True} if masked is not None else {})}


def message_props(role: str, text: str, index: int, capture_text: bool) -> Props | None:
    """`$ai_message` props, None for a role other than user/assistant."""
    if role not in ROLES:
        return None
    p: Props = {"$ai_role": role, "$ai_message_index": index, "$ai_text_length": len(text)}
    if role == "user":
        h = text_hash(text)
        if h is not None:
            p["$ai_text_hash"] = h
        p["$ai_sentiment"] = sentiment(text)
    if capture_text:
        p.update(_text_props("$ai_text", text, MAX_TEXT))
    return p


def feedback_props(
    kind: str, value: float | None, comment: str | None, message_index: int | None, capture_text: bool
) -> Props | None:
    """`$ai_feedback` props, None when kind/value is invalid (thumbs 1|-1, rating integer 1..5)."""
    if kind == "thumbs" and value not in (1, -1):
        return None
    if kind == "rating" and (isinstance(value, bool) or not isinstance(value, (int, float)) or not 1 <= value <= 5 or value != int(value)):
        return None
    if kind not in FEEDBACK_KINDS:
        return None
    p: Props = {"$ai_feedback_kind": kind}
    if kind != "comment":
        p["$ai_feedback_value"] = int(value)  # type: ignore[arg-type]
    if comment and capture_text:
        masked = redact(comment[:MAX_COMMENT])
        p["$ai_feedback_comment"] = masked if masked is not None else comment[:MAX_COMMENT]
    if message_index is not None:
        p["$ai_message_index"] = message_index
    return p


def outcome_props(o: str | None, resolution: str | None) -> Props:
    p: Props = {}
    if (v := outcome(o)) is not None:
        p["$ai_outcome"] = v
    if (r := _label(resolution, 64)) is not None:
        p["$ai_resolution"] = r
    return p


def common_props(
    conversation_id: str | None = None,
    agent_name: str | None = None,
    agent_version: str | None = None,
    trace: str | None = None,
    topic: str | None = None,
    use_case: str | None = None,
) -> Props:
    p: Props = {"$ai_conversation_id": conversation_id or str(uuid.uuid4())}
    for key, v, n in (("$ai_agent_name", agent_name, 128), ("$ai_agent_version", agent_version, 64),
                      ("$ai_topic", topic, 128), ("$ai_use_case", use_case, 128)):  # fmt: skip
        if (s := _label(v, n)) is not None:
            p[key] = s
    if (t := trace_id(trace)) is not None:
        p["$ai_trace_id"] = t
    return p


class AgentConversation:
    """One agent conversation; every event goes through the client's normal `capture`."""

    def __init__(self, emit: Emit, common: Props, capture_text: bool) -> None:
        self._emit, self._common, self._capture_text = emit, common, capture_text
        self._next = 0

    @property
    def id(self) -> str:
        return str(self._common["$ai_conversation_id"])

    def _send(self, event: str, props: Props) -> None:
        self._emit(event, {**self._common, **props})

    def start(self) -> AgentConversation:
        self._send("$ai_conversation_started", {})
        return self

    def message(self, role: str, text: str, index: int | None = None) -> bool:
        """`$ai_message`; the index auto-increments per conversation unless given. False (no event) for a bad role."""
        i = self._next if index is None else index
        p = message_props(role, text, i, self._capture_text)
        if p is None:
            return False
        self._next = i + 1
        self._send("$ai_message", p)
        return True

    def feedback(
        self, kind: str, value: float | None = None, comment: str | None = None, message_index: int | None = None
    ) -> bool:
        p = feedback_props(kind, value, comment, message_index, self._capture_text)
        if p is None:
            return False
        self._send("$ai_feedback", p)
        return True

    def end(self, outcome: str | None = None, resolution: str | None = None) -> None:
        self._send("$ai_conversation_ended", outcome_props(outcome, resolution))

    def outcome(self, outcome: str, resolution: str | None = None) -> bool:
        """A resolution known later (e.g. the support ticket closed). False (no event) for an unknown outcome."""
        p = outcome_props(outcome, resolution)
        if "$ai_outcome" not in p:
            return False
        self._send("$ai_outcome", p)
        return True


def start_conversation(
    capture: Callable[..., None],
    distinct_id: str | None,
    capture_text: bool,
    *,
    conversation_id: str | None = None,
    agent_name: str | None = None,
    agent_version: str | None = None,
    trace_id: str | None = None,
    topic: str | None = None,
    use_case: str | None = None,
) -> AgentConversation:
    """Shared by the sync and asyncio clients: `capture(distinct_id, event, props)` is the client's normal capture."""
    common = common_props(conversation_id, agent_name, agent_version, trace_id, topic, use_case)
    return AgentConversation(lambda event, props: capture(distinct_id, event, props), common, capture_text).start()
