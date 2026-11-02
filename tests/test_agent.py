"""Agent analytics: shared vectors (testdata/agent-analytics) and the client API."""

from __future__ import annotations

from typing import Any

from conftest import load

from lucida import Lucida
from lucida import _agent as a

V: dict[str, Any] = load("agent-analytics/vectors.json")


def test_lexicon_matches_shared_file() -> None:
    lex = load("agent-analytics/lexicon.json")
    assert list(a.NEG) == lex["neg"] and list(a.POS) == lex["pos"]


def test_simhash_vectors() -> None:
    for v in V["simhash"]:
        assert a.text_hash(v["input"]) == v["hash"], v["input"]
    for h in V["hamming"]:
        x, y = a.text_hash(h["a"]), a.text_hash(h["b"])
        assert x is not None and y is not None
        assert bin(int(x, 16) ^ int(y, 16)).count("1") == h["distance"]


def test_sentiment_redaction_validation_vectors() -> None:
    for v in V["sentiment"]:
        assert a.sentiment(v["input"]) == v["sentiment"], v["input"]
    for v in V["redaction"]:
        assert a.redact(v["input"]) == v["output"], v["input"]
    for v in V["trace_id"]:
        assert a.trace_id(v["input"]) == v["output"], v["input"]
    for v in V["outcome"]:
        assert a.outcome(v["input"]) == v["output"], v["input"]
    for f in V["feedback"]:
        assert (a.feedback_props(f["kind"], f["value"], None, None, False) is not None) == f["valid"], f
    for m in V["message"]:
        assert a.message_props(m["role"], m["text"], 0, m["capture_text"]) == m["props"], m["name"]


def ai_events(lc: Lucida) -> list[dict[str, Any]]:
    assert lc.sink is not None
    return [e for e in lc.sink.events if e["event"].startswith("$ai_")]


def test_conversation_events_text_off_by_default() -> None:
    lc = Lucida("lc_pk_x", test_mode=[])
    c = lc.ai_conversation("u1", agent_name="support", trace_id="4BF92F3577B34DA6A3CE929D0E0E4736")
    assert c.message("user", "my card 4242 4242 4242 4242 is wrong")
    assert c.message("assistant", "Sorry")
    assert not c.message("system", "x")
    assert c.message("user", "again", index=7)
    assert c.feedback("thumbs", -1, comment="bad", message_index=1)
    assert not c.feedback("rating", 9)
    c.end("escalated", "handed to support")
    assert not c.outcome("bogus")
    assert c.outcome("resolved")
    evs = ai_events(lc)
    assert [e["event"] for e in evs] == [
        "$ai_conversation_started", "$ai_message", "$ai_message", "$ai_message", "$ai_feedback",
        "$ai_conversation_ended", "$ai_outcome",
    ]  # fmt: skip
    for e in evs:
        p = e["properties"]
        assert e["distinct_id"] == "u1"
        assert p["$ai_conversation_id"] == c.id and p["$ai_agent_name"] == "support"
        assert p["$ai_trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert "$ai_text" not in p and "$ai_feedback_comment" not in p
    assert [e["properties"]["$ai_message_index"] for e in evs if e["event"] == "$ai_message"] == [0, 1, 7]
    assert evs[1]["properties"]["$ai_sentiment"] == -1 and evs[1]["properties"]["$ai_text_length"] == 36
    assert evs[5]["properties"]["$ai_outcome"] == "escalated"
    lc.shutdown()


def test_capture_text_redacts() -> None:
    lc = Lucida("lc_pk_x", test_mode=[], capture_text=True)
    c = lc.ai_conversation("u1", conversation_id="conv-1")
    c.message("user", "mail me at jo.e@ex.com")
    c.feedback("comment", comment="call +1 (415) 555-2671")
    _, msg, fb = ai_events(lc)
    assert msg["properties"]["$ai_conversation_id"] == "conv-1"
    assert msg["properties"]["$ai_text"] == "mail me at ***********" and msg["properties"]["$ai_text_redacted"] is True
    assert fb["properties"]["$ai_feedback_comment"] == "call ** ***** ********"
    assert "$ai_feedback_value" not in fb["properties"]
    lc.shutdown()
