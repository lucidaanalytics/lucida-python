from __future__ import annotations

import json
from pathlib import Path

from lucida import AiConfig, Lucida, ai_config

AGENT = {
    "model": "m-large", "mode": "agent", "version": 2, "instructions": "Help {{name}} {{missing}}",
    "tools": [{"name": "search_docs", "description": "Find docs", "parameters": {"type": "object"}}],
    "parameters": {"temperature": 0.2},
}
FLAG = {
    "key": "assistant", "type": "multivariate", "active": True, "salt": "s", "aggregation": {"kind": "person"},
    "variants": [{"key": "agent", "rollout_pct": 100, "payload": AGENT}], "conditions": [{"rollout_pct": 100}],
}
FALLBACK = AiConfig(model="m-small", messages=[{"role": "system", "content": "Hi {{name}}"}])


def test_renders_completion_agent_and_fallback() -> None:
    c = ai_config("v1", {"model": "m", "version": 3, "messages": [{"role": "user", "content": "Hi {{name}}"}]}, FALLBACK, {"name": "Ada"})
    assert (c.enabled, c.mode, c.version, c.messages) == (True, "completion", 3, [{"role": "user", "content": "Hi Ada"}])
    a = ai_config("agent", AGENT, FALLBACK, {"name": "Ada"})
    assert (a.mode, a.instructions, a.tools[0]["name"], a.messages) == ("agent", "Help Ada {{missing}}", "search_docs", [])
    off = ai_config(None, None, FALLBACK, {"name": "Ada"})
    assert (off.enabled, off.model, off.messages[0]["content"]) == (False, "m-small", "Hi Ada")
    agent_fb = ai_config(None, None, AiConfig(model="x", instructions="Be brief"))
    assert agent_fb.mode == "agent"
    assert ai_config("v", {"messages": []}, FALLBACK).enabled is False  # no model: the fallback


def test_client_serves_the_variation(tmp_path: Path) -> None:
    f = tmp_path / "flags.json"
    f.write_text(json.dumps({"flags": [FLAG]}))
    with Lucida("lc_pk_x", secret_key="lc_sk_x", test_mode=f) as lc:
        c = lc.get_ai_config("assistant", "u1", FALLBACK, variables={"name": "Ada"})
        assert (c.enabled, c.variation, c.mode, c.instructions) == (True, "agent", "agent", "Help Ada {{missing}}")
        assert lc.get_ai_config("missing", "u1", FALLBACK).enabled is False
