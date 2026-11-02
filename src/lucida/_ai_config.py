"""AI Config flags (LC-FLG-25, P-M49): render the served variation of an `ai_config` flag.

A variation is completion mode (chat ``messages``) or agent mode (``instructions`` plus ``tools``).
The SDK only selects and renders it; evaluating model output is Watchtower's job. Mirrors
``packages/sdk-core/src/ai-config.ts``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Any

_VAR = re.compile(r"\{\{\s*([\w.-]+)\s*\}\}")


def _fill(s: str, variables: dict[str, str]) -> str:
    return _VAR.sub(lambda m: variables.get(m.group(1), m.group(0)), s)


@dataclass(frozen=True)
class AiConfig:
    """A rendered AI config. ``enabled`` is False when the flag served nothing and this is the fallback."""

    model: str
    enabled: bool = False
    mode: str = "completion"
    provider: str | None = None
    messages: list[dict[str, str]] = field(default_factory=list)
    instructions: str | None = None
    tools: list[dict[str, Any]] = field(default_factory=list)
    parameters: dict[str, Any] = field(default_factory=dict)
    variation: str | None = None
    version: int | None = None


def ai_config(variant: str | None, payload: Any, fallback: AiConfig, variables: dict[str, str] | None = None) -> AiConfig:
    """The config served as ``variant`` with ``payload`` (or ``fallback``), ``{{var}}`` placeholders filled."""
    v = variables or {}
    p = payload if isinstance(payload, dict) else None
    if variant and p is not None and isinstance(p.get("model"), str):
        msgs = p.get("messages")
        tools = p.get("tools")
        instr = p.get("instructions")
        version = p.get("version")
        base = AiConfig(
            model=p["model"],
            enabled=True,
            mode="agent" if p.get("mode") == "agent" else "completion",
            provider=p.get("provider") or None,
            messages=list(msgs) if isinstance(msgs, list) else [],
            instructions=instr if isinstance(instr, str) else None,
            tools=list(tools) if isinstance(tools, list) else [],
            parameters=dict(p.get("parameters") or {}),
            variation=variant,
            version=version if isinstance(version, int) and not isinstance(version, bool) else None,
        )
    else:
        mode = fallback.mode if fallback.mode != "completion" or fallback.instructions is None else "agent"
        base = replace(fallback, enabled=False, mode=mode)
    return replace(
        base,
        messages=[{"role": m["role"], "content": _fill(m["content"], v)} for m in base.messages],
        instructions=_fill(base.instructions, v) if base.instructions is not None else None,
    )
