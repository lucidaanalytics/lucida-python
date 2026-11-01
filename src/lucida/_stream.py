"""SSE plumbing for GET /v1/flags/stream: a line parser shared by the sync and async clients."""

from __future__ import annotations


class SseEvents:
    """Feed lines (no newline); `feed` returns the event name when a blank line completes a message."""

    def __init__(self) -> None:
        self._event = ""
        self._has_data = False

    def feed(self, line: str) -> str | None:
        if line == "":
            ev, had = self._event or "message", self._has_data
            self._event, self._has_data = "", False
            return ev if had else None
        if line.startswith(":"):
            return None
        key, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if key == "event":
            self._event = value
        elif key == "data":
            self._has_data = True
        return None
