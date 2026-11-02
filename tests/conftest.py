from __future__ import annotations

import gzip
import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx

# $LUCIDA_TESTDATA, else the nearest testdata/ above this file (monorepo and mirror layouts both work)
ROOT = (
    Path(os.environ["LUCIDA_TESTDATA"])
    if os.environ.get("LUCIDA_TESTDATA")
    else next((p / "testdata" for p in Path(__file__).resolve().parents if (p / "testdata").is_dir()), Path("testdata"))
)
HOST = "http://lucida.test"
Responder = Callable[[httpx.Request], httpx.Response | None]


class FakeServer:
    """httpx MockTransport that records requests; set `respond` per test for non-200 replies."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.respond: Responder = lambda r: None
        self.flag_defs: list[dict[str, Any]] = []
        self.etag = '"v1"'
        self.remote_flags: dict[str, Any] = {}
        self.remote_configs: dict[str, Any] = {}

    def handler(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        custom = self.respond(req)
        if custom is not None:
            return custom
        path = req.url.path
        if path == "/v1/flags/stream":
            return httpx.Response(404)  # polling-only unless a test overrides `respond`
        if path == "/v1/flags/definitions":
            if req.headers.get("if-none-match") == self.etag:
                return httpx.Response(304)
            return httpx.Response(200, json={"flags": self.flag_defs}, headers={"ETag": self.etag})
        if path.startswith("/v1/flags/") and path.endswith("/remote-config"):
            key = unquote(req.url.raw_path.decode().split("/")[3])
            if key not in self.remote_configs:
                return httpx.Response(404, json={"title": "no remote-config flag with this key"})
            return httpx.Response(200, json={"key": key, "enabled": True, "payload": self.remote_configs[key]})
        if path == "/v1/flags":
            return httpx.Response(200, json={"flags": self.remote_flags, "request_id": "r1"})
        return httpx.Response(200, json={"status": 1})

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def to(self, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path == path]

    def bodies(self, path: str) -> list[dict[str, Any]]:
        return [body(r) for r in self.to(path)]

    def events(self, path: str = "/v1/batch") -> list[dict[str, Any]]:
        return [e for b in self.bodies(path) for e in b["batch"]]


def body(r: httpx.Request) -> dict[str, Any]:
    raw = r.content
    if r.url.params.get("compression") == "gzip":
        raw = gzip.decompress(raw)
    out: dict[str, Any] = json.loads(raw)
    return out


def load(rel: str) -> Any:
    return json.loads((ROOT / rel).read_text())
