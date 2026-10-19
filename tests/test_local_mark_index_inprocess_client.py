"""IDR-114: the in-process MARK/INDEX client matches httpx's ASGI transport and leaves no reference cycle.

The isolated stage-100 replay (2026-10-17, DEBUG_SAVEALL on one replica) found that all of a Query
replica's cyclic garbage in steady load was httpx Request/Response objects of the in-process MARK/INDEX
reads (25 cyclic objects per read), freed by 0.2-0.5 s young collections every 76-110 s.
"""

from __future__ import annotations

import asyncio
import gc
import json
import unittest

import httpx
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from qdl.reference.local_mark_index import InProcessAsgiClient

_URL = "http://localhost/internal/v2/execution-mark-index/read"


def _app() -> FastAPI:
    app = FastAPI()

    @app.post("/internal/v2/execution-mark-index/read")
    async def read(request: Request, signature: str | None = Header(None, alias="X-QDL-Stable-Signature")):
        body = await request.body()
        if signature != "ok":
            return JSONResponse(status_code=401, content={"detail": "invalid stable read signature"})
        payload = json.loads(body)
        if payload.get("missing"):
            return JSONResponse(status_code=409, content={"detail": "NO_ACTIVE_RECORD"})
        return JSONResponse(
            {"echo": payload, "content_type": request.headers.get("content-type"), "path": request.url.path},
            headers={"X-QDL-Freshness-Basis": "SOURCE_EVENT", "X-QDL-Session-Liveness-Ms": "12"},
        )

    @app.post("/boom")
    async def boom():
        raise RuntimeError("app failure")

    return app


async def _post(client, url, payload, signature="ok"):
    body = json.dumps(payload).encode()
    return await client.post(url, content=body, headers={"Content-Type": "application/json",
                                                          "X-QDL-Stable-Signature": signature}, timeout=1.0)


class InProcessAsgiClientTests(unittest.TestCase):
    def test_responses_match_httpx_asgi_transport(self):
        app = _app()

        async def scenario():
            reference = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://localhost")
            client = InProcessAsgiClient(app)
            cases = (({"a": 1}, "ok"), ({"missing": True}, "ok"), ({"a": 2}, "bad"))
            out = []
            for payload, signature in cases:
                expected = await _post(reference, _URL, payload, signature)
                actual = await _post(client, _URL, payload, signature)
                out.append((expected, actual))
            await reference.aclose()
            await client.aclose()
            return out

        for expected, actual in asyncio.run(scenario()):
            self.assertEqual(actual.status_code, expected.status_code)
            self.assertEqual(actual.json(), expected.json())
            self.assertEqual(actual.content, expected.content)
            for name in ("content-type", "X-QDL-Freshness-Basis", "x-qdl-session-liveness-ms"):
                self.assertEqual(actual.headers.get(name), expected.headers.get(name))
        ok = asyncio.run(scenario())[0][1]
        self.assertEqual(ok.json()["path"], "/internal/v2/execution-mark-index/read")
        self.assertEqual(ok.json()["content_type"], "application/json")
        self.assertEqual(ok.headers["X-QDL-Session-Liveness-Ms"], "12")

    def test_app_exceptions_propagate_like_the_transport(self):
        client = InProcessAsgiClient(_app())
        with self.assertRaises(RuntimeError):
            asyncio.run(_post(client, "http://localhost/boom", {}))

    def test_a_read_leaves_no_cyclic_garbage(self):
        client = InProcessAsgiClient(_app())

        async def reads(count):
            for _ in range(count):
                response = await _post(client, _URL, {"a": 1})
                response.json()

        async def scenario():
            await reads(3)
            enabled = gc.isenabled()
            gc.collect()
            gc.disable()
            try:
                await reads(50)
                return gc.collect()
            finally:
                if enabled:
                    gc.enable()

        self.assertEqual(asyncio.run(scenario()), 0)


if __name__ == "__main__":
    unittest.main()
