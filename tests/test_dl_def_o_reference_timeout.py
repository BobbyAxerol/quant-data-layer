"""DL-DEF-2026-11-27-O: a reference batch's client timeout follows its declared deadline; OKX per-instrument budgets.

Incident (Crossec 2026-11-27 09:35:20Z): a `reference:batch` of 16 FUNDING_RATE 90-day requirements carried
`deadline_ms=120000`, the qdl_sdk RestQueryTransport gave up at its 10 s default while query_kn_1 answered 200 at
10.69 s. The server time came from the OKX REST budget: funding-rate-history (documented 10 requests / 2 s per IP +
instrument) shared the one process-wide `public` bucket (5 burst, 2.5/s) with every other public call.

Covered:
- SDK: every deadline-carrying request (warmup, warmup batch, reference batch) waits for its own deadline + margin when
  that is longer than the transport default; requests without deadlines are unchanged; over **real HTTP** (uvicorn on
  127.0.0.1) a slow 200 is received instead of a ReadTimeout.
- Server: funding-rate-history and open-interest get one bucket per instrument at 40 % of the documented rate; a
  16-instrument batch no longer queues for tokens (timed with a zero-latency fake provider, old vs new).
- Telemetry: slow_request lines carry the consumer id and SDK session hint for attribution.
"""
from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
import unittest
from unittest import mock

import httpx

from qdl.adapters.okx import client as okx_client
from qdl.adapters.okx.client import OKX_INSTRUMENT_SCOPED_LIMITS, OkxRestClient
from qdl.adapters.okx.history import OkxHistoricalClient
from qdl_sdk import StaticBearerCredential
from qdl_sdk.models import Grade
from qdl_sdk.reference import ReferenceProduct, ReferenceRequirement
from qdl_sdk.transport import COLD_TIMEOUT_MARGIN_S, RestQueryTransport

UID = "0b7a3c1e-1111-5222-8333-444455556666"


def reference(deadline_ms=None):
    kwargs = {} if deadline_ms is None else {"deadline_ms": deadline_ms}
    return ReferenceRequirement(instrument_uid=UID, product=ReferenceProduct.FUNDING_RATE, consumer_grade=Grade.ALPHA,
                                source_policy_id="crypto_liquid_v2", start_time_ns=1_000_000_000,
                                end_time_ns=2_000_000_000, limit=10_000, **kwargs)


class _Capture:
    """httpx.AsyncClient stand-in recording the timeout of each call."""

    def __init__(self):
        self.timeouts = []

    async def post(self, path, *, timeout=httpx.USE_CLIENT_DEFAULT, **kwargs):
        self.timeouts.append(timeout)
        return httpx.Response(200, json={"schema": "test", "results": []},
                              request=httpx.Request("POST", "http://test" + path))

    async def aclose(self):
        pass


class SdkTimeoutTests(unittest.TestCase):
    def transport(self, timeout_seconds=10.0):
        capture = _Capture()
        transport = RestQueryTransport("http://test", client=capture, timeout_seconds=timeout_seconds,
                                       credential_provider=StaticBearerCredential("t"))
        return transport, capture

    def test_reference_batch_waits_for_its_declared_deadline(self):
        transport, capture = self.transport(10.0)
        asyncio.run(transport.reference_batch((reference(120_000), reference(30_000)), consumer_id="c",
                                              require_all=True))
        self.assertEqual(capture.timeouts, [120 + COLD_TIMEOUT_MARGIN_S])

    def test_a_deadline_inside_the_transport_default_keeps_the_default(self):
        transport, capture = self.transport(60.0)
        asyncio.run(transport.reference_batch((reference(20_000),), consumer_id="c", require_all=True))
        self.assertEqual(capture.timeouts, [httpx.USE_CLIENT_DEFAULT])

    def test_the_default_deadline_is_also_honoured(self):
        transport, capture = self.transport(5.0)
        asyncio.run(transport.reference_batch((reference(),), consumer_id="c", require_all=True))
        self.assertEqual(capture.timeouts, [20 + COLD_TIMEOUT_MARGIN_S])

    def test_the_warmup_rule_is_the_same_helper(self):
        transport, _ = self.transport(10.0)
        spec = type("Spec", (), {"deadline_ms": 50_000})()
        item = type("Req", (), {"warmup_specification": spec})()
        self.assertEqual(transport._cold_timeout_seconds((item,)), 50 + COLD_TIMEOUT_MARGIN_S)
        self.assertIsNone(transport._deadline_timeout_seconds(()))


class SdkTimeoutOverRealHttpTests(unittest.TestCase):
    """A real HTTP server answers 200 after 1.5 s; the transport default is 1.0 s."""

    @classmethod
    def setUpClass(cls):
        import uvicorn
        from starlette.applications import Starlette
        from starlette.responses import JSONResponse
        from starlette.routing import Route

        async def batch(request):
            body = await request.json()
            await asyncio.sleep(1.5)
            return JSONResponse({"schema": "qdl.reference.batch.test", "items": len(body["requirements"])})

        app = Starlette(routes=[Route("/v2/market-data/reference:batch", batch, methods=["POST"])])
        cls.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cls.sock.bind(("127.0.0.1", 0))
        cls.port = cls.sock.getsockname()[1]
        cls.server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="off"))
        cls.thread = threading.Thread(target=cls.server.run, kwargs={"sockets": [cls.sock]}, daemon=True)
        cls.thread.start()
        deadline = time.monotonic() + 10
        while not cls.server.started and time.monotonic() < deadline:
            time.sleep(0.05)

    @classmethod
    def tearDownClass(cls):
        cls.server.should_exit = True
        cls.thread.join(10)
        cls.sock.close()

    def call(self, deadline_ms):
        async def scenario():
            transport = RestQueryTransport(f"http://127.0.0.1:{self.port}", timeout_seconds=1.0,
                                           credential_provider=StaticBearerCredential("t"))
            try:
                return await transport.reference_batch((reference(deadline_ms),), consumer_id="c", require_all=True)
            finally:
                await transport.close()
        return asyncio.run(scenario())

    def test_a_slow_200_inside_the_declared_deadline_is_received(self):
        self.assertEqual(self.call(3_000)["items"], 1)

    def test_without_the_fix_the_transport_default_cuts_the_same_request(self):
        # The 2.10/2.11.0 behaviour (no per-request timeout on reference_batch): a ReadTimeout at the 1.0 s default.
        with mock.patch.object(RestQueryTransport, "_deadline_timeout_seconds", lambda self, deadlines: None):
            with self.assertRaises(httpx.ReadTimeout):
                self.call(3_000)


class OkxInstrumentBudgetTests(unittest.TestCase):
    def test_documented_instrument_scoped_endpoints_get_one_bucket_per_instrument(self):
        client = OkxRestClient()

        async def scenario():
            a = await client._bucket_for("/api/v5/public/funding-rate-history", {"instId": "A-USDT-SWAP"}, "public")
            a2 = await client._bucket_for("/api/v5/public/funding-rate-history", {"instId": "a-usdt-swap"}, "public")
            b = await client._bucket_for("/api/v5/public/funding-rate-history", {"instId": "B-USDT-SWAP"}, "public")
            oi = await client._bucket_for("/api/v5/public/open-interest", {"instId": "A-USDT-SWAP"}, "public")
            whole = await client._bucket_for("/api/v5/public/open-interest", {"instType": "SWAP"}, "public")
            return a, a2, b, oi, whole

        a, a2, b, oi, whole = asyncio.run(scenario())
        self.assertIs(a, a2)
        self.assertIsNot(a, b)
        self.assertIsNot(a, oi)
        self.assertIs(whole, client._buckets["public"], "no instId: the generic bucket")
        self.assertEqual((a._capacity, a._refill), (2.0, 10 / 2 * 0.4))
        self.assertEqual((oi._capacity, oi._refill), (2.0, 20 / 2 * 0.4))

    def _batch_seconds(self):
        """16 instruments x one 90-day funding page through the real client and history adapter, provider
        concurrency 4 as `ReferenceBatchPolicy`, a zero-latency fake provider: the time is token waiting only."""
        page = [{"instId": "X", "fundingTime": str(1_000_000 + i), "fundingRate": "0.0001"} for i in range(3)]

        def fake_get(url, params=None, timeout=None):
            rows = [{**row, "instId": params["instId"]} for row in page]
            response = mock.Mock(status_code=200)
            response.raise_for_status = lambda: None
            response.json = lambda: {"code": "0", "data": rows if int(params["after"]) > 1_000_000 else []}
            return response

        async def scenario():
            history = OkxHistoricalClient(OkxRestClient())
            gate = asyncio.Semaphore(4)

            async def one(i):
                async with gate:
                    return await history.funding_history(inst_id=f"I{i:02}-USDT-SWAP", start_ms=1_000_000,
                                                         end_ms=1_000_010)
            started = time.monotonic()
            await asyncio.gather(*(one(i) for i in range(16)))
            return time.monotonic() - started

        with mock.patch.object(okx_client.requests, "get", fake_get):
            return asyncio.run(scenario())

    def test_a_16_instrument_funding_batch_no_longer_queues_for_tokens(self):
        new = self._batch_seconds()
        with mock.patch.dict(OKX_INSTRUMENT_SCOPED_LIMITS, {}, clear=True):
            old = self._batch_seconds()
        # Old: 5 burst then 2.5/s for 16 requests: >= 4.4 s of token waits. New: one bucket per instrument.
        self.assertGreaterEqual(old, 4.0, old)
        self.assertLess(new, 1.0, new)


class SlowRequestAttributionTests(unittest.TestCase):
    def test_slow_request_carries_consumer_and_session_bounded(self):
        from qdl.runtime.stall_telemetry import SlowRequestTelemetry
        records = []
        recorder = mock.Mock(record=records.append)

        async def app(scope, receive, send):
            await asyncio.sleep(0.03)
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        async def send(message):
            pass

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        wrapped = SlowRequestTelemetry(app, recorder=recorder, threshold_seconds=0.02)
        headers = [(b"x-qdl-consumer-id", b"alpha.okx.paper.stable"), (b"x-qdl-session", b"crossec-boot-1"),
                   (b"authorization", b"Bearer secret")]
        asyncio.run(wrapped({"type": "http", "method": "POST", "path": "/v2/market-data/reference:batch",
                             "headers": headers}, receive, send))
        asyncio.run(wrapped({"type": "http", "method": "POST", "path": "/x",
                             "headers": [(b"x-qdl-consumer-id", b"bad\x07id")]}, receive, send))
        self.assertEqual((records[0]["consumer"], records[0]["session"]), ("alpha.okx.paper.stable", "crossec-boot-1"))
        self.assertNotIn("secret", json.dumps(records))
        self.assertEqual((records[1]["consumer"], records[1]["session"]), (None, None))


if __name__ == "__main__":
    unittest.main()
