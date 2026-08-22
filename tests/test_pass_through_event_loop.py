"""The pass-through has to work where it is actually deployed.

Both roles that build the query stack serve from inside a running event loop.
Every earlier test called the source from plain synchronous test code, so the
OKX path could call `asyncio.run` and pass while being unable to run even once
in the query role. These tests put the source where it really lives.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import unittest
from pathlib import Path

from qdl.adapters.binance.bar_edge import BinanceBarRawBinding
from qdl.adapters.okx.bar_edge import OkxBarRawBinding
from qdl.query.contracts import (
    ConsumerGrade,
    DataRequirement,
    FeedType,
    RecoveryPolicy,
)
from qdl.runtime.provider_history import (
    ProviderBarHistorySource,
    ProviderHistoryUnavailable,
)
from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
BINANCE_ETH = "ee93fabf-68df-5b50-8924-51bf25a5a757"
OKX_ETH = "e49b54ae-c23d-5351-9e64-47934aac28f8"
INTERVAL = "1m"
INTERVAL_MS = 60_000


def _requirement(uid: str, limit: int = 3) -> DataRequirement:
    return DataRequirement(
        instrument_uid=uid,
        feed=FeedType.BAR,
        consumer_grade=ConsumerGrade.ALPHA,
        source_policy_id="crypto_primary_v2",
        interval=INTERVAL,
        warmup_limit=limit,
        recovery=RecoveryPolicy.FRESH_SNAPSHOT,
    )


class _Envelope:
    def __init__(self, frame: dict, provider: str):
        self.raw_frame_bytes = json.dumps(frame, sort_keys=True).encode()
        self.provider = provider
        self.capture_id = b"\x01" * 16
        self.raw_frame_sha256 = b"\x02" * 32


def _binance_envelopes(binding: BinanceBarRawBinding, limit: int):
    base = 1_700_000_000_000
    return tuple(
        _Envelope(
            {
                "symbol": binding.native_symbol,
                "interval": binding.interval,
                "bar_origin": "BACKFILLED",
                "row": [
                    base + index * INTERVAL_MS,
                    "100", "101", "99", "100.5", "10",
                    base + index * INTERVAL_MS + INTERVAL_MS - 1,
                    "1005", 7, "5", "500", "0",
                ],
            },
            "BINANCE_DIRECT",
        )
        for index in range(limit)
    )


def _okx_envelopes(binding: OkxBarRawBinding, limit: int):
    base = 1_700_000_000_000
    return tuple(
        _Envelope(
            {
                "arg": {
                    "channel": f"candle{binding.interval}",
                    "instId": binding.native_symbol,
                },
                "data": [[
                    str(base + index * INTERVAL_MS),
                    "100", "101", "99", "100.5", "10", "1005", "1005", "1",
                ]],
            },
            "OKX_DIRECT",
        )
        for index in range(limit)
    )


class PassThroughInsideEventLoopTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)

    def _source(self, **kwargs) -> ProviderBarHistorySource:
        def binance_fetcher(binding, *, limit, **rest):
            return _binance_envelopes(binding, limit)

        async def okx_fetcher(binding, *, limit, **rest):
            return _okx_envelopes(binding, limit)

        kwargs.setdefault("binance_fetcher", binance_fetcher)
        kwargs.setdefault("okx_fetcher", okx_fetcher)
        return ProviderBarHistorySource(self.catalog, **kwargs)

    def test_okx_history_runs_inside_a_running_event_loop(self) -> None:
        """The regression: this raised RuntimeError in the query role.

        `asyncio.run` refuses to start a loop on a thread that already has one,
        so the OKX pass-through was unable to answer a single request in the
        role that serves it, while passing when called from sync test code.
        """
        source = self._source()

        async def serve():
            return source.history(_requirement(OKX_ETH))

        envelopes = asyncio.run(serve())
        self.assertEqual(len(envelopes), 3)
        self.assertTrue(all(item.bar.is_final for item in envelopes))

    def test_binance_history_runs_inside_a_running_event_loop(self) -> None:
        source = self._source()

        async def serve():
            return source.history(_requirement(BINANCE_ETH))

        self.assertEqual(len(asyncio.run(serve())), 3)

    def test_history_result_is_served_from_inside_a_running_loop(self) -> None:
        source = self._source()

        async def serve():
            return source.history_result(
                _requirement(OKX_ETH), schema_digest="d" * 64
            )

        result = asyncio.run(serve())
        self.assertEqual(len(result.items), 3)
        self.assertFalse(result.items[0].quality.execution_eligible)

    def test_a_venue_that_never_answers_is_refused_not_waited_on(self) -> None:
        stop = threading.Event()
        self.addCleanup(stop.set)

        def hanging(binding, *, limit, **rest):
            stop.wait(30)
            raise AssertionError("fetch should have been abandoned")

        source = self._source(binance_fetcher=hanging, fetch_timeout_seconds=0.2)
        started = time.monotonic()
        with self.assertRaises(ProviderHistoryUnavailable) as caught:
            source.history(_requirement(BINANCE_ETH))
        elapsed = time.monotonic() - started
        self.assertIn("exceeded", str(caught.exception))
        self.assertLess(elapsed, 5.0)

    def test_a_failing_fetch_reports_the_venue_error_not_the_timeout(self) -> None:
        def failing(binding, *, limit, **rest):
            raise RuntimeError("venue said no")

        source = self._source(binance_fetcher=failing)
        with self.assertRaises(RuntimeError) as caught:
            source.history(_requirement(BINANCE_ETH))
        self.assertEqual(str(caught.exception), "venue said no")

    def test_concurrent_callers_for_one_window_fetch_it_once(self) -> None:
        """The gRPC role serves on a real thread pool.

        Without single-flight, every concurrent consumer asking for the same
        bar period sends its own venue request for a window already in flight.
        """
        calls: list[int] = []
        gate = threading.Barrier(4, timeout=10)

        def counting(binding, *, limit, **rest):
            calls.append(1)
            time.sleep(0.2)
            return _binance_envelopes(binding, limit)

        source = self._source(binance_fetcher=counting)
        results: list[int] = []

        def ask():
            gate.wait()
            results.append(len(source.history(_requirement(BINANCE_ETH))))

        threads = [threading.Thread(target=ask) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(15)
        self.assertEqual(results, [3, 3, 3, 3])
        self.assertEqual(len(calls), 1)

    def test_window_locks_do_not_accumulate(self) -> None:
        source = self._source()
        for boundary in range(5):
            source.history(_requirement(BINANCE_ETH))
            source.cache._entries.clear()
        self.assertEqual(source._inflight, {})


if __name__ == "__main__":
    unittest.main()
