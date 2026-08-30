from __future__ import annotations

import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml

from qdl.runtime.stable_bar_edge import StableBinanceBarEdge


ROOT = Path(__file__).resolve().parents[1]


class _Envelope:
    def __init__(self, *, binding_id: str, runtime: str, open_ms: int) -> None:
        row = [
            open_ms,
            "1",
            "1",
            "1",
            "1",
            "1",
            open_ms + 59_999,
        ]
        payload = (
            {"binding_id": binding_id, "row": row}
            if runtime == "BINANCE"
            else {
                "binding_id": binding_id,
                "data": [[
                    str(open_ms), "1", "1", "1", "1", "1", "1", "1", "1",
                ]],
            }
        )
        self.raw_frame_bytes = json.dumps(payload).encode()


class _Publisher:
    def __init__(self) -> None:
        self.batches: list[tuple[object, ...]] = []

    def publish_many(self, values):
        batch = tuple(values)
        self.batches.append(batch)
        return tuple(range(len(batch)))


def _source(binding_id: str, *, interval: str = "1m", venue: str = "BINANCE"):
    return SimpleNamespace(
        binding_id=binding_id,
        interval=interval,
        instrument=SimpleNamespace(identity=SimpleNamespace(venue=venue)),
    )


def _edge(*pairs, clock, max_workers: int = 32) -> StableBinanceBarEdge:
    edge = object.__new__(StableBinanceBarEdge)
    edge.bindings = tuple(pair for pair in pairs if pair[1].runtime == "BINANCE")
    edge.okx_bindings = tuple(pair for pair in pairs if pair[1].runtime == "OKX")
    edge._rest_fallback_active = True
    edge._last_open_ms = {source.binding_id: 0 for source, _acquisition in pairs}
    edge._retry_attempts = {}
    edge._next_retry_at = {}
    edge._last_retry_log = {}
    edge.settlement_delay_seconds = 0.10
    edge.final_retry_initial_seconds = 0.10
    edge.final_retry_max_seconds = 1.0
    edge.max_concurrent_requests = max_workers
    edge.max_catchup_rows = 1000
    edge.clock = clock
    edge.publisher = _Publisher()
    edge._persist_state = lambda: None
    edge._binance_binding = lambda source: source
    edge._okx_binding = lambda source: source
    return edge


class C419FastFinalBarDeliveryTests(unittest.TestCase):
    def test_old_provider_row_retries_the_same_target_instead_of_waiting_a_full_interval(self):
        now = [120.100]
        source = _source("binance-btc-1m")
        acquisition = SimpleNamespace(runtime="BINANCE")
        edge = _edge((source, acquisition), clock=lambda: now[0])
        old = _Envelope(binding_id=source.binding_id, runtime="BINANCE", open_ms=0)
        target = _Envelope(binding_id=source.binding_id, runtime="BINANCE", open_ms=60_000)

        with patch(
            "qdl.runtime.stable_bar_edge.fetch_latest_closed_bar_raw_envelope",
            side_effect=(old, target),
        ) as fetch:
            self.assertEqual(edge.run_cycle(), 0)
            self.assertEqual(edge._last_open_ms[source.binding_id], 0)
            self.assertAlmostEqual(edge._next_ready_at(now[0]), 120.200, places=3)
            self.assertAlmostEqual(edge._loop_sleep_seconds(now[0]), 0.100, places=3)

            now[0] = 120.200
            self.assertEqual(edge.run_cycle(), 1)

        self.assertEqual(edge._last_open_ms[source.binding_id], 60_000)
        self.assertNotIn(source.binding_id, edge._next_retry_at)
        self.assertEqual([item.kwargs["attempts"] for item in fetch.call_args_list], [1, 1])
        self.assertEqual([item.kwargs["now_ms"] for item in fetch.call_args_list], [120_100, 120_200])
        self.assertEqual(len(edge.publisher.batches), 1)

    def test_one_provider_failure_does_not_block_another_due_binding(self):
        now = [120.100]
        good = _source("binance-good-1m")
        bad = _source("okx-bad-1m", venue="OKX")
        edge = _edge(
            (good, SimpleNamespace(runtime="BINANCE")),
            (bad, SimpleNamespace(runtime="OKX")),
            clock=lambda: now[0],
        )

        def fetch(source, acquisition, *, observed_ms):
            del observed_ms
            if source.binding_id == bad.binding_id:
                raise RuntimeError("provider unavailable")
            return _Envelope(
                binding_id=source.binding_id,
                runtime=acquisition.runtime,
                open_ms=60_000,
            )

        edge._fetch_latest = fetch
        self.assertEqual(edge.run_cycle(), 1)
        self.assertEqual(edge._last_open_ms[good.binding_id], 60_000)
        self.assertEqual(edge._last_open_ms[bad.binding_id], 0)
        self.assertAlmostEqual(edge._next_retry_at[bad.binding_id], 120.200, places=3)

    def test_one_catchup_failure_does_not_block_another_due_binding(self):
        now = [120.100]
        good = _source("binance-good-catchup-1m")
        bad = _source("okx-bad-catchup-1m", venue="OKX")
        edge = _edge(
            (good, SimpleNamespace(runtime="BINANCE")),
            (bad, SimpleNamespace(runtime="OKX")),
            clock=lambda: now[0],
        )

        def fetch(source, acquisition, *, observed_ms):
            del observed_ms
            return _Envelope(
                binding_id=source.binding_id,
                runtime=acquisition.runtime,
                open_ms=60_000,
            )

        def pending(source, _acquisition, envelope, *, observed_ms):
            del observed_ms
            if source.binding_id == bad.binding_id:
                raise RuntimeError("catchup unavailable")
            return ((envelope, 60_000),)

        edge._fetch_latest = fetch
        edge._pending_for_binding = pending
        self.assertEqual(edge.run_cycle(), 1)
        self.assertEqual(edge._last_open_ms[good.binding_id], 60_000)
        self.assertEqual(edge._last_open_ms[bad.binding_id], 0)
        self.assertAlmostEqual(edge._next_retry_at[bad.binding_id], 120.200, places=3)

    def test_due_reads_are_bounded_and_published_in_binding_order(self):
        now = [120.100]
        pairs = tuple(
            (
                _source(binding_id),
                SimpleNamespace(runtime="BINANCE"),
            )
            for binding_id in ("binance-d", "binance-b", "binance-c", "binance-a")
        )
        edge = _edge(*pairs, clock=lambda: now[0], max_workers=2)
        active = 0
        peak = 0
        lock = threading.Lock()

        def fetch(source, acquisition, *, observed_ms):
            nonlocal active, peak
            del observed_ms
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(0.02)
                return _Envelope(
                    binding_id=source.binding_id,
                    runtime=acquisition.runtime,
                    open_ms=60_000,
                )
            finally:
                with lock:
                    active -= 1

        edge._fetch_latest = fetch
        self.assertEqual(edge.run_cycle(), 4)
        self.assertEqual(peak, 2)
        published = [
            json.loads(item.raw_frame_bytes)["binding_id"]
            for item in edge.publisher.batches[0]
        ]
        self.assertEqual(published, [item[0].binding_id for item in pairs])

    def test_all_enabled_crypto_rest_bar_bindings_share_the_fast_policy(self):
        acquisition = yaml.safe_load(
            (ROOT / "config/v2/stable-acquisition-bindings.yaml").read_text(
                encoding="utf-8"
            )
        )
        bindings = [
            item
            for item in acquisition["bindings"]
            if item.get("runtime") in {"BINANCE", "OKX"}
            and item.get("mode") == "PYTHON_REST"
            and str(item.get("provider_kind", "")).endswith("rest_bar")
        ]
        self.assertGreater(len(bindings), 0)
        self.assertTrue(all(item["mode"] == "PYTHON_REST" for item in bindings))

        compose = yaml.safe_load(
            (ROOT / "docker-compose.v2-stable.yml").read_text(encoding="utf-8")
        )
        env = compose["services"]["binance_bar_edge"]["environment"]
        self.assertEqual(env["QDL_STABLE_BAR_SETTLEMENT_DELAY_SECONDS"], "0.10")
        self.assertEqual(env["QDL_STABLE_BAR_RETRY_INITIAL_SECONDS"], "0.10")
        self.assertEqual(env["QDL_STABLE_BAR_RETRY_MAX_SECONDS"], "1.0")
        self.assertEqual(env["QDL_STABLE_BAR_MAX_CONCURRENT_REQUESTS"], "32")


if __name__ == "__main__":
    unittest.main()
