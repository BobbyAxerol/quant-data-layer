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
    edge.repair_only = False
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
    edge.final_settlement_confirmations = 2
    edge.final_settlement_interval_seconds = 1.0
    edge.final_settlement_max_reads = 10
    edge.final_settlement_min_age_seconds = 6.0
    edge.settlement_reads = 0
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

        # The Binance lane reads until the venue repeats the row, so the seam is the
        # settled read and it answers (envelope, settlement stats).
        with patch(
            "qdl.runtime.stable_bar_edge.fetch_settled_closed_bar_raw_envelope",
            side_effect=(
                (old, {"reads": 2, "distinct_rows": 1, "confirmations": 2, "settled_for_ms": 6100}),
                (target, {"reads": 2, "distinct_rows": 1, "confirmations": 2, "settled_for_ms": 6100}),
            ),
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
        self.assertEqual(edge.settlement_reads, 4)

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
        # One batch per binding, published as its read settles; each exactly once.
        published = [
            [json.loads(item.raw_frame_bytes)["binding_id"] for item in batch]
            for batch in edge.publisher.batches
        ]
        self.assertTrue(all(len(batch) == 1 for batch in published))
        self.assertEqual(
            sorted(batch[0] for batch in published),
            sorted(item[0].binding_id for item in pairs),
        )

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




class _RecordingPublisher(_Publisher):
    """Publisher double that logs every publish and can refuse one binding."""

    def __init__(self, events: list, *, fail_binding: str | None = None,
                 missing_ack_binding: str | None = None) -> None:
        super().__init__()
        self.events = events
        self.fail_binding = fail_binding
        self.missing_ack_binding = missing_ack_binding

    def publish_many(self, values):
        batch = tuple(values)
        ids = [json.loads(item.raw_frame_bytes)["binding_id"] for item in batch]
        self.events.append(("publish", tuple(ids)))
        if self.fail_binding in ids:
            raise RuntimeError("broker refused the record")
        self.batches.append(batch)
        if self.missing_ack_binding in ids:
            return ()
        return tuple(range(len(batch)))


def _recording_edge(pairs, events, persisted, **publisher):
    edge = _edge(*pairs, clock=lambda: 120.100)
    edge.publisher = _RecordingPublisher(events, **publisher)

    def persist():
        snapshot = dict(edge._last_open_ms)
        persisted.append(snapshot)
        events.append(("persist", snapshot))

    edge._persist_state = persist
    return edge


class PerBindingPublishTests(unittest.TestCase):
    """The bar edge publishes each binding when its own read settles."""

    def _pairs(self, *binding_ids):
        return tuple(
            (_source(binding_id), SimpleNamespace(runtime="BINANCE"))
            for binding_id in binding_ids
        )

    def test_fast_binding_is_acked_and_persisted_before_the_slow_read_finishes(self):
        events: list = []
        persisted: list = []
        pairs = self._pairs("binance-fast", "binance-slow")
        edge = _recording_edge(pairs, events, persisted)
        slow_may_finish = threading.Event()

        def fetch(source, acquisition, *, observed_ms):
            del observed_ms
            if source.binding_id == "binance-slow":
                # Settles only after the fast binding is durable.
                self.assertTrue(slow_may_finish.wait(5))
                events.append(("slow-read-done",))
            return _Envelope(binding_id=source.binding_id,
                             runtime=acquisition.runtime, open_ms=60_000)

        real_persist = edge._persist_state

        def persist():
            real_persist()
            slow_may_finish.set()

        edge._persist_state = persist
        edge._fetch_latest = fetch
        self.assertEqual(edge.run_cycle(), 2)
        kinds = [event[0] if event[0] != "publish" else ("publish", event[1]) for event in events]
        self.assertEqual(kinds[0], ("publish", ("binance-fast",)))
        self.assertEqual(kinds[1], "persist")
        self.assertEqual(kinds[2], "slow-read-done")
        self.assertEqual(kinds[3], ("publish", ("binance-slow",)))
        self.assertEqual(kinds[4], "persist")
        # ACK before persist, per binding: the first checkpoint holds only fast.
        self.assertEqual(persisted[0]["binance-fast"], 60_000)
        self.assertEqual(persisted[0]["binance-slow"], 0)
        self.assertEqual(persisted[1]["binance-slow"], 60_000)

    def test_publish_failure_mid_cycle_keeps_others_and_raises_after_the_cycle(self):
        for failure in ({"fail_binding": "binance-b"}, {"missing_ack_binding": "binance-b"}):
            with self.subTest(failure=failure):
                events: list = []
                persisted: list = []
                pairs = self._pairs("binance-a", "binance-b", "binance-c")
                edge = _recording_edge(pairs, events, persisted, **failure)
                edge._fetch_latest = lambda source, acquisition, *, observed_ms: _Envelope(
                    binding_id=source.binding_id, runtime=acquisition.runtime, open_ms=60_000)
                with self.assertRaises(RuntimeError):
                    edge.run_cycle()
                self.assertEqual(edge._last_open_ms["binance-a"], 60_000)
                self.assertEqual(edge._last_open_ms["binance-c"], 60_000)
                self.assertEqual(edge._last_open_ms["binance-b"], 0)
                self.assertEqual(persisted[-1], {"binance-a": 60_000, "binance-b": 0,
                                                 "binance-c": 60_000})
                self.assertTrue(all(snapshot["binance-b"] == 0 for snapshot in persisted))
                # Every binding was offered exactly once in the cycle.
                offered = sorted(event[1][0] for event in events if event[0] == "publish")
                self.assertEqual(offered, ["binance-a", "binance-b", "binance-c"])

    def test_restart_resumes_only_the_unacknowledged_binding_without_duplicates(self):
        events: list = []
        persisted: list = []
        pairs = self._pairs("binance-a", "binance-b")
        first = _recording_edge(pairs, events, persisted, fail_binding="binance-b")
        fetch = lambda source, acquisition, *, observed_ms: _Envelope(
            binding_id=source.binding_id, runtime=acquisition.runtime, open_ms=60_000)
        first._fetch_latest = fetch
        with self.assertRaises(RuntimeError):
            first.run_cycle()
        # A restarted edge loads the last persisted checkpoint.
        events_after: list = []
        restarted = _recording_edge(pairs, events_after, [])
        restarted._last_open_ms = dict(persisted[-1])
        restarted._fetch_latest = fetch
        self.assertEqual(restarted.run_cycle(), 1)
        self.assertEqual(
            [event[1] for event in events_after if event[0] == "publish"],
            [("binance-b",)],
        )
        self.assertEqual(restarted._last_open_ms, {"binance-a": 60_000, "binance-b": 60_000})
        # A further cycle at the same instant publishes nothing again.
        self.assertEqual(restarted.run_cycle(), 0)
        self.assertEqual(
            len([event for event in events_after if event[0] == "publish"]), 1
        )

if __name__ == "__main__":
    unittest.main()
