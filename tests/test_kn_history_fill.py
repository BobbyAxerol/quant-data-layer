"""KN-4 D47-3: venue history fill - depth by demand, downstream backpressure,
truthful short provider history.

Unit cases use fakes (no venue, no broker). The Kafka case needs
``QDL_KN_TEST_KAFKA`` = a disposable broker (topics ``kn4-bp-*`` created and
deleted by the test) and is skipped loudly otherwise.
"""
from __future__ import annotations

import os
import unittest
import uuid

from qdl.runtime.bar_history_demand import demanded_history_rows
from qdl.runtime.history_backpressure import KafkaStageBacklog, Stage, stage_backlog


def _bundle():
    bindings = [
        {"binding_id": "b-1m", "feed": "BAR", "instrument_uid": "u1", "interval": "1m"},
        {"binding_id": "b-1h", "feed": "BAR", "instrument_uid": "u1", "interval": "1h"},
        {"binding_id": "b-nobody", "feed": "BAR", "instrument_uid": "u2", "interval": "1m"},
        {"binding_id": "t", "feed": "TRADE", "instrument_uid": "u1", "interval": None},
    ]
    manifests = [
        {"quotas": {"max_warmup_rows": 5000}, "requirements": [{"instrument_uid": "u1", "feed": "BAR", "interval": "1m"}]},
        {"quotas": {"max_warmup_rows": 10000}, "requirements": [{"instrument_uid": "u1", "feed": "BAR", "interval": "1m"},
                                                                {"instrument_uid": "u1", "feed": "TRADE", "interval": None}]},
        {"quotas": {"max_warmup_rows": 500}, "requirements": [{"instrument_uid": "u1", "feed": "BAR", "interval": "1h"}]},
    ]
    return {"catalog": {"bindings": bindings}, "manifests": manifests}


class DemandTests(unittest.TestCase):
    def test_the_largest_demand_of_a_requiring_manifest_per_bar_binding(self):
        self.assertEqual(demanded_history_rows(_bundle()), {"b-1m": 10000, "b-1h": 500, "b-nobody": 0})


class BackpressureTests(unittest.TestCase):
    def test_backlog_counts_unconsumed_records_and_the_gate_closes_over_a_limit(self):
        self.assertEqual(stage_backlog({0: 5, 1: 10}, {0: 8, 1: 10, 2: 4}), 3 + 0 + 4)
        committed = {"core": {0: 100}, "a": {0: 0}}
        ends = {"raw": {0: 150}, "canonical": {0: 20}}
        gate = KafkaStageBacklog([Stage("core", "core", "raw", 100), Stage("a", "a", "canonical", 10)],
                                 read_committed=lambda group, _topic: committed[group],
                                 read_ends=lambda topic: ends[topic])
        admitted, detail = gate()
        self.assertEqual((admitted, detail["over"]), (False, {"a": 20}))
        committed["a"] = {0: 15}
        self.assertEqual(gate()[0], True)

    def test_an_unreadable_backlog_never_admits_history(self):
        def boom(_group, _topic):
            raise RuntimeError("broker down")

        gate = KafkaStageBacklog([Stage("core", "g", "raw", 1)], read_committed=boom, read_ends=lambda _t: {})
        admitted, detail = gate()
        self.assertFalse(admitted)
        self.assertIn("broker down", detail["error"])


class ShortProviderHistoryTests(unittest.TestCase):
    def _binding(self):
        from qdl.adapters.binance.bar_edge import BinanceBarRawBinding

        return BinanceBarRawBinding(market="USDM", product_type="PERPETUAL", native_symbol="NEWUSDT", interval="1m",
                                    subscription_id="s", source_session_id="x", connection_generation=1, lease_epoch=1,
                                    authority_revision=1, partition_plan_epoch=1, adapter_version="t",
                                    config_revision=1, instrument_catalog_revision=1)

    @staticmethod
    def _row(open_ms):
        return [open_ms, "1", "1", "1", "1", "1", open_ms + 59_999, "1", 1, "1", "1", "0"]

    def test_binance_accepts_a_short_window_only_when_nothing_older_exists(self):
        from qdl.adapters.binance.bar_edge import fetch_closed_bar_history_raw_envelopes

        now = 1_000 * 60_000
        listing = now - 30 * 60_000  # 30 closed bars exist

        def fetcher(older_exists):
            def fetch(symbol, *, interval, limit, end_time, market):
                rows = [self._row(t) for t in range(listing, end_time + 1, 60_000)][-limit:]
                if limit == 1 and older_exists and end_time < listing:
                    rows = [self._row(listing - 60_000)]
                return {"data": rows}
            return fetch

        values = fetch_closed_bar_history_raw_envelopes(self._binding(), limit=100, now_ms=now, attempts=1,
                                                        fetcher=fetcher(False), sleep=lambda _s: None,
                                                        allow_short=True)
        self.assertEqual(len(values), 30)
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            fetch_closed_bar_history_raw_envelopes(self._binding(), limit=100, now_ms=now, attempts=1,
                                                   fetcher=fetcher(True), sleep=lambda _s: None, allow_short=True)
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            fetch_closed_bar_history_raw_envelopes(self._binding(), limit=100, now_ms=now, attempts=1,
                                                   fetcher=fetcher(False), sleep=lambda _s: None)

    def test_okx_accepts_a_short_window_only_when_okx_itself_is_exhausted(self):
        import asyncio

        from qdl.adapters.okx.bar_edge import OkxBarRawBinding, fetch_closed_bar_history_raw_envelopes
        from qdl.adapters.okx.history import HistoryCoverage, OkxCandle, OkxCandleHistory

        now = 1_000 * 60_000
        records = tuple(OkxCandle(inst_id="NEW-USDT-SWAP", bar="1m", price_type="TRADE", open_ts_ms=t, open="1",
                                  high="1", low="1", close="1", volume_raw="1", volume_ccy_raw="1",
                                  volume_quote_raw="1", confirmed=True)
                        for t in range(now - 20 * 60_000, now - 60_000 + 1, 60_000))

        class Client:
            def __init__(self, reason):
                self.reason = reason

            async def candles(self, **_kwargs):
                return OkxCandleHistory(records, HistoryCoverage(
                    requested_start_ms=0, requested_end_ms=now, observed_min_ts_ms=records[0].open_ts_ms,
                    observed_max_ts_ms=records[-1].open_ts_ms, complete_left=False, complete_right=True,
                    truncated=False, terminal_reason=self.reason, provider_endpoint="/api/v5/market/history-candles"))

        binding = OkxBarRawBinding(market="SWAP", product_type="PERPETUAL", native_symbol="NEW-USDT-SWAP",
                                   interval="1m", subscription_id="s", source_session_id="x",
                                   connection_generation=1, lease_epoch=1, authority_revision=1,
                                   partition_plan_epoch=1, adapter_version="t", config_revision=1,
                                   instrument_catalog_revision=1)
        values = asyncio.run(fetch_closed_bar_history_raw_envelopes(
            binding, limit=100, now_ms=now, history_client=Client("PROVIDER_EXHAUSTED"), allow_short=True))
        self.assertEqual(len(values), 20)
        with self.assertRaises(RuntimeError):
            asyncio.run(fetch_closed_bar_history_raw_envelopes(
                binding, limit=100, now_ms=now, history_client=Client("MAX_PAGES"), allow_short=True))


class EdgeFillTests(unittest.TestCase):
    """The edge's history loop: depth by demand, a closed gate pauses without
    failing, short and empty venue history are reported and not retried."""

    def _edge(self, *, demand, gate):
        from qdl.runtime.stable_bar_edge import StableBinanceBarEdge

        edge = StableBinanceBarEdge.__new__(StableBinanceBarEdge)
        sources = [type("S", (), {"binding_id": name, "interval": "1m"})() for name in ("a", "b", "c")]
        edge.history_bindings = tuple((source, None) for source in sources)
        edge.history_okx_bindings = ()
        edge.repair_only = False
        edge._history_bootstrap_active = True
        edge._history_bootstrapped = False
        edge._last_open_ms = {}
        edge._history_short = {}
        edge._history_gate_closed_at = None
        edge.history_demand = demand
        edge.history_gate = gate
        edge.warmup_rows = 10_000
        edge.bar_readback = object()
        edge.clock = lambda: 1.0
        edge._settled_observed_ms = lambda: 5_000_000
        edge.history_end_ms = None
        edge._rebase_if_canonical_cache_generation_changed = lambda: False
        edge._rebase_changed_products = lambda: ()
        available = {"a": 10_000, "b": 7, "c": 0}
        self.fetched, self.published = [], []

        self.observed = []

        def fetch(source, _acquisition, *, rows, observed_ms, allow_short=False):
            self.observed.append(observed_ms)
            self.fetched.append((source.binding_id, rows, allow_short))
            return tuple(range(min(rows, available[source.binding_id])))

        def publish(source, _acquisition, values, *, expected_rows):
            self.published.append((source.binding_id, expected_rows))
            edge._last_open_ms[source.binding_id] = 1
            return len(values)

        edge._fetch_history = fetch
        edge._publish_history = publish
        type(edge)._binding_ids = property(lambda self: ("a", "b", "c"))
        return edge

    def test_depth_by_demand_and_short_or_empty_history_are_reported(self):
        edge = self._edge(demand={"a": 5000, "b": 10_000, "c": 0}, gate=None)
        edge.bootstrap_history()
        self.assertEqual(self.fetched, [("a", 5000, True), ("b", 10_000, True), ("c", 1, True)])
        self.assertEqual(self.published, [("a", 5000), ("b", 7)])
        self.assertEqual(edge._history_short, {"b": (10_000, 7), "c": (1, 0)})
        self.assertTrue(edge._history_bootstrapped)
        edge._history_bootstrapped = False
        self.fetched.clear()
        edge.bootstrap_history()
        self.assertEqual(self.fetched, [], "nothing is fetched again: no retry of absent history")

    def test_a_closed_gate_pauses_history_without_failing_and_resumes(self):
        state = {"open": True, "calls": 0}

        def gate():
            state["calls"] += 1
            return (state["open"], {"backlog": {}})

        edge = self._edge(demand=None, gate=gate)
        original = edge._fetch_history

        def fetch_then_close(source, acquisition, **kwargs):
            if source.binding_id == "a":
                state["open"] = False  # the first binding's publish fills the backlog
            return original(source, acquisition, **kwargs)

        edge._fetch_history = fetch_then_close
        self.assertEqual(edge.bootstrap_history(), 10_000)
        self.assertFalse(edge._history_bootstrapped)
        self.assertEqual([item[0] for item in self.published], ["a"])
        state["open"] = True
        edge.bootstrap_history()
        self.assertEqual([item[0] for item in self.published], ["a", "b"])
        self.assertTrue(edge._history_bootstrapped)


class HistoryLiveJoinTests(EdgeFillTests):
    """KN-4 D47-4: history ends where the live log starts; a history-only
    edge leaves every live bar to that log."""

    def test_history_holds_only_bars_closed_before_the_live_start(self):
        edge = self._edge(demand=None, gate=None)
        edge.history_end_ms = 4_000_000
        edge.bootstrap_history()
        self.assertEqual(set(self.observed), {4_000_000})
        edge = self._edge(demand=None, gate=None)
        edge.history_end_ms = 9_000_000  # a later bound never moves the fill past "now"
        edge.bootstrap_history()
        self.assertEqual(set(self.observed), {5_000_000})

    def test_a_history_only_edge_runs_no_live_poll_or_native_recovery(self):
        import time
        from pathlib import Path

        from qdl.runtime.stable_bar_edge import StableBinanceBarEdge
        from qdl.runtime.stable_catalog import StableSourceCatalog
        from qdl.runtime.stable_deployment import StableAcquisitionPlan, stable_authority_record

        root = Path(__file__).resolve().parents[1]
        catalog = StableSourceCatalog.load(root / "config/v2/stable-source-bindings.yaml")
        acquisition = StableAcquisitionPlan.load(root / "config/v2/stable-acquisition-bindings.yaml", catalog=catalog)
        authority = stable_authority_record(
            rust_image_digest="a" * 64, capability_manifest=root / "config/v2/stable-capabilities.yaml",
            contract=root / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=(root / "config/v2/stable-acquisition-bindings.yaml").read_bytes(),
            effective_at_ns=time.time_ns())

        class Publisher:
            def publish_many(self, values):
                return tuple(range(len(tuple(values))))

        def edge(**kwargs):
            return StableBinanceBarEdge(catalog=catalog, acquisition=acquisition, authority=authority,
                                        publisher=Publisher(), warmup_rows=2, clock=lambda: 180.0, **kwargs)

        full = edge()
        self.assertTrue(full._rest_fallback_active and full._native_recovery_active)
        only = edge(history_only=True, history_end_ms=1_790_000_000_000)
        self.assertEqual((only._rest_fallback_active, only._native_recovery_active), (False, False))
        self.assertTrue(only._history_bootstrap_active)
        with self.assertRaises(ValueError):
            edge(history_end_ms=0)


@unittest.skipUnless(os.environ.get("QDL_KN_TEST_KAFKA"), "SKIPPED LOUDLY: QDL_KN_TEST_KAFKA is not set")
class BackpressureKafkaTests(unittest.TestCase):
    def test_group_backlog_is_read_from_a_real_broker(self):
        from confluent_kafka import Consumer, Producer, TopicPartition
        from confluent_kafka.admin import AdminClient, NewTopic

        from qdl.runtime.history_backpressure import kafka_backlog_from_config

        bootstrap = os.environ["QDL_KN_TEST_KAFKA"]
        topic, group = f"kn4-bp-{uuid.uuid4().hex[:8]}", f"kn4-bp-g-{uuid.uuid4().hex[:8]}"
        admin = AdminClient({"bootstrap.servers": bootstrap})
        for future in admin.create_topics([NewTopic(topic, 2, 1)]).values():
            future.result(20)
        try:
            producer = Producer({"bootstrap.servers": bootstrap})
            for index in range(30):
                producer.produce(topic, value=b"x", partition=index % 2)
            producer.flush(20)
            gate = kafka_backlog_from_config(bootstrap, [Stage("a", group, topic, 20)])
            self.assertEqual(gate.backlog(), {"a": 30})
            self.assertFalse(gate()[0])
            consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
            consumer.commit(offsets=[TopicPartition(topic, 0, 15), TopicPartition(topic, 1, 5)], asynchronous=False)
            consumer.close()
            self.assertEqual(gate.backlog(), {"a": 10})
            self.assertTrue(gate()[0])
        finally:
            for future in admin.delete_topics([topic]).values():
                future.result(20)


if __name__ == "__main__":
    unittest.main()
