from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import yaml
from unittest.mock import AsyncMock, patch

from qdl.common.v1 import common_pb2
from qdl.adapters.binance.bar_edge import (
    BinanceBarRawBinding,
    fetch_closed_bar_history_raw_envelopes as fetch_binance_history,
)
from qdl.adapters.okx.bar_edge import (
    OkxBarRawBinding,
    fetch_closed_bar_history_raw_envelopes as fetch_okx_history,
    fetch_latest_closed_bar_raw_envelope as fetch_okx_latest,
)
from qdl.adapters.okx.history import (
    HistoryCoverage,
    OkxCandle,
    OkxCandleHistory,
)
from qdl.adapters.intervals import (
    canonical_interval_ms,
    provider_bar_calendar_anchor_ms,
)
from qdl.canonical.market import canonicalize_okx_bar
from qdl.canonical.trade import TradeContext
from qdl.marketdata.v2 import market_data_pb2
from qdl.runtime.stable_bar_edge import (
    StableBinanceBarEdge,
    _canonical_cache_id,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    stable_authority_record,
)
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig


ROOT = Path(__file__).resolve().parents[1]


def _legacy_rest_bar_fallback(
    acquisition: StableAcquisitionPlan,
) -> StableAcquisitionPlan:
    """Make an explicit test-only REST fallback plan for edge recovery tests."""
    rest_kind = {
        "binance_usdm_bar": "binance_usdm_rest_bar",
        "binance_spot_bar": "binance_spot_rest_bar",
        "okx_bar": "okx_bar",
    }
    return StableAcquisitionPlan(
        schema=acquisition.schema,
        revision=acquisition.revision,
        raw_topic=acquisition.raw_topic,
        canonical_topic=acquisition.canonical_topic,
        quarantine_topic=acquisition.quarantine_topic,
        bindings=tuple(
            replace(
                item,
                mode="PYTHON_REST",
                provider_kind=rest_kind[item.provider_kind],
                websocket_url=None,
                business_websocket_url=None,
            )
            if item.provider_kind in rest_kind
            else item
            for item in acquisition.bindings
        ),
    )


def _binance_binding() -> BinanceBarRawBinding:
    return BinanceBarRawBinding(
        market="USDM",
        product_type="PERPETUAL",
        native_symbol="BTCUSDT",
        interval="1m",
        subscription_id="binance-usdm-bar",
        source_session_id="history-session",
        connection_generation=1,
        lease_epoch=1,
        authority_revision=1,
        partition_plan_epoch=1,
        adapter_version="binance-rest/2.0.0",
        config_revision=1,
        instrument_catalog_revision=1,
    )


def _binance_row(open_time: int) -> list:
    return [
        open_time, "100", "101", "99", "100.5", "10",
        open_time + 59_999, "1005", 7, "4", "402", "0",
    ]


def _okx_binding() -> OkxBarRawBinding:
    return OkxBarRawBinding(
        market="SWAP",
        product_type="PERPETUAL",
        native_symbol="BTC-USDT-SWAP",
        interval="1m",
        subscription_id="okx-swap-bar",
        source_session_id="history-session",
        connection_generation=1,
        lease_epoch=1,
        authority_revision=1,
        partition_plan_epoch=1,
        adapter_version="okx-v5/2.0.0",
        config_revision=1,
        instrument_catalog_revision=1,
    )


class BarHistoryAdapterTests(unittest.TestCase):
    def test_binance_history_preserves_native_rows_and_strict_continuity(self):
        rows = [_binance_row(value) for value in (60_000, 120_000, 180_000)]
        calls = []

        def fetcher(*args, **kwargs):
            calls.append((args, kwargs))
            return {"data": rows}

        values = fetch_binance_history(
            _binance_binding(),
            limit=3,
            now_ms=240_000,
            fetcher=fetcher,
            sleep=lambda _seconds: None,
        )
        self.assertEqual(len(values), 3)
        self.assertEqual(len(calls), 1)
        payloads = [json.loads(item.raw_frame_bytes) for item in values]
        self.assertEqual([item["row"] for item in payloads], rows)
        self.assertEqual({item["bar_origin"] for item in payloads}, {"BACKFILLED"})
        self.assertTrue(all(not item.test_provenance for item in values))

        with self.assertRaisesRegex(RuntimeError, "time gap"):
            fetch_binance_history(
                _binance_binding(),
                limit=2,
                now_ms=240_000,
                fetcher=lambda *_args, **_kwargs: {
                    "data": [_binance_row(60_000), _binance_row(180_000)]
                },
                sleep=lambda _seconds: None,
            )

    def test_binance_max_window_excludes_the_open_candle_at_provider_boundary(self):
        interval_ms = 60_000
        observed_ms = 1_002 * interval_ms + 12_345
        last_closed_open = 1_001 * interval_ms
        rows = [
            _binance_row(last_closed_open - (999 - index) * interval_ms)
            for index in range(1_000)
        ]
        calls = []

        def fetcher(*_args, **kwargs):
            calls.append(kwargs)
            return {"data": rows}

        values = fetch_binance_history(
            _binance_binding(),
            limit=1_000,
            now_ms=observed_ms,
            fetcher=fetcher,
            sleep=lambda _seconds: None,
        )
        self.assertEqual(len(values), 1_000)
        self.assertEqual(calls[0]["limit"], 1_000)
        self.assertEqual(calls[0]["end_time"], 1_002 * interval_ms - 1)

    def test_binance_history_pages_to_the_declared_v2_maximum(self):
        interval_ms = 60_000
        observed_ms = 10_002 * interval_ms + 123
        calls = []

        def fetcher(*_args, **kwargs):
            calls.append(kwargs)
            last_open = int(kwargs["end_time"]) - 59_999
            first_open = last_open - (int(kwargs["limit"]) - 1) * interval_ms
            return {
                "data": [
                    _binance_row(first_open + index * interval_ms)
                    for index in range(int(kwargs["limit"]))
                ]
            }

        values = fetch_binance_history(
            _binance_binding(),
            limit=10_000,
            now_ms=observed_ms,
            fetcher=fetcher,
            sleep=lambda _seconds: None,
        )
        self.assertEqual(len(values), 10_000)
        self.assertEqual([call["limit"] for call in calls], [1_000] * 10)
        self.assertEqual(calls[0]["end_time"], 10_002 * interval_ms - 1)
        payloads = [json.loads(item.raw_frame_bytes) for item in values]
        self.assertEqual(payloads[0]["row"][0], 2 * interval_ms)
        self.assertEqual(payloads[-1]["row"][0], 10_001 * interval_ms)
        with self.assertRaisesRegex(ValueError, "between 1 and 10000"):
            fetch_binance_history(
                _binance_binding(),
                limit=10_001,
                now_ms=observed_ms,
                fetcher=fetcher,
                sleep=lambda _seconds: None,
            )

    def test_binance_history_rejects_a_repeated_page(self):
        interval_ms = 60_000
        observed_ms = 1_002 * interval_ms + 123
        rows = [
            _binance_row((2 + index) * interval_ms)
            for index in range(1_000)
        ]
        with self.assertRaisesRegex(RuntimeError, "exceeds requested end boundary"):
            fetch_binance_history(
                _binance_binding(),
                limit=1_001,
                now_ms=observed_ms,
                # Ignore the requested cursor but still honour the requested
                # page length. The second one-row page is therefore valid in
                # shape yet outside its requested end boundary.
                fetcher=lambda *_args, **kwargs: {
                    "data": rows[-int(kwargs["limit"]):]
                },
                sleep=lambda _seconds: None,
            )

    def test_okx_history_preserves_confirmed_native_rows_and_rejects_partial(self):
        start = 60_000
        records = tuple(
            OkxCandle(
                inst_id="BTC-USDT-SWAP",
                bar="1m",
                price_type="TRADE",
                open_ts_ms=start + index * 60_000,
                open="100",
                high="101",
                low="99",
                close="100.5",
                volume_raw="10",
                volume_ccy_raw="0.1",
                volume_quote_raw="1005",
                confirmed=True,
            )
            for index in range(3)
        )
        coverage = HistoryCoverage(
            requested_start_ms=start,
            requested_end_ms=239_999,
            observed_min_ts_ms=start,
            observed_max_ts_ms=180_000,
            complete_left=True,
            complete_right=True,
            truncated=False,
            terminal_reason="REACHED_REQUEST_START",
            provider_endpoint="/api/v5/market/history-candles",
        )

        class Client:
            async def candles(self, **_kwargs):
                return OkxCandleHistory(records, coverage)

        values = asyncio.run(fetch_okx_history(
            _okx_binding(),
            limit=3,
            now_ms=240_000,
            history_client=Client(),
        ))
        payloads = [json.loads(item.raw_frame_bytes) for item in values]
        self.assertEqual([int(item["data"][0][0]) for item in payloads], [60_000, 120_000, 180_000])
        self.assertEqual({item["arg"]["channel"] for item in payloads}, {"candle1m"})
        self.assertTrue(all(not item.test_provenance for item in values))

        partial = HistoryCoverage(
            requested_start_ms=start,
            requested_end_ms=239_999,
            observed_min_ts_ms=120_000,
            observed_max_ts_ms=180_000,
            complete_left=False,
            complete_right=True,
            truncated=False,
            terminal_reason="PROVIDER_EXHAUSTED",
            provider_endpoint="/api/v5/market/history-candles",
        )

        class Partial:
            async def candles(self, **_kwargs):
                return OkxCandleHistory(records[1:], partial)

        # The refusal now names the cause; partial coverage is not a short
        # window, so the message says which check failed.
        with self.assertRaisesRegex(RuntimeError, "coverage is PARTIAL"):
            asyncio.run(fetch_okx_history(
                _okx_binding(),
                limit=3,
                now_ms=240_000,
                history_client=Partial(),
            ))

    def test_okx_history_uses_an_exclusive_provider_page_boundary(self):
        records = tuple(
            OkxCandle(
                inst_id="BTC-USDT-SWAP",
                bar="1m",
                price_type="TRADE",
                open_ts_ms=300_000 + index * 60_000,
                open="100",
                high="101",
                low="99",
                close="100.5",
                volume_raw="10",
                volume_ccy_raw="0.1",
                volume_quote_raw="1005",
                confirmed=True,
            )
            for index in range(3)
        )
        coverage = HistoryCoverage(
            requested_start_ms=299_999,
            requested_end_ms=599_999,
            observed_min_ts_ms=300_000,
            observed_max_ts_ms=420_000,
            complete_left=True,
            complete_right=True,
            truncated=False,
            terminal_reason="REACHED_REQUEST_START",
            provider_endpoint="/api/v5/market/history-candles",
        )

        class Client:
            calls = []

            async def candles(self, **kwargs):
                self.calls.append(kwargs)
                return OkxCandleHistory(records, coverage)

        client = Client()
        asyncio.run(fetch_okx_history(
            _okx_binding(),
            limit=3,
            now_ms=600_000,
            history_client=client,
        ))
        self.assertEqual(client.calls[0]["end_ms"], 599_999)
        self.assertEqual(client.calls[0]["start_ms"], 299_999)

    def test_okx_latest_closed_bar_retries_provisional_provider_state(self):
        sentinel = object()
        sleeps = []

        async def no_wait(delay):
            sleeps.append(delay)

        with patch(
            "qdl.adapters.okx.bar_edge.fetch_closed_bar_history_raw_envelopes",
            new_callable=AsyncMock,
            side_effect=(RuntimeError("provisional"), (sentinel,)),
        ) as fetch:
            result = asyncio.run(fetch_okx_latest(
                _okx_binding(), attempts=2, sleep=no_wait
            ))
        self.assertIs(result, sentinel)
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual(sleeps, [0.5])

        with patch(
            "qdl.adapters.okx.bar_edge.fetch_closed_bar_history_raw_envelopes",
            new_callable=AsyncMock,
            side_effect=RuntimeError("still provisional"),
        ):
            with self.assertRaisesRegex(RuntimeError, "exhausted attempts=2"):
                asyncio.run(fetch_okx_latest(
                    _okx_binding(), attempts=2, sleep=no_wait
                ))

    def test_okx_final_bar_event_identity_is_transport_restart_and_config_independent(self):
        fixture = json.loads(
            (ROOT / "tests/fixtures/phase2/okx_bar.json").read_text(encoding="utf-8")
        )
        first_context = TradeContext(**fixture["context"])
        second_values = dict(fixture["context"])
        second_values["partition_sequence"] += 99
        second_values["config_revision"] += 1
        second_context = TradeContext(**second_values)
        first = canonicalize_okx_bar(fixture["raw"], first_context)
        second = canonicalize_okx_bar(fixture["raw"], second_context)
        self.assertEqual(first.source_sequence, "1786352340000:1")
        self.assertEqual(first.source_sequence, second.source_sequence)
        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(first.canonical_payload_hash, second.canonical_payload_hash)


class StableBarBootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog_path = ROOT / "config/v2/stable-source-bindings.yaml"
        self.acquisition_path = ROOT / "config/v2/stable-acquisition-bindings.yaml"
        self.catalog = StableSourceCatalog.load(self.catalog_path)
        self.acquisition = StableAcquisitionPlan.load(
            self.acquisition_path, catalog=self.catalog
        )
        self.authority = stable_authority_record(
            rust_image_digest="a" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=self.acquisition_path.read_bytes(),
            effective_at_ns=time.time_ns(),
        )

    class _NoopPublisher:
        def publish_many(self, values):
            return tuple(range(len(tuple(values))))

    @staticmethod
    def _cached_final_bar(
        spool: SQLiteDurableSpool,
        source,
        *,
        open_ms: int,
        source_id: str | None = None,
        venue: str | None = None,
    ) -> None:
        identity = source.instrument.identity
        event = market_data_pb2.EventEnvelope(
            schema_name="qdl.marketdata.bar",
            schema_major=2,
            event_id=hashlib.sha256(
                f"cached-bar:{source.binding_id}:{open_ms}:{source_id or source.source_id}".encode()
            ).digest()[:16],
            instrument_uid=source.instrument.instrument_uid,
            instrument_id=source.instrument.instrument_id,
            instrument_revision=source.instrument.metadata_revision,
            venue=venue or identity.venue,
            market=identity.market,
            product_type=identity.product_type.value,
            native_symbol=source.instrument.native_symbol,
            provider=source.provider,
            source_id=source_id or source.source_id,
            source_role=getattr(common_pb2, f"SOURCE_ROLE_{source.source_role}"),
        )
        event.bar.interval = source.interval or ""
        event.bar.open_time_ns = open_ms * 1_000_000
        event.bar.close_time_ns = (open_ms + canonical_interval_ms(source.interval or "")) * 1_000_000 - 1
        event.bar.is_final = True
        event.bar.lifecycle = market_data_pb2.BAR_LIFECYCLE_FINAL
        spool.append(DurableEvent(
            stream=source.canonical_stream,
            partition_key=source.partition_key,
            event_id=bytes(event.event_id),
            payload=event.SerializeToString(deterministic=True),
            accepted_at_ns=max(1, event.bar.close_time_ns),
        ))

    def _cached_edge(self, directory: str, publisher):
        cache_path = Path(directory) / "canonical-cache.sqlite3"
        spool = SQLiteDurableSpool(SpoolConfig(
            path=cache_path,
            max_records=100,
            max_payload_bytes=1_000_000,
            max_event_bytes=64_000,
            max_storage_bytes=2_000_000,
            min_free_disk_bytes=0,
        ))
        edge = StableBinanceBarEdge(
            catalog=self.catalog,
            acquisition=self.acquisition,
            authority=self.authority,
            publisher=publisher,
            warmup_rows=2,
            canonical_cache_id=_canonical_cache_id(cache_path),
            canonical_cache_path=cache_path,
            clock=lambda: 180.0,
        )
        return spool, edge

    def test_cache_overlap_keeps_durable_final_and_publishes_only_missing_history(self):
        class Envelope:
            def __init__(self, open_ms: int):
                self.raw_frame_bytes = json.dumps({"row": _binance_row(open_ms)}).encode()

        class Publisher:
            def __init__(self):
                self.batches = []

            def publish_many(self, values):
                batch = tuple(values)
                self.batches.append(batch)
                return tuple(range(len(batch)))

        with tempfile.TemporaryDirectory(prefix="qdl-stable-overlap-") as directory:
            publisher = Publisher()
            spool, edge = self._cached_edge(directory, publisher)
            try:
                source, acquisition = next(
                    pair for pair in edge.history_bindings
                    if pair[0].instrument.native_symbol == "BTCUSDT"
                    and pair[0].interval == "1m"
                )
                self._cached_final_bar(spool, source, open_ms=120_000)
                published = edge._publish_history(
                    source,
                    acquisition,
                    (Envelope(60_000), Envelope(120_000)),
                    expected_rows=2,
                )
                self.assertEqual(published, 1)
                self.assertEqual(len(publisher.batches), 1)
                self.assertEqual(
                    [json.loads(item.raw_frame_bytes)["row"][0] for item in publisher.batches[0]],
                    [60_000],
                )
                self.assertEqual(edge._last_open_ms[source.binding_id], 120_000)
            finally:
                spool.close()

    def test_cache_exact_overlap_is_idempotent_without_a_new_kafka_publish(self):
        class Envelope:
            def __init__(self, open_ms: int):
                self.raw_frame_bytes = json.dumps({"row": _binance_row(open_ms)}).encode()

        class Publisher:
            def publish_many(self, _values):
                raise AssertionError("covered durable BARs must not be republished")

        with tempfile.TemporaryDirectory(prefix="qdl-stable-overlap-") as directory:
            spool, edge = self._cached_edge(directory, Publisher())
            try:
                source, acquisition = next(
                    pair for pair in edge.history_bindings
                    if pair[0].instrument.native_symbol == "BTCUSDT"
                    and pair[0].interval == "1m"
                )
                self._cached_final_bar(spool, source, open_ms=60_000)
                self._cached_final_bar(spool, source, open_ms=120_000)
                self.assertEqual(
                    edge._publish_history(
                        source,
                        acquisition,
                        (Envelope(60_000), Envelope(120_000)),
                        expected_rows=2,
                    ),
                    0,
                )
                self.assertEqual(edge._last_open_ms[source.binding_id], 120_000)
            finally:
                spool.close()

    def test_cache_binding_mismatch_fails_closed_before_publish(self):
        class Envelope:
            raw_frame_bytes = json.dumps({"row": _binance_row(60_000)}).encode()

        class Publisher:
            def publish_many(self, _values):
                raise AssertionError("invalid durable partition must not publish")

        with tempfile.TemporaryDirectory(prefix="qdl-stable-overlap-") as directory:
            spool, edge = self._cached_edge(directory, Publisher())
            try:
                source, acquisition = next(
                    pair for pair in edge.history_bindings
                    if pair[0].instrument.native_symbol == "BTCUSDT"
                    and pair[0].interval == "1m"
                )
                self._cached_final_bar(
                    spool, source, open_ms=60_000, source_id="wrong-source"
                )
                with self.assertRaisesRegex(RuntimeError, "partition differs"):
                    edge._publish_history(
                        source, acquisition, (Envelope(),), expected_rows=1
                    )
            finally:
                spool.close()

    def test_cache_identity_mismatch_fails_closed_before_publish(self):
        class Envelope:
            raw_frame_bytes = json.dumps({"row": _binance_row(60_000)}).encode()

        class Publisher:
            def publish_many(self, _values):
                raise AssertionError("invalid durable identity must not publish")

        with tempfile.TemporaryDirectory(prefix="qdl-stable-overlap-") as directory:
            spool, edge = self._cached_edge(directory, Publisher())
            try:
                source, acquisition = next(
                    pair for pair in edge.history_bindings
                    if pair[0].instrument.native_symbol == "BTCUSDT"
                    and pair[0].interval == "1m"
                )
                self._cached_final_bar(
                    spool, source, open_ms=60_000, venue="WRONG_VENUE"
                )
                with self.assertRaisesRegex(RuntimeError, "partition differs"):
                    edge._publish_history(
                        source, acquisition, (Envelope(),), expected_rows=1
                    )
            finally:
                spool.close()

    def test_cache_generation_change_fails_closed_before_bootstrap_publish(self):
        class Envelope:
            raw_frame_bytes = json.dumps({"row": _binance_row(60_000)}).encode()

        class Publisher:
            def publish_many(self, _values):
                raise AssertionError("changed cache generation must not publish")

        with tempfile.TemporaryDirectory(prefix="qdl-stable-overlap-") as directory:
            spool, edge = self._cached_edge(directory, Publisher())
            try:
                source, acquisition = next(
                    pair for pair in edge.history_bindings
                    if pair[0].instrument.native_symbol == "BTCUSDT"
                    and pair[0].interval == "1m"
                )
                with patch(
                    "qdl.runtime.stable_bar_edge._canonical_cache_id",
                    side_effect=(edge.canonical_cache_id, "e" * 32),
                ), self.assertRaisesRegex(RuntimeError, "generation changed"):
                    edge._publish_history(
                        source, acquisition, (Envelope(),), expected_rows=1
                    )
            finally:
                spool.close()

    def test_cache_generation_change_after_ack_does_not_advance_watermark(self):
        class Envelope:
            raw_frame_bytes = json.dumps({"row": _binance_row(60_000)}).encode()

        class Publisher:
            def __init__(self):
                self.batches = []

            def publish_many(self, values):
                batch = tuple(values)
                self.batches.append(batch)
                return tuple(range(len(batch)))

        with tempfile.TemporaryDirectory(prefix="qdl-stable-overlap-") as directory:
            publisher = Publisher()
            spool, edge = self._cached_edge(directory, publisher)
            try:
                source, acquisition = next(
                    pair for pair in edge.history_bindings
                    if pair[0].instrument.native_symbol == "BTCUSDT"
                    and pair[0].interval == "1m"
                )
                with patch(
                    "qdl.runtime.stable_bar_edge._canonical_cache_id",
                    side_effect=(
                        edge.canonical_cache_id,
                        edge.canonical_cache_id,
                        "e" * 32,
                    ),
                ), self.assertRaisesRegex(RuntimeError, "generation changed"):
                    edge._publish_history(
                        source, acquisition, (Envelope(),), expected_rows=1
                    )
                self.assertEqual(len(publisher.batches), 1)
                self.assertEqual(edge._last_open_ms, {})
            finally:
                spool.close()

    def test_cache_short_kafka_ack_does_not_advance_watermark(self):
        class Envelope:
            raw_frame_bytes = json.dumps({"row": _binance_row(60_000)}).encode()

        class Publisher:
            def publish_many(self, _values):
                return ()

        with tempfile.TemporaryDirectory(prefix="qdl-stable-overlap-") as directory:
            spool, edge = self._cached_edge(directory, Publisher())
            try:
                source, acquisition = next(
                    pair for pair in edge.history_bindings
                    if pair[0].instrument.native_symbol == "BTCUSDT"
                    and pair[0].interval == "1m"
                )
                with self.assertRaisesRegex(RuntimeError, "every Kafka ACK"):
                    edge._publish_history(
                        source, acquisition, (Envelope(),), expected_rows=1
                    )
                self.assertEqual(edge._last_open_ms, {})
            finally:
                spool.close()

    def test_bootstrap_publishes_multi_symbol_real_provider_batches_once(self):
        class Envelope:
            def __init__(self, venue: str, open_time: int):
                payload = (
                    {"row": _binance_row(open_time)}
                    if venue == "BINANCE"
                    else {"data": [[str(open_time), "1", "1", "1", "1", "1", "1", "1", "1"]]}
                )
                self.raw_frame_bytes = json.dumps(payload).encode()

        class Publisher:
            def __init__(self):
                self.batches = []

            def publish_many(self, values):
                batch = tuple(values)
                self.batches.append(batch)
                return tuple(range(len(batch)))

        publisher = Publisher()
        edge = StableBinanceBarEdge(
            catalog=self.catalog,
            acquisition=self.acquisition,
            authority=self.authority,
            publisher=publisher,
            warmup_rows=2,
            clock=lambda: 180.0,
        )
        with patch(
            "qdl.runtime.stable_bar_edge.fetch_binance_history",
            return_value=(Envelope("BINANCE", 60_000), Envelope("BINANCE", 120_000)),
        ), patch(
            "qdl.runtime.stable_bar_edge.fetch_okx_history",
            new_callable=AsyncMock,
            return_value=(Envelope("OKX", 60_000), Envelope("OKX", 120_000)),
        ):
            expected_bindings = (
                len(edge.history_bindings) + len(edge.history_okx_bindings)
            )
            self.assertEqual(edge.bootstrap_history(), expected_bindings * 2)
            self.assertEqual(edge.bootstrap_history(), 0)
        self.assertEqual([len(item) for item in publisher.batches], [2] * expected_bindings)
        self.assertEqual(len(edge._last_open_ms), expected_bindings)
        self.assertTrue(edge._history_bootstrapped)

    def test_checkpoint_accepts_provider_calendar_anchored_multiday_watermarks(self):
        with tempfile.TemporaryDirectory(prefix="qdl-stable-calendar-checkpoint-") as raw:
            state_path = Path(raw) / "bar-edge.json"
            seed = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=self._NoopPublisher(),
                warmup_rows=2,
                generation_clock_ns=lambda: 1,
            )
            source = next(
                item
                for item, _ in seed.history_bindings
                if item.interval == "3d" and item.instrument.identity.venue == "BINANCE"
            )
            payload = seed._state_identity_payload(schema="qdl.stable-bar-edge-state.v3")
            payload["connection_generation"] = 1
            payload["last_open_ms"] = {source.binding_id: 1_787_529_600_000}
            state_path.write_text(json.dumps(payload), encoding="utf-8")

            restored = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=self._NoopPublisher(),
                warmup_rows=2,
                state_path=state_path,
                generation_clock_ns=lambda: 2,
            )
            self.assertEqual(restored._last_open_ms[source.binding_id], 1_787_529_600_000)
            self.assertEqual(restored.connection_generation, 2)

    def test_checkpoint_rejects_wrong_provider_multiday_anchor(self):
        with tempfile.TemporaryDirectory(prefix="qdl-stable-calendar-checkpoint-") as raw:
            state_path = Path(raw) / "bar-edge.json"
            seed = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=self._NoopPublisher(),
                warmup_rows=2,
                generation_clock_ns=lambda: 1,
            )
            source = next(
                item
                for item, _ in seed.history_bindings
                if item.interval == "3d" and item.instrument.identity.venue == "BINANCE"
            )
            payload = seed._state_identity_payload(schema="qdl.stable-bar-edge-state.v3")
            payload["connection_generation"] = 1
            payload["last_open_ms"] = {source.binding_id: 1_787_702_400_000}
            state_path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "checkpoint watermark"):
                StableBinanceBarEdge(
                    catalog=self.catalog,
                    acquisition=self.acquisition,
                    authority=self.authority,
                    publisher=self._NoopPublisher(),
                    warmup_rows=2,
                    state_path=state_path,
                    generation_clock_ns=lambda: 2,
                )

    def test_production_final_bars_bootstrap_all_and_poll_explicit_rest_only(self):
        class Envelope:
            def __init__(self, venue: str, open_time: int):
                payload = (
                    {"row": _binance_row(open_time)}
                    if venue == "BINANCE"
                    else {"data": [[str(open_time), "1", "1", "1", "1", "1", "1", "1", "1"]]}
                )
                self.raw_frame_bytes = json.dumps(payload).encode()

        class Publisher:
            def __init__(self):
                self.batches = []

            def publish_many(self, values):
                batch = tuple(values)
                self.batches.append(batch)
                return tuple(range(len(batch)))

        edge = StableBinanceBarEdge(
            catalog=self.catalog,
            acquisition=self.acquisition,
            authority=self.authority,
            publisher=Publisher(),
            warmup_rows=2,
            clock=lambda: 180.0,
        )
        history_ids = {
            source.binding_id
            for source, _acquisition in edge.history_bindings + edge.history_okx_bindings
        }
        poll_ids = {
            source.binding_id
            for source, _acquisition in edge.bindings + edge.okx_bindings
        }
        self.assertEqual(
            history_ids,
            {
                item.binding_id
                for item in self.acquisition.bindings
                if item.enabled
                and item.runtime in {"BINANCE", "OKX"}
                and next(
                    source for source in self.catalog.bindings
                    if source.binding_id == item.binding_id
                ).feed.value == "BAR"
            },
        )
        expected_poll_ids = {
            item.binding_id
            for item in self.acquisition.bindings
            if item.enabled
            and item.mode == "PYTHON_REST"
            and item.runtime in {"BINANCE", "OKX"}
            and next(
                source for source in self.catalog.bindings
                if source.binding_id == item.binding_id
            ).feed.value == "BAR"
        }
        self.assertTrue(any(value.startswith("okx-swap-") for value in history_ids))
        self.assertEqual(poll_ids, expected_poll_ids)
        self.assertTrue(poll_ids <= history_ids)
        with patch(
            "qdl.runtime.stable_bar_edge.fetch_binance_history",
            return_value=(Envelope("BINANCE", 60_000), Envelope("BINANCE", 120_000)),
        ), patch(
            "qdl.runtime.stable_bar_edge.fetch_okx_history",
            new_callable=AsyncMock,
            return_value=(Envelope("OKX", 60_000), Envelope("OKX", 120_000)),
        ):
            self.assertEqual(edge.bootstrap_history(), len(history_ids) * 2)
        self.assertTrue(edge._history_bootstrapped)

    def test_deployed_bootstrap_depth_covers_registered_crypto_bar_demand(self):
        compose = yaml.safe_load(
            (ROOT / "docker-compose.v2-stable.yml").read_text(encoding="utf-8")
        )
        configured = int(
            compose["services"]["binance_bar_edge"]["environment"]
            ["QDL_STABLE_BAR_WARMUP_ROWS"]
        )
        catchup = int(
            compose["services"]["binance_bar_edge"]["environment"]
            ["QDL_STABLE_BAR_MAX_CATCHUP_ROWS"]
        )
        source_by_id = {item.binding_id: item for item in self.catalog.bindings}
        history_uids = {
            source_by_id[item.binding_id].instrument.instrument_uid
            for item in self.acquisition.bindings
            if item.enabled
            and item.runtime in {"BINANCE", "OKX"}
            and source_by_id[item.binding_id].feed.value == "BAR"
        }
        manifest_paths = compose["x-stable-env"][
            "QDL_STABLE_CONSUMER_MANIFESTS"
        ].split(":")
        declared = []
        for container_path in manifest_paths:
            relative = Path(container_path).relative_to("/app")
            manifest = yaml.safe_load((ROOT / relative).read_text(encoding="utf-8"))
            declared.extend(
                int(item["warmup_limit"])
                for item in manifest["spec"]["requirements"]
                if item["feed"] == "BAR"
                and item["instrument_uid"] in history_uids
            )
        self.assertEqual(max(declared), 10_000)
        self.assertGreaterEqual(configured, max(declared))
        self.assertGreaterEqual(catchup, max(declared))

    def test_stable_bar_edge_accepts_the_public_10000_row_bound(self):
        edge = StableBinanceBarEdge(
            catalog=self.catalog,
            acquisition=self.acquisition,
            authority=self.authority,
            publisher=self._NoopPublisher(),
            warmup_rows=10_000,
            max_catchup_rows=10_000,
        )
        self.assertEqual(edge.warmup_rows, 10_000)
        self.assertEqual(edge.max_catchup_rows, 10_000)
        with self.assertRaisesRegex(ValueError, "between 1 and 10000"):
            StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=self._NoopPublisher(),
                warmup_rows=10_001,
            )

    def test_durable_ack_watermark_skips_overlapping_restart_bootstrap(self):
        class Envelope:
            def __init__(self, venue: str, open_time: int):
                payload = (
                    {"row": _binance_row(open_time)}
                    if venue == "BINANCE"
                    else {"data": [[str(open_time), "1", "1", "1", "1", "1", "1", "1", "1"]]}
                )
                self.raw_frame_bytes = json.dumps(payload).encode()

        class Publisher:
            def __init__(self):
                self.batches = []

            def publish_many(self, values):
                batch = tuple(values)
                self.batches.append(batch)
                return tuple(range(len(batch)))

        with tempfile.TemporaryDirectory(prefix="qdl-stable-bar-state-") as directory:
            state_path = Path(directory) / "bar-edge.json"
            first_publisher = Publisher()
            first = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=first_publisher,
                warmup_rows=2,
                state_path=state_path,
                clock=lambda: 180.0,
                generation_clock_ns=lambda: 100,
            )
            def opens(interval: str, provider: str) -> tuple[int, int]:
                duration = canonical_interval_ms(interval)
                anchor = provider_bar_calendar_anchor_ms(interval, provider=provider)
                latest = anchor + 100 * duration
                return latest - duration, latest

            def binance_history(binding, **_kwargs):
                first_open, latest_open = opens(binding.interval, "BINANCE")
                return (
                    Envelope("BINANCE", first_open),
                    Envelope("BINANCE", latest_open),
                )

            async def okx_history(binding, **_kwargs):
                first_open, latest_open = opens(binding.interval, "OKX")
                return (
                    Envelope("OKX", first_open),
                    Envelope("OKX", latest_open),
                )

            with patch(
                "qdl.runtime.stable_bar_edge.fetch_binance_history",
                side_effect=binance_history,
            ), patch(
                "qdl.runtime.stable_bar_edge.fetch_okx_history",
                side_effect=okx_history,
            ):
                expected_bindings = (
                    len(first.history_bindings) + len(first.history_okx_bindings)
                )
                self.assertEqual(first.bootstrap_history(), expected_bindings * 2)

            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["schema"], "qdl.stable-bar-edge-state.v3")
            self.assertEqual(persisted["connection_generation"], 100)
            self.assertEqual(persisted["warmup_rows"], 2)
            self.assertEqual(set(persisted["last_open_ms"]), set(first._binding_ids))
            expected_last = {
                source.binding_id: opens(
                    source.interval or "",
                    acquisition.runtime,
                )[1]
                for source, acquisition in first.history_bindings + first.history_okx_bindings
            }
            self.assertEqual(persisted["last_open_ms"], expected_last)
            okx_source = next(
                source for source, _acquisition in first.history_okx_bindings
                if source.instrument.native_symbol == "BTC-USDT-SWAP"
            )
            self.assertEqual(first._okx_binding(okx_source).connection_generation, 100)
            self.assertEqual(first._okx_binding(okx_source).source_session_id, first.okx_session_id)

            restarted = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=Publisher(),
                warmup_rows=2,
                state_path=state_path,
                clock=lambda: 180.0,
                generation_clock_ns=lambda: 200,
            )
            self.assertTrue(restarted._history_bootstrapped)
            self.assertEqual(restarted._last_open_ms, first._last_open_ms)
            self.assertEqual(restarted.connection_generation, 200)
            self.assertNotEqual(restarted.binance_session_id, first.binance_session_id)
            self.assertTrue(restarted.binance_session_id.endswith("-g200"))
            with patch(
                "qdl.runtime.stable_bar_edge.fetch_binance_history",
                side_effect=AssertionError("restart must not overlap bootstrap"),
            ), patch(
                "qdl.runtime.stable_bar_edge.fetch_okx_history",
                new_callable=AsyncMock,
                side_effect=AssertionError("restart must not overlap bootstrap"),
            ):
                self.assertEqual(restarted.bootstrap_history(), 0)

    def test_changed_canonical_cache_generation_forces_bounded_rebootstrap(self):
        """A checkpoint cannot certify history after its SQLite cache was rebuilt."""
        with tempfile.TemporaryDirectory(prefix="qdl-stable-bar-cache-generation-") as directory:
            state_path = Path(directory) / "bar-edge.json"
            original_cache_id = "a" * 32
            rebuilt_cache_id = "b" * 32
            seed = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=self._NoopPublisher(),
                warmup_rows=2,
                canonical_cache_id=original_cache_id,
                generation_clock_ns=lambda: 100,
            )
            payload = seed._state_identity_payload(
                schema="qdl.stable-bar-edge-state.v4"
            )
            payload["connection_generation"] = 100
            payload["last_open_ms"] = {
                source.binding_id: provider_bar_calendar_anchor_ms(
                    source.interval or "", provider=acquisition.runtime
                ) + 100 * canonical_interval_ms(source.interval or "")
                for source, acquisition in (
                    seed.history_bindings + seed.history_okx_bindings
                )
            }
            state_path.write_text(json.dumps(payload), encoding="utf-8")

            matching = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=self._NoopPublisher(),
                warmup_rows=2,
                state_path=state_path,
                canonical_cache_id=original_cache_id,
                generation_clock_ns=lambda: 200,
            )
            self.assertTrue(matching._history_bootstrapped)
            self.assertEqual(
                set(matching._last_open_ms), set(matching._binding_ids)
            )

            rebuilt = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=self._NoopPublisher(),
                warmup_rows=2,
                state_path=state_path,
                canonical_cache_id=rebuilt_cache_id,
                generation_clock_ns=lambda: 300,
            )
            self.assertFalse(rebuilt._history_bootstrapped)
            self.assertEqual(rebuilt._last_open_ms, {})
            self.assertEqual(rebuilt.connection_generation, 300)
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["schema"], "qdl.stable-bar-edge-state.v4")
            self.assertEqual(persisted["canonical_cache_id"], rebuilt_cache_id)
            self.assertEqual(persisted["last_open_ms"], {})

    def test_legacy_v2_checkpoint_rebases_to_fresh_durable_generation(self):
        class Publisher:
            def publish_many(self, values):
                return tuple(range(len(tuple(values))))

        with tempfile.TemporaryDirectory(prefix="qdl-stable-bar-state-") as directory:
            state_path = Path(directory) / "bar-edge.json"
            seed = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=Publisher(),
                warmup_rows=2,
                generation_clock_ns=lambda: 1,
            )
            legacy = seed._state_identity_payload(
                schema="qdl.stable-bar-edge-state.v2"
            )
            legacy["last_open_ms"] = {}
            state_path.write_text(json.dumps(legacy), encoding="utf-8")

            migrated = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=Publisher(),
                warmup_rows=2,
                state_path=state_path,
                canonical_cache_id="c" * 32,
                generation_clock_ns=lambda: 777,
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["schema"], "qdl.stable-bar-edge-state.v4")
            self.assertEqual(persisted["canonical_cache_id"], "c" * 32)
            self.assertEqual(persisted["connection_generation"], 777)
            self.assertEqual(persisted["last_open_ms"], {})
            self.assertEqual(migrated.connection_generation, 777)
            self.assertFalse(migrated._history_bootstrapped)
            self.assertTrue(migrated.okx_session_id.endswith("-g777"))

    def test_invalid_canonical_cache_identity_fails_closed(self):
        with tempfile.TemporaryDirectory(prefix="qdl-stable-bar-cache-id-") as directory:
            database = Path(directory) / "canonical-cache.sqlite3"
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    "CREATE TABLE cache_identity (singleton INTEGER PRIMARY KEY, cache_id TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO cache_identity (singleton, cache_id) VALUES (1, 'not-a-cache-id')"
                )
                connection.commit()
            finally:
                connection.close()
            with self.assertRaisesRegex(RuntimeError, "identity is invalid"):
                _canonical_cache_id(database)

    def test_canonical_cache_identity_reads_valid_generation(self):
        with tempfile.TemporaryDirectory(prefix="qdl-stable-bar-cache-id-") as directory:
            database = Path(directory) / "canonical-cache.sqlite3"
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    "CREATE TABLE cache_identity (singleton INTEGER PRIMARY KEY, cache_id TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO cache_identity (singleton, cache_id) VALUES (1, ?)",
                    ("d" * 32,),
                )
                connection.commit()
            finally:
                connection.close()
            self.assertEqual(_canonical_cache_id(database), "d" * 32)

    def test_exhausted_connection_generation_fails_closed_before_provider_access(self):
        class Publisher:
            def publish_many(self, values):
                raise AssertionError("provider rows must not be published")

        with tempfile.TemporaryDirectory(prefix="qdl-stable-bar-state-") as directory:
            state_path = Path(directory) / "bar-edge.json"
            seed = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=Publisher(),
                warmup_rows=2,
                generation_clock_ns=lambda: 1,
            )
            exhausted = seed._state_identity_payload(
                schema="qdl.stable-bar-edge-state.v3"
            )
            exhausted["connection_generation"] = (1 << 64) - 1
            exhausted["last_open_ms"] = {}
            state_path.write_text(json.dumps(exhausted), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "generation is exhausted"):
                StableBinanceBarEdge(
                    catalog=self.catalog,
                    acquisition=self.acquisition,
                    authority=self.authority,
                    publisher=Publisher(),
                    warmup_rows=2,
                    state_path=state_path,
                    generation_clock_ns=lambda: 1,
                )

    def test_checkpoint_corruption_and_partial_ack_fail_closed(self):
        class Envelope:
            def __init__(self, open_time: int):
                self.raw_frame_bytes = json.dumps(
                    {"row": _binance_row(open_time)}
                ).encode()

        class MissingAckPublisher:
            def publish_many(self, _values):
                return ()

        with tempfile.TemporaryDirectory(prefix="qdl-stable-bar-state-") as directory:
            state_path = Path(directory) / "bar-edge.json"
            state_path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "fields are invalid"):
                StableBinanceBarEdge(
                    catalog=self.catalog,
                    acquisition=self.acquisition,
                    authority=self.authority,
                    publisher=MissingAckPublisher(),
                    state_path=state_path,
                )

            state_path.unlink()
            edge = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=MissingAckPublisher(),
                warmup_rows=1,
                state_path=state_path,
                clock=lambda: 180.0,
            )
            with patch(
                "qdl.runtime.stable_bar_edge.fetch_binance_history",
                return_value=(Envelope(120_000),),
            ):
                with self.assertRaisesRegex(RuntimeError, "every Kafka ACK"):
                    edge.bootstrap_history()
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["schema"], "qdl.stable-bar-edge-state.v3")
            self.assertGreater(persisted["connection_generation"], 0)
            self.assertEqual(persisted["last_open_ms"], {})
            self.assertEqual(edge._last_open_ms, {})


if __name__ == "__main__":
    unittest.main()


class BarEdgeDeploymentShapeTests(unittest.TestCase):
    """The edge must follow the configured deployment, not a fixed market list.

    It previously refused to start unless Binance Spot/USDM and OKX Spot/SWAP
    were all present, so a deployment could not drop a zero-demand market or
    add a venue without editing this class.
    """

    CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
    ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"

    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(self.CATALOG_PATH)
        self.acquisition = StableAcquisitionPlan.load(
            self.ACQUISITION_PATH, catalog=self.catalog
        )
        self.authority = stable_authority_record(
            rust_image_digest="d" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=self.ACQUISITION_PATH.read_bytes(),
            effective_at_ns=time.time_ns(),
        )

    class _Publisher:
        def publish_many(self, values):
            return tuple(range(len(tuple(values))))

    def _edge(self, catalog, acquisition):
        return StableBinanceBarEdge(
            catalog=catalog,
            acquisition=acquisition,
            authority=self.authority,
            publisher=self._Publisher(),
            warmup_rows=2,
            clock=lambda: 180.0,
        )

    def _rest_bar_binding_ids(self, catalog, acquisition) -> set[str]:
        by_id = {item.binding_id: item for item in catalog.bindings}
        return {
            item.binding_id
            for item in acquisition.bindings
            if item.enabled
            and item.mode == "PYTHON_REST"
            and by_id[item.binding_id].feed.value == "BAR"
        }

    def _reduced(self, directory: Path, keep: set[tuple[str, str]]):
        """Write a catalog/acquisition pair limited to the given (venue, market)."""
        catalog_raw = yaml.safe_load(self.CATALOG_PATH.read_text(encoding="utf-8"))
        keep_uids = {
            item["instrument_uid"]
            for item in catalog_raw["instruments"]
            if (item["venue"], item["market"]) in keep
        }
        catalog_raw["instruments"] = [
            item for item in catalog_raw["instruments"]
            if item["instrument_uid"] in keep_uids
        ]
        catalog_raw["bindings"] = [
            item for item in catalog_raw["bindings"]
            if item["instrument_uid"] in keep_uids
        ]
        kept_bindings = {item["binding_id"] for item in catalog_raw["bindings"]}
        acquisition_raw = yaml.safe_load(
            self.ACQUISITION_PATH.read_text(encoding="utf-8")
        )
        acquisition_raw["bindings"] = [
            item for item in acquisition_raw["bindings"]
            if item["binding_id"] in kept_bindings
        ]
        catalog_path = directory / "catalog.yaml"
        acquisition_path = directory / "acquisition.yaml"
        catalog_path.write_text(yaml.safe_dump(catalog_raw), encoding="utf-8")
        acquisition_path.write_text(yaml.safe_dump(acquisition_raw), encoding="utf-8")
        catalog = StableSourceCatalog.load(catalog_path)
        return catalog, StableAcquisitionPlan.load(acquisition_path, catalog=catalog)

    def test_edge_serves_every_configured_rest_bar_binding(self):
        edge = self._edge(self.catalog, self.acquisition)
        owned = {
            source.binding_id
            for source, _ in edge.bindings + edge.okx_bindings
        }
        self.assertEqual(
            owned, self._rest_bar_binding_ids(self.catalog, self.acquisition)
        )

    def test_binance_branch_carries_bar_bindings_only(self):
        edge = self._edge(self.catalog, _legacy_rest_bar_fallback(self.acquisition))
        self.assertTrue(edge.bindings)
        for source, acquisition in edge.bindings:
            self.assertEqual(source.feed.value, "BAR")
            self.assertEqual(acquisition.runtime, "BINANCE")

    def test_a_deployment_without_spot_markets_is_accepted(self):
        with tempfile.TemporaryDirectory() as raw:
            catalog, acquisition = self._reduced(
                Path(raw), {("BINANCE", "USDM"), ("OKX", "SWAP")}
            )
            acquisition = _legacy_rest_bar_fallback(acquisition)
            markets = {
                item.instrument.identity.market for item in catalog.bindings
            }
            self.assertEqual(markets, {"USDM", "SWAP"})
            edge = self._edge(catalog, acquisition)
            owned = {
                source.binding_id
                for source, _ in edge.bindings + edge.okx_bindings
            }
            self.assertEqual(
                owned, self._rest_bar_binding_ids(catalog, acquisition)
            )

    def test_a_deployment_with_one_venue_is_accepted(self):
        with tempfile.TemporaryDirectory() as raw:
            catalog, acquisition = self._reduced(Path(raw), {("BINANCE", "USDM")})
            edge = self._edge(catalog, _legacy_rest_bar_fallback(acquisition))
            self.assertTrue(edge.bindings)
            self.assertEqual(edge.okx_bindings, ())
