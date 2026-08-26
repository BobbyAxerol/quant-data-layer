from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import tempfile
import time
import unittest
from pathlib import Path

import yaml
from unittest.mock import AsyncMock, patch

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
from qdl.canonical.market import canonicalize_okx_bar
from qdl.canonical.trade import TradeContext
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    stable_authority_record,
)


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

    def test_okx_final_bar_event_identity_is_transport_and_restart_independent(self):
        fixture = json.loads(
            (ROOT / "tests/fixtures/phase2/okx_bar.json").read_text(encoding="utf-8")
        )
        first_context = TradeContext(**fixture["context"])
        second_values = dict(fixture["context"])
        second_values["partition_sequence"] += 99
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
            expected_bindings = len(edge.bindings) + len(edge.okx_bindings)
            self.assertEqual(edge.bootstrap_history(), expected_bindings * 2)
            self.assertEqual(edge.bootstrap_history(), 0)
        self.assertEqual([len(item) for item in publisher.batches], [2] * expected_bindings)
        self.assertEqual(len(edge._last_open_ms), expected_bindings)
        self.assertTrue(edge._history_bootstrapped)

    def test_production_final_okx_bars_are_polled_after_history_bootstrap(self):
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
        self.assertTrue(any(value.startswith("okx-swap-") for value in history_ids))
        self.assertTrue(any(value.startswith("okx-swap-") for value in poll_ids))
        self.assertEqual(history_ids, poll_ids)
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
        self.assertEqual(max(declared), 1000)
        self.assertGreaterEqual(configured, max(declared))

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
            )
            with patch(
                "qdl.runtime.stable_bar_edge.fetch_binance_history",
                return_value=(
                    Envelope("BINANCE", 60_000),
                    Envelope("BINANCE", 120_000),
                ),
            ), patch(
                "qdl.runtime.stable_bar_edge.fetch_okx_history",
                new_callable=AsyncMock,
                return_value=(
                    Envelope("OKX", 60_000),
                    Envelope("OKX", 120_000),
                ),
            ):
                expected_bindings = len(first.bindings) + len(first.okx_bindings)
                self.assertEqual(first.bootstrap_history(), expected_bindings * 2)

            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["schema"], "qdl.stable-bar-edge-state.v2")
            self.assertEqual(persisted["warmup_rows"], 2)
            self.assertEqual(set(persisted["last_open_ms"]), set(first._binding_ids))
            self.assertEqual(set(persisted["last_open_ms"].values()), {120_000})

            restarted = StableBinanceBarEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=Publisher(),
                warmup_rows=2,
                state_path=state_path,
                clock=lambda: 180.0,
            )
            self.assertTrue(restarted._history_bootstrapped)
            self.assertEqual(restarted._last_open_ms, first._last_open_ms)
            self.assertEqual(restarted.binance_session_id, first.binance_session_id)
            with patch(
                "qdl.runtime.stable_bar_edge.fetch_binance_history",
                side_effect=AssertionError("restart must not overlap bootstrap"),
            ), patch(
                "qdl.runtime.stable_bar_edge.fetch_okx_history",
                new_callable=AsyncMock,
                side_effect=AssertionError("restart must not overlap bootstrap"),
            ):
                self.assertEqual(restarted.bootstrap_history(), 0)

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
            self.assertFalse(state_path.exists())
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
