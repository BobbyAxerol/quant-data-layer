from __future__ import annotations

import asyncio
import json
import time
import unittest
from pathlib import Path
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

        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            asyncio.run(fetch_okx_history(
                _okx_binding(),
                limit=3,
                now_ms=240_000,
                history_client=Partial(),
            ))

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

    def test_bootstrap_publishes_four_real_provider_batches_once(self):
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
            self.assertEqual(edge.bootstrap_history(), 8)
            self.assertEqual(edge.bootstrap_history(), 0)
        self.assertEqual([len(item) for item in publisher.batches], [2, 2, 2, 2])
        self.assertEqual(len(edge._last_open_ms), 4)
        self.assertTrue(edge._history_bootstrapped)


if __name__ == "__main__":
    unittest.main()
