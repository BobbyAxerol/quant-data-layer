from __future__ import annotations

import json
import unittest

from qdl.adapters.binance import (
    BinanceBarRawBinding,
    fetch_latest_closed_bar_raw_envelope,
)
from qdl.canonical.market import canonicalize_binance_usdm_rest_bar
from qdl.canonical.trade import TradeContext
from qdl.common.v1 import common_pb2
from qdl.raw.capture import bind_capture_context


class BinanceRestBarEdgeTests(unittest.TestCase):
    def binding(self, market="USDM", product="PERPETUAL"):
        return BinanceBarRawBinding(
            market=market,
            product_type=product,
            native_symbol="BTCUSDT",
            interval="1m",
            subscription_id=f"binance-{market.lower()}-bar",
            source_session_id="binance-rest-session-1",
            connection_generation=1,
            lease_epoch=7,
            authority_revision=1,
            partition_plan_epoch=1,
            adapter_version="binance-rest/2.0.0",
            config_revision=1,
            instrument_catalog_revision=3,
        )

    @staticmethod
    def response(*_args, **_kwargs):
        return {
            "data": [
                [0, "1", "2", "0.5", "1.5", "10", 59_999, "15", 2, "0", "0"],
                [60_000, "1.5", "3", "1", "2", "20", 119_999, "40", 3, "0", "0"],
                [120_000, "2", "4", "2", "3", "30", 179_999, "90", 4, "0", "0"],
            ]
        }

    def test_latest_closed_native_row_is_selected_and_canonicalized(self):
        raw = fetch_latest_closed_bar_raw_envelope(
            self.binding(), now_ms=150_000, fetcher=self.response, sleep=lambda _: None,
            test_provenance=True,
        )
        payload = json.loads(raw.raw_frame_bytes)
        self.assertEqual(payload["row"][0], 60_000)
        self.assertEqual(payload["bar_origin"], "VENUE_NATIVE")
        context = bind_capture_context(
            TradeContext(
                instrument_uid="uid-binance", instrument_id="BINANCE.USDM.PERPETUAL.BTC-USDT",
                instrument_revision=1, venue="BINANCE", market="USDM",
                product_type="PERPETUAL", native_symbol="BTCUSDT",
                provider="BINANCE_DIRECT", source_id="binance-rest-bar", source_role="PRIMARY",
                lease_epoch=7, received_at_ns=1, normalized_at_ns=2, published_at_ns=3,
                partition_sequence=1, normalizer_version="qdl-rust-core/2.0.0",
                adapter_version="binance-rest/2.0.0", config_revision=1,
            ),
            raw,
        )
        event = canonicalize_binance_usdm_rest_bar(payload, context)
        self.assertEqual(event.bar.origin, common_pb2.BAR_ORIGIN_VENUE_NATIVE)
        self.assertEqual(event.bar.volume_unit, common_pb2.QUANTITY_UNIT_BASE_ASSET)
        self.assertEqual(event.bar.base_volume.source_text, "20")
        self.assertEqual(event.bar.quote_volume.source_text, "40")
        self.assertNotIn(common_pb2.QUALITY_FLAG_BACKFILLED, event.quality_flags)

    def test_no_closed_bar_and_bad_market_fail_closed(self):
        with self.assertRaisesRegex(RuntimeError, "no closed bar"):
            fetch_latest_closed_bar_raw_envelope(
                self.binding(), now_ms=1, fetcher=self.response, sleep=lambda _: None,
            )
        with self.assertRaisesRegex(ValueError, "market"):
            self.binding("AUTO", "PERPETUAL")


if __name__ == "__main__":
    unittest.main()
