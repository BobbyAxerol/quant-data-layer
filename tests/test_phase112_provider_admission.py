from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest

from qdl.query import FeedType
from qdl.runtime.universal_realtime import ProviderRealtimeBinding


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/phase112_universal_realtime_provider_admission.py"
SPEC = importlib.util.spec_from_file_location("phase112_provider_admission", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
admission = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = admission
SPEC.loader.exec_module(admission)


def _binding(
    *,
    binding_id: str,
    venue: str = "BINANCE",
    market: str = "USDM",
    feed: FeedType = FeedType.TRADE,
    symbol: str = "BTCUSDT",
    interval: str | None = None,
) -> ProviderRealtimeBinding:
    is_bar = feed is FeedType.BAR
    is_okx = venue == "OKX"
    if is_bar:
        mode = "PYTHON_REST"
        provider_kind = "okx_bar" if is_okx else f"binance_{market.lower()}_rest_bar"
        native_channel = f"candle1m" if is_okx else f"rest-klines/{interval or '1m'}"
        websocket_url = None
    else:
        mode = "RUST_NATIVE"
        provider_kind = "okx_trade" if is_okx and feed is FeedType.TRADE else (
            "okx_bbo" if is_okx else f"binance_{market.lower()}_{'trade' if feed is FeedType.TRADE else 'bbo'}"
        )
        native_channel = (
            "trades" if is_okx and feed is FeedType.TRADE else (
                "bbo-tbt" if is_okx else f"{symbol.lower()}@{'trade' if feed is FeedType.TRADE else 'bookTicker'}"
            )
        )
        websocket_url = (
            "wss://ws.okx.com:8443/ws/v5/public"
            if is_okx
            else (
                "wss://stream.binance.com:9443/ws"
                if market == "SPOT"
                else "wss://fstream.binance.com/ws"
            )
        )
    return ProviderRealtimeBinding(
        binding_id=binding_id,
        instrument_uid=f"uid-{binding_id}",
        instrument_id=f"{venue}.{market}.PERPETUAL.{symbol}",
        venue=venue,
        market=market,
        product_type="PERPETUAL",
        native_symbol=symbol,
        feed=feed,
        interval=interval if is_bar else None,
        source_id=f"source-{binding_id}",
        adapter_version="adapter/2.0.0",
        normalizer_version="qdl-rust-core/2.0.0",
        stale_after_ms=60_000 if is_bar else 15_000,
        require_final_bar=is_bar,
        mode=mode,
        provider_kind=provider_kind,
        native_channel=native_channel,
        websocket_url=websocket_url,
        business_websocket_url=None,
        catalog_revision=11,
        demand_revision=11,
    )


class UniversalRealtimeProviderAdmissionTests(unittest.TestCase):
    def test_native_sessions_are_bounded_and_grouped_by_shared_role(self):
        bindings = tuple(
            _binding(binding_id=f"trade-{index}", symbol=f"S{index}USDT")
            for index in range(205)
        ) + (
            _binding(binding_id="quote-1", feed=FeedType.QUOTE, symbol="QUOTEUSDT"),
            _binding(
                binding_id="okx-trade-1", venue="OKX", market="SWAP",
                feed=FeedType.TRADE, symbol="ETH-USDT-SWAP",
            ),
        )
        groups = admission._native_groups(bindings, max_bindings_per_session=200)
        self.assertEqual(
            [(role, len(values)) for role, values in groups],
            [
                (("BINANCE", "USDM", "QUOTE"), 1),
                (("BINANCE", "USDM", "TRADE"), 200),
                (("BINANCE", "USDM", "TRADE"), 5),
                (("OKX", "SWAP", "TRADE"), 1),
            ],
        )
        self.assertEqual({role[:2] for role, _values in groups}, {("BINANCE", "USDM"), ("OKX", "SWAP")})

    def test_binance_parser_rejects_cross_mixed_symbol_and_filters_only_exact_status(self):
        binding = _binding(binding_id="eth-trade", symbol="ETHUSDT")
        lookup = admission._binance_lookup((binding,))
        observed = admission._binance_observation(
            '{"e":"trade","s":"ETHUSDT","p":"1.25","q":"2","T":1000}',
            bindings=lookup,
            generation=1,
        )
        self.assertEqual(observed.binding_id, binding.binding_id)
        self.assertIsNone(admission._binance_observation(
            '{"e":"trade","s":"ETHUSDT","p":"0","q":"0","X":"NA","st":1}',
            bindings=lookup,
            generation=1,
        ))
        quote = _binding(binding_id="eth-quote", feed=FeedType.QUOTE, symbol="ETHUSDT")
        quote_observed = admission._binance_observation(
            '{"u":7,"s":"ETHUSDT","b":"1","B":"2","a":"3","A":"4","T":1001}',
            bindings=admission._binance_lookup((quote,)),
            generation=1,
        )
        self.assertEqual(quote_observed.binding_id, quote.binding_id)
        self.assertFalse(quote_observed.source_time_missing)
        spot_quote = _binding(
            binding_id="spot-quote", market="SPOT", feed=FeedType.QUOTE, symbol="BTCUSDT"
        )
        spot_observed = admission._binance_observation(
            '{"u":8,"s":"BTCUSDT","b":"1","B":"2","a":"3","A":"4"}',
            bindings=admission._binance_lookup((spot_quote,)),
            generation=1,
        )
        self.assertTrue(spot_observed.source_time_missing)
        self.assertLessEqual(spot_observed.observed_at_ms - spot_observed.source_time_ms, 1)
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "cross-mixed"):
            admission._binance_observation(
                '{"e":"trade","s":"BTCUSDT","p":"1","q":"1","T":1000}',
                bindings=lookup,
                generation=1,
            )

    def test_okx_parser_preserves_bbo_identity_and_decimal_contract(self):
        binding = _binding(
            binding_id="okx-bbo", venue="OKX", market="SWAP",
            feed=FeedType.QUOTE, symbol="ETH-USDT-SWAP",
        )
        lookup = admission._okx_lookup((binding,))
        observed = admission._okx_observation(
            '{"arg":{"channel":"bbo-tbt","instId":"ETH-USDT-SWAP"},'
            '"data":[{"bids":[["1","2","0","1"]],"asks":[["3","4","0","1"]],'
            '"seqId":"9","ts":"1001"}]}',
            bindings=lookup,
            generation=1,
        )
        self.assertEqual(observed.binding_id, binding.binding_id)
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "cross-mixed"):
            admission._okx_observation(
                '{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},'
                '"data":[{"bids":[["1","2"]],"asks":[["3","4"]],"seqId":"9","ts":"1001"}]}',
                bindings=lookup,
                generation=1,
            )

    def test_report_requires_every_binding_and_explicit_reconnect_evidence(self):
        native = _binding(binding_id="eth-trade", symbol="ETHUSDT")
        bar = _binding(binding_id="eth-bar", feed=FeedType.BAR, symbol="ETHUSDT", interval="1m")
        now_ms = time.time_ns() // 1_000_000
        fake_plan = SimpleNamespace(
            inventory_sha256="a" * 64,
            report_payload=lambda: {"schema": "test-plan"},
        )
        plan = admission.ProviderAdmissionPlan(
            plan=fake_plan,
            bindings=(native, bar),
            accepted_missing=(),
            deferred_requirement_ids=(),
        )
        primary = admission.SessionEvidence(
            role=("BINANCE", "USDM", "TRADE"), generation=1,
            binding_ids=(native.binding_id,), ack_count=1, event_count=1,
            observations=(admission.FrameObservation(native.binding_id, now_ms, now_ms, "a" * 64, False, "WEBSOCKET", 1),),
        )
        bar_session = admission.SessionEvidence(
            role=("BINANCE", "USDM", "BAR"), generation=1,
            binding_ids=(bar.binding_id,), ack_count=0, event_count=1,
            observations=(admission.FrameObservation(bar.binding_id, now_ms, now_ms, "b" * 64, True, "HTTP", 1),),
            transport="HTTP",
        )
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "incomplete"):
            admission._report(
                plan, (primary, bar_session), elapsed_seconds=1.0,
                cpu_seconds=0.1, max_rss_kib=1,
                max_bindings_per_session=200, rest_concurrency=16,
            )
        reconnect = admission.SessionEvidence(
            role=("BINANCE", "USDM", "TRADE"), generation=2,
            binding_ids=(native.binding_id,), ack_count=1, event_count=1,
            observations=(admission.FrameObservation(native.binding_id, now_ms, now_ms, "c" * 64, False, "WEBSOCKET", 2),),
        )
        report = admission._report(
            plan, (primary, bar_session, reconnect), elapsed_seconds=1.0,
            cpu_seconds=0.1, max_rss_kib=1,
            max_bindings_per_session=200, rest_concurrency=16,
        )
        self.assertEqual(report["status"], "PASS")
        self.assertTrue(next(item for item in report["bindings"] if item["binding_id"] == native.binding_id)["reconnect_probed"])


if __name__ == "__main__":
    unittest.main()
