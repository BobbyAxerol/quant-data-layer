from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/phase10_realtime_provider_admission.py"
SPEC = importlib.util.spec_from_file_location("phase10_realtime_provider_admission", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
admission = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = admission
SPEC.loader.exec_module(admission)


class RealtimeProviderAdmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.bindings = admission.load_active_provider_bindings()
        cls.native_bindings = tuple(
            item for item in cls.bindings if item.mode == "RUST_NATIVE"
        )
        cls.by_key = {
            (item.native_channel, item.native_symbol): item
            for item in cls.native_bindings
        }

    def test_active_provider_scope_is_all_enabled_binance_usdm_and_okx_swap_bindings(self):
        self.assertEqual(len(self.bindings), 12)
        self.assertEqual(
            {item.binding_id for item in self.bindings},
            {
                "binance-usdm-btcusdt-trade",
                "binance-usdm-btcusdt-quote",
                "binance-usdm-btcusdt-bar-1m",
                "binance-usdm-ethusdt-trade",
                "binance-usdm-ethusdt-quote",
                "binance-usdm-ethusdt-bar-1m",
                "okx-swap-btcusdt-trade",
                "okx-swap-btcusdt-quote",
                "okx-swap-btcusdt-bar-1m",
                "okx-swap-eth-usdt-swap-trade",
                "okx-swap-eth-usdt-swap-quote",
                "okx-swap-eth-usdt-swap-bar-1m",
            },
        )
        self.assertEqual({item.venue for item in self.bindings}, {"BINANCE", "OKX"})
        self.assertEqual({item.market for item in self.bindings}, {"USDM", "SWAP"})
        self.assertEqual(
            {
                item.binding_id
                for item in self.bindings
                if item.mode == "PYTHON_REST"
            },
            {
                "binance-usdm-btcusdt-bar-1m",
                "binance-usdm-ethusdt-bar-1m",
                "okx-swap-btcusdt-bar-1m",
                "okx-swap-eth-usdt-swap-bar-1m",
            },
        )

    def test_binance_native_frames_preserve_binding_identity_and_finality(self):
        trade = admission.parse_binance_data(
            '{"e":"trade","s":"ETHUSDT","p":"1.25","q":"2","T":1000}',
            bindings=self.by_key,
        )
        quote = admission.parse_binance_data(
            '{"e":"bookTicker","s":"BTCUSDT","b":"1","B":"2","a":"3","A":"4","E":1001}',
            bindings=self.by_key,
        )
        self.assertEqual(trade.binding_id, "binance-usdm-ethusdt-trade")
        self.assertEqual(quote.binding_id, "binance-usdm-btcusdt-quote")
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "matching active binding"):
            admission.parse_binance_data(
                '{"e":"trade","s":"SOLUSDT","p":"1","q":"1","T":1}',
                bindings=self.by_key,
            )

    def test_binance_rest_bar_requires_a_final_native_closed_row(self):
        binding = next(
            item
            for item in self.bindings
            if item.binding_id == "binance-usdm-ethusdt-bar-1m"
        )
        payload = {
            "symbol": "ETHUSDT",
            "interval": "1m",
            "bar_origin": "VENUE_NATIVE",
            "row": [1000, "1", "3", "1", "2", "0", 60_999, "0", 0, "0", "0"],
        }
        observation = admission.parse_binance_rest_bar(
            payload,
            binding=binding,
            raw_frame_bytes=b'{}',
            observed_ms=61_000,
        )
        self.assertEqual(observation.binding_id, binding.binding_id)
        self.assertEqual(observation.source_time_ms, 60_999)
        self.assertTrue(observation.final_bar)
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "not closed"):
            admission.parse_binance_rest_bar(
                payload,
                binding=binding,
                raw_frame_bytes=b'{}',
                observed_ms=60_999,
            )

    def test_binance_zero_trade_status_filter_is_exact_and_other_zero_trades_fail_closed(self):
        status = {"e": "trade", "s": "BTCUSDT", "p": "0", "q": "0", "X": "NA", "st": 1}
        self.assertTrue(admission.is_binance_trade_status_frame(status))
        for altered in (
            {**status, "X": "MARKET"},
            {**status, "st": "1"},
            {**status, "p": "0.0"},
            {**status, "q": "1"},
        ):
            self.assertFalse(admission.is_binance_trade_status_frame(altered))
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "outside the positive decimal domain"):
            admission.parse_binance_data(
                '{"e":"trade","s":"BTCUSDT","p":"0","q":"0","T":1,"X":"MARKET","st":1}',
                bindings=self.by_key,
            )

    def test_okx_native_frames_preserve_trade_and_quote_identity(self):
        trade = admission.parse_okx_data(
            '{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"1","px":"1","sz":"2","ts":"1000"}]}',
            bindings=self.by_key,
        )
        quote = admission.parse_okx_data(
            '{"arg":{"channel":"bbo-tbt","instId":"ETH-USDT-SWAP"},"data":[{"bids":[["1","2","0","1"]],"asks":[["3","4","0","1"]],"seqId":"9","ts":"1001"}]}',
            bindings=self.by_key,
        )
        self.assertEqual(trade.binding_id, "okx-swap-btcusdt-trade")
        self.assertEqual(quote.binding_id, "okx-swap-eth-usdt-swap-quote")
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "matching active binding"):
            admission.parse_okx_data(
                '{"arg":{"channel":"trades","instId":"SOL-USDT-SWAP"},"data":[{"instId":"SOL-USDT-SWAP","tradeId":"1","px":"1","sz":"1","ts":"1"}]}',
                bindings=self.by_key,
            )

    def test_okx_rest_bar_requires_a_final_native_closed_row(self):
        binding = next(
            item
            for item in self.bindings
            if item.binding_id == "okx-swap-btcusdt-bar-1m"
        )
        final = admission.parse_okx_rest_bar(
            '{"arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},"data":[["1000","1","3","1","2","0","0","0","1"]]}',
            binding=binding,
            observed_ms=61_000,
        )
        self.assertEqual(final.binding_id, binding.binding_id)
        self.assertEqual(final.source_time_ms, 61_000)
        self.assertTrue(final.final_bar)
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "not final"):
            admission.parse_okx_rest_bar(
                '{"arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},"data":[["1000","1","3","1","2","0","0","0","0"]]}',
                binding=binding,
                observed_ms=61_000,
            )

    def test_session_accumulator_requires_every_binding_and_final_bars(self):
        bars = tuple(item for item in self.bindings if item.feed == "BAR")[:2]
        accumulator = admission._SessionAccumulator(bars)
        accumulator.add(admission.FrameObservation(bars[0].binding_id, 1, "a" * 64, True))
        self.assertFalse(accumulator.complete(require_final_bars=True))
        accumulator.add(admission.FrameObservation(bars[1].binding_id, 1, "b" * 64, False))
        self.assertFalse(accumulator.complete(require_final_bars=True))
        accumulator.add(admission.FrameObservation(bars[1].binding_id, 2, "c" * 64, True))
        self.assertTrue(accumulator.complete(require_final_bars=True))

    def test_session_accumulator_reports_only_bounded_missing_binding_diagnostics(self):
        bars = tuple(item for item in self.bindings if item.feed == "BAR")[:2]
        accumulator = admission._SessionAccumulator(bars)
        self.assertEqual(
            accumulator.missing_binding_ids(),
            tuple(sorted(item.binding_id for item in bars)),
        )
        self.assertEqual(
            accumulator.missing_final_bar_ids(),
            tuple(sorted(item.binding_id for item in bars)),
        )
        accumulator.add(admission.FrameObservation(bars[0].binding_id, 1, "a" * 64, False))
        self.assertEqual(accumulator.missing_binding_ids(), (bars[1].binding_id,))
        self.assertEqual(
            accumulator.missing_final_bar_ids(),
            tuple(sorted(item.binding_id for item in bars)),
        )

    def test_session_evidence_preserves_finality_when_newer_provisional_frame_arrives(self):
        binding = next(item for item in self.bindings if item.feed == "BAR")
        accumulator = admission._SessionAccumulator((binding,))
        accumulator.add(admission.FrameObservation(binding.binding_id, 1, "a" * 64, True))
        accumulator.add(admission.FrameObservation(binding.binding_id, 2, "b" * 64, False))
        evidence = accumulator.evidence(
            role="test", generation=1, ack_count=1, pre_ack_frames=0
        )
        self.assertFalse(evidence.observations[0].final_bar)
        self.assertEqual(evidence.final_bar_binding_ids, (binding.binding_id,))

    def test_okx_request_id_is_short_alphanumeric_and_role_bound(self):
        self.assertEqual(
            admission.okx_request_id(role="OKX:SWAP:PUBLIC", generation=12),
            "qdl10312p",
        )
        self.assertEqual(
            admission.okx_request_id(role="OKX:SWAP:BUSINESS", generation=12),
            "qdl10312b",
        )
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "identity"):
            admission.okx_request_id(role="OKX:SWAP:OTHER", generation=1)

    def test_binance_feed_lanes_are_complete_and_request_ids_are_role_bound(self):
        binance = tuple(item for item in self.native_bindings if item.venue == "BINANCE")
        lanes = admission.binance_admission_lanes(binance)
        self.assertEqual({name: len(items) for name, items in lanes.items()}, {"BAR": 0, "TRADE": 2, "QUOTE": 2})
        self.assertEqual(
            admission.binance_request_id(role="BINANCE:USDM:QUOTE", generation=12),
            10_123,
        )
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "identity"):
            admission.binance_request_id(role="BINANCE:USDM:FLOW", generation=1)

    def test_report_rejects_missing_reconnect_or_stale_binding(self):
        binding = next(item for item in self.bindings if item.mode == "RUST_NATIVE")
        fresh = admission.FrameObservation(binding.binding_id, 10_000_000_000_000, "a" * 64, False)
        session = admission.SessionEvidence("BINANCE:USDM", 1, 1, 0, 1, (fresh,))
        with self.assertRaisesRegex(admission.ProviderAdmissionError, "recovery/reconnect"):
            admission._render_report(
                bindings=(binding,),
                sessions=(session,),
                elapsed_seconds=1.0,
                cpu_seconds=0.1,
                max_rss_kib=1,
            )

    def test_report_accepts_one_http_recovery_for_each_venue_rest_bar(self):
        bindings = tuple(item for item in self.bindings if item.mode == "PYTHON_REST")
        sessions = tuple(
            admission.SessionEvidence(
                f"{binding.venue}:REST_BAR", 1, 0, 0, 1,
                (admission.FrameObservation(binding.binding_id, 10_000_000_000_000, "b" * 64, True),),
                transport="HTTP",
            )
            for binding in bindings
        )
        report = admission._render_report(
            bindings=bindings,
            sessions=sessions,
            elapsed_seconds=1.0,
            cpu_seconds=0.1,
            max_rss_kib=1,
        )
        self.assertEqual(report["rest_closed_bar_recovery_count"], 4)
        self.assertEqual({item["transport"] for item in report["bindings"]}, {"HTTP"})


if __name__ == "__main__":
    unittest.main()
