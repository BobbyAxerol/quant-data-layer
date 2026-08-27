from __future__ import annotations

import unittest

from scripts.phase114_l2_real_provider_capture import (
    ProviderCaptureError,
    active_book_bindings,
    active_binance_book_symbols,
    binance_resnapshot_required,
    validate_binance_replay,
    validate_okx_replay,
)


class Phase114L2ProviderCaptureTests(unittest.TestCase):
    def test_active_symbols_are_derived_from_admitted_l2_inventory(self):
        document = {
            "rows": [
                {"state": "ADMITTED", "venue": "BINANCE", "market": "USDM", "feed": "BOOK_SNAPSHOT", "native_symbol": "BTCUSDT"},
                {"state": "ADMITTED", "venue": "BINANCE", "market": "USDM", "feed": "BOOK_DELTA", "native_symbol": "BTCUSDT_260925"},
                {"state": "ADMITTED", "venue": "OKX", "market": "SWAP", "product_type": "PERPETUAL", "feed": "BOOK_SNAPSHOT", "native_symbol": "BTC-USDT-SWAP", "instrument_uid": "okx-uid", "instrument_id": "OKX.SWAP.PERPETUAL.BTC-USDT", "requirement_id": "okx-rid"},
                {"state": "UNSUPPORTED", "venue": "BINANCE", "market": "USDM", "feed": "BOOK_SNAPSHOT", "native_symbol": "DELISTEDUSDT"},
            ]
        }
        for row in document["rows"][:2]:
            row.update({
                "product_type": "PERPETUAL", "instrument_uid": f"uid-{row['native_symbol']}",
                "instrument_id": f"BINANCE.USDM.PERPETUAL.{row['native_symbol']}",
                "requirement_id": f"rid-{row['native_symbol']}",
            })
        self.assertEqual(active_binance_book_symbols(document), ("BTCUSDT", "BTCUSDT_260925"))
        self.assertEqual(
            [(item.venue, item.market, item.native_symbol) for item in active_book_bindings(document)],
            [("BINANCE", "USDM", "BTCUSDT"), ("BINANCE", "USDM", "BTCUSDT_260925"), ("OKX", "SWAP", "BTC-USDT-SWAP")],
        )

    def test_okx_futures_binding_is_admitted_by_the_same_public_books_protocol(self):
        document = {
            "rows": [{
                "state": "ADMITTED", "venue": "OKX", "market": "FUTURES",
                "product_type": "FUTURE", "feed": "BOOK_DELTA",
                "native_symbol": "BTC-USD-260925", "instrument_uid": "future-uid",
                "instrument_id": "OKX.FUTURES.FUTURE.BTC-USD-260925",
                "requirement_id": "future-rid",
            }]
        }
        binding = active_book_bindings(document)[0]
        self.assertEqual((binding.venue, binding.market, binding.native_symbol), (
            "OKX", "FUTURES", "BTC-USD-260925"
        ))

    def test_binance_snapshot_range_bridge_and_pu_chain_are_required(self):
        frames = [
            {"e": "depthUpdate", "s": "BTCUSDT", "U": 99, "u": 101, "pu": 98},
            {"e": "depthUpdate", "s": "BTCUSDT", "U": 102, "u": 103, "pu": 101},
        ]
        replay = validate_binance_replay(
            symbol="BTCUSDT", snapshot_sequence=100, frames=frames, raw_frames=["a", "b"]
        )
        assert replay is not None
        self.assertEqual((replay.bridge_start, replay.bridge_end, replay.final_sequence), (99, 101, 103))
        self.assertIsNone(
            validate_binance_replay(
                symbol="BTCUSDT", snapshot_sequence=100, frames=frames[:1], raw_frames=["a"]
            )
        )
        with self.assertRaisesRegex(ProviderCaptureError, "continuity"):
            validate_binance_replay(
                symbol="BTCUSDT",
                snapshot_sequence=100,
                frames=[frames[0], {"e": "depthUpdate", "s": "BTCUSDT", "U": 102, "u": 103, "pu": 100}],
                raw_frames=["a", "bad"],
            )

    def test_binance_snapshot_gap_requires_bounded_resnapshot_not_more_deltas(self):
        self.assertTrue(binance_resnapshot_required(
            symbol="BTCUSDT",
            snapshot_sequence=100,
            frames=[{"e": "depthUpdate", "s": "BTCUSDT", "U": 102, "u": 103, "pu": 101}],
        ))
        self.assertFalse(binance_resnapshot_required(
            symbol="BTCUSDT",
            snapshot_sequence=100,
            frames=[{"e": "depthUpdate", "s": "BTCUSDT", "U": 99, "u": 101, "pu": 98}],
        ))
        self.assertFalse(binance_resnapshot_required(
            symbol="BTCUSDT",
            snapshot_sequence=100,
            frames=[{"e": "depthUpdate", "s": "BTCUSDT", "U": 99, "u": 100, "pu": 98}],
        ))

    def test_okx_snapshot_update_and_maintenance_reset_are_not_cross_mixed(self):
        frames = [
            {"arg": {"channel": "books", "instId": "BTC-USDT-SWAP"}, "action": "snapshot", "data": [{"seqId": "20"}]},
            {"arg": {"channel": "books", "instId": "BTC-USDT-SWAP"}, "action": "update", "data": [{"seqId": "21", "prevSeqId": "20"}]},
        ]
        replay = validate_okx_replay(symbol="BTC-USDT-SWAP", frames=frames, raw_frames=["a", "b"])
        assert replay is not None
        self.assertEqual((replay.snapshot_sequence, replay.final_sequence, replay.update_count), (20, 21, 1))
        self.assertIsNone(
            validate_okx_replay(
                symbol="BTC-USDT-SWAP",
                frames=[{"event": "subscribe", "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"}}],
                raw_frames=["ack"],
            )
        )
        self.assertIsNone(
            validate_okx_replay(
                symbol="BTC-USDT-SWAP",
                frames=[frames[0], {"arg": {"channel": "books", "instId": "BTC-USDT-SWAP"}, "action": "update", "data": [{"seqId": "21", "prevSeqId": "-1"}]}],
                raw_frames=["snapshot", "reset"],
            )
        )
        with self.assertRaisesRegex(ProviderCaptureError, "continuity"):
            validate_okx_replay(
                symbol="BTC-USDT-SWAP",
                frames=[frames[0], {"arg": {"channel": "books", "instId": "BTC-USDT-SWAP"}, "action": "update", "data": [{"seqId": "22", "prevSeqId": "18"}]}],
                raw_frames=["a", "bad"],
            )


if __name__ == "__main__":
    unittest.main()
