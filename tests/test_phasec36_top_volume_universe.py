from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from qdl.universe.top_volume import (
    TopVolumeUniversePolicy,
    UniverseAuditStore,
    resolve_binance_usdm_top_volume,
    resolve_okx_swap_top_volume,
)


def _binance_symbols(*symbols: str):
    return {
        "symbols": [
            {
                "symbol": symbol,
                "status": "TRADING",
                "contractType": "PERPETUAL",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
            }
            for symbol in symbols
        ]
    }


def _okx_instruments(*symbols: str):
    return [
        {
            "instType": "SWAP",
            "instId": symbol,
            "state": "live",
            "settleCcy": "USDT",
        }
        for symbol in symbols
    ]


class PhaseC36TopVolumeUniverseTests(unittest.TestCase):
    def test_binance_filters_and_ranks_by_quote_volume_with_deterministic_ties(self):
        exchange = _binance_symbols("BTCUSDT", "ETHUSDT", "SOLUSDT")
        exchange["symbols"].append({
            "symbol": "COINUSDT", "status": "TRADING", "contractType": "CURRENT_QUARTER",
            "quoteAsset": "USDT", "marginAsset": "USDT",
        })
        exchange["symbols"].append({
            "symbol": "HALTEDUSDT", "status": "BREAK", "contractType": "PERPETUAL",
            "quoteAsset": "USDT", "marginAsset": "USDT",
        })
        result = resolve_binance_usdm_top_volume(
            exchange_info=exchange,
            tickers=[
                {"symbol": "ETHUSDT", "quoteVolume": "10"},
                {"symbol": "SOLUSDT", "quoteVolume": "10"},
                {"symbol": "BTCUSDT", "quoteVolume": "20"},
                {"symbol": "COINUSDT", "quoteVolume": "999"},
            ],
            policy=TopVolumeUniversePolicy("BINANCE", "USDM", size=3),
            generated_at_ns=1,
        )
        self.assertEqual([item.native_symbol for item in result.members], ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        self.assertEqual(result.eligible_symbols, ("BTCUSDT", "ETHUSDT", "SOLUSDT"))

    def test_okx_uses_quote_volume_then_safe_quote_notional_fallback(self):
        result = resolve_okx_swap_top_volume(
            instruments=_okx_instruments("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"),
            tickers=[
                {"instId": "BTC-USDT-SWAP", "volCcyQuote": "100"},
                {"instId": "ETH-USDT-SWAP", "volCcy24h": "20", "last": "4"},
                {"instId": "SOL-USDT-SWAP", "vol24h": "999999"},
            ],
            policy=TopVolumeUniversePolicy("OKX", "SWAP", size=2),
            generated_at_ns=2,
        )
        self.assertEqual([item.native_symbol for item in result.members], ["BTC-USDT-SWAP", "ETH-USDT-SWAP"])
        self.assertEqual(result.members[1].quote_volume, "80")

    def test_insufficient_eligible_symbols_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "smaller than the required"):
            resolve_binance_usdm_top_volume(
                exchange_info=_binance_symbols("BTCUSDT"),
                tickers=[{"symbol": "BTCUSDT", "quoteVolume": "1"}],
                policy=TopVolumeUniversePolicy("BINANCE", "USDM", size=2),
                generated_at_ns=3,
            )

    def test_store_audits_rank_out_vs_not_live_and_preserves_digest(self):
        policy = TopVolumeUniversePolicy("BINANCE", "USDM", size=2)
        first = resolve_binance_usdm_top_volume(
            exchange_info=_binance_symbols("BTCUSDT", "ETHUSDT", "SOLUSDT"),
            tickers=[
                {"symbol": "BTCUSDT", "quoteVolume": "30"},
                {"symbol": "ETHUSDT", "quoteVolume": "20"},
                {"symbol": "SOLUSDT", "quoteVolume": "10"},
            ], policy=policy, generated_at_ns=10,
        )
        second = resolve_binance_usdm_top_volume(
            exchange_info=_binance_symbols("BTCUSDT", "SOLUSDT"),
            tickers=[
                {"symbol": "BTCUSDT", "quoteVolume": "30"},
                {"symbol": "SOLUSDT", "quoteVolume": "25"},
            ], policy=policy, generated_at_ns=20,
        )
        with tempfile.TemporaryDirectory() as raw:
            store = UniverseAuditStore(raw, max_audit_files=2)
            first_audit = store.publish(first)
            second_audit = store.publish(second)
            self.assertEqual(first_audit["added"], ["BTCUSDT", "ETHUSDT"])
            self.assertEqual(second_audit["added"], ["SOLUSDT"])
            self.assertEqual(second_audit["removed"], [{"native_symbol": "ETHUSDT", "reason": "DELISTED_OR_NOT_LIVE"}])
            current = json.loads((Path(raw) / "binance-usdm-top2-current.json").read_text())
            self.assertEqual(current["selection_sha256"], second.selection_sha256)
            self.assertEqual(current["eligible_symbols"], ["BTCUSDT", "SOLUSDT"])

    def test_store_marks_still_eligible_member_as_ranked_out(self):
        policy = TopVolumeUniversePolicy("OKX", "SWAP", size=2)
        first = resolve_okx_swap_top_volume(
            instruments=_okx_instruments("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"),
            tickers=[
                {"instId": "BTC-USDT-SWAP", "volCcyQuote": "30"},
                {"instId": "ETH-USDT-SWAP", "volCcyQuote": "20"},
                {"instId": "SOL-USDT-SWAP", "volCcyQuote": "10"},
            ], policy=policy, generated_at_ns=30,
        )
        second = resolve_okx_swap_top_volume(
            instruments=_okx_instruments("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"),
            tickers=[
                {"instId": "BTC-USDT-SWAP", "volCcyQuote": "30"},
                {"instId": "ETH-USDT-SWAP", "volCcyQuote": "10"},
                {"instId": "SOL-USDT-SWAP", "volCcyQuote": "20"},
            ], policy=policy, generated_at_ns=40,
        )
        with tempfile.TemporaryDirectory() as raw:
            store = UniverseAuditStore(raw)
            store.publish(first)
            audit = store.publish(second)
        self.assertEqual(audit["removed"], [{"native_symbol": "ETH-USDT-SWAP", "reason": "RANKED_OUT"}])


if __name__ == "__main__":
    unittest.main()
