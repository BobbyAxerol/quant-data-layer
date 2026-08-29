"""Exact sealed paper-consumer scope for the five-liquid price/bar extension."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "config/v2/phase115c-paper-consumer-source-registry.yaml"
DEMAND_PATH = ROOT / "config/v2/phase115c-paper-consumer-demand.yaml"

BINANCE_SYMBOLS = frozenset({"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT"})
OKX_SYMBOLS = frozenset({
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP",
    "DOGE-USDT-SWAP", "BNB-USDT-SWAP",
})


class Phase115CFiveLiquidHandoffTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.demand = yaml.safe_load(DEMAND_PATH.read_text(encoding="utf-8"))
        cls.registry = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
        cls.requirements = [
            (consumer["consumer_id"], requirement)
            for consumer in cls.demand["consumers"]
            for requirement in consumer["requirements"]
        ]

    def test_exact_five_liquid_consumer_route_scope(self) -> None:
        requirements = self.requirements
        self.assertEqual(len(requirements), 60)
        self.assertEqual(
            Counter(consumer_id for consumer_id, _ in requirements),
            {
                "trading-system.paper.stable": 30,
                "alpha.binance.paper.stable": 15,
                "alpha.okx.paper.stable": 15,
            },
        )
        self.assertEqual(
            {
                (item["venue"], item["market"])
                for _, item in requirements
            },
            {("BINANCE", "USDM"), ("OKX", "SWAP")},
        )
        self.assertEqual(
            {
                symbol
                for _, item in requirements
                if item["venue"] == "BINANCE"
                for symbol in (item["native_symbol"],)
            },
            BINANCE_SYMBOLS,
        )
        self.assertEqual(
            {
                symbol
                for _, item in requirements
                if item["venue"] == "OKX"
                for symbol in (item["native_symbol"],)
            },
            OKX_SYMBOLS,
        )
        self.assertFalse(any(item["feed"].startswith("BOOK_") for _, item in requirements))

    def test_admission_budget_is_exact_and_all_routes_are_provider_admitted(self) -> None:
        self.assertEqual(self.demand["revision"], 2)
        self.assertEqual(self.registry["revision"], 2)
        usage = Counter(
            (item["venue"], item["market"], item["feed"])
            for _, item in self.requirements
        )
        limits = {
            (item["venue"], item["market"], item["feed"]): item["max_slices"]
            for item in self.registry["admission"]["budgets"]
        }
        self.assertEqual(
            {key: (usage[key], limits[key]) for key in sorted(usage)},
            {
                ("BINANCE", "USDM", "BAR"): (15, 15),
                ("BINANCE", "USDM", "QUOTE"): (5, 5),
                ("BINANCE", "USDM", "TRADE"): (10, 10),
                ("OKX", "SWAP", "BAR"): (15, 15),
                ("OKX", "SWAP", "QUOTE"): (5, 5),
                ("OKX", "SWAP", "TRADE"): (10, 10),
            },
        )
        self.assertEqual(self.registry["admission"]["max_total_slices"], 60)


if __name__ == "__main__":
    unittest.main()
