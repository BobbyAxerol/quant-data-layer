"""Exact sealed paper-consumer scope for the five-liquid price/bar extension."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import yaml

from qdl.certification.phase103_consumer_acceptance import (
    DeliveryClass,
    validate_resume_offsets,
)
from qdl.demand import (
    ActiveDemandCompiler,
    ActiveDemandSourceRegistry,
    DemandFeed,
)
from qdl.runtime.production_catalog import ProductionDemandManifest
from qdl_sdk import DataRequirement, Feed, FeedStatusResponse, Grade, StalePolicy
from scripts.phase103_consumer_receipt_acceptance import _quiet_trade_status_is_observable


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "config/v2/phase115c-paper-consumer-source-registry.yaml"
DEMAND_PATH = ROOT / "config/v2/phase115c-paper-consumer-demand.yaml"

BINANCE_SYMBOLS = frozenset({"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT"})
OKX_SYMBOLS = frozenset({
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP",
    "DOGE-USDT-SWAP", "BNB-USDT-SWAP",
})

OKX_TRADE_UIDS = {
    "BTC-USDT-SWAP": "fb26214c-7b9b-5961-95b2-55154755af0f",
    "ETH-USDT-SWAP": "e49b54ae-c23d-5351-9e64-47934aac28f8",
    "SOL-USDT-SWAP": "a6884fb3-1fa0-53e0-9621-d01ba5f9a2de",
    "DOGE-USDT-SWAP": "6c7c9256-2905-5c75-a149-fa0ac36bbbc7",
    "BNB-USDT-SWAP": "f2e37e2b-1386-5a32-9b79-0fd39ec7a5a3",
}


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
        self.assertEqual(len(requirements), 72)
        self.assertEqual(
            Counter(consumer_id for consumer_id, _ in requirements),
            {
                "trading-system.paper.stable": 42,
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
        typed = [
            item
            for consumer_id, item in requirements
            if consumer_id == "trading-system.paper.stable"
            and item["feed"] in {"MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA"}
        ]
        self.assertEqual(len(typed), 12)
        self.assertEqual(
            {(item["feed"], item["source_policy_id"]) for item in typed},
            {
                ("MARK_INDEX_PRICE", "crypto_liquid_v2"),
                ("BOOK_SNAPSHOT", "crypto_liquid_v2"),
                ("BOOK_DELTA", "crypto_liquid_v2"),
            },
        )

    def test_typed_books_preserve_explicit_live_acquisition_contract(self) -> None:
        book_rows = [
            item for _consumer_id, item in self.requirements
            if item["feed"] in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
        ]
        payload = {
            "schema": self.demand["schema"],
            "revision": self.demand["revision"],
            "consumers": [{
                "consumer_id": "typed-book-regression",
                "consumer_grade": "EXECUTION",
                "requirements": book_rows,
            }],
        }
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "books.yaml"
            path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
            manifest = ProductionDemandManifest.load_many((path,))
        books = [
            item for item in manifest.demands
            if item.feed.value in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
        ]
        self.assertEqual(len(books), 8)
        self.assertEqual({item.depth_per_side for item in books}, {100})
        self.assertTrue(all(item.require_live for item in books))
        self.assertEqual(
            {item.max_freshness_ms for item in books if item.feed.value == "BOOK_SNAPSHOT"},
            {60_000},
        )
        self.assertEqual(
            {item.max_freshness_ms for item in books if item.feed.value == "BOOK_DELTA"},
            {2_000},
        )

    def test_admission_budget_is_exact_and_all_routes_are_provider_admitted(self) -> None:
        self.assertEqual(self.demand["revision"], 3)
        self.assertEqual(self.registry["revision"], 3)
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
                ("BINANCE", "USDM", "MARK_INDEX_PRICE"): (2, 2),
                ("BINANCE", "USDM", "BOOK_SNAPSHOT"): (2, 2),
                ("BINANCE", "USDM", "BOOK_DELTA"): (2, 2),
                ("OKX", "SWAP", "BAR"): (15, 15),
                ("OKX", "SWAP", "QUOTE"): (5, 5),
                ("OKX", "SWAP", "TRADE"): (10, 10),
                ("OKX", "SWAP", "MARK_INDEX_PRICE"): (2, 2),
                ("OKX", "SWAP", "BOOK_SNAPSHOT"): (2, 2),
                ("OKX", "SWAP", "BOOK_DELTA"): (2, 2),
            },
        )
        self.assertEqual(self.registry["admission"]["max_total_slices"], 72)

    def test_public_mark_index_contract_compiles_without_alias_rewrite(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            repository = root / "repository"
            alpha = root / "execution_alpha"
            trading = root / "trading_system"
            (repository / "config/v2").mkdir(parents=True)
            (trading / "config/_config").mkdir(parents=True)
            alpha.mkdir()
            (repository / "config/v2/phase115c-paper-consumer-demand.yaml").write_bytes(
                DEMAND_PATH.read_bytes()
            )
            (trading / "config/_config/portfolio_account_config_setup.yaml").write_text(
                "alphas:\n  - alpha_id: fixture_alpha\n    allowed_venues: [BINANCE]\n",
                encoding="utf-8",
            )
            registry = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
            registry["sources"] = [registry["sources"][0]]
            registry_path = repository / "config/v2/registry.yaml"
            registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

            inventory = ActiveDemandCompiler(
                registry=ActiveDemandSourceRegistry.load(registry_path),
                repository_root=repository,
                execution_alpha_root=alpha,
                trading_system_root=trading,
            ).compile()

        marks = [
            item for item in inventory.requirements
            if item.feed is DemandFeed.MARK_INDEX_PRICE
        ]
        self.assertEqual(len(inventory.requirements), 72)
        self.assertEqual(len(marks), 4)
        self.assertTrue(all(item.purpose.value == "EXECUTION" for item in marks))
        self.assertTrue(all(type(item).from_proto(item.to_proto()) == item for item in marks))
        books = [
            item for item in inventory.requirements
            if item.feed in {DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA}
        ]
        self.assertEqual(len(books), 8)
        self.assertEqual({item.depth_levels for item in books}, {100})
        self.assertTrue(all(item.require_live for item in books))
        self.assertEqual(
            {item.max_freshness_ms for item in books if item.feed is DemandFeed.BOOK_SNAPSHOT},
            {60_000},
        )
        self.assertEqual(
            {item.max_freshness_ms for item in books if item.feed is DemandFeed.BOOK_DELTA},
            {2_000},
        )

    @staticmethod
    def _trade_requirement(instrument_uid: str) -> DataRequirement:
        return DataRequirement(
            instrument_uid=instrument_uid,
            feed=Feed.TRADE,
            consumer_grade=Grade.ALPHA,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=15_000,
            max_session_liveness_ms=45_000,
            event_recency_policy=StalePolicy.OBSERVE,
            stale_policy=StalePolicy.BLOCK,
        )

    @staticmethod
    def _product(requirement: DataRequirement) -> SimpleNamespace:
        return SimpleNamespace(
            delivery=DeliveryClass.DURABLE,
            feed=Feed.TRADE,
            requirement=requirement,
        )

    @staticmethod
    def _status(
        requirement: DataRequirement,
        *,
        instrument_uid: str | None = None,
        provider_session_state: str = "LIVE",
        gap_open: bool = False,
    ) -> FeedStatusResponse:
        return FeedStatusResponse.model_validate({
            "schema": "qdl.feed-status.v2",
            "instrument_uid": instrument_uid or requirement.instrument_uid,
            "feed": "TRADE",
            "quality": {
                "state": "LIVE",
                "freshness_ms": 15_001,
                "event_recency_state": "STALE",
                "provider_session_state": provider_session_state,
                "provider_session_liveness_ms": 1,
                "gap_open": gap_open,
                "complete": True,
                "execution_eligible": False,
                "policy_id": "crypto_primary_v2",
                "flags": [],
            },
        })

    def test_five_okx_trade_statuses_are_quiet_safe_and_symbol_isolated(self) -> None:
        """A quiet connection is valid only for its exact five-liquid identity."""
        for symbol, instrument_uid in OKX_TRADE_UIDS.items():
            with self.subTest(symbol=symbol):
                requirement = self._trade_requirement(instrument_uid)
                product = self._product(requirement)
                self.assertTrue(
                    _quiet_trade_status_is_observable(
                        product, requirement, self._status(requirement)
                    )
                )
                other_uid = next(
                    value for value in OKX_TRADE_UIDS.values() if value != instrument_uid
                )
                self.assertFalse(
                    _quiet_trade_status_is_observable(
                        product,
                        requirement,
                        self._status(requirement, instrument_uid=other_uid),
                    )
                )

    def test_five_okx_trade_statuses_fail_closed_for_disconnect_or_gap(self) -> None:
        for symbol, instrument_uid in OKX_TRADE_UIDS.items():
            with self.subTest(symbol=symbol, failure="disconnected"):
                requirement = self._trade_requirement(instrument_uid)
                self.assertFalse(
                    _quiet_trade_status_is_observable(
                        self._product(requirement),
                        requirement,
                        self._status(
                            requirement, provider_session_state="DISCONNECTED"
                        ),
                    )
                )
            with self.subTest(symbol=symbol, failure="gap"):
                requirement = self._trade_requirement(instrument_uid)
                self.assertFalse(
                    _quiet_trade_status_is_observable(
                        self._product(requirement),
                        requirement,
                        self._status(requirement, gap_open=True),
                    )
                )

    def test_five_okx_trade_reconnect_rejects_duplicate_or_stale_cursor(self) -> None:
        """Each symbol keeps a strictly advancing durable resume cursor."""
        for index, symbol in enumerate(OKX_TRADE_UIDS):
            with self.subTest(symbol=symbol, result="reconnect"):
                acknowledged = 1_000 + index * 10
                validate_resume_offsets(
                    acknowledged_offset=acknowledged,
                    resumed_offset=acknowledged + 1,
                )
            for invalid_offset, failure in (
                (acknowledged, "duplicate"),
                (acknowledged - 1, "stale"),
            ):
                with self.subTest(symbol=symbol, failure=failure):
                    with self.assertRaisesRegex(ValueError, "strictly increasing"):
                        validate_resume_offsets(
                            acknowledged_offset=acknowledged,
                            resumed_offset=invalid_offset,
                        )


if __name__ == "__main__":
    unittest.main()
