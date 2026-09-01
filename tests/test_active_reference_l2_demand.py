"""Source-only coverage for the active V2 reference and L2 demand surface."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import yaml

from qdl.consumer.universal_release import (
    UniversalConsumerClass,
    UniversalReleasePolicy,
)
from qdl.demand import (
    ActiveDemandCompiler,
    ActiveDemandInventory,
    ActiveDemandSourceRegistry,
    DemandFeed,
    DemandManifest,
    InventoryError,
    admit_provider_metadata,
    converge_active_demand,
)
from qdl.runtime.l2_demand import build_l2_demand_plan
from scripts.phase111_active_demand_inventory import fetch_provider_metadata


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "config/v2/stable-reference-l2-demand.yaml"
REGISTRY_PATH = ROOT / "config/v2/active-demand-source-registry.yaml"
RELEASE_POLICY_PATH = ROOT / "config/v2/universal-release-policy.yaml"
CONSUMER_ID = "qdl.crypto.reference-l2.stable"


def _binance_symbol(
    symbol: str,
    *,
    base: str,
    contract_type: str = "PERPETUAL",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "status": "TRADING",
        "contractType": contract_type,
        "baseAsset": base,
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "deliveryDate": 1_800_000_000_000 if contract_type != "PERPETUAL" else 0,
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
        ],
    }


def _okx_swap(base: str) -> dict[str, str]:
    return {
        "instType": "SWAP",
        "instId": f"{base}-USDT-SWAP",
        "instFamily": f"{base}-USDT",
        "baseCcy": "",
        "quoteCcy": "USDT",
        "settleCcy": "USDT",
        "ctVal": "0.01",
        "ctMult": "1",
        "ctValCcy": base,
        "tickSz": "0.01",
        "lotSz": "0.001",
        "state": "live",
        "expTime": "",
    }


def _okx_future(base: str, alias: str, expiry: str) -> dict[str, str]:
    return {
        "instType": "FUTURES",
        "instId": f"{base}-USD-{expiry}",
        "instFamily": f"{base}-USD",
        "baseCcy": base,
        "quoteCcy": "USD",
        "settleCcy": "USD",
        "ctVal": "100" if base == "BTC" else "10",
        "ctMult": "1",
        "ctValCcy": base,
        "tickSz": "0.1",
        "lotSz": "1",
        "state": "live",
        "expTime": "1800000000000",
        "alias": alias,
    }


def _metadata_payloads() -> dict[tuple[str, str], object]:
    binance = [
        _binance_symbol(f"{base}USDT", base=base)
        for base in ("BTC", "ETH", "SOL", "DOGE", "BNB")
    ]
    for base in ("BTC", "ETH"):
        binance.extend(
            (
                _binance_symbol(
                    f"{base}USDT_260925",
                    base=base,
                    contract_type="CURRENT_QUARTER",
                ),
                _binance_symbol(
                    f"{base}USDT_261225",
                    base=base,
                    contract_type="NEXT_QUARTER",
                ),
            )
        )
    return {
        ("BINANCE", "USDM"): {"symbols": binance},
        ("OKX", "SWAP"): [_okx_swap(base) for base in ("BTC", "ETH", "SOL", "DOGE", "BNB")],
        ("OKX", "FUTURES"): [
            _okx_future("BTC", "quarter", "260925"),
            _okx_future("BTC", "next_quarter", "261225"),
            _okx_future("ETH", "quarter", "260925"),
            _okx_future("ETH", "next_quarter", "261225"),
        ],
    }


class ActiveReferenceL2DemandTests(unittest.TestCase):
    @staticmethod
    def _inventory() -> ActiveDemandInventory:
        manifest = DemandManifest.load_many((MANIFEST_PATH,))
        return ActiveDemandInventory(
            revision=2,
            requirements=manifest.requirements,
            source_documents=(),
            candidates=(),
            exclusions=(),
            input_sha256="a" * 64,
        )

    def test_manifest_declares_five_reference_perpetuals_and_continuous_l2(self):
        manifest = DemandManifest.load_many((MANIFEST_PATH,))
        self.assertEqual(len(manifest.requirements), 33)
        reference = [
            item
            for item in manifest.requirements
            if item.feed
            not in {DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA}
        ]
        books = [
            item
            for item in manifest.requirements
            if item.feed in {DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA}
        ]
        self.assertEqual(len(reference), 13)
        self.assertEqual(len(books), 20)
        self.assertEqual({item.consumer_id for item in manifest.requirements}, {CONSUMER_ID})
        self.assertEqual(
            {
                (item.universe.venue, item.universe.market)
                for item in reference
            },
            {("BINANCE", "USDM"), ("OKX", "SWAP")},
        )
        self.assertFalse(any(
            item.universe.venue == "OKX"
            and item.universe.market == "SWAP"
            and item.feed is DemandFeed.BASIS
            for item in reference
        ))
        continuous = [item for item in books if item.universe.kind.value == "CONTINUOUS"]
        self.assertEqual(len(continuous), 16)
        self.assertTrue(all(not item.universe.native_symbols for item in continuous))
        self.assertEqual(
            {
                (item.universe.venue, item.universe.market)
                for item in continuous
            },
            {("BINANCE", "USDM"), ("OKX", "FUTURES")},
        )

    def test_registered_universal_source_compiles_without_runtime_access(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            repository = root / "repository"
            alpha = root / "execution_alpha"
            trading = root / "trading_system"
            (repository / "config/v2").mkdir(parents=True)
            (trading / "config/_config").mkdir(parents=True)
            alpha.mkdir()
            (repository / "config/v2/stable-reference-l2-demand.yaml").write_bytes(
                MANIFEST_PATH.read_bytes()
            )
            (trading / "config/_config/portfolio_account_config_setup.yaml").write_text(
                "alphas:\n  - alpha_id: fixture_alpha\n    allowed_venues: [BINANCE]\n",
                encoding="utf-8",
            )
            registry = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
            registry["sources"] = [
                item
                for item in registry["sources"]
                if item["source_id"] == "stable-reference-l2-demand"
            ]
            registry_path = repository / "config/v2/registry.yaml"
            registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

            inventory = ActiveDemandCompiler(
                registry=ActiveDemandSourceRegistry.load(registry_path),
                repository_root=repository,
                execution_alpha_root=alpha,
                trading_system_root=trading,
            ).compile()

        self.assertEqual(len(inventory.requirements), 33)
        self.assertEqual(
            {item.source_kind for item in inventory.candidates},
            {"UNIVERSAL_DEMAND_V1"},
        )

    def test_admission_and_l2_plan_resolve_exactly_twelve_physical_books(self):
        inventory = self._inventory()
        admission = admit_provider_metadata(inventory, _metadata_payloads())
        self.assertTrue(admission.passed)
        self.assertEqual(len(admission.rows), 89)
        convergence = converge_active_demand(
            inventory,
            admission,
            ActiveDemandSourceRegistry.load(REGISTRY_PATH).admission_policy,
        )
        self.assertTrue(convergence.passed)
        plan = build_l2_demand_plan(
            inventory=inventory,
            admission=admission,
            convergence=convergence,
        )
        self.assertEqual(len(plan.bindings), 12)
        self.assertEqual(len(plan.topology.subscriptions), 12)
        self.assertEqual(plan.topology.service_role_count, 3)
        self.assertEqual(
            set(plan.topology.runtime_roles),
            {("BINANCE", "USDM"), ("OKX", "SWAP"), ("OKX", "FUTURES")},
        )
        self.assertTrue(
            all(
                set(item.feeds) == {DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA}
                and item.requested_depth_per_side == 100
                for item in plan.bindings
            )
        )
        self.assertFalse(plan.report_payload()["runtime_applied"])

    def test_okx_quarter_alias_missing_or_ambiguous_fails_closed(self):
        requirement = next(
            item
            for item in self._inventory().requirements
            if item.universe.venue == "OKX"
            and item.universe.market == "FUTURES"
            and item.universe.continuous_family == "BTC-USD"
            and item.universe.continuous_roll_policy == "CURRENT_QUARTER"
        )
        inventory = ActiveDemandInventory(
            revision=2,
            requirements=(requirement,),
            source_documents=(),
            candidates=(),
            exclusions=(),
            input_sha256="b" * 64,
        )
        missing = admit_provider_metadata(
            inventory,
            {("OKX", "FUTURES"): [_okx_future("BTC", "next_quarter", "261225")]},
        )
        self.assertEqual(missing.rows[0].state, "MISSING_CONTINUOUS_CONTRACT")
        ambiguous = admit_provider_metadata(
            inventory,
            {
                ("OKX", "FUTURES"): [
                    _okx_future("BTC", "quarter", "260925"),
                    _okx_future("BTC", "quarter", "260926"),
                ]
            },
        )
        self.assertEqual(ambiguous.rows[0].state, "AMBIGUOUS_CONTINUOUS_CONTRACT")

    def test_okx_futures_metadata_fetch_uses_the_documented_market_parameter(self):
        requirement = next(
            item
            for item in self._inventory().requirements
            if item.universe.venue == "OKX" and item.universe.market == "FUTURES"
        )
        inventory = ActiveDemandInventory(
            revision=2,
            requirements=(requirement,),
            source_documents=(),
            candidates=(),
            exclusions=(),
            input_sha256="c" * 64,
        )
        calls: list[tuple[str, dict[str, str]]] = []

        class Response:
            def raise_for_status(self) -> None:
                return None

            @staticmethod
            def json() -> dict[str, object]:
                return {"code": "0", "data": [_okx_future("BTC", "quarter", "260925")]}

        def get(url: str, *, params: dict[str, str], **_kwargs: object) -> Response:
            calls.append((url, params))
            return Response()

        payloads = fetch_provider_metadata(
            inventory,
            timeout_seconds=1.0,
            attempts=1,
            get=get,
            sleep=lambda _: self.fail("no retry is expected"),
        )
        self.assertEqual(calls, [("https://www.okx.com/api/v5/public/instruments", {"instType": "FUTURES"})])
        self.assertEqual(set(payloads), {("OKX", "FUTURES")})

    def test_universal_release_policy_classifies_the_reference_l2_product(self):
        policy = UniversalReleasePolicy.load(RELEASE_POLICY_PATH, manifest_root=ROOT)
        self.assertEqual(
            policy.classify(CONSUMER_ID),
            UniversalConsumerClass.REFERENCE_L2,
        )
        with self.assertRaises(ValueError):
            policy.classify("qdl.crypto.reference-l2-typo")


if __name__ == "__main__":
    unittest.main()
