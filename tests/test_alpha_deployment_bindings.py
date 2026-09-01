from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

import yaml

from qdl.consumer.universal_release import ConsumerRouteBinding


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/compile_alpha_deployment_bindings.py"


def _module():
    spec = importlib.util.spec_from_file_location("alpha_deployment_bindings", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class AlphaDeploymentBindingCompilerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tool = _module()
        cls.catalog_path = ROOT / "config/v2/stable-source-bindings.yaml"
        cls.reference_path = ROOT / "consumers/stable/reference-l2-stable.yaml"
        cls.release_path = ROOT / "config/v2/stable-v2-release-routing.yaml"
        cls.policy_path = ROOT / "config/v2/universal-release-policy.yaml"
        cls.catalog = _load(cls.catalog_path)
        cls.reference = _load(cls.reference_path)
        cls.release = _load(cls.release_path)
        cls.policy = _load(cls.policy_path)

    @staticmethod
    def _route(
        *,
        venue: str,
        market: str,
        symbol: str,
        feed: str,
        interval: str | None = None,
        source_policy_id: str = "crypto_primary_v2",
        provider_plane: str = "REALTIME",
        fallback: str = "BLOCKED",
        fallback_rule_id: str | None = None,
        blocked_reason: str | None = "V1_EQUIVALENCE_UNPROVEN",
        freshness: int = 15_000,
        final: bool = False,
        live: bool = True,
    ) -> dict[str, object]:
        return {
            "venue": venue,
            "market": market,
            "product_type": "PERPETUAL",
            "native_symbol": symbol,
            "feed": feed,
            "interval": interval,
            "source_policy_id": source_policy_id,
            "provider_plane": provider_plane,
            "max_freshness_ms": freshness,
            "require_final_bars": final,
            "require_live": live,
            "gap_policy": "BLOCK",
            "fallback": fallback,
            "fallback_rule_id": fallback_rule_id,
            "blocked_reason": blocked_reason,
        }

    def _deployment(
        self,
        *,
        deployment_id: str,
        alpha_id: str,
        profile: str,
        status: str = "DECLARED",
        maxlen: int = 2_500,
        min_bars: int = 200,
        routes: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "deployment_id": deployment_id,
            "alpha_id": alpha_id,
            "alpha_directory": alpha_id.split("_")[0],
            "profile": profile,
            "mode": "paper",
            "status": status,
            "blocked_reason": None,
            "compose": {"path": "alphas/example/docker-compose.yml", "service": alpha_id, "sha256": "a" * 64},
            "sources": {"strategy_config_path": "alphas/example/config.yaml", "strategy_config_sha256": "b" * 64},
            "history": {"maxlen": maxlen, "min_bars": min_bars},
            "routes": routes,
        }

    def _inventory(self, deployments: list[dict[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {
            "schema": "execution-alpha.data-requirements.v1",
            "revision": 1,
            "registry_path": "runtime/config/v2_alpha_deployment_requirements.yaml",
            "registry_sha256": "c" * 64,
            "deployments": deployments,
        }
        value["inventory_sha256"] = self.tool._digest(value)
        return value

    def _compile(self, inventory: dict[str, object]) -> dict[str, object]:
        return self.tool.compile_inventory(
            inventory=inventory,
            catalog=self.catalog,
            reference_manifest=self.reference,
            release_routing=self.release,
            release_routing_sha256=self.tool._sha256_file(self.release_path),
            policy=self.policy,
            policy_sha256=self.tool._sha256_file(self.policy_path),
            catalog_sha256=self.tool._sha256_file(self.catalog_path),
            reference_manifest_sha256=self.tool._sha256_file(self.reference_path),
        )

    def test_compiles_native_binance_and_okx_bindings_with_standard_contract(self) -> None:
        binance = self._deployment(
            deployment_id="adaptive:binance:paper",
            alpha_id="adaptive_hma_cpp_00115m",
            profile="directional_bar",
            maxlen=2_500,
            min_bars=400,
            routes=[self._route(
                venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="BAR", interval="15m",
                freshness=180_000, final=True, live=False,
                blocked_reason="V1_FINAL_BAR_EQUIVALENCE_UNPROVEN",
            )],
        )
        okx = self._deployment(
            deployment_id="adaptive:okx:no-order",
            alpha_id="adaptive_hma_cpp_00115m",
            profile="directional_bar",
            status="DECLARED_NO_ORDER_PROBE",
            maxlen=5_000,
            min_bars=400,
            routes=[self._route(
                venue="OKX", market="SWAP", symbol="ETH-USDT-SWAP", feed="BAR", interval="15m",
                freshness=180_000, final=True, live=False,
                blocked_reason="V1_FINAL_BAR_EQUIVALENCE_UNPROVEN",
            )],
        )
        report = self._compile(self._inventory([binance, okx]))
        self.assertEqual(
            [item["status"] for item in report["deployments"]],
            ["ADMITTED", "ADMITTED"],
            msg=str({item["deployment_id"]: item["reason"] for item in report["deployments"]}),
        )
        for item in report["deployments"]:
            binding = ConsumerRouteBinding.from_canonical_mapping(item["binding"])
            self.assertEqual(len(binding.products), 1)
            product = binding.products[0]
            self.assertEqual(product.feed, "BAR")
            self.assertEqual(product.interval, "15m")
            self.assertTrue(product.require_final_bars)
            self.assertFalse(product.execution_grade)
            self.assertEqual(product.fallback, "BLOCKED")
        self.assertEqual(report["deployments"][0]["history"], {"maxlen": 2_500, "min_bars": 400})
        self.assertEqual(report["deployments"][1]["history"], {"maxlen": 5_000, "min_bars": 400})

    def test_grid_l2_and_binance_basis_reference_are_admitted_without_execution_grade(self) -> None:
        grid_routes = [
            self._route(
                venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="BAR", interval="1h",
                freshness=180_000, final=True, live=False,
                blocked_reason="V1_FINAL_BAR_EQUIVALENCE_UNPROVEN",
            ),
            self._route(
                venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="TRADE",
                fallback="V1", fallback_rule_id="V1_TRADE_EQUIVALENCE_CERTIFIED", blocked_reason=None,
            ),
            self._route(venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="QUOTE"),
            self._route(
                venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="MARK_INDEX_PRICE",
                source_policy_id="crypto_liquid_v2", provider_plane="REFERENCE",
            ),
            self._route(
                venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="BOOK_SNAPSHOT",
                source_policy_id="crypto_liquid_v2", provider_plane="L2", freshness=60_000,
            ),
            self._route(
                venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="BOOK_DELTA",
                source_policy_id="crypto_liquid_v2", provider_plane="L2", freshness=2_000,
            ),
        ]
        basis_routes = [
            self._route(
                venue="BINANCE", market="USDM", symbol="BTCUSDT", feed=feed,
                interval="1d" if feed in {"OPEN_INTEREST", "LONG_SHORT_RATIO", "TAKER_FLOW", "BASIS"} else None,
                source_policy_id="crypto_liquid_v2", provider_plane="REFERENCE",
                freshness=86_400_000, live=False,
            )
            for feed in (
                "FUNDING_RATE", "OPEN_INTEREST", "LONG_SHORT_RATIO", "TAKER_FLOW",
                "MARK_INDEX_PRICE", "CONTRACT_METADATA", "BASIS",
            )
        ]
        report = self._compile(self._inventory([
            self._deployment(
                deployment_id="grid:binance:paper", alpha_id="dynamic_grid_0011h",
                profile="grid_l2", maxlen=5_000, min_bars=100, routes=grid_routes,
            ),
            self._deployment(
                deployment_id="basis:binance:paper", alpha_id="basis_arb_binance_0011d",
                profile="basis_reference", maxlen=10_000, min_bars=365, routes=basis_routes,
            ),
        ]))
        self.assertEqual(
            [item["status"] for item in report["deployments"]],
            ["ADMITTED", "ADMITTED"],
            msg=str({item["deployment_id"]: item["reason"] for item in report["deployments"]}),
        )
        compiled = {item["deployment_id"]: item for item in report["deployments"]}
        grid = ConsumerRouteBinding.from_canonical_mapping(compiled["grid:binance:paper"]["binding"])
        self.assertEqual({item.feed for item in grid.products}, {
            "BAR", "TRADE", "QUOTE", "MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA",
        })
        self.assertTrue(all(not item.execution_grade for item in grid.products))
        basis = ConsumerRouteBinding.from_canonical_mapping(compiled["basis:binance:paper"]["binding"])
        self.assertEqual(len(basis.products), 7)
        self.assertEqual({item.provider_plane for item in basis.products}, {"REFERENCE"})
        self.assertEqual(compiled["basis:binance:paper"]["history"]["maxlen"], 10_000)

    def test_missing_or_invalid_required_capability_blocks_whole_deployment(self) -> None:
        unsupported_okx_basis = self._deployment(
            deployment_id="basis:okx:no-order", alpha_id="basis_arb_binance_0011d",
            profile="basis_reference", status="DECLARED_NO_ORDER_PROBE", routes=[self._route(
                venue="OKX", market="SWAP", symbol="BTC-USDT-SWAP", feed="BASIS", interval="1d",
                source_policy_id="crypto_liquid_v2", provider_plane="REFERENCE", freshness=86_400_000, live=False,
            )],
        )
        unknown = self._deployment(
            deployment_id="unknown:binance:paper", alpha_id="adaptive_hma_cpp_unknown",
            profile="directional_bar", routes=[self._route(
                venue="BINANCE", market="USDM", symbol="UNKNOWNUSDT", feed="BAR", interval="15m",
                freshness=180_000, final=True, live=False,
            )],
        )
        invalid_fallback = self._deployment(
            deployment_id="invalid:binance:paper", alpha_id="dynamic_grid_invalid",
            profile="grid_l2", routes=[self._route(
                venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="QUOTE",
                fallback="V1", fallback_rule_id="INVALID", blocked_reason=None,
            )],
        )
        out_of_bounds = self._deployment(
            deployment_id="long:binance:paper", alpha_id="adaptive_hma_cpp_long",
            profile="directional_bar", maxlen=10_001, routes=[self._route(
                venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="BAR", interval="15m",
                freshness=180_000, final=True, live=False,
            )],
        )
        report = self._compile(self._inventory([unsupported_okx_basis, unknown, invalid_fallback, out_of_bounds]))
        blocked = {item["deployment_id"]: item["reason"] for item in report["deployments"]}
        self.assertTrue(blocked["basis:okx:no-order"].startswith("CAPABILITY_UNAVAILABLE:BASIS"))
        self.assertTrue(blocked["unknown:binance:paper"].startswith("CATALOG_IDENTITY_UNAVAILABLE"))
        self.assertIn("V1 fallback is allowed only", blocked["invalid:binance:paper"])
        self.assertIn("must not exceed 10000", blocked["long:binance:paper"])
        self.assertTrue(all(item["binding"] is None for item in report["deployments"]))

    def test_optional_unavailable_metric_is_explicit_and_never_becomes_zero(self) -> None:
        required_bar = self._route(
            venue="OKX", market="SWAP", symbol="BTC-USDT-SWAP", feed="BAR", interval="1h",
            freshness=180_000, final=True, live=False,
        )
        optional_basis = self._route(
            venue="OKX", market="SWAP", symbol="BTC-USDT-SWAP", feed="BASIS", interval="1d",
            source_policy_id="crypto_liquid_v2", provider_plane="REFERENCE",
            freshness=86_400_000, live=False,
        )
        optional_basis["required"] = False
        deployment = self._deployment(
            deployment_id="optional:okx:no-order", alpha_id="basis_arb_binance_0011d",
            profile="basis_reference", status="DECLARED_NO_ORDER_PROBE",
            routes=[required_bar, optional_basis],
        )

        report = self._compile(self._inventory([deployment]))
        rendered = report["deployments"][0]
        self.assertEqual(rendered["status"], "ADMITTED")
        self.assertEqual(rendered["optional_unavailable"], [{
            "venue": "OKX",
            "market": "SWAP",
            "product_type": "PERPETUAL",
            "native_symbol": "BTC-USDT-SWAP",
            "feed": "BASIS",
            "interval": "1d",
            "source_policy_id": "crypto_liquid_v2",
            "reason": "CAPABILITY_UNAVAILABLE:BASIS:crypto_liquid_v2",
        }])
        binding = ConsumerRouteBinding.from_canonical_mapping(rendered["binding"])
        self.assertEqual([(product.feed, product.interval) for product in binding.products], [("BAR", "1h")])

        only_optional = deepcopy(deployment)
        only_optional["deployment_id"] = "optional-only:okx:no-order"
        only_optional["routes"] = [optional_basis]
        no_product = self._compile(self._inventory([only_optional]))["deployments"][0]
        self.assertEqual(no_product["status"], "BLOCKED")
        self.assertEqual(no_product["reason"], "NO_ADMITTED_REQUIRED_ROUTE")
        self.assertEqual(no_product["optional_unavailable"], [])

    def test_rejects_tampered_inventory_and_non_boolean_policy(self) -> None:
        deployment = self._deployment(
            deployment_id="tampered:binance:paper", alpha_id="adaptive_hma_cpp_tampered",
            profile="directional_bar", routes=[self._route(
                venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="BAR", interval="15m",
                freshness=180_000, final=True, live=False,
            )],
        )
        inventory = self._inventory([deployment])
        tampered = deepcopy(inventory)
        tampered["deployments"][0]["history"]["maxlen"] = 5_000
        with self.assertRaisesRegex(self.tool.DeploymentBindingError, "inventory checksum differs"):
            self._compile(tampered)

        invalid = deepcopy(inventory)
        invalid["deployments"][0]["routes"][0]["require_final_bars"] = "true"
        invalid["inventory_sha256"] = self.tool._digest({
            key: value for key, value in invalid.items() if key != "inventory_sha256"
        })
        report = self._compile(invalid)
        self.assertEqual(report["deployments"][0]["status"], "BLOCKED")
        self.assertIn("must be boolean", report["deployments"][0]["reason"])

        invalid_required = deepcopy(inventory)
        invalid_required["deployments"][0]["routes"][0]["required"] = "false"
        invalid_required["inventory_sha256"] = self.tool._digest({
            key: value for key, value in invalid_required.items() if key != "inventory_sha256"
        })
        report = self._compile(invalid_required)
        self.assertEqual(report["deployments"][0]["status"], "BLOCKED")
        self.assertIn("route required must be boolean", report["deployments"][0]["reason"])

    def test_is_deterministic_and_write_scope_is_named_output_only(self) -> None:
        deployment = self._deployment(
            deployment_id="adaptive:binance:paper", alpha_id="adaptive_hma_cpp_00115m",
            profile="directional_bar", routes=[self._route(
                venue="BINANCE", market="USDM", symbol="ETHUSDT", feed="BAR", interval="15m",
                freshness=180_000, final=True, live=False,
            )],
        )
        inventory = self._inventory([deployment])
        first = self._compile(inventory)
        second = self._compile(deepcopy(inventory))
        self.assertEqual(first, second)
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "bindings"
            changed = self.tool.write_compilation(output, first)
            self.assertEqual(len(changed), 2)
            self.assertEqual(self.tool.write_compilation(output, first), ())
            binding_paths = list(output.glob("*.binding.json"))
            self.assertEqual(len(binding_paths), 1)
            payload = json.loads(binding_paths[0].read_text(encoding="utf-8"))
            self.assertEqual(
                ConsumerRouteBinding.from_canonical_mapping(payload).canonical_mapping(),
                payload,
            )


if __name__ == "__main__":
    unittest.main()
