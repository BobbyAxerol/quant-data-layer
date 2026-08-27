"""Source-only Phase 11.4 L2 demand-plan coverage.

Every provider payload in this module is explicit test metadata. The tests
exercise control-plane identity, depth, TTL and shared topology only; no
socket, REST request, durable write, runtime role or order action occurs.
"""

from __future__ import annotations

import unittest

from qdl.demand import (
    ActiveDemandInventory,
    AdmissionBudget,
    AdmissionPolicy,
    DataRequirement,
    DemandFeed,
    DemandPurpose,
    UniverseSelector,
    UniverseSelectorKind,
    admit_provider_metadata,
    converge_active_demand,
)
from qdl.runtime.l2_demand import build_l2_demand_plan


def _requirement(
    *,
    consumer_id: str,
    venue: str,
    market: str,
    product_type: str,
    symbol: str,
    feed: DemandFeed = DemandFeed.BOOK_SNAPSHOT,
    depth: int = 100,
    source_policy_id: str = "crypto_primary_v2",
    execution_grade: bool = False,
    purpose: DemandPurpose = DemandPurpose.ALPHA,
) -> DataRequirement:
    return DataRequirement(
        consumer_id=consumer_id,
        purpose=purpose,
        universe=UniverseSelector(
            selector_id=f"{consumer_id}:{symbol}:{feed.value}",
            kind=UniverseSelectorKind.EXPLICIT,
            venue=venue,
            market=market,
            product_type=product_type,
            native_symbols=(symbol,),
        ),
        feed=feed,
        source_policy_id=source_policy_id,
        max_freshness_ms=60_000,
        priority=100,
        ttl_seconds=180,
        require_live=True,
        execution_grade=execution_grade,
        depth_levels=depth,
        configuration_revision=114,
    )


def _inventory(*requirements: DataRequirement) -> ActiveDemandInventory:
    return ActiveDemandInventory(
        revision=114,
        requirements=requirements,
        source_documents=(),
        candidates=(),
        exclusions=(),
        input_sha256="a" * 64,
    )


def _metadata() -> dict[tuple[str, str], object]:
    return {
        ("BINANCE", "USDM"): {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                    ],
                },
                {
                    "symbol": "BTCUSDT_260925",
                    "status": "TRADING",
                    "contractType": "CURRENT_QUARTER",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "deliveryDate": 1_790_000_000_000,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                    ],
                },
            ]
        },
        ("OKX", "SWAP"): [
            {
                "instType": "SWAP",
                "instId": "BTC-USDT-SWAP",
                "instFamily": "BTC-USDT",
                "ctValCcy": "BTC",
                "settleCcy": "USDT",
                "ctVal": "0.01",
                "ctMult": "1",
                "tickSz": "0.01",
                "lotSz": "0.001",
                "state": "live",
            }
        ],
    }


def _policy() -> AdmissionPolicy:
    return AdmissionPolicy(
        max_subscriptions_per_connection=200,
        max_total_slices=16,
        budgets=(
            AdmissionBudget("BINANCE", "USDM", DemandFeed.BOOK_SNAPSHOT, 8),
            AdmissionBudget("BINANCE", "USDM", DemandFeed.BOOK_DELTA, 8),
            AdmissionBudget("OKX", "SWAP", DemandFeed.BOOK_SNAPSHOT, 8),
            AdmissionBudget("OKX", "SWAP", DemandFeed.BOOK_DELTA, 8),
        ),
    )


class Phase114L2DemandPlanTests(unittest.TestCase):
    def test_multifeed_books_share_one_binding_per_instrument_and_role(self):
        inventory = _inventory(
            _requirement(
                consumer_id="basis-arb",
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                symbol="BTCUSDT",
            ),
            _requirement(
                consumer_id="basis-arb",
                venue="BINANCE",
                market="USDM",
                product_type="FUTURE",
                symbol="BTCUSDT_260925",
            ),
            _requirement(
                consumer_id="grid-observer",
                venue="OKX",
                market="SWAP",
                product_type="PERPETUAL",
                symbol="BTC-USDT-SWAP",
            ),
            _requirement(
                consumer_id="grid-observer",
                venue="OKX",
                market="SWAP",
                product_type="PERPETUAL",
                symbol="BTC-USDT-SWAP",
                feed=DemandFeed.BOOK_DELTA,
                depth=50,
            ),
        )
        admission = admit_provider_metadata(inventory, _metadata())
        convergence = converge_active_demand(inventory, admission, _policy())
        plan = build_l2_demand_plan(
            inventory=inventory, admission=admission, convergence=convergence
        )

        self.assertEqual(len(plan.bindings), 3)
        self.assertEqual(plan.topology.service_role_count, 2)
        self.assertEqual(
            set(plan.topology.runtime_roles), {("BINANCE", "USDM"), ("OKX", "SWAP")}
        )
        self.assertEqual(len(plan.topology.subscriptions), 3)
        binance = [item for item in plan.bindings if item.venue == "BINANCE"]
        self.assertEqual(len(binance), 2)
        self.assertTrue(
            all(
                item.provider_protocol == "BINANCE_DIFF_DEPTH"
                and item.native_channel.endswith("@depth@100ms")
                and item.rest_snapshot_url == "https://fapi.binance.com/fapi/v1/depth"
                and item.requested_depth_per_side == 100
                for item in binance
            )
        )
        okx = next(item for item in plan.bindings if item.venue == "OKX")
        self.assertEqual(okx.provider_protocol, "OKX_PUBLIC_BOOKS")
        self.assertEqual(okx.native_channel, "books")
        self.assertIsNone(okx.rest_snapshot_url)
        self.assertEqual(set(okx.feeds), {DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA})
        self.assertEqual(okx.requested_depth_per_side, 100)
        self.assertFalse(plan.report_payload()["runtime_applied"])

    def test_basis_grid_and_reactive_consumers_share_one_physical_book(self):
        inventory = _inventory(
            _requirement(
                consumer_id="basis-arb-read-only",
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                symbol="BTCUSDT",
                feed=DemandFeed.BOOK_SNAPSHOT,
                depth=100,
            ),
            _requirement(
                consumer_id="reactive-grid-read-only",
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                symbol="BTCUSDT",
                feed=DemandFeed.BOOK_DELTA,
                depth=80,
                execution_grade=True,
                purpose=DemandPurpose.EXECUTION,
            ),
            _requirement(
                consumer_id="limit-price-read-only",
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                symbol="BTCUSDT",
                feed=DemandFeed.BOOK_SNAPSHOT,
                depth=20,
                execution_grade=True,
                purpose=DemandPurpose.EXECUTION,
            ),
        )
        admission = admit_provider_metadata(inventory, _metadata())
        convergence = converge_active_demand(inventory, admission, _policy())
        plan = build_l2_demand_plan(
            inventory=inventory, admission=admission, convergence=convergence
        )
        self.assertEqual(len(plan.bindings), 1)
        binding = plan.bindings[0]
        self.assertEqual(
            binding.consumer_ids,
            ("basis-arb-read-only", "limit-price-read-only", "reactive-grid-read-only"),
        )
        self.assertEqual(binding.requested_depth_per_side, 100)
        self.assertTrue(binding.execution_grade)
        self.assertEqual(plan.topology.connection_count, 1)
        self.assertEqual(plan.topology.service_role_count, 1)
        self.assertFalse(plan.report_payload()["runtime_applied"])

    def test_book_without_explicit_depth_is_rejected_before_planning(self):
        inventory = _inventory(
            _requirement(
                consumer_id="invalid-book",
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                symbol="BTCUSDT",
                depth=0,
            )
        )
        admission = admit_provider_metadata(inventory, _metadata())
        convergence = converge_active_demand(inventory, admission, _policy())
        with self.assertRaisesRegex(ValueError, "explicit positive depth"):
            build_l2_demand_plan(
                inventory=inventory, admission=admission, convergence=convergence
            )


if __name__ == "__main__":
    unittest.main()
