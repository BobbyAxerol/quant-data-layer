from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from qdl.demand import (
    ActiveDemandInventory,
    AdmissionBudget,
    AdmissionPolicy,
    DataRequirement,
    DemandFeed,
    DemandPurpose,
    DemandTopologyPlanner,
    UniverseSelector,
    UniverseSelectorKind,
    admit_provider_metadata,
    converge_active_demand,
)
from qdl.demand.topology import DemandTopology
from qdl.ingestion.contracts import plan_shards
from qdl.runtime.production_catalog import ProductionCatalogBuilder
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl.runtime.universal_realtime import (
    build_universal_realtime_plan,
    provider_realtime_bindings,
)


def _binance_usdm(symbol: str, *, contract_type: str = "PERPETUAL") -> dict[str, object]:
    return {
        "symbol": symbol,
        "status": "TRADING",
        "contractType": contract_type,
        "baseAsset": symbol.split("USDT", 1)[0].split("_", 1)[0],
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "deliveryDate": 1_800_000_000_000 if contract_type != "PERPETUAL" else 0,
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
        ],
    }


def _binance_spot(symbol: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "status": "TRADING",
        "baseAsset": symbol.removesuffix("USDT"),
        "quoteAsset": "USDT",
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
        ],
    }


def _okx(inst_type: str, inst_id: str) -> dict[str, str]:
    if inst_type == "SPOT":
        base, quote = inst_id.split("-", 1)
        return {
            "instType": "SPOT", "instId": inst_id, "baseCcy": base,
            "quoteCcy": quote, "tickSz": "0.01", "lotSz": "0.001", "state": "live",
        }
    base = inst_id.split("-", 1)[0]
    return {
        "instType": "SWAP", "instId": inst_id, "instFamily": f"{base}-USDT",
        "ctValCcy": base, "settleCcy": "USDT", "ctVal": "0.01", "ctMult": "1",
        "tickSz": "0.01", "lotSz": "0.001", "state": "live",
    }


def _requirement(
    *,
    consumer_id: str,
    venue: str,
    market: str,
    product_type: str,
    symbol: str,
    feed: DemandFeed,
    interval: str | None = None,
    depth_levels: int = 0,
    basis_contract_type: str | None = None,
    basis_series: str | None = None,
) -> DataRequirement:
    return DataRequirement(
        consumer_id=consumer_id,
        purpose=DemandPurpose.ALPHA,
        universe=UniverseSelector(
            selector_id=f"{consumer_id}:{symbol}:{feed.value}:{interval or 'point'}",
            kind=UniverseSelectorKind.EXPLICIT,
            venue=venue,
            market=market,
            product_type=product_type,
            native_symbols=(symbol,),
        ),
        feed=feed,
        source_policy_id="crypto_primary_v2",
        interval=interval,
        warmup_limit=700 if feed is DemandFeed.BAR else 0,
        max_freshness_ms=(
            3 * {"15m": 900_000, "1h": 3_600_000, "1d": 86_400_000}[interval]
            if feed is DemandFeed.BAR else 60_000
            if feed in {DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA} else 15_000
        ),
        priority=100,
        ttl_seconds=180,
        require_final_bars=feed is DemandFeed.BAR,
        require_live=feed in {DemandFeed.TRADE, DemandFeed.QUOTE, DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA},
        execution_grade=False,
        depth_levels=depth_levels,
        configuration_revision=11,
        basis_contract_type=basis_contract_type,
        basis_series=basis_series,
    )


class UniversalRealtimePlanTests(unittest.TestCase):
    def _inventory(self) -> ActiveDemandInventory:
        requirements = (
            _requirement(consumer_id="alpha.binance", venue="BINANCE", market="USDM", product_type="PERPETUAL", symbol="ETHUSDT", feed=DemandFeed.TRADE),
            _requirement(consumer_id="alpha.binance", venue="BINANCE", market="USDM", product_type="PERPETUAL", symbol="ETHUSDT", feed=DemandFeed.QUOTE),
            _requirement(consumer_id="alpha.binance", venue="BINANCE", market="USDM", product_type="PERPETUAL", symbol="ETHUSDT", feed=DemandFeed.BAR, interval="15m"),
            _requirement(consumer_id="alpha.binance", venue="BINANCE", market="USDM", product_type="PERPETUAL", symbol="BNBUSDT", feed=DemandFeed.BAR, interval="1d"),
            _requirement(consumer_id="alpha.basis", venue="BINANCE", market="USDM", product_type="FUTURE", symbol="BTCUSDT_270326", feed=DemandFeed.BAR, interval="1d"),
            _requirement(consumer_id="alpha.okx", venue="OKX", market="SWAP", product_type="PERPETUAL", symbol="SOL-USDT-SWAP", feed=DemandFeed.TRADE),
            _requirement(consumer_id="alpha.okx", venue="OKX", market="SWAP", product_type="PERPETUAL", symbol="SOL-USDT-SWAP", feed=DemandFeed.QUOTE),
            _requirement(consumer_id="alpha.okx", venue="OKX", market="SWAP", product_type="PERPETUAL", symbol="SOL-USDT-SWAP", feed=DemandFeed.BAR, interval="1h"),
            _requirement(consumer_id="alpha.binance.spot", venue="BINANCE", market="SPOT", product_type="SPOT", symbol="SOLUSDT", feed=DemandFeed.TRADE),
            _requirement(consumer_id="alpha.binance.spot", venue="BINANCE", market="SPOT", product_type="SPOT", symbol="SOLUSDT", feed=DemandFeed.QUOTE),
            _requirement(consumer_id="alpha.binance.spot", venue="BINANCE", market="SPOT", product_type="SPOT", symbol="SOLUSDT", feed=DemandFeed.BAR, interval="15m"),
            _requirement(consumer_id="alpha.okx.spot", venue="OKX", market="SPOT", product_type="SPOT", symbol="ETH-USDT", feed=DemandFeed.TRADE),
            _requirement(consumer_id="alpha.okx.spot", venue="OKX", market="SPOT", product_type="SPOT", symbol="ETH-USDT", feed=DemandFeed.QUOTE),
            _requirement(consumer_id="alpha.okx.spot", venue="OKX", market="SPOT", product_type="SPOT", symbol="ETH-USDT", feed=DemandFeed.BAR, interval="1d"),
            _requirement(consumer_id="alpha.missing", venue="BINANCE", market="USDM", product_type="PERPETUAL", symbol="DELISTEDUSDT", feed=DemandFeed.TRADE),
            _requirement(
                consumer_id="alpha.reference",
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                symbol="BTCUSDT",
                feed=DemandFeed.BASIS,
                interval="1d",
                basis_contract_type="CURRENT_QUARTER",
                basis_series="CONTINUOUS",
            ),
        )
        return ActiveDemandInventory(
            revision=11,
            requirements=requirements,
            source_documents=(),
            candidates=(),
            exclusions=(),
            input_sha256="a" * 64,
        )

    @staticmethod
    def _policy(inventory: ActiveDemandInventory) -> AdmissionPolicy:
        budgets = {
            (item.universe.venue, item.universe.market, item.feed)
            for item in inventory.requirements
        }
        return AdmissionPolicy(
            max_subscriptions_per_connection=2,
            max_total_slices=100,
            budgets=tuple(
                AdmissionBudget(venue=venue, market=market, feed=feed, max_slices=100)
                for venue, market, feed in sorted(budgets, key=lambda item: (item[0], item[1], item[2].value))
            ),
        )

    @staticmethod
    def _metadata() -> dict[tuple[str, str], object]:
        return {
            ("BINANCE", "USDM"): {
                "serverTime": 1,
                "symbols": [
                    _binance_usdm("ETHUSDT"),
                    _binance_usdm("BNBUSDT"),
                    _binance_usdm("BTCUSDT"),
                    _binance_usdm("BTCUSDT_270326", contract_type="CURRENT_QUARTER"),
                ],
            },
            ("BINANCE", "SPOT"): {
                "serverTime": 1,
                "symbols": [_binance_spot("SOLUSDT")],
            },
            ("OKX", "SWAP"): [_okx("SWAP", "SOL-USDT-SWAP")],
            ("OKX", "SPOT"): [_okx("SPOT", "ETH-USDT")],
        }

    def _plan(self):
        inventory = self._inventory()
        admission = admit_provider_metadata(inventory, self._metadata())
        convergence = converge_active_demand(inventory, admission, self._policy(inventory))
        plan = build_universal_realtime_plan(
            inventory=inventory,
            admission=admission,
            convergence=convergence,
            builder=ProductionCatalogBuilder(
                catalog_revision=11,
                source_policy_revision=11,
                authority_revision=11,
            ),
        )
        return inventory, admission, convergence, plan

    def test_active_multivenue_demand_becomes_one_shared_catalog_and_runtime_shape(self):
        _inventory, _admission, convergence, plan = self._plan()
        self.assertEqual(plan.binding_count, 14)
        self.assertEqual(plan.final_bar_binding_count, 6)
        self.assertEqual(len(plan.deferred_requirement_ids), 1)
        self.assertEqual(len(plan.unsupported_requirement_ids), 1)
        self.assertEqual(plan.topology.service_role_count, 4)
        self.assertEqual(len(plan.topology.subscriptions), 8)
        self.assertEqual(plan.topology.connection_count, 8)
        self.assertEqual(
            {item.feed.value.upper() for item in plan.topology.subscriptions},
            {"TRADE", "BBO"},
        )
        self.assertEqual(
            set(plan.final_bar_runtime_roles),
            {("BINANCE", "SPOT"), ("BINANCE", "USDM"), ("OKX", "SPOT"), ("OKX", "SWAP")},
        )
        self.assertEqual(convergence.selected_slice_count, 15)
        self.assertEqual(
            {item["mode"] for item in plan.bundle.acquisition_plan["bindings"] if item["provider_kind"].endswith("rest_bar")},
            {"PYTHON_REST"},
        )
        channels = {
            item["native_channel"]
            for item in plan.bundle.acquisition_plan["bindings"]
            if item["provider_kind"] == "okx_bar"
        }
        self.assertEqual(channels, {"candle1H", "candle1Dutc"})
        futures = next(
            item for item in plan.bundle.source_catalog["bindings"]
            if item["binding_id"] == "binance-usdm-btcusdt-270326-bar-1d"
        )
        self.assertEqual(futures["quality"]["stale_after_ms"], 259_200_000)
        self.assertEqual(
            plan.owners_by_binding["binance-usdm-ethusdt-trade"],
            ("alpha.binance",),
        )

    def test_generated_plan_loads_and_keeps_four_shared_ingestor_roles(self):
        _inventory, _admission, _convergence, plan = self._plan()
        with tempfile.TemporaryDirectory() as directory:
            paths = plan.bundle.write(Path(directory) / "dark")
            catalog = StableSourceCatalog.load(paths["source_catalog"])
            dated = next(
                item
                for item in catalog.instruments
                if item.instrument_id == "BINANCE.USDM.FUTURE.BTC-USDT-270326"
            )
            self.assertEqual(dated.expiry_time_ns, 1_800_000_000_000_000_000)
            acquisition = StableAcquisitionPlan.load(paths["acquisition_plan"], catalog=catalog)
            authority = {
                "schema": "qdl.authority-record.v1",
                "slice_id": "phase112-source-test",
                "revision": 11,
                "mode": "RUST_SHADOW",
                "candidate_image_digest": "sha256:" + "1" * 64,
                "capability_manifest_digest": "2" * 64,
                "contract_digest": "3" * 64,
                "partition_plan_digest": "4" * 64,
                "public_write_allowed": False,
                "legacy_write_allowed": False,
                "approved_by": "phase112-test",
                "effective_at_ns": 1,
            }
            runtime = acquisition.demand_runtime_configs(
                catalog=catalog,
                authority=authority,
                binding_ids=plan.owners_by_binding,
            )
            self.assertEqual(runtime["core_worker_count"], 3)
            self.assertEqual(set(runtime["ingestors"]), {
                "binance-spot", "binance-usdm", "okx-spot", "okx-swap",
            })
            self.assertEqual(sum(len(item["bindings"]) for item in runtime["ingestors"].values()), 8)

    def test_provider_projection_covers_every_admitted_binding_without_symbol_fallback(self):
        _inventory, _admission, _convergence, plan = self._plan()
        bindings = provider_realtime_bindings(plan)
        self.assertEqual(len(bindings), plan.binding_count)
        self.assertEqual(
            {item.binding_id for item in bindings},
            set(plan.owners_by_binding),
        )
        native = [item for item in bindings if item.mode == "RUST_NATIVE"]
        bars = [item for item in bindings if item.mode == "PYTHON_REST"]
        self.assertEqual(len(native), 8)
        self.assertEqual(len(bars), plan.final_bar_binding_count)
        self.assertTrue(all(item.feed.value in {"TRADE", "QUOTE"} for item in native))
        self.assertTrue(all(item.feed.value == "BAR" and item.require_final_bar for item in bars))
        dated = next(item for item in bars if item.product_type == "FUTURE")
        self.assertEqual(dated.native_symbol, "BTCUSDT_270326")
        self.assertIsNone(dated.websocket_url)

    def test_selective_demand_removal_is_an_unsubscribe_not_a_new_runtime_role(self):
        _inventory, _admission, _convergence, plan = self._plan()
        retained = tuple(plan.topology.subscriptions[:-1])
        next_topology = DemandTopology(
            demand_revision=plan.topology.demand_revision + 1,
            subscriptions=retained,
            shards=plan_shards(retained, max_streams=2, config_revision=12),
            runtime_roles=tuple(sorted({(item.venue, item.market) for item in retained})),
            core_binding_ids=plan.topology.core_binding_ids,
            provisioning_required=(),
        )
        actions = DemandTopologyPlanner(max_subscriptions_per_connection=2).reconcile(
            plan.topology,
            next_topology,
        )
        self.assertEqual(
            [item.kind.value for item in actions].count("UNSUBSCRIBE"),
            1,
        )
        self.assertLessEqual(next_topology.service_role_count, plan.topology.service_role_count)

    def test_book_aliases_share_one_native_subscription_and_one_core_state_machine(self):
        requirements = (
            _requirement(
                consumer_id="alpha.binance.book",
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                symbol="BTCUSDT",
                feed=DemandFeed.BOOK_SNAPSHOT,
                depth_levels=100,
            ),
            _requirement(
                consumer_id="alpha.binance.book",
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                symbol="BTCUSDT",
                feed=DemandFeed.BOOK_DELTA,
                depth_levels=100,
            ),
            _requirement(
                consumer_id="alpha.okx.book",
                venue="OKX",
                market="SWAP",
                product_type="PERPETUAL",
                symbol="SOL-USDT-SWAP",
                feed=DemandFeed.BOOK_SNAPSHOT,
                depth_levels=100,
            ),
            _requirement(
                consumer_id="alpha.okx.book",
                venue="OKX",
                market="SWAP",
                product_type="PERPETUAL",
                symbol="SOL-USDT-SWAP",
                feed=DemandFeed.BOOK_DELTA,
                depth_levels=100,
            ),
        )
        inventory = ActiveDemandInventory(
            revision=12,
            requirements=requirements,
            source_documents=(),
            candidates=(),
            exclusions=(),
            input_sha256="b" * 64,
        )
        admission = admit_provider_metadata(inventory, self._metadata())
        convergence = converge_active_demand(inventory, admission, self._policy(inventory))
        plan = build_universal_realtime_plan(
            inventory=inventory,
            admission=admission,
            convergence=convergence,
            builder=ProductionCatalogBuilder(
                catalog_revision=12,
                source_policy_revision=12,
                authority_revision=12,
            ),
        )
        self.assertEqual(plan.binding_count, 4)
        self.assertEqual(len(plan.topology.subscriptions), 2)
        self.assertEqual({item.feed.value for item in plan.topology.subscriptions}, {"book"})

        with tempfile.TemporaryDirectory() as directory:
            paths = plan.bundle.write(Path(directory) / "books")
            catalog = StableSourceCatalog.load(paths["source_catalog"])
            acquisition = StableAcquisitionPlan.load(paths["acquisition_plan"], catalog=catalog)
            book_pairs = {}
            for source in catalog.bindings:
                if source.feed.value.startswith("BOOK_"):
                    book_pairs.setdefault(source.source_id, []).append(source)
            self.assertEqual({len(items) for items in book_pairs.values()}, {2})
            self.assertEqual(
                {items[0].partition_key for items in book_pairs.values()},
                {items[1].partition_key for items in book_pairs.values()},
            )
            authority = {
                "schema": "qdl.authority-record.v1",
                "slice_id": "phase112-book-test",
                "revision": 12,
                "mode": "RUST_SHADOW",
                "candidate_image_digest": "sha256:" + "1" * 64,
                "capability_manifest_digest": "2" * 64,
                "contract_digest": "3" * 64,
                "partition_plan_digest": "4" * 64,
                "public_write_allowed": False,
                "legacy_write_allowed": False,
                "approved_by": "phase112-book-test",
                "effective_at_ns": 1,
            }
            core = acquisition.core_config(
                catalog=catalog,
                authority=authority,
                binding_ids=frozenset(plan.owners_by_binding),
            )
            self.assertEqual(len(core["core"]["bindings"]), 2)
            self.assertTrue(all(item["l2"]["depth_per_side"] == 100 for item in core["core"]["bindings"]))


if __name__ == "__main__":
    unittest.main()
