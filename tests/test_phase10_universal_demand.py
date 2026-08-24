from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

import yaml

from qdl.demand.v1 import demand_pb2
from qdl.demand import (
    DataRequirement,
    DemandFeed,
    DemandLeaseRegistry,
    DemandManifest,
    DemandPurpose,
    DemandResolver,
    DemandState,
    DemandTopologyPlanner,
    demand_transition_allowed,
    UniverseRegistry,
    UniverseSelector,
    UniverseSelectorKind,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl.runtime.universal_demand import UniversalDemandPlanner
from scripts.phase10_real_provider_admission import (
    DemandSlice as ProviderDemandSlice,
    _load_slices as load_provider_slices,
    run as run_provider_admission,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
EXAMPLE_UNIVERSES = ROOT / "config/v2/universes/crypto-and-vn-core-v1.json"
EXAMPLE_MANIFEST = ROOT / "config/v2/universal-demand.example.yaml"


class UniversalDemandTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(CATALOG_PATH)
        cls.acquisition = StableAcquisitionPlan.load(
            ACQUISITION_PATH, catalog=cls.catalog
        )

    def test_python_generated_demand_matches_cross_language_golden(self):
        message = demand_pb2.DataRequirement(
            consumer_id="alpha.grid",
            purpose=demand_pb2.DEMAND_PURPOSE_ALPHA,
            universe=demand_pb2.UniverseSelector(
                selector_id="binance-usdm-major",
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                explicit_symbols=demand_pb2.ExplicitSymbols(
                    native_symbols=["BTCUSDT", "ETHUSDT"]
                ),
            ),
            feed=demand_pb2.FEED_TYPE_TRADE,
            max_freshness_ms=15_000,
            priority=10,
            ttl_seconds=180,
            require_live=True,
            source_policy_id="crypto_primary_v2",
            configuration_revision=7,
        )
        expected = (
            ROOT / "contracts/golden/demand/universal-demand.bin"
        ).read_bytes()
        self.assertEqual(message.SerializeToString(deterministic=True), expected)

    @staticmethod
    def _registry(root: Path) -> UniverseRegistry:
        path = root / "universe.json"
        path.write_text(
            """{
  "schema": "qdl.v2.universe-registry.v1",
  "revision": 7,
  "universes": [
    {
      "universe_id": "binance-usdm-core",
      "venue": "BINANCE",
      "market": "USDM",
      "product_type": "PERPETUAL",
      "symbols": [
        {"native_symbol": "BTCUSDT", "segments": ["core", "liquid"]},
        {"native_symbol": "ETHUSDT", "segments": ["core", "liquid"]}
      ]
    },
    {
      "universe_id": "okx-swap-core",
      "venue": "OKX",
      "market": "SWAP",
      "product_type": "PERPETUAL",
      "symbols": [
        {"native_symbol": "BTC-USDT-SWAP", "segments": ["core", "liquid"]},
        {"native_symbol": "ETH-USDT-SWAP", "segments": ["core", "liquid"]}
      ]
    }
  ]
}
""",
            encoding="utf-8",
        )
        return UniverseRegistry.load(path)

    @staticmethod
    def _requirement(
        *,
        consumer_id: str = "alpha.grid",
        purpose: DemandPurpose = DemandPurpose.ALPHA,
        feed: DemandFeed = DemandFeed.TRADE,
        selector: UniverseSelector | None = None,
        interval: str | None = None,
        priority: int = 100,
    ) -> DataRequirement:
        return DataRequirement(
            consumer_id=consumer_id,
            purpose=purpose,
            universe=selector
            or UniverseSelector(
                selector_id="binance-usdm-core",
                kind=UniverseSelectorKind.UNIVERSE_REF,
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                universe_ref="binance-usdm-core",
            ),
            feed=feed,
            source_policy_id="crypto_primary_v2",
            interval=interval,
            warmup_limit=700 if feed is DemandFeed.BAR else 0,
            max_freshness_ms=180_000 if feed is DemandFeed.BAR else 15_000,
            priority=priority,
            ttl_seconds=180,
            require_final_bars=feed is DemandFeed.BAR,
            require_live=True,
            execution_grade=purpose is DemandPurpose.EXECUTION,
            configuration_revision=7,
        )

    def test_proto_roundtrip_keeps_selector_and_exact_requirement_identity(self):
        selector = UniverseSelector(
            selector_id="binance-core-segment",
            kind=UniverseSelectorKind.SEGMENT,
            venue="BINANCE",
            market="USDM",
            product_type="PERPETUAL",
            universe_ref="binance-usdm-core",
            segment_id="core",
            expected_universe_sha256="a" * 64,
        )
        requirement = self._requirement(
            feed=DemandFeed.BAR,
            selector=selector,
            interval="1m",
        )
        encoded = requirement.to_proto().SerializeToString(deterministic=True)
        decoded = requirement.to_proto().__class__()
        decoded.ParseFromString(encoded)
        restored = DataRequirement.from_proto(decoded)
        self.assertEqual(restored.canonical_mapping(), requirement.canonical_mapping())
        self.assertEqual(restored.requirement_id, requirement.requirement_id)
        self.assertEqual(decoded.universe.universe_ref, "binance-usdm-core")
        self.assertEqual(decoded.universe.segment_id, "core")

    def test_universe_registry_manifest_and_segment_resolution_are_strict(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = self._registry(root)
            selector = UniverseSelector(
                selector_id="binance-core",
                kind=UniverseSelectorKind.SEGMENT,
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                universe_ref="binance-usdm-core",
                segment_id="core",
                expected_universe_sha256=registry.sha256,
            )
            self.assertEqual(registry.resolve(selector), ("BTCUSDT", "ETHUSDT"))
            invalid = replace(selector, expected_universe_sha256="b" * 64)
            with self.assertRaisesRegex(ValueError, "digest"):
                registry.resolve(invalid)
            manifest_path = root / "manifest.yaml"
            payload = {
                "schema": "qdl.v2.universal-demand.v1",
                "revision": 7,
                "requirements": [self._requirement(selector=selector).canonical_mapping()],
            }
            manifest_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
            manifest = DemandManifest.load_many([manifest_path])
            self.assertEqual(manifest.revision, 7)
            self.assertEqual(len(manifest.requirements), 1)

    def test_resolver_deduplicates_consumers_and_keeps_execution_policy_strict(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = self._registry(Path(directory))
            selector = UniverseSelector(
                selector_id="binance-btc",
                kind=UniverseSelectorKind.EXPLICIT,
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                native_symbols=("BTCUSDT",),
            )
            alpha = self._requirement(
                consumer_id="alpha.grid",
                selector=selector,
                priority=100,
            )
            execution = self._requirement(
                consumer_id="trading-system.execution",
                purpose=DemandPurpose.EXECUTION,
                selector=selector,
                priority=10,
            )
            path = Path(directory) / "manifest.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "schema": "qdl.v2.universal-demand.v1",
                        "revision": 9,
                        "requirements": [alpha.canonical_mapping(), execution.canonical_mapping()],
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            resolved = DemandResolver(catalog=self.catalog, universes=registry).resolve_manifest(
                DemandManifest.load_many([path])
            )
            self.assertEqual(len(resolved), 1)
            item = resolved[0]
            self.assertEqual(item.requirement.purpose, DemandPurpose.EXECUTION)
            self.assertEqual(item.effective_priority, 10)
            self.assertEqual(item.consumer_ids, ("alpha.grid", "trading-system.execution"))
            self.assertTrue(item.provisioned)
            self.assertEqual(item.state, DemandState.REQUESTED)
            alpha_only = DemandResolver(catalog=self.catalog, universes=registry).resolve_requirement(
                alpha,
                demand_revision=9,
            )[0]
            self.assertEqual(item.requirement_id, alpha_only.requirement_id)

    def test_binance_spot_catalog_binding_resolves_through_its_own_capability_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = self._registry(Path(directory))
            requirement = self._requirement(
                selector=UniverseSelector(
                    selector_id="binance-spot-btc",
                    kind=UniverseSelectorKind.EXPLICIT,
                    venue="BINANCE",
                    market="SPOT",
                    product_type="SPOT",
                    native_symbols=("BTCUSDT",),
                )
            )
            resolved = DemandResolver(catalog=self.catalog, universes=registry).resolve_requirement(
                requirement,
                demand_revision=1,
            )
            self.assertEqual(len(resolved), 1)
            self.assertTrue(resolved[0].capability.enabled)
            self.assertTrue(resolved[0].provisioned)
            self.assertEqual(resolved[0].binding_id, "binance-spot-btcusdt-trade")

    def test_unsupported_metric_is_truthful_and_does_not_create_subscription(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = self._registry(Path(directory))
            selector = UniverseSelector(
                selector_id="binance-btc",
                kind=UniverseSelectorKind.EXPLICIT,
                venue="BINANCE",
                market="USDM",
                product_type="PERPETUAL",
                native_symbols=("BTCUSDT",),
            )
            requirement = self._requirement(
                feed=DemandFeed.LONG_SHORT_RATIO,
                selector=selector,
            )
            resolved = DemandResolver(catalog=self.catalog, universes=registry).resolve_requirement(
                requirement,
                demand_revision=1,
            )
            self.assertEqual(resolved[0].state, DemandState.UNSUPPORTED)
            topology = DemandTopologyPlanner(max_subscriptions_per_connection=200).build(
                resolved,
                demand_revision=1,
            )
            self.assertEqual(topology.subscriptions, ())
            self.assertEqual(topology.provisioning_required, ())

    def test_ttl_release_and_state_transitions_are_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = self._registry(Path(directory))
            resolved = DemandResolver(catalog=self.catalog, universes=registry).resolve_requirement(
                self._requirement(
                    selector=UniverseSelector(
                        selector_id="binance-btc",
                        kind=UniverseSelectorKind.EXPLICIT,
                        venue="BINANCE",
                        market="USDM",
                        product_type="PERPETUAL",
                        native_symbols=("BTCUSDT",),
                    )
                ),
                demand_revision=1,
            )
            leases = DemandLeaseRegistry(clock_ns=lambda: 1_000)
            leases.renew("alpha.grid", resolved, ttl_seconds=30, now_ns=1_000)
            requirement_id = resolved[0].requirement_id
            transition = leases.transition(
                requirement_id,
                DemandState.CONNECTING,
                reason="adapter_subscription_ack_pending",
                now_ns=2_000,
            )
            self.assertEqual(transition.current, DemandState.CONNECTING)
            self.assertEqual(len(leases.desired(now_ns=2_000)), 1)
            expired = leases.expire(now_ns=30_000_001_001)
            self.assertEqual(len(expired), 1)
            self.assertEqual(leases.desired(now_ns=30_000_001_001), ())
            self.assertEqual(leases.transitions()[-1].current, DemandState.EXPIRED)
            renewed = leases.renew("alpha.grid", resolved, now_ns=30_000_001_002)
            self.assertEqual(renewed[0].state, DemandState.REQUESTED)
            self.assertEqual(leases.desired(now_ns=30_000_001_002)[0].state, DemandState.REQUESTED)
            self.assertEqual(leases.transitions()[-1].reason, "lease_renewed_after_expiry")
            self.assertTrue(demand_transition_allowed(DemandState.REQUESTED, DemandState.CONNECTING))
            self.assertFalse(demand_transition_allowed(DemandState.LIVE, DemandState.REQUESTED))
            with self.assertRaisesRegex(ValueError, "invalid demand transition"):
                leases.transition(
                    requirement_id,
                    DemandState.LIVE,
                    reason="skips_continuity_proof",
                    now_ns=30_000_001_003,
                )

    def test_dynamic_shards_grow_without_symbol_container_growth(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = self._registry(Path(directory))
            base = DemandResolver(catalog=self.catalog, universes=registry).resolve_requirement(
                self._requirement(
                    selector=UniverseSelector(
                        selector_id="binance-btc",
                        kind=UniverseSelectorKind.EXPLICIT,
                        venue="BINANCE",
                        market="USDM",
                        product_type="PERPETUAL",
                        native_symbols=("BTCUSDT",),
                    )
                ),
                demand_revision=11,
            )[0]
            many = (base,) + tuple(
                replace(
                    base,
                    instrument_uid=f"phase10-{index:04d}",
                    instrument_id=f"BINANCE.USDM.PERPETUAL.TOKEN-{index}",
                    native_symbol=f"TOKEN{index}USDT",
                    binding_id=f"phase10-binding-{index:04d}",
                    provisioned=True,
                )
                for index in range(499)
            )
            planner = DemandTopologyPlanner(max_subscriptions_per_connection=200)
            first = planner.build(many, demand_revision=11)
            second = planner.build(many, demand_revision=11)
            self.assertEqual(first, second)
            self.assertEqual(first.connection_count, 3)
            self.assertEqual(first.service_role_count, 1)
            smaller = planner.build(
                (replace(many[0], demand_revision=10),),
                demand_revision=10,
            )
            actions = planner.reconcile(smaller, first)
            self.assertTrue(any(item.kind.value == "SUBSCRIBE" for item in actions))
            self.assertTrue(any(item.kind.value == "REBIND_SHARD" for item in actions))

    def test_existing_stable_acquisition_compiles_one_shared_core_config(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = self._registry(Path(directory))
            manifest = DemandManifest(
                revision=12,
                requirements=(
                    self._requirement(
                        selector=UniverseSelector(
                            selector_id="binance-usdm-core",
                            kind=UniverseSelectorKind.UNIVERSE_REF,
                            venue="BINANCE",
                            market="USDM",
                            product_type="PERPETUAL",
                            universe_ref="binance-usdm-core",
                        )
                    ),
                ),
                source_paths=(str(Path(directory) / "manifest.yaml"),),
                sha256="c" * 64,
            )
            plan = UniversalDemandPlanner(
                catalog=self.catalog,
                universes=registry,
                max_subscriptions_per_connection=200,
            ).compile(manifest)
            self.assertEqual(plan.topology.service_role_count, 1)
            self.assertEqual(plan.topology.connection_count, 1)
            self.assertEqual(len(plan.topology.core_binding_ids), 2)
            authority = {
                "schema": "qdl.authority-record.v1",
                "slice_id": "phase10-isolated",
                "revision": 1,
                "mode": "RUST_SHADOW",
                "candidate_image_digest": "sha256:" + "1" * 64,
                "capability_manifest_digest": "2" * 64,
                "contract_digest": "3" * 64,
                "partition_plan_digest": "4" * 64,
                "public_write_allowed": False,
                "legacy_write_allowed": False,
                "approved_by": "phase10-test",
                "effective_at_ns": 1,
            }
            runtime = self.acquisition.demand_runtime_configs(
                catalog=self.catalog,
                authority=authority,
                binding_ids=plan.topology.core_binding_ids,
            )
            self.assertEqual(runtime["core_worker_count"], 3)
            self.assertEqual(len(runtime["core"]), 3)
            self.assertEqual(set(runtime["ingestors"]), {"binance-usdm"})
            self.assertEqual(runtime["ingestors"]["binance-usdm"]["bindings"].__len__(), 2)

    def test_read_only_provider_admission_covers_every_declared_crypto_slice(self):
        slices = load_provider_slices(ROOT / "config/v2/stable-crypto-demand.yaml")
        self.assertEqual(len(slices), 18)
        self.assertEqual(
            {(item.venue, item.market, item.native_symbol) for item in slices},
            {
                ("BINANCE", "SPOT", "BTCUSDT"),
                ("BINANCE", "USDM", "BTCUSDT"),
                ("BINANCE", "USDM", "ETHUSDT"),
                ("OKX", "SPOT", "BTC-USDT"),
                ("OKX", "SWAP", "BTC-USDT-SWAP"),
                ("OKX", "SWAP", "ETH-USDT-SWAP"),
            },
        )

        class Response:
            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        def fake_get(url, **_):
            if "binance.com" in url:
                if url.endswith("/trades"):
                    return Response([{"id": 1, "price": "1", "qty": "1", "time": 1_000}])
                if url.endswith("/ticker/bookTicker"):
                    return Response({"bidPrice": "1", "askPrice": "2"})
                return Response([[0, "1", "2", "1", "1", "1", 1_000]])
            if url.endswith("/trades"):
                return Response({"code": "0", "data": [{"px": "1", "sz": "1", "tradeId": "1", "ts": "1000"}]})
            if url.endswith("/books"):
                return Response({"code": "0", "data": [{"bids": [["1", "1"]], "asks": [["2", "1"]], "ts": "1000"}]})
            return Response({"code": "0", "data": [["1000", "1", "2", "1", "1", "1", "1", "1", "1"]]})

        report = run_provider_admission(
            ROOT / "config/v2/stable-crypto-demand.yaml",
            timeout_seconds=1.0,
            get=fake_get,
        )
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["slice_count"], 18)
        self.assertTrue(all(len(item["payload_sha256"]) == 64 for item in report["slices"]))
        self.assertTrue(all("payload" not in item for item in report["slices"]))

    def test_example_files_compile_exact_current_multivenue_catalog_without_runtime_mutation(self):
        registry = UniverseRegistry.load(EXAMPLE_UNIVERSES)
        manifest = DemandManifest.load_many([EXAMPLE_MANIFEST])
        self.assertEqual(registry.revision, 1)
        self.assertEqual(manifest.revision, 1)
        self.assertGreaterEqual(len(manifest.requirements), 4)
        self.assertTrue(all(item.require_live for item in manifest.requirements))
        plan = UniversalDemandPlanner(
            catalog=self.catalog,
            universes=registry,
            max_subscriptions_per_connection=200,
        ).compile(manifest)
        self.assertEqual(len(plan.resolved), 7)
        self.assertEqual(
            plan.topology.runtime_roles,
            (("BINANCE", "USDM"), ("HNX", "VN_DERIVATIVES"), ("OKX", "SWAP")),
        )
        vn = next(item for item in plan.resolved if item.native_symbol == "VN30F1M")
        self.assertTrue(vn.capability.enabled)
        self.assertTrue(vn.provisioned)
        self.assertEqual(vn.binding_id, "dnse-vn30f1m-bar-1m")
        self.assertEqual(vn.state, DemandState.REQUESTED)
        self.assertFalse(plan.execution_ready)


if __name__ == "__main__":
    unittest.main()
