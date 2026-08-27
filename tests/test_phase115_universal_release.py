from __future__ import annotations

import copy
from dataclasses import replace
import hashlib
import json
import tempfile
from types import SimpleNamespace
import unittest
from pathlib import Path

import yaml

from qdl.certification.phase115_universal_release import (
    build_universal_no_order_acceptance_scope,
)
from qdl.consumer.universal_release import (
    ConsumerRouteBinding,
    UniversalReleaseManifest,
    UniversalReleaseCoverage,
    UniversalReleasePolicy,
    build_universal_release_manifest,
)
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
from qdl.runtime.production_catalog import ProductionCatalogBuilder
from qdl.runtime.universal_realtime import build_universal_realtime_plan
from scripts.phase115_prepare_universal_release import validate_evidence_bundle
from scripts.phase115_render_consumer_route_binding import main as render_consumer_binding


ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "config/v2/universal-release-policy.yaml"


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
) -> DataRequirement:
    return DataRequirement(
        consumer_id=consumer_id,
        purpose=(
            DemandPurpose.EXECUTION
            if consumer_id.startswith("trading-system.")
            else DemandPurpose.ALPHA
        ),
        universe=UniverseSelector(
            selector_id=f"{consumer_id}:{symbol}:{feed.value}:{interval or ''}",
            kind=UniverseSelectorKind.EXPLICIT,
            venue=venue,
            market=market,
            product_type=product_type,
            native_symbols=(symbol,),
        ),
        feed=feed,
        source_policy_id="crypto_primary_v2",
        interval=interval,
        warmup_limit=30 if feed is DemandFeed.BAR else 0,
        max_freshness_ms=180_000 if feed is DemandFeed.BAR else 60_000,
        require_final_bars=feed is DemandFeed.BAR,
        require_live=True,
        execution_grade=consumer_id.startswith("trading-system."),
        depth_levels=depth_levels,
        configuration_revision=115,
    )


def _inventory() -> ActiveDemandInventory:
    values = (
        _requirement(
            consumer_id="trading-system.paper.stable", venue="BINANCE", market="USDM",
            product_type="PERPETUAL", symbol="BTCUSDT", feed=DemandFeed.TRADE,
        ),
        _requirement(
            consumer_id="fib_sl_tp_strength_0015m", venue="BINANCE", market="USDM",
            product_type="PERPETUAL", symbol="ETHUSDT", feed=DemandFeed.BAR, interval="15m",
        ),
        _requirement(
            consumer_id="dynamic_grid_long_only_1h", venue="BINANCE", market="USDM",
            product_type="PERPETUAL", symbol="BTCUSDT", feed=DemandFeed.BOOK_SNAPSHOT, depth_levels=100,
        ),
        _requirement(
            consumer_id="basis_arb_binance_1d", venue="BINANCE", market="USDM",
            product_type="PERPETUAL", symbol="BTCUSDT", feed=DemandFeed.FUNDING_RATE,
        ),
        _requirement(
            consumer_id="rsiboundportfolioA001_1d", venue="OKX", market="SWAP",
            product_type="PERPETUAL", symbol="BTC-USDT-SWAP", feed=DemandFeed.QUOTE,
        ),
    )
    return ActiveDemandInventory(
        revision=115,
        requirements=values,
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
                    "symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL",
                    "baseAsset": "BTC", "quoteAsset": "USDT", "marginAsset": "USDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                    ],
                },
                {
                    "symbol": "ETHUSDT", "status": "TRADING", "contractType": "PERPETUAL",
                    "baseAsset": "ETH", "quoteAsset": "USDT", "marginAsset": "USDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                    ],
                },
            ]
        },
        ("OKX", "SWAP"): [
            {
                "instType": "SWAP", "instId": "BTC-USDT-SWAP", "instFamily": "BTC-USDT",
                "ctValCcy": "BTC", "settleCcy": "USDT", "ctVal": "0.01", "ctMult": "1",
                "tickSz": "0.01", "lotSz": "0.001", "state": "live",
            },
        ],
    }


def _policy() -> AdmissionPolicy:
    return AdmissionPolicy(
        max_subscriptions_per_connection=200,
        max_total_slices=32,
        budgets=(
            AdmissionBudget("BINANCE", "USDM", DemandFeed.TRADE, 8),
            AdmissionBudget("BINANCE", "USDM", DemandFeed.BAR, 8),
            AdmissionBudget("BINANCE", "USDM", DemandFeed.BOOK_SNAPSHOT, 8),
            AdmissionBudget("BINANCE", "USDM", DemandFeed.FUNDING_RATE, 8),
            AdmissionBudget("OKX", "SWAP", DemandFeed.QUOTE, 8),
        ),
    )


def _manifest():
    inventory = _inventory()
    admission = admit_provider_metadata(inventory, _metadata())
    convergence = converge_active_demand(inventory, admission, _policy())
    realtime = build_universal_realtime_plan(
        inventory=inventory,
        admission=admission,
        convergence=convergence,
        builder=ProductionCatalogBuilder(
            catalog_revision=115,
            source_policy_revision=115,
            authority_revision=1,
        ),
    )
    l2 = build_l2_demand_plan(
        inventory=inventory,
        admission=admission,
        convergence=convergence,
    )
    coverage = UniversalReleaseCoverage.from_phase_plans(
        inventory=inventory,
        admission=admission,
        convergence=convergence,
        realtime_plan=realtime,
        realtime_evidence_sha256="c" * 64,
        reference_evidence_sha256="b" * 64,
        l2_plan=l2,
        l2_evidence_sha256="d" * 64,
    )
    policy = UniversalReleasePolicy.load(POLICY_PATH, manifest_root=ROOT)
    return build_universal_release_manifest(
        policy=policy,
        inventory=inventory,
        admission=admission,
        convergence=convergence,
        coverage=coverage,
        release_revision=1,
    ), inventory, admission, convergence, coverage, policy


class UniversalReleaseManifestTests(unittest.TestCase):
    def test_stable_paper_alpha_identities_are_classified_without_broad_prefix(self):
        policy = UniversalReleasePolicy.load(POLICY_PATH, manifest_root=ROOT)
        self.assertEqual(
            policy.classify("alpha.binance.paper.stable").value,
            "SINGLE_SYMBOL_ALPHA",
        )
        self.assertEqual(
            policy.classify("alpha.okx.paper.stable").value,
            "SINGLE_SYMBOL_ALPHA",
        )
        with self.assertRaisesRegex(ValueError, "unclassified"):
            policy.classify("alpha.unknown.paper.stable")

    def test_full_admitted_inventory_is_v2_primary_and_is_deterministic(self):
        manifest, inventory, admission, convergence, coverage, policy = _manifest()
        repeated = build_universal_release_manifest(
            policy=policy,
            inventory=inventory,
            admission=admission,
            convergence=convergence,
            coverage=coverage,
            release_revision=1,
        )
        self.assertEqual(manifest.digest, repeated.digest)
        self.assertEqual(len(manifest.products), len(admission.rows))
        self.assertFalse(manifest.exclusions)
        self.assertTrue(all(item.route == "V2_PRIMARY" for item in manifest.products))
        self.assertEqual(
            next(item for item in manifest.products if item.feed == "TRADE").fallback,
            "V1",
        )
        self.assertTrue(all(
            item.fallback == "BLOCKED"
            for item in manifest.products if item.feed != "TRADE" or item.venue == "OKX"
        ))
        self.assertEqual(
            {item.consumer_class.value for item in manifest.products},
            {"TRADING_SYSTEM", "SINGLE_SYMBOL_ALPHA", "GRID_REACTIVE_BRACKET", "BASIS_ARB", "PORTFOLIO_MULTI_SYMBOL"},
        )

    def test_missing_instrument_is_explicitly_excluded_not_silently_routed(self):
        manifest, inventory, admission, convergence, coverage, policy = _manifest()
        target = next(item for item in admission.rows if item.native_symbol == "ETHUSDT")
        missing = replace(target, state="MISSING_INSTRUMENT", instrument_uid=None, instrument_id=None, reason="ETHUSDT")
        replaced_admission = replace(
            admission,
            rows=tuple(missing if item.requirement_id == target.requirement_id else item for item in admission.rows),
        )
        replaced_convergence = replace(
            convergence,
            readiness=tuple(
                replace(item, state=type(item.state).UNSUPPORTED, execution_eligible=False, reason="ETHUSDT")
                if item.requirement_id == target.requirement_id else item
                for item in convergence.readiness
            ),
        )
        replacement = build_universal_release_manifest(
            policy=policy,
            inventory=inventory,
            admission=replaced_admission,
            convergence=replaced_convergence,
            coverage=coverage,
            release_revision=2,
        )
        self.assertEqual(len(replacement.products), len(manifest.products) - 1)
        self.assertEqual(len(replacement.exclusions), 1)
        self.assertEqual(replacement.exclusions[0].state, "MISSING_INSTRUMENT")
        self.assertEqual(replacement.exclusions[0].requirement_id, target.requirement_id)

    def test_missing_coverage_unknown_consumer_and_changed_capability_fail_closed(self):
        manifest, inventory, admission, convergence, coverage, policy = _manifest()
        del manifest
        omitted = replace(coverage, realtime_requirement_ids=coverage.realtime_requirement_ids[1:])
        with self.assertRaisesRegex(ValueError, "no provider plane"):
            build_universal_release_manifest(
                policy=policy, inventory=inventory, admission=admission,
                convergence=convergence, coverage=omitted, release_revision=1,
            )
        unknown_inventory = replace(
            inventory,
            requirements=tuple(replace(item, consumer_id="unknown_alpha_001") if index == 0 else item
                               for index, item in enumerate(inventory.requirements)),
        )
        # A changed consumer identity requires a matching fresh admission; the
        # existing one must fail at the inventory checksum boundary first.
        with self.assertRaisesRegex(ValueError, "different inventory digests"):
            build_universal_release_manifest(
                policy=policy, inventory=unknown_inventory, admission=admission,
                convergence=convergence, coverage=coverage, release_revision=1,
            )
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            copied_config = root / "config/v2"
            copied_config.mkdir(parents=True)
            capability = copied_config / "stable-capabilities.yaml"
            capability.write_bytes((ROOT / "config/v2/stable-capabilities.yaml").read_bytes())
            policy_data = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
            policy_data["capability_matrix"]["sha256"] = "0" * 64
            path = copied_config / "universal-release-policy.yaml"
            path.write_text(yaml.safe_dump(policy_data, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "checksum differs"):
                UniversalReleasePolicy.load(path, manifest_root=root)

    def test_no_order_acceptance_scope_covers_every_product_without_mutation(self):
        manifest, *_ = _manifest()
        scope = build_universal_no_order_acceptance_scope(manifest)
        report = scope.report_payload()
        self.assertEqual(report["item_count"], len(manifest.products))
        self.assertEqual(report["order_actions"], 0)
        self.assertEqual(report["direct_provider_connections"], 0)
        self.assertEqual(report["runtime_route_mutations"], 0)
        self.assertEqual(
            {item.requirement_id for item in scope.items},
            {item.requirement_id for item in manifest.products},
        )

    def test_consumer_route_binding_is_deterministic_isolated_and_round_trips(self):
        manifest, *_ = _manifest()
        consumer_id = manifest.products[0].consumer_id
        binding = ConsumerRouteBinding.from_manifest(manifest, consumer_id=consumer_id)
        repeated = ConsumerRouteBinding.from_manifest(manifest, consumer_id=consumer_id)
        self.assertEqual(binding.binding_sha256, repeated.binding_sha256)
        self.assertEqual(binding.universal_manifest_sha256, manifest.digest)
        self.assertTrue(binding.products)
        self.assertTrue(all(item.consumer_id == consumer_id for item in binding.products))
        self.assertEqual(binding.independent_v1_venues, ("DNSE",))
        parsed = ConsumerRouteBinding.from_canonical_mapping(binding.canonical_mapping())
        self.assertEqual(parsed.canonical_mapping(), binding.canonical_mapping())
        self.assertEqual(
            UniversalReleaseManifest.from_canonical_mapping(manifest.canonical_mapping()).digest,
            manifest.digest,
        )

    def test_consumer_route_binding_rejects_unknown_tampered_and_duplicate_routes(self):
        manifest, *_ = _manifest()
        with self.assertRaisesRegex(ValueError, "no admitted product"):
            ConsumerRouteBinding.from_manifest(manifest, consumer_id="unknown.consumer")
        binding = ConsumerRouteBinding.from_manifest(
            manifest, consumer_id=manifest.products[0].consumer_id
        )
        tampered = binding.canonical_mapping()
        tampered["products"][0]["native_symbol"] = "TAMPERED"
        with self.assertRaisesRegex(ValueError, "checksum differs"):
            ConsumerRouteBinding.from_canonical_mapping(tampered)
        duplicate = binding.canonical_mapping()
        duplicate["products"].append(dict(duplicate["products"][0]))
        duplicate["binding_sha256"] = hashlib.sha256(
            json.dumps(
                {
                    key: item
                    for key, item in duplicate.items()
                    if key != "binding_sha256"
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        with self.assertRaisesRegex(ValueError, "duplicate route identities"):
            ConsumerRouteBinding.from_canonical_mapping(duplicate)

    def test_canonical_manifest_and_binding_reject_boolean_numeric_coercion(self):
        manifest, *_ = _manifest()

        invalid_manifest_revision = manifest.canonical_mapping()
        invalid_manifest_revision["revision"] = True
        with self.assertRaisesRegex(ValueError, "positive integer"):
            UniversalReleaseManifest.from_canonical_mapping(invalid_manifest_revision)

        invalid_manifest_boolean = manifest.canonical_mapping()
        invalid_manifest_boolean["products"][0]["require_live"] = "true"
        with self.assertRaisesRegex(ValueError, "must be boolean"):
            UniversalReleaseManifest.from_canonical_mapping(invalid_manifest_boolean)

        binding = ConsumerRouteBinding.from_manifest(
            manifest, consumer_id=manifest.products[0].consumer_id
        )
        invalid_binding = copy.deepcopy(binding.canonical_mapping())
        invalid_binding["release_revision"] = True
        invalid_binding["binding_sha256"] = hashlib.sha256(
            json.dumps(
                {
                    key: item
                    for key, item in invalid_binding.items()
                    if key != "binding_sha256"
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        with self.assertRaisesRegex(ValueError, "positive integer"):
            ConsumerRouteBinding.from_canonical_mapping(invalid_binding)

    def test_pure_renderer_writes_only_one_named_consumer_binding(self):
        manifest, *_ = _manifest()
        consumer_id = manifest.products[0].consumer_id
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            artifact = root / "release.json"
            output = root / "consumer-binding.json"
            artifact.write_text(json.dumps({
                "schema": "qdl.phase115.universal-release-preflight.v1",
                "release_manifest": manifest.canonical_mapping(),
                "release_summary": manifest.report_payload(),
            }), encoding="utf-8")
            self.assertEqual(render_consumer_binding([
                "--release-artifact", str(artifact),
                "--consumer-id", consumer_id,
                "--output", str(output),
            ]), 0)
            self.assertTrue(output.is_file())
            parsed = ConsumerRouteBinding.from_canonical_mapping(
                json.loads(output.read_text(encoding="utf-8"))
            )
            self.assertEqual(parsed.consumer_id, consumer_id)
            self.assertTrue(all(item.consumer_id == consumer_id for item in parsed.products))


class UniversalReleaseEvidenceTests(unittest.TestCase):
    def test_evidence_bundle_requires_one_exact_admission_generation(self):
        inventory_sha256 = "1" * 64
        metadata_sha256 = {"BINANCE/USDM": "2" * 64}
        admission = {
            "schema": "qdl.v2.active-demand-provider-admission.v1",
            "status": "PASS",
            "provenance": "REAL_PROVIDER_METADATA_READ_ONLY",
            "inventory_sha256": inventory_sha256,
            "metadata_sha256": metadata_sha256,
            "row_count": 1,
            "pass_count": 1,
            "failure_count": 0,
            "rows": [],
        }
        admission_digest = hashlib.sha256(
            json.dumps(admission, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        realtime = {
            "schema": "qdl.phase112.universal-realtime-provider-admission.v1",
            "status": "PASS",
            "provenance": "REAL_PROVIDER_DIRECT_READ_ONLY",
            "inventory_sha256": inventory_sha256,
            "metadata_sha256": metadata_sha256,
            "admission_evidence_sha256": admission_digest,
            "bindings": [{"binding_id": "realtime-1"}],
            "runtime_mutations": 0,
            "production_writes": 0,
            "raw_provider_frames_persisted": 0,
        }
        reference = {
            "schema": "qdl.phase113.universal-warmup-reference-admission.v1",
            "status": "PASS",
            "provenance": "REAL_PROVIDER_READ_ONLY",
            "inventory_sha256": inventory_sha256,
            "metadata_sha256": metadata_sha256,
            "runtime_mutations": 0,
            "production_writes": 0,
            "provider_writes": 0,
            "raw_payload_persisted": False,
            "reference_results": [{
                "instrument_uid": "instrument-1",
                "product": "FUNDING_RATE",
                "expected": "AVAILABLE",
                "status": "OK",
            }],
        }
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            paths = {
                name: root / f"{name}.json"
                for name in ("admission", "realtime", "reference")
            }
            for name, payload in (
                ("admission", admission), ("realtime", realtime), ("reference", reference)
            ):
                paths[name].write_text(json.dumps(payload), encoding="utf-8")
            evidence = validate_evidence_bundle(
                inventory_sha256=inventory_sha256,
                admission_payload=admission,
                metadata_sha256=metadata_sha256,
                realtime_binding_ids={"realtime-1"},
                reference_pairs={("instrument-1", "FUNDING_RATE")},
                l2_plan=None,
                admission_path=paths["admission"],
                realtime_path=paths["realtime"],
                reference_path=paths["reference"],
                l2_path=None,
            )
            self.assertEqual(evidence["admission_evidence_sha256"], admission_digest)
            stale = dict(realtime)
            stale["inventory_sha256"] = "3" * 64
            paths["realtime"].write_text(json.dumps(stale), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "inventory differs"):
                validate_evidence_bundle(
                    inventory_sha256=inventory_sha256,
                    admission_payload=admission,
                    metadata_sha256=metadata_sha256,
                    realtime_binding_ids={"realtime-1"},
                    reference_pairs={("instrument-1", "FUNDING_RATE")},
                    l2_plan=None,
                    admission_path=paths["admission"],
                    realtime_path=paths["realtime"],
                    reference_path=paths["reference"],
                    l2_path=None,
                )

    def test_l2_evidence_must_match_exact_active_requirement_binding(self):
        inventory_sha256 = "4" * 64
        metadata_sha256 = {"BINANCE/USDM": "5" * 64}
        admission = {
            "schema": "qdl.v2.active-demand-provider-admission.v1",
            "status": "PASS",
            "provenance": "REAL_PROVIDER_METADATA_READ_ONLY",
            "inventory_sha256": inventory_sha256,
            "metadata_sha256": metadata_sha256,
            "row_count": 1,
            "pass_count": 1,
            "failure_count": 0,
            "rows": [],
        }
        admission_digest = hashlib.sha256(
            json.dumps(admission, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        realtime = {
            "schema": "qdl.phase112.universal-realtime-provider-admission.v1",
            "status": "PASS",
            "provenance": "REAL_PROVIDER_DIRECT_READ_ONLY",
            "inventory_sha256": inventory_sha256,
            "metadata_sha256": metadata_sha256,
            "admission_evidence_sha256": admission_digest,
            "bindings": [{"binding_id": "realtime-2"}],
            "runtime_mutations": 0,
            "production_writes": 0,
            "raw_provider_frames_persisted": 0,
        }
        reference = {
            "schema": "qdl.phase113.universal-warmup-reference-admission.v1",
            "status": "PASS",
            "provenance": "REAL_PROVIDER_READ_ONLY",
            "inventory_sha256": inventory_sha256,
            "metadata_sha256": metadata_sha256,
            "runtime_mutations": 0,
            "production_writes": 0,
            "provider_writes": 0,
            "raw_payload_persisted": False,
            "reference_results": [],
        }
        binding = {
            "venue": "BINANCE",
            "market": "USDM",
            "product_type": "PERPETUAL",
            "native_symbol": "BTCUSDT",
            "instrument_uid": "instrument-2",
            "instrument_id": "BINANCE.USDM.PERPETUAL.BTC-USDT",
            "requirement_ids": ["requirement-2"],
        }
        l2 = {
            "schema": "qdl.phase114.l2-real-provider-capture.v1",
            "status": "PASS",
            "provenance": ["REAL_BINANCE_USDM_PUBLIC_WS_REST"],
            "inventory_sha256": inventory_sha256,
            "metadata_sha256": metadata_sha256,
            "admission_sha256": admission_digest,
            "runtime_mutations": 0,
            "production_writes": 0,
            "raw_provider_bytes_persisted": 0,
            "required_bindings": [binding],
        }
        plan = SimpleNamespace(bindings=(SimpleNamespace(**binding),))
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            paths = {name: root / f"{name}.json" for name in ("admission", "realtime", "reference", "l2")}
            for name, payload in (
                ("admission", admission), ("realtime", realtime), ("reference", reference), ("l2", l2)
            ):
                paths[name].write_text(json.dumps(payload), encoding="utf-8")
            validate_evidence_bundle(
                inventory_sha256=inventory_sha256,
                admission_payload=admission,
                metadata_sha256=metadata_sha256,
                realtime_binding_ids={"realtime-2"},
                reference_pairs=set(),
                l2_plan=plan,
                admission_path=paths["admission"],
                realtime_path=paths["realtime"],
                reference_path=paths["reference"],
                l2_path=paths["l2"],
            )
            mismatched = dict(l2)
            mismatched["required_bindings"] = [dict(binding, requirement_ids=["different-requirement"])]
            paths["l2"].write_text(json.dumps(mismatched), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "L2 evidence bindings differ"):
                validate_evidence_bundle(
                    inventory_sha256=inventory_sha256,
                    admission_payload=admission,
                    metadata_sha256=metadata_sha256,
                    realtime_binding_ids={"realtime-2"},
                    reference_pairs=set(),
                    l2_plan=plan,
                    admission_path=paths["admission"],
                    realtime_path=paths["realtime"],
                    reference_path=paths["reference"],
                    l2_path=paths["l2"],
                )


if __name__ == "__main__":
    unittest.main()
