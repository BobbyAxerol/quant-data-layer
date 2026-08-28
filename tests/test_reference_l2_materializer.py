"""Strict source-only tests for Reference/L2 stable materialization.

Fixtures below are deliberately provider-shaped test data.  They never enter a
runtime artifact or a real-provider certificate; production materialization
requires a bounded authenticated metadata read through the CLI.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import unittest

import yaml

from qdl.demand import (
    ActiveDemandInventory,
    ActiveDemandSourceRegistry,
    DemandManifest,
    admit_provider_metadata,
    converge_active_demand,
)
from qdl.query import FeedType
from qdl.runtime.reference_l2_materializer import (
    REFERENCE_L2_CONSUMER_ID,
    build_reference_l2_materialization,
)


ROOT = Path(__file__).resolve().parents[1]
DEMAND_PATH = ROOT / "config/v2/stable-reference-l2-demand.yaml"
REGISTRY_PATH = ROOT / "config/v2/active-demand-source-registry.yaml"
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
SCOPE_PATH = ROOT / "config/v2/stable-authority-promotion-scope.yaml"


def _load(path: Path) -> dict:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _managed_l2(item: dict) -> bool:
    source = item.get("source")
    return (
        isinstance(source, dict)
        and item.get("feed") in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
        and source.get("source_policy_id") == "crypto_liquid_v2"
        and str(source.get("source_id", "")).endswith("-book-primary-v2")
    )


def _fixture_baseline() -> tuple[dict, dict, dict]:
    """Remove only this dynamic product from the checked-in stable documents.

    The actual production catalog intentionally carries provider-discovered
    current/next quarterlies. Unit fixtures use stable synthetic expiry names,
    so they must not accidentally assert that today's discovered expiry is
    immutable.  Unrelated existing catalog/binding/acquisition identity stays
    exactly as checked in.
    """

    catalog = _load(CATALOG_PATH)
    acquisition = _load(ACQUISITION_PATH)
    scope = _load(SCOPE_PATH)
    managed = [item for item in catalog["bindings"] if _managed_l2(item)]
    managed_ids = {item["binding_id"] for item in managed}
    retained_uids = {
        item["instrument_uid"] for item in catalog["bindings"]
        if item["binding_id"] not in managed_ids
    }
    catalog["bindings"] = [
        item for item in catalog["bindings"] if item["binding_id"] not in managed_ids
    ]
    catalog["instruments"] = [
        item for item in catalog["instruments"] if item["instrument_uid"] in retained_uids
    ]
    acquisition["bindings"] = [
        item for item in acquisition["bindings"] if item["binding_id"] not in managed_ids
    ]
    scope["binding_ids"] = [
        item for item in scope["binding_ids"] if item not in managed_ids
    ]
    return catalog, acquisition, scope


def _binance_symbol(symbol: str, base: str, contract_type: str = "PERPETUAL") -> dict:
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


def _okx_swap(base: str) -> dict:
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


def _okx_future(base: str, alias: str, expiry: str) -> dict:
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


def _metadata() -> dict[tuple[str, str], object]:
    binance = [
        _binance_symbol(f"{base}USDT", base)
        for base in ("BTC", "ETH", "SOL", "DOGE", "BNB")
    ]
    for base in ("BTC", "ETH"):
        binance.extend((
            _binance_symbol(f"{base}USDT_260925", base, "CURRENT_QUARTER"),
            _binance_symbol(f"{base}USDT_261225", base, "NEXT_QUARTER"),
        ))
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


def _rolled_metadata() -> dict[tuple[str, str], object]:
    result = deepcopy(_metadata())
    symbols = result[("BINANCE", "USDM")]["symbols"]
    for item in symbols:
        if item["contractType"] == "CURRENT_QUARTER":
            item["symbol"] = item["symbol"].replace("260925", "261225")
        elif item["contractType"] == "NEXT_QUARTER":
            item["symbol"] = item["symbol"].replace("261225", "270327")
    futures = result[("OKX", "FUTURES")]
    for item in futures:
        if item["alias"] == "quarter":
            item["instId"] = item["instId"].replace("260925", "261225")
        else:
            item["instId"] = item["instId"].replace("261225", "270327")
    return result


class ReferenceL2MaterializerTests(unittest.TestCase):
    @staticmethod
    def _inventory() -> ActiveDemandInventory:
        manifest = DemandManifest.load_many((DEMAND_PATH,))
        return ActiveDemandInventory(
            revision=manifest.revision,
            requirements=manifest.requirements,
            source_documents=(),
            candidates=(),
            exclusions=(),
            input_sha256="f" * 64,
        )

    def _materialize(
        self,
        payloads: dict[tuple[str, str], object],
        *,
        catalog: dict | None = None,
        acquisition: dict | None = None,
        scope: dict | None = None,
        consumer_manifest: dict | None = None,
    ):
        inventory = self._inventory()
        admission = admit_provider_metadata(inventory, payloads)
        convergence = converge_active_demand(
            inventory,
            admission,
            ActiveDemandSourceRegistry.load(REGISTRY_PATH).admission_policy,
        )
        baseline_catalog, baseline_acquisition, baseline_scope = _fixture_baseline()
        return build_reference_l2_materialization(
            inventory=inventory,
            admission=admission,
            convergence=convergence,
            current_catalog_document=catalog or baseline_catalog,
            current_acquisition_document=acquisition or baseline_acquisition,
            current_promotion_scope_document=scope or baseline_scope,
            current_consumer_manifest_document=consumer_manifest,
        )

    def test_materializes_reference_catalog_and_shared_l2_contract(self):
        original_catalog, original_acquisition, _scope = _fixture_baseline()
        materialized = self._materialize(_metadata())

        self.assertEqual(materialized.summary["runtime_mutations"], 0)
        self.assertEqual(materialized.summary["provider_requests"], 0)
        self.assertEqual(materialized.summary["physical_l2_book_count"], 12)
        self.assertEqual(materialized.summary["logical_l2_binding_count"], 24)
        self.assertEqual(materialized.summary["reference_entitlement_count"], 55)
        self.assertEqual(materialized.summary["l2_shared_runtime_role_count"], 3)
        self.assertEqual(
            {tuple(item) for item in materialized.summary["l2_runtime_roles"]},
            {("BINANCE", "USDM"), ("OKX", "SWAP"), ("OKX", "FUTURES")},
        )

        expected_existing = {
            item["binding_id"]: item
            for item in original_catalog["bindings"]
        }
        actual = {item["binding_id"]: item for item in materialized.source_catalog["bindings"]}
        for binding_id, item in expected_existing.items():
            self.assertEqual(actual[binding_id], item)
        self.assertEqual(
            materialized.source_catalog["catalog_revision"],
            original_catalog["catalog_revision"] + 1,
        )

        books = [
            item for item in materialized.source_catalog["bindings"]
            if item["feed"] in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
        ]
        self.assertEqual(len(books), 24)
        grouped: dict[tuple[str, str], list[dict]] = {}
        for item in books:
            grouped.setdefault((item["instrument_uid"], item["source"]["source_id"]), []).append(item)
        self.assertEqual(len(grouped), 12)
        self.assertTrue(all({row["feed"] for row in rows} == {"BOOK_SNAPSHOT", "BOOK_DELTA"} for rows in grouped.values()))

        acquired = {item["binding_id"]: item for item in materialized.acquisition_plan["bindings"]}
        self.assertEqual(len(acquired), len(materialized.source_catalog["bindings"]))
        self.assertEqual(
            original_acquisition["bindings"][0],
            acquired[original_acquisition["bindings"][0]["binding_id"]],
        )

        manifest = materialized.consumer_manifest
        self.assertEqual(manifest["metadata"]["id"], REFERENCE_L2_CONSUMER_ID)
        self.assertEqual(manifest["spec"]["rollback_contract"], "V2")
        self.assertEqual(manifest["spec"]["execution_dependency"], "FORBIDDEN")
        requirements = manifest["spec"]["requirements"]
        self.assertEqual(len(requirements), 79)
        self.assertEqual(sum(row["feed"] == FeedType.MARK_INDEX_PRICE.value for row in requirements), 10)
        self.assertEqual(sum(row["feed"] in {"BOOK_SNAPSHOT", "BOOK_DELTA"} for row in requirements), 24)
        self.assertTrue(all(row["consumer_grade"] == "RESEARCH" for row in requirements))
        by_uid = {
            row["instrument_uid"]: row
            for row in materialized.source_catalog["instruments"]
        }
        self.assertFalse(any(
            row["feed"] == FeedType.BASIS.value
            and by_uid[row["instrument_uid"]]["venue"] == "OKX"
            and by_uid[row["instrument_uid"]]["market"] == "SWAP"
            for row in requirements
        ))

    def test_output_is_idempotent_against_its_own_documents(self):
        first = self._materialize(_metadata())
        repeated = self._materialize(
            _metadata(),
            catalog=first.source_catalog,
            acquisition=first.acquisition_plan,
            scope=first.promotion_scope,
            consumer_manifest=first.consumer_manifest,
        )
        self.assertEqual(repeated.source_catalog, first.source_catalog)
        self.assertEqual(repeated.acquisition_plan, first.acquisition_plan)
        self.assertEqual(repeated.promotion_scope, first.promotion_scope)
        self.assertEqual(repeated.consumer_manifest, first.consumer_manifest)
        self.assertFalse(repeated.summary["source_changed"])
        self.assertFalse(repeated.summary["acquisition_changed"])
        self.assertFalse(repeated.summary["promotion_scope_changed"])

    def test_quarterly_roll_replaces_acquisition_but_retains_old_catalog_lineage(self):
        first = self._materialize(_metadata())
        second = self._materialize(
            _rolled_metadata(),
            catalog=first.source_catalog,
            acquisition=first.acquisition_plan,
            scope=first.promotion_scope,
            consumer_manifest=first.consumer_manifest,
        )
        old_binding_ids = {
            item["binding_id"] for item in first.source_catalog["bindings"]
            if item["feed"] in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
            and ("260925" in item["binding_id"])
        }
        new_binding_ids = {item["binding_id"] for item in second.source_catalog["bindings"]}
        self.assertTrue(old_binding_ids)
        self.assertFalse(old_binding_ids & new_binding_ids)
        old_instruments = [
            item for item in second.source_catalog["instruments"]
            if "260925" in item["native_symbol"]
        ]
        self.assertTrue(old_instruments)
        active_uids = {item["instrument_uid"] for item in second.source_catalog["bindings"]}
        self.assertTrue(all(item["instrument_uid"] not in active_uids for item in old_instruments))
        self.assertGreater(
            second.source_catalog["catalog_revision"], first.source_catalog["catalog_revision"]
        )
        self.assertGreater(
            second.consumer_manifest["metadata"]["revision"],
            first.consumer_manifest["metadata"]["revision"],
        )

    def test_missing_quarterly_metadata_fails_closed_before_render(self):
        payloads = _metadata()
        payloads[("OKX", "FUTURES")] = [
            item for item in payloads[("OKX", "FUTURES")]
            if not (item["instId"].startswith("BTC-") and item["alias"] == "quarter")
        ]
        with self.assertRaisesRegex(Exception, "fully admitted convergence"):
            self._materialize(payloads)


if __name__ == "__main__":
    unittest.main()
