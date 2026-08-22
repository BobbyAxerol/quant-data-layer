from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path

import yaml

from qdl.adapters.binance_spot import parse_spot_exchange_info
from qdl.adapters.binance_usdm import parse_exchange_info
from qdl.runtime.production_catalog import (
    ProductionCatalogBuilder,
    ProductionDemandManifest,
    load_okx_instruments,
)
from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
DEMAND_PATH = ROOT / "config/v2/stable-crypto-demand.yaml"
CAPTURES = ROOT / "config/v2/captures"
PROVENANCE = CAPTURES / "provenance.json"
CRYPTO_VENUES = {"BINANCE", "OKX"}

# Fields where the committed catalog disagrees with the provider capture that
# regenerates it. Each was verified directly against the raw provider response,
# and the committed value is the wrong one: the Binance Spot tick and step were
# copied from the USD-M record, and the two OKX steps do not match lotSz.
#
# Pinned so the set can shrink but never grow. Correcting them bumps the catalog
# revision, which strands the BAR edge checkpoint (see C.19), so it belongs to an
# approved rollout rather than to this test. See plan section C.27.
KNOWN_METADATA_DRIFT = {
    ("BINANCE.SPOT.SPOT.BTC-USDT", "price_tick"),
    ("BINANCE.SPOT.SPOT.BTC-USDT", "quantity_step"),
    ("OKX.SPOT.SPOT.BTC-USDT", "quantity_step"),
    ("OKX.SWAP.PERPETUAL.BTC-USDT", "quantity_step"),
}
IDENTITY_FIELDS = (
    "instrument_uid", "instrument_id", "venue", "market", "product_type",
    "canonical_symbol", "native_symbol", "base_asset", "quote_asset",
    "settlement_asset", "asset_class",
)


def _regenerate() -> dict:
    demand = ProductionDemandManifest.load_many([DEMAND_PATH])
    live = StableSourceCatalog.load(CATALOG_PATH)
    usdm = parse_exchange_info(
        json.loads((CAPTURES / "binance-usdm-exchangeinfo.filtered.json").read_text()),
        valid_from_ns=0,
    )
    spot = parse_spot_exchange_info(
        json.loads((CAPTURES / "binance-spot-exchangeinfo.filtered.json").read_text()),
        valid_from_ns=0,
    )
    okx = load_okx_instruments(
        CAPTURES / "okx-instruments-swap.filtered.json"
    ) + load_okx_instruments(CAPTURES / "okx-instruments-spot.filtered.json")
    return ProductionCatalogBuilder(
        catalog_revision=live.catalog_revision,
        source_policy_revision=live.source_policy_revision,
        authority_revision=live.authority_revision,
    ).build(
        demand=demand, binance_usdm=usdm, okx_rows=okx, binance_spot=spot
    ).source_catalog


class CaptureProvenanceTests(unittest.TestCase):
    """A capture is evidence only while its bytes still match what was recorded."""

    def setUp(self) -> None:
        self.provenance = json.loads(PROVENANCE.read_text(encoding="utf-8"))

    def test_every_committed_capture_matches_its_recorded_hash(self):
        for entry in self.provenance["captures"]:
            with self.subTest(venue=entry["venue"], market=entry["market"]):
                data = (ROOT / entry["filtered_capture"]).read_bytes()
                self.assertEqual(
                    hashlib.sha256(data).hexdigest(),
                    entry["filtered_capture_sha256"],
                )
                self.assertEqual(len(data), entry["filtered_capture_bytes"])

    def test_provenance_declares_the_metadata_is_not_fabricated(self):
        self.assertFalse(self.provenance["fabricated_metadata"])
        for entry in self.provenance["captures"]:
            self.assertTrue(entry["endpoint"].startswith("https://"))
            self.assertGreater(entry["full_response_bytes"], entry["filtered_capture_bytes"])


class CatalogRegenerationTests(unittest.TestCase):
    """The committed catalog must be reproducible from the committed inputs."""

    def setUp(self) -> None:
        self.generated = {
            item["instrument_id"]: item for item in _regenerate()["instruments"]
        }
        raw = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
        self.committed = {
            item["instrument_id"]: item
            for item in raw["instruments"]
            if item["venue"] in CRYPTO_VENUES
        }

    def test_the_same_instrument_set_is_produced(self):
        self.assertEqual(set(self.generated), set(self.committed))

    def test_identity_fields_reproduce_exactly(self):
        for instrument_id in sorted(self.generated):
            for field in IDENTITY_FIELDS:
                with self.subTest(instrument=instrument_id, field=field):
                    self.assertEqual(
                        str(self.generated[instrument_id].get(field)),
                        str(self.committed[instrument_id].get(field)),
                    )

    def test_metadata_drift_does_not_grow(self):
        drift = set()
        for instrument_id in sorted(self.generated):
            for field in ("price_tick", "quantity_step", "contract_multiplier"):
                generated = self.generated[instrument_id].get(field)
                committed = self.committed[instrument_id].get(field)
                # Compare numerically: 0.1 and 0.10 are the same tick.
                if float(generated) != float(committed):
                    drift.add((instrument_id, field))
        self.assertEqual(
            drift - KNOWN_METADATA_DRIFT,
            set(),
            "the committed catalog drifted further from the provider capture",
        )
        self.assertTrue(
            drift.issubset(KNOWN_METADATA_DRIFT),
            "unexpected metadata drift",
        )


if __name__ == "__main__":
    unittest.main()
