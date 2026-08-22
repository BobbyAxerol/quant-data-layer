from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

import yaml

from qdl.domain.instrument import InstrumentIdentity, ProductType
from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"


def _raw() -> dict:
    return yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))


def _write(directory: Path, payload: dict) -> StableSourceCatalog:
    path = directory / "catalog.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return StableSourceCatalog.load(path)


class CatalogInstrumentDeclarationTests(unittest.TestCase):
    """An instrument may be declared without a materialised binding.

    A bound feed is acquired and stored; a pass-through history request only
    needs the instrument's identity and metadata. The catalog previously
    derived instruments from bindings alone, so the second case could not be
    expressed at all.
    """

    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)

    def test_declared_instruments_are_retained(self):
        declared = {item.identity.instrument_uid for item in self.catalog.instruments}
        bound = {
            binding.instrument.identity.instrument_uid
            for binding in self.catalog.bindings
        }
        self.assertEqual(declared, bound, "today every instrument is still bound")
        self.assertTrue(declared)

    def test_a_bound_instrument_resolves(self):
        uid = self.catalog.bindings[0].instrument.identity.instrument_uid
        self.assertEqual(
            self.catalog.instrument_for(uid).identity.instrument_uid, uid
        )

    def test_an_unbound_instrument_is_kept_and_resolvable(self):
        payload = _raw()
        spare = copy.deepcopy(payload["instruments"][0])
        # The UID is a deterministic UUIDv5 of the instrument id, so it has to
        # be derived rather than invented.
        identity = InstrumentIdentity.create(
            venue="BINANCE",
            market="USDM",
            product_type=ProductType("PERPETUAL"),
            canonical_symbol="SOL-USDT",
        )
        spare["instrument_uid"] = identity.instrument_uid
        spare["instrument_id"] = identity.instrument_id
        spare["canonical_symbol"] = "SOL-USDT"
        spare["native_symbol"] = "SOLUSDT"
        spare["base_asset"] = "SOL"
        payload["instruments"].append(spare)
        with tempfile.TemporaryDirectory() as raw:
            catalog = _write(Path(raw), payload)
            bound = {
                binding.instrument.identity.instrument_uid
                for binding in catalog.bindings
            }
            self.assertNotIn(identity.instrument_uid, bound)
            resolved = catalog.instrument_for(identity.instrument_uid)
            self.assertEqual(resolved.native_symbol, "SOLUSDT")
            self.assertEqual(len(catalog.instruments), len(payload["instruments"]))

    def test_an_undeclared_instrument_uid_fails_closed(self):
        with self.assertRaises(KeyError):
            self.catalog.instrument_for("00000000-0000-5000-8000-00000000dead")

    def test_a_binding_cannot_reference_an_undeclared_instrument(self):
        payload = _raw()
        payload["bindings"][0]["instrument_uid"] = "00000000-0000-5000-8000-00000000dead"
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaises(ValueError):
                _write(Path(raw), payload)

    def test_duplicate_instrument_uids_fail_closed(self):
        payload = _raw()
        payload["instruments"].append(copy.deepcopy(payload["instruments"][0]))
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaises(ValueError):
                _write(Path(raw), payload)


if __name__ == "__main__":
    unittest.main()
