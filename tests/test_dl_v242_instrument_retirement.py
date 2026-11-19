"""DL v2.4.2: retiring venue-delisted instruments through the declaration tool (the one non-additive path).

Base: the committed stage C r8 repository (catalogue r16), which still declares OKX `RVN-USDT-SWAP` (delisted
2026-11-19) and the expired Binance quarterlies `BTCUSDT_260925` / `ETHUSDT_260925`. Listings: the v2.4.2 listing
fixtures (OKX lists BTC only; Binance lists BTCUSDT and RVNUSDT as TRADING), so the three are missing and Binance
`RVNUSDT` is listed and live. The full render + gate on the live runtime set is the r10 evidence render.
"""
import json
import tempfile
import unittest
from pathlib import Path

import yaml

from scripts import dl_declaration as tool

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "upgrade/evidence/releases/v2.3.1/declaration-stage-c-r8/repo"
LISTINGS = [
    {"venue": "OKX", "market": "SWAP", "path": ROOT / "tests/fixtures/v242_listings/okx-instruments-swap.json"},
    {"venue": "BINANCE", "market": "USDM", "path": ROOT / "tests/fixtures/v242_listings/binance-usdm-exchangeinfo.json"},
]
RVN = "OKX.SWAP.PERPETUAL.RVN-USDT"
EXPIRED = ("BINANCE.USDM.FUTURE.BTC-USDT-260925", "BINANCE.USDM.FUTURE.ETH-USDT-260925")
RVN_BINDING = "okx-swap-rvn-usdt-swap-bar-1d"


def _retire(*ids, runtime=None):
    return tool.RetiredBase(BASE, runtime or {}, [{"instrument_id": i, "reason": "delisted"} for i in ids], LISTINGS)


def _doc(root, rel):
    raw = (root / rel).read_bytes()
    return json.loads(raw) if raw.lstrip().startswith(b"{") else yaml.safe_load(raw)


class RetiredBaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.retired = _retire(RVN, *EXPIRED)
        cls.catalog = _doc(cls.retired.root, tool.REPO_FILES["catalog"])
        cls.base_catalog = _doc(BASE, tool.REPO_FILES["catalog"])

    def test_only_the_retired_instruments_and_their_rows_leave(self):
        ids = {row["instrument_id"] for row in self.catalog["instruments"]}
        base_ids = {row["instrument_id"] for row in self.base_catalog["instruments"]}
        self.assertEqual(base_ids - ids, {RVN, *EXPIRED})
        self.assertIn("BINANCE.USDM.PERPETUAL.RVN-USDT", ids, "Binance RVNUSDT is still TRADING and stays")
        bindings = {row["binding_id"] for row in self.catalog["bindings"]}
        base_bindings = {row["binding_id"]: row for row in self.base_catalog["bindings"]}
        self.assertEqual(set(base_bindings) - bindings, {RVN_BINDING})
        summary = self.retired.summary
        self.assertEqual(summary["binding_ids"], [RVN_BINDING])
        self.assertEqual(summary["catalog_revision"], [16, 17])
        self.assertEqual((summary["demand_rows_removed"], summary["routing_products_removed"]), (1, 1))
        self.assertEqual(summary["scope_ids_removed"], [RVN_BINDING])
        self.assertEqual(summary["manifest_requirements_removed"], {"alpha.okx.paper.stable": 1})
        acquisition = {row["binding_id"] for row in _doc(self.retired.root, tool.REPO_FILES["acquisition"])["bindings"]}
        self.assertNotIn(RVN_BINDING, acquisition)

    def test_retained_bindings_keep_their_rows_and_lineage(self):
        base = {row["binding_id"]: row for row in self.base_catalog["bindings"]}
        changed = [row["binding_id"] for row in self.catalog["bindings"] if row != base[row["binding_id"]]]
        self.assertEqual(changed, [], "a retained binding's catalogue row (lineage included) must not move")

    def test_the_manifest_row_goes_but_its_revision_is_left_to_the_render(self):
        rel = Path("consumers/stable/alpha-okx-paper.yaml")
        base, retired = _doc(BASE, rel), _doc(self.retired.root, rel)
        uid = next(r["instrument_uid"] for r in self.base_catalog["instruments"] if r["instrument_id"] == RVN)
        self.assertTrue(any(r.get("instrument_uid") == uid for r in base["spec"]["requirements"]))
        self.assertFalse(any(r.get("instrument_uid") == uid for r in retired["spec"]["requirements"]))
        self.assertEqual(retired["metadata"]["revision"], base["metadata"]["revision"])

    def test_the_same_inputs_give_the_same_bytes(self):
        again = _retire(RVN, *EXPIRED)
        for rel in tool.REPO_FILES.values():
            self.assertEqual((again.root / rel).read_bytes(), (self.retired.root / rel).read_bytes(), rel)


class RetirementRefusalTests(unittest.TestCase):
    def test_a_listed_live_instrument_is_refused(self):
        with self.assertRaisesRegex(tool.DeclarationRefused, "missing or not live"):
            _retire("BINANCE.USDM.PERPETUAL.RVN-USDT")

    def test_an_unknown_instrument_is_refused(self):
        with self.assertRaisesRegex(tool.DeclarationRefused, "does not hold"):
            _retire("OKX.SWAP.PERPETUAL.NOPE-USDT")

    def test_a_live_ingested_binding_is_refused(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "ingestor.json"
            path.write_text(json.dumps({"bindings": [{"source_id": "okx-swap-rvn-usdt-swap-bar-1d-primary-v2"}]}))
            with self.assertRaisesRegex(tool.DeclarationRefused, "live ingestor"):
                _retire(RVN, runtime={"ingestors": {"ingestor-okx-swap.json": path}})

    def test_core_rows_are_retired_in_the_copy_only(self):
        with tempfile.TemporaryDirectory() as raw:
            core = Path(raw) / "rust_core.json"
            rows = [{"source_id": "okx-swap-rvn-usdt-swap-bar-1d-primary-v2", "instrument_uid": "x"},
                    {"source_id": "kept", "instrument_uid": "y"}]
            core.write_text(json.dumps({"authority": {}, "core": {"bindings": rows}}))
            retired = _retire(RVN, runtime={"cores": [core]})
            copy = json.loads(Path(retired.runtime["cores"][0]).read_text())
            self.assertEqual([r["source_id"] for r in copy["core"]["bindings"]], ["kept"])
            self.assertEqual(len(json.loads(core.read_text())["core"]["bindings"]), 2, "the live file is untouched")

    def test_a_deployed_realm_manifest_drops_only_the_retired_row_and_keeps_its_tokens(self):
        source = BASE / "consumers/stable/alpha-okx-paper.yaml"  # stands in for a realm file (same schema)
        retired = _retire(RVN, runtime={"realm_manifests": {"alpha.okx.paper.stable": source}})
        before = yaml.safe_load(source.read_text())
        after = yaml.safe_load(Path(retired.runtime["realm_manifests"]["alpha.okx.paper.stable"]).read_text())
        self.assertEqual(after["metadata"]["revision"], before["metadata"]["revision"] + 1)
        self.assertEqual(after["metadata"]["min_token_revision"], 1, "advisory floor: current tokens stay valid")
        removed = [r for r in before["spec"]["requirements"] if r not in after["spec"]["requirements"]]
        self.assertEqual(len(removed), 1)
        self.assertEqual(len(after["spec"]["requirements"]), len(before["spec"]["requirements"]) - 1)
        self.assertEqual(retired.summary["realm_manifests"]["alpha.okx.paper.stable"]["requirements_removed"], 1)
        self.assertEqual(yaml.safe_load(source.read_text()), before, "the deployed file is untouched")

    def test_the_declaration_field_is_validated(self):
        base = {"schema": tool.DECLARATION_SCHEMA, "declaration_revision": 10,
                "consumers": [{"consumer_id": "c", "manifest": "m.yaml"}],
                "venue_listings": [{"venue": "OKX", "market": "SWAP", "path": "x.json"}]}
        cases = {
            "list of": [{"instrument_id": RVN}],
            "unique": [{"instrument_id": RVN, "reason": "a"}, {"instrument_id": RVN, "reason": "b"}],
        }
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "declaration.yaml"
            for message, retire in cases.items():
                with self.subTest(message=message):
                    path.write_text(yaml.safe_dump({**base, "retire_instruments": retire}))
                    with self.assertRaisesRegex(tool.DeclarationRefused, message):
                        tool.load_declaration(path)
            path.write_text(yaml.safe_dump({**base, "declaration_revision": 9, "venue_listings": [],
                                            "retire_instruments": [{"instrument_id": RVN, "reason": "a"}]}))
            with self.assertRaisesRegex(tool.DeclarationRefused, "needs venue_listings"):
                tool.load_declaration(path)


if __name__ == "__main__":
    unittest.main()
