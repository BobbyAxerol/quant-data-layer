"""DL v2.4.1 canonical instrument metadata v1: rules, fail-closed loader, Query `canonical` field, SDK helpers.

Fixtures are verbatim public venue responses captured 2026-11-19T08:40:58Z (Binance /fapi/v1/constituents,
/fapi/v1/premiumIndex, OKX /api/v5/public/instruments and /mark-price, SWAP) and the live r16 catalogue rows.
"""
import copy
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from qdl.runtime import canonical_metadata as cm
from scripts import canonical_instrument_metadata as tool
from qdl_sdk import instruments
from qdl_sdk.errors import CanonicalMetadataUnavailable
from qdl_sdk.models import InstrumentView

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/v241_canonical"
RULES = ROOT / "config/v2/canonical-underlying-rules.yaml"


def rendered():
    return tool.render(FIXTURES / "catalog.json", FIXTURES / "captures", RULES, 1)


class RulesAndRenderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = rendered()
        cls.by_symbol = {(e["venue"], e["native_symbol"]): e for e in cls.document["instruments"]}

    def entry(self, venue, symbol):
        return self.by_symbol[(venue, symbol)]

    def test_every_verified_rule_cites_venue_documentation_and_unverified_carries_nothing(self):
        for rule in json.loads(RULES.read_text())["rules"]:
            with self.subTest(rule=rule["id"]):
                if rule["status"] == "VERIFIED":
                    self.assertTrue(rule["doc_urls"] and all(u.startswith("https://") for u in rule["doc_urls"]))
                    self.assertTrue(rule["doc_quotes"])
        for entry in self.document["instruments"]:
            if entry["status"] == "UNVERIFIED":
                self.assertIsNone(entry["price_multiplier"])
                self.assertIsNone(entry["quantity_multiplier"])
                self.assertIsNone(entry["underlying_instrument_uid"])
                self.assertEqual(entry["aliases"], [])

    def test_binance_1000x_contracts_and_their_okx_counterparts(self):
        for base in ("PEPE", "BONK", "FLOKI", "SHIB"):
            with self.subTest(base=base):
                binance = self.entry("BINANCE", f"1000{base}USDT")
                okx = self.entry("OKX", f"{base}-USDT-SWAP")
                self.assertEqual((binance["status"], binance["underlying_asset"]), ("VERIFIED", base))
                self.assertEqual((binance["price_multiplier"], binance["quantity_multiplier"]), ("1000", "1000"))
                self.assertEqual(okx["price_multiplier"], "1")
                self.assertEqual(binance["underlying_instrument_uid"], okx["underlying_instrument_uid"])
                self.assertIn(okx["native_symbol"], [a["native_symbol"] for a in binance["aliases"]])
        self.assertEqual(self.entry("OKX", "PEPE-USDT-SWAP")["quantity_multiplier"], "10000000")

    def test_luna2_is_okx_luna_through_the_reviewed_rename_only(self):
        luna2 = self.entry("BINANCE", "LUNA2USDT")
        self.assertEqual((luna2["rule_id"], luna2["underlying_asset"]), ("BINANCE_USDM_RENAME_LUNA2", "LUNA"))
        self.assertEqual(luna2["underlying_instrument_uid"], self.entry("OKX", "LUNA-USDT-SWAP")["underlying_instrument_uid"])

    def test_a_digit_leading_asset_is_not_read_as_a_multiplier(self):
        oneinch = self.entry("BINANCE", "1INCHUSDT")
        self.assertEqual((oneinch["underlying_asset"], oneinch["price_multiplier"]), ("1INCH", "1"))

    def test_fail_closed_cases(self):
        # A reused ticker: OKX QNT prices 4.6x Binance QNT, so neither side is verified.
        for venue, symbol in (("BINANCE", "QNTUSDT"), ("OKX", "QNT-USDT-SWAP")):
            entry = self.entry(venue, symbol)
            self.assertEqual(entry["status"], "UNVERIFIED")
            self.assertIn("cross-venue price ratio", entry["source"]["evidence"])
        self.assertEqual(self.entry("BINANCE", "SPACEUSDT")["status"], "UNVERIFIED")  # no binance constituent
        self.assertEqual(self.entry("OKX", "BTC-USD-SWAP")["rule_id"], "OKX_INVERSE_CONTRACT")
        self.assertEqual(self.entry("HOSE", "FPT")["rule_id"], "NO_RULE")

    def test_an_unreviewed_spelling_is_unverified(self):
        captures = FIXTURES / "captures"
        catalog = json.loads((FIXTURES / "catalog.json").read_text())
        item = copy.deepcopy(next(i for i in catalog["instruments"] if i["native_symbol"] == "1000PEPEUSDT"))
        item["base_asset"] = "PEPE2"
        entry = tool._binance(item, tool._rules(RULES), captures, "t")
        self.assertEqual(entry["status"], "UNVERIFIED")

    def test_the_gate_passes_and_the_runtime_loader_accepts_the_render(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "canonical-instrument-metadata.json"
            path.write_text(json.dumps(self.document))
            result = tool.gate(path, FIXTURES / "catalog.json", FIXTURES / "captures")
        self.assertEqual(result["status"], "PASS", result["problems"])
        self.assertEqual(len(cm.CanonicalMetadataIndex(self.document)), len(self.document["instruments"]))


class LoaderFailClosedTests(unittest.TestCase):
    def setUp(self):
        self.document = rendered()

    def resealed(self, document):
        document["sha256"] = cm.document_sha256(document)
        return document

    def test_tampering_or_unknown_fields_refuse_the_whole_file(self):
        tampered = copy.deepcopy(self.document)
        tampered["instruments"][0]["native_symbol"] += "X"
        with self.assertRaisesRegex(cm.CanonicalMetadataError, "sha256"):
            cm.CanonicalMetadataIndex(tampered)
        extra = copy.deepcopy(self.document)
        extra["instruments"][0]["multiplier"] = "1"
        with self.assertRaisesRegex(cm.CanonicalMetadataError, "fields"):
            cm.CanonicalMetadataIndex(self.resealed(extra))

    def test_unverified_entries_may_not_carry_multipliers(self):
        document = copy.deepcopy(self.document)
        entry = next(e for e in document["instruments"] if e["status"] == "UNVERIFIED")
        entry["price_multiplier"] = "1"
        with self.assertRaisesRegex(cm.CanonicalMetadataError, "UNVERIFIED"):
            cm.CanonicalMetadataIndex(self.resealed(document))

    def test_non_positive_multiplier_and_dangling_alias_refused(self):
        document = copy.deepcopy(self.document)
        entry = next(e for e in document["instruments"] if e["status"] == "VERIFIED")
        entry["price_multiplier"] = "0"
        with self.assertRaisesRegex(cm.CanonicalMetadataError, "positive"):
            cm.CanonicalMetadataIndex(self.resealed(document))
        document = copy.deepcopy(self.document)
        entry = next(e for e in document["instruments"] if e["aliases"])
        entry["aliases"][0]["instrument_uid"] = "00000000-0000-0000-0000-000000000000"
        with self.assertRaisesRegex(cm.CanonicalMetadataError, "unknown instrument"):
            cm.CanonicalMetadataIndex(self.resealed(document))

    def test_environment_opt_in(self):
        self.assertIsNone(cm.index_from_environment({}))
        with self.assertRaises(cm.CanonicalMetadataError):
            cm.index_from_environment({"QDL_CANONICAL_INSTRUMENT_METADATA": "/nonexistent/canonical.json"})


def view(entry, index):
    item = index.get(entry["instrument_uid"])
    return InstrumentView.model_validate({
        "instrument_uid": entry["instrument_uid"], "instrument_id": entry["instrument_id"],
        "venue": entry["venue"], "market": entry["market"], "product_type": "PERPETUAL",
        "canonical_symbol": "X", "metadata_revision": 1, "asset_class": "DERIVATIVE",
        "native_symbol": entry["native_symbol"], "status": "ACTIVE",
        **({"canonical": item.as_public()} if item is not None else {})})


class SdkHelperTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = rendered()
        cls.index = cm.CanonicalMetadataIndex(cls.document)
        cls.views = {(e["venue"], e["native_symbol"]): view(e, cls.index) for e in cls.document["instruments"]}

    def test_1000x_prices_and_quantities_normalise_exactly(self):
        pepe_b, pepe_o = self.views[("BINANCE", "1000PEPEUSDT")], self.views[("OKX", "PEPE-USDT-SWAP")]
        self.assertEqual(instruments.to_underlying_price(pepe_b, "0.0038000"), Decimal("0.0000038"))
        self.assertEqual(instruments.to_underlying_price(pepe_o, Decimal("0.0000038")), Decimal("0.0000038"))
        self.assertEqual(instruments.to_underlying_quantity(pepe_b, 5), Decimal(5000))
        self.assertEqual(instruments.to_underlying_quantity(pepe_o, "0.1"), Decimal("1000000.0"))
        for base in ("BONK", "FLOKI", "SHIB", "PEPE"):
            self.assertTrue(instruments.same_underlying(self.views[("BINANCE", f"1000{base}USDT")],
                                                        self.views[("OKX", f"{base}-USDT-SWAP")]))
        self.assertTrue(instruments.same_underlying(self.views[("BINANCE", "LUNA2USDT")],
                                                    self.views[("OKX", "LUNA-USDT-SWAP")]))
        self.assertFalse(instruments.same_underlying(self.views[("BINANCE", "BTCUSDT")],
                                                     self.views[("OKX", "PEPE-USDT-SWAP")]))
        self.assertEqual(instruments.to_underlying_price(self.views[("BINANCE", "BTCUSDT")], "91234.5"),
                         Decimal("91234.5"))

    def test_missing_or_unverified_metadata_is_a_typed_refusal_never_1_to_1(self):
        for key, code in ((("BINANCE", "QNTUSDT"), "CANONICAL_METADATA_UNVERIFIED"),
                          (("OKX", "BTC-USD-SWAP"), "CANONICAL_METADATA_UNVERIFIED")):
            with self.subTest(key=key), self.assertRaises(CanonicalMetadataUnavailable) as caught:
                instruments.to_underlying_price(self.views[key], "1")
            self.assertEqual(caught.exception.code, code)
        bare = self.views[("BINANCE", "BTCUSDT")].model_copy(update={"canonical": None})
        for helper in (instruments.to_underlying_price, instruments.to_underlying_quantity):
            with self.assertRaises(CanonicalMetadataUnavailable) as caught:
                helper(bare, "1")
            self.assertEqual(caught.exception.code, "CANONICAL_METADATA_MISSING")
        with self.assertRaises(CanonicalMetadataUnavailable):
            instruments.same_underlying(bare, self.views[("OKX", "BTC-USDT-SWAP")])

    def test_floats_are_refused(self):
        with self.assertRaises(TypeError):
            instruments.to_underlying_price(self.views[("BINANCE", "BTCUSDT")], 1.5)

    def test_an_older_server_without_canonical_still_parses(self):
        payload = self.views[("BINANCE", "BTCUSDT")].model_dump(by_alias=True)
        payload.pop("canonical")
        self.assertIsNone(InstrumentView.model_validate(payload).canonical)


class QueryEndpointTests(unittest.TestCase):
    def test_canonical_is_added_only_when_served(self):
        from qdl.api_v2.router import _instrument_item
        document = rendered()
        index = cm.CanonicalMetadataIndex(document)
        entry = next(e for e in document["instruments"] if e["native_symbol"] == "1000PEPEUSDT")
        identity = SimpleNamespace(instrument_uid=entry["instrument_uid"], instrument_id=entry["instrument_id"])
        item = SimpleNamespace(identity=identity, metadata_revision=1,
                               asset_class=SimpleNamespace(value="DERIVATIVE"), native_symbol=entry["native_symbol"],
                               status=SimpleNamespace(value="ACTIVE"))
        from qdl.query.service import V2QueryService
        service = SimpleNamespace(canonical_for=lambda uid: V2QueryService.canonical_for(
            SimpleNamespace(canonical_metadata=index), uid))
        served = _instrument_item(service, item)
        self.assertEqual(served["canonical"]["price_multiplier"], "1000")
        self.assertEqual(served["canonical"]["schema"], "qdl.instrument.canonical-metadata.v1")
        plain = _instrument_item(SimpleNamespace(canonical_for=lambda uid: None), item)
        self.assertNotIn("canonical", plain)


if __name__ == "__main__":
    unittest.main()
