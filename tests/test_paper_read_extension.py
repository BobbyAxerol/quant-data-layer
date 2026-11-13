"""IDR-075 preparation tests; metadata fixtures are TEST_ONLY, not live proof."""
from copy import deepcopy
from pathlib import Path
import unittest

import yaml

from qdl.consumer.manifest import ConsumerManifestLoader
from qdl.consumer.universal_release import ConsumerRouteBinding
from scripts.phase115_render_consumer_route_binding import binding_from_stable_release
from scripts.prepare_paper_read_extension import (
    digest,
    prepare_paper_extension,
    prepare_paper_linear_swap_extension,
)
from tests.v226_stage_a_support import V226_STAGE_A
from tests.inverse_extension_support import pre_inverse_paper_manifest, without_inverse_products

ROOT = Path(__file__).resolve().parents[1]


class PaperReadExtensionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Production inputs of the 2026-10-09 render: the deployed catalog r12
        # (= the committed catalog, which already declares BTC-USD-SWAP) and the
        # pre-extension Paper manifest r10 and its binding (recovered, pinned).
        catalog = yaml.safe_load((ROOT / "config/v2/stable-source-bindings.yaml").read_text())
        manifest = pre_inverse_paper_manifest()
        binding = without_inverse_products(
            binding_from_stable_release(ROOT / "config/v2/stable-v2-release-routing.yaml",
                ROOT, "trading-system.paper.stable").canonical_mapping(),
            manifest_revision=manifest["metadata"]["revision"], reseal=digest)
        cls.inputs = dict(catalog=catalog, manifest=manifest, binding=binding)

    def setUp(self):
        self.source = deepcopy(self.inputs)

    def prepare(self):
        return prepare_paper_extension(**self.source)

    def reseal(self):
        self.source["binding"].pop("binding_sha256")
        self.source["binding"]["binding_sha256"] = digest(self.source["binding"])

    def test_preserves_every_old_requirement_and_product_and_budget(self):
        result = self.prepare()
        self.assertEqual(self.source, self.inputs)
        self.assertEqual(result["manifest"]["metadata"]["revision"], 11)
        self.assertEqual(len(result["binding"]["products"]), 64)
        self.assertEqual(result["manifest"]["spec"]["quotas"], self.source["manifest"]["spec"]["quotas"])
        for row in self.source["manifest"]["spec"]["requirements"]:
            self.assertIn(row, result["manifest"]["spec"]["requirements"])
        for row in self.source["binding"]["products"]:
            self.assertIn(row, result["binding"]["products"])
        ConsumerRouteBinding.from_canonical_mapping(result["binding"])

    def test_exact_inverse_feeds_native_units_and_no_duplicate_fx(self):
        result = self.prepare()
        inverse = [p for p in result["binding"]["products"] if p["native_symbol"] == "BTC-USD-SWAP"]
        self.assertEqual({p["feed"] for p in inverse}, {"QUOTE", "MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA"})
        self.assertTrue(all(p["venue"] == "OKX" and p["interval"] is None for p in inverse))
        self.assertEqual(sum(p["native_symbol"] == "BTC-USDT-SWAP" and p["feed"] == "MARK_INDEX_PRICE"
                             for p in result["binding"]["products"]), 1)
        self.assertEqual(result["consumer_environment"]["DATA_LAYER_V2_JWT_MANIFEST_REVISION"], "11")
        self.assertEqual(result["consumer_environment"]["DATA_LAYER_V2_RELEASE_MANIFEST_SHA256"],
                         result["binding"]["universal_manifest_sha256"])

    def test_v2_input_seals_manifest_policy_without_schema_break(self):
        self.source["binding"].pop("request_policies")
        self.source["binding"]["schema"] = "qdl.v2.consumer-route-binding.v2"
        self.reseal()
        result = self.prepare()
        self.assertEqual(result["binding"]["schema"], "qdl.v2.consumer-route-binding.v3")
        self.assertEqual(len(result["binding"]["request_policies"]), 64)
        manifest = ConsumerManifestLoader.from_mapping(result["manifest"])
        self.assertEqual(manifest.manifest_sha256, result["provenance"]["consumer_manifest_sha256"])

    def test_wrong_realm_subject_or_revision_fails(self):
        for field, value in (("environment", "live"), ("subject", "wrong"), ("revision", 11)):
            with self.subTest(field=field):
                self.source = deepcopy(self.inputs)
                self.source["manifest"]["metadata"][field] = value
                with self.assertRaises(ValueError):
                    self.prepare()

    def test_wrong_catalog_identity_or_collateral_fails(self):
        for field, value in (("settlement_asset", "USDT"), ("native_symbol", "BTC-USDT-SWAP")):
            with self.subTest(field=field):
                self.source = deepcopy(self.inputs)
                row = next(i for i in self.source["catalog"]["instruments"] if i["native_symbol"] == "BTC-USD-SWAP")
                row[field] = value
                with self.assertRaises(ValueError):
                    self.prepare()

    def test_unacquired_inverse_feed_fails(self):
        uid = next(i["instrument_uid"] for i in self.source["catalog"]["instruments"] if i["native_symbol"] == "BTC-USD-SWAP")
        self.source["catalog"]["bindings"] = [b for b in self.source["catalog"]["bindings"]
            if not (b["instrument_uid"] == uid and b["feed"] == "BOOK_DELTA")]
        with self.assertRaises((ValueError, KeyError)):
            self.prepare()

    def test_missing_linear_fx_fails(self):
        self.source["binding"]["products"] = [p for p in self.source["binding"]["products"]
            if not (p["native_symbol"] == "BTC-USDT-SWAP" and p["feed"] == "MARK_INDEX_PRICE")]
        ids = {p["requirement_id"] for p in self.source["binding"]["products"]}
        self.source["binding"]["request_policies"] = [p for p in self.source["binding"]["request_policies"]
                                                      if p["requirement_id"] in ids]
        self.reseal()
        with self.assertRaisesRegex(ValueError, "FX entitlement"):
            self.prepare()

    def test_policy_or_quality_drift_fails_even_when_checksum_resealed(self):
        for field in ("policy", "quality"):
            with self.subTest(field=field):
                self.source = deepcopy(self.inputs)
                if field == "policy":
                    self.source["binding"]["request_policies"][0]["max_session_liveness_ms"] = 99999
                else:
                    self.source["binding"]["products"][0]["max_freshness_ms"] += 1
                self.reseal()
                with self.assertRaises(ValueError):
                    self.prepare()

    def test_bad_checksum_and_duplicate_and_repeat_fail(self):
        self.source["binding"]["binding_sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            self.prepare()
        self.source = deepcopy(self.inputs)
        self.source["manifest"]["spec"]["requirements"].append(
            deepcopy(self.source["manifest"]["spec"]["requirements"][0]))
        with self.assertRaises(ValueError):
            self.prepare()
        self.source = deepcopy(self.inputs)
        result = self.prepare()
        self.source.update(manifest=result["manifest"], binding=result["binding"])
        with self.assertRaisesRegex(ValueError, "already present"):
            self.prepare()

    def test_quota_raise_is_applied_only_to_the_named_keys_and_recorded(self):
        plain = self.prepare()
        raised = prepare_paper_extension(**self.source, quotas={"max_streams": 77, "requests_per_minute": 3000})
        quotas = raised["manifest"]["spec"]["quotas"]
        before = self.source["manifest"]["spec"]["quotas"]
        self.assertEqual((quotas["max_streams"], quotas["requests_per_minute"]), (77, 3000))
        self.assertEqual({k: v for k, v in quotas.items() if k not in ("max_streams", "requests_per_minute")},
                         {k: v for k, v in before.items() if k not in ("max_streams", "requests_per_minute")})
        self.assertEqual(raised["provenance"]["quota_change"],
                         {"max_streams": {"from": before["max_streams"], "to": 77},
                          "requests_per_minute": {"from": before["requests_per_minute"], "to": 3000}})
        self.assertNotIn("quota_change", plain["provenance"])
        self.assertEqual(raised["binding"]["products"], plain["binding"]["products"])
        self.assertNotEqual(raised["consumer_environment"], plain["consumer_environment"])  # new manifest hash pairs
        ConsumerManifestLoader.from_mapping(raised["manifest"])

    def test_quota_change_never_lowers_or_touches_other_keys(self):
        current = self.source["manifest"]["spec"]["quotas"]
        for bad in ({"max_streams": current["max_streams"]}, {"requests_per_minute": 1000},
                    {"max_batch_items": 100}, {"max_streams": "77"}):
            with self.assertRaisesRegex(ValueError, "quota change"):
                prepare_paper_extension(**self.source, quotas=bad)

    def _d1_r11(self):
        # IDR-127: the deployed r11 realm file (sha256 efd482fa...) was rendered on 2026-10-09 by this script from
        # these inputs with the D1 quota raise.
        return prepare_paper_extension(**self.source, quotas={"max_streams": 77, "requests_per_minute": 3000})

    def test_committed_r13_manifest_is_the_reproduced_d1_then_v226_stage_a_render(self):
        # r12 (quotas 150/4500 picked by hand) was deployed ~7 min on 2026-11-13 and refused RATE_LIMITED; it is
        # retired, so the stage A manifest is r13 with quotas DERIVED from the products (qdl/consumer/quota_model.py).
        r11 = self._d1_r11()
        self.assertEqual(r11["manifest"]["metadata"]["revision"], 11)
        r13 = prepare_paper_linear_swap_extension(
            catalog=self.source["catalog"], manifest=r11["manifest"], binding=r11["binding"],
            symbols=list(V226_STAGE_A), revision=13)
        committed = (ROOT / "consumers/stable/trading-system-paper.yaml").read_bytes()
        self.assertEqual(yaml.safe_dump(r13["manifest"], sort_keys=False).encode(), committed)
        self.assertEqual(r13["provenance"]["consumer_manifest_sha256"],
                         ConsumerManifestLoader.load(ROOT / "consumers/stable/trading-system-paper.yaml").manifest_sha256)
        self.assertEqual(r13["consumer_environment"]["DATA_LAYER_V2_JWT_MANIFEST_REVISION"], "13")
        self.assertEqual(r13["manifest"]["spec"]["quotas"]["requests_per_minute"], 10_000)
        self.assertEqual(r13["manifest"]["spec"]["quotas"]["max_streams"], 160)
        with self.assertRaisesRegex(ValueError, "below the modelled demand"):
            prepare_paper_linear_swap_extension(
                catalog=self.source["catalog"], manifest=r11["manifest"], binding=r11["binding"],
                symbols=list(V226_STAGE_A), quotas={"max_streams": 150, "requests_per_minute": 4500}, revision=13)
        with self.assertRaisesRegex(ValueError, "above the base revision"):
            prepare_paper_linear_swap_extension(
                catalog=self.source["catalog"], manifest=r11["manifest"], binding=r11["binding"],
                symbols=list(V226_STAGE_A), revision=11)

    def test_linear_extension_adds_only_requested_feeds_and_fails_closed(self):
        r11 = self._d1_r11()
        base = dict(catalog=self.source["catalog"], manifest=r11["manifest"], binding=r11["binding"])
        result = prepare_paper_linear_swap_extension(**base, symbols=["ARB-USDT-SWAP"])
        added = [p for p in result["binding"]["products"] if p["native_symbol"] == "ARB-USDT-SWAP"]
        self.assertEqual({p["feed"] for p in added}, {"QUOTE", "MARK_INDEX_PRICE"})
        self.assertTrue(all(p["execution_grade"] and p["fallback"] == "BLOCKED" for p in added))
        self.assertEqual(len(result["binding"]["products"]), len(r11["binding"]["products"]) + 2)
        # 66 V2 products need ceil10(66 * 1.5) = 100 streams and more than 3000 rpm: the quotas are raised to the
        # model, never lowered and never left below it.
        self.assertEqual(result["manifest"]["spec"]["quotas"]["max_streams"], 100)
        self.assertGreaterEqual(result["manifest"]["spec"]["quotas"]["requests_per_minute"], 3000)
        ConsumerRouteBinding.from_canonical_mapping(result["binding"])
        with self.assertRaisesRegex(ValueError, "already entitled"):
            prepare_paper_linear_swap_extension(**base, symbols=["BTC-USDT-SWAP"])
        with self.assertRaisesRegex(ValueError, "not an OKX linear USDT swap"):
            prepare_paper_linear_swap_extension(**base, symbols=["BTC-USD-SWAP"])
        with self.assertRaises(Exception):  # the catalog has BAR only for this swap: no QUOTE source
            prepare_paper_linear_swap_extension(**base, symbols=["ZIL-USDT-SWAP"])
        with self.assertRaisesRegex(ValueError, "unique list"):
            prepare_paper_linear_swap_extension(**base, symbols=["ARB-USDT-SWAP", "ARB-USDT-SWAP"])
        with self.assertRaisesRegex(ValueError, "feeds must be a subset"):
            prepare_paper_linear_swap_extension(**base, symbols=["ARB-USDT-SWAP"], feeds=["TRADE"])
