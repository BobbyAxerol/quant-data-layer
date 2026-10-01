from copy import deepcopy
from pathlib import Path
import unittest

import yaml

from qdl.runtime.production_catalog import ProductionDemandManifest
from qdl.runtime.sandbox_extension import _digest, prepare_sandbox_extension
from scripts.compile_consumer_realms import compile_realm
from scripts.phase115_render_consumer_route_binding import binding_from_stable_release

ROOT = Path(__file__).resolve().parents[1]


class SandboxExtensionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        manifest = compile_realm(yaml.safe_load(
            (ROOT / "consumers/stable/trading-system-paper.yaml").read_text()), "sandbox")
        binding = binding_from_stable_release(
            ROOT / "config/v2/stable-v2-release-routing.yaml", ROOT,
            "trading-system.paper.stable").canonical_mapping()
        binding["consumer_id"] = manifest["metadata"]["id"]
        binding["consumer_manifest_revision"] = 1
        for row in binding["products"]:
            row["consumer_id"] = binding["consumer_id"]
        binding.pop("binding_sha256")
        binding["binding_sha256"] = _digest(binding)
        cls.inputs = dict(
            catalog=yaml.safe_load((ROOT / "config/v2/stable-source-bindings.yaml").read_text()),
            acquisition=yaml.safe_load((ROOT / "config/v2/stable-acquisition-bindings.yaml").read_text()),
            manifest=manifest, binding=binding,
            demand=ProductionDemandManifest.load_many([ROOT / "config/v2/okx-inverse-sandbox-demand.yaml"]),
            template_uid="fb26214c-7b9b-5961-95b2-55154755af0f",
            okx_rows=[dict(instId="BTC-USD-SWAP", instType="SWAP", instFamily="BTC-USD",
                           ctType="inverse", ctValCcy="USD", settleCcy="BTC", ctVal="100",
                           ctMult="1", lotSz="0.1", tickSz="0.1", state="live")],
        )

    def test_preserves_old_scope_and_seals_new_generation(self):
        source = deepcopy(self.inputs)
        out = prepare_sandbox_extension(**source)
        self.assertEqual(source, self.inputs)
        self.assertEqual(len(out["binding"]["products"]), 64)
        self.assertEqual(out["manifest"]["metadata"]["revision"], 2)
        self.assertEqual(out["manifest"]["spec"]["quotas"], source["manifest"]["spec"]["quotas"])
        for product in source["binding"]["products"]:
            self.assertIn(product, out["binding"]["products"])
        for row in source["catalog"]["bindings"]:
            self.assertIn(row, out["catalog"]["bindings"])
        self.assertNotEqual(out["binding"]["universal_manifest_sha256"], source["binding"]["universal_manifest_sha256"])
        self.assertEqual(out["provenance"]["status"], "PREPARED_NOT_ACTIVATED_NOT_CERTIFIED")

    def test_rejects_paper_and_live_scope(self):
        for realm in ("paper", "live"):
            with self.subTest(realm=realm):
                source = deepcopy(self.inputs)
                source["manifest"]["metadata"]["environment"] = realm
                with self.assertRaises(ValueError):
                    prepare_sandbox_extension(**source)

    def test_rejects_wrong_collateral_or_non_active_metadata(self):
        for field, value in (("settleCcy", "USDT"), ("state", "suspend"), ("ctType", "linear")):
            with self.subTest(field=field):
                source = deepcopy(self.inputs)
                source["okx_rows"][0][field] = value
                with self.assertRaises(ValueError):
                    prepare_sandbox_extension(**source)

    def test_rejects_missing_template_and_quality_drift(self):
        for change in ("missing", "freshness", "fallback", "identity"):
            with self.subTest(change=change):
                source = deepcopy(self.inputs)
                if change == "missing":
                    source["template_uid"] = "a953e16e-7138-5562-b5e8-c337a44d0b65"
                else:
                    row = next(r for r in source["binding"]["products"]
                               if r["instrument_uid"] == source["template_uid"] and r["feed"] == "QUOTE")
                    if change == "freshness":
                        row["max_freshness_ms"] += 1
                    elif change == "fallback":
                        row["fallback"] = "V1"
                    else:
                        row["native_symbol"] = "BTC-USD-SWAP"
                    source["binding"].pop("binding_sha256")
                    source["binding"]["binding_sha256"] = _digest(source["binding"])
                with self.assertRaises(ValueError):
                    prepare_sandbox_extension(**source)

    def test_rejects_repeat_application(self):
        out = prepare_sandbox_extension(**deepcopy(self.inputs))
        source = {**self.inputs, **{k: out[k] for k in ("catalog", "acquisition", "manifest", "binding")}}
        with self.assertRaisesRegex(ValueError, "already exists"):
            prepare_sandbox_extension(**source)

    def test_rejects_subject_and_manifest_revision_mismatch(self):
        for field, value in (("subject", "spiffe://qdl/sandbox/other"), ("revision", 2)):
            with self.subTest(field=field):
                source = deepcopy(self.inputs)
                source["manifest"]["metadata"][field] = value
                with self.assertRaises(ValueError):
                    prepare_sandbox_extension(**source)
