"""DL v2.4.0 S2 x DL v2.3.2 P2: the advisory token revision composes with the r8 realm successor (offline).

Inputs are the stage C r8 evidence files (declaration-stage-c-r8: runtime catalogue with 102 ON_CHANGE QUOTE
bindings, consumer realm alpha r17 with its declared floor 15). Checks:
- the v2.3.2 successor (`v232_realm_bundle_successor`) adds only `delivery_semantics` on the 102 QUOTE bindings and
  keeps the manifest bytes; under v2.4.0 that realm already accepts every older token (the rule is in code), so S2
  needs no manifest change or extra realm render;
- the next declaration render of a changed manifest carries BOTH: floor 1 (`next_min_token_revision`) in its bundle
  entry and the catalogue's `delivery_semantics`.
"""
from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

import yaml

from qdl.consumer import ConsumerManifestLoader
from qdl.consumer.manifest_compat import next_min_token_revision
from qdl.consumer.selectors import SelectorContext
from qdl.runtime.stable_catalog import StableSourceCatalog
from scripts import kn_gateway_bundle
from scripts.v232_realm_bundle_successor import successor

ROOT = Path(__file__).resolve().parents[1]
R8 = ROOT / "upgrade/evidence/releases/v2.3.1/declaration-stage-c-r8"
CATALOG = R8 / "runtime/stable-source-bindings.yaml"
UNIVERSES = R8 / "runtime/universes"
REALM = R8 / "consumer-realms/alpha.okx.paper.stable-r17.yaml"


def _context():
    return SelectorContext.from_catalog(StableSourceCatalog.load(CATALOG), universe_dir=UNIVERSES)


class R8RealmCompositionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compiled = kn_gateway_bundle.compile_bundle(
            environment="paper", catalog_path=CATALOG, manifest_paths=[REALM], universe_dir=UNIVERSES)

    def _deployed_r8(self):
        """The deployed r8 realm shape: the same body before v2.3.2 compiled `delivery_semantics`."""
        body = {k: v for k, v in copy.deepcopy(self.compiled).items() if k != "sha256"}
        for entry in body["catalog"]["bindings"]:
            entry.pop("delivery_semantics", None)
        body["sha256"] = kn_gateway_bundle.bundle_sha256(body)
        return body

    def test_the_successor_adds_102_on_change_quotes_and_keeps_the_r17_manifest(self):
        base = self._deployed_r8()
        body, report = successor(base, self.compiled["catalog"])
        self.assertEqual(report["marked_on_change"], 102)
        self.assertTrue(report["manifests_unchanged"])
        entry = next(m for m in body["manifests"] if m["consumer_id"] == "alpha.okx.paper.stable")
        self.assertEqual((entry["manifest_revision"], entry["min_token_revision"]), (17, 15))
        # v2.4.0 S2: the readers ignore the declared floor 15 and accept 1..17 with this same realm.
        manifest = ConsumerManifestLoader.load(REALM, selector_context=_context())
        self.assertEqual(manifest.accepted_token_revisions, range(1, 18))
        self.assertFalse(manifest.accepts_token_revision(18))

    def test_the_next_render_carries_floor_1_and_delivery_semantics_together(self):
        doc = yaml.safe_load(REALM.read_text())
        previous = ConsumerManifestLoader.from_mapping(doc, selector_context=_context())
        successor_doc = copy.deepcopy(doc)
        successor_doc["metadata"]["revision"] = 18
        candidate = ConsumerManifestLoader.from_mapping(successor_doc, selector_context=_context())
        successor_doc["metadata"]["min_token_revision"] = next_min_token_revision(previous, candidate)
        self.assertEqual(successor_doc["metadata"]["min_token_revision"], 1)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "alpha.okx.paper.stable.yaml"
            path.write_text(yaml.safe_dump(successor_doc, sort_keys=False), encoding="utf-8")
            rendered = kn_gateway_bundle.compile_bundle(
                environment="paper", catalog_path=CATALOG, manifest_paths=[path], universe_dir=UNIVERSES)
        entry = next(m for m in rendered["manifests"] if m["consumer_id"] == "alpha.okx.paper.stable")
        self.assertEqual((entry["manifest_revision"], entry["min_token_revision"]), (18, 1))
        on_change = [b for b in rendered["catalog"]["bindings"] if b.get("delivery_semantics") == "ON_CHANGE"]
        self.assertEqual(len(on_change), 102)
        self.assertEqual({b["feed"] for b in on_change}, {"QUOTE"})


if __name__ == "__main__":
    unittest.main()
