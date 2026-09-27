"""Real compiled alpha identity/manifest, test-only JWT/provider, no network."""
from copy import deepcopy
from pathlib import Path
import unittest
import yaml
from fastapi.testclient import TestClient
from qdl.api_v2 import create_v2_app
from qdl.consumer import ConsumerManifestLoader
from qdl.domain.instrument import InstrumentRegistry
from qdl.query import InstrumentQuery, MemoryMarketDataBackend, V2QueryService
from qdl.reference.batch import ReferenceBatch
from qdl.runtime.stable_catalog import StableSourceCatalog
from tests.phase7_support import make_identity, make_token
from tests.test_phase113_reference_v2 import FixtureReferenceAdapter, NOW_NS, grants
from scripts.phase24315_materialize_alpha_reference_entitlements import extend_reference_from_demand
from qdl.demand.resolver import DemandManifest

ROOT = Path(__file__).resolve().parents[1]

class AlphaReferenceAccessTests(unittest.TestCase):
    def setUp(self):
        self.catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
        payload = yaml.safe_load((ROOT / "consumers/stable/alpha-okx-paper.yaml").read_text())
        self.manifest = ConsumerManifestLoader.from_mapping(payload)
        self.subject = payload["metadata"]["subject"]
        registry = InstrumentRegistry()
        for instrument in self.catalog.instruments:
            registry.register(instrument, [])
        self.adapter = FixtureReferenceAdapter()
        service = V2QueryService(instruments=InstrumentQuery(registry), backend=MemoryMarketDataBackend(),
            entitlements=grants(), reference_batch=ReferenceBatch({("OKX", "SWAP"): self.adapter}, clock_ns=lambda: NOW_NS),
            reference_source_id=lambda i: f"{i.identity.venue}_DIRECT", clock_ns=lambda: NOW_NS)
        self.client = TestClient(create_v2_app(service, identity_service=make_identity(self.manifest)))
        self.addCleanup(self.client.close)
        self.headers = {"Authorization": "Bearer " + make_token(self.subject, manifest_revision=self.manifest.manifest_revision),
                        "X-QDL-Consumer-ID": self.manifest.consumer_id, "X-QDL-Purpose": "INTERNAL_ALPHA"}
        self.rows = [{"instrument_uid": r.instrument_uid, "product": r.feed.value,
                      "interval": r.interval, "consumer_grade": "ALPHA", "source_policy_id": r.source_policy_id,
                      "start_time_ns": NOW_NS - 86400_000_000_000, "end_time_ns": NOW_NS, "limit": 1}
                     for r in self.manifest.requirements if r.interval == "1d" and r.feed.value in
                     {"OPEN_INTEREST", "LONG_SHORT_RATIO", "TAKER_FLOW"}]
        for row in self.rows:
            if row["product"] == "LONG_SHORT_RATIO":
                row["long_short_kind"] = "GLOBAL_ACCOUNT"

    def post(self, rows, headers=None):
        return self.client.post("/v2/market-data/reference:batch", headers=headers or self.headers,
                                json={"consumer_id": self.manifest.consumer_id, "require_all": True, "requirements": rows})

    def test_fifteen_compiled_entitlements_authenticated_on_public_api(self):
        self.assertEqual(len(self.rows), 15)
        self.assertEqual(len({r["instrument_uid"] for r in self.rows}), 5)
        response = self.post(self.rows)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(response.json()["results"]), 15)
        self.assertEqual(self.adapter.calls, 15)

    def test_negative_scope_interval_grade_and_revision_never_call_provider(self):
        denied = []
        for field, value in (("interval", "1h"), ("source_policy_id", "unapproved"), ("consumer_grade", "EXECUTION")):
            row = dict(self.rows[0]); row[field] = value; denied.append(row)
        row = dict(self.rows[0]); row["instrument_uid"] = next(i.instrument_uid for i in self.catalog.instruments if i.identity.venue == "BINANCE")
        denied.append(row)
        for row in denied:
            with self.subTest(row=row):
                self.assertIn(self.post([row]).status_code, (400, 403, 422))
        headers = dict(self.headers)
        headers["Authorization"] = "Bearer " + make_token(self.subject, manifest_revision=self.manifest.manifest_revision - 1)
        self.assertIn(self.post(self.rows, headers).status_code, (401, 403))
        self.assertEqual(self.adapter.calls, 0)

    def test_catalog_demand_extension_is_idempotent_and_preserves_other_products(self):
        reference = yaml.safe_load((ROOT / "consumers/stable/reference-l2-stable.yaml").read_text())
        demand = DemandManifest.load_many((ROOT / "config/v2/stable-reference-l2-demand.yaml",))
        self.assertEqual(extend_reference_from_demand(reference, demand, self.catalog), reference)
        changed = deepcopy(reference)
        row = next(r for r in changed["spec"]["requirements"] if r["feed"] == "LONG_SHORT_RATIO")
        row["max_freshness_ms"] = 1
        with self.assertRaisesRegex(ValueError, "conflicts"):
            extend_reference_from_demand(changed, demand, self.catalog)

if __name__ == "__main__":
    unittest.main()
