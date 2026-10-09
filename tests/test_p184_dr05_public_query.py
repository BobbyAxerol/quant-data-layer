"""Isolated public HTTP wiring: real JWT/manifest, bounded fake quota only."""
import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from qdl.query import InstrumentQuery, MemoryMarketDataBackend, V2QueryService
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.security.data_plane import InMemoryMinuteQuota
from qdl_sdk.credentials import RotatingJwtCredentialProvider
from scripts import p184_dr05_dl_fault as dr
from scripts.p184_dr05_observer import public_query_app
from tests import test_p184_dr05_dl_fault as fixtures


class PublicQueryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name) / "work"
        dr.prepare(self.work)
        dr.material(self.work, fixtures.MaterialTests.exported())
        catalog = StableSourceCatalog.load(self.work / "catalog.yaml")
        service = V2QueryService(instruments=InstrumentQuery(catalog.instrument_registry()),
            backend=MemoryMarketDataBackend(), entitlements=catalog.entitlements())
        self.quota = InMemoryMinuteQuota()
        with patch("qdl.security.data_plane.RedisMinuteQuota.from_url", return_value=self.quota) as factory:
            self.app = public_query_app(self.work, service, None)
        ns = json.loads((self.work / "packet.json").read_text())["namespace"]
        factory.assert_called_once_with("redis://localhost:6379/0", prefix="qdl:stable:v2:paper:" + ns)
        self.client = TestClient(self.app)
        self.addCleanup(self.client.close)
        self.consumers = {v: json.loads((self.work / ("ts-consumer." + v + ".json")).read_text())
                          for v in ("binance", "okx")}
        self.uids = {b["venue"].lower(): b["instrument_uid"]
                     for b in fixtures.MaterialTests.exported()["core"]["bindings"]}

    def headers(self, venue, **overrides):
        c = self.consumers[venue]
        settings = dict(private_key_file=self.work / (venue + ".key"), key_id=c["jwt_key_id"],
            algorithm="RS256", issuer=c["issuer"], audience=c["audience"], subject=c["subject"],
            environment=c["environment"], consumer_manifest_revision=c["manifest_revision"],
            roles=("market_data_reader", "historical_reader", "stream_consumer"),
            lifetime_seconds=300, refresh_before_seconds=30)
        settings.update(overrides)
        token = asyncio.run(RotatingJwtCredentialProvider(**settings).get_token())
        return {"Authorization": "Bearer " + token, "X-QDL-Consumer-ID": c["consumer_id"],
                "X-QDL-Purpose": "INTERNAL_EXECUTION"}

    def test_public_routes_present_and_metadata_authorized(self):
        paths = self.client.get("/openapi.json").json()["paths"]
        for route in ("/v2/instruments/{identity}",
                      "/v2/market-data/{instrument_uid}/snapshot",
                      "/v2/market-data/{instrument_uid}/warmup",
                      "/v2/feeds/{instrument_uid}/status"):
            self.assertIn(route, paths)
        for venue, uid in self.uids.items():
            response = self.client.get(f"/v2/instruments/{uid}", headers=self.headers(venue))
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["instrument_uid"], uid)

    def test_auth_negatives(self):
        for venue, uid in self.uids.items():
            url = f"/v2/instruments/{uid}"
            self.assertEqual(self.client.get(url).status_code, 401)
            for override in ({"environment": "live"}, {"consumer_manifest_revision": 999}):
                self.assertEqual(self.client.get(url, headers=self.headers(venue, **override)).status_code, 401)
            other = "okx" if venue == "binance" else "binance"
            headers = self.headers(venue) | {"X-QDL-Consumer-ID": self.consumers[other]["consumer_id"]}
            self.assertEqual(self.client.get(url, headers=headers).status_code, 403)

    def test_no_feed_global_grant(self):
        for venue, uid in self.uids.items():
            response = self.client.get(f"/v2/market-data/{uid}/snapshot",
                params={"feed": "QUOTE", "consumer_grade": "EXECUTION", "source_policy_id": "ungranted"},
                headers=self.headers(venue))
            self.assertEqual(response.status_code, 403, response.text)

    def test_quota_fail_closed(self):
        from qdl.security.data_plane import DataPlaneAccessError
        with patch.object(self.quota, "consume", side_effect=DataPlaneAccessError(
                "DEPENDENCY_UNAVAILABLE", "isolated quota unavailable", status_code=503)):
            venue = "binance"
            response = self.client.get(f"/v2/instruments/{self.uids[venue]}", headers=self.headers(venue))
            self.assertEqual(response.status_code, 503, response.text)

    def test_nonisolated_packet_rejected(self):
        path = self.work / "packet.json"
        packet = json.loads(path.read_text())
        packet["provenance"] = "PRODUCTION"
        path.write_text(json.dumps(packet))
        with self.assertRaisesRegex(ValueError, "TEST_ONLY"):
            public_query_app(self.work, None, None)
