"""B4 (DL v2.3.1): ``POST /v2/market-data/snapshot:batch`` keeps every single-snapshot rule per item.

The batch is a transport change only: one HTTP call (one quota unit) carries
many latest reads. Each item must equal the single ``GET .../snapshot`` read of
the same requirement (same view, same typed refusal), authorization is the
single route's for every item, and the call is bounded by the manifest's
``max_batch_items``. Uses the Phase 5 in-memory API harness (no Redis).
"""
from __future__ import annotations

import asyncio
import unittest
from dataclasses import replace
from unittest.mock import patch

from qdl.api_v2.models import SNAPSHOT_BATCH_SCHEMA, SnapshotBatchResponse
from qdl.query import AccessPurpose, CanonicalErrorCode, QueryProblem, QueryServiceError
import tests.test_fund_phase5_api as phase5


ROUTE = "/v2/market-data/snapshot:batch"


class _CountingQuota:
    def __init__(self, inner):
        self.inner = inner
        self.calls = 0

    def consume(self, manifest):
        self.calls += 1
        self.inner.consume(manifest)


class SnapshotBatchRouteTests(unittest.TestCase):
    # Reuse the Phase 5 fixture (registry, memory backend, manifest, client) without its tests.
    # Imported as a module so the loader does not collect (and rerun) the Phase 5 tests here.
    setUp = phase5.Phase5ApiTests.setUp
    headers = phase5.Phase5ApiTests.headers
    params = phase5.Phase5ApiTests.params

    def item(self, **overrides):
        values = {
            "instrument_uid": self.binance.instrument_uid,
            "feed": "BAR",
            "interval": "1m",
            "consumer_grade": "ALPHA",
            "source_policy_id": "alpha_crypto_primary_v1",
            "max_freshness_ms": 10000,
        }
        values.update(overrides)
        return values

    def batch(self, *items, consumer_id=None):
        return self.client.post(ROUTE, json={
            "consumer_id": consumer_id or self.consumer_id,
            "requirements": list(items),
        })

    def test_each_item_equals_the_single_snapshot_read(self):
        single = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/snapshot", params=self.params()
        )
        self.assertEqual(single.status_code, 200, single.text)
        response = self.batch(self.item(), self.item())
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["schema"], SNAPSHOT_BATCH_SCHEMA)
        SnapshotBatchResponse.model_validate(body)
        self.assertEqual((body["partial"], body["success_count"], body["error_count"]), (False, 2, 0))
        for result in body["results"]:
            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["instrument_uid"], self.binance.instrument_uid)
            self.assertEqual(result["feed"], "BAR")
            self.assertIsNone(result.get("problem"))
            # Same view bytes as the single read (quality, source, cursor, watermark, payload).
            self.assertEqual(result["data"], single.json()["data"])

    def test_a_batch_item_is_the_single_routes_requirement(self):
        # Same DataRequirement (so the same requirement digest the cursor binds) for an EXECUTION QUOTE probe.
        from qdl.api_v2.models import RequirementModel
        from qdl.query import (BarRevisionPolicy, ConsumerGrade, FeedType, GapPolicy, RecoveryPolicy,
                               StalePolicy)
        from qdl.replay.cursor_v3 import requirement_digest

        router = phase5.router_module
        model = RequirementModel.model_validate({
            "instrument_uid": self.okx.instrument_uid, "feed": "QUOTE", "consumer_grade": "EXECUTION",
            "source_policy_id": "crypto_primary_v2", "require_full_coverage": True, "require_final_bars": True,
            "stale_policy": "BLOCK", "gap_policy": "BLOCK", "recovery": "SNAPSHOT_AND_REPLAY",
            "bar_revision_policy": "EMIT_REVISIONS", "max_freshness_ms": 2000,
            "max_session_liveness_ms": 2000, "event_recency_policy": "OBSERVE",
        })
        single = router._query_requirement(
            self.okx.instrument_uid, FeedType.QUOTE, ConsumerGrade.EXECUTION, "crypto_primary_v2", None, 0,
            2000, StalePolicy.OBSERVE, 2000, True, True, StalePolicy.BLOCK, GapPolicy.BLOCK,
            RecoveryPolicy.SNAPSHOT_AND_REPLAY, BarRevisionPolicy.EMIT_REVISIONS,
        )
        batch = router._requirement(model)
        self.assertEqual(batch, single)
        self.assertEqual(requirement_digest(batch), requirement_digest(single))

    def test_a_data_refusal_is_that_items_typed_problem_like_the_single_read(self):
        # The OKX requirement is authorized by the manifest but has no latest value: DATA_NOT_READY.
        single = self.client.get(
            f"/v2/market-data/{self.okx.instrument_uid}/snapshot", params=self.params()
        )
        self.assertGreaterEqual(single.status_code, 400)
        response = self.batch(self.item(), self.item(instrument_uid=self.okx.instrument_uid))
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual((body["partial"], body["success_count"], body["error_count"]), (True, 1, 1))
        ok, refused = body["results"]
        self.assertEqual(ok["status"], "OK")
        problem = refused["problem"]
        self.assertIsNone(refused.get("data"))
        self.assertEqual(refused["status"], problem["code"])
        self.assertEqual(problem["code"], single.json()["code"])
        self.assertEqual(problem["status"], single.status_code)
        self.assertEqual(problem["retryable"], single.json()["retryable"])
        self.assertEqual(problem["detail"], single.json()["detail"])
        self.assertEqual(problem["instrument_uid"], self.okx.instrument_uid)

    def test_a_stale_item_keeps_its_data_stale_refusal(self):
        original = self.service.snapshot

        def stale_for_okx(requirement, **kwargs):
            if requirement.instrument_uid == self.okx.instrument_uid:
                raise QueryServiceError(
                    QueryProblem(CanonicalErrorCode.DATA_STALE, "provider session liveness expired", False),
                    request_id=kwargs["request_id"], instrument_uid=requirement.instrument_uid,
                    quality_state="STALE",
                )
            return original(requirement, **kwargs)

        with patch.object(self.service, "snapshot", side_effect=stale_for_okx):
            response = self.batch(self.item(instrument_uid=self.okx.instrument_uid), self.item())
        self.assertEqual(response.status_code, 200, response.text)
        refused, ok = response.json()["results"]
        self.assertEqual(ok["status"], "OK")
        self.assertEqual(refused["status"], "DATA_STALE")
        self.assertEqual(refused["problem"]["status"], 503)
        self.assertEqual(refused["problem"]["quality_state"], "STALE")

    def test_one_call_is_one_quota_unit_whatever_its_item_count(self):
        counting = _CountingQuota(self.identity.quota)
        self.identity.quota = counting
        response = self.batch(*[self.item() for _ in range(20)])
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(response.json()["results"]), 20)
        self.assertEqual(counting.calls, 1)

    def test_manifest_max_batch_items_bounds_the_call(self):
        response = self.batch(*[self.item() for _ in range(51)])  # manifest max_batch_items = 50
        self.assertEqual(response.status_code, 429, response.text)
        self.assertEqual(response.json()["code"], "QUOTA_EXCEEDED")

    def test_an_unauthorized_item_refuses_the_whole_call_before_any_read(self):
        reads = []
        original = self.service.snapshot

        def recording(requirement, **kwargs):
            reads.append(requirement.instrument_uid)
            return original(requirement, **kwargs)

        with patch.object(self.service, "snapshot", side_effect=recording):
            response = self.batch(self.item(), self.item(source_policy_id="not_in_manifest"))
        self.assertEqual(response.status_code, 403, response.text)
        self.assertEqual(reads, [])

    def test_snapshot_permission_purpose_and_consumer_are_required(self):
        self.assertEqual(self.batch(self.item(), consumer_id="someone-else").status_code, 403)
        unauthenticated = self.client.post(ROUTE, headers={"Authorization": ""}, json={
            "consumer_id": self.consumer_id, "requirements": [self.item()]})
        self.assertEqual(unauthenticated.status_code, 401)
        bad_purpose = self.client.post(ROUTE, headers=self.headers("INTERNAL_RESEARCH"), json={
            "consumer_id": self.consumer_id, "requirements": [self.item()]})
        self.assertIn(bad_purpose.status_code, (400, 403))

        from qdl.consumer import ConsumerManifestLoader
        from tests.phase7_support import make_identity, manifest_mapping
        from fastapi.testclient import TestClient
        from qdl.api_v2 import create_v2_app

        payload = manifest_mapping(
            consumer_id=self.consumer_id, subject=self.subject,
            instrument_uid=self.binance.instrument_uid, source_policy_id="alpha_crypto_primary_v1",
            permissions=("history:read", "status:read"),
        )
        client = TestClient(create_v2_app(
            self.service, identity_service=make_identity(ConsumerManifestLoader.from_mapping(payload))),
            raise_server_exceptions=False)
        client.headers.update(self.headers())
        no_snapshot = client.post(ROUTE, json={"consumer_id": self.consumer_id, "requirements": [self.item()]})
        self.assertEqual(no_snapshot.status_code, 403, no_snapshot.text)

    def test_warmup_items_and_empty_or_unknown_fields_are_refused(self):
        self.assertEqual(self.batch(self.item(warmup_limit=2)).status_code, 400)
        self.assertEqual(self.batch().status_code, 400)
        extra = self.client.post(ROUTE, json={
            "consumer_id": self.consumer_id, "requirements": [self.item()], "require_all": True})
        self.assertEqual(extra.status_code, 400)

    def test_the_service_runs_all_items_under_one_admission_permit(self):
        lane = self.service._hot_snapshot_admission_for()
        before = lane.stats()["admitted"]
        requirement = replace(self.requirement, warmup_limit=0)
        result = asyncio.run(self.service.snapshot_batch_async(
            (requirement, requirement, requirement),
            purpose=AccessPurpose.INTERNAL_ALPHA, consumer_id=self.consumer_id))
        self.assertEqual(lane.stats()["admitted"] - before, 1)
        self.assertEqual(len(result.results), 3)
        self.assertTrue(all(item.result is not None and item.error is None for item in result.results))
        self.assertEqual({item.result.request_id for item in result.results}, {result.request_id})

    def test_a_lane_refusal_is_one_typed_rate_limited_envelope(self):
        from qdl.query.lanes import ReadLaneAtCapacity

        lane = self.service._hot_snapshot_admission_for()

        async def refuse(*_args, **_kwargs):
            raise ReadLaneAtCapacity("read lane is at byte reservation capacity")

        with patch.object(lane, "run", side_effect=refuse):
            response = self.batch(self.item())
        self.assertEqual(response.status_code, 429, response.text)
        self.assertEqual(response.json()["code"], "RATE_LIMITED")


if __name__ == "__main__":
    unittest.main()
