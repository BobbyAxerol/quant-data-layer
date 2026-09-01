from __future__ import annotations

import unittest

import httpx

from qdl_sdk import (
    AsyncDataLayerClient,
    DataRequirement,
    Feed,
    Grade,
    RestQueryTransport,
    StaticBearerCredential,
    StalePolicy,
)
from qdl_sdk.errors import ContinuityError


class _UnusedStreamTransport:
    async def close(self) -> None:
        return None


class _StatusQueryTransport:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls: list[tuple[DataRequirement, str]] = []

    async def feed_status(self, requirement: DataRequirement, *, consumer_id: str) -> dict:
        self.calls.append((requirement, consumer_id))
        return self.payload


class QdlSdkFeedStatusTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _requirement() -> DataRequirement:
        return DataRequirement(
            instrument_uid="a953e16e-7138-5562-b5e8-c337a44d0b65",
            feed=Feed.QUOTE,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=2_000,
            max_session_liveness_ms=45_000,
            stale_policy=StalePolicy.BLOCK,
        )

    @staticmethod
    def _payload(requirement: DataRequirement) -> dict:
        return {
            "schema": "qdl.feed-status.v2",
            "instrument_uid": requirement.instrument_uid,
            "feed": requirement.feed.value,
            "quality": {
                "state": "STALE",
                "freshness_ms": 2_001,
                "event_recency_state": "STALE",
                "provider_session_state": "LIVE",
                "provider_session_liveness_ms": 10,
                "gap_open": False,
                "complete": True,
                "execution_eligible": False,
                "policy_id": requirement.source_policy_id,
                "flags": [],
            },
        }

    async def test_status_exposes_quiet_session_quality_without_returning_a_snapshot(self):
        requirement = self._requirement()
        transport = _StatusQueryTransport(self._payload(requirement))
        client = AsyncDataLayerClient(
            query_transport=transport,
            stream_transport=_UnusedStreamTransport(),
            consumer_id="trading-system.paper.stable",
        )

        result = await client.feed_status(requirement)

        self.assertEqual(result.feed, Feed.QUOTE)
        self.assertEqual(result.quality.event_recency_state, "STALE")
        self.assertEqual(result.quality.provider_session_state, "LIVE")
        self.assertFalse(result.quality.execution_eligible)
        self.assertEqual(transport.calls, [(requirement, "trading-system.paper.stable")])

    async def test_status_rejects_wrong_governed_identity(self):
        requirement = self._requirement()
        payload = self._payload(requirement)
        payload["feed"] = Feed.TRADE.value
        client = AsyncDataLayerClient(
            query_transport=_StatusQueryTransport(payload),
            stream_transport=_UnusedStreamTransport(),
            consumer_id="trading-system.paper.stable",
        )

        with self.assertRaisesRegex(ContinuityError, "feed does not match"):
            await client.feed_status(requirement)

    async def test_rest_transport_uses_status_route_and_excludes_warmup_only_params(self):
        requirement = self._requirement()
        observed: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            observed["path"] = request.url.path
            observed["params"] = dict(request.url.params)
            observed["authorization"] = request.headers.get("Authorization")
            return httpx.Response(
                200,
                request=request,
                json=self._payload(requirement),
            )

        async with httpx.AsyncClient(
            base_url="http://qdl.test",
            transport=httpx.MockTransport(handler),
        ) as raw_client:
            transport = RestQueryTransport(
                "http://qdl.test",
                client=raw_client,
                credential_provider=StaticBearerCredential("test-token"),
            )
            result = await transport.feed_status(
                requirement,
                consumer_id="trading-system.paper.stable",
            )

        self.assertEqual(result["schema"], "qdl.feed-status.v2")
        self.assertEqual(
            observed["path"],
            f"/v2/feeds/{requirement.instrument_uid}/status",
        )
        self.assertEqual(observed["authorization"], "Bearer test-token")
        self.assertNotIn("limit", observed["params"])
        self.assertEqual(observed["params"]["feed"], "QUOTE")


if __name__ == "__main__":
    unittest.main()
