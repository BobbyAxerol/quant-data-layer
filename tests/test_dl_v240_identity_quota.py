"""DL v2.4.0 S1/S2/S3/S7/S8: verified-token cache, advisory token revision, quota on admission only,
per-replica token buckets with session fairness, role claims ignored (offline, no runtime)."""

from __future__ import annotations

import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient
from redis.exceptions import RedisError

from qdl.api_v2 import create_v2_app
from qdl.consumer import ConsumerManifestLoader
from qdl.consumer.manifest_compat import (
    ADVISORY_TOKEN_REVISION_FLOOR,
    declared_floor_problems,
    next_min_token_revision,
)
from qdl.security import (
    DataPlaneAccessError,
    DataPlaneIdentityService,
    DataPlanePermission,
    DataPlaneSecurityConfig,
    LocalTokenBucketQuota,
    RedisMinuteQuota,
    VerifiedTokenCache,
)
from qdl.consumer import ConsumerManifestRegistry
from tests.phase7_support import (
    TEST_AUDIENCE,
    TEST_ISSUER,
    TEST_KEY_ID,
    TEST_SECRET,
    make_manifest,
    make_token,
    manifest_mapping,
)
from tests.test_fund_phase7_contract_security import Phase7Fixture

ROOT = Path(__file__).resolve().parents[1]


def _security(keys=None) -> DataPlaneSecurityConfig:
    return DataPlaneSecurityConfig(
        environment="paper", issuer=TEST_ISSUER, audience=TEST_AUDIENCE,
        keys_by_id=keys or {TEST_KEY_ID: TEST_SECRET}, algorithms=("HS256",),
    )


class _CountingVerifier:
    def __init__(self, inner):
        self.inner = inner
        self.calls = 0

    def verify(self, token, *, expected_environment):
        self.calls += 1
        return self.inner.verify(token, expected_environment=expected_environment)

    def has_key(self, key_id):
        return self.inner.has_key(key_id)


class VerifiedTokenCacheTests(unittest.TestCase):
    def setUp(self):
        self.manifest = make_manifest(
            consumer_id="v240.alpha", subject="spiffe://qdl/paper/v240-alpha", instrument_uid="uid-1",
        )
        self.identity = DataPlaneIdentityService(
            _security(), ConsumerManifestRegistry((self.manifest,)),
            quota=LocalTokenBucketQuota(share=1.0),
        )
        self.counting = _CountingVerifier(self.identity._verifier)
        self.identity._verifier = self.counting

    def test_a_valid_token_is_verified_once_then_served_from_the_cache(self):
        token = make_token(self.manifest.subject)
        for _ in range(5):
            access = self.identity.authenticate(token, consumer_id=self.manifest.consumer_id)
        self.assertEqual(self.counting.calls, 1)
        self.assertEqual(access.consumer_id, self.manifest.consumer_id)
        self.assertEqual(self.identity.token_cache.stats()["hits"], 4)

    def test_every_per_request_check_still_runs_on_a_cache_hit(self):
        token = make_token(self.manifest.subject)
        self.identity.authenticate(token, consumer_id=self.manifest.consumer_id)
        with self.assertRaises(DataPlaneAccessError) as mismatch:
            self.identity.authenticate(token, consumer_id="someone.else")
        self.assertEqual(mismatch.exception.code, "CONSUMER_MISMATCH")
        self.assertEqual(self.counting.calls, 1)

    def test_an_expired_entry_is_reverified_and_refused(self):
        clock = [1_000.0]
        cache = VerifiedTokenCache(max_entries=8, clock=lambda: clock[0])
        principal = SimpleNamespace(expires_at=1_010, key_id=TEST_KEY_ID)
        key = VerifiedTokenCache.key("t")
        cache.put(key, principal)
        self.assertIs(cache.get(key, key_is_trusted=lambda _k: True), principal)
        clock[0] = 1_010.0
        self.assertIsNone(cache.get(key, key_is_trusted=lambda _k: True))
        self.assertEqual(cache.stats()["entries"], 0)

    def test_a_rotated_out_key_evicts_and_rotate_keys_clears(self):
        token = make_token(self.manifest.subject)
        self.identity.authenticate(token, consumer_id=self.manifest.consumer_id)
        self.assertEqual(self.identity.token_cache.stats()["entries"], 1)
        key = VerifiedTokenCache.key(token)
        self.assertIsNone(self.identity.token_cache.get(key, key_is_trusted=lambda _k: False))
        self.identity.authenticate(token, consumer_id=self.manifest.consumer_id)
        self.identity.rotate_keys(_security({"other-key": b"other-secret-material-32-bytes-xx"}))
        self.assertEqual(self.identity.token_cache.stats()["entries"], 0)
        with self.assertRaises(DataPlaneAccessError):
            self.identity.authenticate(token, consumer_id=self.manifest.consumer_id)

    def test_the_cache_is_bounded_lru(self):
        cache = VerifiedTokenCache(max_entries=2, clock=lambda: 0.0)
        for name in ("a", "b", "c"):
            cache.put(VerifiedTokenCache.key(name), SimpleNamespace(expires_at=10, key_id="k"))
        self.assertIsNone(cache.get(VerifiedTokenCache.key("a"), key_is_trusted=lambda _k: True))
        self.assertIsNotNone(cache.get(VerifiedTokenCache.key("c"), key_is_trusted=lambda _k: True))
        self.assertEqual(cache.stats()["entries"], 2)

    def test_a_bad_signature_is_never_cached(self):
        forged = make_token(self.manifest.subject, secret=b"not-the-key-not-the-key-not-the-k")
        for _ in range(2):
            with self.assertRaises(DataPlaneAccessError):
                self.identity.authenticate(forged, consumer_id=self.manifest.consumer_id)
        self.assertEqual(self.counting.calls, 2)
        self.assertEqual(self.identity.token_cache.stats()["entries"], 0)


class RoleClaimsIgnoredTests(unittest.TestCase):
    def test_unknown_empty_or_absent_roles_authenticate_and_the_manifest_gates(self):
        manifest = make_manifest(
            consumer_id="v240.reader", subject="spiffe://qdl/paper/v240-reader", instrument_uid="uid-1",
            permissions=("snapshot:read",),
        )
        identity = DataPlaneIdentityService(_security(), ConsumerManifestRegistry((manifest,)))
        for roles in (("root",), (), ("auditor",)):
            access = identity.authenticate(
                make_token(manifest.subject, roles=roles), consumer_id=manifest.consumer_id,
            )
            access.require_permission(DataPlanePermission.SNAPSHOT_READ)
            with self.assertRaises(DataPlaneAccessError) as denied:
                access.require_permission(DataPlanePermission.HISTORY_READ)
            self.assertEqual(denied.exception.code, "PERMISSION_DENIED")

    def test_control_plane_verifier_still_enforces_roles(self):
        from qdl.security import ServiceTokenVerifier

        verifier = ServiceTokenVerifier(
            issuer=TEST_ISSUER, audience=TEST_AUDIENCE, keys_by_id={TEST_KEY_ID: TEST_SECRET},
            algorithms=("HS256",),
        )
        with self.assertRaises(PermissionError):
            verifier.verify(make_token("s", roles=("root",)), expected_environment="paper")


class AdvisoryTokenRevisionTests(unittest.TestCase):
    def test_every_stable_manifest_accepts_the_advisory_range(self):
        paths = sorted((ROOT / "consumers/stable").glob("*.yaml"))
        self.assertEqual(len(paths), 6)
        for path in paths:
            manifest = ConsumerManifestLoader.load(path, selector_context=_selector_context())
            self.assertEqual(manifest.accepted_token_revisions, range(1, manifest.manifest_revision + 1), path.name)
            self.assertTrue(manifest.accepts_token_revision(1))
            self.assertTrue(manifest.accepts_token_revision(manifest.manifest_revision))
            self.assertFalse(manifest.accepts_token_revision(manifest.manifest_revision + 1))

    def test_a_non_additive_bump_keeps_the_floor_and_old_tokens_authenticate(self):
        old = ConsumerManifestLoader.from_mapping(manifest_mapping(
            consumer_id="v240.alpha", subject="spiffe://qdl/paper/v240-alpha", instrument_uid="uid-1",
        ))
        payload = manifest_mapping(
            consumer_id="v240.alpha", subject="spiffe://qdl/paper/v240-alpha", instrument_uid="uid-2",
        )
        payload["metadata"]["revision"] = 5
        payload["metadata"]["min_token_revision"] = 1
        new = ConsumerManifestLoader.from_mapping(payload)
        self.assertEqual(next_min_token_revision(old, new), ADVISORY_TOKEN_REVISION_FLOOR)
        self.assertEqual(declared_floor_problems(old, new), [])
        identity = DataPlaneIdentityService(_security(), ConsumerManifestRegistry((new,)))
        access = identity.authenticate(
            make_token(new.subject, manifest_revision=1), consumer_id=new.consumer_id,
        )
        # The active manifest still decides entitlement: the old row is gone.
        self.assertEqual({item.instrument_uid for item in access.manifest.requirements}, {"uid-2"})
        with self.assertRaises(DataPlaneAccessError):
            identity.authenticate(make_token(new.subject, manifest_revision=6), consumer_id=new.consumer_id)


def _selector_context():
    from qdl.consumer.selectors import SelectorContext
    from qdl.runtime.stable_catalog import StableSourceCatalog

    catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
    return SelectorContext.from_catalog(catalog, universe_dir=ROOT / "config/v2/universes")


class LocalTokenBucketTests(unittest.TestCase):
    def setUp(self):
        self.clock = [0.0]
        # 1,200 rpm at share 0.5 = 10/s; burst 2 s = 20 (minimum 10).
        self.quota = LocalTokenBucketQuota(
            share=0.5, burst_seconds=2.0, session_share=0.25, clock=lambda: self.clock[0],
        )
        mapping = manifest_mapping(
            consumer_id="v240.alpha", subject="spiffe://qdl/paper/v240-alpha", instrument_uid="uid-1",
        )
        mapping["spec"]["quotas"]["requests_per_minute"] = 1200
        self.manifest = ConsumerManifestLoader.from_mapping(mapping)

    def drain(self, session=None, limit=1_000):
        taken = 0
        for _ in range(limit):
            try:
                self.quota.acquire(self.manifest, session=session).settle(charged=True)
            except DataPlaneAccessError:
                break
            taken += 1
        return taken

    def test_identity_bucket_burst_and_refill(self):
        self.assertEqual(self.drain(), 20)
        self.clock[0] += 0.5
        self.assertEqual(self.drain(), 5)

    def test_a_refunded_unit_is_returned(self):
        self.assertEqual(self.drain(), 20)
        lease_quota = LocalTokenBucketQuota(share=0.5, burst_seconds=2.0, clock=lambda: self.clock[0])
        lease = lease_quota.acquire(self.manifest)
        lease.settle(charged=False)
        lease.settle(charged=True)  # settles once
        taken = 0
        while True:
            try:
                lease_quota.acquire(self.manifest).settle(charged=True)
            except DataPlaneAccessError:
                break
            taken += 1
        self.assertEqual(taken, 20)

    def test_a_noisy_session_is_throttled_alone(self):
        # Session share 0.25: 2.5/s, burst max(10, 5) = 10 per session.
        self.assertEqual(self.drain(session="noisy"), 10)
        with self.assertRaises(DataPlaneAccessError) as refused:
            self.quota.acquire(self.manifest, session="noisy")
        self.assertIn("session", refused.exception.detail)
        self.assertEqual(self.drain(session="quiet-1", limit=3), 3)
        self.assertEqual(self.drain(session="quiet-2", limit=3), 3)
        self.assertEqual(self.quota.stats()["refused_identity"], 0)

    def test_the_trading_system_gets_its_full_rate_on_every_replica(self):
        mapping = manifest_mapping(consumer_id="trading-system.paper.stable",
                                   subject="spiffe://qdl/paper/ts", instrument_uid="uid-1")
        mapping["spec"]["quotas"]["requests_per_minute"] = 1200
        ts = ConsumerManifestLoader.from_mapping(mapping)
        taken = 0
        while True:
            try:
                self.quota.acquire(ts).settle(charged=True)
            except DataPlaneAccessError:
                break
            taken += 1
        self.assertEqual(taken, 40)  # 20/s x 2 s: share 1.0, not the alpha 0.5
        self.assertEqual(self.quota.share_for("alpha.okx.paper.stable"), 0.5)
        self.assertEqual(self.quota.share_for("trading-system.sandbox.stable"), 1.0)

    def test_a_shared_redis_outage_never_refuses(self):
        class _Broken:
            def eval(self, *args, **kwargs):
                raise RedisError("down")

            def ping(self):
                raise RedisError("down")

        shared = RedisMinuteQuota(_Broken(), prefix="qdl:stable:v2:test")
        quota = LocalTokenBucketQuota(share=1.0, shared=shared, clock=lambda: self.clock[0])
        for _ in range(5):
            quota.acquire(self.manifest).settle(charged=True)
        self.assertEqual(quota.stats()["shared_failures"], 5)

    def test_a_shared_exhausted_answer_closes_the_identity_for_the_minute(self):
        class _Exhausted:
            def eval(self, *args, **kwargs):
                return [0, 99_999]

        shared = RedisMinuteQuota(_Exhausted(), prefix="qdl:stable:v2:test")
        wall = [120.0]
        quota = LocalTokenBucketQuota(share=1.0, shared=shared, clock=lambda: self.clock[0],
                                      wall_clock=lambda: wall[0])
        quota.acquire(self.manifest).settle(charged=True)
        with self.assertRaises(DataPlaneAccessError):
            quota.acquire(self.manifest)
        wall[0] = 180.0
        quota.acquire(self.manifest).settle(charged=False)

    def test_environment_defaults_are_local_only(self):
        quota = LocalTokenBucketQuota.from_environment({}, redis_url=None, redis_prefix="qdl:stable:v2:x")
        self.assertIsNone(quota.shared)
        self.assertEqual((quota.share, quota.burst_seconds, quota.session_share), (0.5, 10.0, 0.125))
        with self.assertRaises(ValueError):
            LocalTokenBucketQuota.from_environment({"QDL_V2_SHARED_QUOTA": "redis"}, redis_url=None,
                                                   redis_prefix="qdl:stable:v2:x")


class QuotaOnAdmissionRestTests(unittest.TestCase):
    """S3 end to end through the REST router: refusals never keep a unit."""

    def setUp(self):
        self.fixture = Phase7Fixture()
        mapping = manifest_mapping(
            consumer_id=self.fixture.consumer_id, subject=self.fixture.subject,
            instrument_uid=self.fixture.record.instrument_uid,
        )
        mapping["spec"]["quotas"]["requests_per_minute"] = 60  # 1/s, burst 10
        manifest = ConsumerManifestLoader.from_mapping(mapping)
        self.quota = LocalTokenBucketQuota(share=1.0, burst_seconds=1.0, clock=lambda: 0.0)
        identity = DataPlaneIdentityService(_security(), ConsumerManifestRegistry((manifest,)), quota=self.quota)
        self.client = TestClient(
            create_v2_app(self.fixture.service, identity_service=identity),
            raise_server_exceptions=False,
        )
        self.route = f"/v2/market-data/{self.fixture.record.instrument_uid}/snapshot"
        self.params = {"feed": "BAR", "interval": "1m", "source_policy_id": "alpha_binance_v1",
                       "consumer_grade": "ALPHA", "max_freshness_ms": 10_000}

    def tokens(self):
        return self.quota._identities[self.fixture.consumer_id][0]

    def test_refusals_return_their_unit_and_served_reads_keep_it(self):
        headers = self.fixture.headers()
        for _ in range(25):  # requirement outside the manifest: 403, every time
            refused = self.client.get(self.route, params=self.params | {"interval": "5m"}, headers=headers)
            self.assertEqual(refused.status_code, 403, refused.text)
        purpose = self.client.get(self.route, params=self.params,
                                  headers=headers | {"X-QDL-Purpose": "INTERNAL_EXECUTION"})
        self.assertEqual(purpose.status_code, 403)
        self.assertEqual(self.tokens(), 10.0)
        served = [self.client.get(self.route, params=self.params, headers=headers).status_code for _ in range(12)]
        self.assertEqual(served[:10], [200] * 10)
        self.assertEqual(served[10:], [429, 429])
        self.assertEqual(self.tokens(), 0.0)

    def test_a_session_header_is_optional_and_only_narrows(self):
        headers = self.fixture.headers() | {"X-QDL-Session": "s-1"}
        response = self.client.get(self.route, params=self.params, headers=headers)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn(f"{self.fixture.consumer_id}#s-1", self.quota._sessions)


class SdkSessionHeaderTests(unittest.TestCase):
    """SDK 2.8.0: ``session_id`` adds X-QDL-Session; the default request is unchanged."""

    def _headers(self, **kwargs):
        import asyncio

        import httpx

        from qdl_sdk import DataRequirement as SdkRequirement, Feed, Grade
        from qdl_sdk.credentials import StaticBearerCredential
        from qdl_sdk.transport import RestQueryTransport

        seen = []

        def handler(request):
            seen.append(dict(request.headers))
            return httpx.Response(503, json={"code": "DEPENDENCY_UNAVAILABLE", "detail": "test", "retryable": True})

        async def call():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://q")
            transport = RestQueryTransport("http://q", client=client,
                                           credential_provider=StaticBearerCredential("t" * 40), **kwargs)
            requirement = SdkRequirement(instrument_uid="u", feed=Feed.TRADE, consumer_grade=Grade.ALPHA,
                                         source_policy_id="p")
            try:
                await transport.snapshot(requirement, consumer_id="c")
            except Exception:  # noqa: BLE001 - only the request headers matter
                pass
            await client.aclose()

        asyncio.run(call())
        return seen[0]

    def test_warmup_timeout_outlives_the_server_queue_wait(self):
        from qdl_sdk import DataRequirement as SdkRequirement, Feed, Grade
        from qdl_sdk.models import WarmupSpecification
        from qdl_sdk.transport import RestQueryTransport

        transport = RestQueryTransport("http://q", timeout_seconds=10.0)
        warm = SdkRequirement(instrument_uid="u", feed=Feed.BAR, consumer_grade=Grade.ALPHA, source_policy_id="p",
                              interval="1m", warmup=WarmupSpecification(rows=2500, deadline_ms=20_000))
        self.assertEqual(transport._cold_timeout_seconds((warm,)), 22.0)
        short = SdkRequirement(instrument_uid="u", feed=Feed.BAR, consumer_grade=Grade.ALPHA, source_policy_id="p",
                               interval="1m", warmup=WarmupSpecification(rows=2500, deadline_ms=5_000))
        self.assertIsNone(transport._cold_timeout_seconds((short,)))  # 10 s transport timeout already longer
        plain = SdkRequirement(instrument_uid="u", feed=Feed.TRADE, consumer_grade=Grade.ALPHA, source_policy_id="p")
        self.assertIsNone(transport._cold_timeout_seconds((plain,)))

    def test_default_sends_no_session_header_and_session_id_sends_it(self):
        self.assertNotIn("x-qdl-session", self._headers())
        self.assertEqual(self._headers(session_id="alpha-7")["x-qdl-session"], "alpha-7")
        from qdl_sdk.transport import RestQueryTransport

        with self.assertRaises(ValueError):
            RestQueryTransport("http://q", session_id="")


if __name__ == "__main__":
    unittest.main()
