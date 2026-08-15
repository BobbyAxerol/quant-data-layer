from __future__ import annotations

import asyncio
import tempfile
import threading
import time
import unittest
from pathlib import Path

import httpx
import yaml
from fastapi import FastAPI, Request
from redis.exceptions import ConnectionError as RedisConnectionError

from qdl.consumer import ConsumerManifestLoader
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.runtime.beta import BetaRuntimeConfig, build_beta_handoff, install_beta_health
from qdl.runtime.bounds import BoundedRequestMiddleware, RequestBounds
from qdl.runtime.lease import (
    ActivePassiveGatewayLease,
    GatewayFenced,
    InMemoryAsyncGatewayLeaseStore,
)
from qdl.runtime.readiness import (
    CallableReadinessProbe,
    ComponentReadiness,
    ComponentState,
    MeasuredRuntimeReadiness,
)
from qdl.security import DataPlaneAccessError, RedisMinuteQuota
from qdl.stream import DurableStreamGateway
from qdl.transport import CursorExpired, DurableEvent, SQLiteDurableSpool, SpoolConfig
from tests.phase7_support import manifest_mapping


STREAM = "md.canonical.v2.trade"
PARTITION_A = "instrument-a/trade/source"
PARTITION_B = "instrument-b/trade/source"


def event(sequence: int, partition: str = PARTITION_A) -> DurableEvent:
    return DurableEvent(
        stream=STREAM,
        partition_key=partition,
        event_id=sequence.to_bytes(16, "big"),
        payload=f"event-{sequence}".encode(),
        accepted_at_ns=max(1, sequence),
    )


def ready(name: str, state: ComponentState = ComponentState.READY):
    return ComponentReadiness(
        name,
        state,
        checked_at_ns=time.time_ns(),
    )


class Phase71ReadinessAndBoundsTests(unittest.IsolatedAsyncioTestCase):
    async def test_readiness_is_measured_fail_closed_and_recovers(self):
        state = [ComponentState.NOT_READY]
        readiness = MeasuredRuntimeReadiness(
            role="query_v2",
            authority="V1",
            config_revision="phase71",
            probes=(
                CallableReadinessProbe("query_store", lambda: ready("query_store", state[0])),
                CallableReadinessProbe("cursor_signer", lambda: ready("cursor_signer")),
            ),
        )
        first = await readiness.snapshot()
        self.assertFalse(first.ready)
        self.assertEqual(first.status, "NOT_READY")
        state[0] = ComponentState.READY
        recovered = await readiness.snapshot()
        self.assertTrue(recovered.ready)
        self.assertEqual(recovered.status, "READY")

        state[0] = ComponentState.STANDBY
        standby = await readiness.snapshot()
        self.assertFalse(standby.ready)
        self.assertEqual(standby.status, "STANDBY")

    async def test_health_endpoint_reflects_dependency_state(self):
        state = [ComponentState.STANDBY]
        readiness = MeasuredRuntimeReadiness(
            role="stream_v2",
            authority="V1",
            config_revision="phase71",
            probes=(CallableReadinessProbe(
                "gateway_lease", lambda: ready("gateway_lease", state[0])
            ),),
        )
        app = FastAPI()
        install_beta_health(app, readiness, {"role": "stream_v2"})
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://beta"
        ) as client:
            self.assertEqual((await client.get("/health/live")).status_code, 200)
            response = await client.get("/health/ready")
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json()["status"], "STANDBY")
            state[0] = ComponentState.READY
            response = await client.get("/health/ready")
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["ready"])

    async def test_request_bounds_cover_bytes_encoding_deadline_and_admission(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        app = FastAPI()
        app.add_middleware(
            BoundedRequestMiddleware,
            bounds=RequestBounds(
                max_request_bytes=8,
                max_query_string_bytes=8,
                request_deadline_seconds=0.25,
                admission_timeout_seconds=0.01,
                max_concurrent_requests=1,
            ),
        )

        @app.post("/body")
        async def body(request: Request):
            return {"size": len(await request.body())}

        @app.get("/hold")
        async def hold():
            entered.set()
            await release.wait()
            return {"ok": True}

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://beta"
        ) as client:
            self.assertEqual((await client.post("/body", content=b"123456789")).status_code, 413)
            self.assertEqual((await client.post(
                "/body", content=b"1", headers={"content-encoding": "gzip"}
            )).status_code, 415)
            self.assertEqual((await client.get("/body?123456789")).status_code, 414)

            first = asyncio.create_task(client.get("/hold"))
            await entered.wait()
            self.assertEqual((await client.get("/hold")).status_code, 429)
            release.set()
            self.assertEqual((await first).status_code, 200)

        timeout_app = FastAPI()
        timeout_app.add_middleware(
            BoundedRequestMiddleware,
            bounds=RequestBounds(request_deadline_seconds=0.01),
        )

        @timeout_app.get("/slow")
        async def slow():
            await asyncio.sleep(0.05)
            return {"ok": True}

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=timeout_app), base_url="http://beta"
        ) as client:
            self.assertEqual((await client.get("/slow")).status_code, 504)


class Phase71LeaseAndGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(self.temp.name) / "beta.sqlite3",
            min_free_disk_bytes=0,
        ))
        self.handoff = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec({"beta": b"b" * 32}, active_key_id="beta"),
        )

    async def asyncTearDown(self):
        self.spool.close()
        self.temp.cleanup()

    def token(self, consumer: str = "paper-alpha") -> str:
        watermark = self.handoff.capture_watermark(
            stream=STREAM, partition_key=PARTITION_A
        )
        return self.handoff.issue(
            consumer_id=consumer,
            snapshot_id="snapshot-1",
            snapshot_watermark=watermark,
            ttl_seconds=3600,
        ).token

    async def test_active_passive_uses_monotonic_fencing_epoch(self):
        now = [1_000_000_000]
        store = InMemoryAsyncGatewayLeaseStore(clock_ns=lambda: now[0])
        active = ActivePassiveGatewayLease(
            store, shard_id="public", owner_id="a", clock_ns=lambda: now[0]
        )
        passive = ActivePassiveGatewayLease(
            store, shard_id="public", owner_id="b", clock_ns=lambda: now[0]
        )
        self.assertTrue(await active.acquire_once())
        first_epoch = active.assert_active()
        self.assertFalse(await passive.acquire_once())
        with self.assertRaises(GatewayFenced):
            passive.assert_active()

        now[0] += 16_000_000_000
        self.assertTrue(await passive.acquire_once())
        self.assertGreater(passive.assert_active(), first_epoch)
        with self.assertRaises(GatewayFenced):
            active.assert_active(first_epoch)

    async def test_replay_registration_barrier_has_no_live_handoff_gap(self):
        replay_started = threading.Event()
        release_replay = threading.Event()
        original = self.handoff.replay

        def blocking_replay(**kwargs):
            replay_started.set()
            release_replay.wait(timeout=2)
            return original(**kwargs)

        self.handoff.replay = blocking_replay
        gateway = DurableStreamGateway(handoff=self.handoff, sink=self.spool)
        token = self.token()
        opening = asyncio.create_task(gateway.open(
            consumer_id="paper-alpha",
            stream=STREAM,
            partition_key=PARTITION_A,
            token=token,
        ))
        self.assertTrue(await asyncio.to_thread(replay_started.wait, 1))
        publishing = asyncio.create_task(gateway.publish(event(1)))
        await asyncio.sleep(0.02)
        self.assertFalse(publishing.done())
        release_replay.set()
        subscription = await opening
        await publishing
        self.assertEqual((await subscription.next_live()).stored.cursor.offset, 1)
        await subscription.close()

    async def test_blocking_partition_does_not_serialize_unrelated_partition(self):
        started = threading.Event()
        release = threading.Event()
        spool = self.spool

        class BlockingSink:
            def append(self, item):
                if item.partition_key == PARTITION_A:
                    started.set()
                    release.wait(timeout=2)
                return spool.append(item)

        gateway = DurableStreamGateway(handoff=self.handoff, sink=BlockingSink())
        blocked = asyncio.create_task(gateway.publish(event(1, PARTITION_A)))
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        peer = await asyncio.wait_for(gateway.publish(event(2, PARTITION_B)), 0.25)
        self.assertEqual(peer.cursor.offset, 1)
        release.set()
        self.assertEqual((await blocked).cursor.offset, 1)

    async def test_server_replay_bound_is_fail_closed(self):
        gateway = DurableStreamGateway(
            handoff=self.handoff, sink=self.spool, max_replay_events=5
        )
        with self.assertRaisesRegex(ValueError, "server bound"):
            await gateway.open(
                consumer_id="paper-alpha",
                stream=STREAM,
                partition_key=PARTITION_A,
                token=self.token(),
                replay_limit=6,
            )

    async def test_truncated_replay_requires_fresh_snapshot_instead_of_live_gap(self):
        token = self.token()
        for sequence in range(1, 4):
            self.spool.append(event(sequence))
        gateway = DurableStreamGateway(
            handoff=self.handoff,
            sink=self.spool,
            max_replay_events=2,
        )
        with self.assertRaisesRegex(CursorExpired, "fresh snapshot"):
            await gateway.open(
                consumer_id="paper-alpha",
                stream=STREAM,
                partition_key=PARTITION_A,
                token=token,
                replay_limit=2,
            )


class Phase71ConfigAndTopologyTests(unittest.TestCase):
    def config(self, root: Path, **overrides) -> BetaRuntimeConfig:
        values = {
            "role": "query_v2",
            "instance_id": "query-v2-beta-1",
            "environment": "paper",
            "config_revision": "phase71",
            "authority_revision": 1,
            "schema_digest": "a" * 64,
            "state_dir": root,
            "durable_state_dir": root,
            "audit_path": root / "audit.jsonl",
            "manifest_paths": (root / "consumer.yaml",),
            "source_bindings_path": None,
            "internal_ingest_secret": None,
            "redis_url": "redis://qdl_beta_redis:6379/0",
            "redis_prefix": "qdl:beta:v2:paper:phase71",
            "consumer_group": "qdl-beta-phase71",
            "cursor_keys": {"beta": b"b" * 32},
            "active_cursor_key_id": "beta",
            "cursor_ttl_seconds": 3600,
            "http_port": 18100,
            "grpc_port": 18110,
            "max_request_bytes": 1024,
            "max_concurrent_requests": 10,
            "max_concurrent_rpcs": 10,
            "max_streams": 10,
            "max_buffer_events": 100,
            "max_replay_events": 100,
            "lease_shard_id": "public",
            "lease_ttl_seconds": 15,
            "lease_renew_seconds": 5.0,
        }
        values.update(overrides)
        return BetaRuntimeConfig(**values)

    def test_config_rejects_legacy_namespace_weak_keys_and_shared_audit_path(self):
        root = Path("/tmp/qdl-phase71")
        with self.assertRaisesRegex(ValueError, "beta Redis prefix"):
            self.config(root, redis_prefix="events.market")
        with self.assertRaisesRegex(ValueError, "256 bits"):
            self.config(root, cursor_keys={"beta": b"weak"})
        with self.assertRaisesRegex(ValueError, "inside beta state"):
            self.config(root, audit_path=Path("/tmp/v1-audit.jsonl"))

    def test_beta_handoff_loads_the_isolated_active_key(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spool = SQLiteDurableSpool(SpoolConfig(
                path=root / "spool.sqlite3", min_free_disk_bytes=0
            ))
            try:
                handoff = build_beta_handoff(self.config(root), spool)
                grant = handoff.issue(
                    consumer_id="phase71",
                    snapshot_id="snapshot-1",
                    snapshot_watermark=handoff.capture_watermark(
                        stream=STREAM, partition_key=PARTITION_A
                    ),
                    ttl_seconds=60,
                )
                self.assertTrue(grant.token)
            finally:
                spool.close()

    def test_compose_isolated_nonroot_bounded_and_immutable(self):
        compose_path = Path(__file__).parents[1] / "docker-compose.phase7-beta.yml"
        raw = compose_path.read_text()
        rendered = raw.replace(
            "${QDL_BETA_IMAGE:?set QDL_BETA_IMAGE to an immutable sha256 image ID or digest}",
            "sha256:" + "1" * 64,
        ).replace(
            "${QDL_BETA_INIT_IMAGE:?set QDL_BETA_INIT_IMAGE to an immutable sha256 image ID or digest}",
            "sha256:" + "2" * 64,
        ).replace(
            "${QDL_BETA_REDIS_IMAGE:?set QDL_BETA_REDIS_IMAGE to an immutable sha256 image ID or digest}",
            "sha256:" + "3" * 64,
        )
        compose = yaml.safe_load(rendered)
        network = compose["networks"]["qdl_beta_internal"]
        self.assertTrue(network["internal"])
        self.assertIn("qdl_beta_ingress", compose["networks"])
        self.assertNotIn("execution_network", raw)
        self.assertNotIn("events.market", raw)
        self.assertIn("qdl:beta:v2:", raw)
        self.assertEqual(compose["services"]["qdl_beta_redis"]["user"], "999:999")
        self.assertIn("yes", compose["services"]["qdl_beta_redis"]["command"])
        self.assertEqual(
            compose["services"]["qdl_beta_redis"]["volumes"],
            ["qdl_beta_redis_state:/data"],
        )
        for name in ("qdl_query_v2_beta", "qdl_stream_v2_beta_a", "qdl_stream_v2_beta_b"):
            service = compose["services"][name]
            self.assertEqual(service["user"], "10001:10001")
            self.assertTrue(service["read_only"])
            self.assertEqual(service["cap_drop"], ["ALL"])
            self.assertEqual(
                service["networks"], ["qdl_beta_internal", "qdl_beta_ingress"]
            )
            self.assertTrue(all(str(port).startswith("127.0.0.1:") for port in service["ports"]))

    def test_shared_redis_quota_is_atomic_and_fails_closed(self):
        payload = manifest_mapping(
            consumer_id="phase71-quota",
            subject="spiffe://qdl/paper/phase71-quota",
            instrument_uid="00000000-0000-0000-0000-000000000001",
        )
        payload["spec"]["quotas"]["requests_per_minute"] = 2
        manifest = ConsumerManifestLoader.from_mapping(payload)

        class FakeRedis:
            def __init__(self):
                self.values = {}
                self.unavailable = False

            def eval(self, script, key_count, key, limit, ttl):
                del script, key_count, ttl
                if self.unavailable:
                    raise RedisConnectionError("offline")
                self.values[key] = self.values.get(key, 0) + 1
                return [int(self.values[key] <= int(limit)), self.values[key]]

            def ping(self):
                if self.unavailable:
                    raise RedisConnectionError("offline")
                return True

            def close(self):
                return None

        backend = FakeRedis()
        first = RedisMinuteQuota(backend, prefix="qdl:beta:v2:paper:phase71")
        second = RedisMinuteQuota(backend, prefix="qdl:beta:v2:paper:phase71")
        first.consume(manifest)
        second.consume(manifest)
        with self.assertRaisesRegex(DataPlaneAccessError, "quota is exhausted"):
            first.consume(manifest)
        backend.unavailable = True
        with self.assertRaises(DataPlaneAccessError) as captured:
            second.consume(manifest)
        self.assertEqual(captured.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
