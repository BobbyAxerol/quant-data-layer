from __future__ import annotations

import base64
import asyncio
import hashlib
import hmac
import json
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from qdl.canary import DeterministicPaperSignalState
from qdl.canonical.trade import canonical_event
from qdl.consumer import ConsumerManifestLoader
from qdl.query import (
    AccessPurpose,
    CanonicalErrorCode,
    DataRequirement,
    QueryServiceError,
)
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.runtime.canary_bridge import (
    CanonicalV1Bridge,
    V1ReadOnlyBarSource,
    V1ReadOnlyBridgeConfig,
    install_internal_canonical_ingest,
)
from qdl.runtime.canary_source import (
    CanarySourceCatalog,
    ConsumerHandoffCursorIssuer,
    SpoolCanonicalQueryBackend,
    build_canary_query_stack,
)
from qdl.stream import DurableStreamGateway, SlowConsumer
from qdl.transport import CursorExpired, SQLiteDurableSpool, SpoolConfig


ROOT = Path(__file__).parents[1]
CATALOG_PATH = ROOT / "config/phase7/canary-sources.yaml"
MONITOR_PATH = ROOT / "consumers/beta/phase7-monitoring-binance.yaml"
PAPER_PATH = ROOT / "consumers/beta/phase7-paper-alpha-binance.yaml"
SCHEMA_DIGEST = "a" * 64
SECRET = b"phase72-internal-ingest-secret-32bytes"


def row(index: int, *, now_ms: int) -> list:
    open_time = now_ms - (5 - index) * 60_000
    return [
        open_time,
        f"{63000 + index}.10",
        f"{63001 + index}.20",
        f"{62999 + index}.30",
        f"{63000 + index}.40",
        f"{10 + index}.500",
        open_time + 59_999,
        "100.0",
        100 + index,
        "4.0",
        "50.0",
        "0",
    ]


class Phase72CatalogTests(unittest.TestCase):
    def test_catalog_and_consumers_are_strict_read_only_and_deterministic(self):
        catalog = CanarySourceCatalog.load(CATALOG_PATH)
        self.assertEqual(len(catalog.bindings), 1)
        binding = catalog.bindings[0]
        self.assertEqual(
            binding.instrument.instrument_uid,
            "a953e16e-7138-5562-b5e8-c337a44d0b65",
        )
        self.assertEqual(binding.read.kind, "BINANCE_CRYPTO_OHLCV")
        self.assertTrue(binding.read.path.startswith("/v1/"))
        for path in (MONITOR_PATH, PAPER_PATH):
            manifest = ConsumerManifestLoader.load(path)
            self.assertEqual(manifest.execution_dependency, "FORBIDDEN")
            self.assertEqual(manifest.rollback_contract, "V1")
            self.assertEqual(manifest.requirements[0].instrument_uid, binding.instrument.instrument_uid)

    def test_unknown_catalog_fields_and_direct_provider_path_fail_closed(self):
        payload = json.loads(json.dumps(__import__("yaml").safe_load(
            CATALOG_PATH.read_text(encoding="utf-8")
        )))
        payload["unknown"] = True
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.yaml"
            path.write_text(__import__("yaml").safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unknown or missing"):
                CanarySourceCatalog.load(path)
        with self.assertRaisesRegex(ValueError, "internal/loopback"):
            V1ReadOnlyBridgeConfig(
                source_catalog_path=str(CATALOG_PATH),
                v1_base_url="https://fapi.binance.com",
                ingest_urls=("http://stream:18101",),
                ingest_secret=SECRET,
            )


class Phase72SourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_v1_reader_preserves_native_rows_and_filters_open_bar(self):
        catalog = CanarySourceCatalog.load(CATALOG_PATH)
        binding = catalog.bindings[0]
        now_ms = int(time.time() * 1000)
        rows = [row(index, now_ms=now_ms) for index in range(4)]
        rows.append([
            now_ms,
            "1", "1", "1", "1", "1",
            now_ms + 59_999,
            "1", 1, "1", "1", "0",
        ])

        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.host, "v1-source.internal")
            return httpx.Response(200, json={
                "provider": "binance",
                "market": "usdm",
                "symbol": "BTCUSDT",
                "requested_interval": "1m",
                "data": rows,
            })

        client = httpx.AsyncClient(
            base_url="http://v1-source.internal",
            transport=httpx.MockTransport(handler),
        )
        source = V1ReadOnlyBarSource("http://v1-source.internal", client=client)
        config = V1ReadOnlyBridgeConfig(
            source_catalog_path=str(CATALOG_PATH),
            v1_base_url="http://v1-source.internal",
            ingest_urls=("http://stream:18101",),
            ingest_secret=SECRET,
        )
        bridge = CanonicalV1Bridge(
            config=config,
            catalog=catalog,
            source=source,
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
            clock_ns=lambda: now_ms * 1_000_000,
        )
        try:
            fetched, envelopes = await bridge.prepare(binding, warmup=True)
            self.assertEqual(fetched, tuple(rows))
            self.assertEqual(len(envelopes), 4)
            self.assertEqual(envelopes[-1].bar.close.source_text, rows[3][4])
            self.assertTrue(all(item.bar.is_final for item in envelopes))
        finally:
            await bridge.close()
            await client.aclose()


class Phase72BackendAndIngestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.catalog = CanarySourceCatalog.load(CATALOG_PATH)
        self.binding = self.catalog.bindings[0]
        self.spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(self.temp.name) / "spool.sqlite3",
            min_free_disk_bytes=0,
        ))
        self.handoff = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec(
                {"phase72": b"c" * 32}, active_key_id="phase72"
            ),
        )
        self.gateway = DurableStreamGateway(handoff=self.handoff, sink=self.spool)
        now_ms = int(time.time() * 1000)
        bridge = CanonicalV1Bridge(
            config=V1ReadOnlyBridgeConfig(
                source_catalog_path=str(CATALOG_PATH),
                v1_base_url="http://data_layer:8100",
                ingest_urls=("http://unused:18101",),
                ingest_secret=SECRET,
            ),
            catalog=self.catalog,
            source=object(),
            client=object(),
            clock_ns=lambda: now_ms * 1_000_000,
        )
        self.envelopes = bridge.canonical_closed_bars(
            self.binding,
            tuple(row(index, now_ms=now_ms) for index in range(3)),
        )

    def tearDown(self):
        self.spool.close()
        self.temp.cleanup()

    def requirement(self) -> DataRequirement:
        return ConsumerManifestLoader.load(PAPER_PATH).requirements[0]

    def body(self, envelope) -> bytes:
        return json.dumps({
            "schema": "qdl.phase7.2.canonical-ingest.v1",
            "batch_id": "00000000-0000-4000-8000-000000000001",
            "events": [base64.b64encode(
                envelope.SerializeToString(deterministic=True)
            ).decode()],
        }, sort_keys=True, separators=(",", ":")).encode()

    def test_internal_ingest_is_authenticated_final_only_and_idempotent(self):
        app = FastAPI()
        install_internal_canonical_ingest(
            app,
            gateway=self.gateway,
            catalog=self.catalog,
            secret=SECRET,
        )
        client = TestClient(app)
        body = self.body(self.envelopes[0])
        self.assertEqual(client.post(
            "/internal/canonical/events", content=body
        ).status_code, 401)
        signature = "sha256=" + hmac.new(SECRET, body, hashlib.sha256).hexdigest()
        first = client.post(
            "/internal/canonical/events",
            content=body,
            headers={"X-QDL-Bridge-Signature": signature},
        )
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(first.json()["accepted"], 1)
        duplicate = client.post(
            "/internal/canonical/events",
            content=body,
            headers={"X-QDL-Bridge-Signature": signature},
        )
        self.assertEqual(duplicate.json()["duplicates"], 1)

        open_bar = type(self.envelopes[0])()
        open_bar.CopyFrom(self.envelopes[0])
        open_bar.bar.is_final = False
        open_bar.bar.lifecycle = 1
        body = self.body(open_bar)
        signature = "sha256=" + hmac.new(SECRET, body, hashlib.sha256).hexdigest()
        self.assertEqual(client.post(
            "/internal/canonical/events",
            content=body,
            headers={"X-QDL-Bridge-Signature": signature},
        ).status_code, 422)

    def test_query_and_cursor_share_one_watermark_and_bind_consumer(self):
        for envelope in self.envelopes:
            self.spool.append(canonical_event(envelope, accepted_at_ns=time.time_ns()))
        backend = SpoolCanonicalQueryBackend(
            self.spool,
            self.catalog,
            schema_digest=SCHEMA_DIGEST,
        )
        requirement = self.requirement()
        history = backend.history(requirement)
        self.assertIsNotNone(history)
        self.assertEqual(history.watermark_offset, 3)
        self.assertEqual(history.items[-1].payload["close"], self.envelopes[-1].bar.close.source_text)
        issuer = ConsumerHandoffCursorIssuer(
            self.handoff, self.catalog, ttl_seconds=3600
        )
        alpha = issuer.bind_history(
            requirement, history, consumer_id="paper-alpha"
        )
        monitor = issuer.bind_history(
            requirement, history, consumer_id="monitor"
        )
        self.assertNotEqual(alpha.stream_cursor, monitor.stream_cursor)
        scope = self.handoff.resolve_scope(
            token=alpha.stream_cursor, consumer_id="paper-alpha"
        )
        self.assertEqual(scope.watermark_offset, 3)
        with self.assertRaisesRegex(ValueError, "consumer scope"):
            self.handoff.resolve_scope(
                token=alpha.stream_cursor, consumer_id="monitor"
            )

    def test_stale_and_missing_bar_fail_closed_for_paper_consumer(self):
        requirement = replace(self.requirement(), warmup_limit=3)
        for envelope in self.envelopes:
            self.spool.append(canonical_event(envelope, accepted_at_ns=time.time_ns()))
        latest_close = int(self.envelopes[-1].bar.close_time_ns)
        service, _backend, _issuer = build_canary_query_stack(
            spool=self.spool,
            catalog=self.catalog,
            schema_digest=SCHEMA_DIGEST,
            handoff=self.handoff,
            cursor_ttl_seconds=3600,
        )
        service.backend._clock_ns = lambda: latest_close + 181_000_000_000
        with self.assertRaises(QueryServiceError) as stale:
            service.warmup(requirement, purpose=AccessPurpose.INTERNAL_ALPHA)
        self.assertEqual(stale.exception.problem.code, CanonicalErrorCode.DATA_STALE)

        self.spool.close()
        path = Path(self.temp.name) / "gap-spool.sqlite3"
        self.spool = SQLiteDurableSpool(SpoolConfig(path=path, min_free_disk_bytes=0))
        self.handoff = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec(
                {"phase72": b"c" * 32}, active_key_id="phase72"
            ),
        )
        for envelope in (self.envelopes[0], self.envelopes[2]):
            self.spool.append(canonical_event(envelope, accepted_at_ns=time.time_ns()))
        service, backend, _issuer = build_canary_query_stack(
            spool=self.spool,
            catalog=self.catalog,
            schema_digest=SCHEMA_DIGEST,
            handoff=self.handoff,
            cursor_ttl_seconds=3600,
        )
        gap_requirement = replace(requirement, warmup_limit=2)
        with self.assertRaises(QueryServiceError) as gap:
            service.warmup(gap_requirement, purpose=AccessPurpose.INTERNAL_ALPHA)
        self.assertEqual(gap.exception.problem.code, CanonicalErrorCode.OPEN_SEQUENCE_GAP)
        self.assertEqual(len(backend.open_gaps()), 1)

    def test_cursor_expiry_and_slow_consumer_require_explicit_replay(self):
        now = [1_000_000_000]
        codec = SignedHandoffCursorCodec(
            {"phase72": b"e" * 32},
            active_key_id="phase72",
            clock_ns=lambda: now[0],
        )
        handoff = GapFreeHandoff(self.spool, codec, clock_ns=lambda: now[0])
        grant = handoff.issue(
            consumer_id="phase72-paper",
            snapshot_id="snapshot-expiry",
            snapshot_watermark=handoff.capture_watermark(
                stream=self.binding.stream,
                partition_key=self.binding.partition_key,
            ),
            ttl_seconds=1,
        )
        now[0] += 2_000_000_000
        with self.assertRaises(CursorExpired):
            handoff.resolve_scope(
                token=grant.token, consumer_id="phase72-paper"
            )

        async def exercise_slow_consumer():
            live_handoff = GapFreeHandoff(
                self.spool,
                SignedHandoffCursorCodec(
                    {"phase72": b"f" * 32}, active_key_id="phase72"
                ),
            )
            gateway = DurableStreamGateway(
                handoff=live_handoff, sink=self.spool, max_buffer_events=1
            )
            token = live_handoff.issue(
                consumer_id="phase72-slow",
                snapshot_id="snapshot-slow",
                snapshot_watermark=live_handoff.capture_watermark(
                    stream=self.binding.stream,
                    partition_key=self.binding.partition_key,
                ),
                ttl_seconds=60,
            ).token
            subscription = await gateway.open(
                consumer_id="phase72-slow",
                stream=self.binding.stream,
                partition_key=self.binding.partition_key,
                token=token,
                max_buffer_events=1,
            )
            await gateway.publish(canonical_event(
                self.envelopes[0], accepted_at_ns=time.time_ns()
            ))
            await gateway.publish(canonical_event(
                self.envelopes[1], accepted_at_ns=time.time_ns()
            ))
            with self.assertRaises(SlowConsumer):
                await subscription.next_live()
            await subscription.close()

        asyncio.run(exercise_slow_consumer())

    def test_paper_signal_state_is_revision_aware_and_reproducible(self):
        first = DeterministicPaperSignalState(max_bars=3)
        second = DeterministicPaperSignalState(max_bars=3)
        values = (
            (1, 0, "10.0"),
            (2, 0, "11.0"),
            (3, 0, "12.0"),
            (2, 1, "11.5"),
        )
        for target in (first, second):
            for open_time_ns, revision, close in values:
                target.apply_bar(
                    open_time_ns=open_time_ns,
                    revision=revision,
                    close=close,
                )
        self.assertEqual(first.digest, second.digest)
        self.assertEqual(first.signal, second.signal)
        with self.assertRaisesRegex(ValueError, "revision regressed"):
            first.apply_bar(open_time_ns=2, revision=0, close="11.0")


if __name__ == "__main__":
    unittest.main()
