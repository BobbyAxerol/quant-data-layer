"""TEST_ONLY: in-process SDK transports, no HTTP server or provider traffic."""
from __future__ import annotations

import asyncio
import copy
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from qdl_sdk.client import AsyncDataLayerClient
from qdl_sdk.errors import CursorExpiredError, DataLayerError
from qdl_sdk.models import ControlEvent, StreamEvent
from qdl.marketdata.v2 import market_data_pb2
from scripts import benchmark_kn_universe as bench
from tests.test_fund_phase5_stream_sdk import FakeQueryTransport, ScriptedStreamTransport

DAY = 86400_000_000_000


def profile_mapping(count=3, limit=3):
    return {
        "provenance": "TEST_ONLY", "interval": "1d", "limit": limit,
        "as_of_ns": DAY * 10000, "bar_anchor_ns": 0, "maxlen": min(2, limit),
        "batch_size": 2, "max_batch_rows": 10000,
        "identities": [{"id": "TEST_ONLY", "subject": "spiffe://test/reader", "environment": "test",
                        "manifest_revision": 1, "max_batch_items": 50, "max_streams": 60, "tls": {k: "/TEST_ONLY/" + k for k in ("ca_file", "cert_file", "key_file")},
                        "jwt": {"private_key_file": "/TEST_ONLY/jwt", "key_id": "TEST_ONLY",
                                "issuer": "TEST_ONLY", "audience": "TEST_ONLY", "roles": ["historical_reader"]}}],
        "targets": [{"replica": "a", "query": "https://query-a.test", "stream": "stream-a.test:8210",
                     "route_revision": "TEST_ONLY"}],
        "products": [{"consumer_id": "TEST_ONLY", "venue": "BINANCE" if i % 2 == 0 else "OKX",
                      "symbol": f"TEST_ONLY_{i}", "instrument_id": f"TEST_ONLY:{i}",
                      "requirement": {"instrument_uid": f"TEST_ONLY_{i}", "feed": "BAR",
                                      "consumer_grade": "ALPHA", "source_policy_id": "TEST_ONLY",
                                      "interval": "1d", "warmup_limit": limit}}
                     for i in range(count)]}


class LocalQuery:
    def __init__(self, profile, *, mutate=None, failure_call=None, delay=0):
        self.products = {p.requirement.instrument_uid: p for p in profile.products}
        self.calls, self.mutate, self.failure_call, self.delay = [], mutate, failure_call, delay
        self.close = AsyncMock()
        self.warmup = AsyncMock(side_effect=AssertionError("no second warmup"))
        self.cancelled = False

    async def warmup_batch(self, requirements, *, consumer_id, require_all):
        self.calls.append((tuple(requirements), consumer_id, require_all))
        try:
            await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        if self.failure_call == len(self.calls):
            raise DataLayerError("DATA_STALE", "SECRET raw provider message")
        results = []
        for req in requirements:
            product = self.products[req.instrument_uid]
            raw = await FakeQueryTransport("TEST_ONLY_SECRET_CURSOR").warmup(req, consumer_id=consumer_id)
            base = raw["data"][0]
            base.update(instrument_id=product.instrument_id)
            base["source"]["venue"] = product.venue
            base["quality"]["flags"] = ["TEST_ONLY"]
            raw["data"] = []
            for i in range(min(req.warmup_specification.rows, 3)):
                view = copy.deepcopy(base)
                view["payload"].update(open_time_ns=(20 + i) * DAY, close_time_ns=(21 + i) * DAY)
                view["observed_at_ns"] = (21 + i) * DAY
                view["received_at_ns"] = (21 + i) * DAY + 1000000
                raw["data"].append(view)
            raw.update(count=len(raw["data"]), data_as_of_ns=raw["data"][-1]["observed_at_ns"])
            item = {"instrument_uid": req.instrument_uid, "status": "OK", "data": raw}
            if self.mutate:
                self.mutate(item, len(self.calls))
            results.append(item)
        failures = sum(x.get("problem") is not None for x in results)
        return json.loads(json.dumps({"request_id": "TEST_ONLY", "partial": failures > 0,
                                      "success_count": len(results) - failures, "error_count": failures,
                                      "results": results}))


def local_client(profile, **kwargs):
    return AsyncDataLayerClient(query_transport=LocalQuery(profile, **kwargs),
                               stream_transport=ScriptedStreamTransport([]), consumer_id="TEST_ONLY",
                               max_reconnect_attempts=0)


class UniverseBenchmarkTests(unittest.IsolatedAsyncioTestCase):
    async def run_one(self, profile=None, client=None, **kwargs):
        profile = profile or bench.Profile.model_validate(profile_mapping())
        client = client or local_client(profile)
        return await bench.run_target(profile, profile.identities[0], profile.targets[0], client, **kwargs)

    async def test_real_client_factory_uses_sdk_constructor_signatures(self):
        p = bench.Profile.model_validate(profile_mapping())
        with patch.object(bench, "WorkloadTlsConfig", autospec=True) as tls, \
             patch.object(bench, "RotatingJwtCredentialProvider", autospec=True), \
             patch.object(bench, "MeasuredQuery", autospec=True), \
             patch.object(bench, "GrpcStreamTransport", autospec=True):
            client = bench.client_for(p.identities[0], p.targets[0], p.timeout_ms)
            tls.assert_called_once_with("/TEST_ONLY/ca_file", "/TEST_ONLY/cert_file", "/TEST_ONLY/key_file")
            self.assertEqual(client.consumer_id, "TEST_ONLY")
            await client.close()

    async def test_inventory_never_opens_client_or_credentials(self):
        p = bench.Profile.model_validate(profile_mapping(350, 5000))
        factory = Mock(side_effect=AssertionError("network"))
        report = await bench.run(p, factory=factory)
        self.assertEqual((report["status"], report["declared_products"]), ("INVENTORY_ONLY", 350))
        self.assertEqual(report["provenance"], "TEST_ONLY")
        factory.assert_not_called()

    def test_manifest_roundtrip_and_closed_declared_scope(self):
        raw = profile_mapping()
        self.assertEqual(bench.Profile.model_validate_json(json.dumps(raw)).limit, 3)
        mutations = [lambda p: p.update(limit=5000), lambda p: p.update(batch_size=101),
                     lambda p: p.update(max_batch_rows=0), lambda p: p.update(stream_ms=100),
                     lambda p: p["products"].append(p["products"][0]),
                     lambda p: p["identities"][0]["tls"].update(key_file="inline-secret"),
                     lambda p: p["targets"][0].update(query="https://user:secret@host"),
                     lambda p: p["products"][0]["requirement"].update(interval="1m"),
                     lambda p: p["products"][0]["requirement"].update(consumer_grade="EXECUTION")]
        for mutate in mutations:
            value = copy.deepcopy(raw)
            mutate(value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                bench.Profile.model_validate(value)

    async def test_sdk_row_budget_no_prefetch_scratch_discard_and_counts(self):
        p = bench.Profile.model_validate(profile_mapping(5, 5000))
        p.batch_size = 50
        client, windows = local_client(p), []
        async def ready(product, window):
            self.assertEqual(len(client.query_transport.calls), len(windows) // 2 + 1)
            self.assertEqual(len(window), 2)
            windows.append(window)
        report = await self.run_one(p, client, on_ready=ready)
        self.assertEqual([len(x[0]) for x in client.query_transport.calls], [2, 2, 1])
        self.assertTrue(all(not x[2] for x in client.query_transport.calls))
        self.assertEqual((report["offered"], report["usable"], report["returned_rows"]), (5, 5, 15))
        self.assertTrue(all(not w for w in windows))
        self.assertTrue(all(x["short_history"] and x["quality_flags"] == ["TEST_ONLY"] for x in report["items"]))
        self.assertIsNone(report["response_body_bytes"])
        self.assertIsNone(report["decode_ms"])
        self.assertNotIn("validated_json_bytes", report["items"][0])
        text = json.dumps(report)
        for forbidden in ("SECRET", "source_text", "schema_digest", "sha256"):
            self.assertNotIn(forbidden, text)

    async def test_timing_before_queue_after_sdk_window_callback_separate(self):
        p = bench.Profile.model_validate(profile_mapping(2))
        client, clock = local_client(p), SimpleNamespace(now=0.)
        fake_time = SimpleNamespace(perf_counter=lambda: clock.now, time_ns=lambda: DAY * 30)
        class Gate:
            async def __aenter__(self):
                clock.now += .010
            async def __aexit__(self, *args):
                return None
        original = bench.apply_window
        def apply(*args):
            clock.now += .020
            original(*args)
        async def ready(product, window):
            clock.now += .030
        with patch.object(bench, "time", fake_time), patch.object(bench, "apply_window", apply):
            report = await self.run_one(p, client, semaphore=Gate(), on_ready=ready)
        self.assertAlmostEqual(report["queue_ms"], 10)
        self.assertAlmostEqual(report["chunks"][0]["sdk_complete_ms"], 10)
        self.assertAlmostEqual(report["first_item_usable_ms"], 30)
        self.assertAlmostEqual(report["all_items_usable_ms"], 80)
        self.assertAlmostEqual(report["first_chunk_complete_ms"], 110)
        self.assertAlmostEqual(report["all_callbacks_complete_ms"], 110)
        self.assertGreater(report["items"][0]["source_ages_ms"]["observed"], 110)

    async def test_typed_partial_failure_identity_flags_denominator(self):
        def fail(item, call):
            if item["instrument_uid"] != "TEST_ONLY_1":
                return
            item.pop("data")
            item.update(status="ERROR", problem={"type": "TEST_ONLY", "title": "TEST_ONLY", "status": 503,
                "code": "DATA_STALE", "detail": "SECRET", "request_id": "TEST_ONLY", "retryable": True,
                "diagnostics": {"evaluated_at_ns": DAY, "state": "STALE", "freshness_ms": 123,
                    "event_recency_state": "STALE", "provider_session_state": "LIVE", "execution_eligible": False,
                    "gap_open": False, "complete": True, "reason_codes": ["TEST_ONLY", "EVENT_STALE"]}})
        p = bench.Profile.model_validate(profile_mapping())
        report = await self.run_one(p, local_client(p, mutate=fail))
        self.assertEqual((report["offered"], report["usable"], report["failed"]), (3, 2, 1))
        self.assertIsNone(report["all_items_usable_ms"])
        row = report["items"][1]
        self.assertEqual((row["venue"], row["symbol"], row["error_code"]), ("OKX", "TEST_ONLY_1", "DATA_STALE"))
        self.assertIn("EVENT_STALE", row["quality_flags"])
        self.assertNotIn("SECRET", json.dumps(report))

    async def test_transport_abort_keeps_unattempted_universe(self):
        p = bench.Profile.model_validate(profile_mapping(5))
        report = await self.run_one(p, local_client(p, failure_call=2))
        self.assertEqual((report["offered"], report["usable"], report["failed"], report["not_attempted"]), (5, 2, 2, 1))
        self.assertIsNone(report["all_items_usable_ms"])
        self.assertEqual(report["items"][2]["failure_scope"], "CHUNK")
        self.assertNotIn("SECRET", json.dumps(report))

    async def test_sdk_and_window_failures_are_not_usable(self):
        mutations = {
            "uid": (lambda x: x["data"]["data"][0].update(instrument_uid="wrong"), "CONFLICT"),
            "identity": (lambda x: x["data"]["data"][0].update(instrument_id="wrong"), "CONFLICT"),
            "venue": (lambda x: x["data"]["data"][0]["source"].update(venue="wrong"), "CONFLICT"),
            "decimal": (lambda x: x["data"]["data"][0]["payload"]["open"].update(source_text="99"), "SCHEMA_NOT_SUPPORTED"),
            "final": (lambda x: x["data"]["data"][0]["payload"].update(lifecycle="IN_PROGRESS"), "DATA_NOT_READY"),
            "gap": (lambda x: x["data"]["data"].pop(1), "PARTIAL_RESULT"),
            "coverage": (lambda x: x["data"].update(coverage="PARTIAL"), "PARTIAL_RESULT"),
            "anchor": (lambda x: x["data"]["data"][0]["payload"].update(open_time_ns=20 * DAY + 1), "BAR_ANCHOR_MISMATCH"),
        }
        for name, (mutate, code) in mutations.items():
            p = bench.Profile.model_validate(profile_mapping(1))
            with self.subTest(name=name):
                report = await self.run_one(p, local_client(p, mutate=lambda item, _: mutate(item)))
                self.assertIsNone(report["first_item_usable_ms"])
                self.assertEqual(report["items"][0]["error_code"], code)

    async def test_gap_and_cutoff_not_silently_truncated(self):
        p = bench.Profile.model_validate(profile_mapping(1))
        p.as_of_ns = DAY * 22
        report = await self.run_one(p)
        self.assertEqual(report["items"][0]["error_code"], "AS_OF_EXCEEDED")
        p.as_of_ns = DAY * 10000
        def gap(item, _):
            item["data"]["data"].pop(1)
            item["data"]["count"] = 2
        report = await self.run_one(p, local_client(p, mutate=gap))
        self.assertEqual(report["items"][0]["error_code"], "OPEN_SEQUENCE_GAP")

    async def test_timeout_and_external_cancellation_close_no_prefetch(self):
        p = bench.Profile.model_validate(profile_mapping(5))
        p.timeout_ms = 5
        client = local_client(p, delay=10)
        report = await self.run_one(p, client)
        self.assertEqual((report["timed_out"], report["not_attempted"]), (2, 3))
        self.assertTrue(client.query_transport.cancelled)
        p.timeout_ms = 120000
        client = local_client(p, delay=10)
        task = asyncio.create_task(bench.run(p, execute=True, factory=lambda *args: client))
        while not client.query_transport.calls:
            await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        client.query_transport.close.assert_awaited_once()
        self.assertEqual(len(client.query_transport.calls), 1)

    async def test_callback_failure_not_readiness_and_windows_cleared(self):
        seen = []
        async def fail(product, window):
            seen.append(window)
            raise RuntimeError("SECRET")
        report = await self.run_one(on_ready=fail)
        self.assertEqual(report["status"], "INCOMPLETE")
        self.assertIsNone(report["all_items_usable_ms"])
        self.assertEqual(report["usable"], 0)
        self.assertTrue(all(r["usable_ms"] is None for r in report["items"]))
        self.assertIsNone(report["all_callbacks_complete_ms"])
        self.assertTrue(all(not w for w in seen))

    async def test_selected_stream_reuses_warmup_applies_before_ack(self):
        p = bench.Profile.model_validate(profile_mapping(1))
        p.stream_ms, p.products[0].stream_approved = 100, True
        client = local_client(p)
        event = StreamEvent(1, "TEST_ONLY_NEXT_CURSOR", market_data_pb2.EventEnvelope())
        stream = ScriptedStreamTransport([[ControlEvent("REPLAYING", "TEST_ONLY"),
                                           ControlEvent("LIVE", "TEST_ONLY"), event]])
        client.stream_transport = stream
        def project(event, *, template, requirement):
            return template
        with patch.object(bench, "market_data_view_from_stream", side_effect=project):
            report = await self.run_one(p, client)
        self.assertEqual(report["items"][0]["stream"]["status"], "APPLIED")
        client.query_transport.warmup.assert_not_awaited()
        self.assertEqual(report["items"][0]["stream"]["controls"], {"REPLAYING": 1, "LIVE": 1})
        self.assertEqual(stream.tokens, ["TEST_ONLY_SECRET_CURSOR"])
        self.assertEqual(stream.iterators[0].close_calls, 1)
        self.assertEqual(client.cursor_store.load(client._cursor_key(p.products[0].requirement)).offset, 1)

    async def test_paired_targets_do_not_crosswire_or_auto_stream(self):
        raw = profile_mapping(1)
        raw["targets"].append({"replica": "b", "query": "https://query-b.test", "stream": "stream-b.test:8210",
                               "route_revision": "TEST_ONLY"})
        p = bench.Profile.model_validate(raw)
        clients = []
        def factory(identity, target, timeout):
            clients.append((target, local_client(p)))
            return clients[-1][1]
        report = await bench.run(p, execute=True, factory=factory)
        self.assertEqual([r["target"]["replica"] for r in report["runs"]], ["a", "b"])
        self.assertEqual([t.stream for t, _ in clients], ["stream-a.test:8210", "stream-b.test:8210"])
        for _, client in clients:
            self.assertEqual(client.stream_transport.tokens, [])
            client.query_transport.close.assert_awaited_once()

    def test_decode_counter_without_http_or_fabricated_wire_time(self):
        query = object.__new__(bench.MeasuredQuery)
        raw = b'{"TEST_ONLY": true}'
        response = SimpleNamespace(content=raw, json=lambda: json.loads(raw), is_success=True)
        self.assertEqual(query._decode(response), {"TEST_ONLY": True})
        self.assertEqual(query.body_bytes, len(raw))
        self.assertGreaterEqual(query.decode_ms, 0)

    async def test_setup_failure_preserves_declared_denominator(self):
        p = bench.Profile.model_validate(profile_mapping())
        report = await bench.run(p, execute=True, factory=Mock(side_effect=OSError("SECRET")))
        result = report["runs"][0]
        self.assertEqual((result["offered"], result["not_attempted"], result["usable"]), (3, 3, 0))
        self.assertEqual(result["error_code"], "OSError")
        self.assertEqual(report["status"], "INCOMPLETE")
        self.assertNotIn("SECRET", json.dumps(report))

    def test_declared_quota_not_sdk_http_maximum(self):
        raw = profile_mapping(350)
        raw.pop("batch_size")
        self.assertEqual(bench.Profile.model_validate(raw).batch_size, 50)
        raw["batch_size"] = 51
        with self.assertRaisesRegex(ValueError, "consumer quota"):
            bench.Profile.model_validate(raw)
        raw["batch_size"], raw["stream_ms"] = 50, 100
        for product in raw["products"]:
            product["stream_approved"] = True
        with self.assertRaisesRegex(ValueError, "stream selection"):
            bench.Profile.model_validate(raw)

    async def test_resnapshot_applied_before_projection_and_unknown_control_fails(self):
        p = bench.Profile.model_validate(profile_mapping(1))
        p.stream_ms, p.products[0].stream_approved = 100, True
        for invalid in (False, True):
            client = local_client(p)
            raw = await client.query_transport.warmup_batch(
                [p.products[0].requirement], consumer_id="TEST_ONLY", require_all=False)
            replacement = raw["results"][0]["data"]
            replacement["stream_cursor"] = "TEST_ONLY_REPLACEMENT"
            for row in replacement["data"]:
                row["payload"]["open_time_ns"] += DAY
                row["payload"]["close_time_ns"] += DAY
            client.query_transport.calls.clear()
            client.query_transport.warmup = AsyncMock(return_value=replacement)
            event = StreamEvent(2, "TEST_ONLY_NEXT", market_data_pb2.EventEnvelope())
            stream = ScriptedStreamTransport(
                [[ControlEvent("SNAPSHOT_REQUIRED", "TEST_ONLY")]] if invalid else
                [[CursorExpiredError("CURSOR_EXPIRED", "TEST_ONLY")], [event]])
            client.stream_transport = stream
            def project(event, *, template, requirement):
                self.assertEqual(template.payload.open_time_ns, 23 * DAY)
                return template
            with patch.object(bench, "market_data_view_from_stream", side_effect=project) as projection:
                report = await self.run_one(p, client)
            result = report["items"][0]["stream"]
            if invalid:
                self.assertEqual(result["error_code"], "STREAM_CONTROL")
                self.assertIsNone(result["usable_ms"])
                projection.assert_not_called()
            else:
                self.assertEqual(result["status"], "APPLIED")
                self.assertEqual(result["controls"], {"SNAPSHOT_REPLACED": 1})
                self.assertEqual(stream.tokens, ["TEST_ONLY_SECRET_CURSOR", "TEST_ONLY_REPLACEMENT"])
                client.query_transport.warmup.assert_awaited_once()

    def test_cli_schema_without_live_access(self):
        with patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(bench.main(["--schema"]), 0)
        self.assertIn("products", json.loads(output.getvalue())["properties"])


if __name__ == "__main__":
    unittest.main()
