from __future__ import annotations

import asyncio
from collections import Counter
import copy
from contextlib import asynccontextmanager
from dataclasses import replace
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from scripts import benchmark_consumer_endpoints as bench
from tests.universe_support import UNIVERSE_PER_VENUE, UNIVERSE_TOTAL


class ConsumerEndpointBenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.profile = json.loads((bench.ROOT / "config/examples/consumer-endpoint-benchmark.json").read_text())
        cls.manifest, cls.scope = bench.load_scope("trading-system.paper.stable")

    def test_exact_ts_scope_not_two_per_feed(self):
        self.assertEqual(len(self.scope.products), 60)
        self.assertEqual(Counter(p.venue for p in self.scope.products), {"BINANCE": 30, "OKX": 30})
        cases = bench.cases_for(self.scope.products, 8)
        singles = {ps[0].identity for op, ps in cases if op in ("snapshot", "reference_batch") and len(ps) == 1}
        self.assertEqual(singles, {p.identity for p in self.scope.products})

    def test_all_release_products_and_metric_intervals(self):
        from qdl.consumer import StableReleaseRoutePlan
        release = StableReleaseRoutePlan.load(bench.ROOT / "config/v2/stable-v2-release-routing.yaml", manifest_root=bench.ROOT)
        products = []
        for consumer in release.consumers:
            if not any(x.route == "V2_PRIMARY" for x in consumer.products):
                continue
            _, scope = bench.load_scope(consumer.consumer_id)
            products.extend(scope.products)
            cases = bench.cases_for(scope.products, 8)
            singles = {ps[0].identity for op, ps in cases if op in ("snapshot", "reference_batch") and len(ps) == 1}
            self.assertEqual(singles, {p.identity for p in scope.products})
        self.assertEqual(len(products), 314 + UNIVERSE_TOTAL)
        reference_feeds = set()
        for p in products:
            if p.delivery.value == "ON_DEMAND":
                r = bench.reference_product(p)
                self.assertEqual(r.sdk_requirement.source_policy_id, p.source_policy_id)
                self.assertEqual(r.sdk_requirement.consumer_grade.value, p.requirement.consumer_grade.value)
                reference_feeds.add(p.feed.value)
        self.assertTrue({"FUNDING_RATE", "OPEN_INTEREST", "BASIS", "MARK_INDEX_PRICE", "TAKER_FLOW", "CONTRACT_METADATA", "LONG_SHORT_RATIO"} <= reference_feeds)

    def test_openapi_coverage_and_fail_closed_drift(self):
        snapshot = json.loads((bench.ROOT / "contracts/v2/openapi.snapshot.json").read_text())
        self.assertEqual(len(bench.endpoint_coverage(snapshot)), 11)
        snapshot["paths"]["/v2/new-read"] = {"get": {}}
        with self.assertRaisesRegex(ValueError, "coverage drift"):
            bench.endpoint_coverage(snapshot)

    def test_all_operation_cases_with_no_unbounded_batches(self):
        cases = bench.cases_for(self.scope.products, 8)
        self.assertEqual({op for op, _ in cases}, set(bench.OPERATIONS))
        self.assertLessEqual(max(len(ps) for _, ps in cases), 8)
        bars = [p for p in self.scope.products if p.feed.value == "BAR"]
        self.assertEqual(len([1 for op, _ in cases if op == "history"]), len(bars))

    def test_profile_rejects_inline_secret_http_invalid_budget(self):
        self.assertEqual(bench.validate_profile(self.profile)["limits"]["rounds"], 3)
        for update in ({"secret": "not-allowed"}, {"queries": ["http://query"]},
                       {"queries": ["https://user:pass@query"]}, {"limits": {"rounds": 0}},
                       {"limits": {"rounds": 1.5}}, {"limits": {"requests_per_second": 100}}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                bench.validate_profile({**self.profile, **update})

    def test_no_duplicate_consumer_identity(self):
        p = copy.deepcopy(self.profile)
        p["consumers"] *= 2
        with self.assertRaises(ValueError):
            bench.validate_profile(p)

    def test_operation_filter_is_explicit_not_a_hidden_cap(self):
        self.assertEqual(bench.validate_profile({**self.profile, "operations": ["reference_batch"]})["operations"], ["reference_batch"])
        with self.assertRaises(ValueError):
            bench.validate_profile({**self.profile, "operations": ["order"]})

    def test_percentiles_and_no_p99_without_samples(self):
        self.assertIsNone(bench.percentiles([])["p50_ms"])
        self.assertIsNone(bench.percentiles([10, 20])["p99_ms"])
        self.assertEqual(bench.percentiles(range(100))["p99_ms"], 98)

    def test_validation_is_inside_usable_boundary(self):
        async def call():
            return {"data": {}}
        with patch.object(bench.time, "perf_counter", side_effect=[1, 2, 4, 5]):
            result = asyncio.run(bench.measure(call, Mock(), 1))
        self.assertEqual(result["response_ms"], 1000)
        self.assertEqual(result["usable_ms"], 3000)

    def test_invalid_response_not_a_usable_sample_and_diagnostics_preserved(self):
        raw = {"results": [{"instrument_uid": "a", "problem": {"code": "DATA_STALE", "detail": "secret"}}]}
        def invalid(_):
            raise ValueError("raw secret")
        result = asyncio.run(bench.measure(AsyncMock(return_value=raw), invalid, 1))
        self.assertEqual(result["status"], "FAIL")
        self.assertIsNone(result["usable_ms"])
        self.assertEqual(result["diagnostics"][0]["problem_code"], "DATA_STALE")
        self.assertNotIn("secret", json.dumps(result))

    def test_timeout_disconnection_and_quiet_distinct(self):
        async def wait():
            await asyncio.sleep(10)
        timeout = asyncio.run(bench.measure(wait, Mock(), .001))
        quiet = asyncio.run(bench.measure(AsyncMock(side_effect=bench.NoEvent()), Mock(), 1))
        disconnected = asyncio.run(bench.measure(AsyncMock(side_effect=ConnectionError()), Mock(), 1))
        self.assertEqual(timeout["status"], "FAIL")
        self.assertEqual(quiet["status"], "NO_EVENT")
        self.assertEqual(disconnected["status"], "FAIL")
        self.assertTrue(all(x["usable_ms"] is None for x in (timeout, quiet, disconnected)))

    def test_quality_age_not_request_latency_and_payload_excluded(self):
        raw = {"data": {"instrument_uid": "x", "observed_at_ns": 1,
                        "payload": {"price": "123"}, "cursor": "secret", "quality": {"state": "STALE", "gap_open": True}}}
        d = bench.diagnostics(raw)[0]
        self.assertEqual(d["quality"]["state"], "STALE")
        self.assertIn("event_age_at_client_ms", d)
        self.assertNotIn("price", json.dumps(d))
        self.assertNotIn("secret", json.dumps(d))

    def test_warmup_historical_current_validation(self):
        p = next(p for p in self.scope.products if p.feed.value == "BAR")
        with patch("qdl_sdk.client._validate_query_payload", return_value=SimpleNamespace(data=["old", "new"])), \
                patch("qdl.certification.phase103_consumer_acceptance.validate_product_view") as validate:
            bench.validate_operation("history", (p,), {})
            self.assertEqual([x.kwargs["require_current_quality"] for x in validate.call_args_list], [False, True])
        with patch("qdl_sdk.client._validate_query_payload", return_value=SimpleNamespace(data=[])), self.assertRaises(ValueError):
            bench.validate_operation("warmup", (p,), {})

    def test_snapshot_uses_domain_identity_validator(self):
        p = self.scope.products[0]
        with patch("qdl_sdk.client._validate_query_payload", return_value=SimpleNamespace(data="view")), \
                patch("qdl.certification.phase103_consumer_acceptance.validate_product_view", side_effect=ValueError("identity")):
            with self.assertRaises(ValueError):
                bench.validate_operation("snapshot", (p,), {})

    def test_status_valid_does_not_claim_price_usable(self):
        p = self.scope.products[0]
        with patch("qdl_sdk.client._validate_feed_status_payload") as validate:
            bench.validate_operation("feed_status", (p,), {"quality": {"state": "STALE"}})
            validate.assert_called_once()

    def test_partial_batch_fails_and_not_hidden(self):
        from qdl_sdk.client import AsyncDataLayerClient
        p = next(p for p in self.scope.products if p.feed.value == "BAR")
        with patch.object(AsyncDataLayerClient, "_validate_batch_chunk", return_value=SimpleNamespace(partial=True)):
            with self.assertRaisesRegex(ValueError, "PARTIAL_RESULT"):
                bench.validate_operation("warmup_batch", (p,), {})

    def test_inventory_no_credentials_or_requests(self):
        p = bench.validate_profile(self.profile)
        with patch("qdl_sdk.transport.RestQueryTransport", side_effect=AssertionError("network must not run")):
            result = asyncio.run(bench.run_client(p, inventory=True))
        self.assertEqual(result["status"], "INVENTORY_ONLY")
        self.assertEqual(result["scope"]["product_count"], 60)

    def test_deadline_permission_and_disabled_stream_remain_visible(self):
        p = bench.validate_profile(self.profile)
        p["limits"]["max_seconds"] = 0
        manifest = replace(self.manifest, allowed_permissions=frozenset({"snapshot:read", "stream:read"}))
        query, stream = AsyncMock(), AsyncMock()
        with patch.object(bench, "load_scope", return_value=(manifest, self.scope)), \
                patch("qdl_sdk.tls.WorkloadTlsConfig"), patch("qdl_sdk.credentials.RotatingJwtCredentialProvider"), \
                patch("qdl_sdk.transport.RestQueryTransport", return_value=query), \
                patch("qdl_sdk.transport.GrpcStreamTransport", return_value=stream):
            result = asyncio.run(bench.run_client(p))
        states = Counter(r["status"] for r in result["results"])
        self.assertGreater(states["PERMISSION_EXCLUDED"], 0)
        self.assertGreater(states["NOT_MEASURED_DEADLINE"], 0)
        self.assertGreater(states["NOT_MEASURED_STREAM_DISABLED"], 0)
        self.assertEqual(states["SAFETY_BLOCKED"], 2)
        self.assertEqual(result["status"], "INCOMPLETE")
        query.snapshot.assert_not_called()
        self.assertEqual(query.close.await_count, 2)

    def test_unsafe_diagnostic_cannot_be_enabled_by_operation_filter(self):
        p = bench.validate_profile({**self.profile, "operations": ["gaps"]})
        query, stream = AsyncMock(), AsyncMock()
        with patch.object(bench, "load_scope", return_value=(self.manifest, self.scope)), \
                patch("qdl_sdk.tls.WorkloadTlsConfig"), patch("qdl_sdk.credentials.RotatingJwtCredentialProvider"), \
                patch("qdl_sdk.transport.RestQueryTransport", return_value=query), \
                patch("qdl_sdk.transport.GrpcStreamTransport", return_value=stream):
            result = asyncio.run(bench.run_client(p))
        self.assertEqual(result["status"], "INCOMPLETE")
        self.assertEqual(sum(r["status"] == "SAFETY_BLOCKED" for r in result["results"]), 2)
        query._read_request.assert_not_called()

    def test_stream_no_event_and_disconnect_close_session(self):
        product = next(p for p in self.scope.products if p.feed.value == "TRADE")
        for error, expected in ((asyncio.TimeoutError(), bench.NoEvent), (ConnectionError(), ConnectionError)):
            closed = []
            @asynccontextmanager
            async def session(_):
                try:
                    yield SimpleNamespace(__anext__=AsyncMock(side_effect=error))
                finally:
                    closed.append(True)
            client = SimpleNamespace(warmup_then_stream=session)
            with self.assertRaises(expected):
                asyncio.run(bench.invoke("stream", (product,), AsyncMock(), client, self.manifest, .01))
            self.assertEqual(closed, [True])

    def test_container_mounts_only_tool_public_contracts_exact_keys(self):
        p = bench.validate_profile(self.profile)
        c = copy.deepcopy(p["consumers"][0])
        with tempfile.TemporaryDirectory() as tmp:
            key = Path(tmp) / "identity"
            key.touch()
            c["tls"] = {k: str(key) for k in c["tls"]}
            c["jwt"]["private_key_file"] = str(key)
            cmd, inner = bench.docker_command(p, c, "qdl-endpoint-bench-test", "sha256:" + "a" * 64, False)
        self.assertNotIn("docker.sock", " ".join(cmd))
        self.assertNotIn("--privileged", cmd)
        self.assertIn("--rm", cmd)
        self.assertEqual(cmd.count("--mount"), 8)
        self.assertEqual(inner["consumers"][0]["tls"]["key_file"], "/bench-id/tls-key_file")

    def test_cleanup_only_exact_container_and_failure_visible(self):
        with patch.object(bench.subprocess, "run", return_value=SimpleNamespace(stdout="")) as run:
            bench.cleanup_container("qdl-endpoint-bench-x")
            self.assertIn("name=^/qdl-endpoint-bench-x$", run.call_args.args[0])
        with patch.object(bench.subprocess, "run", return_value=SimpleNamespace(stdout="still-there")):
            with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
                bench.cleanup_container("qdl-endpoint-bench-x")

    def test_reports_keep_per_binding_and_batch_wall(self):
        p = self.scope.products[0]
        report = {"consumer_id": self.manifest.consumer_id, "results": [{"replica": "query-1", "operation": "snapshot",
            "products": [p.evidence()], "status": "FAIL", "usable_latency": bench.percentiles([])}]}
        with tempfile.TemporaryDirectory() as tmp:
            bench.write_reports(Path(tmp), [report])
            self.assertIn(p.native_symbol, (Path(tmp) / "report.csv").read_text())
            self.assertIn("FAIL", (Path(tmp) / "report.md").read_text())


if __name__ == "__main__":
    unittest.main()
