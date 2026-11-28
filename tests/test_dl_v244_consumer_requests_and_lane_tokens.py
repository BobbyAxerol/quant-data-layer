"""DL-BL-2026-11-25-D per-consumer Query request counts, and typed tokens for provider-lane SOURCE_UNAVAILABLE.

- The counter is the DL load gate's per-alpha signal: one bounded, payload-free ``qdl_consumer_requests`` line per
  window, keyed by consumer id and route *template*.
- The token fix closes the 2026-11-25 19:30Z burst: SOURCE_UNAVAILABLE MARK_INDEX_PRICE reached consumers with
  ``reason_tokens=[]`` (the same class as DL-DEF-2026-11-26-G).
"""
from __future__ import annotations

import asyncio
import json
import types
import unittest
from unittest import mock

from qdl.query.service import _provider_lane_failure_detail, _typed_provider_detail
from qdl.runtime.consumer_request_log import OVERFLOW, ConsumerRequestLog
from qdl_sdk.client import _batch_item_problems


class Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class ConsumerRequestLogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock(1_000_020.0)
        self.lines: list[str] = []
        self.log = ConsumerRequestLog(window_seconds=60, max_consumers=2, max_routes=2, clock=self.clock,
                                      emit=self.lines.append)

    def test_one_line_per_window_with_counts_by_consumer_and_route(self):
        for _ in range(3):
            self.log.record("alpha.okx.paper.stable", "/v2/market-data/{instrument_uid}/snapshot")
        self.log.record("alpha.okx.paper.stable", "/v2/market-data/warmup:batch")
        self.log.record("trading-system.paper.stable", "/v2/market-data/reference:batch")
        self.assertEqual(self.lines, [], "nothing is emitted inside a window")
        self.clock.now += 60
        self.log.record("alpha.okx.paper.stable", "/v2/market-data/warmup:batch")
        (line,) = self.lines
        event = json.loads(line)
        self.assertEqual(event["event"], "qdl_consumer_requests")
        self.assertEqual(event["window_s"], 60)
        self.assertEqual(event["total"], 5)
        self.assertEqual(event["consumers"], {
            "alpha.okx.paper.stable": {"/v2/market-data/warmup:batch": 1,
                                       "/v2/market-data/{instrument_uid}/snapshot": 3},
            "trading-system.paper.stable": {"/v2/market-data/reference:batch": 1}})
        self.assertNotIn("instrument_uid\":", line)  # route templates only

    def test_consumers_and_routes_are_bounded(self):
        for index in range(5):
            self.log.record(f"c{index}", "/r")
        for index in range(5):
            self.log.record("c0", f"/r{index}")
        line = self.log.flush()
        consumers = json.loads(line)["consumers"]
        self.assertEqual(set(consumers), {"c0", "c1", OVERFLOW})
        self.assertEqual(consumers[OVERFLOW], {"/r": 3})
        self.assertEqual(set(consumers["c0"]), {"/r", "/r0", OVERFLOW})

    def test_an_empty_window_emits_nothing(self):
        self.assertIsNone(self.log.flush())

    def test_the_router_records_the_authenticated_consumer_and_the_route_template(self):
        import importlib

        router = importlib.import_module("qdl.api_v2.router")  # the module (the package exports an APIRouter)

        access = types.SimpleNamespace(consumer_id="alpha.okx.paper.stable")
        lease = mock.Mock()
        identity = mock.Mock()
        identity.authenticate.return_value = access
        identity.admit.return_value = lease
        request = types.SimpleNamespace(
            app=types.SimpleNamespace(state=types.SimpleNamespace(v2_identity_service=identity)),
            headers={}, state=types.SimpleNamespace(),
            scope={"route": types.SimpleNamespace(path="/v2/market-data/{instrument_uid}/snapshot")})
        credentials = types.SimpleNamespace(scheme="Bearer", credentials="token")
        with mock.patch.object(router, "record_consumer_request") as record:
            self.assertIs(router._data_access(request, credentials, "alpha.okx.paper.stable"), access)
        record.assert_called_once_with("alpha.okx.paper.stable", "/v2/market-data/{instrument_uid}/snapshot")


class ProviderLaneTokenTests(unittest.TestCase):
    @staticmethod
    def tokens(detail: str) -> list[str]:
        item = types.SimpleNamespace(instrument_uid="u", problem=types.SimpleNamespace(
            code="SOURCE_UNAVAILABLE", detail=detail, retryable=True, retry_after_ms=None, request_id="r"))
        return _batch_item_problems("sdk_reference_batch", [item], lambda *_: "MARK_INDEX_PRICE")[
            "item_problems"][0]["reason_tokens"]

    def test_a_lane_timeout_is_typed(self):
        detail = _provider_lane_failure_detail(asyncio.TimeoutError("secret upstream text"))
        self.assertEqual(self.tokens(detail), ["PROVIDER_LANE_FAILED", "PROVIDER_LANE_DEADLINE", "TIMEOUT_ERROR"])
        self.assertNotIn("secret upstream text", detail)

    def test_any_lane_exception_is_typed_by_its_class(self):
        class AdmissionDeadlineExceeded(RuntimeError):
            pass

        self.assertEqual(self.tokens(_provider_lane_failure_detail(AdmissionDeadlineExceeded())),
                         ["PROVIDER_LANE_FAILED", "PROVIDER_LANE_DEADLINE", "ADMISSION_DEADLINE_EXCEEDED"])
        self.assertEqual(self.tokens(_provider_lane_failure_detail(ConnectionResetError())),
                         ["PROVIDER_LANE_FAILED", "CONNECTION_RESET_ERROR"])

    def test_an_untyped_provider_error_gets_its_code_or_an_explicit_untyped_token(self):
        self.assertEqual(self.tokens(_typed_provider_detail("PROVIDER_TIMEOUT", "upstream read timed out")),
                         ["PROVIDER_TIMEOUT"])
        self.assertEqual(self.tokens(_typed_provider_detail(None, "reference provider request failed")),
                         ["PROVIDER_ERROR_UNTYPED"])
        already = "COMPONENT_STALE LIVE_VIEW_STALE mark component too old"
        self.assertEqual(_typed_provider_detail("LIVE_VIEW_STALE", already), already)


if __name__ == "__main__":
    unittest.main()
