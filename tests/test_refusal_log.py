"""Astra R6: bounded, payload-free refusal lines at the layer that refused (qdl/runtime/refusal_log.py)."""

from __future__ import annotations

import hashlib
import json
import unittest

from qdl.runtime.refusal_log import RefusalLog, reason_tokens

DETAIL = "HOT_BACKUP_UNAVAILABLE_PRIMARY_BEHIND: HOT_READER_BUSY (price 64123.5 for BNB-USDT-SWAP)"


class ReasonTokenTests(unittest.TestCase):
    def test_typed_codes_only_in_order_deduplicated_and_bounded(self):
        self.assertEqual(reason_tokens(DETAIL), ("HOT_BACKUP_UNAVAILABLE_PRIMARY_BEHIND", "HOT_READER_BUSY"))
        self.assertEqual(reason_tokens("required data exceeds its freshness policy (SESSION_STATE)"),
                         ("SESSION_STATE",))
        self.assertEqual(reason_tokens("plain words, BTC USDT 12_3"), ())
        self.assertEqual(reason_tokens(None), ())
        many = " ".join(f"CODE_{i}" for i in range(10)) + " CODE_0"
        self.assertEqual(reason_tokens(many), ("CODE_0", "CODE_1", "CODE_2", "CODE_3"))
        self.assertEqual(reason_tokens("A_" + "B" * 80), ())


class RefusalLogTests(unittest.TestCase):
    def log(self, **kwargs):
        self.clock = [0.0]
        self.lines: list[dict] = []
        return RefusalLog(clock=lambda: self.clock[0], emit=lambda line: self.lines.append(json.loads(line)),
                          replica="query-1", **kwargs)

    def test_a_line_is_payload_free_and_joins_the_harness_digest(self):
        log = self.log()
        self.assertTrue(log.record(origin="query", code="DATA_NOT_READY", detail=DETAIL, status=503,
                                   retryable=True, request_id="r-1", instrument_uid="u-1",
                                   route="/v2/market-data/{instrument_uid}/snapshot"))
        (line,) = self.lines
        self.assertEqual(line, {
            "event": "qdl_refusal", "origin": "query", "code": "DATA_NOT_READY", "status": 503,
            "retryable": True, "reasons": ["HOT_BACKUP_UNAVAILABLE_PRIMARY_BEHIND", "HOT_READER_BUSY"],
            "detail_sha256": hashlib.sha256(DETAIL.encode()).hexdigest(), "request_id": "r-1",
            "instrument_uid": "u-1", "route": "/v2/market-data/{instrument_uid}/snapshot", "replica": "query-1",
        })
        self.assertNotIn("64123.5", json.dumps(line))

    def test_volume_is_bounded_per_key_and_the_suppressed_count_is_reported_later(self):
        log = self.log(burst=2, window_seconds=10)
        emitted = [log.record(origin="query", code="DATA_NOT_READY", detail=DETAIL) for _ in range(7)]
        self.assertEqual(emitted, [True, True, False, False, False, False, False])
        # Another reason is another key with its own budget.
        self.assertTrue(log.record(origin="query", code="DATA_NOT_READY", detail="SOURCE_BOUNDARY_UNKNOWN: x"))
        self.clock[0] = 10.0
        self.assertTrue(log.record(origin="query", code="DATA_NOT_READY", detail=DETAIL))
        self.assertEqual(self.lines[-1]["suppressed_before"], 5)
        self.assertTrue(log.record(origin="query", code="DATA_NOT_READY", detail=DETAIL))
        self.assertNotIn("suppressed_before", self.lines[-1])
        self.assertEqual(len(self.lines), 5)

    def test_key_cardinality_is_bounded(self):
        log = self.log(burst=1, max_keys=2)
        for index in range(5):
            log.record(origin="query", code="DATA_NOT_READY", detail=f"REASON_{index}")
        self.assertEqual([line["reasons"] for line in self.lines], [["REASON_0"], ["REASON_1"], ["REASON_2"]])
        self.assertEqual(len(log._counts), 3)  # two keys + the shared overflow key
        self.assertEqual(log._counts[("query", "DATA_NOT_READY", "OVERFLOW")], 3)

    def test_a_failing_sink_never_raises(self):
        def broken(_line):
            raise OSError("stderr closed")

        log = RefusalLog(emit=broken, replica="q")
        self.assertFalse(log.record(origin="query", code="X", detail=DETAIL))

    def test_default_sink_is_the_qdl_refusal_warning_logger(self):
        with self.assertLogs("qdl.refusal", level="WARNING") as captured:
            RefusalLog(replica="q").record(origin="query", code="DATA_STALE", detail="x (SESSION_STATE)")
        self.assertEqual(json.loads(captured.records[0].getMessage())["reasons"], ["SESSION_STATE"])


class ExecutionMarkIndexEdgeTests(unittest.TestCase):
    """The private MARK/INDEX edge keeps the view's own reason, which the reference reader normalises away."""

    def test_an_unavailable_view_leaves_its_reason(self):
        from types import SimpleNamespace
        from unittest.mock import patch

        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from qdl.runtime import execution_mark_index as edge
        from qdl.runtime.internal_auth import stable_hmac_signature

        class View:
            async def read(self, **_kwargs):
                return edge.ExecutionMarkIndexRead(None, "MARKET_CACHE_BACKUP_UNAVAILABLE")

        secret = b"s" * 32
        app = FastAPI()
        edge.install_execution_mark_index_read(
            app, gateway=SimpleNamespace(assert_active=lambda epoch=None: 1), view=View(), secret=secret)
        body = json.dumps({"schema": edge._LEGACY_REQUEST_SCHEMA, "instrument_uid": "u-1",
                           "instrument_revision": 1, "source_policy_id": "p", "max_freshness_ms": 2000}).encode()
        lines: list[dict] = []
        log = RefusalLog(emit=lambda line: lines.append(json.loads(line)), replica="q")
        with patch.object(edge, "REFUSALS", log):
            response = TestClient(app).post("/internal/v2/execution/mark-index/latest", content=body,
                                            headers={"X-QDL-Stable-Signature": stable_hmac_signature(secret, body)})
        self.assertEqual(response.status_code, 409)
        (line,) = lines
        self.assertEqual((line["origin"], line["reasons"], line["instrument_uid"]),
                         ("execution_mark_index", ["MARKET_CACHE_BACKUP_UNAVAILABLE"], "u-1"))
        self.assertEqual(line["detail_sha256"], hashlib.sha256(response.json()["detail"].encode()).hexdigest())


if __name__ == "__main__":
    unittest.main()
