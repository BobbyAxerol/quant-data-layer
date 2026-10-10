"""Offline tests for scripts/p184_g2_mark_trace_transit.py (synthetic trace rows, test-only provenance)."""

import json
import os
import tempfile
import unittest
from unittest import mock

from scripts import p184_g2_mark_trace_transit as g2

T0 = 1_791_600_000_000_000_000
MS = 1_000_000


def mark(instrument, start_ms, mark_ms, index_ms, reply_ms, receive_ms=None, outcome="APPLIED", feed="MARK_INDEX_PRICE"):
    return {"canonical_instrument_id": instrument, "feed": feed, "outcome": outcome,
            "writer_started_wall_ns": T0 + start_ms * MS,
            "mark_source_event_time_ns": T0 + mark_ms * MS, "index_source_event_time_ns": T0 + index_ms * MS,
            "redis_reply_wall_ns": T0 + reply_ms * MS,
            "sdk_receive_wall_ns": T0 + (receive_ms if receive_ms is not None else reply_ms - 10) * MS,
            "sdk_receive_basis": "ADAPTER_REFERENCE_RETURN"}


class SummaryTests(unittest.TestCase):
    def test_new_event_transit_counts_only_advancing_events(self):
        rows = [mark("OKX:SWAP:BTC-USDT-SWAP", 0, 0, 0, 400),
                mark("OKX:SWAP:BTC-USDT-SWAP", 500, 0, 0, 900),      # repeat of the same event: age only
                mark("OKX:SWAP:BTC-USDT-SWAP", 1000, 800, 200, 1300)]  # new mark component
        result = g2.summarise(rows, skip_seconds=0)
        okx = result["venues"]["OKX"]
        self.assertEqual(okx["new_event_transit_ms"]["n"], 2)
        self.assertEqual(okx["new_event_transit_ms"]["max"], 500.0)
        self.assertEqual(okx["all_applied_newest_age_ms"]["n"], 3)
        self.assertEqual(okx["all_applied_newest_age_ms"]["max"], 900.0)
        self.assertEqual(okx["index_age_ms"]["max"], 1100.0)
        self.assertEqual(okx["g2_verdict"], "INSIDE")
        self.assertIn("OKX:SWAP:BTC-USDT-SWAP", result["instruments"])

    def test_verdict_uses_max_and_p95_per_venue(self):
        rows = [mark("BINANCE:USD_M:BTCUSDT", i * 1000, i * 1000, i * 1000, i * 1000 + 450) for i in range(19)]
        rows.append(mark("BINANCE:USD_M:BTCUSDT", 20_000, 20_000, 20_000, 21_000))  # max == 1000 is outside (< 1000)
        result = g2.summarise(rows, skip_seconds=0)
        self.assertEqual(result["venues"]["BINANCE"]["new_event_transit_ms"]["max"], 1000.0)
        self.assertEqual(result["venues"]["BINANCE"]["g2_verdict"], "OUTSIDE")
        self.assertIsNone(result["venues"]["BINANCE"]["new_event_transit_ms"]["p99"])  # n < 100

    def test_split_and_invalid_rows_are_reported_not_clamped(self):
        rows = [mark("OKX:SWAP:ETH-USDT-SWAP", 0, 100, 50, 400, receive_ms=350),
                mark("OKX:SWAP:ETH-USDT-SWAP", 10, 900, 900, 800),  # reply before event: clock skew
                dict(mark("OKX:SWAP:ETH-USDT-SWAP", 20, 950, 950, 1200), redis_reply_wall_ns="NOT_MEASURED"),
                mark("OKX:SWAP:ETH-USDT-SWAP", 30, 960, 960, 1300, outcome="REJECTED"),
                mark("OKX:SWAP:ETH-USDT-SWAP", 40, 0, 0, 50, feed="QUOTE")]
        result = g2.summarise(rows, skip_seconds=0)
        okx = result["venues"]["OKX"]
        self.assertEqual(okx["upstream_ms"]["max"], 250.0)
        self.assertEqual(okx["writer_ms"]["max"], 50.0)
        self.assertEqual(result["invalid"], {"OKX:missing_clock": 1, "OKX:negative_transit": 1})
        self.assertEqual(result["outcomes"]["OKX:REJECTED"], 1)

    def test_startup_window_is_skipped(self):
        rows = [mark("OKX:SWAP:BTC-USDT-SWAP", 0, 0, 0, 5000), mark("OKX:SWAP:BTC-USDT-SWAP", 41_000, 41_000, 41_000, 41_300)]
        result = g2.summarise(rows)  # default 40 s start-up skip
        self.assertEqual(result["venues"]["OKX"]["new_event_transit_ms"]["n"], 1)
        self.assertEqual(result["venues"]["OKX"]["new_event_transit_ms"]["max"], 300.0)

    def test_main_reads_trace_files_and_writes_exclusively(self):
        report = {"worker": "worker-x", "part": 0, "stop_reason": "EXPIRY", "recorded": 1, "dropped_at_export": 2,
                  "records": [mark("OKX:SWAP:BTC-USDT-SWAP", 0, 0, 0, 300)]}
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "delivery-worker-x-1-123.json")
            with open(path, "w") as handle:
                json.dump(report, handle)
            out = os.path.join(directory, "g2.json")
            with open(os.devnull, "w") as sink, mock.patch("sys.stdout", sink):
                self.assertEqual(g2.main([os.path.join(directory, "delivery-*.json"), "--skip-seconds", "0",
                                          "--out", out]), 0)
                with self.assertRaises(FileExistsError):
                    g2.main([path, "--out", out])
            with open(out) as handle:
                written = json.load(handle)
        self.assertEqual(written["dropped_at_export_total"], 2)
        self.assertEqual(written["venues"]["OKX"]["g2_verdict"], "INSIDE")


if __name__ == "__main__":
    unittest.main()
