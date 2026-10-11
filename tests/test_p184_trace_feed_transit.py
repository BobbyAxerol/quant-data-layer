"""Offline tests for scripts/p184_trace_feed_transit.py (synthetic trace rows, test-only provenance)."""

import contextlib
import io
import json
import os
import tempfile
import unittest

from scripts import p184_trace_feed_transit as transit

T0 = 1_791_600_000_000_000_000
MONO0 = 2_000_000_000_000_000
MS = 1_000_000


def row(feed, instrument, start_ms, event_ms, reply_ms, *, receive_ms=None, outcome="APPLIED",
        basis="SDK_STREAM_RETURN", event_id=None):
    receive_ms = start_ms - 5 if receive_ms is None else receive_ms
    return {"feed": feed, "canonical_instrument_id": instrument, "outcome": outcome, "sdk_receive_basis": basis,
            "writer_started_wall_ns": T0 + start_ms * MS, "redis_reply_wall_ns": T0 + reply_ms * MS,
            "source_event_time_ns": T0 + event_ms * MS if basis == "SDK_STREAM_RETURN" else "NOT_MEASURED",
            "writer_started_monotonic_ns": MONO0 + start_ms * MS, "redis_reply_monotonic_ns": MONO0 + reply_ms * MS,
            "sdk_receive_monotonic_ns": MONO0 + receive_ms * MS,
            "event_id_sha256": event_id or "NOT_MEASURED",
            "envelope_received_at_ns": T0 + (event_ms + 20) * MS, "sdk_receive_wall_ns": T0 + receive_ms * MS}


class TransitTests(unittest.TestCase):
    def test_first_apply_per_event_identity_is_gated_repeats_are_counted(self):
        rows = [row("TRADE", "BINANCE:USD_M:BTCUSDT", 100, 0, 120, event_id="a" * 64),
                row("TRADE", "BINANCE:USD_M:BTCUSDT", 900, 0, 950, event_id="a" * 64),   # same trade again
                row("TRADE", "BINANCE:USD_M:BTCUSDT", 1000, 900, 1010, outcome="REFRESHED"),
                row("TRADE", "BINANCE:USD_M:BTCUSDT", 1100, 1000, 1300, event_id="b" * 64)]
        result = transit.summarise(rows, skip_seconds=0)
        block = result["transit"]["TRADE|BINANCE"]
        self.assertEqual(block["n"], 2)
        self.assertEqual(block["max"], 300.0)
        self.assertEqual(result["excluded_from_transit"]["TRADE|BINANCE|repeated_event_identity"], 1)
        self.assertEqual(result["excluded_from_transit"]["TRADE|BINANCE|outcome_REFRESHED"], 1)
        split = block["split"]
        self.assertEqual(split["venue_to_ingest_ms"]["max"], 20.0)
        self.assertEqual(split["ingest_to_sdk_ms"]["max"], 75.0)  # ingest event+20, SDK return start-5
        self.assertEqual(split["sdk_to_reply_ms"]["max"], 205.0)

    def test_records_without_a_venue_clock_are_counted_not_substituted(self):
        rows = [row("BOOK_DELTA", "OKX:SWAP:BTC-USDT-SWAP", 100, 0, 150, basis="SDK_WARMUP_RETURN"),
                row("BOOK_DELTA", "OKX:SWAP:BTC-USDT-SWAP", 200, 100, 260, event_id="c" * 64)]
        result = transit.summarise(rows, skip_seconds=0)
        self.assertEqual(result["transit"]["BOOK_DELTA|OKX"]["n"], 1)
        self.assertEqual(result["excluded_from_transit"]["BOOK_DELTA|OKX|no_venue_clock_SDK_WARMUP_RETURN"], 1)

    def test_negative_transit_is_invalid_never_clamped(self):
        rows = [row("QUOTE", "OKX:SWAP:ETH-USDT-SWAP", 100, 500, 120, event_id="d" * 64)]
        result = transit.summarise(rows, skip_seconds=0)
        self.assertNotIn("QUOTE|OKX", result["transit"])
        self.assertEqual(result["invalid"]["QUOTE|OKX|negative_transit"], 1)

    def test_verdicts_follow_the_frozen_rows(self):
        quote = [row("QUOTE", "OKX:SWAP:ETH-USDT-SWAP", i * 10, i * 10, i * 10 + 200, event_id=f"{i:064x}")
                 for i in range(1, 120)]
        quote.append(row("QUOTE", "OKX:SWAP:ETH-USDT-SWAP", 5000, 5000, 6000, event_id="e" * 64))  # max 1000 ms
        result = transit.summarise(quote, skip_seconds=0)
        self.assertEqual(result["transit"]["QUOTE|OKX"]["verdict"], "OUTSIDE")  # QUOTE max must stay < 1000
        trade = [dict(r, feed="TRADE") for r in quote]
        self.assertEqual(transit.summarise(trade, skip_seconds=0)["transit"]["TRADE|OKX"]["verdict"], "INSIDE")
        book = [row("BOOK_SNAPSHOT", "BINANCE:USD_M:BTCUSDT", i * 10, i * 10, i * 10 + (1200 if i > 115 else 100),
                    event_id=f"{i:064x}") for i in range(1, 120)]
        block = transit.summarise(book, skip_seconds=0)["transit"]["BOOK_SNAPSHOT|BINANCE"]
        self.assertLessEqual(block["p95"], 500.0)
        self.assertGreater(block["p99"], 1000.0)
        self.assertEqual(block["verdict"], "OUTSIDE")  # p99 bound applies when n >= 100

    def test_g7_segments_use_monotonic_clocks_for_every_reply(self):
        rows = [row("MARK_INDEX_PRICE", "BINANCE:USD_M:BTCUSDT", 100, 0, 104, receive_ms=99,
                    basis="ADAPTER_REFERENCE_RETURN"),
                row("QUOTE", "BINANCE:USD_M:BTCUSDT", 200, 150, 290, receive_ms=80, outcome="DUPLICATE")]
        result = transit.summarise(rows, skip_seconds=0)
        self.assertEqual(result["g7"]["MARK_INDEX_PRICE"]["sdk_to_writer_ms"]["max"], 1.0)
        self.assertEqual(result["g7"]["MARK_INDEX_PRICE"]["writer_to_redis_ms"]["max"], 4.0)
        self.assertEqual(result["g7"]["QUOTE"]["sdk_to_writer_ms"]["max"], 120.0)
        self.assertEqual(result["g7"]["QUOTE"]["verdict"],
                         {"sdk_to_writer_ms": "OUTSIDE", "writer_to_redis_ms": "OUTSIDE"})
        self.assertEqual(result["bar_records"], 0)

    def test_cli_reads_files_and_writes_out_exclusively(self):
        with tempfile.TemporaryDirectory() as directory:
            trace = os.path.join(directory, "delivery-w-1-1.json")
            with open(trace, "w") as handle:
                json.dump({"worker": "w", "part": 0, "stop_reason": "EXPIRY", "recorded": 1, "dropped_at_export": 0,
                           "records": [row("TRADE", "OKX:SWAP:BTC-USDT-SWAP", 100, 50, 130, event_id="f" * 64)]},
                          handle)
            out = os.path.join(directory, "out.json")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(transit.main([trace, "--skip-seconds", "0", "--out", out]), 0)
            with open(out) as handle:
                report = json.load(handle)
            self.assertEqual(report["transit"]["TRADE|OKX"]["max"], 80.0)
            with self.assertRaises(FileExistsError):
                transit.main([trace, "--skip-seconds", "0", "--out", out])

    def test_duplicate_applied_counts_only_canonical_event_ids(self):
        digest = "a" * 64
        rows = [row("TRADE", "OKX:SWAP:BTC-USDT-SWAP", 100, 50, 110, event_id=digest),
                row("TRADE", "OKX:SWAP:BTC-USDT-SWAP", 200, 50, 210, event_id=digest),
                row("TRADE", "OKX:SWAP:BTC-USDT-SWAP", 300, 60, 310),
                row("TRADE", "OKX:SWAP:BTC-USDT-SWAP", 400, 60, 410)]
        result = transit.summarise(rows, skip_seconds=0)
        self.assertEqual(result["duplicate_applied_event_id"], {"TRADE|OKX": 1})
        self.assertEqual(result["excluded_from_transit"]["TRADE|OKX|repeated_event_identity"], 2)

    def test_slim_loader_keeps_only_the_fields_summarise_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "delivery-w-1-2.json")
            record = {**row("TRADE", "OKX:SWAP:X", 100, 50, 110), "mark_source_event_time_ns": 1, "payload_extra": 2}
            with open(path, "w") as handle:
                json.dump({"records": [record], "stop_reason": "EXPIRY", "dropped_at_export": 0}, handle)
            files, records = transit.load([path], fields=transit.FIELDS)
            self.assertEqual(set(records[0]) - set(transit.FIELDS), set())
            self.assertEqual(files[0]["stop_reason"], "EXPIRY")


if __name__ == "__main__":
    unittest.main()
