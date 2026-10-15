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

    def test_gateway_clocks_split_ingest_to_sdk_when_present(self):
        # ingest = event+20 ms = 20; SDK return = start-5 = 95.
        full = dict(row("TRADE", "OKX:SWAP:BTC-USDT-SWAP", 100, 0, 120, event_id="1" * 64),
                    gateway_dispatch_wall_ns=T0 + 30 * MS, gateway_dequeue_wall_ns=T0 + 35 * MS,
                    gateway_send_wall_ns=T0 + 85 * MS)
        send_only = dict(row("TRADE", "OKX:SWAP:BTC-USDT-SWAP", 200, 100, 220, event_id="2" * 64),
                         gateway_dispatch_wall_ns="NOT_MEASURED", gateway_dequeue_wall_ns="NOT_MEASURED",
                         gateway_send_wall_ns=T0 + 190 * MS)
        absent = row("TRADE", "OKX:SWAP:BTC-USDT-SWAP", 300, 200, 320, event_id="3" * 64)
        disordered = dict(row("TRADE", "OKX:SWAP:BTC-USDT-SWAP", 400, 300, 420, event_id="4" * 64),
                          gateway_send_wall_ns=T0 + 399 * MS)  # after the SDK return (395)
        block = transit.summarise([full, send_only, absent, disordered], skip_seconds=0)["transit"]["TRADE|OKX"]
        self.assertEqual(block["n"], 4)
        self.assertEqual(block["gateway_split"], {"measured": 2, "not_measured": 1, "out_of_order": 1})
        split = block["split"]
        self.assertEqual(split["ingest_to_sdk_ms"]["n"], 4, "the unsplit segment keeps every record")
        self.assertEqual(split["ingest_to_gateway_send_ms"]["n"], 2)
        self.assertEqual(split["ingest_to_gateway_send_ms"]["max"], 70.0)  # 190 - 120; the other 85 - 20 = 65
        self.assertEqual(split["gateway_send_to_sdk_ms"]["max"], 10.0)     # 95 - 85; the other 195 - 190 = 5
        # Only the fully stamped record splits the gateway part further.
        self.assertEqual(split["ingest_to_gateway_dispatch_ms"]["n"], 1)
        self.assertEqual(split["ingest_to_gateway_dispatch_ms"]["max"], 10.0)
        self.assertEqual(split["gateway_dispatch_to_dequeue_ms"]["max"], 5.0)
        self.assertEqual(split["gateway_dequeue_to_send_ms"]["max"], 50.0)

    def test_records_without_gateway_clocks_keep_the_previous_split(self):
        block = transit.summarise([row("TRADE", "OKX:SWAP:BTC-USDT-SWAP", 100, 0, 120, event_id="5" * 64)],
                                  skip_seconds=0)["transit"]["TRADE|OKX"]
        self.assertEqual(set(block["split"]), {"venue_to_ingest_ms", "ingest_to_sdk_ms", "sdk_to_reply_ms"})
        self.assertEqual(block["gateway_split"], {"measured": 0, "not_measured": 1, "out_of_order": 0})


if __name__ == "__main__":
    unittest.main()


FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "p184_native")
# Trimmed from a TEST-stack N1 run (r2b6-s-redis-stall, native-trace-binance): the summary is unchanged except for
# emptied checkpoint sessions/hot cohort; the raw file keeps 4 TRADE + 2 QUOTE records of the capped activation.
SUMMARY = os.path.join(FIXTURES, "summary-native-fixture-43-1792024672908395216.json")
RAW = os.path.join(FIXTURES, "delivery-native-fixture-43-1792024672908395216.json")
STARTED, EXPIRES = 1792024466147444849, 1792024672908395216


class NativeWriterTests(unittest.TestCase):
    def test_native_stream_return_carries_the_venue_clock_and_the_basis_is_recorded(self):
        rows = [row("TRADE", "OKX:SWAP:X", 50_000 + i, 49_900 + i, 50_010 + i, event_id=f"{i:064x}") for i in range(3)]
        for r in rows[1:]:
            r["sdk_receive_basis"] = "NATIVE_STREAM_RETURN"
        out = transit.summarise(rows, from_ns=T0, to_ns=T0 + 60_000 * MS)
        self.assertEqual(out["transit"]["TRADE|OKX"]["n"], 3)
        self.assertEqual(out["transit"]["TRADE|OKX"]["stream_basis"],
                         {"NATIVE_STREAM_RETURN": 2, "SDK_STREAM_RETURN": 1})
        other = dict(rows[0], sdk_receive_basis="QUERY_RETURN", event_id_sha256="f" * 64)
        self.assertIn("TRADE|OKX|no_venue_clock_QUERY_RETURN",
                      transit.summarise([other], from_ns=T0, to_ns=T0 + 60_000 * MS)["excluded_from_transit"])

    def test_real_native_raw_record_is_gated(self):
        files, records = transit.load([RAW], fields=transit.FIELDS)
        out = transit.summarise(records, from_ns=STARTED, to_ns=EXPIRES)
        self.assertEqual(out["transit"]["TRADE|BINANCE"]["stream_basis"], {"NATIVE_STREAM_RETURN": 4})
        self.assertEqual(out["transit"]["QUOTE|BINANCE"]["n"], 2)
        kinds = transit.files_by_kind(files + [{"worker": "worker-abc", "stop_reason": "ROTATED", "dropped_at_export": 2}])
        self.assertEqual(kinds["native"]["stop_reasons"], {"CAP": 1})
        self.assertEqual(kinds["python"], {"files": 1, "stop_reasons": {"ROTATED": 1}, "dropped_at_export_total": 2,
                                           "first_start_ns": None, "last_start_ns": None})
        self.assertLessEqual(STARTED, kinds["native"]["first_start_ns"])

    def test_summary_rows_reproduce_the_writer_quantiles_and_merge_by_bucket_sums(self):
        with open(SUMMARY) as handle:
            own = {(r["route"]["feed"], r["route"]["provider"]): r["stream"] for r in
                   json.load(handle)["ack_latency"]["routes"]}
        out = transit.native_summary([SUMMARY], from_ns=STARTED + 1, to_ns=EXPIRES - 1)
        self.assertEqual(out["coverage_gaps"], [])
        metric = own[("TRADE", "BINANCE")]["applied"]["metrics"]["source_event_to_ack"]
        block = out["transit"]["TRADE|BINANCE"]
        self.assertEqual((block["n"], block["p50"], block["p95"], block["p99"], block["max"]),
                         (metric["n"], metric["p50_upper_ms"], metric["p95_upper_ms"], metric["p99_upper_ms"],
                          metric["max_ms"]))
        self.assertEqual(block["verdict"], "OUTSIDE")  # this TEST run stalled Redis for seconds
        g7 = out["g7"]["TRADE"]["writer_to_redis_ms"]
        self.assertEqual(g7["n"], sum(own[("TRADE", "BINANCE")][o]["metrics"]["writer_to_ack"]["n"]
                                      for o in transit.NATIVE_OUTCOMES))
        self.assertEqual(set(out["g7"]["TRADE"]["verdict"]), set(transit.G7_GATES))
        twice = transit.native_summary([SUMMARY, SUMMARY], from_ns=STARTED + 1, to_ns=EXPIRES - 1)
        self.assertEqual(twice["transit"]["TRADE|BINANCE"]["n"], 2 * block["n"])
        self.assertEqual(twice["transit"]["TRADE|BINANCE"]["p95"], block["p95"])
        self.assertEqual(out["window_scope"]["pre_load_seconds"], 0.0)

    def test_summary_coverage_gaps(self):
        late = transit.native_summary([SUMMARY], from_ns=STARTED - 10 * 10**9, to_ns=EXPIRES - 1)
        self.assertTrue(any("does not span the load window" in g for g in late["coverage_gaps"]))
        none = transit.native_summary([], from_ns=STARTED, to_ns=EXPIRES)
        self.assertIn("no native summary overlaps the load window", none["coverage_gaps"])
        orphan = transit.native_summary([SUMMARY], from_ns=STARTED + 1, to_ns=EXPIRES - 1, raw_activations=[
            {"worker": "native-other", "pid": 7, "first_start_ns": STARTED + 5, "last_start_ns": STARTED + 9}])
        self.assertTrue(any("no final summary" in g for g in orphan["coverage_gaps"]))
        own = transit.native_summary([SUMMARY], from_ns=STARTED + 1, to_ns=EXPIRES - 1,
                                     raw_activations=transit.native_raw_activations([RAW]))
        self.assertEqual(own["coverage_gaps"], [])
        later = transit.native_summary([SUMMARY], from_ns=EXPIRES + 1, to_ns=EXPIRES + 10)
        self.assertEqual([a["overlaps_load"] for a in later["activations"]], [False])
        with tempfile.TemporaryDirectory() as tmp:
            with open(SUMMARY) as handle:
                broken = json.load(handle)
            broken["ack_latency"]["routes"][0]["stream"]["applied"]["metrics"]["source_event_to_ack"][
                "sparse_bucket_counts"] = []
            bad = os.path.join(tmp, "summary-native-x-1-1.json")
            with open(bad, "w") as handle:
                json.dump(broken, handle)
            out = transit.native_summary([bad], from_ns=STARTED + 1, to_ns=EXPIRES - 1)
            self.assertTrue(any("bucket counts" in g for g in out["coverage_gaps"]))

    def test_cli_adds_the_native_summary(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()) as stdout:
            out = os.path.join(tmp, "o.json")
            transit.main([RAW, "--from-ns", str(STARTED + 1), "--to-ns", str(EXPIRES - 1),
                          "--native-summary", os.path.join(FIXTURES, "summary-native-*.json"), "--out", out])
        report = json.loads(stdout.getvalue())
        self.assertEqual(report["native_summary"]["coverage_gaps"], [])
        self.assertEqual(report["files_by_kind"]["native"]["stop_reasons"], {"CAP": 1})
