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
# The fixture summary predates observed_end_unix_ns (S100-5): its end is bounded by pinned process-end evidence.
ENDED = [{"started_ns": STARTED - 3 * 10**9, "finished_ns": EXPIRES, "last_seen_ns": EXPIRES - 10**6}]


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
        out = transit.native_summary([SUMMARY], from_ns=STARTED + 1, to_ns=EXPIRES - 1, process_ends=ENDED)
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
        twice = transit.native_summary([SUMMARY, SUMMARY], from_ns=STARTED + 1, to_ns=EXPIRES - 1, process_ends=ENDED)
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
                                     raw_activations=transit.native_raw_activations([RAW]), process_ends=ENDED)
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
                          "--native-summary", os.path.join(FIXTURES, "summary-native-*.json"), "--out", out,
                          "--process-end", f"{STARTED - 1}:{EXPIRES}:{EXPIRES - 10**6}"])
        report = json.loads(stdout.getvalue())
        self.assertEqual(report["native_summary"]["coverage_gaps"], [])
        self.assertEqual(report["files_by_kind"]["native"]["stop_reasons"], {"CAP": 1})


# ---- S100-5 activation end and G10 native apply-decision counters (2026-10-15) ----
from pathlib import Path  # noqa: E402

from scripts import p184_gate_campaign as campaign  # noqa: E402

S = 10**9
# p184-final-campaign-20261015T134656Z: activation start/configured expiry, load window, G11 Docker clocks.
C_STARTED, C_EXPIRES = 1792071877331145845, 1792075472941005697
C_FROM, C_TO = int(1792072125.1225328 * S), int(1792073925.1225328 * S)
C_DOCKER_STARTED, C_DOCKER_FINISHED = "2026-10-15T13:44:34.835485096Z", "2026-10-15T14:19:42.261233777Z"
C_LAST_SEEN_MS = 1792073982213


def native_report(started, expires, *, observed_end=None, integrity=None, worker="native-w", pid=34):
    """A minimal final native summary (no routes) of the trace.rs shape."""
    report = {"schema": transit.NATIVE_SUMMARY_SCHEMA, "final": True, "worker": worker, "pid": pid,
              "started_unix_ns": started, "started_monotonic_ns": 1_000 * S, "expires_unix_ns": expires,
              "ack_latency": {"schema": transit.NATIVE_ACK_SCHEMA, "scope": transit.NATIVE_ACK_SCOPE,
                              "cumulative": True, "bucket_upper_bounds_ms": [1.0, 2.0], "routes": [],
                              "rejected_route_rows": 0}}
    if observed_end is not None:
        report["observed_end_unix_ns"] = observed_end
    if integrity is not None:
        report["apply_integrity"] = {"schema": transit.NATIVE_INTEGRITY_SCHEMA, "rejected_route_units": 0,
                                     "routes": integrity}
    return report


def integrity_route(provider, *, duplicate=0, first_mono=None, last_mono=None, ambiguous=0, replay=0, feed="TRADE"):
    return {"route": {"provider": provider, "symbol": "X", "feed": feed, "interval": None}, "units": 10, "rows": 20,
            "stream_rows": 19, "unconfirmed_rows": 0, "replay_attempted": replay,
            "replay_decisions": {"applied": duplicate, "refreshed": 0, "duplicate_ignored": replay - duplicate},
            "decisions": {"applied": 18, "refreshed": 1, "duplicate_ignored": 1}, "duplicate_applied": duplicate,
            "duplicate_applied_first_monotonic_ns": first_mono, "duplicate_applied_last_monotonic_ns": last_mono,
            "ambiguous_after_unconfirmed": ambiguous, "epochs_total": 2, "epochs_dropped": 0}


def write(tmp, report, name="summary-native-w-34-1.json"):
    path = os.path.join(tmp, name)
    with open(path, "w") as handle:
        json.dump(report, handle)
    return path


class ActivationEndTests(unittest.TestCase):
    def test_configured_expiry_is_never_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = transit.native_summary([write(tmp, native_report(C_STARTED, C_EXPIRES))], from_ns=C_FROM, to_ns=C_TO)
        activation = out["activations"][0]
        self.assertEqual((activation["end_basis"], activation["end_upper_ns"]), ("NOT_MEASURED", None))
        self.assertEqual(activation["expires_basis"], "CONFIGURED_DEADLINE_NOT_COVERAGE")
        self.assertTrue(any("end not measured" in g for g in out["coverage_gaps"]))
        scope = out["window_scope"]
        self.assertIsNone(scope["activation_to_ns"])
        self.assertIsNone(scope["post_load_seconds"])  # not the receipt's 1547.8 s
        self.assertEqual(scope["configured_expires_unix_ns_not_coverage"], C_EXPIRES)
        self.assertEqual(scope["pre_load_seconds"], 247.8)

    def test_campaign_20261015_bound_by_the_g11_process_end(self):
        end = {"started_ns": campaign.docker_time_ns(C_DOCKER_STARTED),
               "finished_ns": campaign.docker_time_ns(C_DOCKER_FINISHED), "last_seen_ns": C_LAST_SEEN_MS * 10**6}
        self.assertEqual(end["finished_ns"], 1792073982261233777)
        with tempfile.TemporaryDirectory() as tmp:
            out = transit.native_summary([write(tmp, native_report(C_STARTED, C_EXPIRES))], from_ns=C_FROM,
                                         to_ns=C_TO, process_ends=[end])
        self.assertEqual(out["coverage_gaps"], [])
        activation = out["activations"][0]
        self.assertEqual((activation["end_basis"], activation["end_upper_ns"], activation["end_lower_ns"]),
                         ("PROCESS_END_EVIDENCE", end["finished_ns"], end["last_seen_ns"]))
        scope = out["window_scope"]
        self.assertEqual(scope["end_basis"], "PROCESS_END_EVIDENCE")
        self.assertEqual(scope["post_load_seconds"], 57.1)
        self.assertEqual((scope["load_seconds"], scope["load_seconds_covered"]), (1800.0, 1800.0))
        self.assertIn("NOT_SEPARABLE", scope["distribution_scope"])

    def test_process_evidence_of_another_run_does_not_apply(self):
        later_run = {"started_ns": C_STARTED + 10 * S, "finished_ns": C_TO + 60 * S, "last_seen_ns": None}
        earlier_run = {"started_ns": None, "finished_ns": C_STARTED - S, "last_seen_ns": None}
        report = native_report(C_STARTED, C_EXPIRES)
        self.assertEqual(transit.activation_end(report, [later_run, earlier_run])["basis"], "NOT_MEASURED")
        self.assertEqual(transit.parse_process_end(":5:")["finished_ns"], 5)
        with self.assertRaises(ValueError):
            transit.parse_process_end("9:5")

    def test_summary_observed_end_wins_is_capped_by_expiry_and_can_show_an_early_stop(self):
        stopped = C_TO - 100 * S  # the process ended inside the load: a gap, whatever the expiry says
        with tempfile.TemporaryDirectory() as tmp:
            out = transit.native_summary([write(tmp, native_report(C_STARTED, C_EXPIRES, observed_end=stopped))],
                                         from_ns=C_FROM, to_ns=C_TO)
        self.assertEqual(out["activations"][0]["end_basis"], "SUMMARY_OBSERVED_END")
        self.assertTrue(any("does not span the load window" in g for g in out["coverage_gaps"]))
        self.assertEqual(out["window_scope"]["load_seconds_covered"], 1700.0)
        late = transit.activation_end(native_report(C_STARTED, C_EXPIRES, observed_end=C_EXPIRES + S))
        self.assertEqual(late["end_upper_ns"], C_EXPIRES)  # aggregates stop counting at the expiry

    def test_g11_process_end_reads_the_probe_observations(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "g11").mkdir()
            (d / "g11" / "g11-observations.jsonl").write_text("\n".join(json.dumps(x) for x in (
                {"kind": "armed", "writer": {"started": C_DOCKER_STARTED, "finished": "0001-01-01T00:00:00Z"}},
                {"kind": "restart", "last_old_seen_ms": C_LAST_SEEN_MS,
                 "new": {"finished": C_DOCKER_FINISHED, "started": "2026-10-15T14:19:42.382229525Z"}})) + "\n")
            end = campaign.g11_process_end(d)
            self.assertEqual(end["finished_ns"], 1792073982261233777)
            self.assertEqual(end["started_ns"], 1792071874835485096)
            self.assertEqual(end["last_seen_ns"], C_LAST_SEEN_MS * 10**6)
            (d / "g11" / "g11-observations.jsonl").write_text(json.dumps({"kind": "armed", "writer": {}}) + "\n")
            self.assertIsNone(campaign.g11_process_end(d))


class NativeG10Tests(unittest.TestCase):
    END = C_TO + 60 * S

    def analyse(self, routes, **report):
        with tempfile.TemporaryDirectory() as tmp:
            return transit.native_summary([write(tmp, native_report(C_STARTED, C_EXPIRES, observed_end=self.END,
                                                                    integrity=routes, **report))],
                                          from_ns=C_FROM, to_ns=C_TO)

    def score(self, native):
        logs = {"trade_gap": 0, "trade_replay_expired": 0, "trade_overflow": 0, "log_polls": 10, "log_unreadable": 0}
        return campaign.score_g10(logs, {}, [], sample_scope="native raw sample spans 30 s",
                                  native_g10=native.get("g10"), native_gaps=native.get("coverage_gaps"))

    def mono(self, unix_ns):  # inverse of the summary anchor (started_monotonic_ns = 1000 s)
        return unix_ns - C_STARTED + 1_000 * S

    def test_zero_duplicates_applied_over_a_spanning_activation_passes_and_replays_are_not_duplicates(self):
        native = self.analyse([integrity_route("BINANCE", replay=7), integrity_route("OKX")])
        self.assertEqual(native["coverage_gaps"], [])
        v, counters, why = self.score(native)
        self.assertEqual((v, why), ("PASS", []))
        self.assertEqual(counters["duplicates_applied"], 0)
        self.assertEqual(counters["replay_attempted"], {"TRADE|BINANCE": 7, "TRADE|OKX": 0})
        self.assertIn("native apply-decision counters", counters["duplicates_applied_scope"])

    def test_a_duplicate_applied_inside_the_load_fails_and_outside_it_does_not_count(self):
        inside = self.analyse([integrity_route("BINANCE", duplicate=1, replay=1, first_mono=self.mono(C_FROM + S),
                                               last_mono=self.mono(C_FROM + S)), integrity_route("OKX")])
        v, counters, _ = self.score(inside)
        self.assertEqual((v, counters["duplicates_applied"]), ("FAIL", 1))
        before = self.analyse([integrity_route("BINANCE", duplicate=2, replay=2, first_mono=self.mono(C_STARTED + S),
                                               last_mono=self.mono(C_FROM - S)), integrity_route("OKX")])
        self.assertEqual(before["g10"]["per_feed_venue"]["TRADE|BINANCE"]["duplicate_applied_in_window"], 0)
        self.assertEqual(self.score(before)[0], "PASS")
        unknown = self.analyse([integrity_route("BINANCE", duplicate=1, replay=1), integrity_route("OKX")])
        self.assertEqual(self.score(unknown)[0], "FAIL")  # no clock: counted as inside (conservative)

    def test_missing_counters_ambiguous_rows_and_uncovered_activation_are_not_measured(self):
        missing = self.analyse(None)
        v, counters, why = self.score(missing)
        self.assertEqual(v, "NOT_MEASURED")
        self.assertTrue(any("sample only" in w for w in why))
        ambiguous = self.analyse([integrity_route("BINANCE", ambiguous=2), integrity_route("OKX")])
        v, _, why = self.score(ambiguous)
        self.assertEqual(v, "NOT_MEASURED")
        self.assertTrue(any("lost apply reply" in w for w in why))
        one_venue = self.analyse([integrity_route("OKX")])
        self.assertTrue(any("BINANCE" in w for w in self.score(one_venue)[2]))
        with tempfile.TemporaryDirectory() as tmp:  # end unknown: the counters cannot PASS
            native = transit.native_summary([write(tmp, native_report(C_STARTED, C_EXPIRES, integrity=[
                integrity_route("BINANCE"), integrity_route("OKX")]))], from_ns=C_FROM, to_ns=C_TO)
        v, _, why = self.score(native)
        self.assertEqual(v, "NOT_MEASURED")
        self.assertTrue(any("end not measured" in w for w in why))
