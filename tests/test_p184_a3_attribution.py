"""P18.4 stage-100 amendment A3 (owner 2026-10-18 ~10:10Z): external attribution of G4 samples and G9 losses.

Negatives first: a missing probe, a different probe event, an internal segment over its limit, retention overflow,
an unretained maximum, an incomplete lane timeline, a recovery over budget and an EXTERNAL count over the cap all stay
UNKNOWN / FAIL. Parity: every stored scorecard (053803Z / 135719Z / 074624Z / 092255Z) is reproduced byte-identically
when A3 is not recorded, and a recorded-but-mismatched A3 is not applied. Host-runnable (stdlib only).
"""
from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import p184_a3_attribution as a3
from scripts import p184_a3_evidence as evidence
from scripts import p184_gate_campaign as campaign
from tests.p184_receipt_support import as_recorded, mirror

STATE = Path.home() / ".local/state/qdl-v2"
RECEIPTS = [STATE / name for name in ("p184-final-campaign-20261018T053803Z", "p184-final-campaign-20261017T135719Z",
                                      "p184-final-campaign-20261018T074624Z", "p184-final-campaign-20261018T092255Z")]
S = 1_000_000_000
MS = 1_000_000
T0 = 1_792_315_975 * S

LIMITS = {"probe_healthy_ms": 150.0, "lane_late_margin_ms": 50.0, "reset_pad_s": 2.0, "episode_merge_s": 60.0,
          "recovery_budget_s": 60.0, "max_external_fraction": 0.01, "max_external_episodes_per_venue": 3,
          "ingestor_runq_wait_ms_max_thread": 50.0, "rx_persist_samples": 1, "clock_bracket_s": 60.0,
          "host_clock_bound_ms": 10.0, "core_raw_age_ms_max": 500.0, "gateway_dispatch_lag_ms_max": 500.0,
          "gateway_lag_bounds_ms": [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000],
          "g4_internal_segment_ms_by_row": {"book_snapshot|OKX|": {"receive_to_canonical_ms": 300.0,
                                                                   "canonical_to_consumer_ms": 200.0}}}
SEGMENTS = LIMITS["g4_internal_segment_ms_by_row"]["book_snapshot|OKX|"]


def local(runtime="OKX", *, start=T0 - 30 * S, end=T0 + 30 * S, throttled_at=None, runq=5.0, rx_at=None,
          clock_bound=0.2):
    samples = []
    t = start
    while t <= end:
        samples.append({"kind": "local", "runtime": runtime, "at_ns": t,
                        "d_nr_throttled": 3 if throttled_at is not None and abs(t - throttled_at) < S else 0,
                        "runq_wait_ms_max_thread": runq})
        t += S
    tcp = [json.dumps({"kind": "tcp", "at_ns": x, "rx_persist_samples_max":
                       3 if rx_at is not None and abs(x - rx_at) < S else 0})
           for x in range(start, end + 1, S)]
    clock = [{"kind": "clock", "at_ns": x, "clock": {"bound_ms": clock_bound}} for x in range(start, end + 1, 30 * S)]
    return {"local": samples, "clock": clock, "tcp": {runtime: tcp}}


def fresh(lag_ms, ts_ms=T0 // MS):
    return {"r": T0 + int(lag_ms * MS), "ts_ms": (ts_ms,)}


def okx_book_event(*, lag_ms=212.0, canonical_ms=95.0, consumer_ms=40.0, seq=99919003072, gen=67019, offset=1):
    source = T0
    received = source + int(lag_ms * MS)
    produced_ms = received / MS + canonical_ms
    seen = int(produced_ms * MS + consumer_ms * MS)
    return {"topic": "md.canonical.v2", "partition": 5, "offset": offset, "feed": "book_snapshot",
            "source_event_ns": source, "received_at_ns": received, "producer_timestamp_ms": produced_ms,
            "observer_receipt_wall_ns": seen, "visibility_ms": round((seen - source) / MS, 3),
            "identity": {"venue": "OKX", "native_symbol": "BTC-USD-SWAP",
                         "source_session_id": f"okx-public-001-{gen}-1792314743906872432",
                         "source_sequence": f"g{gen}:{seq}"}}


def classify(event, probes=None, episodes_=(), loc=None, row_limit=300.7, segments=SEGMENTS):
    return a3.classify_g4_event(event, row_limit_ms=row_limit, probes=probes or {}, episodes_=list(episodes_),
                                local=loc or local(), limits=LIMITS, segments=segments)


class IdentityTests(unittest.TestCase):
    def test_lane_and_generation_from_the_canonical_session_id(self):
        self.assertEqual(a3.lane_of_session("okx-public-001-67019-1792314743906872432"), ("okx-public-001", 67019))
        self.assertEqual(a3.lane_of_session("binance-USDM-public-quote-001-64-1792294904139856629"),
                         ("binance-public-quote-001", 64))
        self.assertEqual(a3.lane_of_session("binance-USDM-market-markindex-001-50-1792294904110969669"),
                         ("binance-market-markindex-001", 50))
        self.assertIsNone(a3.lane_of_session("garbage"))

    def test_venue_ids_follow_the_canonical_source_sequence(self):
        self.assertEqual(a3.venue_id(okx_book_event()), ("books", 99919003072))
        quote = {"feed": "quote", "identity": {"venue": "BINANCE", "source_sequence": "11851275276076"}}
        mark = {"feed": "mark_index_price",
                "identity": {"venue": "BINANCE", "source_sequence": "1792315513000:1792315513000:1:2:d18b"}}
        depth = {"feed": "book_delta", "identity": {"venue": "BINANCE", "source_sequence": "g182:11851275278740"}}
        self.assertEqual(a3.venue_id(quote), ("bookTicker", 11851275276076))
        self.assertEqual(a3.venue_id(mark), ("markPrice", 1792315513000))
        self.assertEqual(a3.venue_id(depth), ("depth", 11851275278740))
        self.assertIsNone(a3.venue_id({"feed": "quote", "identity": {"venue": "OKX", "source_sequence": "1"}}))

    def test_inverse_rows_use_the_pooled_okx_segment_limits(self):
        self.assertEqual(a3.segment_limits_for("book_snapshot|OKX_INVERSE|", LIMITS["g4_internal_segment_ms_by_row"]),
                         SEGMENTS)
        self.assertIsNone(a3.segment_limits_for("trade|OKX|", LIMITS["g4_internal_segment_ms_by_row"]))


def progress(at, runtime, lanes):
    return {"role": "ingestor", "event": "qdl_native_raw_ingestor_progress", "at_ns": at, "runtime": runtime,
            "container": "c", "lanes": lanes}


def lane(name, gen, frames=10, age=5.0, gap=50.0):
    return {"lane": name, "generation": gen, "frames": frames, "last_frame_age_ms": age,
            "read_gap_ms": {"max": gap, "max_at_ns": 0}}


def binance_timeline(*, recover=True, other_reading=True):
    trade, book = "binance-public-trade-001", "binance-public-book-001"
    rows = [progress(T0 - 10 * S, "BINANCE", [lane(trade, 56, age=2000.0), lane(book, 182)]),
            {"role": "ingestor", "event": "qdl_native_session_heartbeat_failed", "at_ns": T0, "runtime": "BINANCE",
             "generation": 56, "container": "c"},
            progress(T0 + 2 * S, "BINANCE", [lane(trade, 57, frames=0), lane(book, 182, frames=10 if other_reading
                                                                             else 0)]),
            {"role": "ingestor", "event": "qdl_native_session_heartbeat_failed", "at_ns": T0 + 10 * S,
             "runtime": "BINANCE", "generation": 57, "container": "c"},
            progress(T0 + 12 * S, "BINANCE", [lane(trade, 57, frames=0), lane(book, 182)])]
    if recover:
        rows.append(progress(T0 + 22 * S, "BINANCE", [lane(trade, 58, frames=40), lane(book, 182)]))
    for t in range(T0 - 30 * S, T0 + 120 * S, 3 * S):
        rows.append({"role": "core", "event": "qdl_realtime_core_progress", "at_ns": t, "container": "core1",
                     "raw_age_websocket_ms": {"max": 90.0}})
    buckets = [0] * 13
    for t in range(T0 - 30 * S, T0 + 120 * S, 10 * S):
        buckets = [b + (5 if i == 5 else 0) for i, b in enumerate(buckets)]
        rows.append({"role": "gateway", "event": "qdl_kn_gateway_metrics", "at_ns": t, "container": "gw1",
                     "dispatch_lag_buckets": list(buckets)})
    return sorted(rows, key=lambda r: r["at_ns"])


class EpisodeTests(unittest.TestCase):
    def test_binance_faults_join_one_episode_by_generation(self):
        (episode,) = a3.episodes(binance_timeline())
        self.assertEqual(episode["lane"], "binance-public-trade-001")
        self.assertEqual([f["generation"] for f in episode["faults"]], [56, 57])
        self.assertEqual(episode["last_good_ns"], T0 - 12 * S)
        self.assertEqual(episode["first_new_bounds_ns"], [T0 + 12 * S, T0 + 22 * S])
        self.assertTrue(episode["complete"])

    def test_missing_recovery_or_silent_ingestor_makes_the_episode_incomplete(self):
        (episode,) = a3.episodes(binance_timeline(recover=False))
        self.assertIn("first frame of a newer generation", episode["missing"])
        (episode,) = a3.episodes(binance_timeline(other_reading=False))
        self.assertIn("another lane of the ingestor reading at the fault", episode["missing"])

    def test_ambiguous_generation_is_not_assigned(self):
        rows = [progress(T0 - S, "BINANCE", [lane("binance-market-bar-001", 50), lane("binance-market-markindex-001",
                                                                                      50)]),
                {"role": "ingestor", "event": "qdl_native_session_disconnected", "at_ns": T0, "runtime": "BINANCE",
                 "generation": 50, "container": "c", "error": "IO error: Connection reset by peer (os error 104)"}]
        (episode,) = a3.episodes(rows)
        self.assertIsNone(episode["lane"])
        self.assertFalse(episode["complete"])


class LocalCheckTests(unittest.TestCase):
    def test_clean_seconds_pass_and_each_local_signal_fails(self):
        self.assertEqual(a3.local_check(local(), "OKX", T0, T0 + S, LIMITS), (True, []))
        for name, loc in (("throttled", local(throttled_at=T0)), ("runq", local(runq=80.0)),
                          ("rx queued", local(rx_at=T0)), ("clock over", local(clock_bound=25.0)),
                          ("no samples", {"local": [], "clock": [], "tcp": {}})):
            with self.subTest(name=name):
                self.assertFalse(a3.local_check(loc, "OKX", T0, T0 + S, LIMITS)[0])


class G4EventTests(unittest.TestCase):
    def test_same_event_fresh_lane_on_time_is_session_upstream(self):
        event = okx_book_event()
        out = classify(event, probes={("books", "BTC-USD-SWAP", 99919003072): fresh(33)})
        self.assertEqual(out["attribution"], "EXTERNAL_SESSION_UPSTREAM", out)

    def test_a_late_fresh_probe_is_not_evidence_in_v1(self):
        # Review finding 1: venue 20 ms, internal 285 ms, a slow probe at 160 ms must not make it EXTERNAL.
        self.assertEqual(classify(okx_book_event(), probes={("books", "BTC-USD-SWAP", 99919003072): fresh(205)})
                         ["attribution"], "UNKNOWN")
        internal = okx_book_event(lag_ms=20.0, canonical_ms=200.0, consumer_ms=85.0)
        self.assertEqual(classify(internal, probes={("books", "BTC-USD-SWAP", 99919003072): fresh(160)},
                                  row_limit=300.0)["attribution"], "UNKNOWN")

    def test_same_id_with_a_different_venue_timestamp_is_not_the_same_event(self):
        probes = {("books", "BTC-USD-SWAP", 99919003072): fresh(33, ts_ms=T0 // MS - 400)}
        out = classify(okx_book_event(), probes=probes)
        self.assertEqual(out["attribution"], "UNKNOWN")
        self.assertTrue(any("different venue timestamp" in r for r in out["reasons"]))

    def test_probe_missing_or_different_event_is_unknown(self):
        self.assertEqual(classify(okx_book_event())["attribution"], "UNKNOWN")
        different = {("books", "BTC-USD-SWAP", 99919003071): fresh(30)}  # healthy, but not the same event
        out = classify(okx_book_event(), probes=different)
        self.assertEqual(out["attribution"], "UNKNOWN")
        self.assertTrue(any("supporting only" in r for r in out["reasons"]))

    def test_internal_segment_over_limit_is_internal_even_with_a_healthy_probe(self):
        probes = {("books", "BTC-USD-SWAP", 99919003072): fresh(33)}
        for event in (okx_book_event(canonical_ms=310.0), okx_book_event(consumer_ms=210.0),
                      okx_book_event(lag_ms=100.0, canonical_ms=250.0, consumer_ms=150.0)):  # total 400 > 300.7
            with self.subTest(event=event["visibility_ms"]):
                self.assertEqual(classify(event, probes=probes)["attribution"], "INTERNAL_SEGMENT_OVER_LIMIT")

    def test_local_signals_or_missing_segment_limits_keep_it_unknown(self):
        probes = {("books", "BTC-USD-SWAP", 99919003072): fresh(33)}
        self.assertEqual(classify(okx_book_event(), probes=probes, loc=local(rx_at=T0))["attribution"], "UNKNOWN")
        self.assertEqual(classify(okx_book_event(), probes=probes, segments=None)["attribution"], "UNKNOWN")

    def test_event_in_flight_across_a_complete_lane_episode_is_venue_reset(self):
        trade = {"topic": "t", "partition": 0, "offset": 9, "feed": "trade", "source_event_ns": T0 - S,
                 "received_at_ns": T0 + 23 * S, "producer_timestamp_ms": (T0 + 23 * S) / MS + 20,
                 "observer_receipt_wall_ns": T0 + 23 * S + 50 * MS, "visibility_ms": 24050.0,
                 "identity": {"venue": "BINANCE", "native_symbol": "BTCUSDT",
                              "source_session_id": "binance-USDM-public-trade-001-58-1792294904108326893",
                              "source_sequence": "8867600378"}}
        eps = a3.episodes(binance_timeline())
        loc = local("BINANCE", end=T0 + 60 * S)
        out = a3.classify_g4_event(trade, row_limit_ms=30000.0, probes={}, episodes_=eps, local=loc, limits=LIMITS,
                                   segments={"receive_to_canonical_ms": 300.0, "canonical_to_consumer_ms": 200.0})
        self.assertEqual(out["attribution"], "EXTERNAL_VENUE_RESET", out)
        self.assertEqual(out["episode_recovery_s"], 34.0)  # from the last good frame (T0 - 12 s) to T0 + 22 s
        incomplete = a3.episodes(binance_timeline(recover=False))
        out = a3.classify_g4_event(trade, row_limit_ms=30000.0, probes={}, episodes_=incomplete, local=loc,
                                   limits=LIMITS, segments={"receive_to_canonical_ms": 300.0,
                                                            "canonical_to_consumer_ms": 200.0})
        self.assertEqual(out["attribution"], "UNKNOWN")


class G4WindowTests(unittest.TestCase):
    def retention(self, events, *, overflow=0, threshold=300.7, max_event=None):
        return {"threshold_ms": threshold, "cap": 256, "events": events, "overflow": overflow,
                "max_event": max_event or (max(events, key=lambda e: e["visibility_ms"]) if events else None)}

    def rescue(self, retention, probes):
        return a3.rescue_g4_window(retention, row_limit_ms=300.7, n_admitted=1000, probes=probes, episodes_=[],
                                   local=local(), limits=LIMITS, segments=SEGMENTS)

    def test_all_units_external_rescues_and_any_gap_does_not(self):
        e1, e2 = okx_book_event(seq=1, offset=1), okx_book_event(seq=2, offset=2, lag_ms=230.0)
        good = {("books", "BTC-USD-SWAP", 1): fresh(30), ("books", "BTC-USD-SWAP", 2): fresh(31)}
        self.assertTrue(self.rescue(self.retention([e1, e2]), good)["rescued"])
        partial = {("books", "BTC-USD-SWAP", 1): fresh(30)}
        out = self.rescue(self.retention([e1, e2]), partial)
        self.assertEqual((out["rescued"], out["status"], out["external"]), (False, "NOT_ALL_EXTERNAL", 1))

    def test_overflow_unretained_maximum_threshold_and_no_retention_are_incomplete(self):
        e1 = okx_book_event(seq=1)
        bigger = okx_book_event(seq=9, offset=9, lag_ms=400.0)
        cases = {"overflow": self.retention([e1], overflow=3),
                 "max not retained": self.retention([e1], max_event=bigger),
                 "threshold above limit": self.retention([e1], threshold=400.0), "none": None}
        for name, retention in cases.items():
            with self.subTest(case=name):
                out = self.rescue(retention, {("books", "BTC-USD-SWAP", 1): fresh(30)})
                self.assertEqual((out["rescued"], out["status"]), (False, "INCOMPLETE"))


class ScoreG4Tests(unittest.TestCase):
    TABLE = {"book_snapshot|OKX|": {"candidate_p95_limit_ms": 300.0}}

    def windows(self, p95=320.0, lower=319.0):
        stats = {"n": 500, "p95": p95, "p95_bucket_lower": lower, "max": 400.0, "percentile_basis": "BUCKET_UPPER_BOUND"}
        return [{"summary": {"book_snapshot|OKX|": {"visibility_ms": stats}}, "inverse_rows": {}}]

    def test_rescue_changes_only_the_rescued_row_and_a_failed_rescue_keeps_fail(self):
        state, rows, _ = campaign.score_g4(self.windows(), self.TABLE)
        self.assertEqual((state, rows[0]["verdict"]), ("FAIL", "FAIL"))
        yes = lambda key, failing, limit, n: {"rescued": True, "failing_windows": failing}  # noqa: E731
        no = lambda key, failing, limit, n: {"rescued": False, "failing_windows": failing}  # noqa: E731
        self.assertEqual(campaign.score_g4(self.windows(), self.TABLE, a3_rescue=yes)[0], "PASS")
        state, rows, _ = campaign.score_g4(self.windows(), self.TABLE, a3_rescue=no)
        self.assertEqual((state, rows[0]["verdict"], rows[0]["a3"]["rescued"]), ("FAIL", "FAIL", False))
        clean = self.windows(p95=250.0, lower=249.0)
        self.assertNotIn("a3", campaign.score_g4(clean, self.TABLE, a3_rescue=yes)[1][0])  # never called when inside

    def test_the_row_cap_fails_a_rescue_of_every_window(self):
        ctx = campaign.A3Context.__new__(campaign.A3Context)
        ctx.a3, ctx.limits, ctx.missing, ctx.episodes = a3, LIMITS, [], []
        ctx.local, ctx.probes = local(), {0: {("books", "BTC-USD-SWAP", s): fresh(30) for s in range(20)}}
        events = [okx_book_event(seq=s, offset=s) for s in range(20)]
        ctx.aggregates = {0: {"rows": {"book_snapshot|OKX|": {"exceedances": {
            "threshold_ms": 300.0, "events": events, "overflow": 0, "max_event": events[0]}}}}}
        out = ctx.rescue("book_snapshot|OKX|", [0], 300.0, [1000])  # 20 EXTERNAL > 1 % of 1000
        self.assertFalse(out["rescued"])
        self.assertFalse(out["cap_ok"])
        out = ctx.rescue("book_snapshot|OKX|", [0], 300.0, [5000])  # 20 <= 50
        self.assertTrue(out["rescued"], out)
        ctx.missing = ["a3-local"]
        self.assertFalse(ctx.rescue("book_snapshot|OKX|", [0], 300.0, [5000])["rescued"])


def heartbeat(at, not_ready, demanded=64):
    return {"kind": "heartbeat", "at_ns": at, "key_tail": "k",
            "sample": {"readable": True, "age_s": 3.0, "purpose": True, "last_seen_unix": at / S,
                       "worker_liveness": {"0": "LIVE", "1": "LIVE"},
                       "demanded_v2_slices": demanded, "session_ready_v2_slices": demanded, "not_ready": not_ready}}


class G9Tests(unittest.TestCase):
    ROW = {"provider": "BINANCE", "symbol": "BTCUSDT", "feed": "TRADE", "interval": None,
           "reason_code": "TRADE_SESSION_UNVERIFIED", "component_age_ms": 10184}
    LANES = {("BINANCE", "trade", "BTCUSDT"): {"binance-public-trade-001"}}

    def samples(self, restore=True):
        rows = [heartbeat(T0 - 20 * S, []), heartbeat(T0 + 10 * S, [self.ROW])]
        rows.append(heartbeat(T0 + 40 * S, [] if restore else [self.ROW]))
        return rows

    def classify(self, samples, timeline, lanes=None, loc=None):
        (loss,) = a3.g9_loss_events(samples)
        return a3.classify_g9_loss(loss, lanes=self.LANES if lanes is None else lanes, episodes_=a3.episodes(timeline),
                                   local=loc or local("BINANCE", start=T0 - 60 * S, end=T0 + 120 * S),
                                   timeline=timeline, limits=LIMITS, samples=samples)

    def test_a_loss_at_a_complete_episode_restored_in_budget_is_external(self):
        out = self.classify(self.samples(), binance_timeline())
        self.assertEqual(out["attribution"], "EXTERNAL_SESSION_INTERRUPTION", out)
        self.assertEqual(out["recovery_upper_s"], 52.0)  # from the outage start (last good frame T0 - 12 s)

    def test_negatives_stay_unknown_or_over_budget(self):
        self.assertEqual(self.classify(self.samples(restore=False), binance_timeline())["attribution"], "UNKNOWN")
        self.assertEqual(self.classify(self.samples(), binance_timeline(recover=False))["attribution"], "UNKNOWN")
        self.assertEqual(self.classify(self.samples(), binance_timeline(), lanes={})["attribution"], "UNKNOWN")
        self.assertEqual(self.classify(self.samples(), binance_timeline(),
                                       loc=local("BINANCE", end=T0 + 120 * S, throttled_at=T0))["attribution"],
                         "UNKNOWN")
        quiet = [r for r in binance_timeline() if r.get("event") != "qdl_native_session_heartbeat_failed"]
        self.assertEqual(self.classify(self.samples(), quiet)["attribution"], "UNKNOWN")
        late = [heartbeat(T0 - 20 * S, []), heartbeat(T0 + 10 * S, [self.ROW]), heartbeat(T0 + 40 * S, [self.ROW]),
                heartbeat(T0 + 70 * S, [])]
        self.assertEqual(self.classify(late, binance_timeline())["attribution"], "RECOVERY_OVER_BUDGET")

    def test_rescore_cap_and_episode_count(self):
        ext = {"attribution": "EXTERNAL_SESSION_INTERRUPTION", "episode_first_fault_ns": T0}
        eps = [{"runtime": "BINANCE", "first_fault_ns": T0}]
        ok = a3.rescore_g9([ext] * 10, demanded=64, readable_samples=60, limits=LIMITS, episodes_=eps)
        self.assertEqual((ok["cap_ok"], ok["losses_after_a3"]), (True, 0))
        over = a3.rescore_g9([ext] * 40, demanded=64, readable_samples=60, limits=LIMITS, episodes_=eps)
        self.assertEqual((over["cap_ok"], over["losses_after_a3"]), (False, 40))
        mixed = a3.rescore_g9([ext, {"attribution": "UNKNOWN"}], demanded=64, readable_samples=60, limits=LIMITS,
                              episodes_=eps)
        self.assertEqual(mixed["losses_after_a3"], 1)


class EvidenceTests(unittest.TestCase):
    def test_log_filters_keep_whitelisted_fields_only(self):
        progress_line = ('2026-10-18T09:33:03.727492361Z {"event":"qdl_native_raw_ingestor_progress","runtime":'
                         '"BINANCE","interval_s":10.0,"lanes":[{"lane":"binance-public-trade-001","generation":57,'
                         '"frames":45,"last_frame_age_ms":2722.1,"read_gap_ms":{"max":233.8,"max_at_ns":5,"mean":1},'
                         '"produce_ms":{"max":10.5,"max_at_ns":6,"n":45},"secret":"x"}]}')
        out = evidence.filter_line("ingestor", "c", progress_line)
        self.assertEqual(out["at_ns"], 1792315983727492361)
        self.assertNotIn("secret", out["lanes"][0])
        self.assertEqual(out["lanes"][0]["read_gap_ms"], {"max": 233.8, "max_at_ns": 5})
        fault = evidence.filter_line("ingestor", "c", '2026-10-18T09:33:12.961239821Z {"attempt":1,"error":"IO error: '
                                     'Connection reset by peer (os error 104)","event":"qdl_native_session_'
                                     'disconnected","generation":64,"runtime":"BINANCE"}')
        self.assertEqual((fault["event"], fault["generation"]), ("qdl_native_session_disconnected", 64))
        self.assertIsNone(evidence.filter_line("ingestor", "c", "2026-10-18T09:33:12.9Z plain text"))
        gw = evidence.filter_line("gateway", "g", '2026-10-18T09:33:12.9Z {"event":"qdl_kn_gateway_metrics",'
                                  '"dispatch_lag_ms":{"buckets":[0,1],"n":1},"bundle_sha256":"zz"}')
        self.assertEqual(gw["dispatch_lag_buckets"], [0, 1])
        self.assertNotIn("bundle_sha256", gw)

    def test_proc_parsers(self):
        self.assertEqual(evidence.parse_cpu_stat("usage_usec 10\nnr_throttled 2\n"),
                         {"usage_usec": 10, "nr_throttled": 2})
        self.assertEqual(evidence.parse_schedstat("123 456 7\n"), 456)
        self.assertIsNone(evidence.parse_schedstat("x"))

    def test_gateway_and_core_dl_path_checks(self):
        timeline = binance_timeline()
        self.assertEqual(a3.dl_path_check(timeline, T0, T0 + 30 * S, LIMITS), (True, []))
        slow = [dict(r, dispatch_lag_buckets=[b + (2 if i == 12 and r["at_ns"] > T0 else 0)
                                              for i, b in enumerate(r["dispatch_lag_buckets"])])
                if r.get("role") == "gateway" else r for r in timeline]
        self.assertFalse(a3.dl_path_check(slow, T0, T0 + 30 * S, LIMITS)[0])
        core = [dict(r, raw_age_websocket_ms={"max": 900.0}) if r.get("role") == "core" else r for r in timeline]
        self.assertFalse(a3.dl_path_check(core, T0, T0 + 30 * S, LIMITS)[0])


TOOL = campaign.sha256_file(campaign.ROOT / "scripts/p184_gate_campaign.py")


class ApplicabilityTests(unittest.TestCase):
    def amendment(self, **over):
        out = {"schema": campaign.A3_SCHEMA, "amendment_id": "A3", "scope": {"gates": ["G2", "G4", "G9"]},
               "limits": LIMITS, "frozen_before_candidate": True, "decision_owner": "owner", "frozen_at_unix": 100.0,
               "attribution_module": {"schema": a3.A3_SCHEMA}, "runtime_images": {"images": {"q": "sha256:1"}},
               "profile": {"image": "qdl-v2-python:2.2.2-d6d2637"},
               "workload": {"sessions": 100},
               "pinned": {**{name: {"new_sha256": sha} for name, sha in campaign.a3_code_sha256().items()},
                          "gate_campaign_tool": {"new_sha256": TOOL}, "budget": {"new_sha256": "b"},
                          "g4_addendum": {"new_sha256": "a"}, "g4_amendment_1": {"new_sha256": "1"},
                          "g4_amendment_2": {"new_sha256": "2"}}}
        out.update(over)
        return out

    def run_case(self, amendment, **campaign_over):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "a3.json"
            path.write_text(json.dumps(amendment))
            c = {"a3_path": str(path), "a3_sha256": campaign.sha256_file(path), "tool_sha256": TOOL,
                 "budget_sha256": "b", "g4_addendum_sha256": "a", "g4_amendment_sha256": "1",
                 "g4_amendment_2_sha256": "2", "a3_runtime_images": {"q": "sha256:1"}, "started_unix": 200.0,
                 "profile_image": "qdl-v2-python:2.2.2-d6d2637", "sessions": 100, "role": "candidate",
                 "a3_apparatus_sha256": campaign.a3_code_sha256()}
            c.update(campaign_over)
            return campaign.applicable_a3(c)

    def test_applies_only_when_every_pin_matches(self):
        self.assertIsNotNone(self.run_case(self.amendment())[0])
        for name, (amend, over) in {
            "tool": (self.amendment(), {"tool_sha256": "other"}),
            "runtime images": (self.amendment(), {"a3_runtime_images": {"q": "sha256:2"}}),
            "client profile image": (self.amendment(), {"profile_image": "qdl-v2-python:2.2.4-31a5155"}),
            "frozen after start": (self.amendment(frozen_at_unix=300.0), {}),
            "amendment 2": (self.amendment(), {"g4_amendment_2_sha256": "x"}),
            "scope": (self.amendment(scope={"gates": ["G2", "G4", "G6", "G9"]}), {}),
            "edited after recording": (self.amendment(), {"a3_sha256": "0" * 64}),
            "other workload": (self.amendment(), {"sessions": 50}),
            "run-time apparatus differs": (self.amendment(), {"a3_apparatus_sha256": {
                **campaign.a3_code_sha256(), "a3_attribution": "0" * 64}}),
            "scoring code differs from the pin": (self.amendment(pinned={
                **self.amendment()["pinned"], "a3_attribution": {"new_sha256": "0" * 64}}), {}),
        }.items():
            with self.subTest(case=name):
                found, notes, _ = self.run_case(amend, **over)
                self.assertIsNone(found)
                self.assertTrue(notes and notes[0].startswith("A3 not applied"))
        self.assertEqual(campaign.applicable_a3({}), (None, [], None))

    def test_exceedance_thresholds_cover_table_and_gated_addendum_rows_only(self):
        table = {"book_delta|OKX|": {"candidate_p95_limit_ms": 200.0},
                 campaign.G4_AMENDMENT_ROW: {"candidate_p95_limit_ms": 150.0}}
        addendum = {"rows": {"book_snapshot|OKX_INVERSE|": {"status": "GATED", "candidate_max_limit_ms": 300.7},
                             "bar|OKX|1h": {"status": "REPORT_ONLY"}}}
        self.assertEqual(campaign.a3_exceedance_thresholds(table, addendum),
                         {"book_delta|OKX|": 200.0, "book_snapshot|OKX_INVERSE|": 300.7})


class CommandPlanA3Tests(unittest.TestCase):
    def test_candidate_plan_carries_smoke_sampler_probe_exceedances_and_allowance(self):
        args = argparse.Namespace(state_tools="/tools", load_timeout=2400, budget=str(campaign.ROOT /
                                  "config/v2/p184-stage100-budget.json"), writer="w", g11=False, g11_wait=900,
                                  sessions=100)
        plan = {"binding": "b", "market_redis": {"container": "r", "db": 1}, "heartbeat_redis": {"container": "h",
                "db": 0}, "writer_log_dir": "/l"}
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(campaign, "A3_PATH",
                                                                         Path(temporary) / "a3.json"):
            (Path(temporary) / "a3.json").write_text("{}")
            d = Path(temporary) / "camp"
            commands = campaign.command_plan(args, plan, d, 1_792_316_000.0)
        self.assertIn("target-matrix", commands["client_smoke"])
        self.assertIn("--query-allowance", commands["driver"])
        self.assertEqual(commands["a3_local"][2:4], ["scripts/p184_a3_evidence.py", "sample"])
        window = commands["g4_windows"][0]
        self.assertTrue(window["binance_probe_container"].startswith("qdl-p184-g4obs-camp-"))
        self.assertIn("--exceedance-ms", window["command"])
        thresholds = json.loads(window["command"][window["command"].index("--exceedance-ms") + 1])
        self.assertIn("book_snapshot|OKX_INVERSE|", thresholds)


class ReviewFindingTests(unittest.TestCase):
    """Negatives for the independent review of 2026-10-18 (findings 2, 3, 6, 7, 9, 10, 12)."""

    def okx_timeline(self, cause, *, extra=()):
        lane_ = "okx-public-001"
        rows = [progress(T0 - 10 * S, "OKX", [lane(lane_, 67019, age=300.0), lane("okx-public-002", 209)]),
                {"role": "ingestor", "event": "qdl_native_session_disconnected", "at_ns": T0, "runtime": "OKX",
                 "lane": lane_, "generation": 67019, "cause": cause, "container": "c"},
                progress(T0 + 10 * S, "OKX", [lane(lane_, 67020, frames=50), lane("okx-public-002", 209)]),
                *extra]
        return sorted(rows, key=lambda r: r["at_ns"])

    def test_dl_initiated_session_ends_never_start_an_episode(self):
        for cause in ("STALENESS_HANDOVER", "BOOK_RESYNC", "WRITE_ERROR"):
            with self.subTest(cause=cause):
                self.assertEqual(a3.episodes(self.okx_timeline(cause)), [])
        (episode,) = a3.episodes(self.okx_timeline("READ_ERROR"))
        self.assertTrue(episode["complete"], episode)
        handover = {"role": "ingestor", "event": "qdl_native_session_disconnected", "at_ns": T0 - 5 * S,
                    "runtime": "OKX", "lane": "okx-public-001", "generation": 67018, "cause": "STALENESS_HANDOVER",
                    "container": "c"}
        (episode,) = a3.episodes(self.okx_timeline("READ_ERROR", extra=[handover]))
        self.assertFalse(episode["complete"])
        self.assertTrue(any("non-venue session end" in m for m in episode["missing"]))
        binance = {"role": "ingestor", "event": "qdl_native_session_disconnected", "runtime": "BINANCE",
                   "error": "IO error: Broken pipe (os error 32)"}
        self.assertFalse(a3.venue_fault(binance))
        self.assertFalse(a3.venue_fault({"event": "qdl_native_connect_failed", "runtime": "BINANCE"}))

    def test_collector_keeps_the_okx_cause(self):
        line = ('2026-10-18T09:33:12.961239821Z {"event":"qdl_native_session_disconnected","runtime":"OKX","lane":'
                '"okx-public-001","generation":5,"cause":"BOOK_RESYNC","detail":{"code":"X1","text":"t"}}')
        out = evidence.filter_line("ingestor", "c", line)
        self.assertEqual((out["cause"], out["detail_code"]), ("BOOK_RESYNC", "X1"))

    def test_g9_onset_before_the_last_good_frame_or_unclean_onset_seconds_stay_unknown(self):
        row = dict(G9Tests.ROW, component_age_ms=25_000)  # onset T0 - 15 s, before the last good frame T0 - 12 s
        samples = [heartbeat(T0 - 20 * S, []), heartbeat(T0 + 10 * S, [row]), heartbeat(T0 + 40 * S, [])]
        (loss,) = a3.g9_loss_events(samples)
        out = a3.classify_g9_loss(loss, lanes=G9Tests.LANES, episodes_=a3.episodes(binance_timeline()),
                                  local=local("BINANCE", start=T0 - 60 * S, end=T0 + 120 * S),
                                  timeline=binance_timeline(), limits=LIMITS, samples=samples)
        self.assertEqual(out["attribution"], "UNKNOWN")
        samples = [heartbeat(T0 - 20 * S, []), heartbeat(T0 + 10 * S, [G9Tests.ROW]), heartbeat(T0 + 40 * S, [])]
        (loss,) = a3.g9_loss_events(samples)
        throttled = local("BINANCE", start=T0 - 60 * S, end=T0 + 120 * S, throttled_at=T0 - 11 * S)
        out = a3.classify_g9_loss(loss, lanes=G9Tests.LANES, episodes_=a3.episodes(binance_timeline()), local=throttled,
                                  timeline=binance_timeline(), limits=LIMITS, samples=samples)
        self.assertEqual(out["attribution"], "UNKNOWN")  # throttled at the onset seconds, not only at the fault

    def test_g9_reason_and_writer_liveness(self):
        other = dict(G9Tests.ROW, reason_code="PRICING_COMPONENT_STALE")
        samples = [heartbeat(T0 - 20 * S, []), heartbeat(T0 + 10 * S, [other]), heartbeat(T0 + 40 * S, [])]
        (loss,) = a3.g9_loss_events(samples)
        out = a3.classify_g9_loss(loss, lanes=G9Tests.LANES, episodes_=a3.episodes(binance_timeline()),
                                  local=local("BINANCE", start=T0 - 60 * S, end=T0 + 120 * S),
                                  timeline=binance_timeline(), limits=LIMITS, samples=samples)
        self.assertEqual(out["attribution"], "UNKNOWN")
        samples = [heartbeat(T0 - 20 * S, []), heartbeat(T0 + 10 * S, [G9Tests.ROW]), heartbeat(T0 + 40 * S, [])]
        samples[1]["sample"]["worker_liveness"] = {"0": "REPORT_EXPIRED"}
        (loss,) = a3.g9_loss_events(samples)
        out = a3.classify_g9_loss(loss, lanes=G9Tests.LANES, episodes_=a3.episodes(binance_timeline()),
                                  local=local("BINANCE", start=T0 - 60 * S, end=T0 + 120 * S),
                                  timeline=binance_timeline(), limits=LIMITS, samples=samples)
        self.assertEqual(out["attribution"], "UNKNOWN")

    def test_exact_clocks_decide_units_not_the_rounded_visibility(self):
        e1 = okx_book_event(seq=1, offset=1)
        maximum = okx_book_event(seq=2, offset=2, lag_ms=220.0)
        maximum["visibility_ms"] = 300.7  # rounded; exact clocks give 355 ms
        retention = {"threshold_ms": 300.7, "events": [e1], "overflow": 0, "max_event": maximum}
        out = a3.rescue_g4_window(retention, row_limit_ms=300.7, n_admitted=1000,
                                  probes={("books", "BTC-USD-SWAP", 1): fresh(30)}, episodes_=[], local=local(),
                                  limits=LIMITS, segments=SEGMENTS)
        self.assertEqual(out["status"], "INCOMPLETE")

    def test_window_cap_makes_a_p95_failure_unrescuable(self):
        events = [okx_book_event(seq=k, offset=k) for k in range(6)]
        retention = {"threshold_ms": 300.7, "events": events, "overflow": 0, "max_event": events[0]}
        probes = {("books", "BTC-USD-SWAP", k): fresh(30) for k in range(6)}
        out = a3.rescue_g4_window(retention, row_limit_ms=300.7, n_admitted=100, probes=probes, episodes_=[],
                                  local=local(), limits=LIMITS, segments=SEGMENTS)
        self.assertEqual((out["rescued"], out["status"]), (False, "WINDOW_CAP_EXCEEDED"))

    def test_socket_sampler_gap_or_error_and_missing_fields_fail(self):
        gap = local()
        gap["tcp"]["OKX"] = [x for x in gap["tcp"]["OKX"] if abs(json.loads(x)["at_ns"] - T0) > 3 * S]
        self.assertFalse(a3.local_check(gap, "OKX", T0, T0 + S, LIMITS)[0])
        err = local()
        err["tcp"]["OKX"].append(json.dumps({"kind": "tcp_error", "at_ns": T0 - 5 * S}))
        self.assertFalse(a3.local_check(err, "OKX", T0, T0 + S, LIMITS)[0])
        field = local()
        field["local"] = [{k: v for k, v in x.items() if k != "runq_wait_ms_max_thread"} for x in field["local"]]
        self.assertFalse(a3.local_check(field, "OKX", T0, T0 + S, LIMITS)[0])

    def test_dl_path_needs_every_core_and_gateway_and_real_dispatch(self):
        timeline = binance_timeline() + [{"role": "core", "event": "qdl_realtime_core_progress",
                                          "at_ns": T0 - 300 * S, "container": "core2",
                                          "raw_age_websocket_ms": {"max": 10.0}}]
        self.assertFalse(a3.dl_path_check(timeline, T0, T0 + 30 * S, LIMITS)[0])  # core2 silent in the span
        frozen = [dict(r, dispatch_lag_buckets=[0] * 13) if r.get("role") == "gateway" else r
                  for r in binance_timeline()]
        self.assertFalse(a3.dl_path_check(frozen, T0, T0 + 30 * S, LIMITS)[0])  # no dispatch at all


@unittest.skipUnless(all((d / "scorecard.json").exists() for d in RECEIPTS), "existing receipts not on this host")
class ReceiptParityTests(unittest.TestCase):
    """A3 not recorded -> every gate of every stored scorecard is byte-identical (incl. the invalid 092255Z).
    Scored on mirrored receipts: the run worktrees named in their campaign.json were removed (p184_receipt_support)."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.dirs = [mirror(d, Path(cls.tmp.name)) for d in RECEIPTS]

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_every_gate_identical_without_a3(self):
        for d in self.dirs:
            stored = json.loads((d / "scorecard.json").read_text())
            fresh = json.loads(json.dumps(campaign.build_scorecard(d), default=str))
            self.assertEqual(as_recorded(json.dumps(fresh["gates"], sort_keys=True), d),
                             json.dumps(stored["gates"], sort_keys=True), d.name)
            self.assertNotIn("a3", fresh)

    def test_a_recorded_but_unpinned_a3_is_not_applied_and_changes_no_verdict(self):
        d = self.dirs[-1]
        stored = json.loads((d / "scorecard.json").read_text())
        with tempfile.TemporaryDirectory() as temporary:
            copy = Path(temporary) / d.name
            # A shallow mirror: campaign.json with an A3 record, every other file read from the receipt.
            copy.mkdir()
            for item in d.iterdir():
                if item.name != "campaign.json":
                    (copy / item.name).symlink_to(item)
            c = json.loads((d / "campaign.json").read_text())
            fake = Path(temporary) / "a3.json"
            fake.write_text(json.dumps({"schema": campaign.A3_SCHEMA}))
            c.update(a3_path=str(fake), a3_sha256=campaign.sha256_file(fake))
            (copy / "campaign.json").write_text(json.dumps(c))
            fresh = json.loads(json.dumps(campaign.build_scorecard(copy), default=str))
        self.assertEqual([g["verdict"] for g in fresh["gates"]], [g["verdict"] for g in stored["gates"]])
        self.assertFalse(fresh["a3"]["applied"])


if __name__ == "__main__":
    unittest.main()
