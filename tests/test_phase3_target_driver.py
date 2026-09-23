"""v2.1.1 target-run accounting, frozen budget and driver behaviour.

The owner's contract lists what must be proven before live load: target
arithmetic, missing feed, under-issued traffic, per-session starvation, strict
quota rejection, cancellation/recovery, cold/hot overlap and bounded memory.
Arithmetic, missing feed and strict quota are pinned in
``test_phase3_target_workload``; this module pins the rest against the
accounting types and the driver's own poll loop, with no network.
"""

from __future__ import annotations

import asyncio
from collections import Counter
import copy
from dataclasses import dataclass
import importlib.util
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

from qdl.certification.phase3_consumer_load import (
    BarSeries,
    DeclaredRateTicker,
    PollLedger,
    TARGET_STAGE_MIX,
    evaluate_latency,
    evaluate_target_acceptance,
    load_target_budget,
)


_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts" / "phase3_consumer_load_acceptance.py"
_SPEC = importlib.util.spec_from_file_location("phase3_target_driver", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_DRIVER = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _DRIVER
_SPEC.loader.exec_module(_DRIVER)
_BUDGET = load_target_budget(json.loads((_ROOT / "config/v2/v211-target-acceptance-budget.json").read_text()))


@dataclass(frozen=True)
class _Feed:
    value: str


@dataclass(frozen=True)
class _Product:
    venue: str = "OKX"
    native_symbol: str = "BTC-USDT-SWAP"
    feed: _Feed = _Feed("QUOTE")
    interval: str | None = None
    consumer_id: str = "alpha.okx.paper.stable"
    delivery: _Feed = _Feed("DURABLE")
    source_policy_id: str = "crypto_primary_v2"


class TickerTests(unittest.TestCase):
    def test_offered_counts_every_due_tick_inside_the_window(self):
        for phase in (0.0, 0.37, 0.999):
            self.assertEqual(DeclaredRateTicker(start=0.0, period=1.0, phase=phase, end=90.0).offered, 90)
        self.assertEqual(DeclaredRateTicker(start=10.0, period=60.0, phase=5.0, end=310.0).offered, 5)

    def test_a_client_a_full_period_behind_misses_ticks_it_cannot_retime(self):
        ticker = DeclaredRateTicker(start=0.0, period=1.0, phase=0.0, end=10.0)
        self.assertEqual(ticker.take(0.5), (0.0, 0))
        # Ticks due at 1 and 2 had their whole period elapse: missed, not re-timed.
        self.assertEqual(ticker.take(3.2), (3.0, 2))
        due, missed = ticker.take(float("inf"))
        self.assertIsNone(due)
        self.assertEqual(missed, 6)

    def test_invalid_windows_are_refused(self):
        for kwargs in ({"period": 0.0, "phase": 0.0}, {"period": 1.0, "phase": 1.0},
                       {"period": 1.0, "phase": -0.1}):
            with self.assertRaises(ValueError):
                DeclaredRateTicker(start=0.0, end=1.0, **kwargs)


class LedgerAndSeriesTests(unittest.TestCase):
    def test_ledger_balances_only_when_every_offered_tick_has_a_fate(self):
        ledger = PollLedger(offered=5)
        ledger.sent, ledger.completed, ledger.missed = 4, 3, 1
        self.assertFalse(ledger.balanced)
        ledger.record_failure("RATE_LIMITED")
        self.assertTrue(ledger.balanced)
        self.assertEqual(ledger.evidence()["failure_codes"], {"RATE_LIMITED": 1})

    def test_bar_series_appends_dedups_and_refuses_fifo_or_gap(self):
        minute = 60_000_000_000
        series = BarSeries(maxlen=3, interval_ns=minute)
        for index in range(5):
            self.assertEqual(series.offer(index * minute), "APPEND")
        self.assertEqual(len(series), 3)  # bounded memory: maxlen, not history
        self.assertEqual(series.offer(4 * minute), "REPEAT")
        with self.assertRaisesRegex(ValueError, "FIFO"):
            series.offer(3 * minute)
        with self.assertRaisesRegex(ValueError, "gap"):
            series.offer(6 * minute)
        self.assertEqual((series.appended, series.repeats), (5, 1))


class BudgetTests(unittest.TestCase):
    def test_committed_budget_pins_the_owners_targets(self):
        latency = _BUDGET["latency_ms"]
        self.assertEqual((latency["QUOTE_SNAPSHOT"]["p95"], latency["QUOTE_SNAPSHOT"]["p99"]), (100, 250))
        self.assertEqual((latency["TRADE_SNAPSHOT"]["p95"], latency["TRADE_SNAPSHOT"]["p99"]), (100, 250))
        self.assertEqual((latency["MARK_INDEX_REFERENCE"]["p95"], latency["MARK_INDEX_REFERENCE"]["p99"]),
                         (250, 500))
        self.assertEqual((latency["L2_SNAPSHOT"]["p95"], latency["L2_SNAPSHOT"]["p99"]), (300, 750))
        self.assertEqual((latency["BAR_LATEST"]["p95"], latency["BAR_LATEST"]["p99"]), (1000, 2000))
        self.assertEqual(_BUDGET["final"]["burst"]["extra_fraction"], 0.25)
        self.assertEqual(_BUDGET["final"]["burst"]["seconds"], 10)

    def test_a_budget_that_drifts_from_the_frozen_profile_is_refused(self):
        drifted = copy.deepcopy(dict(_BUDGET))
        drifted["stages"]["50"]["mix"] = [20, 15, 10, 4]
        with self.assertRaisesRegex(ValueError, "differs"):
            load_target_budget(drifted)
        drifted = copy.deepcopy(dict(_BUDGET))
        drifted["stages"]["20"]["seconds"] = 60
        with self.assertRaisesRegex(ValueError, "differs"):
            load_target_budget(drifted)

    def test_latency_rule_depends_on_sample_size_and_never_passes_empty(self):
        target, rule = {"p95": 100, "p99": 250}, _BUDGET["sample_rule"]
        self.assertEqual(evaluate_latency([], target, rule)["status"], "FAIL")
        self.assertEqual(evaluate_latency([10.0] * 149 + [300.0], target, rule)["status"], "PASS")
        self.assertEqual(evaluate_latency([10.0] * 97 + [260.0] * 3, target, rule)["status"], "FAIL")
        self.assertEqual(evaluate_latency([10.0] * 49 + [240.0], target, rule)["rule"], "p95; p99 reported only")
        small = evaluate_latency([10.0, 260.0], target, rule)
        self.assertEqual((small["status"], small["rule"]), ("FAIL", "small sample: max within p99 target"))


def _passing_receipt(final: bool = False) -> dict[str, object]:
    series = {}
    for name in _BUDGET["latency_ms"]:
        for venue in ("BINANCE", "OKX"):
            series[f"{name}|{venue}|STEADY"] = [20.0] * 120
    if final:
        series["QUOTE_SNAPSHOT|OKX|BURST"] = [30.0] * 10
        series["QUOTE_SNAPSHOT|OKX|RECONNECT"] = [30.0] * 10
    ledger = PollLedger(offered=120, sent=120, completed=120).evidence()
    return {
        "latency_series": series,
        "poll_ledgers": [{"poll": "s1:CANDLE:0", **ledger}],
        "scheduler_lag_ms": {"p99": 3.0},
        "streams": [
            {"name": "s1-bar", "feed": "BAR", "events": 2, "errors": 0, "final_bars": 2},
            {"name": "s2-trade", "feed": "TRADE", "events": 90, "errors": 0, "final_bars": 0},
        ],
        "cold": [{"venue": venue, "rows": rows, "returned": rows}
                 for venue in ("BINANCE", "OKX") for rows in (2500, 5000)],
        "setup": {"seconds": 40.0, "retries": 3, "failures": []},
        "leaked_tasks": 0,
        "fault_windows": {"BURST": [120.0, 130.0], "SLOW_READER": 1, "RECONNECT": 9} if final else {},
    }


def _passing_ts() -> dict[str, object]:
    sample = {"ready": 60, "demanded": 60, "fallback": 0, "v2_error": 7}
    return {"samples": [sample] * 12, "disconnect_codes": {"DATA_STALE": 4},
            "run_disconnects_per_minute": 2.0, "baseline_disconnects_per_minute": 1.5}


class AcceptanceTests(unittest.TestCase):
    def _evaluate(self, receipt=None, ts=None, final=False):
        return evaluate_target_acceptance(budget=_BUDGET, stage=50 if final else 20, final=final,
                                          receipt=receipt or _passing_receipt(final), trading_system=ts or _passing_ts())

    def test_a_clean_run_passes_every_listed_gate(self):
        result = self._evaluate()
        self.assertEqual(result["status"], "PASS", result["failed_gates"])
        self.assertEqual(self._evaluate(final=True)["status"], "PASS")

    def test_under_issued_traffic_fails_instead_of_disappearing(self):
        receipt = _passing_receipt()
        receipt["poll_ledgers"][0].update(sent=110, completed=110, missed=10)
        result = self._evaluate(receipt)
        self.assertIn("requests:no_missed_ticks", result["failed_gates"])
        receipt["poll_ledgers"][0].update(missed=0, balanced=False)
        self.assertIn("requests:offered_equals_sent_plus_missed", self._evaluate(receipt)["failed_gates"])

    def test_a_starved_session_and_a_rejected_read_fail(self):
        receipt = _passing_receipt()
        receipt["poll_ledgers"].append({"poll": "s9:GRID:0", **PollLedger(offered=120, missed=120).evidence()})
        self.assertIn("requests:no_starved_session", self._evaluate(receipt)["failed_gates"])
        receipt = _passing_receipt()
        receipt["poll_ledgers"][0].update(completed=119, failed=1, failure_codes={"RATE_LIMITED": 1})
        self.assertIn("requests:no_failures", self._evaluate(receipt)["failed_gates"])

    def test_a_lagging_client_invalidates_the_run(self):
        receipt = _passing_receipt()
        receipt["scheduler_lag_ms"] = {"p99": 400.0}
        self.assertIn("client:scheduler_lag_valid", self._evaluate(receipt)["failed_gates"])

    def test_silent_live_stream_and_missing_final_bar_fail(self):
        receipt = _passing_receipt()
        receipt["streams"][1]["events"] = 0
        receipt["streams"][0]["final_bars"] = 0
        failed = self._evaluate(receipt)["failed_gates"]
        self.assertIn("streams:every_live_stream_delivered", failed)
        self.assertIn("streams:final_bar_every_bar_stream", failed)

    def test_cold_overlap_must_cover_both_rows_on_both_venues(self):
        receipt = _passing_receipt()
        receipt["cold"] = receipt["cold"][:3]
        self.assertIn("cold:2500_5000_overlap_per_venue", self._evaluate(receipt)["failed_gates"])

    def test_trading_system_regression_fails(self):
        ts = _passing_ts()
        ts["samples"] = [*ts["samples"], {"ready": 51, "demanded": 60, "fallback": 0, "v2_error": 7}]
        self.assertIn("ts:ready_60_every_sample", self._evaluate(ts=ts)["failed_gates"])
        ts = _passing_ts()
        ts["disconnect_codes"] = {"UNAUTHENTICATED": 1}
        self.assertIn("ts:no_auth_or_manifest_disconnect", self._evaluate(ts=ts)["failed_gates"])
        ts = _passing_ts()
        ts["run_disconnects_per_minute"] = 30.0
        self.assertIn("ts:disconnects_within_baseline", self._evaluate(ts=ts)["failed_gates"])

    def test_final_run_requires_its_fault_windows(self):
        receipt = _passing_receipt(final=True)
        receipt["fault_windows"]["RECONNECT"] = 0
        self.assertIn("final:fault_windows_ran", self._evaluate(receipt, final=True)["failed_gates"])


class _Recorder(_DRIVER._TargetRecorder):
    pass


class DriverPollLoopTests(unittest.TestCase):
    def _run(self, read, *, period=0.05, seconds=0.5, cancel_after=None):
        recorder = _Recorder(windows={})

        async def scenario():
            start = time.monotonic() + 0.02
            task = asyncio.create_task(_DRIVER._target_poll_loop(
                client=None, label="s1:CANDLE:0", operation="SNAPSHOT", products=(_Product(),),
                period=period, phase=0.0, start=start, end=start + seconds, recorder=recorder,
            ))
            if cancel_after is not None:
                await asyncio.sleep(cancel_after)
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return [item for item in asyncio.all_tasks() if item is not asyncio.current_task()]

        with patch.object(_DRIVER, "_target_read", read):
            leaked = asyncio.run(scenario())
        return recorder, leaked

    def test_every_tick_completes_on_a_fast_server(self):
        async def read(client, operation, products):
            return None
        recorder, leaked = self._run(read)
        (ledger,) = recorder.ledgers
        self.assertEqual((ledger["offered"], ledger["completed"], ledger["missed"]), (10, 10, 0))
        self.assertTrue(ledger["balanced"])
        self.assertEqual(len(recorder.latency["QUOTE_SNAPSHOT|OKX|STEADY"]), 10)
        self.assertEqual(leaked, [])

    def test_a_slow_server_produces_missed_ticks_not_a_lower_rate(self):
        async def read(client, operation, products):
            await asyncio.sleep(0.16)
        recorder, _ = self._run(read)
        (ledger,) = recorder.ledgers
        self.assertEqual(ledger["offered"], 10)
        self.assertGreater(ledger["missed"], 0)
        self.assertTrue(ledger["balanced"], ledger)

    def test_a_typed_rejection_is_counted_by_code(self):
        class Rejected(Exception):
            code = "RATE_LIMITED"

        async def read(client, operation, products):
            raise Rejected("consumer is at its finite pending bound")
        recorder, _ = self._run(read, seconds=0.2)
        (ledger,) = recorder.ledgers
        self.assertEqual(ledger["failure_codes"], {"RATE_LIMITED": 4})
        self.assertEqual(recorder.error_count, 4)

    def test_cancellation_leaves_a_visible_unbalanced_ledger_and_no_leaked_work(self):
        async def read(client, operation, products):
            await asyncio.sleep(10)
        recorder, leaked = self._run(read, cancel_after=0.1)
        (ledger,) = recorder.ledgers
        self.assertFalse(ledger["balanced"])
        self.assertEqual(leaked, [])


class DriverHelperTests(unittest.TestCase):
    def test_worker_count_keeps_about_thirteen_sessions_per_process(self):
        self.assertEqual([_DRIVER.target_worker_count(n) for n in sorted(TARGET_STAGE_MIX)], [1, 2, 3, 4])

    def test_fault_windows_label_cold_and_hot_overlap_separately(self):
        recorder = _Recorder(windows={"BURST": (10.0, 20.0), "RECONNECT": (30.0, 45.0)})
        self.assertEqual([recorder.window(at) for at in (5.0, 10.0, 19.9, 20.0, 31.0, 50.0)],
                         ["STEADY", "BURST", "BURST", "STEADY", "RECONNECT", "STEADY"])

    def test_served_replica_is_visible_to_the_caller(self):
        class Replica:
            base_url = "https://query_v2_2:8200"

            async def snapshot(self, *args, **kwargs):
                return {"ok": True}

        async def scenario():
            _DRIVER._SERVED_BY.set(None)
            wrapped = _DRIVER._AttributedReplica(Replica(), "query_v2_2")
            await wrapped.snapshot()
            return _DRIVER._SERVED_BY.get(), wrapped.base_url

        self.assertEqual(asyncio.run(scenario()), ("query_v2_2", "https://query_v2_2:8200"))

    def test_merge_keeps_every_ledger_and_sums_exact_counts(self):
        @dataclass
        class Plan:
            stage: int = 5
            class_counts: tuple = (2, 1, 1, 1)
            stream_count: int = 9
            hot_requests_per_second: float = 5.0
            demands: tuple = ()

        def worker(index, completed):
            return {
                "worker": index, "setup": {"seconds": 10.0 + index, "retries": index, "failures": []},
                "latency_series": {"QUOTE_SNAPSHOT|OKX|STEADY": [1.0] * completed},
                "latency_groups": {"QUOTE_SNAPSHOT|OKX|BTC|query_v2_1|STEADY": [1.0] * completed},
                "stream_samples": [], "poll_ledgers": [{"poll": f"w{index}", "completed": completed}],
                "scheduler_lag_ms": [0.5] * completed, "streams": [{"reconnects": 0, "restored": 0}],
                "bar_series": {}, "cold": [], "counters": {"x": 1}, "errors": [], "error_count": 0,
                "leaked_tasks": 0, "fault_windows": {}, "cpu_seconds": 1.0, "max_rss_kib": 1,
            }

        merged = _DRIVER._merge_target([worker(0, 3), worker(1, 4)], plan=Plan(), final=False)
        self.assertEqual(len(merged["latency_series"]["QUOTE_SNAPSHOT|OKX|STEADY"]), 7)
        self.assertEqual([item["poll"] for item in merged["poll_ledgers"]], ["w0", "w1"])
        self.assertEqual(merged["setup"], {"seconds": 11.0, "retries": 1, "failures": []})
        self.assertEqual(merged["counters"], {"x": 2})
        self.assertEqual((merged["scheduler_lag_ms"]["n"], merged["scheduler_lag_ms"]["p99"]), (7, 0.5))


if __name__ == "__main__":
    unittest.main()
