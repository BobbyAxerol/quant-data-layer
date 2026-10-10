"""P18.4 G6 instrumentation of the stage-100 driver: per-binding view age and typed eligibility (no network).

Synthetic views only (test provenance). Pins: the policy table, the UNSAFE rule (eligible although the policy
rejects it), quiet-continuity exceptions, bounded per-worker evidence, merging, and that the poll loop records a view
the client later refused exactly once.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import importlib.util
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

_ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("phase3_driver_g6", _ROOT / "scripts/phase3_consumer_load_acceptance.py")
_DRIVER = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _DRIVER
_SPEC.loader.exec_module(_DRIVER)


def classify(**overrides):
    base = dict(feed="QUOTE", freshness_ms=100, eligible=True, state="LIVE", gap_open=False, complete=True,
                event_recency_state="LIVE", provider_session_state="LIVE", session_liveness_ms=500,
                max_session_liveness_ms=2000, observe=True, on_change=False, policy_ms=2000)
    base.update(overrides)
    return _DRIVER.g6_classify(**base)


class G6ClassifyTests(unittest.TestCase):
    def test_policy_table(self):
        self.assertEqual(_DRIVER.g6_policy_ms("TRADE", None, 9), 3000)
        self.assertEqual(_DRIVER.g6_policy_ms("BOOK_SNAPSHOT", None, 9), 60000)
        self.assertEqual(_DRIVER.g6_policy_ms("BAR", "1m", 9), 180000)
        self.assertEqual(_DRIVER.g6_policy_ms("BAR", "5m", 600000), 600000)  # own sealed requirement
        self.assertEqual(_DRIVER.g6_policy_ms("FUNDING_RATE", None, None), None)

    def test_fresh_eligible_and_not_eligible(self):
        self.assertEqual(classify(), ("ELIGIBLE", []))
        self.assertEqual(classify(eligible=False, freshness_ms=99999), ("NOT_ELIGIBLE", []))

    def test_eligible_but_stale_without_quiet_proof_is_unsafe(self):
        self.assertEqual(classify(freshness_ms=2500), ("UNSAFE", ["AGE_BEYOND_POLICY"]))
        state, reasons = classify(gap_open=True, state="GAPPED")
        self.assertEqual(state, "UNSAFE")
        self.assertEqual(set(reasons), {"GAP_OPEN", "STATE_GAPPED"})
        self.assertEqual(classify(session_liveness_ms=5000)[1], ["SESSION_BEYOND_POLICY"])

    def test_quiet_continuity_under_observe_is_eligible_quiet(self):
        trade = classify(feed="TRADE", freshness_ms=9000, policy_ms=3000, event_recency_state="STALE")
        self.assertEqual(trade, ("ELIGIBLE_QUIET", []))
        quote = classify(freshness_ms=9000, event_recency_state="STALE", on_change=True)
        self.assertEqual(quote, ("ELIGIBLE_QUIET", []))
        # A generic stale QUOTE cannot self-upgrade; neither can a quiet TRADE on a dead session.
        self.assertEqual(classify(freshness_ms=9000, event_recency_state="STALE")[0], "UNSAFE")
        self.assertEqual(classify(feed="TRADE", freshness_ms=9000, policy_ms=3000, event_recency_state="STALE",
                                  provider_session_state="DISCONNECTED")[0], "UNSAFE")


class G6RecorderTests(unittest.TestCase):
    def test_bounded_reservoir_exact_counts_and_merge(self):
        one, two = _DRIVER._G6Recorder(reservoir=8), _DRIVER._G6Recorder(reservoir=8)
        for age in range(100):
            one.view({"key": "QUOTE|OKX|", "age_ms": age, "policy_ms": 2000, "class": "ELIGIBLE", "reasons": []},
                     "STEADY")
        two.view({"key": "QUOTE|OKX|", "age_ms": 5000, "policy_ms": 2000, "class": "UNSAFE",
                  "reasons": ["AGE_BEYOND_POLICY"]}, "BURST")
        two.refusal("TRADE|OKX|", "DATA_STALE", "STEADY")
        self.assertEqual(len(one.rows["QUOTE|OKX|"]["ages"]), 8)
        merged = _DRIVER.merge_g6([one.evidence(), two.evidence()])
        row = merged["QUOTE|OKX|"]
        self.assertEqual((row["n"], row["max_age_ms"], row["unsafe"]), (101, 5000, 1))
        self.assertEqual(row["windows"], {"STEADY": 100, "BURST": 1})
        self.assertEqual(row["age_ms"]["n"], 9)
        self.assertEqual(merged["TRADE|OKX|"]["refusals"], {"DATA_STALE": 1})


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


class G6PollLoopTests(unittest.TestCase):
    def _run(self, read):
        recorder = _DRIVER._TargetRecorder(windows={})

        async def scenario():
            start = time.monotonic() + 0.02
            await _DRIVER._target_poll_loop(client=None, label="s1", operation="SNAPSHOT", products=(_Product(),),
                                            period=0.05, phase=0.0, start=start, end=start + 0.2, recorder=recorder)
        with patch.object(_DRIVER, "_target_read", read):
            asyncio.run(scenario())
        return recorder.g6.evidence()

    def test_a_refused_unsafe_view_is_counted_once_as_unsafe(self):
        async def read(client, operation, products):
            _DRIVER._G6_VIEW.set([{"key": "QUOTE|OKX|", "age_ms": 4000, "policy_ms": 2000, "class": "UNSAFE",
                                   "reasons": ["AGE_BEYOND_POLICY"]}])
            raise ValueError("V2 receipt exceeds the governed freshness bound")
        row = self._run(read)["QUOTE|OKX|"]
        self.assertEqual(row["classes"], {"UNSAFE": 4})
        self.assertEqual(row["refusals"], {})

    def test_a_read_without_a_view_is_a_typed_refusal(self):
        class Rejected(Exception):
            code = "RATE_LIMITED"

        async def read(client, operation, products):
            raise Rejected("x")
        row = self._run(read)["QUOTE|OKX|"]
        self.assertEqual((row["n"], row["refusals"]), (0, {"RATE_LIMITED": 4}))

    def test_success_records_the_view(self):
        async def read(client, operation, products):
            _DRIVER._G6_VIEW.set([{"key": "QUOTE|OKX|", "age_ms": 120, "policy_ms": 2000, "class": "ELIGIBLE",
                                   "reasons": []}])
        row = self._run(read)["QUOTE|OKX|"]
        self.assertEqual((row["n"], row["classes"], row["max_age_ms"]), (4, {"ELIGIBLE": 4}, 120))


if __name__ == "__main__":
    unittest.main()
