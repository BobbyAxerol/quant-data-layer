"""P18.4 G6 instrumentation of the stage-100 driver: per-binding view age and typed eligibility (no network).

Synthetic views only (test provenance). Pins: the policy of the requirement a view was read with (D3 table only for an
EXECUTION requirement, as min with it; any other grade its own requirement), the UNSAFE rule (eligible although the
policy rejects it), quiet-continuity exceptions, the MARK reference age basis, bounded per-worker evidence, merging,
that the poll loop records a view the client later refused exactly once, and parity of the pure functions with their
verbatim copy in the TS execution probe (scripts/p184_g6_ts_execution_probe.py).
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
_PROBE_SPEC = importlib.util.spec_from_file_location("p184_g6_ts_probe", _ROOT / "scripts/p184_g6_ts_execution_probe.py")
_PROBE = importlib.util.module_from_spec(_PROBE_SPEC)
sys.modules[_PROBE_SPEC.name] = _PROBE
_PROBE_SPEC.loader.exec_module(_PROBE)


def classify(**overrides):
    base = dict(feed="QUOTE", freshness_ms=100, eligible=True, state="LIVE", gap_open=False, complete=True,
                event_recency_state="LIVE", provider_session_state="LIVE", session_liveness_ms=500,
                max_session_liveness_ms=2000, observe=True, on_change=False, policy_ms=2000)
    base.update(overrides)
    return _DRIVER.g6_classify(**base)


class G6ClassifyTests(unittest.TestCase):
    def test_policy_is_the_requirement_it_was_read_with(self):
        policy = _DRIVER.g6_policy
        # EXECUTION: the frozen D3 table, as min with the requirement (stricter consumer intent wins).
        self.assertEqual(policy("TRADE", None, grade="EXECUTION", requirement_max_ms=3000), (3000, "D3_EXECUTION"))
        self.assertEqual(policy("QUOTE", None, grade="EXECUTION", requirement_max_ms=15000), (2000, "D3_EXECUTION"))
        self.assertEqual(policy("TRADE", None, grade="EXECUTION", requirement_max_ms=900), (900, "D3_EXECUTION"))
        self.assertEqual(policy("BOOK_SNAPSHOT", None, grade="EXECUTION", requirement_max_ms=None),
                         (60000, "D3_EXECUTION"))
        self.assertEqual(policy("BAR", "1m", grade="EXECUTION", requirement_max_ms=None), (180000, "D3_EXECUTION"))
        self.assertEqual(policy("BAR", "5m", grade="EXECUTION", requirement_max_ms=600000), (600000, "D3_EXECUTION"))
        # Any other grade: only its own requirement; the D3 table never applies.
        self.assertEqual(policy("QUOTE", None, grade="ALPHA", requirement_max_ms=5000), (5000, "REQUIREMENT_ALPHA"))
        self.assertEqual(policy("MARK_INDEX_PRICE", None, grade=_Feed("ALPHA"), requirement_max_ms=60000),
                         (60000, "REQUIREMENT_ALPHA"))
        self.assertEqual(policy("FUNDING_RATE", None, grade=None, requirement_max_ms=None),
                         (None, "REQUIREMENT_UNKNOWN"))

    def test_alpha_quote_at_3000_ms_is_eligible_not_unsafe(self):
        policy_ms, basis = _DRIVER.g6_policy("QUOTE", None, grade="ALPHA", requirement_max_ms=5000)
        self.assertEqual(classify(freshness_ms=3000, policy_ms=policy_ms), ("ELIGIBLE", []))
        self.assertEqual(basis, "REQUIREMENT_ALPHA")

    def test_execution_quote_at_2500_ms_without_quiet_proof_is_unsafe(self):
        policy_ms, basis = _DRIVER.g6_policy("QUOTE", None, grade="EXECUTION", requirement_max_ms=2000)
        self.assertEqual(classify(freshness_ms=2500, policy_ms=policy_ms), ("UNSAFE", ["AGE_BEYOND_POLICY"]))
        self.assertEqual(basis, "D3_EXECUTION")

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
class _Requirement:
    feed: _Feed = _Feed("QUOTE")
    consumer_grade: _Feed = _Feed("ALPHA")
    max_freshness_ms: int | None = 5000


@dataclass(frozen=True)
class _Product:
    venue: str = "OKX"
    native_symbol: str = "BTC-USDT-SWAP"
    feed: _Feed = _Feed("QUOTE")
    interval: str | None = None
    consumer_id: str = "alpha.okx.paper.stable"
    delivery: _Feed = _Feed("DURABLE")
    source_policy_id: str = "crypto_primary_v2"
    requirement: _Requirement = _Requirement()


@dataclass(frozen=True)
class _SdkReference:
    max_freshness_ms: int | None = 60000


@dataclass(frozen=True)
class _Reference:
    venue: str = "BINANCE"
    requirement: _Requirement = _Requirement(feed=_Feed("MARK_INDEX_PRICE"), max_freshness_ms=60000)
    sdk_requirement: _SdkReference = _SdkReference()


@dataclass(frozen=True)
class _Observation:
    observed_at_ns: int
    labels: dict


@dataclass(frozen=True)
class _Data:
    observations: list


@dataclass(frozen=True)
class _Item:
    data: _Data
    status: str = "OK"


@dataclass(frozen=True)
class _Response:
    results: list


MS = 1_000_000


class G6ReferenceTests(unittest.TestCase):
    def _mark(self, reference, *, observed_ns, labels, sent_ns, now_ns):
        response = _Response([_Item(_Data([_Observation(observed_ns, labels)]))])
        (row,) = _DRIVER._g6_reference_observations([reference], response, sent_ns=sent_ns, now_ns=now_ns)
        return row

    def test_alpha_mark_at_2300_ms_is_not_unsafe_and_is_aged_from_request_sent(self):
        sent = 10_000 * MS
        labels = {"freshness_basis": "PROVIDER_CONFIRMATION", "provider_confirmation_ns": str(sent - 2300 * MS)}
        row = self._mark(_Reference(), observed_ns=sent - 2600 * MS, labels=labels, sent_ns=sent,
                         now_ns=sent + 400 * MS)
        self.assertEqual((row["class"], row["policy_ms"], row["policy_basis"]), ("ELIGIBLE", 60000,
                                                                                 "REQUIREMENT_ALPHA"))
        self.assertEqual((row["age_ms"], row["client_age_ms"]), (2300, 2700))
        self.assertEqual(row["age_basis"], "REQUEST_SENT_MINUS_SERVER_FRESHNESS_ANCHOR")
        self.assertEqual(row["key"], "MARK_INDEX_PRICE|BINANCE|@REQUIREMENT_ALPHA")

    def test_execution_mark_keeps_its_use_time_basis_and_d3_policy(self):
        execution = _Reference(requirement=_Requirement(feed=_Feed("MARK_INDEX_PRICE"), consumer_grade=_Feed(
            "EXECUTION"), max_freshness_ms=2000), sdk_requirement=_SdkReference(2000))
        sent = 10_000 * MS
        row = self._mark(execution, observed_ns=sent - 2300 * MS, labels={}, sent_ns=sent, now_ns=sent + 100 * MS)
        self.assertEqual((row["class"], row["reasons"]), ("UNSAFE", ["STALE_SERVED_WITHOUT_QUIET_PROOF"]))
        self.assertEqual((row["age_ms"], row["policy_ms"], row["key"]), (2400, 2000, "MARK_INDEX_PRICE|BINANCE|"))

    def test_anchor_follows_the_server_rule(self):
        anchor = _DRIVER.g6_reference_anchor_ns
        self.assertEqual(anchor([(5, {}), (7, {})]), 5)
        self.assertEqual(anchor([(5, {"freshness_basis": "PROVIDER_CONFIRMATION", "provider_confirmation_ns": "9"})]),
                         9)
        self.assertIsNone(anchor([(5, {"freshness_basis": "PROVIDER_CONFIRMATION"})]))
        self.assertIsNone(anchor([(5, {"freshness_basis": "SOURCE_EVENT"}),
                                  (6, {"freshness_basis": "PROVIDER_CONFIRMATION", "provider_confirmation_ns": "9"})]))
        self.assertIsNone(anchor([]))


class G6ProbeParityTests(unittest.TestCase):
    """The TS execution probe carries verbatim copies of the pure functions; both must agree on every case."""

    def test_policy_parity(self):
        cases = [(feed, interval, grade, req) for feed, interval in (("TRADE", None), ("QUOTE", None),
                                                                     ("MARK_INDEX_PRICE", None), ("BOOK_DELTA", None),
                                                                     ("BOOK_SNAPSHOT", None), ("BAR", "1m"),
                                                                     ("BAR", "5m"), ("FUNDING_RATE", None))
                 for grade in ("EXECUTION", "ALPHA", None, _Feed("EXECUTION")) for req in (None, 900, 2000, 60000)]
        for feed, interval, grade, req in cases:
            self.assertEqual(_DRIVER.g6_policy(feed, interval, grade=grade, requirement_max_ms=req),
                             _PROBE.g6_policy(feed, interval, grade=grade, requirement_max_ms=req))
        self.assertEqual(_DRIVER._G6_POLICY_MS, _PROBE._G6_POLICY_MS)
        self.assertEqual(_DRIVER.G6_D3_BASIS, _PROBE.G6_D3_BASIS)

    def test_classify_parity(self):
        import itertools
        axes = dict(feed=("QUOTE", "TRADE", "BOOK_DELTA", "MARK_INDEX_PRICE"), freshness_ms=(None, 100, 2500, 9000),
                    eligible=(True, False), state=(None, "LIVE", "GAPPED"), gap_open=(False, True),
                    complete=(True, False), event_recency_state=("LIVE", "STALE", None),
                    provider_session_state=("LIVE", "DISCONNECTED", "NOT_APPLICABLE", None),
                    session_liveness_ms=(None, 500, 5000), max_session_liveness_ms=(None, 2000),
                    observe=(True, False), on_change=(True, False), policy_ms=(None, 2000, 3000))
        names = list(axes)
        count = 0
        for values in itertools.product(*axes.values()):
            case = dict(zip(names, values))
            self.assertEqual(_DRIVER.g6_classify(**case), _PROBE.g6_classify(**case), case)
            count += 1
        self.assertGreater(count, 50000)

    def test_reference_parity(self):
        label_sets = [{}, {"freshness_basis": "PROVIDER_CONFIRMATION", "provider_confirmation_ns": str(9_500 * MS)},
                      {"freshness_basis": "PROVIDER_CONFIRMATION"}, {"freshness_basis": "SOURCE_EVENT"}]
        for grade in ("EXECUTION", "ALPHA", None):
            for labels in label_sets:
                for observed in (1, 7_000 * MS, 9_000 * MS):
                    observations = [(observed, labels)]
                    self.assertEqual(_DRIVER.g6_reference_anchor_ns(observations),
                                     _PROBE.g6_reference_anchor_ns(observations))
                    kwargs = dict(grade=grade, observations=observations, sent_ns=10_000 * MS,
                                  receipt_ns=10_300 * MS)
                    self.assertEqual(_DRIVER.g6_reference_age(**kwargs), _PROBE.g6_reference_age(**kwargs))
        for age in (None, 100, 2300, 70000):
            for policy in (None, 2000, 60000):
                for quiet in (True, False):
                    kwargs = dict(age_ms=age, policy_ms=policy, quiet_proven=quiet)
                    self.assertEqual(_DRIVER.g6_reference_classify(**kwargs), _PROBE.g6_reference_classify(**kwargs))

    def test_probe_rows_have_the_campaign_shape(self):
        rows = _PROBE.Rows()
        rows.view("QUOTE|OKX|", {"age_ms": 300, "policy_ms": 2000, "policy_basis": "D3_EXECUTION", "class": "ELIGIBLE",
                                 "reasons": []})
        rows.view("QUOTE|OKX|", {"age_ms": 2500, "policy_ms": 2000, "policy_basis": "D3_EXECUTION", "class": "UNSAFE",
                                 "reasons": ["AGE_BEYOND_POLICY"]})
        rows.refusal("TRADE|OKX|", "DATA_STALE")
        summary = rows.summary()
        quote = summary["QUOTE|OKX|"]
        self.assertEqual((quote["n"], quote["unsafe"], quote["max_age_ms"], quote["policy_basis"]),
                         (2, 1, 2500, "D3_EXECUTION"))
        self.assertEqual(quote["classes"], {"ELIGIBLE": 1, "UNSAFE": 1})
        self.assertEqual(quote["age_ms"]["n"], 2)
        self.assertEqual(summary["TRADE|OKX|"]["refusals"], {"DATA_STALE": 1})


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
        row = self._run(read)["QUOTE|OKX|@REQUIREMENT_ALPHA"]
        self.assertEqual((row["n"], row["refusals"], row["policy_basis"]), (0, {"RATE_LIMITED": 4},
                                                                            "REQUIREMENT_ALPHA"))

    def test_success_records_the_view(self):
        async def read(client, operation, products):
            _DRIVER._G6_VIEW.set([{"key": "QUOTE|OKX|", "age_ms": 120, "policy_ms": 2000, "class": "ELIGIBLE",
                                   "reasons": []}])
        row = self._run(read)["QUOTE|OKX|"]
        self.assertEqual((row["n"], row["classes"], row["max_age_ms"]), (4, {"ELIGIBLE": 4}, 120))


if __name__ == "__main__":
    unittest.main()
