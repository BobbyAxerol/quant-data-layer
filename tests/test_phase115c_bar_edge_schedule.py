from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from qdl.adapters.intervals import canonical_interval_ms, latest_closed_boundary_ms
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge


def _edge(*sources: SimpleNamespace) -> StableBinanceBarEdge:
    edge = object.__new__(StableBinanceBarEdge)
    edge.repair_only = False
    edge._last_open_ms = {}
    edge.bindings = tuple((source, SimpleNamespace(runtime="BINANCE")) for source in sources)
    edge.okx_bindings = ()
    edge.warmup_rows = 1000
    edge.settlement_delay_seconds = 2.0
    edge._rest_fallback_active = True
    return edge


class Phase115CBarScheduleTests(unittest.TestCase):
    def test_history_bootstrap_runs_before_recurring_scheduler_wait(self) -> None:
        class StopAfterFirstWait:
            def __init__(self) -> None:
                self.waits: list[float] = []
                self.stopped = False

            def is_set(self) -> bool:
                return self.stopped

            def wait(self, seconds: float) -> bool:
                self.waits.append(seconds)
                self.stopped = True
                return True

        edge = object.__new__(StableBinanceBarEdge)
        edge.repair_only = False
        edge._history_bootstrap_active = True
        edge._history_bootstrapped = False
        edge._rest_fallback_active = False
        edge._stopped = StopAfterFirstWait()
        edge.clock = lambda: 1_785_600_120.0
        edge._next_ready_at = Mock(return_value=1_785_600_180.0)

        def bootstrap() -> int:
            edge._history_bootstrapped = True
            return 56

        edge.bootstrap_history = Mock(side_effect=bootstrap)
        edge.run_cycle = Mock()

        edge.run_forever()

        edge.bootstrap_history.assert_called_once_with()
        edge.run_cycle.assert_not_called()
        self.assertEqual(edge._stopped.waits, [60.0])

    def test_long_intervals_have_truthful_bounded_bootstrap_depth(self) -> None:
        edge = _edge()
        self.assertEqual(
            edge._bootstrap_rows_for(SimpleNamespace(interval="1m")), 1000
        )
        self.assertEqual(
            edge._bootstrap_rows_for(SimpleNamespace(interval="1d")), 1000
        )
        self.assertEqual(
            edge._bootstrap_rows_for(SimpleNamespace(interval="3d")), 365
        )
        self.assertEqual(
            edge._bootstrap_rows_for(SimpleNamespace(interval="1w")), 156
        )

    def test_kn_history_honors_demand_not_the_legacy_three_year_horizon(self) -> None:
        edge = _edge()
        edge.bar_readback = object()
        edge.warmup_rows = 10000
        edge.history_demand = {"daily": 5000, "weekly": 10000}
        self.assertEqual(edge._bootstrap_rows_for(SimpleNamespace(binding_id="daily", interval="1d")), 5000)
        self.assertEqual(edge._bootstrap_rows_for(SimpleNamespace(binding_id="weekly", interval="1w")), 10000)
        self.assertEqual(edge._bootstrap_rows_for(SimpleNamespace(binding_id="unclaimed", interval="1d")), 1)
        edge.warmup_rows = 2500
        self.assertEqual(edge._bootstrap_rows_for(SimpleNamespace(binding_id="daily", interval="1d")), 2500)

    def test_kn_weekly_checkpoint_never_requests_pre_epoch_rows(self) -> None:
        source = SimpleNamespace(binding_id="weekly", interval="1w")
        edge = _edge(source)
        edge.bar_readback = object()
        edge.canonical_cache_path = None
        edge.warmup_rows = 10000
        edge.history_bindings = edge.bindings
        edge.history_okx_bindings = ()
        edge._durable_final_bar_opens = Mock(side_effect=lambda _source, opens: opens)
        self.assertEqual(edge._checkpoint_history_gaps({"weekly": 1790035200000}), {})
        opens = edge._durable_final_bar_opens.call_args.args[1]
        self.assertTrue(opens)
        self.assertGreater(min(opens), 0)
        self.assertLess(len(opens), 10000)

    def test_kn_provider_gap_does_not_starve_other_venue_or_claim_completion(self) -> None:
        from qdl.adapters.intervals import BarHistoryGapError
        for provider in ("Binance", "OKX"):
            with self.subTest(provider=provider):
                bad = SimpleNamespace(binding_id="bad", interval="1d")
                good = SimpleNamespace(binding_id="good", interval="1d")
                edge = _edge(bad)
                edge.bar_readback = object()
                edge._history_bootstrapped = False
                edge._history_bootstrap_active = True
                edge._history_short = {}
                edge.history_bindings = edge.bindings
                edge.history_okx_bindings = ((good, SimpleNamespace(runtime="OTHER")),)
                edge._rebase_if_canonical_cache_generation_changed = Mock()
                edge._rebase_changed_products = Mock()
                edge._history_gate_open = Mock(return_value=True)
                edge._settled_observed_ms = Mock(return_value=1790000000000)
                edge.clock = Mock(return_value=1000)
                edge.warmup_rows = 2
                def fetch(source, *_args, **_kwargs):
                    if source.binding_id == "bad":
                        raise BarHistoryGapError(provider, "TEST_ONLY", "1d", 86400000, 3 * 86400000)
                    return (object(), object())
                edge._fetch_history = Mock(side_effect=fetch)
                def publish(source, *_args, **_kwargs):
                    edge._last_open_ms[source.binding_id] = 1790000000000
                    return 2
                edge._publish_history = Mock(side_effect=publish)
                self.assertEqual(edge.bootstrap_history(), 2)
                self.assertFalse(edge._history_bootstrapped)
                self.assertEqual(set(edge._last_open_ms), {"good"})
                self.assertEqual(edge.bootstrap_history(), 0)
                self.assertEqual(edge._fetch_history.call_count, 2)
                edge.clock.return_value = 1003
                edge._fetch_history.side_effect = None
                edge._fetch_history.return_value = (object(), object())
                self.assertEqual(edge.bootstrap_history(), 2)
                self.assertTrue(edge._history_bootstrapped)
                self.assertEqual(edge._history_retry, {})
                # Unknown/admission errors are never swallowed as a product gap.
                edge._last_open_ms.clear()
                edge._history_bootstrapped = False
                edge._fetch_history.side_effect = RuntimeError("TEST_ONLY 418 admission closed")
                with self.assertRaisesRegex(RuntimeError, "418"):
                    edge.bootstrap_history()

    def test_due_check_skips_unchanged_long_bar_without_provider_call(self) -> None:
        source = SimpleNamespace(binding_id="weekly", interval="1w")
        edge = _edge(source)
        observed = 1_785_600_123_000
        interval_ms = canonical_interval_ms("1w")
        latest_open = latest_closed_boundary_ms("1w", observed) - interval_ms
        edge._last_open_ms[source.binding_id] = latest_open
        self.assertFalse(edge._binding_is_due(source, observed_ms=observed))
        edge._settled_observed_ms = lambda: observed
        self.assertEqual(edge.run_cycle(), 0)

    def test_next_wake_uses_the_next_weekly_close_not_next_minute(self) -> None:
        source = SimpleNamespace(binding_id="weekly", interval="1w")
        edge = _edge(source)
        now = 1_785_600_123.0
        interval_ms = canonical_interval_ms("1w")
        edge._last_open_ms[source.binding_id] = (
            latest_closed_boundary_ms("1w", int(now * 1000)) - interval_ms
        )
        ready = edge._next_ready_at(now)
        self.assertGreaterEqual(ready - now, 60.0 * 60.0)
        self.assertLessEqual(ready - now, 8.0 * 86_400.0)


if __name__ == "__main__":
    unittest.main()
