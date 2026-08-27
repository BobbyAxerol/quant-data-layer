from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from qdl.adapters.intervals import canonical_interval_ms, latest_closed_boundary_ms
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge


def _edge(*sources: SimpleNamespace) -> StableBinanceBarEdge:
    edge = object.__new__(StableBinanceBarEdge)
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
        ready = edge._next_ready_at(now)
        self.assertGreaterEqual(ready - now, 60.0 * 60.0)
        self.assertLessEqual(ready - now, 8.0 * 86_400.0)


if __name__ == "__main__":
    unittest.main()
