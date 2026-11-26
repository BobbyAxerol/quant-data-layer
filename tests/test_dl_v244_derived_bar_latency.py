"""DL-DEF-2026-11-26-I: derived bars are not blocked by the REST poll, and a self-inflicted gap is never declared.

Incident (2026-11-26 00:00Z). Crossec stopped fail-closed because its 00:00Z decision came 4 min 48 s late.
- **REST cycle:** at 00:00Z the bar edge's REST poll read about 532 closed bars (483 of them 1d), paced by provider
  admission with 503 retries, until 00:02:40. The derived cycle runs after it in the same loop, so nothing derived
  was published until 00:03:05.
- **Self-inflicted gap:** that first pass was already past the 120 s grace. The 15m 23:45 bucket of 20 OKX
  instruments was then declared a gap: its 5m 23:55 constituent had been derived in the same pass and was not yet
  durable.
- **Cascade:** the chain healed one level per 30 s recheck. 1h landed at 00:04:47 and 12h at 00:06:25.

Covered, on the real catalogue, a real edge and an in-process cache with a projector that applies records only on
``tick()``:
- the midnight chain 1m (native) → 5m → 15m → 30m → 1h → 2h → 6h → 12h, first derived pass 200 s late: no gap is
  recorded, and the chain climbs one level per pass (0.5 s cadence), not per 30 s;
- the threaded loop: while a slow REST cycle blocks the main loop (sleeps, standing in for 503-paced reads), the
  derived thread publishes 1h within 60 s and 12h within 90 s of the boundary;
- a real missing native constituent still records a gap after the grace, and the gap propagates up the chain.
"""
from __future__ import annotations

import json
import threading
import time
import unittest

from qdl.adapters.intervals import canonical_interval_ms
from qdl.runtime import stable_bar_edge as edge_module
from tests.test_dl_v242_bar_edge_rebase_resume import ETH, ROW, DAY0, MINUTE, HOUR, Projector, _Base
from tests.test_kn_bar_readback import put_rows
from tests.test_v230_derived_bars import _envelope

MIDNIGHT = DAY0 + 24 * HOUR
CHAIN = (("5m", "1m"), ("15m", "5m"), ("30m", "15m"), ("1h", "30m"), ("2h", "1h"), ("6h", "2h"), ("12h", "6h"))
# The newest open each binding has before midnight; the midnight targets are the next ones.
WATERMARKS = {"1m": MIDNIGHT - MINUTE, "5m": MIDNIGHT - 10 * MINUTE, "15m": MIDNIGHT - 30 * MINUTE,
              "30m": MIDNIGHT - HOUR, "1h": MIDNIGHT - 2 * HOUR, "2h": MIDNIGHT - 4 * HOUR,
              "6h": MIDNIGHT - 12 * HOUR, "12h": MIDNIGHT - 24 * HOUR}
TARGETS = {"5m": MIDNIGHT - 5 * MINUTE, "15m": MIDNIGHT - 15 * MINUTE, "30m": MIDNIGHT - 30 * MINUTE,
           "1h": MIDNIGHT - HOUR, "2h": MIDNIGHT - 2 * HOUR, "6h": MIDNIGHT - 6 * HOUR, "12h": MIDNIGHT - 12 * HOUR}


class DelayedProjector(Projector):
    """Kafka + core + projector double whose records become durable only on ``tick()``."""

    def __init__(self, redis, sources) -> None:
        super().__init__(redis, sources)
        self.buffer: list = []
        self.lock = threading.Lock()
        self.immediate = False

    def publish_many(self, values):
        values = list(values)
        with self.lock:
            if self.immediate:
                return super().publish_many(values)
            self.buffer.extend(values)
        return tuple(range(len(values)))

    def tick(self) -> None:
        with self.lock:
            pending, self.buffer = self.buffer, []
            if pending:
                super().publish_many(pending)


class _Chain(_Base):
    def setUp(self) -> None:
        derived = []
        parents = {}
        pairs = [self.pair(ETH + "1m")]
        for interval, base in CHAIN:
            acquisition = self.derived(ETH + interval, base)
            pairs.append(self.pair(ETH + interval, acquisition))
            derived.append((self.sources[ETH + interval], acquisition))
            parents[ETH + interval] = ETH + base
        self.make_edge(tuple(pairs), derived=tuple(derived), parents=parents, warmup_rows=200,
                       now_ms=MIDNIGHT + 200_000)
        self.projector = DelayedProjector(self.redis, [source for source, _a in pairs])
        self.edge.publisher = self.projector
        for interval, last in WATERMARKS.items():
            step = canonical_interval_ms(interval)
            count = 60 if interval in {"1m", "5m"} else 6
            self.fill(ETH + interval, last, count, 1)
        # The 1m bars of the last minutes arrive natively (ingestor), up to 23:59.
        self.edge._last_open_ms.update({ETH + interval: last for interval, last in WATERMARKS.items()})
        self.edge._rebase_changed_products()  # record generations; nothing rebases
        self.edge._persist_state = lambda: None

    def published(self, interval: str) -> list[int]:
        return [item[1] for item in self.projector.published if item[0] == ETH + interval]

    def derived_pass(self) -> None:
        self.edge.run_derived_cycle()
        self.projector.tick()
        self.now[0] += edge_module._DERIVED_POLL_SECONDS


class MidnightChainTests(_Chain):
    def test_a_late_first_pass_climbs_the_chain_without_any_gap(self):
        passes = 0
        while TARGETS["12h"] not in self.published("12h") and passes < 40:
            self.derived_pass()
            passes += 1
        for interval, target in TARGETS.items():
            self.assertIn(target, self.published(interval), msg=interval)
        self.assertEqual(self.edge.derived_gaps_recorded, 0, "no self-inflicted gap")
        # One chain level per pass at the 0.5 s cadence (7 levels), never one level per 30 s recheck.
        self.assertLessEqual(passes, 2 * len(CHAIN))

    def test_the_old_wall_clock_rule_reproduces_the_incident_gap(self):
        """Contrast: without the pending-input rule the same late pass declares the 15m gap (v2.4.3 behaviour)."""

        self.edge._derived_pending_constituents = lambda *_args: frozenset()
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="WARNING") as logs:
            for _ in range(4):
                self.derived_pass()
        self.assertTrue(any("derived BAR gap binding=okx-swap-eth-usdt-swap-bar-15m" in line for line in logs.output))
        self.assertGreater(self.edge.derived_gaps_recorded, 0)

    def test_a_real_missing_native_constituent_still_records_a_gap_after_the_grace(self):
        lost = MIDNIGHT - 2 * MINUTE  # 23:58 never arrives
        lpk_rows = self.redis.hashes
        for key, fields in lpk_rows.items():
            if "|BAR|1m:" in key:
                fields.pop(str(lost), None)
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="WARNING") as logs:
            for _ in range(20):
                self.derived_pass()
        self.assertNotIn(TARGETS["5m"], self.published("5m"))
        gaps = [line for line in logs.output if "derived BAR gap binding" in line]
        self.assertTrue(any("bar-5m" in line for line in gaps))
        # The gap propagates: 15m 23:45 is missing a truly lost 5m bar, which is not pending.
        self.assertTrue(any("bar-15m" in line for line in gaps))
        self.assertNotIn(TARGETS["12h"], self.published("12h"))


class DerivedThreadTests(_Chain):
    def test_a_slow_rest_cycle_does_not_delay_derived_bars(self):
        """The threaded loop at the incident shape: the main loop is stuck in a slow REST cycle."""

        edge = self.edge
        self.projector.immediate = True  # durability lag is covered above; this measures scheduling
        boundary_wall = time.monotonic()
        rest_started = threading.Event()
        release = threading.Event()

        def slow_rest_cycle():
            rest_started.set()
            release.wait(10.0)  # stands in for 2.5 min of admission-paced 1d reads with 503 retries
            return 0

        edge._rest_fallback_active = True
        edge.run_cycle = slow_rest_cycle
        edge.bootstrap_history = lambda *args, **kwargs: 0
        edge.verify_serving = lambda: {}
        edge._native_recovery_active = False
        edge._heartbeat_path = None
        edge.clock = lambda: (MIDNIGHT + 5_000) / 1000 + (time.monotonic() - boundary_wall)
        main = threading.Thread(target=edge.run_forever, daemon=True)
        main.start()
        try:
            self.assertTrue(rest_started.wait(5.0))
            deadline = time.monotonic() + 8.0
            while TARGETS["12h"] not in self.published("12h") and time.monotonic() < deadline:
                time.sleep(0.05)
            elapsed = time.monotonic() - boundary_wall
            self.assertFalse(release.is_set())
            self.assertIn(TARGETS["1h"], self.published("1h"))
            self.assertIn(TARGETS["12h"], self.published("12h"))
            self.assertLess(elapsed + 5, 60, "1h within 60 s of the boundary")
            self.assertLess(elapsed + 5, 90, "12h within 90 s of the boundary")
            self.assertEqual(edge.derived_gaps_recorded, 0)
        finally:
            release.set()
            edge._stopped.set()
            main.join(timeout=15)
            thread = getattr(edge, "_derived_thread", None)
            if thread is not None:
                thread.join(timeout=5)
        self.assertFalse(main.is_alive())


if __name__ == "__main__":
    unittest.main()
