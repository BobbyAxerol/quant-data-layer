"""Binance final-BAR settlement: publish only a closed bar the venue repeated.

Measured 2026-09-16: after a 1m close, Binance kline replicas disagree about the
same closed bar for roughly five seconds (observed trade counts 3155/3232/3263
cycling) before converging. A single read at close + 0.10 s captured the partial
bar and the lane never revised it.
"""

from __future__ import annotations

import unittest

from qdl.adapters.binance.bar_edge import (
    BinanceBarRawBinding,
    fetch_settled_closed_bar_raw_envelope,
)

INTERVAL_MS = 60_000
OPEN_MS = 1_789_000_000_000 - (1_789_000_000_000 % INTERVAL_MS) - INTERVAL_MS
OBSERVED_MS = OPEN_MS + INTERVAL_MS + 100


def _row(trades: int, volume: str, close: str = "75800.0") -> list:
    return [OPEN_MS, "75700.0", "75900.0", "75600.0", close, volume,
            OPEN_MS + INTERVAL_MS - 1, "1000.0", trades, "1.0", "2.0", "0"]


def _binding() -> BinanceBarRawBinding:
    return BinanceBarRawBinding(
        market="USDM",
        product_type="PERPETUAL",
        native_symbol="BTCUSDT",
        interval="1m",
        subscription_id="binance-usdm-btcusdt-bar-stable-001",
        source_session_id="session-1",
        connection_generation=1,
        lease_epoch=1,
        authority_revision=1,
        partition_plan_epoch=1,
        adapter_version="binance-usdm/2.0.0",
        config_revision=1,
        instrument_catalog_revision=9,
    )


class _Venue:
    """Replays a scripted sequence of kline answers, one per read."""

    def __init__(self, sequence):
        self.sequence = list(sequence)
        self.reads = 0

    def __call__(self, *args, **kwargs):
        rows = self.sequence[min(self.reads, len(self.sequence) - 1)]
        self.reads += 1
        return {"data": [list(r) for r in rows]}


class BinanceFinalBarSettlementTests(unittest.TestCase):
    def setUp(self):
        self.slept: list[float] = []

    def _fetch(self, sequence, **kwargs):
        venue = _Venue(sequence)
        envelope, settlement = fetch_settled_closed_bar_raw_envelope(
            _binding(), now_ms=OBSERVED_MS, fetcher=venue,
            sleep=self.slept.append, **kwargs,
        )
        return envelope, settlement, venue

    def test_a_repeated_row_is_published_after_two_agreeing_reads(self):
        settled = _row(3263, "121.527")
        _envelope, settlement, venue = self._fetch([[settled], [settled]])
        self.assertEqual(venue.reads, 2)
        self.assertEqual(settlement, {"reads": 2, "distinct_rows": 1, "confirmations": 2})
        self.assertEqual(self.slept, [1.0])

    def test_disagreeing_replicas_are_read_until_they_converge(self):
        partial_a, partial_b, settled = _row(3155, "120.379"), _row(3232, "121.090"), _row(3263, "121.527")
        _envelope, settlement, venue = self._fetch(
            [[partial_a], [partial_b], [settled], [partial_a], [settled], [settled]]
        )
        self.assertEqual(venue.reads, 6)
        self.assertEqual(settlement["distinct_rows"], 5)
        self.assertEqual(settlement["reads"], 6)

    def test_the_published_row_is_exactly_one_the_venue_returned(self):
        partial, settled = _row(3155, "120.379"), _row(3263, "121.527")
        envelope, _settlement, _venue = self._fetch([[partial], [settled], [settled]])
        frame = envelope.raw_frame_bytes.decode("utf-8")
        self.assertIn("121.527", frame)
        self.assertNotIn("120.379", frame)

    def test_a_venue_that_never_settles_fails_closed_without_publishing(self):
        flapping = [[_row(3155, "120.379")], [_row(3232, "121.090")]] * 5
        with self.assertRaises(RuntimeError) as raised:
            self._fetch(flapping, max_reads=6)
        self.assertIn("did not settle", str(raised.exception))
        self.assertIn("BTCUSDT", str(raised.exception))

    def test_one_confirmation_preserves_the_single_read_behaviour(self):
        _envelope, settlement, venue = self._fetch([[_row(3155, "120.379")]], confirmations=1)
        self.assertEqual(venue.reads, 1)
        self.assertEqual(settlement["reads"], 1)
        self.assertEqual(self.slept, [])

    def test_invalid_settlement_parameters_are_refused(self):
        for kwargs in ({"confirmations": 0}, {"confirmations": 3, "max_reads": 2}, {"confirm_interval_seconds": -1.0}):
            with self.subTest(**kwargs):
                with self.assertRaises(ValueError):
                    self._fetch([[_row(1, "1.0")]], **kwargs)

    def test_no_closed_bar_is_an_explicit_error(self):
        open_bar = [[OPEN_MS + INTERVAL_MS, "1", "1", "1", "1", "1", OPEN_MS + 2 * INTERVAL_MS - 1, "1", 1, "1", "1", "0"]]
        with self.assertRaises(RuntimeError) as raised:
            self._fetch([open_bar])
        self.assertIn("no closed bar", str(raised.exception))


class EdgeWiringTests(unittest.TestCase):
    def test_edge_validates_the_settlement_parameters(self):
        from qdl.runtime.stable_bar_edge import StableBinanceBarEdge
        import inspect
        signature = inspect.signature(StableBinanceBarEdge.__init__)
        for name, default in (("final_settlement_confirmations", 2), ("final_settlement_interval_seconds", 1.0), ("final_settlement_max_reads", 8)):
            self.assertIn(name, signature.parameters)
            self.assertEqual(signature.parameters[name].default, default)
        source = inspect.getsource(StableBinanceBarEdge.__init__)
        self.assertIn("stable BAR settled confirmations must be between 1 and 5", source)

    def test_binance_lane_uses_the_settled_read_and_okx_is_untouched(self):
        import inspect
        from qdl.runtime import stable_bar_edge
        source = inspect.getsource(stable_bar_edge.StableBinanceBarEdge._fetch_latest)
        self.assertIn("fetch_settled_closed_bar_raw_envelope", source)
        self.assertIn("fetch_okx_latest", source)
        self.assertNotIn("fetch_latest_closed_bar_raw_envelope(\n", source)


if __name__ == "__main__":
    unittest.main()
