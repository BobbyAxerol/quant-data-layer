from __future__ import annotations

import json
import unittest
from pathlib import Path

from qdl.adapters.binance.bar_edge import BinanceBarRawBinding
from qdl.query.contracts import (
    ConsumerGrade,
    DataRequirement,
    FeedType,
    RecoveryPolicy,
)
from qdl.runtime.provider_history import (
    ProviderBarHistorySource,
    ProviderHistoryUnavailable,
)
from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
BINANCE_ETH = "ee93fabf-68df-5b50-8924-51bf25a5a757"
OKX_ETH = "e49b54ae-c23d-5351-9e64-47934aac28f8"
VN30F1M = "d0ac3d4c-2374-5690-8af5-b970448f91d3"


def _binance_row(open_ms: int, close: str = "100.5") -> list:
    return [
        open_ms, "100", "101", "99", close, "10",
        open_ms + 59_999, "1005", 7, "5", "500", "0",
    ]


def _requirement(uid: str, **overrides) -> DataRequirement:
    values = dict(
        instrument_uid=uid,
        feed=FeedType.BAR,
        consumer_grade=ConsumerGrade.ALPHA,
        source_policy_id="crypto_primary_v2",
        interval="1m",
        warmup_limit=3,
        recovery=RecoveryPolicy.FRESH_SNAPSHOT,
    )
    values.update(overrides)
    return DataRequirement(**values)


class _Envelope:
    """Stands in for a RawProviderEnvelope produced by a venue fetcher."""

    def __init__(self, frame: dict, provider: str = "BINANCE_DIRECT"):
        self.raw_frame_bytes = json.dumps(frame, sort_keys=True).encode()
        self.provider = provider
        self.capture_id = b"\x01" * 16
        self.raw_frame_sha256 = b"\x02" * 32


class ProviderPassThroughTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.calls: list[dict] = []

    def _source(self, rows: list[list], interval: str = "1m", **kwargs):
        def fetcher(binding: BinanceBarRawBinding, *, limit: int, **rest):
            self.calls.append({"binding": binding, "limit": limit, **rest})
            return tuple(
                _Envelope({
                    "symbol": binding.native_symbol,
                    "interval": binding.interval,
                    "bar_origin": "BACKFILLED",
                    "row": row,
                })
                for row in rows
            )

        return ProviderBarHistorySource(
            self.catalog, binance_fetcher=fetcher, **kwargs
        )

    def test_it_serves_only_a_declared_fresh_snapshot_bar_request(self):
        source = self._source([])
        self.assertTrue(source.serves(_requirement(BINANCE_ETH)))
        self.assertFalse(
            source.serves(_requirement(BINANCE_ETH, feed=FeedType.TRADE, interval=None))
        )
        self.assertFalse(
            source.serves(
                _requirement(BINANCE_ETH, recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY)
            )
        )

    def test_a_vn_instrument_is_not_served_by_the_crypto_pass_through(self):
        source = self._source([])
        self.assertFalse(source.serves(_requirement(VN30F1M)))

    def test_an_undeclared_instrument_is_not_served(self):
        source = self._source([])
        self.assertFalse(
            source.serves(_requirement("00000000-0000-5000-8000-00000000dead"))
        )

    def test_history_returns_canonical_bars_for_the_declared_interval(self):
        rows = [_binance_row(1_787_385_300_000 + index * 60_000) for index in range(3)]
        envelopes = self._source(rows).history(_requirement(BINANCE_ETH))
        self.assertEqual(len(envelopes), 3)
        for envelope in envelopes:
            self.assertEqual(envelope.bar.interval, "1m")
            self.assertTrue(envelope.bar.is_final)
            self.assertEqual(envelope.instrument_uid, BINANCE_ETH)
            self.assertEqual(envelope.native_symbol, "ETHUSDT")

    def test_the_fetch_is_bounded_by_the_declared_warmup_limit(self):
        rows = [_binance_row(1_787_385_300_000 + index * 60_000) for index in range(2)]
        self._source(rows).history(_requirement(BINANCE_ETH, warmup_limit=2))
        self.assertEqual(self.calls[0]["limit"], 2)
        self.assertFalse(self.calls[0]["test_provenance"])

    def test_a_short_window_fails_closed(self):
        rows = [_binance_row(1_787_385_300_000)]
        with self.assertRaises(ProviderHistoryUnavailable):
            self._source(rows).history(_requirement(BINANCE_ETH, warmup_limit=3))

    def test_an_unsupported_interval_fails_closed(self):
        with self.assertRaises(ProviderHistoryUnavailable):
            self._source([]).history(_requirement(BINANCE_ETH, interval="1M"))

    def test_an_unbounded_warmup_limit_fails_closed(self):
        for limit in (0, 5000):
            with self.subTest(limit=limit):
                with self.assertRaises(ProviderHistoryUnavailable):
                    self._source([]).history(
                        _requirement(BINANCE_ETH, warmup_limit=limit)
                    )

    def test_a_requirement_it_does_not_serve_is_refused_not_answered(self):
        with self.assertRaises(ProviderHistoryUnavailable):
            self._source([]).history(
                _requirement(BINANCE_ETH, recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY)
            )

    def test_the_descriptor_carries_the_resolved_instrument_identity(self):
        rows = [_binance_row(1_787_385_300_000 + index * 60_000) for index in range(3)]
        self._source(rows).history(_requirement(BINANCE_ETH))
        binding = self.calls[0]["binding"]
        self.assertEqual(binding.native_symbol, "ETHUSDT")
        self.assertEqual(binding.market, "USDM")
        self.assertEqual(binding.interval, "1m")
        self.assertEqual(
            binding.instrument_catalog_revision, self.catalog.catalog_revision
        )


if __name__ == "__main__":
    unittest.main()
