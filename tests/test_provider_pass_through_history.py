from __future__ import annotations

import json
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

from qdl.adapters.binance.bar_edge import BinanceBarRawBinding
from qdl.query.contracts import (
    ConsumerGrade,
    DataRequirement,
    FeedType,
    RecoveryPolicy,
)
from qdl.runtime.provider_history import (
    PASS_THROUGH_QUALITY_FLAG,
    PASS_THROUGH_STREAM_CURSOR,
    ProviderBarHistorySource,
    ProviderHistoryUnavailable,
)
from qdl.query.results import CoverageStatus
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_source import StableConsumerCursorIssuer

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


class PassThroughHistoryResultTests(ProviderPassThroughTests):
    """The response must be unable to pass as authoritative output."""

    DIGEST = "a" * 64

    def _result(self, count: int = 3, **overrides):
        rows = [
            _binance_row(1_787_385_300_000 + index * 60_000)
            for index in range(count)
        ]
        source = self._source(rows)
        return source.history_result(
            _requirement(BINANCE_ETH, warmup_limit=count, **overrides),
            schema_digest=self.DIGEST,
        )

    def test_no_item_is_ever_execution_eligible(self):
        result = self._result()
        self.assertTrue(result.items)
        for item in result.items:
            self.assertFalse(item.quality.execution_eligible)
            self.assertFalse(item.source.authoritative)

    def test_every_item_is_flagged_as_pass_through(self):
        for item in self._result().items:
            self.assertIn(PASS_THROUGH_QUALITY_FLAG, item.quality.flags)

    def test_the_cursor_is_an_explicit_non_resumable_sentinel(self):
        result = self._result()
        self.assertEqual(result.stream_cursor, PASS_THROUGH_STREAM_CURSOR)
        self.assertEqual(result.watermark_offset, 0)
        for item in result.items:
            self.assertEqual(item.cursor, PASS_THROUGH_STREAM_CURSOR)
            self.assertEqual(item.watermark_offset, 0)

    def test_cursor_issuer_preserves_explicit_no_replay_history_and_item(self):
        handoff = Mock()
        issuer = StableConsumerCursorIssuer(handoff, self.catalog, ttl_seconds=60)
        requirement = _requirement(BINANCE_ETH)
        result = self._result()

        self.assertIs(
            issuer.bind_history(requirement, result, consumer_id="alpha-test"),
            result,
        )
        self.assertIs(
            issuer.bind_item(requirement, result.items[-1], consumer_id="alpha-test"),
            result.items[-1],
        )
        handoff.issue.assert_not_called()

    def test_cursor_issuer_rejects_an_inconsistent_no_replay_sentinel(self):
        issuer = StableConsumerCursorIssuer(Mock(), self.catalog, ttl_seconds=60)
        result = self._result()
        with self.assertRaisesRegex(ValueError, "FRESH_SNAPSHOT and zero watermark"):
            issuer.bind_history(
                _requirement(BINANCE_ETH),
                replace(result, watermark_offset=1),
                consumer_id="alpha-test",
            )
        with self.assertRaisesRegex(ValueError, "FRESH_SNAPSHOT and zero watermark"):
            issuer.bind_item(
                replace(_requirement(BINANCE_ETH), recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY),
                result.items[-1],
                consumer_id="alpha-test",
            )

    def test_the_snapshot_id_is_deterministic_for_the_same_window(self):
        first = self._result()
        second = self._result()
        self.assertEqual(first.snapshot_id, second.snapshot_id)
        self.assertNotEqual(self._result(count=2).snapshot_id, first.snapshot_id)

    def test_coverage_and_ordering_follow_the_fetched_window(self):
        result = self._result()
        self.assertEqual(result.coverage, CoverageStatus.FULL)
        opens = [item.payload["open_time_ns"] for item in result.items]
        self.assertEqual(opens, sorted(opens))
        self.assertEqual(result.data_as_of_ns, result.items[-1].payload["close_time_ns"])

    def test_decimal_text_and_units_come_from_the_shared_builder(self):
        item = self._result().items[0]
        self.assertEqual(item.payload["open"], "100")
        self.assertEqual(item.payload["close"], "100.5")
        self.assertTrue(item.payload["is_final"])
        self.assertIn("volume_unit", item.payload)
        self.assertEqual(item.interval, "1m")

    def test_a_window_older_than_the_declared_freshness_is_reported_stale(self):
        result = self._result(max_freshness_ms=1)
        self.assertEqual(result.items[0].quality.state, "STALE")


if __name__ == "__main__":
    unittest.main()
