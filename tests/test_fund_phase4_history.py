from __future__ import annotations

import tempfile
import unittest
from datetime import time
from pathlib import Path

import pandas as pd

from qdl.history import (
    AtomicParquetCatalog,
    BarRecord,
    LocalObjectStore,
    SessionWindow,
    SnapshotConflict,
    aggregate_bars,
    select_revisions,
)
from qdl.history.vn_migration import interpret_legacy_vn_timestamp, migrate_vn_parquet


def bar(index: int, **overrides) -> BarRecord:
    values = {
        "instrument_uid": "uid",
        "instrument_id": "TEST.MARKET.EQUITY.X",
        "instrument_revision": 1,
        "source_id": "provider",
        "source_role": "PRIMARY",
        "interval": "1m",
        "open_time_ns": 1_704_099_600_000_000_000 + index * 60_000_000_000,
        "close_time_ns": 1_704_099_660_000_000_000 + index * 60_000_000_000,
        "open": str(100 + index),
        "high": str(102 + index),
        "low": str(99 + index),
        "close": str(101 + index),
        "volume": str(10 + index),
        "trade_count": index + 1,
        "is_final": True,
        "revision": 0,
        "origin": "VENUE_NATIVE",
        "normalizer_version": "qdl/phase4",
        "config_revision": 1,
        "raw_lineage_json": f'{{"event":{index}}}',
    }
    values.update(overrides)
    return BarRecord(**values)


class BarOracleTests(unittest.TestCase):
    def test_ohlcv_first_max_min_last_sum_and_session_boundary(self):
        records = [bar(i) for i in range(5)]
        aggregated = aggregate_bars(
            records,
            output_interval="5m",
            interval_minutes=5,
            timezone_name="UTC",
            sessions=(SessionWindow(time(9), time(17)),),
        )
        self.assertEqual(len(aggregated), 1)
        result = aggregated[0]
        self.assertEqual(result.open, "100")
        self.assertEqual(result.high, "106")
        self.assertEqual(result.low, "99")
        self.assertEqual(result.close, "105")
        self.assertEqual(result.volume, "60")
        self.assertEqual(result.trade_count, 15)

    def test_revision_selection_is_explicit_and_conflict_fails(self):
        original = bar(0)
        revised = bar(0, close="102", revision=1, origin="RECONCILED")
        self.assertEqual(select_revisions([original, revised]), [revised])
        with self.assertRaisesRegex(ValueError, "conflicting"):
            select_revisions([original, bar(0, close="100")])

    def test_daily_bucket_respects_dst_timezone(self):
        # 2024-03-10 spans the America/New_York DST transition (23-hour day).
        first = bar(0, open_time_ns=1_710_072_000_000_000_000,
                    close_time_ns=1_710_072_060_000_000_000)
        daily = aggregate_bars(
            [first], output_interval="1d", interval_minutes=None,
            timezone_name="America/New_York",
        )[0]
        self.assertEqual(daily.close_time_ns - daily.open_time_ns, 23 * 3600 * 1_000_000_000)


class AtomicCatalogTests(unittest.TestCase):
    def test_crash_boundaries_expose_old_or_new_snapshot_never_partial(self):
        with tempfile.TemporaryDirectory() as directory:
            catalog = AtomicParquetCatalog(LocalObjectStore(Path(directory)))
            first = catalog.commit(
                "market_data/bar_v2", [bar(0)], source_cursor_start="0",
                source_cursor_end="1", normalizer_version="qdl/phase4",
                config_revision=1, expected_parent_snapshot_id=None,
            )
            for boundary in ("after_data", "after_manifest"):
                with self.assertRaisesRegex(RuntimeError, "injected crash"):
                    catalog.commit(
                        "market_data/bar_v2", [bar(0), bar(1)],
                        source_cursor_start="2", source_cursor_end="3",
                        normalizer_version="qdl/phase4", config_revision=1,
                        expected_parent_snapshot_id=first.snapshot_id,
                        crash_at=boundary,
                    )
                self.assertEqual(catalog.current("market_data/bar_v2").snapshot_id, first.snapshot_id)
                self.assertEqual(catalog.read(dataset_id="market_data/bar_v2"), [bar(0)])
            second = catalog.commit(
                "market_data/bar_v2", [bar(0), bar(1)], source_cursor_start="2",
                source_cursor_end="3", normalizer_version="qdl/phase4",
                config_revision=1, expected_parent_snapshot_id=first.snapshot_id,
            )
            self.assertEqual(catalog.current("market_data/bar_v2").snapshot_id, second.snapshot_id)
            self.assertEqual(len(catalog.read(dataset_id="market_data/bar_v2")), 2)

    def test_concurrent_parent_mismatch_fails_without_moving_head(self):
        with tempfile.TemporaryDirectory() as directory:
            catalog = AtomicParquetCatalog(LocalObjectStore(Path(directory)))
            head = catalog.commit(
                "bars", [bar(0)], source_cursor_start="0", source_cursor_end="1",
                normalizer_version="qdl/phase4", config_revision=1,
                expected_parent_snapshot_id=None,
            )
            with self.assertRaises(SnapshotConflict):
                catalog.commit(
                    "bars", [bar(1)], source_cursor_start="1", source_cursor_end="2",
                    normalizer_version="qdl/phase4", config_revision=1,
                    expected_parent_snapshot_id=None,
                )
            self.assertEqual(catalog.current("bars").snapshot_id, head.snapshot_id)


class VnMigrationTests(unittest.TestCase):
    def test_mixed_legacy_utc_and_vn_naive_timestamps_are_explicit_not_fabricated(self):
        utc_ns, utc_mode = interpret_legacy_vn_timestamp(
            pd.Timestamp("2026-06-01 02:00:00"), derivative=True
        )
        local_ns, local_mode = interpret_legacy_vn_timestamp(
            pd.Timestamp("2026-07-01 09:00:00"), derivative=True
        )
        self.assertEqual(utc_ns, local_ns - 30 * 24 * 3600 * 1_000_000_000)
        self.assertEqual(utc_mode, "LEGACY_NAIVE_UTC")
        self.assertEqual(local_mode, "LEGACY_NAIVE_ASIA_HO_CHI_MINH")
        with self.assertRaisesRegex(ValueError, "ambiguous/invalid"):
            interpret_legacy_vn_timestamp(pd.Timestamp("2026-07-01 20:00:00"), derivative=True)

    def test_sparse_source_rows_remain_sparse(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "VN30F1M.parquet"
            pd.DataFrame([
                {"time": "2026-07-01 09:00:00", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10},
                {"time": "2026-07-01 09:02:00", "open": 2, "high": 3, "low": 2, "close": 3, "volume": 20},
            ]).to_parquet(path, index=False)
            rows, report = migrate_vn_parquet(
                path, instrument_uid="uid", instrument_id="DNSE.VN.FUTURE.VN30F1M",
                interval="1m", interval_minutes=1, source_id="legacy",
                derivative=True, normalizer_version="phase4", config_revision=1,
            )
            self.assertEqual(len(rows), 2)
            self.assertEqual(report["fabricated_rows"], 0)
            self.assertEqual(rows[1].open_time_ns - rows[0].open_time_ns, 120_000_000_000)

    def test_mixed_timestamp_exact_duplicate_is_deduplicated_with_full_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "VN30F1M.parquet"
            pd.DataFrame([
                {"time": "2026-07-01 02:00:00", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10},
                {"time": "2026-07-01 09:00:00", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10},
            ]).to_parquet(path, index=False)
            rows, report = migrate_vn_parquet(
                path, instrument_uid="uid", instrument_id="DNSE.VN.FUTURE.VN30F1M",
                interval="1m", interval_minutes=1, source_id="legacy",
                derivative=True, normalizer_version="phase4", config_revision=1,
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(report["exact_duplicate_groups"], 1)
            self.assertEqual(report["exact_duplicate_rows_removed"], 1)
            self.assertIn('"source_rows":[', rows[0].raw_lineage_json)
            self.assertEqual(
                rows[0].timestamp_interpretation, "LEGACY_NAIVE_ASIA_HO_CHI_MINH"
            )

    def test_mixed_timestamp_conflicting_revision_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "VN30F1M.parquet"
            pd.DataFrame([
                {"time": "2026-07-01 02:00:00", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10},
                {"time": "2026-07-01 09:00:00", "open": 1, "high": 3, "low": 1, "close": 2, "volume": 10},
            ]).to_parquet(path, index=False)
            with self.assertRaisesRegex(ValueError, "conflicting OHLCV revisions"):
                migrate_vn_parquet(
                    path, instrument_uid="uid", instrument_id="DNSE.VN.FUTURE.VN30F1M",
                    interval="1m", interval_minutes=1, source_id="legacy",
                    derivative=True, normalizer_version="phase4", config_revision=1,
                )


if __name__ == "__main__":
    unittest.main()
