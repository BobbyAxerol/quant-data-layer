import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from app.database import preload


class TestPreloadMaterialization(unittest.TestCase):
    def _sample_1m(self):
        return pd.DataFrame(
            [
                {"time": "2026-01-02 09:15:00", "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100},
                {"time": "2026-01-02 09:16:00", "open": 10.5, "high": 12, "low": 10, "close": 11, "volume": 200},
                {"time": "2026-01-02 09:29:00", "open": 11, "high": 13, "low": 8, "close": 12, "volume": 300},
                {"time": "2026-01-02 09:30:00", "open": 20, "high": 21, "low": 19, "close": 20.5, "volume": 400},
                {"time": "2026-01-02 09:31:00", "open": 20.5, "high": 22, "low": 18, "close": 21, "volume": 500},
            ]
        )

    def test_resample_15m_ohlcv_rules(self):
        result = preload.resample_vn_ohlcv(self._sample_1m(), "15m", "FPT")

        self.assertEqual(len(result), 2)
        first = result.iloc[0]
        self.assertEqual(first["time"], pd.Timestamp("2026-01-02 09:15:00"))
        self.assertEqual(first["open"], 10)
        self.assertEqual(first["high"], 13)
        self.assertEqual(first["low"], 8)
        self.assertEqual(first["close"], 12)
        self.assertEqual(first["volume"], 600)

    def test_normalize_preload_interval_aliases(self):
        self.assertEqual(preload.normalize_preload_interval("5min"), "5m")
        self.assertEqual(preload.normalize_preload_interval("10"), "10m")
        self.assertEqual(preload.normalize_preload_interval("15min"), "15m")
        self.assertEqual(preload.normalize_preload_interval("60m"), "1h")
        self.assertEqual(preload.normalize_preload_interval("240m"), "4h")
        with self.assertRaises(ValueError):
            preload.normalize_preload_interval("2m")

    def test_materialize_symbol_intervals_writes_sibling_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            one_min_dir = root / "1m"
            one_min_dir.mkdir()
            self._sample_1m().to_parquet(one_min_dir / "FPT.parquet", index=False)

            report = preload.materialize_symbol_intervals(
                "FPT",
                intervals=("5m", "10m", "15m", "30m", "1h", "4h"),
                preload_dir=str(one_min_dir),
            )

            self.assertTrue((root / "5m" / "FPT.parquet").exists())
            self.assertTrue((root / "10m" / "FPT.parquet").exists())
            self.assertTrue((root / "15m" / "FPT.parquet").exists())
            self.assertTrue((root / "30m" / "FPT.parquet").exists())
            self.assertTrue((root / "1h" / "FPT.parquet").exists())
            self.assertTrue((root / "4h" / "FPT.parquet").exists())
            self.assertEqual(
                [row["interval"] for row in report["intervals"]],
                ["5m", "10m", "15m", "30m", "1h", "4h"],
            )

            df_15m = pd.read_parquet(root / "15m" / "FPT.parquet")
            self.assertEqual(len(df_15m), 2)

    def test_read_preload_data_lazy_materializes_missing_interval(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            one_min_dir = root / "1m"
            one_min_dir.mkdir()
            self._sample_1m().to_parquet(one_min_dir / "FPT.parquet", index=False)

            with patch.object(preload, "PRELOAD_DIR", str(one_min_dir)):
                df = preload.read_preload_data("FPT", interval="15m", limit=10)

            self.assertEqual(len(df), 2)
            self.assertTrue((root / "15m" / "FPT.parquet").exists())

    def test_preload_needs_topup_only_for_existing_canonical(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            one_min_dir = root / "1m"
            one_min_dir.mkdir()
            self._sample_1m().to_parquet(one_min_dir / "FPT.parquet", index=False)

            now = pd.Timestamp("2026-01-02 10:00:00", tz="Asia/Ho_Chi_Minh").to_pydatetime()
            report = preload.preload_needs_topup(
                "FPT",
                now_local=now,
                preload_dir=str(one_min_dir),
                max_lag_minutes=1,
            )

            self.assertTrue(report["needs_topup"])
            self.assertEqual(report["reason"], "stale")

            missing = preload.preload_needs_topup(
                "MISSING",
                now_local=now,
                preload_dir=str(one_min_dir),
            )
            self.assertFalse(missing["needs_topup"])
            self.assertEqual(missing["reason"], "canonical_missing")


if __name__ == "__main__":
    unittest.main()
