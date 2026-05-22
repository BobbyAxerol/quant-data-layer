import unittest
from datetime import datetime, timedelta

import pandas as pd

from app.alpha.strategy import MovingAverageCrossAlpha


class DummyClient:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def fetch_preload(self, symbol: str, limit=None):
        return self.df


class TestMovingAverageCrossAlpha(unittest.TestCase):
    def setUp(self):
        start = datetime(2026, 1, 1, 9, 0)
        rows = []
        for i in range(120):
            timestamp = start + timedelta(minutes=i)
            close = 100.0 + i
            rows.append({
                "time": timestamp,
                "open": close - 0.5,
                "high": close + 0.5,
                "low": close - 1.0,
                "close": close,
                "volume": 100,
            })

        self.df = pd.DataFrame(rows)
        self.alpha = MovingAverageCrossAlpha(
            client=DummyClient(self.df),
            symbol="SSI",
            high_tf="15min",
            fast=2,
            slow=4,
        )

    def test_resample_high_tf(self):
        high_tf_df = self.alpha._resample_high_tf(self.df)
        self.assertEqual(len(high_tf_df), 8)
        self.assertTrue((high_tf_df["close"] > 0).all())

    def test_calculate_signals(self):
        high_tf_df = self.alpha._resample_high_tf(self.df)
        signal_df = self.alpha._calculate_signals(high_tf_df)
        self.assertIn("fast_sma", signal_df.columns)
        self.assertIn("slow_sma", signal_df.columns)
        self.assertIn("signal", signal_df.columns)
        self.assertEqual(signal_df["signal"].iloc[0], 0)
        self.assertTrue((signal_df["fast_sma"] >= signal_df["slow_sma"]).all())

    def test_prepare_warmup_populates_signal_df(self):
        self.alpha.prepare_warmup()
        self.assertIsNotNone(self.alpha.signal_df)
        self.assertGreater(len(self.alpha.signal_df), 0)


if __name__ == "__main__":
    unittest.main()
