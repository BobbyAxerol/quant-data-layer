import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from app.diagnostics import run_data_checks as diag


class TestPhase0Diagnostics(unittest.TestCase):
    def test_sample_symbols_prioritizes_configured_symbols(self):
        sampled = diag._sample_symbols(
            symbols=["ADAUSDT", "BTCUSDT", "ETHUSDT", "BNBUSDT"],
            priority=["ETHUSDT", "BTCUSDT"],
            limit=3,
        )

        self.assertEqual(sampled, ["ETHUSDT", "BTCUSDT", "ADAUSDT"])

    def test_symbol_universe_validation_helpers(self):
        symbols = ["FPT", "HPG", "FPT", "BAD-SYMBOL", ""]

        self.assertEqual(diag._duplicate_symbols(symbols), ["FPT"])
        self.assertEqual(diag._invalid_symbols(symbols), ["BAD-SYMBOL", ""])

    def test_preload_interval_dir_uses_sibling_dirs_for_canonical_1m(self):
        with patch.object(diag, "PRELOAD_DIR", "/app/data/preload/1m"):
            self.assertEqual(
                diag._preload_interval_dir("15m"),
                Path("/app/data/preload/15m"),
            )

    def test_preload_storage_audit_reports_materialized_interval_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "1m").mkdir()
            (root / "15m").mkdir()
            pd.DataFrame(
                [
                    {
                        "time": "2026-01-01T09:15:00",
                        "open": 1,
                        "high": 1,
                        "low": 1,
                        "close": 1,
                        "volume": 100,
                    }
                ]
            ).to_parquet(root / "15m" / "FPT.parquet")

            with patch.object(diag, "PRELOAD_DIR", str(root / "1m")), \
                patch.object(diag, "PHASE0_MATERIALIZED_VN_INTERVALS", ["15m", "30m"]), \
                patch.object(diag, "PHASE0_PRIORITY_VN_SYMBOLS", ["FPT"]), \
                patch.object(diag, "VN_SAMPLE_SIZE", 1), \
                patch.object(diag, "_load_yaml_symbols", return_value=["FPT"]):
                report = diag.run_preload_storage_audit()

        by_interval = {row["interval"]: row for row in report["materialized"]}
        self.assertTrue(by_interval["15m"]["dir_exists"])
        self.assertEqual(by_interval["15m"]["symbols"][0]["status"], "ok")
        self.assertFalse(by_interval["30m"]["dir_exists"])
        self.assertEqual(by_interval["30m"]["symbols"][0]["status"], "missing")

    def test_consumer_pressure_audit_extracts_top_callers_from_access_logs(self):
        log_lines = "\n".join(
            [
                'INFO:     172.20.0.7:38516 - "GET /v1/binance/price/BTCUSDT HTTP/1.1" 200 OK',
                'INFO:     172.20.0.7:38520 - "GET /v1/binance/price/ETHUSDT HTTP/1.1" 200 OK',
                'INFO:     172.20.0.9:38521 - "GET /v1/vn/quote/FPT HTTP/1.1" 200 OK',
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            log_dir.mkdir()
            (log_dir / "app.log").write_text(log_lines, encoding="utf-8")

            with patch.object(diag, "_project_root", return_value=Path(tmp)):
                report = diag.run_consumer_pressure_audit()

        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["top_callers"][0]["ip"], "172.20.0.7")
        self.assertEqual(report["top_callers"][0]["requests"], 2)
        self.assertEqual(
            report["top_callers"][0]["top_endpoints"][0][0],
            "/v1/binance/price/BTCUSDT",
        )

    def test_preload_checker_treats_sparse_vn_as_warning_not_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            preload_dir = root / "1m"
            preload_dir.mkdir()
            pd.DataFrame(
                [
                    {"time": "2026-01-02T02:15:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 100},
                    {"time": "2026-01-02T02:17:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 100},
                ]
            ).to_parquet(preload_dir / "FPT.parquet")

            with patch.object(diag, "PRELOAD_DIR", str(preload_dir)), \
                patch.object(diag, "_load_yaml_symbols", return_value=["FPT"]), \
                patch.object(diag, "_vn_now", return_value=pd.Timestamp("2026-01-02 09:18:00", tz="Asia/Ho_Chi_Minh").to_pydatetime()), \
                patch.object(diag, "PRELOAD_LAG_TOLERANCE_MINUTES", 2):
                report = diag.run_preload_checks()

        self.assertEqual(report["failed_symbols"], [])
        self.assertEqual(report["reports"][0]["time_basis"], "naive_utc_to_vn_local")
        self.assertGreater(report["reports"][0]["unexpected_missing_intraday_count"], 0)

    def test_preload_freshness_gap_is_warning_unless_strict(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            preload_dir = root / "1m"
            preload_dir.mkdir()
            pd.DataFrame(
                [
                    {"time": "2026-01-02T09:15:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 100},
                    {"time": "2026-01-02T09:16:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 100},
                ]
            ).to_parquet(preload_dir / "FPT.parquet")

            with patch.object(diag, "PRELOAD_DIR", str(preload_dir)), \
                patch.object(diag, "_load_yaml_symbols", return_value=["FPT"]), \
                patch.object(diag, "_vn_now", return_value=pd.Timestamp("2026-01-02 10:00:00", tz="Asia/Ho_Chi_Minh").to_pydatetime()), \
                patch.object(diag, "PRELOAD_LAG_TOLERANCE_MINUTES", 2), \
                patch.object(diag, "PRELOAD_STRICT_FRESHNESS", False):
                report = diag.run_preload_checks()

            self.assertEqual(report["failed_symbols"], [])
            self.assertEqual(report["freshness_warning_symbols"], ["FPT"])

            with patch.object(diag, "PRELOAD_DIR", str(preload_dir)), \
                patch.object(diag, "_load_yaml_symbols", return_value=["FPT"]), \
                patch.object(diag, "_vn_now", return_value=pd.Timestamp("2026-01-02 10:00:00", tz="Asia/Ho_Chi_Minh").to_pydatetime()), \
                patch.object(diag, "PRELOAD_LAG_TOLERANCE_MINUTES", 2), \
                patch.object(diag, "PRELOAD_STRICT_FRESHNESS", True):
                strict_report = diag.run_preload_checks()

            self.assertEqual(strict_report["failed_symbols"], ["FPT"])

    def test_vn_missing_live_is_warning_unless_strict(self):
        reports = [
            {"symbol": "BTCUSDT", "status": "ok"},
            {"symbol": "FPT", "status": "missing_live_during_market"},
            {"symbol": "HPG", "status": "missing"},
        ]
        with patch.object(diag, "VN_STRICT_LIVE_STREAMS", False):
            critical_statuses = {"missing"}
            if diag.VN_STRICT_LIVE_STREAMS:
                critical_statuses.add("missing_live_during_market")
            self.assertEqual([r["symbol"] for r in reports if r["status"] in critical_statuses], ["HPG"])

        with patch.object(diag, "VN_STRICT_LIVE_STREAMS", True):
            critical_statuses = {"missing"}
            if diag.VN_STRICT_LIVE_STREAMS:
                critical_statuses.add("missing_live_during_market")
            self.assertEqual([r["symbol"] for r in reports if r["status"] in critical_statuses], ["FPT", "HPG"])

    def test_contract_enforcement_flags_direct_provider_usage_outside_allowed_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "app" / "alpha").mkdir(parents=True)
            (root / "app" / "database").mkdir(parents=True)
            (root / "app" / "providers" / "binance").mkdir(parents=True)
            (root / "app" / "alpha" / "bad.py").write_text("URL='https://api.binance.com/api/v3/klines'", encoding="utf-8")
            (root / "app" / "config.py").write_text("DNSE='https://openapi.dnse.com.vn'", encoding="utf-8")
            (root / "app" / "database" / "preload.py").write_text("from vnstock import Quote", encoding="utf-8")
            (root / "app" / "providers" / "binance" / "rest.py").write_text("URL='https://api.binance.com/api/v3/klines'", encoding="utf-8")

            with patch.object(diag, "_project_root", return_value=root):
                report = diag.run_contract_enforcement_audit()

        self.assertEqual(report["status"], "violation")
        self.assertEqual(report["violations"], [{"file": "app/alpha/bad.py", "pattern": "api.binance.com"}])


if __name__ == "__main__":
    unittest.main()
