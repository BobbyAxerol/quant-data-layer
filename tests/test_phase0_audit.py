import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.phase0_audit import _git_head, collect_source_plan, scan_consumers


class Phase0ConsumerInventoryTests(unittest.TestCase):
    def test_scan_reports_contracts_without_secret_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "consumer.py").write_text(
                """
from app.sdk import DataLayerClient
URL = 'http://data_layer:8100/v1/binance/price/BTCUSDT'
CHANNEL = 'stream:trade:binance_usdm:BTCUSDT'
DIRECT = 'https://api.binance.com/api/v3/time'
API_KEY = 'must-not-appear'
""".strip()
            )

            report = scan_consumers([("sample", root)])["sample"]

        encoded = json.dumps(report)
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["scanned_files"], 1)
        self.assertIn("/v1/binance/price/BTCUSDT", encoded)
        self.assertIn("stream:trade:binance_usdm:BTCUSDT", encoded)
        self.assertIn("consumer.py", report["sdk_files"])
        self.assertIn("consumer.py", report["direct_provider_files"]["binance_direct"])
        self.assertNotIn("must-not-appear", encoded)

    def test_scan_skips_logs_data_tests_and_missing_roots(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for ignored in ("logs", "data", "tests"):
                path = root / ignored
                path.mkdir()
                (path / "ignored.py").write_text("URL='/v1/should/not/appear'")
            (root / "runtime.py").write_text("URL='/v1/health'")

            report = scan_consumers([("sample", root), ("missing", root / "missing")])

        self.assertEqual(report["sample"]["scanned_files"], 1)
        self.assertEqual(report["sample"]["routes"], [{"value": "/v1/health", "references": 1}])
        self.assertEqual(report["missing"]["status"], "missing")

    def test_cli_runs_directly_for_host_inventory_without_app_dependencies(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "audit.json"
            result = subprocess.run(
                [
                    sys.executable,
                    str(repo_root / "scripts" / "phase0_audit.py"),
                    "--repo-root",
                    str(repo_root),
                    "--output",
                    str(output),
                ],
                cwd=repo_root,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_text())
            self.assertEqual(report["audit_mode"], "read_only")
            self.assertEqual(len(report["samples"]), 1)

    def test_missing_git_binary_does_not_break_runtime_audit(self):
        with mock.patch("scripts.phase0_audit.subprocess.run", side_effect=FileNotFoundError):
            self.assertIsNone(_git_head(Path("/tmp")))

    def test_source_plan_calculates_spot_off_shard_reduction(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "symbols_spot.json").write_text(json.dumps([f"S{i}" for i in range(201)]))
            (root / "symbols.json").write_text(json.dumps([f"F{i}" for i in range(101)]))

            plan = collect_source_plan(root, batch_size=100)

        self.assertEqual(plan["estimated_full_shards"], 10)
        self.assertEqual(plan["estimated_spot_off_shards"], 4)
        self.assertEqual(plan["estimated_shard_reduction_percent"], 60.0)


if __name__ == "__main__":
    unittest.main()
