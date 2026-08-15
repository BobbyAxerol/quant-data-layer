import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import phase0_provider_smoke


class Phase0ProviderSmokeTests(unittest.TestCase):
    def test_all_checks_are_bounded_and_read_only(self):
        for _, path, _ in phase0_provider_smoke.CHECKS:
            self.assertNotIn("/run", path)
            self.assertNotIn("/append", path)
            self.assertNotIn("fresh=true", path.lower())
        history_paths = [path for _, path, _ in phase0_provider_smoke.CHECKS if "ohlcv" in path]
        self.assertTrue(all("limit=2" in path for path in history_paths))

    def test_report_fails_when_required_check_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "smoke.json"
            with (
                mock.patch.object(
                    phase0_provider_smoke,
                    "_http_json",
                    side_effect=[{"ok": False, "error": "offline"}]
                    + [{"ok": True, "status": 200}] * (len(phase0_provider_smoke.CHECKS) - 1),
                ),
                mock.patch("builtins.print"),
            ):
                exit_code = phase0_provider_smoke.main(["--output", str(output)])
            report = json.loads(output.read_text())

        self.assertEqual(exit_code, 1)
        self.assertEqual(report["summary"]["failed"], 1)
        self.assertEqual(report["summary"]["failures"], ["service_health"])


if __name__ == "__main__":
    unittest.main()
