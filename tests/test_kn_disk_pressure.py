"""Synthetic policy regressions; no Docker or production filesystem mutation."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.kn_disk_pressure import DiskPressure, sample_disk


def observation(stamp=0, size=800, inodes=800, **updates):
    row = dict(device=1, monotonic=stamp, total_bytes=1000,
               available_bytes=size, total_inodes=1000, available_inodes=inodes)
    row.update(updates)
    return row


class DiskPressureTests(unittest.TestCase):
    def policy(self):
        return DiskPressure(100, 200, 50, 100, 10, 20)

    def test_statvfs_uses_unprivileged_capacity(self):
        fs = SimpleNamespace(f_blocks=100, f_frsize=4096, f_bavail=20,
                             f_bfree=30, f_files=1000, f_favail=50, f_ffree=60)
        with patch("scripts.kn_disk_pressure.os.stat", return_value=SimpleNamespace(st_dev=2)), \
                patch("scripts.kn_disk_pressure.os.statvfs", return_value=fs), \
                patch("scripts.kn_disk_pressure.time.monotonic", return_value=42):
            self.assertEqual(sample_disk("/test"), dict(device=2, monotonic=42,
                total_bytes=409600, available_bytes=81920, total_inodes=1000, available_inodes=50))

    def test_missing_path_and_real_temporary_filesystem(self):
        with tempfile.TemporaryDirectory() as folder:
            self.assertIsNone(sample_disk(str(Path(folder) / "absent")))
            row = sample_disk(folder)
            self.assertGreater(row["total_bytes"], 0)
            self.assertGreaterEqual(row["available_inodes"], 0)

    def test_baseline_unknown_then_stable_healthy(self):
        p = self.policy()
        self.assertEqual(p.observe("/a", observation())["reasons"], ["DISK_OBSERVATION_UNKNOWN"])
        result = p.observe("/a", observation(5))
        self.assertFalse(result["pressure"])
        self.assertEqual(result["metrics"]["bytes"], {"growth_per_second": 0, "seconds_to_reserve": None})

    def test_byte_and_inode_exhaustion_inclusive(self):
        for unit, value, code in (("available_bytes", 100, "DISK_BYTES_RESERVE"),
                                  ("available_bytes", 0, "DISK_BYTES_RESERVE"),
                                  ("available_inodes", 50, "DISK_INODES_RESERVE"),
                                  ("available_inodes", 0, "DISK_INODES_RESERVE")):
            with self.subTest(unit=unit, value=value):
                p = self.policy()
                p.observe("/a", observation())
                self.assertIn(code, p.observe("/a", observation(5, **{unit: value}))["reasons"])

    def test_growth_measured_from_monotonic_not_wall_time(self):
        p = self.policy()
        p.observe("/a", observation())
        p.observe("/a", observation(5))
        result = p.observe("/a", observation(10, size=500, inodes=500))
        self.assertEqual(result["metrics"]["bytes"]["growth_per_second"], 60)
        self.assertAlmostEqual(result["metrics"]["bytes"]["seconds_to_reserve"], 400 / 60)
        self.assertIn("DISK_BYTES_TIME_TO_RESERVE", result["reasons"])
        self.assertIn("DISK_INODES_TIME_TO_RESERVE", result["reasons"])

    def test_missing_malformed_and_nonfinite_fail_closed(self):
        bad = [None, {}, [], observation(5, available_bytes=-1),
               observation(5, available_bytes=1001), observation(5, available_bytes=True),
               observation(5, total_bytes=0), observation(5, total_inodes=0),
               observation(float("nan")), observation(float("inf")),
               observation(5, available_inodes="100"), observation(5, device=None)]
        for value in bad:
            with self.subTest(value=value):
                p = self.policy()
                p.observe("/a", observation())
                self.assertIn("DISK_OBSERVATION_UNKNOWN", p.observe("/a", value)["reasons"])
                self.assertIn("DISK_OBSERVATION_UNKNOWN", p.observe("/a", observation(10))["reasons"])

    def test_observation_journal_is_strict_json_and_schema_bounded(self):
        import json
        bad = [observation(float("nan")), observation(float("inf")),
               observation(5, available_bytes=float("nan")),
               {"unexpected": object()}, None]
        for value in bad:
            with self.subTest(value=value):
                result = self.policy().observe("/a", value)
                self.assertIsNone(result["observation"])
                self.assertIn("DISK_OBSERVATION_UNKNOWN", result["reasons"])
                json.dumps(result, allow_nan=False)
        row = observation(extra=float("nan"), payload=object())
        result = self.policy().observe("/a", row)
        self.assertEqual(result["observation"], observation())
        json.dumps(result, allow_nan=False)

    def test_clock_and_filesystem_changes_fail_closed(self):
        for value in (observation(0), observation(-1), observation(5, device=2),
                      observation(5, total_bytes=2000), observation(5, total_inodes=2000)):
            with self.subTest(value=value):
                p = self.policy()
                p.observe("/a", observation())
                self.assertIn("DISK_OBSERVATION_UNKNOWN", p.observe("/a", value)["reasons"])

    def test_capacity_recovery_hysteresis_and_independent_paths(self):
        p = self.policy()
        p.observe("/a", observation())
        p.observe("/b", observation())
        p.observe("/a", observation(5, size=90, inodes=40))
        result = p.observe("/a", observation(10, size=150, inodes=75))
        self.assertIn("DISK_BYTES_RECOVERY_PENDING", result["reasons"])
        self.assertIn("DISK_INODES_RECOVERY_PENDING", result["reasons"])
        self.assertFalse(p.observe("/b", observation(5))["pressure"])
        self.assertFalse(p.observe("/a", observation(15, size=200, inodes=100))["pressure"])

    def test_time_hysteresis_requires_recovery_horizon(self):
        p = self.policy()
        p.observe("/a", observation(0, size=100))
        result = p.observe("/a", observation(5, size=700))
        self.assertFalse(result["pressure"])
        self.assertTrue(p.observe("/a", observation(10, size=400))["pressure"])
        result = p.observe("/a", observation(15, size=325))
        self.assertEqual(result["metrics"]["bytes"]["seconds_to_reserve"], 15)
        self.assertIn("DISK_BYTES_TIME_TO_RESERVE", result["reasons"])
        self.assertFalse(p.observe("/a", observation(20, size=325))["pressure"])

    def test_invalid_policies_rejected(self):
        for values in ((0, 200, 50, 100, 10, 20), (100, 100, 50, 100, 10, 20),
                       (100, 200, 50, 50, 10, 20), (100, 200, 50, 100, 20, 10),
                       (100, 200, 50, 100, float("nan"), 20),
                       (100, 200, 50, 100, 10, float("inf"))):
            with self.subTest(values=values), self.assertRaises(ValueError):
                DiskPressure(*values)
