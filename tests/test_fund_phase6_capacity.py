from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.phase6_capacity_certification import certify


class CapacityCertificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_normal_and_burst_windows_preserve_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            result = await certify(
                events_per_window=200,
                partitions=10,
                normal_rate=200,
                burst_rate=500,
                output=Path(directory) / "capacity.json",
            )
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["canonical_queue_rejected"], 0)
        self.assertFalse(result["replay_mismatch"])
        self.assertEqual(len(result["windows"]), 3)
        self.assertTrue(all("durable_latency_p999_ms" in item for item in result["windows"]))


if __name__ == "__main__":
    unittest.main()
