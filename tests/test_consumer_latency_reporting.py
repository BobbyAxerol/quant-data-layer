from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import measure_consumer_request_latency as probe


class ConsumerLatencyReportingTests(unittest.TestCase):
    def test_latest_bar_and_full_warmup_are_distinct_operations(self):
        bar = SimpleNamespace(feed=probe.Feed.BAR, warmup_limit=1000)
        self.assertEqual(probe.read_operation(bar, bar_snapshot=False), "warmup")
        self.assertEqual(probe.read_operation(bar, bar_snapshot=True), "snapshot")

    def test_revision_comes_from_exact_manifest(self):
        with patch.dict(probe.os.environ, {}, clear=True):
            self.assertEqual(probe.manifest_revision(), 10)
        with patch.dict(probe.os.environ, {"QDL_MANIFEST_REVISION": "9"}, clear=True):
            with self.assertRaisesRegex(ValueError, "differs"):
                probe.manifest_revision()

    def test_manifest_subject_cannot_be_substituted(self):
        with patch.object(probe, "CONSUMER", "unregistered"):
            with self.assertRaisesRegex(ValueError, "identity"):
                probe.manifest_revision()

    def test_product_labels_are_unique_and_preserve_interval(self):
        rows = probe.requirements()
        self.assertEqual(len(rows), len({label for label, _ in rows}))
        for label, requirement in rows:
            self.assertIn(requirement.instrument_uid, label)
            self.assertEqual(label.count("/1m"), 1 if requirement.interval == "1m" else 0)

    def test_validation_is_inside_timed_usable_boundary(self):
        calls = []

        async def read():
            calls.append("read")
            return 42

        def validate(value):
            self.assertEqual(value, 42)
            calls.append("validate")

        def clock():
            calls.append("clock")
            return len(calls)

        with patch.object(probe.time, "perf_counter", clock):
            samples, error = asyncio.run(probe.time_calls(read, 1, validate=validate))
        self.assertEqual(calls, ["clock", "read", "validate", "clock"])
        self.assertEqual(samples, [3000])
        self.assertIsNone(error)

    def test_invalid_data_is_not_successful_latency(self):
        async def read():
            return None

        def validate(_):
            raise ValueError("unusable")

        samples, error = asyncio.run(probe.time_calls(read, 3, validate=validate))
        self.assertEqual(samples, [])
        self.assertIn("unusable", error)

    def test_small_sample_does_not_claim_p99(self):
        self.assertIsNone(probe.summarise("p", "snapshot", [1, 2], None)["p99_ms"])
        self.assertIsNotNone(probe.summarise("p", "snapshot", list(range(100)), None)["p99_ms"])

    def test_warmup_validates_history_separately_from_latest(self):
        with patch("qdl.certification.phase103_consumer_acceptance.validate_product_view") as validate:
            probe.validate_read("p", SimpleNamespace(data=["old", "current"]), warmup=True)
            self.assertEqual(validate.call_args_list[0].kwargs, {"require_current_quality": False})
            self.assertEqual(validate.call_args_list[1].args, ("p", "current"))
        with self.assertRaisesRegex(ValueError, "no usable"):
            probe.validate_read("p", SimpleNamespace(data=[]), warmup=True)

    def test_scope_is_loaded_from_release_not_fabricated_requirements(self):
        root = Path(probe.ROOT)
        products = probe.governed_products(
            root / "config/v2/stable-v2-release-routing.yaml",
            root / "config/v2/stable-source-bindings.yaml",
            root / "config/v2/stable-acquisition-bindings.yaml",
        )
        self.assertEqual(len(products), 60)


if __name__ == "__main__":
    unittest.main()
