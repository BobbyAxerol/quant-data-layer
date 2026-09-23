"""KN-1 K1.4: the frozen candidate budget stays tied to the v2.1.1 budget and
to its own measured arithmetic.

The budget inherits every latency/stage/workload gate from
``config/v2/v211-target-acceptance-budget.json`` by hash, so a later edit of
either file is caught here instead of silently moving a threshold.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
BUDGET = ROOT / "config/v2/kn-v220-candidate-budget.json"


class CandidateBudgetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.budget = json.loads(BUDGET.read_text(encoding="utf-8"))

    def test_inherited_budget_hash_matches_the_frozen_file(self):
        inherited = self.budget["inherits"]
        actual = hashlib.sha256((ROOT / inherited["path"]).read_bytes()).hexdigest()
        self.assertEqual(actual, inherited["sha256"])
        v211 = json.loads((ROOT / inherited["path"]).read_text(encoding="utf-8"))
        for key in inherited["unchanged"]:
            self.assertIn(key, v211)

    def test_cpu_budget_and_denominator(self):
        cpu = self.budget["cpu"]
        self.assertEqual(cpu["steady_state_vcpu_max"], 5.0)
        self.assertIn("kafka1", cpu["denominator"])
        allocation = cpu["target_allocation_vcpu_unverified"]
        parts = sum(v for k, v in allocation.items() if isinstance(v, (int, float)) and k != "sum")
        self.assertAlmostEqual(parts, allocation["sum"], places=6)
        self.assertLessEqual(allocation["sum"], cpu["steady_state_vcpu_max"])

    def test_market_cache_sizing_is_consistent(self):
        cache = self.budget["market_cache"]
        sizing = cache["sizing"]
        per_row = cache["measured_bytes_per_bar_row"]["compact_listpack_bucket120"]
        self.assertEqual(sizing["rows_at_cap"], 140 * 12_064 + (500 + 2_064))
        self.assertAlmostEqual(sizing["rows_at_cap"] * per_row, sizing["steady_bytes_at_cap"], delta=1e6)
        self.assertAlmostEqual(2 * sizing["steady_bytes_at_cap"], sizing["peak_bytes_during_rebuild"], delta=1e6)
        self.assertLess(sizing["peak_bytes_during_rebuild"], sizing["maxmemory_bytes"])
        self.assertLess(sizing["maxmemory_bytes"], sizing["container_memory_limit_bytes"])
        self.assertEqual(cache["required_config"]["maxmemory-policy"], "noeviction")

    def test_state_topics_never_change_the_canonical_topology(self):
        self.assertFalse(self.budget["kafka"]["partition_change_allowed"])
        topics = self.budget["retention"]["state_topics"]
        self.assertEqual(set(k for k in topics if k != "approval"), {"md.latest.v2", "md.bars.v2"})
        for name in ("md.latest.v2", "md.bars.v2"):
            self.assertEqual(topics[name]["cleanup.policy"], "compact")
            self.assertEqual(topics[name]["replication_factor"], 3)
            self.assertGreaterEqual(topics[name]["delete.retention.ms"], self.budget["retention"]["tombstone_lifetime_ms"])


if __name__ == "__main__":
    unittest.main()
