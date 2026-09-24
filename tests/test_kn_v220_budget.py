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
        measured = cache["measured_bytes_per_bar_row"]
        # KN-1 review F5: sized on the contract-complete lossless row, not on
        # the 275 B compact row that omitted coordinates/quality/provenance.
        # R2: the LPK-derived row, lossless across mixed history, at the
        # allocator-aligned bucket; no per-product header.
        per_row = measured["lpk_state_listpack_bucket116"]
        self.assertGreater(per_row, measured["superseded_r1_identity_header_bucket64_not_lossless"])
        self.assertEqual(sizing["rows_at_cap"], 140 * 12_064 + (500 + 2_064))
        self.assertAlmostEqual(sizing["rows_at_cap"] * per_row, sizing["bar_rows_bytes_at_cap"], delta=1e6)
        other = sizing["other_structures_bytes"]
        self.assertEqual(other["bar_identity_headers"], 0)
        counted = sum(v for v in other.values() if isinstance(v, int))
        self.assertAlmostEqual(sizing["bar_rows_bytes_at_cap"] + counted, sizing["steady_bytes_at_cap"], delta=1e6)
        self.assertGreater(other["latest_state_keys"], 0)
        self.assertAlmostEqual(12_064 * per_row, sizing["largest_product_bytes"], delta=1e3)
        # Staging product + the old generation until physically reclaimed.
        self.assertAlmostEqual(sizing["steady_bytes_at_cap"] + 2 * sizing["largest_product_bytes"],
                               sizing["peak_bytes_during_rebuild"], delta=1e6)
        self.assertLess(sizing["peak_bytes_during_rebuild"], sizing["maxmemory_bytes"])
        self.assertAlmostEqual(sizing["maxmemory_bytes"] - sizing["peak_bytes_during_rebuild"],
                               sizing["headroom_below_maxmemory_bytes"], delta=1e6)
        # The sensitivity is real: an unaligned bucket or the full canonical row does not fit.
        self.assertGreater(sizing["rows_at_cap"] * measured["lpk_state_listpack_by_bucket"]["64"],
                           sizing["maxmemory_bytes"])
        self.assertGreater(sizing["rows_at_cap"] * measured["canonical_state_best_bucket64"], sizing["maxmemory_bytes"])
        self.assertGreater(2 * sizing["steady_bytes_at_cap"], sizing["maxmemory_bytes"])
        self.assertLess(sizing["maxmemory_bytes"], sizing["container_memory_limit_bytes"])
        self.assertEqual(sizing["maxmemory_bytes"], 1_288_490_188, "no RAM increase by default")
        self.assertEqual(cache["required_config"]["maxmemory-policy"], "noeviction")
        # The listpack threshold must hold the measured row, or buckets convert
        # to hashtable (measured at 512).
        self.assertGreaterEqual(cache["required_config"]["hash-max-listpack-value"], 694)

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
