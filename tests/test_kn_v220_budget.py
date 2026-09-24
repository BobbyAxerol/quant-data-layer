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

    def test_projector_cpu_is_the_kn3_measurement(self):
        # Astra KN-3 review R1: the 0.3 hypothesis is not met (0.376 measured
        # for both replicas at the 3,000/s challenge); each replica is
        # allocated 0.5 (the cold build is CPU-bound at that cap).
        allocation = self.budget["cpu"]["target_allocation_vcpu_unverified"]
        self.assertNotIn("projector", allocation, "multi-replica roles are named *_total")
        measured = allocation["projector_measured"]
        self.assertEqual(measured["allocation_vcpu_per_replica"], 0.5)
        self.assertEqual(measured["replicas"], 2)
        challenge = measured["challenge_3000_per_s"]
        self.assertAlmostEqual(challenge["replica_a_vcpu_mean"] + challenge["replica_b_vcpu_mean"],
                               challenge["two_replicas_vcpu_mean"], places=6)
        self.assertGreater(challenge["two_replicas_vcpu_mean"], 0.3, "the KN-1 0.3 is not met")
        self.assertGreaterEqual(allocation["projector_total"], challenge["two_replicas_vcpu_mean"])
        self.assertLessEqual(allocation["projector_total"],
                             measured["replicas"] * measured["allocation_vcpu_per_replica"])
        self.assertTrue(all(p95 <= measured["allocation_vcpu_per_replica"]
                            for p95 in measured["whole_run"]["replica_vcpu_p95"]))
        for name in ("resources-cold-and-live.txt", "resources-whole-run.txt"):
            self.assertIn(name, measured["evidence"])

    def test_market_cache_sizing_is_consistent(self):
        cache = self.budget["market_cache"]
        sizing = cache["sizing"]
        measured = cache["measured_bytes_per_bar_row"]
        # KN-1 review F5: sized on the contract-complete lossless row, not on
        # the 275 B compact row that omitted coordinates/quality/provenance.
        # R2: the LPK-derived row, lossless across mixed history, at the
        # allocator-aligned bucket; no per-product header.
        # Astra KN-3 review R1: the byte figures are NOT re-derived yet; they
        # stay on the labelled DOGE sample until the full-cap measurement.
        doge = measured["doge_1m_kn1"]
        self.assertIn("doge_1m_kn1.lpk_state_listpack_bucket116", sizing["bytes_per_row_basis"])
        per_row = doge["lpk_state_listpack_bucket116"]
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
        self.assertGreater(sizing["rows_at_cap"] * doge["lpk_state_listpack_by_bucket"]["64"],
                           sizing["maxmemory_bytes"])
        self.assertGreater(sizing["rows_at_cap"] * measured["canonical_state_best_bucket64"], sizing["maxmemory_bytes"])
        self.assertGreater(2 * sizing["steady_bytes_at_cap"], sizing["maxmemory_bytes"])
        self.assertLess(sizing["maxmemory_bytes"], sizing["container_memory_limit_bytes"])
        self.assertEqual(sizing["maxmemory_bytes"], 1_288_490_188, "no RAM increase by default")
        self.assertEqual(cache["required_config"]["maxmemory-policy"], "noeviction")
        # The listpack threshold must hold the measured row, or buckets convert
        # to hashtable (measured at 512).
        self.assertGreaterEqual(cache["required_config"]["hash-max-listpack-value"], 694)

    def test_measured_full_cap_sizing_fits_the_adopted_limits(self):
        # Astra KN-3 review R1: bucket 112, maxmemory 1.5e9 in a 1.75 GiB
        # container, confirmed by the full-cap measurement (steady + rolling
        # rebuild peak), not by the DOGE hypothesis.
        cache = self.budget["market_cache"]
        measured = cache["sizing_measured_kn3_r1"]
        self.assertAlmostEqual(measured["used_memory_measured_bytes"] / measured["rows_measured"],
                               measured["bytes_per_row_all_structures"], delta=0.5)
        self.assertEqual(measured["rows_at_cap"], 140 * 12_064 + 4_064 + 3 * 2_064)
        self.assertAlmostEqual(measured["rows_at_cap"] * measured["bytes_per_row_all_structures"],
                               measured["steady_bytes_at_cap"], delta=1e3)
        self.assertEqual(measured["steady_bytes_at_cap"] + measured["warm_rebuild_peak_delta_measured_bytes"],
                         measured["peak_bytes_during_rebuild"])
        self.assertEqual(measured["maxmemory_bytes"] - measured["peak_bytes_during_rebuild"],
                         measured["headroom_below_maxmemory_bytes"])
        self.assertGreater(measured["headroom_below_maxmemory_bytes"], 0)
        self.assertEqual(measured["maxmemory_bytes"], 1_500_000_000)
        self.assertEqual(measured["container_memory_limit_bytes"], 1_879_048_192, "1.75 GiB")
        self.assertLess(measured["used_memory_rss_measured_bytes"] * measured["rows_at_cap"] / measured["rows_measured"],
                        measured["container_memory_limit_bytes"])
        # The KN-1 cap would not hold a rebuild at full cap (steady alone
        # fits it by less than 4 MB).
        self.assertGreater(measured["peak_bytes_during_rebuild"], cache["sizing"]["maxmemory_bytes"])
        self.assertLess(cache["sizing"]["maxmemory_bytes"] - measured["steady_bytes_at_cap"], 4_000_000)
        self.assertIn("superseded", cache["sizing"]["status"])

    def test_real_mix_bar_layout_is_bucket_112(self):
        cache = self.budget["market_cache"]
        real = cache["measured_bytes_per_bar_row"]["real_mix_kn3"]
        self.assertIn("buckets of 112 opens", cache["bar_layout"])
        sweep = real["lpk_state_listpack_by_bucket"]
        self.assertEqual(sweep["112"], real["lpk_state_listpack_bucket112"])
        self.assertEqual(min(sweep, key=sweep.get), "112", "112 is the lowest measured real-mix point")
        self.assertIn("280,762 real rows of 45 products", real["sample"])
        for name in ("bucket-size-sweep.txt", "cache-sizing-cold.txt", "cache-encoding.txt"):
            self.assertIn(name, real["evidence"])
        whole = real["whole_cache_bucket116"]
        self.assertEqual(whole["binance_rows"] + whole["okx_rows"], whole["bar_rows"])
        self.assertAlmostEqual(whole["dataset_bytes_per_row"], 824, delta=0.5)
        # The real mix costs more than the DOGE sample the sizing bytes still use.
        doge = cache["measured_bytes_per_bar_row"]["doge_1m_kn1"]
        self.assertGreater(real["lpk_state_listpack_bucket112"], doge["lpk_state_listpack_bucket116"])
        self.assertGreater(whole["dataset_bytes_per_row"], sweep["116"])

    def test_state_topics_never_change_the_canonical_topology(self):
        self.assertFalse(self.budget["kafka"]["partition_change_allowed"])
        topics = self.budget["retention"]["state_topics"]
        self.assertEqual({k for k, v in topics.items() if isinstance(v, dict)}, {"md.latest.v2", "md.bars.v2"})
        self.assertNotIn("md.canonical.v2", topics)
        for name in ("md.latest.v2", "md.bars.v2"):
            self.assertEqual(topics[name]["cleanup.policy"], "compact")
            self.assertEqual(topics[name]["replication_factor"], 3)
            self.assertGreaterEqual(topics[name]["delete.retention.ms"], self.budget["retention"]["tombstone_lifetime_ms"])
            # Owner decision, Astra KN-3 review R1: bounded segments on both state topics.
            self.assertEqual(topics[name]["segment.ms"], 3_600_000)
            self.assertEqual(topics[name]["segment.bytes"], 134_217_728)
        self.assertEqual(topics["md.bars.v2"]["segment.ms"], topics["md.bars.v2"]["min.compaction.lag.ms"])


if __name__ == "__main__":
    unittest.main()
