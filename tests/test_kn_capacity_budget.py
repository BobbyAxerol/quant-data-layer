"""Whole-cache arithmetic is a planning check, not a live capacity certificate."""
import json
from pathlib import Path
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]

class WholeCacheBudgetTests(unittest.TestCase):
    def test_whole_cache_includes_execution_and_rebuild_without_lowering_contract(self):
        budget = json.loads((ROOT / "config/v2/kn-v220-candidate-budget.json").read_text())
        whole = budget["whole_cache_d48"]
        base = budget["market_cache"]["sizing_measured_kn3_r1"]
        daily = {}
        for name in ("alpha-binance-paper", "alpha-okx-paper"):
            doc = yaml.safe_load((ROOT / f"consumers/stable/{name}.yaml").read_text())
            rows = doc["spec"]["requirements"]
            hot = {r["instrument_uid"] for r in rows if r["feed"] == "TRADE"}
            for row in rows:
                if row["feed"] == "BAR" and row["interval"] == "1d" and row["instrument_uid"] not in hot:
                    key = (row["instrument_uid"], row["interval"], row["source_policy_id"])
                    daily[key] = max(daily.get(key, 0), row["warmup_limit"])
        self.assertEqual(len(daily), whole["additional_d48_products"])
        self.assertEqual(set(daily.values()), {10000})
        additional = sum(v + whole["retained_headroom_per_product"] for v in daily.values())
        self.assertEqual(additional, whole["additional_rows_at_cap"])
        total = base["rows_at_cap"] + additional
        self.assertEqual(total, whole["total_rows_at_cap"])
        steady = int(total * base["bytes_per_row_all_structures"])
        self.assertEqual(steady, whole["steady_bytes_at_cap_extrapolated"])
        self.assertEqual(steady + base["warm_rebuild_peak_delta_measured_bytes"], whole["peak_bytes_at_cap_extrapolated"])
        self.assertGreater(whole["peak_bytes_at_cap_extrapolated"], base["maxmemory_bytes"])
        self.assertFalse(whole["existing_cap_fits_full_retention"])
        self.assertEqual(whole["status"], "FROZEN_ALLOCATOR_ENVELOPE_RUNTIME_ACCEPTANCE_PENDING")

    def test_full_allocator_cap_keeps_staging_and_runtime_limits_explicit(self):
        budget = json.loads((ROOT / "config/v2/kn-v220-candidate-budget.json").read_text())
        measured = budget["kn5_full_capacity_allocator"]
        self.assertGreaterEqual(measured["full_cap_rows"], budget["whole_cache_d48"]["total_rows_at_cap"])
        self.assertEqual(measured["evicted_keys"], 0)
        self.assertFalse(measured["market_data_certificate"])
        self.assertFalse(measured["production_mutation"])
        self.assertLess(measured["with_staging_used_bytes"], measured["candidate_maxmemory_bytes"])
        self.assertGreater(measured["candidate_container_memory_bytes"], measured["candidate_maxmemory_bytes"])
        self.assertGreater(measured["headroom_below_maxmemory_bytes"], 900_000_000)

if __name__ == "__main__":
    unittest.main()
