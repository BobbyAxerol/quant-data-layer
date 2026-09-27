"""Same stop rules as KN-4, limited to the new KN role allowlist."""
import unittest
from scripts.kn_production_watch import stop_reasons, PROJECT


class ProductionWatchTests(unittest.TestCase):
    def test_healthy(self):
        assert stop_reasons({"available_memory": 8*1024**3, "containers": {}}) == []

    def test_historical_legacy_oom_is_not_native_oom(self):
        assert stop_reasons({"available_memory": 8*1024**3, "containers": {
            PROJECT+"-projector_v2-1": {"oom": True}}}) == []

    def test_native_oom(self):
        result = stop_reasons({"available_memory": 8*1024**3, "containers": {
            PROJECT+"-market_projector_1-1": {"memory_events": {"oom_kill": 1}}}})
        assert result == ["NATIVE_OOM:market_projector_1"]

    def test_resource_and_ts_fences(self):
        row = {"available_memory": 1024**3, "containers": {}, "ts_mark_index_unavailable_60s": 10}
        assert len(stop_reasons(row, 60)) == 3
        row["available_memory"] = 4*1024**3
        row["ts_mark_index_unavailable_60s"] = 9
        assert stop_reasons(row, 59) == []
