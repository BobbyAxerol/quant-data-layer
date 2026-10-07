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


class ExplicitAuthorityTests(unittest.TestCase):
    def test_stop_authority_never_comes_from_name(self):
        from scripts.kn_production_watch import explicit_stop_ids
        row = {"containers": {PROJECT + "-market_projector_1-1": {"id": "a" * 64}}}
        self.assertEqual(explicit_stop_ids(row, []), [])
        self.assertEqual(explicit_stop_ids(row, ["a" * 64]), ["a" * 64])
        for ids in (["market_projector_1"], ["a" * 12], ["b" * 64], ["a" * 64] * 2):
            with self.assertRaises(ValueError):
                explicit_stop_ids(row, ids)

    def test_guard_is_read_only_unless_exact_ids_supplied(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch, Mock
        from scripts.kn_production_watch import main
        row = {"available_memory": 1024**3, "containers": {
            PROJECT + "-market_projector_1-1": {"id": "a" * 64}}}
        for approved in (False, True):
            with self.subTest(approved=approved), tempfile.TemporaryDirectory() as folder:
                argv = ["watch", "--out", str(Path(folder) / "result.jsonl"),
                        "--seconds", "1", "--stop-file", str(Path(folder) / "stop")]
                if approved:
                    argv += ["--stop-container-id", "a" * 64]
                with patch("sys.argv", argv), patch("scripts.kn_production_watch.sample", return_value=row), \
                        patch("scripts.kn_production_watch.cpu_ticks", return_value=(100, 50)), \
                        patch("scripts.kn_production_watch.subprocess.run", return_value=Mock(stdout="", stderr="")) as run:
                    self.assertEqual(main(), 3)
                    stops = [c.args[0] for c in run.call_args_list if c.args[0][:2] == ["docker", "stop"]]
                    self.assertEqual(stops, [["docker", "stop", "-t", "15", "a" * 64]] if approved else [])
