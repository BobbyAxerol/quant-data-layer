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


class DiskWatchIntegrationTests(unittest.TestCase):
    def test_disk_alert_unknown_and_exhaustion_never_authorize_stop(self):
        import json
        import tempfile
        from pathlib import Path
        from unittest.mock import Mock, patch
        from scripts import kn_production_watch as watch
        from tests.test_kn_disk_pressure import observation

        for current in (None, observation(5, size=0), observation(5, inodes=0),
                        observation(5, size=500)):
            with self.subTest(current=current), tempfile.TemporaryDirectory() as folder:
                out = Path(folder) / "result.jsonl"
                argv = ["watch", "--out", str(out), "--seconds", "1",
                        "--stop-file", str(Path(folder) / "stop"),
                        "--ts-container", "explicit-writer", "--disk-path", "/test",
                        "--disk-reserve-bytes", "100", "--disk-recovery-bytes", "200",
                        "--disk-reserve-inodes", "50", "--disk-recovery-inodes", "100",
                        "--disk-reserve-seconds", "10", "--disk-recovery-seconds", "20"]
                row = {"available_memory": 8 * 1024**3, "containers": {
                    PROJECT + "-market_projector_1-1": {"id": "a" * 64}}}
                with patch("sys.argv", argv), patch.object(watch, "sample", return_value=row) as sample, \
                        patch.object(watch, "sample_disk", side_effect=[observation(), current]), \
                        patch.object(watch, "cpu_ticks", return_value=(100, 50)), \
                        patch.object(watch.subprocess, "run", return_value=Mock(stdout="", stderr="")) as run:
                    self.assertEqual(watch.main(), 3)
                    sample.assert_called_once_with("explicit-writer")
                    self.assertEqual([c.args[0] for c in run.call_args_list],
                                     [["docker", "logs", "--since", "60s", "--tail", "1000", "explicit-writer"]])
                result = json.loads(out.read_text())
                self.assertTrue(result["guard_stop"])
                self.assertTrue(result["disk_pressure"]["/test"]["pressure"])
                self.assertTrue(all(r.startswith("DISK_") for r in result["stop_reasons"]))

    def test_cli_incomplete_invalid_policy_rejected_before_sampling(self):
        import contextlib
        import io
        from unittest.mock import patch
        from scripts import kn_production_watch as watch
        complete = ["--disk-path", "/test", "--disk-reserve-bytes", "100",
                    "--disk-recovery-bytes", "200", "--disk-reserve-inodes", "50",
                    "--disk-recovery-inodes", "100", "--disk-reserve-seconds", "10",
                    "--disk-recovery-seconds", "20"]
        cases = [["--disk-path", "/test"], ["--disk-reserve-bytes", "100"],
                 complete + ["--disk-path", "/test"],
                 complete + ["--disk-reserve-seconds", "nan"],
                 complete + ["--disk-recovery-bytes", "100"]]
        for flags in cases:
            with self.subTest(flags=flags), patch("sys.argv", [
                    "watch", "--out", "/unused", "--stop-file", "/unused-stop"] + flags), \
                    patch.object(watch, "sample") as sample, \
                    patch.object(watch, "sample_disk") as disk, \
                    contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    watch.main()
                self.assertEqual(error.exception.code, 2)
                sample.assert_not_called()
                disk.assert_not_called()

    def test_sampler_inspects_selected_ts_container(self):
        from unittest.mock import Mock, patch
        from scripts import kn_production_watch as watch
        for name in ("market_data_service", "explicit-writer"):
            with self.subTest(name=name), \
                    patch.object(watch.subprocess, "check_output", return_value=""), \
                    patch.object(watch.subprocess, "run", return_value=Mock(returncode=1)) as run, \
                    patch.object(watch.Path, "read_text", return_value="MemAvailable: 4096 kB"):
                result = watch.sample(name)
                self.assertEqual(result["available_memory"], 4096 * 1024)
                run.assert_called_once_with(["docker", "inspect", name], capture_output=True, text=True)

    def test_default_healthy_watcher_does_not_sample_disks(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import Mock, patch
        from scripts import kn_production_watch as watch
        with tempfile.TemporaryDirectory() as folder:
            out = Path(folder) / "result.jsonl"
            argv = ["watch", "--out", str(out), "--seconds", "1",
                    "--stop-file", str(Path(folder) / "stop")]
            with patch("sys.argv", argv), \
                    patch.object(watch, "sample", return_value={
                        "available_memory": 8 * 1024**3, "containers": {}}) as sample, \
                    patch.object(watch, "sample_disk") as disk, \
                    patch.object(watch, "cpu_ticks", return_value=(100, 50)), \
                    patch.object(watch.time, "monotonic", side_effect=[0, 0, 2]), \
                    patch.object(watch.time, "sleep"), \
                    patch.object(watch.subprocess, "run", return_value=Mock(stdout="", stderr="")) as run:
                self.assertEqual(watch.main(), 0)
                sample.assert_called_once_with("market_data_service")
                disk.assert_not_called()
                self.assertEqual(len(run.call_args_list), 1)
                self.assertNotIn("disk_pressure", out.read_text())
