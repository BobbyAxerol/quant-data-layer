"""Isolated ownership/SQLite/process recovery tests; never use runtime state."""

from __future__ import annotations

import json
import multiprocessing
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from qdl.runtime.projector_watchdog import check_projector
from qdl.transport.kafka_projector import ConfluentProjectorBroker, KafkaProjectorConfig
from qdl.transport.sqlite_spool import SQLiteDurableSpool, SpoolConfig


class LegacyKlineContractTests(unittest.TestCase):
    def test_crypto_bars_never_depend_on_legacy_v1_kline(self):
        from qdl.consumer import StableReleaseRoutePlan, requirement_key
        root = Path(__file__).parents[1]
        plan = StableReleaseRoutePlan.load(root / "config/v2/stable-v2-release-routing.yaml", manifest_root=root)
        bars = 0
        for consumer in plan.consumers:
            routes = {route.requirement_key: route for route in consumer.products}
            for requirement in consumer.manifest.requirements:
                if requirement.feed.value != "BAR" or requirement.source_policy_id != "crypto_primary_v2":
                    continue
                route = routes[requirement_key(requirement)]
                self.assertEqual(route.route, "V2_PRIMARY")
                self.assertEqual(route.fallback, "BLOCKED")
                bars += 1
        self.assertGreater(bars, 50)


class BrokerOwnershipTests(unittest.TestCase):
    def test_close_waits_for_poll_and_health_does_not_overlap_or_exceed_its_budget(self):
        polling, release, closed = threading.Event(), threading.Event(), threading.Event()

        class Consumer:
            def __init__(self, _config):
                pass

            def subscribe(self, *args, **kwargs):
                pass

            def assignment(self):
                return []

            def consume(self, *args):
                polling.set()
                if not release.wait(3):
                    raise TimeoutError("test poll was not released")
                return []

            def list_topics(self, **kwargs):
                raise AssertionError("metadata must not overlap poll or run after close")

            def close(self):
                closed.set()

        with tempfile.TemporaryDirectory() as directory:
            cert = Path(directory) / "test-only.pem"
            cert.write_text("test-only")
            broker = ConfluentProjectorBroker(KafkaProjectorConfig(
                "unreachable:9092", "test", "test", (), "canonical", cert, cert, cert,
            ), consumer_factory=Consumer)
            with ThreadPoolExecutor(max_workers=2) as pool:
                poll = pool.submit(broker.poll_batch, 2, 1)
                try:
                    self.assertTrue(polling.wait(1))
                    start = time.monotonic()
                    self.assertFalse(broker.ping(0.05))
                    self.assertLess(time.monotonic() - start, 0.5)
                    close = pool.submit(broker.close)
                    self.assertFalse(closed.wait(0.1))
                finally:
                    release.set()
                self.assertEqual(poll.result(timeout=2), [])
                close.result(timeout=2)
            self.assertTrue(closed.is_set())
            self.assertFalse(broker.ping())
            with self.assertRaisesRegex(RuntimeError, "closed"):
                broker.checkpoint_many(())
            with self.assertRaisesRegex(RuntimeError, "closed"):
                broker.poll_batch(1, 1)
            broker.close()


def _hydrate(path, entered, writer_done, result):
    try:
        with SQLiteDurableSpool(SpoolConfig(Path(path), min_free_disk_bytes=0)) as spool:
            def read_legacy():
                entered.set()
                if not writer_done.wait(5):
                    raise TimeoutError("legacy read held the writer lock")
                return 100
            result.put(spool.hydrate_final_bar_watermark(
                stream="canonical", partition_key="bar", legacy_lookup=read_legacy,
            ))
    except BaseException as error:
        result.put(type(error).__name__ + ":" + str(error))


class SpoolOwnershipTests(unittest.TestCase):
    def test_other_process_can_commit_while_legacy_history_is_read_and_newer_fence_wins(self):
        ctx = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cache.sqlite3"
            with SQLiteDurableSpool(SpoolConfig(path, min_free_disk_bytes=0)) as writer:
                entered, done, result = ctx.Event(), ctx.Event(), ctx.Queue()
                child = ctx.Process(target=_hydrate, args=(str(path), entered, done, result))
                child.start()
                try:
                    self.assertTrue(entered.wait(5))
                    writer._connection.execute("PRAGMA busy_timeout=200")
                    writer.seed_final_bar_watermark(
                        stream="canonical", partition_key="bar", close_time_ns=200,
                    )
                    done.set()
                    self.assertEqual(result.get(timeout=5), 200)
                    child.join(5)
                    self.assertEqual(child.exitcode, 0)
                    self.assertTrue(writer.integrity_check())
                finally:
                    done.set()
                    if child.is_alive():
                        child.terminate()
                    child.join(5)
                    result.close()
                    result.join_thread()

    def test_close_does_not_request_exclusive_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            spool = SQLiteDurableSpool(SpoolConfig(Path(directory) / "s.db", min_free_disk_bytes=0))
            statements = []
            spool._connection.set_trace_callback(statements.append)
            spool.close()
            self.assertIn("PRAGMA wal_checkpoint(PASSIVE)", statements)
            self.assertFalse(any("TRUNCATE" in statement for statement in statements))


class ProjectorWatchdogTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.proc = Path(self.directory.name) / "proc"
        (self.proc / "1/task/1").mkdir(parents=True)
        (self.proc / "7").mkdir()
        (self.proc / "1/task/1/children").write_text("7 ")
        self.heartbeat = Path(self.directory.name) / "heartbeat.json"
        (self.proc / "1/cmdline").write_bytes(b"/sbin/docker-init\0--\0python\0")
        (self.proc / "7/cmdline").write_bytes(b"python\0-m\0app.entrypoints.projector_v2_stable\0")
        fields = ["0"] * 20
        fields[1] = "1"
        fields[19] = str(10 * os.sysconf("SC_CLK_TCK"))
        (self.proc / "7/stat").write_text("7 (python) " + " ".join(fields))
        self.killed = []

    def check(self, seconds, payload=None):
        if payload is not None:
            self.heartbeat.write_text(json.dumps(payload))
        return check_projector(
            self.heartbeat, proc=self.proc, now_ns=int(seconds * 1e9),
            terminate=lambda *args: self.killed.append(args),
        )

    def test_startup_or_recent_loop_never_restarts(self):
        self.assertFalse(self.check(20))
        self.assertTrue(self.check(1000, {"role": "stable_projector", "pid": 7, "monotonic_ns": 990_000_000_000}))
        self.assertEqual(self.killed, [])

    def test_stale_is_unhealthy_before_restart_deadline(self):
        self.assertFalse(self.check(80))
        self.assertEqual(self.killed, [])
        self.assertFalse(self.check(131))
        self.assertEqual(self.killed, [(7, signal.SIGKILL)])

    def test_old_generation_future_or_invalid_heartbeat_cannot_hide_stall(self):
        for payload in ({"pid": 99}, [], {"role": "stable_projector", "pid": 7, "monotonic_ns": 999_000_000_000}):
            with self.subTest(payload=payload):
                self.assertFalse(self.check(200, payload))
        self.assertEqual(len(self.killed), 3)

    def test_cannot_signal_host_or_another_role(self):
        (self.proc / "7/cmdline").write_bytes(b"python\0-m\0app.entrypoints.query_v2_stable\0")
        self.assertFalse(self.check(999))
        self.assertEqual(self.killed, [])

    def test_external_probe_can_end_native_call_that_holds_worker_gil(self):
        child = subprocess.Popen([
            sys.executable, "-c",
            "import ctypes; print('ready', flush=True); ctypes.PyDLL(None).sleep(30)",
        ], stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), "ready")
            self.assertFalse(check_projector(
                self.heartbeat, proc=self.proc, now_ns=200_000_000_000,
                terminate=lambda _pid, sig: os.kill(child.pid, sig),
            ))
            self.assertEqual(child.wait(timeout=3), -signal.SIGKILL)
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=3)
            child.stdout.close()

    def test_compose_enables_watchdog_only_on_six_projectors(self):
        import yaml
        services = yaml.safe_load((Path(__file__).parents[1] / "docker-compose.v2-stable.yml").read_text())["services"]
        enabled = []
        for name, service in services.items():
            if service.get("healthcheck", {}).get("test") == ["CMD", "python", "-m", "qdl.runtime.projector_watchdog"]:
                enabled.append(name)
                self.assertEqual(service["restart"], "unless-stopped")
                self.assertTrue(service["init"])
                self.assertIn("QDL_STABLE_HEARTBEAT_PATH", service["environment"])
        self.assertEqual(set(enabled), {"projector_v2", *(f"projector_v2_{i}" for i in range(2, 7))})


if __name__ == "__main__":
    unittest.main()
