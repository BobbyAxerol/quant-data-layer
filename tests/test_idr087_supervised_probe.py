"""IDR-087: the history backpressure Kafka probe runs in a supervised child.

librdkafka 2.15.0 segfaulted in ``AdminClient.list_consumer_group_offsets``
during a broker stop (G12 isolated harness). These cases use fake probes in a
real spawned child (no broker): a SIGSEGV or a hang in the child becomes a
typed ``ProbeUnavailable``, the parent survives, the gate stays closed, and a
healthy child returns the same numbers as the in-process reader.
"""
from __future__ import annotations

import os
import signal
import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from qdl.runtime.history_backpressure import KafkaStageBacklog, Stage, kafka_backlog_from_config
from qdl.runtime.isolated_probe import ProbeUnavailable, SupervisedProbe, exit_reason

_COMMITTED = {"core": {0: 100, 1: 7}, "a": {0: 0}}
_ENDS = {"raw": {0: 150, 1: 7, 2: 3}, "canonical": {0: 20}}
_STAGES = (Stage("core", "core", "raw", 100), Stage("a", "a", "canonical", 10))


# Module-level factories: the spawned child imports them by name.
def fake_kafka_reader(stages):
    """The production composition (per-stage readers -> backlog) with fake reads."""
    return KafkaStageBacklog(stages, read_committed=lambda group, _topic: _COMMITTED[group],
                             read_ends=lambda topic: _ENDS[topic]).backlog


def segv_probe(marker=None):
    def probe():
        if marker is None or not Path(marker).exists():
            if marker is not None:
                Path(marker).write_text("crashed once")
            os.kill(os.getpid(), signal.SIGSEGV)
        return {"core": 1, "a": 2}
    return probe


def hang_probe():
    def probe():
        time.sleep(60)
    return probe


def raising_probe():
    def probe():
        raise RuntimeError("KafkaException: broker transport failure")
    return probe


def pid_probe():
    return os.getpid


class SupervisedProbeTests(unittest.TestCase):
    def _probe(self, factory, args=(), timeout_s=20.0):
        probe = SupervisedProbe(factory, args, timeout_s=timeout_s)
        self.addCleanup(probe.close)
        return probe

    def test_a_native_crash_in_the_child_is_typed_and_the_parent_survives(self):
        probe = self._probe(segv_probe)
        with self.assertRaises(ProbeUnavailable) as caught:
            probe()
        self.assertEqual((caught.exception.reason, caught.exception.exitcode), ("SIGSEGV", -signal.SIGSEGV))
        self.assertIsNone(probe.proc)

    def test_the_next_call_after_a_crash_starts_a_fresh_child(self):
        with TemporaryDirectory() as directory:
            probe = self._probe(segv_probe, (str(Path(directory) / "crashed"),))
            with self.assertRaises(ProbeUnavailable):
                probe()
            self.assertEqual(probe(), {"core": 1, "a": 2})
            self.assertEqual(probe.restarts, 1)

    def test_a_hang_is_killed_at_the_hard_timeout(self):
        probe = self._probe(hang_probe, timeout_s=1.0)
        started = time.monotonic()
        with self.assertRaises(ProbeUnavailable) as caught:
            probe()
        self.assertEqual(caught.exception.reason, "TIMEOUT")
        self.assertLess(time.monotonic() - started, 8.0)
        self.assertIsNone(probe.proc)

    def test_a_client_error_in_the_child_is_typed_and_keeps_the_child(self):
        probe = self._probe(raising_probe)
        with self.assertRaises(ProbeUnavailable) as caught:
            probe()
        self.assertEqual(caught.exception.reason, "CHILD_ERROR")
        self.assertIn("broker transport failure", caught.exception.detail)
        self.assertTrue(probe.proc.is_alive())

    def test_the_child_is_reused_and_is_not_the_parent(self):
        probe = self._probe(pid_probe)
        child = probe()
        self.assertNotEqual(child, os.getpid())
        self.assertEqual(probe(), child)
        self.assertEqual(probe.restarts, 0)

    def test_spawn_is_the_start_method(self):
        # fork would copy the parent's librdkafka threads/locks (BAR publisher) into the child.
        self.assertEqual(SupervisedProbe(pid_probe, timeout_s=1).context.get_start_method(), "spawn")
        with self.assertRaises(ValueError):
            SupervisedProbe(pid_probe, timeout_s=0)

    def test_exit_reasons(self):
        self.assertEqual([exit_reason(code) for code in (-11, -9, 0, 3, None)],
                         ["SIGSEGV", "SIGKILL", "EXIT_0", "EXIT_3", "NO_EXIT"])


class SupervisedBacklogGateTests(unittest.TestCase):
    def test_a_healthy_child_returns_the_in_process_numbers(self):
        in_process = KafkaStageBacklog(_STAGES, read_committed=lambda group, _topic: _COMMITTED[group],
                                       read_ends=lambda topic: _ENDS[topic])
        probe = SupervisedProbe(fake_kafka_reader, (_STAGES,), timeout_s=20)
        self.addCleanup(probe.close)
        gate = KafkaStageBacklog(_STAGES, read_backlog=probe)
        self.assertEqual(gate.backlog(), in_process.backlog())
        self.assertEqual(gate.backlog(), {"core": 53, "a": 20})
        self.assertEqual(gate(), in_process())
        self.assertEqual(gate(), (False, {"backlog": {"core": 53, "a": 20}, "over": {"a": 20}}))

    def test_a_crashed_or_hung_probe_keeps_the_gate_closed(self):
        for factory, timeout_s, reason in ((segv_probe, 20.0, "SIGSEGV"), (hang_probe, 1.0, "TIMEOUT")):
            with self.subTest(reason=reason):
                probe = SupervisedProbe(factory, timeout_s=timeout_s)
                self.addCleanup(probe.close)
                admitted, detail = KafkaStageBacklog(_STAGES, read_backlog=probe)()
                self.assertFalse(admitted)
                self.assertEqual(detail["unknown"], reason)
                self.assertIn("ProbeUnavailable", detail["error"])

    def test_the_bar_edge_pauses_history_on_an_unknown_backlog(self):
        from qdl.runtime.stable_bar_edge import StableBinanceBarEdge

        probe = SupervisedProbe(segv_probe, timeout_s=20)
        self.addCleanup(probe.close)
        edge = object.__new__(StableBinanceBarEdge)
        edge.history_gate = KafkaStageBacklog(_STAGES, read_backlog=probe)
        edge._history_gate_closed_at = None
        edge.clock = lambda: 1000.0
        with self.assertLogs("qdl.runtime.stable_bar_edge", "WARNING") as logs:
            self.assertFalse(edge._history_gate_open())
        self.assertEqual(edge._history_gate_closed_at, 1000.0)
        self.assertIn("SIGSEGV", logs.output[0])

    def test_production_wiring_isolates_the_kafka_client_and_starts_lazily(self):
        from qdl.runtime import history_backpressure

        gate = kafka_backlog_from_config("broker:9092", _STAGES, {"security.protocol": "ssl"}, timeout_s=7)
        probe = gate._read_backlog
        self.assertIsInstance(probe, SupervisedProbe)
        self.assertIs(probe.factory, history_backpressure.kafka_backlog_in_process)
        self.assertEqual(probe.args, ("broker:9092", _STAGES, {"security.protocol": "ssl"}))
        self.assertEqual(probe.timeout_s, 7.0)
        self.assertIsNone(probe.proc)  # no child, and no Kafka client, until the first gate call

    def test_the_gate_never_imports_the_kafka_client_in_the_parent(self):
        # The bar edge itself does load confluent-kafka (BAR publisher); that is why the
        # child is spawned, not forked. The gate adds no librdkafka client to the parent.
        import subprocess

        code = ("import sys\n"
                "from qdl.runtime.history_backpressure import Stage, kafka_backlog_from_config\n"
                "kafka_backlog_from_config('broker:9092', [Stage('a', 'g', 't', 1)])\n"
                "print(sorted(m for m in sys.modules if m.startswith('confluent_kafka')))\n")
        result = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True, text=True, timeout=60,
                                check=True)
        self.assertEqual(result.stdout.strip(), "[]")

    def test_both_reader_shapes_are_exclusive(self):
        with self.assertRaises(ValueError):
            KafkaStageBacklog(_STAGES)
        with self.assertRaises(ValueError):
            KafkaStageBacklog(_STAGES, read_committed=lambda *_: {}, read_ends=lambda _t: {},
                              read_backlog=lambda: {})


if __name__ == "__main__":
    unittest.main()
