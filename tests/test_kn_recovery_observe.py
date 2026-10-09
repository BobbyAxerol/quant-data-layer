"""TEST_ONLY fakes for the KN native observation adapter; no broker, Redis or Docker."""
import json
import subprocess
import unittest
from concurrent.futures import Future
from unittest.mock import Mock

from scripts.kn_disk_pressure import DiskPressure
from scripts.kn_recovery_observe import (
    Deadline, DeadlineExpired, KafkaAdminSource, NativeObserver, ObservePolicy,
    docker_role_reader, redis_heartbeat_reader)
from scripts.kn_recovery_state import GATES, RecoveryController, RecoveryPolicy

TOPICS = {"md.canonical.v2": 2}
GROUPS = {"kn-projector-v3-a": {"md.canonical.v2": 2}}
ROLES = {name: {"id": d * 64, "image": "sha256:" + d * 64, "stage": stage}
         for name, d, stage in (("kafka1", "a", "infra"), ("market_projector_1", "b", "projector"),
                                ("market_data_service", "c", "reader"))}


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class FakeKafka:
    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.brokers = [1, 2, 3]
        self.isr = [1, 2, 3]
        self.leader = 1
        self.committed = {("md.canonical.v2", 0): 100, ("md.canonical.v2", 1): 200}
        self.ends = {("md.canonical.v2", 0): 100, ("md.canonical.v2", 1): 200}
        self.fail = {}
        self.delay = {}

    def _enter(self, name, deadline):
        self.calls.append(name)
        self.clock.now += self.delay.get(name, 0)
        if name in self.fail:
            raise self.fail[name]

    def describe_cluster(self, deadline):
        self._enter("describe_cluster", deadline)
        return {"broker_ids": list(self.brokers), "controller_id": self.brokers[0] if self.brokers else None}

    def describe_topics(self, topics, deadline):
        self._enter("describe_topics", deadline)
        return {t: [{"partition": p, "leader": self.leader, "replicas": [1, 2, 3], "isr": list(self.isr)}
                    for p in range(TOPICS[t])] for t in topics}

    def committed_offsets(self, group, roster, deadline):
        self._enter("committed_offsets", deadline)
        return dict(self.committed)

    def log_end_offsets(self, partitions, deadline):
        self._enter("log_end_offsets", deadline)
        return dict(self.ends)


def beat(clock_wall, **details):
    row = {"demanded_v2_slices": 60, "ready_v2_slices": 60, "session_ready_v2_slices": 60,
           "unhealthy_v2_slices": 0}
    row.update(details)
    status = row.pop("status", "READY")
    age = row.pop("age", 1)
    return json.dumps({"service_name": "market_data", "status": status,
                       "last_seen_unix": clock_wall - age, "details": row})


class Harness:
    def __init__(self, **policy):
        self.clock = Clock()
        self.wall = 2_000_000_000.0
        self.kafka = FakeKafka(self.clock)
        self.states = {name: "running" for name in ROLES}
        self.heartbeats = [beat(self.wall)]
        self.heartbeat_error = None
        self.disk_available = 800
        self.disk_calls = 0
        values = dict(disk_paths=["/kafka"], expected_brokers=3, topics=TOPICS, min_isr=2,
                      projector_groups=GROUPS, max_projector_lag_records=10,
                      heartbeat_max_age_seconds=30, role_stages={n: r["stage"] for n, r in ROLES.items()},
                      expected_demanded_slices=60)
        values.update(policy)
        self.observer = NativeObserver(ObservePolicy(**values), DiskPressure(100, 200, 50, 100, 10, 20),
                                       self.kafka, self.read_heartbeats, self.read_roles,
                                       sample=self.sample, clock=self.clock,
                                       wall_clock=lambda: self.wall)

    def sample(self, path):
        self.disk_calls += 1
        if self.disk_available is None:
            return None
        return {"device": 1, "monotonic": self.clock.now, "total_bytes": 1000,
                "available_bytes": self.disk_available, "total_inodes": 1000, "available_inodes": 800}

    def read_heartbeats(self, deadline):
        if self.heartbeat_error:
            raise self.heartbeat_error
        return list(self.heartbeats)

    def read_roles(self, deadline):
        return {n: {"id": r["id"], "image": r["image"], "state": self.states[n]} for n, r in ROLES.items()}

    def cycle(self, budget=5):
        report = self.observer.observe(budget)
        self.clock.now += 1
        self.wall += 1
        return report

    def primed(self):
        self.cycle()
        return self


class SourceTests(unittest.TestCase):
    def test_first_cycle_without_disk_baseline_is_unknown(self):
        report = Harness().cycle()
        self.assertEqual(report["gates"]["disk_safe"]["status"], "UNKNOWN")
        self.assertIsNone(report["observation"]["disk_safe"])
        self.assertEqual(report["gates"]["consumer_eligible"]["status"], "NOT_EVALUATED")
        self.assertEqual(report["mode"], "OBSERVE_ONLY")
        self.assertEqual(report["mutations"], 0)

    def test_healthy_all_gates_true(self):
        report = Harness().primed().cycle()
        for gate in GATES:
            self.assertIs(report["observation"][gate], True, gate)
        self.assertEqual(report["observation"]["roles"]["kafka1"]["state"], "running")

    def test_disk_missing_and_pressure(self):
        h = Harness().primed()
        h.disk_available = None
        self.assertIsNone(h.cycle()["observation"]["disk_safe"])
        h = Harness().primed()
        h.disk_available = 90
        report = h.cycle()
        self.assertIs(report["observation"]["disk_safe"], False)
        self.assertEqual(report["gates"]["disk_safe"]["reason"], "DISK_PRESSURE")
        self.assertEqual(h.kafka.calls, [])  # short-circuit: no Kafka call under pressure

    def test_quorum_partial_isr_offline_and_brokers_missing(self):
        for setup, reason in ((lambda h: setattr(h.kafka, "isr", [1, 2]), "UNDER_REPLICATED"),
                              (lambda h: setattr(h.kafka, "isr", [1]), "ISR_BELOW_MIN"),
                              (lambda h: setattr(h.kafka, "leader", None), "OFFLINE_PARTITION"),
                              (lambda h: setattr(h.kafka, "brokers", [1, 2]), "BROKERS_MISSING")):
            with self.subTest(reason=reason):
                h = Harness().primed()
                setup(h)
                report = h.cycle()
                self.assertEqual(report["gates"]["quorum_isr_ready"]["reason"], reason)
                self.assertIs(report["observation"]["quorum_isr_ready"], False)
                self.assertNotIn("committed_offsets", h.kafka.calls)

    def test_partial_isr_tolerated_only_by_explicit_policy(self):
        h = Harness(max_under_replicated=2).primed()
        h.kafka.isr = [1, 2]
        self.assertIs(h.cycle()["observation"]["quorum_isr_ready"], True)

    def test_quorum_timeout_and_error_are_unknown_without_retry(self):
        h = Harness().primed()
        h.kafka.fail["describe_cluster"] = TimeoutError()
        report = h.cycle()
        self.assertIsNone(report["observation"]["quorum_isr_ready"])
        self.assertEqual(report["gates"]["quorum_isr_ready"]["evidence"]["error_type"], "TimeoutError")
        self.assertEqual(h.kafka.calls.count("describe_cluster"), 1)  # single attempt, no retry

    def test_late_answer_after_absolute_deadline_is_unknown(self):
        h = Harness().primed()
        h.kafka.delay["describe_topics"] = 6
        report = h.cycle(budget=5)
        self.assertEqual(report["gates"]["quorum_isr_ready"]["reason"], "DEADLINE_EXCEEDED")
        self.assertIsNone(report["observation"]["quorum_isr_ready"])

    def test_expired_deadline_skips_remaining_calls(self):
        h = Harness().primed()
        h.kafka.delay["describe_cluster"] = 2
        h.kafka.delay["describe_topics"] = 2
        h.kafka.delay["committed_offsets"] = 2
        report = h.cycle(budget=5)
        self.assertEqual(report["gates"]["projector_progress_ready"]["reason"], "DEADLINE_EXCEEDED")
        self.assertNotIn("log_end_offsets", h.kafka.calls)

    def test_projector_lagging_stalled_regression_and_missing_commit(self):
        h = Harness().primed()
        h.cycle()  # committed-offset baseline
        h.kafka.ends = {k: v + 50 for k, v in h.kafka.ends.items()}
        self.assertEqual(h.cycle()["gates"]["projector_progress_ready"]["reason"], "PROJECTOR_STALLED")
        h.kafka.committed = {k: v + 5 for k, v in h.kafka.committed.items()}
        report = h.cycle()
        self.assertEqual(report["gates"]["projector_progress_ready"]["reason"], "PROJECTOR_LAGGING")
        self.assertIs(report["observation"]["projector_progress_ready"], False)
        h.kafka.committed = {k: v - 10 for k, v in h.kafka.committed.items()}
        report = h.cycle()
        self.assertEqual(report["gates"]["projector_progress_ready"]["status"], "FATAL")
        self.assertIsNone(report["observation"]["projector_progress_ready"])
        h = Harness().primed()
        h.kafka.committed[("md.canonical.v2", 1)] = -1001
        self.assertEqual(h.cycle()["gates"]["projector_progress_ready"]["reason"], "NO_COMMITTED_OFFSET")
        h = Harness().primed()
        del h.kafka.ends[("md.canonical.v2", 1)]
        self.assertEqual(h.cycle()["gates"]["projector_progress_ready"]["reason"], "OFFSET_ROSTER_MISMATCH")

    def test_consumer_heartbeat_cases(self):
        cases = (
            (lambda h: setattr(h, "heartbeats", []), "UNKNOWN", "HEARTBEAT_MISSING"),
            (lambda h: setattr(h, "heartbeats", [beat(h.wall, age=31)]), "UNKNOWN", "HEARTBEAT_STALE"),
            (lambda h: setattr(h, "heartbeats", [beat(h.wall, age=-5)]), "UNKNOWN", "HEARTBEAT_FROM_FUTURE"),
            (lambda h: setattr(h, "heartbeats", ["{not json"]), "UNKNOWN", "HEARTBEAT_UNPARSEABLE"),
            (lambda h: setattr(h, "heartbeats", [beat(h.wall, session_ready_v2_slices="60")]),
             "UNKNOWN", "HEARTBEAT_UNPARSEABLE"),
            (lambda h: setattr(h, "heartbeat_error", TimeoutError()), "UNKNOWN", "SOURCE_ERROR"),
            (lambda h: setattr(h, "heartbeats", [beat(h.wall, demanded_v2_slices=50,
                                                       ready_v2_slices=50, session_ready_v2_slices=50)]),
             "UNKNOWN", "DEMAND_ROSTER_MISMATCH"),
            (lambda h: setattr(h, "heartbeats", [beat(h.wall, status="DEGRADED")]), "NOT_READY", "TS_NOT_READY"),
            (lambda h: setattr(h, "heartbeats", [beat(h.wall, session_ready_v2_slices=59, ready_v2_slices=59,
                                                       unhealthy_v2_slices=1)]), "NOT_READY", "TS_NOT_READY"),
            (lambda h: setattr(h, "heartbeats", [beat(h.wall), beat(h.wall, status="DEGRADED")]),
             "NOT_READY", "TS_NOT_READY"),
        )
        for setup, status, reason in cases:
            with self.subTest(reason=reason, status=status):
                h = Harness().primed()
                setup(h)
                gate = h.cycle()["gates"]["consumer_eligible"]
                self.assertEqual((gate["status"], gate["reason"]), (status, reason))

    def test_running_container_never_makes_unknown_ready(self):
        h = Harness().primed()
        h.heartbeats = []
        report = h.cycle()
        self.assertEqual(report["observation"]["roles"]["market_data_service"]["state"], "running")
        self.assertIsNone(report["observation"]["consumer_eligible"])

    def test_stopped_stage_role_turns_unknown_into_not_ready(self):
        h = Harness().primed()
        h.states["kafka1"] = "exited"
        h.kafka.fail["describe_cluster"] = TimeoutError()
        gate = h.cycle()["gates"]["quorum_isr_ready"]
        self.assertEqual((gate["status"], gate["reason"]), ("NOT_READY", "STAGE_ROLE_NOT_RUNNING"))
        self.assertEqual(gate["evidence"]["unknown"], "SOURCE_ERROR")

    def test_fatal_never_downgraded(self):
        h = Harness().primed()
        h.cycle()
        h.states["market_projector_1"] = "exited"
        h.kafka.committed = {k: v - 1 for k, v in h.kafka.committed.items()}
        self.assertEqual(h.cycle()["gates"]["projector_progress_ready"]["status"], "FATAL")

    def test_role_source_failure_yields_null_roles(self):
        h = Harness().primed()
        h.read_roles = Mock(side_effect=OSError())
        h.observer.read_roles = h.read_roles
        report = h.cycle()
        self.assertIsNone(report["observation"]["roles"])
        self.assertEqual(report["roles_status"]["status"], "UNKNOWN")

    def test_policy_validation(self):
        base = dict(disk_paths=["/k"], expected_brokers=3, topics=TOPICS, min_isr=2,
                    projector_groups=GROUPS, max_projector_lag_records=0,
                    heartbeat_max_age_seconds=30, role_stages={"x": "infra"})
        ObservePolicy(**base)
        for key, value in (("disk_paths", []), ("expected_brokers", 0), ("min_isr", True),
                           ("topics", {"t": 0}), ("projector_groups", {}),
                           ("max_projector_lag_records", -1), ("heartbeat_max_age_seconds", float("inf")),
                           ("role_stages", {"x": "other"}), ("topics", {"t": 2000})):
            with self.subTest(key=key), self.assertRaises(ValueError):
                ObservePolicy(**dict(base, **{key: value}))

    def test_deadline(self):
        clock = Clock()
        deadline = Deadline(2, clock)
        self.assertEqual(deadline.remaining(), 2)
        clock.now += 2
        with self.assertRaises(DeadlineExpired):
            deadline.remaining()
        for value in (0, -1, float("nan"), "1"):
            with self.assertRaises(ValueError):
                Deadline(value, clock)


class NativeWiringTests(unittest.TestCase):
    """Real confluent_kafka request/result types with a fake AdminClient (no network)."""

    def setUp(self):
        self.clock = Clock()
        self.deadline = Deadline(5, self.clock)

    @staticmethod
    def done(value):
        future = Future()
        future.set_result(value)
        return future

    def test_admin_calls_are_bounded_and_parsed(self):
        from confluent_kafka import TopicPartition, TopicPartitionInfo, Node, ConsumerGroupTopicPartitions
        from confluent_kafka.admin import DescribeClusterResult, TopicDescription, ListOffsetsResultInfo
        nodes = [Node(i, "h", 9092) for i in (1, 2, 3)]
        admin = Mock()
        admin.describe_cluster.return_value = self.done(DescribeClusterResult(nodes[0], nodes))
        admin.describe_topics.return_value = {"t": self.done(TopicDescription(
            "t", None, False, [TopicPartitionInfo(0, nodes[0], nodes, nodes[:2])]))}
        admin.list_consumer_group_offsets.return_value = {"g": self.done(
            ConsumerGroupTopicPartitions("g", [TopicPartition("t", 0, 7)]))}
        admin.list_offsets.return_value = {TopicPartition("t", 0): self.done(ListOffsetsResultInfo(9, -1, -1))}
        source = KafkaAdminSource(admin)
        self.assertEqual(source.describe_cluster(self.deadline), {"broker_ids": [1, 2, 3], "controller_id": 1})
        self.assertEqual(source.describe_topics(("t",), self.deadline),
                         {"t": [{"partition": 0, "leader": 1, "replicas": [1, 2, 3], "isr": [1, 2]}]})
        self.assertEqual(source.committed_offsets("g", {"t": 1}, self.deadline), {("t", 0): 7})
        self.assertEqual(source.log_end_offsets((("t", 0),), self.deadline), {("t", 0): 9})
        for call in (admin.describe_cluster, admin.describe_topics,
                     admin.list_consumer_group_offsets, admin.list_offsets):
            self.assertLessEqual(call.call_args.kwargs["request_timeout"], 5)
        from confluent_kafka import IsolationLevel
        self.assertEqual(admin.list_offsets.call_args.kwargs["isolation_level"], IsolationLevel.READ_COMMITTED)
        self.assertFalse(any("alter" in name or "delete" in name or "create" in name
                             for name, _, _ in admin.mock_calls))

    def test_expired_deadline_makes_no_admin_call(self):
        admin = Mock()
        self.clock.now += 5
        with self.assertRaises(DeadlineExpired):
            KafkaAdminSource(admin).describe_cluster(self.deadline)
        admin.describe_cluster.assert_not_called()

    def test_redis_reader_is_scan_get_only_and_bounded(self):
        client = Mock()
        client.scan_iter.return_value = iter(["service:heartbeat:market_data:a"])
        client.get.return_value = b"{}"
        self.assertEqual(redis_heartbeat_reader(client)(self.deadline), [b"{}"])
        self.assertEqual({c[0] for c in client.mock_calls}, {"scan_iter", "get"})
        client.scan_iter.return_value = iter(["k%d" % i for i in range(100)])
        with self.assertRaises(ValueError):
            redis_heartbeat_reader(client)(self.deadline)

    def test_docker_reader_inspect_only_and_identity_checked(self):
        rows = "\n".join("%s|%s|/%s|running" % (r["id"], r["image"], n) for n, r in ROLES.items())
        run = Mock(return_value=subprocess.CompletedProcess([], 0, rows))
        roles = docker_role_reader(ROLES, run=run)(self.deadline)
        self.assertEqual(roles["kafka1"]["state"], "running")
        self.assertEqual(run.call_args.args[0][:2], ["docker", "inspect"])
        self.assertLessEqual(run.call_args.kwargs["timeout"], 5)
        run.return_value = subprocess.CompletedProcess([], 0, rows.replace("/kafka1", "/other"))
        with self.assertRaises(ValueError):
            docker_role_reader(ROLES, run=run)(self.deadline)


class EndToEndTests(unittest.TestCase):
    """Adapter -> existing RecoveryController: ordered progression and fail-closed UNKNOWN."""

    def test_ordered_gate_progression_and_unknown(self):
        h = Harness()
        controller = RecoveryController(
            RecoveryPolicy(deadline_seconds=120, max_observation_age_seconds=10, stable_samples=2,
                           retry_seconds=2, max_start_attempts=3), ROLES, h.clock)

        def step():
            report = h.observer.observe(5)
            h.clock.now += 1
            h.wall += 1
            return controller.step(report["observation"])

        trace = []
        # 1: no disk baseline -> HOLD_UNKNOWN, no proposal.
        result = step()
        trace.append(result["state"])
        self.assertEqual((result["state"], result["actions"]), ("HOLD_UNKNOWN", []))
        # 2: broker exited and unreachable -> definitive NOT_READY -> exact start proposal.
        h.states["kafka1"] = "exited"
        h.kafka.fail["describe_cluster"] = TimeoutError()
        result = step()
        trace.append(result["state"])
        self.assertEqual(result["state"], "WAIT_QUORUM")
        self.assertEqual(result["actions"][0]["role"], "kafka1")
        # 3: broker running, ISR partial -> still WAIT_QUORUM, no further action.
        h.states["kafka1"] = "running"
        del h.kafka.fail["describe_cluster"]
        h.kafka.isr = [1, 2]
        result = step()
        trace.append(result["state"])
        self.assertEqual((result["state"], result["actions"]), ("WAIT_QUORUM", []))
        # 4: ISR full, projector lagging -> WAIT_PROJECTOR.
        h.kafka.isr = [1, 2, 3]
        h.kafka.ends = {k: v + 500 for k, v in h.kafka.ends.items()}
        result = step()
        trace.append(result["state"])
        self.assertEqual(result["state"], "WAIT_PROJECTOR")
        # 5: projector caught up, TS not ready -> WAIT_CONSUMER.
        h.kafka.committed = dict(h.kafka.ends)
        h.heartbeats = [beat(h.wall, status="DEGRADED", session_ready_v2_slices=40,
                             ready_v2_slices=40, unhealthy_v2_slices=20)]
        result = step()
        trace.append(result["state"])
        self.assertEqual(result["state"], "WAIT_CONSUMER")
        # 6: TS heartbeat missing while the reader container is Up -> HOLD_UNKNOWN.
        h.heartbeats = []
        result = step()
        trace.append(result["state"])
        self.assertEqual(result["state"], "HOLD_UNKNOWN")
        # 7-8: TS session-ready -> stable samples -> SERVING.
        for expected in ("WAIT_CONSUMER", "SERVING"):
            h.heartbeats = [beat(h.wall)]
            result = step()
            trace.append(result["state"])
            self.assertEqual(result["state"], expected)
        # 9: stale heartbeat after SERVING -> HOLD_UNKNOWN (new bounded episode).
        h.heartbeats = [beat(h.wall, age=60)]
        result = step()
        trace.append(result["state"])
        self.assertEqual(result["state"], "HOLD_UNKNOWN")
        self.assertEqual(trace, ["HOLD_UNKNOWN", "WAIT_QUORUM", "WAIT_QUORUM", "WAIT_PROJECTOR",
                                 "WAIT_CONSUMER", "HOLD_UNKNOWN", "WAIT_CONSUMER", "SERVING",
                                 "HOLD_UNKNOWN"])

    def test_unknown_gate_never_reaches_serving(self):
        h = Harness().primed()
        h.heartbeats = []
        controller = RecoveryController(
            RecoveryPolicy(deadline_seconds=20, max_observation_age_seconds=10, stable_samples=1,
                           retry_seconds=1, max_start_attempts=1), ROLES, h.clock)
        states = set()
        for _ in range(25):
            report = h.observer.observe(5)
            h.clock.now += 1
            h.wall += 1
            states.add(controller.step(report["observation"])["state"])
        self.assertNotIn("SERVING", states)
        self.assertIn("BLOCKED", states)


if __name__ == "__main__":
    unittest.main()
