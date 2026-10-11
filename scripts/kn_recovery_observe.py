#!/usr/bin/env python3
"""Read-only native observation adapter for the KN recovery coordinator (D4/R2).

Purpose: turn real observation sources into the input of
``kn_recovery_state.RecoveryController.step``:

- disk pressure: ``kn_disk_pressure.sample_disk`` + operator ``DiskPressure``
  policy (reused, not duplicated; the same module the production watcher uses);
- Kafka quorum/ISR: AdminClient ``describe_cluster``/``describe_topics``;
- projector committed progress: ``list_consumer_group_offsets`` against the
  read_committed log end (``list_offsets``, LSO) for the projector groups;
- consumer eligibility: the TS writer heartbeat
  (``service:heartbeat:market_data:*``: status / demanded_v2_slices /
  session_ready_v2_slices / last_seen_unix) read through a provided callable;
- role identity/state: a provided callable (default ``docker inspect``).

Boundary: observe/propose only. No Kafka group join or offset commit, no Redis
write, no container lifecycle call; ``mutations`` is always 0. Every source call
receives the remaining time of ONE absolute deadline; there are no retries, and
an answer that arrives after the deadline is UNKNOWN. A container being "Up"
never makes a gate READY; a stage role that is observed not running only turns
that stage's UNKNOWN gate into a definitive NOT_READY so the coordinator may
propose an exact start. Missing, stale, unparseable, late, FATAL or UNKNOWN
evidence is emitted as a non-boolean gate, which the coordinator holds as
HOLD_UNKNOWN; it can never become SERVING.

Gates are evaluated in the coordinator's order and short-circuit: once a gate is
not READY, later sources are not queried (reported NOT_EVALUATED, emitted False,
which the coordinator never reaches because it stops at the first failed gate).
"""
import argparse
import json
import math
import os
from pathlib import Path
import subprocess
import time

if __package__:
    from scripts.kn_disk_pressure import DiskPressure, sample_disk
    from scripts.kn_recovery_state import GATES, SCHEMA, STAGES
else:
    from kn_disk_pressure import DiskPressure, sample_disk
    from kn_recovery_state import GATES, SCHEMA, STAGES


REPORT_SCHEMA = "qdl.kn.recovery-native-report.v1"
READY, NOT_READY, UNKNOWN, FATAL, NOT_EVALUATED = (
    "READY", "NOT_READY", "UNKNOWN", "FATAL", "NOT_EVALUATED")
GATE_STAGE = {"quorum_isr_ready": "infra", "projector_progress_ready": "projector",
              "consumer_eligible": "reader"}
HEARTBEAT_PATTERN = "service:heartbeat:market_data:*"
MAX_PARTITIONS = 1024
MAX_HEARTBEATS = 64
MAX_HEARTBEAT_BYTES = 1 << 20


class DeadlineExpired(Exception):
    pass


class Deadline:
    """One absolute monotonic deadline shared by every call of one cycle."""

    def __init__(self, seconds, clock=time.monotonic):
        if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("budget must be finite and positive")
        self._clock = clock
        self.at = clock() + seconds

    def remaining(self):
        left = self.at - self._clock()
        if not left > 0:
            raise DeadlineExpired("DEADLINE_EXCEEDED")
        return left

    def expired(self):
        return not self.at - self._clock() > 0


def _gate(status, reason, /, **evidence):
    return {"status": status, "reason": reason, "evidence": evidence}


def _combine(results):
    for status in (FATAL, UNKNOWN, NOT_READY):
        for result in results:
            if result["status"] == status:
                return result
    return results[0] if results else _gate(UNKNOWN, "NO_SOURCES")


def _count(value):
    return type(value) is int and value >= 0


class ObservePolicy:
    """Explicit operator inputs; no production threshold is defaulted here."""

    def __init__(self, *, disk_paths, expected_brokers, topics, min_isr, projector_groups,
                 max_projector_lag_records, heartbeat_max_age_seconds, role_stages,
                 max_under_replicated=0, heartbeat_future_skew_seconds=1.0,
                 expected_demanded_slices=None, expected_heartbeat_instances=None):
        if (not isinstance(disk_paths, (list, tuple)) or not disk_paths
                or len(set(disk_paths)) != len(disk_paths)
                or any(not isinstance(p, str) or not p for p in disk_paths)):
            raise ValueError("disk_paths must be distinct nonempty strings")
        for name, value in (("expected_brokers", expected_brokers), ("min_isr", min_isr)):
            if type(value) is not int or value <= 0:
                raise ValueError(name + " must be a positive integer")
        for name, value in (("max_under_replicated", max_under_replicated),
                            ("max_projector_lag_records", max_projector_lag_records)):
            if not _count(value):
                raise ValueError(name + " must be a non-negative integer")
        for name, value in (("heartbeat_max_age_seconds", heartbeat_max_age_seconds),
                            ("heartbeat_future_skew_seconds", heartbeat_future_skew_seconds)):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(name + " must be finite and positive")
        for name, value in (("expected_demanded_slices", expected_demanded_slices),
                            ("expected_heartbeat_instances", expected_heartbeat_instances)):
            if value is not None and (type(value) is not int or value <= 0):
                raise ValueError(name + " must be a positive integer or None")
        self.topics = self._roster(topics, "topics")
        if not isinstance(projector_groups, dict) or not projector_groups:
            raise ValueError("projector_groups must be a nonempty mapping")
        self.projector_groups = {}
        for group, roster in projector_groups.items():
            if not isinstance(group, str) or not group:
                raise ValueError("invalid projector group")
            self.projector_groups[group] = self._roster(roster, "projector group roster")
        if (not isinstance(role_stages, dict) or not role_stages
                or any(not isinstance(k, str) or v not in STAGES for k, v in role_stages.items())):
            raise ValueError("role_stages must map role names to stages")
        self.disk_paths = tuple(disk_paths)
        self.expected_brokers = expected_brokers
        self.min_isr = min_isr
        self.max_under_replicated = max_under_replicated
        self.max_projector_lag_records = max_projector_lag_records
        self.heartbeat_max_age_seconds = heartbeat_max_age_seconds
        self.heartbeat_future_skew_seconds = heartbeat_future_skew_seconds
        self.expected_demanded_slices = expected_demanded_slices
        self.expected_heartbeat_instances = expected_heartbeat_instances
        self.role_stages = dict(role_stages)

    @staticmethod
    def _roster(value, name):
        if (not isinstance(value, dict) or not value
                or any(not isinstance(t, str) or not t or type(n) is not int or n <= 0
                       for t, n in value.items())
                or sum(value.values()) > MAX_PARTITIONS):
            raise ValueError(name + " must map topics to bounded partition counts")
        return dict(value)


class NativeObserver:
    """Stateful across cycles only for disk baselines and committed-offset regression.

    Sources (all read-only; each receives the cycle ``Deadline``):
    - ``kafka``: object with describe_cluster(deadline) -> {broker_ids, controller_id},
      describe_topics(topics, deadline) -> {topic: [{partition, leader, replicas, isr}]},
      committed_offsets(group, roster, deadline) -> {(topic, partition): offset},
      log_end_offsets(partitions, deadline) -> {(topic, partition): offset}
    - ``read_heartbeats(deadline)`` -> list of raw JSON str/bytes or dicts
    - ``read_roles(deadline)`` -> {role: {id, image, state}}
    - ``sample(path)`` -> kn_disk_pressure.sample_disk shaped dict or None
    """

    def __init__(self, policy, disk_pressure, kafka, read_heartbeats, read_roles,
                 sample=sample_disk, clock=time.monotonic, wall_clock=time.time):
        if not isinstance(policy, ObservePolicy) or not isinstance(disk_pressure, DiskPressure):
            raise ValueError("policy and DiskPressure are required")
        self.policy = policy
        self.disk = disk_pressure
        self.kafka = kafka
        self.read_heartbeats = read_heartbeats
        self.read_roles = read_roles
        self.sample = sample
        self.clock = clock
        self.wall_clock = wall_clock
        self._committed = {}

    # ---- sources -------------------------------------------------------
    def _call(self, deadline, function, *args):
        """Single bounded attempt; late answers are not trusted."""
        try:
            deadline.remaining()
            value = function(*args, deadline)
        except DeadlineExpired:
            return None, _gate(UNKNOWN, "DEADLINE_EXCEEDED")
        except Exception as exc:  # source failures are evidence gaps, never success
            return None, _gate(UNKNOWN, "SOURCE_ERROR", error_type=type(exc).__name__)
        if deadline.expired():
            return None, _gate(UNKNOWN, "DEADLINE_EXCEEDED", late=True)
        return value, None

    def observe_disk(self):
        results = []
        for path in self.policy.disk_paths:
            outcome = self.disk.observe(path, self.sample(path))
            reasons = list(outcome["reasons"])
            definitive = [r for r in reasons if r != "DISK_OBSERVATION_UNKNOWN"]
            if definitive:
                results.append(_gate(NOT_READY, "DISK_PRESSURE", path=path, reasons=reasons))
            elif reasons:
                results.append(_gate(UNKNOWN, "DISK_OBSERVATION_UNKNOWN", path=path))
            else:
                results.append(_gate(READY, "DISK_SAFE", path=path, metrics=outcome.get("metrics")))
        return _combine(results)

    def observe_quorum(self, deadline):
        cluster, error = self._call(deadline, self.kafka.describe_cluster)
        if error:
            return error
        try:
            brokers = cluster["broker_ids"]
            controller = cluster["controller_id"]
            if (not isinstance(brokers, (list, tuple)) or any(type(b) is not int for b in brokers)
                    or len(set(brokers)) != len(brokers)):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            return _gate(UNKNOWN, "CLUSTER_UNPARSEABLE")
        if len(brokers) < self.policy.expected_brokers:
            return _gate(NOT_READY, "BROKERS_MISSING", brokers=sorted(brokers),
                         expected=self.policy.expected_brokers)
        if len(brokers) > self.policy.expected_brokers or controller not in brokers:
            return _gate(UNKNOWN, "CLUSTER_ROSTER_MISMATCH", brokers=sorted(brokers),
                         controller=controller)
        topics, error = self._call(deadline, self.kafka.describe_topics, tuple(self.policy.topics))
        if error:
            return error
        offline, below_min, under = [], [], []
        try:
            if set(topics) != set(self.policy.topics):
                return _gate(UNKNOWN, "TOPIC_ROSTER_MISMATCH")
            for topic, count in self.policy.topics.items():
                partitions = topics[topic]
                if sorted(p["partition"] for p in partitions) != list(range(count)):
                    return _gate(UNKNOWN, "PARTITION_ROSTER_MISMATCH", topic=topic)
                for row in partitions:
                    replicas, isr, leader = list(row["replicas"]), list(row["isr"]), row["leader"]
                    if (any(type(v) is not int for v in replicas + isr)
                            or not set(isr) <= set(replicas) or not replicas):
                        return _gate(UNKNOWN, "PARTITION_UNPARSEABLE", topic=topic)
                    where = (topic, row["partition"])
                    if leader is None or type(leader) is not int or leader < 0 or leader not in isr:
                        offline.append(where)
                    if len(isr) < self.policy.min_isr:
                        below_min.append(where)
                    if len(isr) < len(replicas):
                        under.append(where)
        except (KeyError, TypeError, ValueError):
            return _gate(UNKNOWN, "PARTITION_UNPARSEABLE")
        evidence = {"brokers": sorted(brokers), "offline": len(offline),
                    "below_min_isr": len(below_min), "under_replicated": len(under),
                    "first": [list(x) for x in (offline + below_min + under)[:16]]}
        if offline:
            return _gate(NOT_READY, "OFFLINE_PARTITION", **evidence)
        if below_min:
            return _gate(NOT_READY, "ISR_BELOW_MIN", **evidence)
        if len(under) > self.policy.max_under_replicated:
            return _gate(NOT_READY, "UNDER_REPLICATED", **evidence)
        return _gate(READY, "QUORUM_ISR_READY", **evidence)

    def observe_projector(self, deadline):
        results = []
        for group, roster in self.policy.projector_groups.items():
            expected = {(t, p) for t, n in roster.items() for p in range(n)}
            committed, error = self._call(deadline, self.kafka.committed_offsets, group, roster)
            if error:
                return dict(error, evidence=dict(error["evidence"], group=group))
            ends, error = self._call(deadline, self.kafka.log_end_offsets, tuple(sorted(expected)))
            if error:
                return dict(error, evidence=dict(error["evidence"], group=group))
            if (not isinstance(committed, dict) or not isinstance(ends, dict)
                    or set(committed) != expected or set(ends) != expected
                    or any(type(v) is not int for v in list(committed.values()) + list(ends.values()))):
                return _gate(UNKNOWN, "OFFSET_ROSTER_MISMATCH", group=group)
            if any(v < 0 for v in committed.values()):
                return _gate(UNKNOWN, "NO_COMMITTED_OFFSET", group=group)
            if any(committed[k] > ends[k] for k in expected):
                return _gate(UNKNOWN, "COMMITTED_BEYOND_END", group=group)
            previous = self._committed.get(group, {})
            if any(committed[k] < previous.get(k, -1) for k in expected):
                # A committed offset moving backwards means reset/replay: operator review.
                return _gate(FATAL, "COMMITTED_REGRESSION", group=group)
            self._committed[group] = dict(committed)
            lags = {k: ends[k] - committed[k] for k in expected}
            worst = max(lags.values())
            advanced = bool(previous) and any(committed[k] > previous[k] for k in expected)
            evidence = {"group": group, "max_lag_records": worst,
                        "total_lag_records": sum(lags.values()),
                        "advanced_since_previous": advanced if previous else None}
            if worst > self.policy.max_projector_lag_records:
                reason = "PROJECTOR_STALLED" if previous and not advanced else "PROJECTOR_LAGGING"
                results.append(_gate(NOT_READY, reason, **evidence))
            else:
                results.append(_gate(READY, "PROJECTOR_CAUGHT_UP", **evidence))
        return _combine(results)

    def observe_consumer(self, deadline):
        rows, error = self._call(deadline, self.read_heartbeats)
        if error:
            return error
        if not isinstance(rows, (list, tuple)) or not rows:
            return _gate(UNKNOWN, "HEARTBEAT_MISSING")
        if len(rows) > MAX_HEARTBEATS:
            return _gate(UNKNOWN, "HEARTBEAT_ROSTER_TOO_LARGE")
        expected = self.policy.expected_heartbeat_instances
        if expected is not None and len(rows) != expected:
            return _gate(UNKNOWN, "HEARTBEAT_INSTANCE_MISMATCH", instances=len(rows))
        now = self.wall_clock()
        results = []
        for raw in rows:
            try:
                if isinstance(raw, (bytes, str)):
                    if len(raw) > MAX_HEARTBEAT_BYTES:
                        raise ValueError
                    raw = json.loads(raw)
                beat = raw
                seen, status, details = beat["last_seen_unix"], beat["status"], beat["details"]
                if (type(seen) not in (int, float) or not math.isfinite(seen)
                        or not isinstance(status, str) or not isinstance(details, dict)):
                    raise ValueError
                demanded = details["demanded_v2_slices"]
                session = details["session_ready_v2_slices"]
                if not _count(demanded) or not _count(session) or session > demanded:
                    raise ValueError
                extra = {k: details[k] for k in ("ready_v2_slices", "unhealthy_v2_slices") if k in details}
                if any(not _count(v) for v in extra.values()):
                    raise ValueError
            except (KeyError, TypeError, ValueError):
                results.append(_gate(UNKNOWN, "HEARTBEAT_UNPARSEABLE"))
                continue
            age = now - seen
            evidence = {"status": status, "demanded": demanded, "session_ready": session,
                        "age_seconds": round(age, 3)}
            if age < -self.policy.heartbeat_future_skew_seconds:
                results.append(_gate(UNKNOWN, "HEARTBEAT_FROM_FUTURE", **evidence))
            elif age > self.policy.heartbeat_max_age_seconds:
                results.append(_gate(UNKNOWN, "HEARTBEAT_STALE", **evidence))
            elif (self.policy.expected_demanded_slices is not None
                  and demanded != self.policy.expected_demanded_slices):
                results.append(_gate(UNKNOWN, "DEMAND_ROSTER_MISMATCH", **evidence))
            elif (status != "READY" or demanded == 0 or session != demanded
                  or extra.get("ready_v2_slices", session) != session
                  or extra.get("unhealthy_v2_slices", 0) != 0):
                results.append(_gate(NOT_READY, "TS_NOT_READY", **evidence))
            else:
                results.append(_gate(READY, "TS_SESSION_READY", **evidence))
        return _combine(results)

    def observe_roles(self, deadline):
        roles, error = self._call(deadline, self.read_roles)
        if error:
            return None, error
        if not isinstance(roles, dict) or set(roles) != set(self.policy.role_stages):
            return None, _gate(UNKNOWN, "ROLE_ROSTER_MISMATCH")
        clean = {}
        for name, role in roles.items():
            if not isinstance(role, dict) or any(not isinstance(role.get(k), str)
                                                 for k in ("id", "image", "state")):
                return None, _gate(UNKNOWN, "ROLE_UNPARSEABLE", role=name)
            clean[name] = {k: role[k] for k in ("id", "image", "state")}
        return clean, None

    # ---- composition ---------------------------------------------------
    def observe(self, budget_seconds):
        """One bounded cycle -> {schema, mode, observation, gates, roles_status}."""
        started = self.clock()
        deadline = Deadline(budget_seconds, self.clock)
        roles, role_error = self.observe_roles(deadline)
        gates = {"disk_safe": self.observe_disk()}
        observers = {"quorum_isr_ready": self.observe_quorum,
                     "projector_progress_ready": self.observe_projector,
                     "consumer_eligible": self.observe_consumer}
        blocked = gates["disk_safe"]["status"] != READY
        for gate in GATES[1:]:
            if blocked:
                gates[gate] = _gate(NOT_EVALUATED, "UPSTREAM_GATE_NOT_READY")
                continue
            result = observers[gate](deadline)
            if result["status"] == UNKNOWN and roles is not None:
                stopped = sorted(n for n, s in self.policy.role_stages.items()
                                 if s == GATE_STAGE[gate] and roles[n]["state"] != "running")
                if stopped:
                    # Not-running is definitive negative evidence; running is never positive.
                    result = _gate(NOT_READY, "STAGE_ROLE_NOT_RUNNING", roles=stopped,
                                   unknown=result["reason"])
            gates[gate] = result
            blocked = result["status"] != READY
        observation = {"schema": SCHEMA, "observed_monotonic": started,
                       "roles": roles if roles is not None else None}
        for gate in GATES:
            status = gates[gate]["status"]
            observation[gate] = (True if status == READY else
                                 False if status in (NOT_READY, NOT_EVALUATED) else None)
        return {"schema": REPORT_SCHEMA, "mode": "OBSERVE_ONLY", "mutations": 0,
                "deadline_monotonic": deadline.at, "observation": observation, "gates": gates,
                "roles_status": role_error or _gate(READY, "ROLES_OBSERVED")}


# ---- native source wiring (lazy imports; read-only calls only) ---------------
class KafkaAdminSource:
    """confluent_kafka AdminClient wrapper; every request uses the remaining budget."""

    def __init__(self, admin):
        self.admin = admin

    def describe_cluster(self, deadline):
        result = self.admin.describe_cluster(request_timeout=deadline.remaining()).result(
            timeout=deadline.remaining())
        controller = getattr(result, "controller", None)
        return {"broker_ids": [node.id for node in result.nodes],
                "controller_id": controller.id if controller is not None else None}

    def describe_topics(self, topics, deadline):
        from confluent_kafka import TopicCollection
        futures = self.admin.describe_topics(TopicCollection(list(topics)),
                                             request_timeout=deadline.remaining())
        out = {}
        for topic, future in futures.items():
            description = future.result(timeout=deadline.remaining())
            out[topic] = [{"partition": p.id,
                           "leader": p.leader.id if p.leader is not None else None,
                           "replicas": [n.id for n in p.replicas], "isr": [n.id for n in p.isr]}
                          for p in description.partitions]
        return out

    def committed_offsets(self, group, roster, deadline):
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
        request = ConsumerGroupTopicPartitions(
            group, [TopicPartition(t, p) for t, n in sorted(roster.items()) for p in range(n)])
        result = self.admin.list_consumer_group_offsets(
            [request], request_timeout=deadline.remaining())[group].result(timeout=deadline.remaining())
        out = {}
        for item in result.topic_partitions:
            if item.error is not None:
                raise ValueError("partition offset error")
            out[(item.topic, item.partition)] = item.offset
        return out

    def log_end_offsets(self, partitions, deadline):
        from confluent_kafka import IsolationLevel, TopicPartition
        from confluent_kafka.admin import OffsetSpec
        futures = self.admin.list_offsets(
            {TopicPartition(t, p): OffsetSpec.latest() for t, p in partitions},
            isolation_level=IsolationLevel.READ_COMMITTED, request_timeout=deadline.remaining())
        return {(tp.topic, tp.partition): future.result(timeout=deadline.remaining()).offset
                for tp, future in futures.items()}


def redis_heartbeat_reader(client, pattern=HEARTBEAT_PATTERN):
    """Read-only SCAN/GET; client must carry socket timeouts <= the cycle budget."""
    def read(deadline):
        rows = []
        for key in client.scan_iter(match=pattern, count=100):
            deadline.remaining()
            if len(rows) >= MAX_HEARTBEATS:
                raise ValueError("heartbeat roster exceeds bound")
            value = client.get(key)
            if value is not None:
                rows.append(value)
        return rows
    return read


def docker_role_reader(roles, run=subprocess.run):
    """One read-only `docker inspect` for the pinned roster IDs; no lifecycle call."""
    by_id = {role["id"]: name for name, role in roles.items()}

    def read(deadline):
        result = run(["docker", "inspect", "--format",
                      "{{.Id}}|{{.Image}}|{{.Name}}|{{.State.Status}}", *by_id],
                     capture_output=True, text=True, timeout=deadline.remaining(), check=False)
        if result.returncode:
            raise ValueError("inspect failed")
        out = {}
        for line in result.stdout.splitlines():
            ident, image, name, state = line.split("|")
            role = by_id.get(ident)
            if role is None or "/" + role != name or role in out:
                raise ValueError("inspect identity drift")
            out[role] = {"id": ident, "image": image, "state": state}
        return out
    return read


def kafka_tls_config(bootstrap, tls):
    """Same mTLS shape as scripts/p184_dr08_isolated.py:kafka_security."""
    config = {"bootstrap.servers": bootstrap}
    if tls:
        config.update({"security.protocol": "SSL", "ssl.ca.location": tls["ca"],
                       "ssl.certificate.location": tls["cert"], "ssl.key.location": tls["key"]})
    return config


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--config", required=True, type=Path,
                        help="JSON: bootstrap, tls{ca,cert,key}, roles, policy, disk_thresholds, "
                             "redis_url_env, heartbeat_pattern")
    parser.add_argument("--budget-seconds", type=float, default=8.0)
    parser.add_argument("--disk-baseline-seconds", type=float, default=1.0,
                        help="Second disk sample spacing so growth is measured, not assumed")
    args = parser.parse_args(argv)
    config = json.loads(args.config.read_text())
    roles = config["roles"]
    policy = ObservePolicy(role_stages={n: r["stage"] for n, r in roles.items()}, **config["policy"])
    disk = DiskPressure(*(config["disk_thresholds"][k] for k in (
        "reserve_bytes", "recovery_bytes", "reserve_inodes", "recovery_inodes",
        "reserve_seconds", "recovery_seconds")))
    if not 0 < args.disk_baseline_seconds < args.budget_seconds:
        parser.error("disk baseline must be inside the budget")
    from confluent_kafka.admin import AdminClient
    import redis
    url = os.environ[config["redis_url_env"]]
    socket_timeout = min(2.0, args.budget_seconds)
    client = redis.Redis.from_url(url, socket_timeout=socket_timeout,
                                  socket_connect_timeout=socket_timeout)
    observer = NativeObserver(
        policy, disk, KafkaAdminSource(AdminClient(kafka_tls_config(config["bootstrap"], config.get("tls")))),
        redis_heartbeat_reader(client, config.get("heartbeat_pattern", HEARTBEAT_PATTERN)),
        docker_role_reader(roles))
    for path in policy.disk_paths:  # baseline only; never asserts readiness
        disk.observe(path, sample_disk(path))
    time.sleep(args.disk_baseline_seconds)
    report = observer.observe(args.budget_seconds - args.disk_baseline_seconds)
    output = dict(report["observation"], adapter_report=report)
    print(json.dumps(output, separators=(",", ":"), allow_nan=False, default=list))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
