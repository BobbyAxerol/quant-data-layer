"""KN-3 K3.1: the state topic / projector ACL packet is exact, sealed,
idempotent and refuses anything outside its allowlist.

Unit tests drive a fake broker that speaks the Kafka 4.2 CLI output format
(captured from the pinned ``apache/kafka@sha256:9516fb76...`` image). The
integration test runs against a disposable broker and is skipped unless
``QDL_KN_TEST_KAFKA_CONTAINER`` names one (it needs the host Docker CLI).
"""

from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest

from scripts import kn_state_topics_packet as packet
from scripts.kn_state_topics_packet import (
    CommandResult,
    Refused,
    apply,
    build_plan,
    isolated_target,
    plan_commands,
    production_target,
    review,
    seal,
    validate_commands,
    validate_plan,
    verify,
)

PRINCIPAL = "User:kn-projector"
# Owner decision, Astra KN-3 review R1: both state topics, never md.canonical.v2.
SEGMENT_CONFIGS = {"segment.ms": "3600000", "segment.bytes": "134217728"}
ISO = {"backend": "isolated-plaintext", "bootstrap": "localhost:9092", "exec_container": "kn3-pkt-kafka"}

EXACT_ACLS = {
    ("TOPIC", "md.canonical.v2", "LITERAL", "READ"),
    ("TOPIC", "md.canonical.v2", "LITERAL", "DESCRIBE"),
    ("GROUP", "kn-projector-v3-a", "LITERAL", "READ"),
    ("GROUP", "kn-projector-v3-b", "LITERAL", "READ"),
    ("TOPIC", "md.latest.v2", "LITERAL", "WRITE"),
    ("TOPIC", "md.latest.v2", "LITERAL", "DESCRIBE"),
    ("TOPIC", "md.latest.v2", "LITERAL", "READ"),
    ("TOPIC", "md.bars.v2", "LITERAL", "WRITE"),
    ("TOPIC", "md.bars.v2", "LITERAL", "DESCRIBE"),
    ("TOPIC", "md.bars.v2", "LITERAL", "READ"),
    ("TRANSACTIONAL_ID", "kn-projector-v3-", "PREFIXED", "WRITE"),
    ("TRANSACTIONAL_ID", "kn-projector-v3-", "PREFIXED", "DESCRIBE"),
    ("CLUSTER", "kafka-cluster", "LITERAL", "IDEMPOTENT_WRITE"),
}


class FakeBroker:
    """In-memory broker answering the packet's CLI commands in Kafka 4.2 format."""

    def __init__(self) -> None:
        self.topics: dict[str, dict] = {}
        self.acls: list[dict[str, str]] = []
        self.calls: list[list[str]] = []

    def describe(self):
        return {"backend": "fake"}

    def preflight(self):
        return None

    def add_topic(self, name, partitions, rf, configs):
        self.topics[name] = {"partitions": partitions, "rf": rf, "configs": dict(configs)}

    def add_acl(self, rtype, name, pattern, principal, operation, permission="ALLOW"):
        row = {"resource_type": rtype, "resource_name": name, "pattern_type": pattern,
               "principal": principal, "host": "*", "operation": operation, "permission": permission}
        if row not in self.acls:
            self.acls.append(row)

    def run(self, command):
        command = list(command)
        self.calls.append(command)
        tool, args = command[0], command[1:]
        if tool == "kafka-topics.sh" and args == ["--list"]:
            return CommandResult(0, "".join(f"{n}\n" for n in sorted(self.topics)), "")
        if tool == "kafka-topics.sh" and args[:2] == ["--describe", "--topic"]:
            topic = self.topics.get(args[2])
            if topic is None:
                return CommandResult(1, "", f"Topic '{args[2]}' does not exist as expected")
            configs = ",".join(f"{k}={v}" for k, v in topic["configs"].items())
            lines = [f"Topic: {args[2]}\tTopicId: AbCdEf\tPartitionCount: {topic['partitions']}\t"
                     f"ReplicationFactor: {topic['rf']}\tConfigs: {configs}"]
            replicas = ",".join(str(i + 1) for i in range(topic["rf"]))
            lines += [f"\tTopic: {args[2]}\tPartition: {p}\tLeader: 1\tReplicas: {replicas}\tIsr: {replicas}"
                      "\tElr: \tLastKnownElr: " for p in range(topic["partitions"])]
            return CommandResult(0, "\n".join(lines) + "\n", "")
        if tool == "kafka-topics.sh" and args[:3] == ["--create", "--if-not-exists", "--topic"]:
            name = args[3]
            if name not in self.topics:
                configs = dict(args[i + 1].split("=", 1) for i, t in enumerate(args) if t == "--config")
                self.add_topic(name, int(args[args.index("--partitions") + 1]),
                               int(args[args.index("--replication-factor") + 1]), configs)
            return CommandResult(0, f"Created topic {name}.\n", "")
        if tool == "kafka-acls.sh" and args == ["--list"]:
            out = []
            groups: dict[tuple, list] = {}
            for row in self.acls:
                groups.setdefault((row["resource_type"], row["resource_name"], row["pattern_type"]), []).append(row)
            for (rtype, name, pattern), rows in groups.items():
                out.append(f"Current ACLs for resource `ResourcePattern(resourceType={rtype}, name={name}, "
                           f"patternType={pattern})`:")
                out += [f"\t(principal={r['principal']}, host={r['host']}, operation={r['operation']}, "
                        f"permissionType={r['permission']})" for r in rows]
                out.append("")
            return CommandResult(0, "\n".join(out) + "\n", "")
        if tool == "kafka-acls.sh" and args[:2] == ["--add", "--allow-principal"]:
            principal = args[2]
            ops = [packet._OPERATION_FROM_CLI[args[i + 1]] for i, t in enumerate(args) if t == "--operation"]
            if "--cluster" in args:
                rtype, name, pattern = "CLUSTER", "kafka-cluster", "LITERAL"
            else:
                flag = next(t for t in args if t in ("--topic", "--group", "--transactional-id"))
                rtype = {"--topic": "TOPIC", "--group": "GROUP", "--transactional-id": "TRANSACTIONAL_ID"}[flag]
                name = args[args.index(flag) + 1]
                pattern = args[args.index("--resource-pattern-type") + 1].upper()
            for op in ops:
                self.add_acl(rtype, name, pattern, principal, op)
            return CommandResult(0, "Adding ACLs ...\n", "")
        raise AssertionError(f"fake broker got an unexpected command: {command}")

    def mutations(self):
        return [c for c in self.calls if "--create" in c or "--add" in c]


def _plan(**overrides):
    kwargs = {"principal": PRINCIPAL, "target": ISO}
    kwargs.update(overrides)
    return build_plan(**kwargs)


def _provisioned(plan) -> FakeBroker:
    broker = FakeBroker()
    result = apply(plan, broker, seal(plan)[1])
    assert result["status"] == "PASS", result
    broker.calls.clear()
    return broker


class PlanTests(unittest.TestCase):
    def test_acl_plan_is_exact(self):
        plan = _plan()
        tuples = {(r["resource_type"], r["resource_name"], r["pattern_type"], r["operation"]) for r in plan["acls"]}
        self.assertEqual(tuples, EXACT_ACLS)
        self.assertEqual(len(plan["acls"]), 13)
        for row in plan["acls"]:
            self.assertEqual((row["principal"], row["host"], row["permission"]), (PRINCIPAL, "*", "ALLOW"))
            self.assertNotIn(row["operation"], ("ALL", "ALTER", "DELETE", "CREATE", "ALTER_CONFIGS"))

    def test_topics_come_from_the_budget(self):
        budget = json.loads(packet.BUDGET.read_text())["retention"]["state_topics"]
        plan = build_plan(principal=PRINCIPAL, target=production_target())
        self.assertIsNone(plan["replication_override"])
        by_name = {t["name"]: t for t in plan["topics"]}
        self.assertEqual(sorted(by_name), ["md.bars.v2", "md.latest.v2"])
        for name, topic in by_name.items():
            spec = budget[name]
            self.assertEqual(topic["partitions"], spec["partitions"])
            self.assertEqual(topic["replication_factor"], spec["replication_factor"])
            expected = {k: str(spec[k]) for k in ("cleanup.policy", "min.insync.replicas",
                                                  "delete.retention.ms", "min.compaction.lag.ms",
                                                  "segment.ms", "segment.bytes") if k in spec}
            expected.update({"unclean.leader.election.enable": "false", "compression.type": "producer"})
            self.assertEqual(topic["configs"], expected)
            self.assertEqual({k: topic["configs"][k] for k in SEGMENT_CONFIGS}, SEGMENT_CONFIGS, name)
            self.assertLessEqual(set(topic["configs"]), packet.ALLOWED_TOPIC_CONFIGS)
        self.assertEqual(by_name["md.bars.v2"]["configs"]["min.compaction.lag.ms"], "3600000")
        self.assertNotIn("min.compaction.lag.ms", by_name["md.latest.v2"]["configs"])
        self.assertEqual(by_name["md.latest.v2"]["replication_factor"], 3)
        self.assertEqual(by_name["md.latest.v2"]["configs"]["min.insync.replicas"], "2")

    def test_commands_are_explicit_and_exact(self):
        commands = plan_commands(_plan())
        self.assertEqual(len(commands), 9)
        self.assertEqual(commands[0][:5], ["kafka-topics.sh", "--create", "--if-not-exists", "--topic", "md.bars.v2"])
        self.assertIn(["kafka-acls.sh", "--add", "--allow-principal", PRINCIPAL, "--operation", "Describe",
                       "--operation", "Read", "--topic", "md.canonical.v2",
                       "--resource-pattern-type", "literal"], commands)
        self.assertIn(["kafka-acls.sh", "--add", "--allow-principal", PRINCIPAL, "--operation", "Describe",
                       "--operation", "Write", "--transactional-id", "kn-projector-v3-",
                       "--resource-pattern-type", "prefixed"], commands)
        self.assertIn(["kafka-acls.sh", "--add", "--allow-principal", PRINCIPAL, "--operation",
                       "IdempotentWrite", "--cluster"], commands)
        for create in commands[:2]:
            configs = [create[i + 1] for i, token in enumerate(create) if token == "--config"]
            self.assertIn("segment.ms=3600000", configs, create[4])
            self.assertIn("segment.bytes=134217728", configs, create[4])
            self.assertEqual(len(configs), 8 if create[4] == "md.bars.v2" else 7, configs)
        flat = [token for command in commands for token in command]
        for forbidden in ("--alter", "--delete", "--remove", "kafka-configs.sh", "All", "*"):
            self.assertNotIn(forbidden, flat)
        validate_commands(_plan(), commands)

    def test_token_changes_when_any_plan_field_changes(self):
        base = _plan()
        _, token = seal(base)
        variants = [
            _plan(principal="User:kn-projector-2"),
            _plan(stage_a_group="kn-projector-v3-x"),
            _plan(stage_b_group="kn-projector-v3-y"),
            _plan(transactional_prefix="kn-projector-v4-"),
            _plan(target={**ISO, "bootstrap": "localhost:9093"}),
            _plan(replication_override=(1, 1)),
            build_plan(principal=PRINCIPAL, target=production_target()),
        ]
        edits = [("partitions", 12), ("replication_factor", 2)]
        for field, value in edits:
            changed = json.loads(json.dumps(base))
            changed["topics"][0][field] = value
            variants.append(changed)
        changed = json.loads(json.dumps(base))
        changed["topics"][1]["configs"]["delete.retention.ms"] = "1"
        variants.append(changed)
        changed = json.loads(json.dumps(base))
        changed["topics"][0]["configs"]["segment.ms"] = "604800000"
        variants.append(changed)
        changed = json.loads(json.dumps(base))
        changed["budget"]["sha256"] = "0" * 64
        variants.append(changed)
        tokens = {seal(v)[1] for v in variants}
        self.assertEqual(len(tokens), len(variants))
        self.assertNotIn(token, tokens)
        self.assertEqual(seal(_plan())[1], token, "the token is deterministic")
        self.assertRegex(token, r"^APPLY_QDL_KN3_STATE_TOPICS_[0-9a-f]{16}$")


class RefusalTests(unittest.TestCase):
    def test_apply_without_or_with_wrong_token_is_refused_before_any_call(self):
        plan = _plan()
        for confirmation in (None, "", "APPLY_QDL_KN3_STATE_TOPICS_0000000000000000",
                             seal(_plan(principal="User:other"))[1]):
            broker = FakeBroker()
            result = apply(plan, broker, confirmation)
            self.assertEqual(result["status"], "REFUSED")
            self.assertEqual(broker.calls, [])
            self.assertEqual(result["mutations"], 0)

    def test_plan_touching_canonical_or_other_topics_is_refused(self):
        plan = _plan()
        for bad in ("md.canonical.v2", "md.raw.realtime.v2"):
            tampered = json.loads(json.dumps(plan))
            tampered["topics"].append({**tampered["topics"][0], "name": bad})
            with self.assertRaises(Refused):
                validate_plan(tampered)
            tampered = json.loads(json.dumps(plan))
            tampered["topics"][0]["name"] = bad
            with self.assertRaises(Refused):
                validate_plan(tampered)

    def test_topic_configs_outside_the_allowlist_or_missing_segments_are_refused(self):
        plan = _plan()
        for mutate in (
            lambda configs: configs.update({"retention.ms": "1000"}),
            lambda configs: configs.update({"segment.jitter.ms": "0"}),
            lambda configs: configs.pop("segment.ms"),
            lambda configs: configs.pop("segment.bytes"),
        ):
            tampered = json.loads(json.dumps(plan))
            mutate(tampered["topics"][1]["configs"])
            with self.assertRaises(Refused):
                validate_plan(tampered)
        command = plan_commands(plan)[0] + ["--config", "retention.ms=1000"]
        with self.assertRaises(Refused):
            validate_commands(plan, [command, *plan_commands(plan)[1:]])

    def test_budget_without_segment_settings_is_refused(self):
        budget = json.loads(packet.BUDGET.read_text())
        for topic in ("md.latest.v2", "md.bars.v2"):
            for key in SEGMENT_CONFIGS:
                edited = json.loads(json.dumps(budget))
                del edited["retention"]["state_topics"][topic][key]
                with tempfile.TemporaryDirectory(prefix="kn3-pkt-") as directory:
                    path = Path(directory) / "budget.json"
                    path.write_text(json.dumps(edited), encoding="utf-8")
                    with self.assertRaises(Refused, msg=(topic, key)):
                        _plan(budget_path=path)

    def test_forbidden_acls_are_refused(self):
        plan = _plan()
        extra_rows = [
            {"operation": "ALL"}, {"operation": "ALTER"}, {"operation": "DELETE"}, {"operation": "CREATE"},
            {"resource_name": "*"}, {"permission": "DENY"}, {"principal": "User:*"},
            {"resource_name": "md.canonical.v2", "operation": "WRITE"},
            {"pattern_type": "PREFIXED"},
        ]
        for change in extra_rows:
            tampered = json.loads(json.dumps(plan))
            row = {**tampered["acls"][-1], "resource_type": "TOPIC", "resource_name": "md.latest.v2",
                   "pattern_type": "LITERAL", "operation": "WRITE"}
            row.update(change)
            tampered["acls"].append(row)
            with self.assertRaises(Refused, msg=str(change)):
                validate_plan(tampered)
        tampered = json.loads(json.dumps(plan))
        tampered["acls"].pop()
        with self.assertRaises(Refused):
            validate_plan(tampered)

    def test_forbidden_commands_are_refused(self):
        plan = _plan()
        good = plan_commands(plan)
        bad_commands = [
            ["kafka-configs.sh", "--alter", "--entity-type", "topics", "--entity-name", "md.canonical.v2",
             "--add-config", "retention.ms=1"],
            ["kafka-topics.sh", "--alter", "--topic", "md.canonical.v2", "--partitions", "12"],
            ["kafka-topics.sh", "--delete", "--topic", "md.latest.v2"],
            ["kafka-topics.sh", "--create", "--if-not-exists", "--topic", "md.canonical.v2"],
            ["kafka-acls.sh", "--remove", "--allow-principal", PRINCIPAL, "--operation", "Read", "--topic", "x"],
            ["kafka-acls.sh", "--add", "--allow-principal", PRINCIPAL, "--operation", "All", "--topic", "x"],
            ["kafka-acls.sh", "--add", "--allow-principal", PRINCIPAL, "--operation", "Read", "--topic", "*"],
            ["kafka-acls.sh", "--add", "--allow-principal", PRINCIPAL, "--operation", "Write",
             "--topic", "md.canonical.v2"],
            ["kafka-acls.sh", "--add", "--allow-principal", "User:other", "--operation", "Read", "--topic", "x"],
            ["kafka-consumer-groups.sh", "--reset-offsets", "--group", "kn-projector-v3-a"],
        ]
        for command in bad_commands:
            with self.assertRaises(Refused, msg=command):
                validate_commands(plan, [*good, command])
        with self.assertRaises(Refused):
            validate_commands(plan, good[:-1])

    def test_invalid_parameters_and_production_override_are_refused(self):
        for principal in ("kn-projector", "User:*", "User:ANONYMOUS", "User:a,b", ""):
            with self.assertRaises(Refused, msg=principal):
                _plan(principal=principal)
        with self.assertRaises(Refused):
            _plan(stage_b_group="kn-projector-v3-a")
        for prefix in ("kn-projector-v3", "*", "kn-"):
            with self.assertRaises(Refused, msg=prefix):
                _plan(transactional_prefix=prefix)
        with self.assertRaises(Refused):
            build_plan(principal=PRINCIPAL, target=production_target(), replication_override=(1, 1))
        for override in ((4, 2), (1, 2), (0, 0)):
            with self.assertRaises(Refused):
                _plan(replication_override=override)
        with self.assertRaises(Refused):
            isolated_target("localhost:9092", "qdl_v2_stable_candidate-kafka1-1")

    def test_cli_refuses_override_in_production_and_missing_principal(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = packet.main(["--projector-principal", PRINCIPAL, "--replication-override", "1,1"])
        self.assertEqual(code, 4)
        self.assertIn("isolated", json.loads(out.getvalue())["error"])
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                packet.main(["review"])
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(packet.main(["apply", "--projector-principal", PRINCIPAL]), 4)  # no --cert-dir

    def test_session_never_runs_unsealed_mutations_or_reads(self):
        broker = FakeBroker()
        session = packet._Session(broker, plan_commands(_plan()))
        with self.assertRaises(Refused):
            session.mutate(["kafka-topics.sh", "--create", "--if-not-exists", "--topic", "md.canonical.v2"])
        with self.assertRaises(Refused):
            session.read(["kafka-topics.sh", "--describe", "--topic", "md.canonical.v2"])
        self.assertEqual(broker.calls, [])


class ReviewTests(unittest.TestCase):
    def test_review_is_offline_and_exits_distinctly(self):
        plan = _plan()
        receipt = review(plan)
        self.assertEqual(receipt["status"], "REVIEW_REQUIRED")
        self.assertEqual(receipt["confirmation_token"], seal(plan)[1])
        self.assertEqual(receipt["mutations"], 0)
        with tempfile.TemporaryDirectory(prefix="kn3-pkt-") as directory:
            out_path = Path(directory) / "review.json"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = packet.main(["--projector-principal", PRINCIPAL, "--out", str(out_path)])
            self.assertEqual(code, 10)
            written = json.loads(out_path.read_text())
            self.assertEqual(written, json.loads(stdout.getvalue()))
            self.assertEqual(written["plan"]["target"]["backend"], "admin-container")
            self.assertIsNone(written["runner"])
            self.assertIn("receipt_sha256", written)


class ApplyVerifyTests(unittest.TestCase):
    def test_fresh_apply_then_idempotent_reapply(self):
        plan = _plan()
        broker = FakeBroker()
        first = apply(plan, broker, seal(plan)[1])
        self.assertEqual(first["status"], "PASS", first.get("error"))
        self.assertEqual(first["mutations"], 9)
        self.assertEqual(len(broker.mutations()), 9)
        self.assertEqual({a["action"] for a in first["actions"]}, {"created", "added"})
        broker.calls.clear()
        second = apply(plan, broker, seal(plan)[1])
        self.assertEqual(second["status"], "PASS")
        self.assertEqual(second["mutations"], 0)
        self.assertEqual(broker.mutations(), [])
        self.assertEqual({a["action"] for a in second["actions"]}, {"exists_exact", "already_present"})
        checked = verify(plan, broker)
        self.assertEqual(checked["status"], "PASS")
        self.assertEqual(checked["verify"]["acls"]["present"], 13)

    def test_partial_state_only_adds_what_is_missing(self):
        plan = _plan()
        broker = _provisioned(plan)
        broker.acls = [r for r in broker.acls if r["resource_type"] != "GROUP"]
        result = apply(plan, broker, seal(plan)[1])
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["mutations"], 2)
        self.assertTrue(all("--group" in c for c in broker.mutations()))

    def test_verify_before_apply_reports_everything_missing(self):
        result = verify(_plan(), FakeBroker())
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(len(result["verify"]["acls"]["missing"]), 13)
        self.assertFalse(result["verify"]["topics"]["md.latest.v2"]["exists"])
        self.assertEqual(result["mutations"], 0)

    def test_verify_detects_each_wrong_topic_field(self):
        plan = _plan()
        cases = [
            ("md.latest.v2", "partitions", 3, "partitions"),
            ("md.latest.v2", "rf", 2, "replication_factor"),
            ("md.bars.v2", "config", ("min.insync.replicas", "1"), "config:min.insync.replicas"),
            ("md.bars.v2", "config", ("cleanup.policy", "delete"), "config:cleanup.policy"),
            ("md.latest.v2", "config", ("cleanup.policy", "compact,delete"), "config:cleanup.policy"),
            ("md.latest.v2", "config", ("delete.retention.ms", "86400000"), "config:delete.retention.ms"),
            ("md.bars.v2", "config", ("min.compaction.lag.ms", "0"), "config:min.compaction.lag.ms"),
            ("md.bars.v2", "drop", "min.compaction.lag.ms", "config:min.compaction.lag.ms"),
            ("md.latest.v2", "config", ("min.compaction.lag.ms", "3600000"), "config:min.compaction.lag.ms"),
            ("md.latest.v2", "config", ("retention.ms", "1000"), "config:retention.ms"),
            ("md.latest.v2", "config", ("unclean.leader.election.enable", "true"),
             "config:unclean.leader.election.enable"),
            ("md.bars.v2", "drop", "compression.type", "config:compression.type"),
            ("md.latest.v2", "config", ("segment.ms", "604800000"), "config:segment.ms"),
            ("md.bars.v2", "config", ("segment.bytes", "1073741824"), "config:segment.bytes"),
            ("md.bars.v2", "drop", "segment.ms", "config:segment.ms"),
            ("md.latest.v2", "drop", "segment.bytes", "config:segment.bytes"),
            ("md.latest.v2", "config", ("segment.jitter.ms", "0"), "config:segment.jitter.ms"),
        ]
        for topic, kind, value, field in cases:
            broker = _provisioned(plan)
            entry = broker.topics[topic]
            if kind == "config":
                entry["configs"][value[0]] = value[1]
            elif kind == "drop":
                entry["configs"].pop(value)
            else:
                entry[kind] = value
            result = verify(plan, broker)
            self.assertEqual(result["status"], "FAIL", field)
            fields = [m["field"] for m in result["verify"]["topics"][topic]["mismatches"]]
            self.assertIn(field, fields)
            self.assertEqual(broker.mutations(), [])

    def test_verify_detects_missing_and_extra_acls(self):
        plan = _plan()
        broker = _provisioned(plan)
        broker.acls = [r for r in broker.acls if not (r["resource_type"] == "CLUSTER")]
        result = verify(plan, broker)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual([(m["resource_type"], m["operation"]) for m in result["verify"]["acls"]["missing"]],
                         [("CLUSTER", "IDEMPOTENT_WRITE")])
        extras = [
            ("TOPIC", "md.latest.v2", "LITERAL", PRINCIPAL, "ALTER", "ALLOW"),
            ("TOPIC", "md.canonical.v2", "LITERAL", PRINCIPAL, "WRITE", "ALLOW"),
            ("TOPIC", "md.bars.v2", "LITERAL", "User:*", "READ", "ALLOW"),
            ("TOPIC", "md.", "PREFIXED", PRINCIPAL, "READ", "ALLOW"),
            ("TOPIC", "*", "LITERAL", PRINCIPAL, "DESCRIBE", "ALLOW"),
            ("GROUP", "kn-projector-v3-a", "LITERAL", PRINCIPAL, "DELETE", "ALLOW"),
            ("TRANSACTIONAL_ID", "kn-projector-v3-x", "LITERAL", PRINCIPAL, "WRITE", "ALLOW"),
            ("TRANSACTIONAL_ID", "kn-", "PREFIXED", PRINCIPAL, "WRITE", "ALLOW"),
            ("CLUSTER", "kafka-cluster", "LITERAL", PRINCIPAL, "ALTER", "ALLOW"),
            ("TOPIC", "md.latest.v2", "LITERAL", PRINCIPAL, "READ", "DENY"),
        ]
        for extra in extras:
            broker = _provisioned(plan)
            broker.add_acl(*extra)
            result = verify(plan, broker)
            self.assertEqual(result["status"], "FAIL", extra)
            found = result["verify"]["acls"]["extra"]
            self.assertEqual(len(found), 1, extra)
            self.assertEqual((found[0]["resource_name"], found[0]["operation"], found[0]["permission"]),
                             (extra[1], extra[4], extra[5]))

    def test_other_principals_and_unrelated_resources_do_not_fail_verify(self):
        plan = _plan()
        broker = _provisioned(plan)
        broker.add_acl("TOPIC", "md.latest.v2", "LITERAL", "User:phase8-consumer", "READ")
        broker.add_acl("TOPIC", "md.projector.public.v2", "LITERAL", PRINCIPAL, "READ")
        result = verify(plan, broker)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["verify"]["acls"]["principal_acls_outside_plan_resources_count"], 1)

    def test_existing_topic_with_different_config_is_a_hard_stop(self):
        plan = _plan()
        broker = FakeBroker()
        bars = next(t for t in plan["topics"] if t["name"] == "md.bars.v2")
        broker.add_topic("md.bars.v2", 6, bars["replication_factor"], {**bars["configs"], "cleanup.policy": "delete"})
        result = apply(plan, broker, seal(plan)[1])
        self.assertEqual(result["status"], "HARD_STOP")
        self.assertEqual(result["mutations"], 0)
        self.assertEqual(broker.mutations(), [])
        self.assertIn("md.bars.v2", result["hard_stop"]["topics"])
        self.assertEqual(broker.topics["md.bars.v2"]["configs"]["cleanup.policy"], "delete", "never altered")

    def test_topics_created_with_the_old_config_are_drift(self):
        """Topics created before the segment settings (the pre-R1 budget: no
        segment override) fail verify and stop apply; nothing is altered."""
        plan = _plan()
        broker = FakeBroker()
        for topic in plan["topics"]:
            old = {k: v for k, v in topic["configs"].items() if k not in SEGMENT_CONFIGS}
            broker.add_topic(topic["name"], topic["partitions"], topic["replication_factor"], old)
        checked = verify(plan, broker)
        self.assertEqual(checked["status"], "FAIL")
        for name in ("md.latest.v2", "md.bars.v2"):
            mismatches = {m["field"]: m for m in checked["verify"]["topics"][name]["mismatches"]}
            self.assertEqual(set(mismatches), {"config:segment.ms", "config:segment.bytes"}, name)
            self.assertEqual((mismatches["config:segment.ms"]["expected"], mismatches["config:segment.ms"]["actual"]),
                             ("3600000", None))
            self.assertEqual(mismatches["config:segment.bytes"]["expected"], "134217728")
        result = apply(plan, broker, seal(plan)[1])
        self.assertEqual((result["status"], result["mutations"]), ("HARD_STOP", 0))
        self.assertEqual(set(result["hard_stop"]["topics"]), {"md.latest.v2", "md.bars.v2"})
        self.assertEqual(broker.mutations(), [])
        for name in ("md.latest.v2", "md.bars.v2"):
            self.assertFalse(set(SEGMENT_CONFIGS) & set(broker.topics[name]["configs"]), "never altered")

    def test_extra_acl_is_a_hard_stop_before_any_mutation(self):
        plan = _plan()
        broker = FakeBroker()
        broker.add_acl("TOPIC", "md.latest.v2", "LITERAL", PRINCIPAL, "ALL")
        result = apply(plan, broker, seal(plan)[1])
        self.assertEqual(result["status"], "HARD_STOP")
        self.assertEqual(broker.mutations(), [])

    def test_failed_command_is_an_error_with_bounded_evidence(self):
        plan = _plan()
        broker = FakeBroker()
        original = broker.run

        def failing(command):
            if "--create" in command:
                return CommandResult(1, "", "x" * 5000)
            return original(command)

        broker.run = failing
        result = apply(plan, broker, seal(plan)[1])
        self.assertEqual(result["status"], "ERROR")
        self.assertEqual(result["mutations"], 1)
        self.assertLessEqual(max(len(e["stderr_tail"]) for e in result["executed"]), 400)
        self.assertNotIn("stdout", result["executed"][0])


class RunnerTests(unittest.TestCase):
    def test_production_runner_matches_stable_admin_and_mounts_certs_read_only(self):
        runner = packet.AdminContainerRunner(Path("/nonexistent-cert-dir"))
        argv = runner.argv(["kafka-topics.sh", "--list"])
        self.assertEqual(runner.image, packet.admin_image())
        self.assertIn("@sha256:", runner.image)
        self.assertIn("/nonexistent-cert-dir:/etc/kafka/secrets:ro", argv)
        self.assertEqual(argv[argv.index("--command-config") + 1], "/etc/kafka/secrets/admin.properties")
        self.assertEqual(argv[argv.index("--network") + 1], "qdl_v2_stable_candidate_stable_internal")
        self.assertIn("--read-only", argv)
        self.assertEqual(argv[-1], "--list")
        with self.assertRaises(Refused):
            runner.preflight()

    def test_isolated_runner_uses_docker_exec_without_credentials(self):
        runner = packet.DockerExecRunner("kn3-pkt-kafka", "localhost:9092")
        argv = runner.argv(["kafka-acls.sh", "--list"])
        self.assertEqual(argv, ["docker", "exec", "kn3-pkt-kafka", "/opt/kafka/bin/kafka-acls.sh",
                                "--bootstrap-server", "localhost:9092", "--list"])

    def test_parsers_handle_real_kafka_42_output(self):
        described = ("Topic: probe.x\tTopicId: Pq9swTjXScm2kZCOFRWQ9g\tPartitionCount: 2\tReplicationFactor: 1\t"
                     "Configs: min.insync.replicas=1,cleanup.policy=compact,delete,delete.retention.ms=604800000\n"
                     "\tTopic: probe.x\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n"
                     "\tTopic: probe.x\tPartition: 1\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n")
        parsed = packet.parse_topic_describe(described)["probe.x"]
        self.assertEqual(parsed["configs"], {"min.insync.replicas": "1", "cleanup.policy": "compact,delete",
                                             "delete.retention.ms": "604800000"})
        self.assertEqual(parsed["replicas_per_partition"], {0: 1, 1: 1})
        listing = ("Current ACLs for resource `ResourcePattern(resourceType=CLUSTER, name=kafka-cluster, "
                   "patternType=LITERAL)`:\n\t(principal=User:probe, host=*, operation=IDEMPOTENT_WRITE, "
                   "permissionType=ALLOW)\n\n")
        self.assertEqual(packet.parse_acl_listing(listing), [{
            "resource_type": "CLUSTER", "resource_name": "kafka-cluster", "pattern_type": "LITERAL",
            "principal": "User:probe", "host": "*", "operation": "IDEMPOTENT_WRITE", "permission": "ALLOW"}])


@unittest.skipUnless(os.environ.get("QDL_KN_TEST_KAFKA_CONTAINER"),
                     "needs QDL_KN_TEST_KAFKA_CONTAINER naming a disposable isolated Kafka broker "
                     "(host Docker CLI); the lead runs it")
class IsolatedBrokerIntegrationTest(unittest.TestCase):
    """apply -> verify -> re-apply on a real disposable broker (RF 1 override)."""

    def test_apply_verify_reapply_and_detection_on_a_real_broker(self):
        container = os.environ["QDL_KN_TEST_KAFKA_CONTAINER"]
        bootstrap = os.environ.get("QDL_KN_TEST_KAFKA_BOOTSTRAP", "localhost:9092")
        runner = packet.DockerExecRunner(container, bootstrap)
        runner.preflight()
        plan = build_plan(principal=PRINCIPAL, target=isolated_target(bootstrap, container),
                          replication_override=(1, 1))
        token = seal(plan)[1]
        evidence: dict = {"token": token, "plan_sha256": seal(plan)[0]}

        refused = apply(plan, runner, "APPLY_QDL_KN3_STATE_TOPICS_0000000000000000")
        self.assertEqual((refused["status"], refused["executed"]), ("REFUSED", []))

        first = apply(plan, runner, token)
        self.assertEqual(first["status"], "PASS", first.get("error") or first.get("verify"))
        second = apply(plan, runner, token)
        self.assertEqual(second["status"], "PASS")
        self.assertEqual(second["mutations"], 0)
        self.assertEqual({a["action"] for a in second["actions"]}, {"exists_exact", "already_present"})
        checked = verify(plan, runner)
        self.assertEqual(checked["status"], "PASS")
        self.assertEqual(checked["verify"]["acls"]["present"], 13)
        evidence.update(first={"status": first["status"], "mutations": first["mutations"],
                               "actions": [a["action"] for a in first["actions"]]},
                        second={"status": second["status"], "mutations": second["mutations"]},
                        verify={"status": checked["status"],
                                "topics": {n: t["actual"] for n, t in checked["verify"]["topics"].items()},
                                "acls_present": checked["verify"]["acls"]["present"]})

        # An extra ACL added outside the packet is detected by verify and
        # stops apply before any mutation; the harness removes it after.
        extra = ["kafka-acls.sh", "--add", "--allow-principal", PRINCIPAL, "--operation", "Alter",
                 "--topic", "md.latest.v2"]
        self.assertEqual(runner.run(extra).returncode, 0)
        try:
            flagged = verify(plan, runner)
            self.assertEqual(flagged["status"], "FAIL")
            self.assertEqual([(e["resource_name"], e["operation"]) for e in flagged["verify"]["acls"]["extra"]],
                             [("md.latest.v2", "ALTER")])
            stopped = apply(plan, runner, token)
            self.assertEqual((stopped["status"], stopped["mutations"]), ("HARD_STOP", 0))
        finally:
            removed = runner.run(["kafka-acls.sh", "--remove", "--force", "--allow-principal", PRINCIPAL,
                                  "--operation", "Alter", "--topic", "md.latest.v2"])
            self.assertEqual(removed.returncode, 0)

        # A topic with the pre-R1 config (no segment override): the harness
        # deletes the two overrides with kafka-configs (the packet never can),
        # verify FAILs on exactly those, apply stops; then the harness restores.
        strip = ["kafka-configs.sh", "--alter", "--entity-type", "topics", "--entity-name", "md.latest.v2",
                 "--delete-config", "segment.ms,segment.bytes"]
        self.assertEqual(runner.run(strip).returncode, 0)
        try:
            deadline = time.monotonic() + 30
            while True:
                drift = verify(plan, runner)
                fields = sorted(m["field"] for m in drift["verify"]["topics"]["md.latest.v2"]["mismatches"])
                if fields or time.monotonic() > deadline:
                    break
                time.sleep(0.5)
            self.assertEqual(drift["status"], "FAIL")
            self.assertEqual(fields, ["config:segment.bytes", "config:segment.ms"])
            self.assertEqual(drift["verify"]["topics"]["md.bars.v2"]["mismatches"], [])
            stopped = apply(plan, runner, token)
            self.assertEqual((stopped["status"], stopped["mutations"]), ("HARD_STOP", 0))
            self.assertEqual(list(stopped["hard_stop"]["topics"]), ["md.latest.v2"])
        finally:
            restore = ["kafka-configs.sh", "--alter", "--entity-type", "topics", "--entity-name", "md.latest.v2",
                       "--add-config", "segment.ms=3600000,segment.bytes=134217728"]
            self.assertEqual(runner.run(restore).returncode, 0)
        deadline = time.monotonic() + 30
        while verify(plan, runner)["status"] != "PASS" and time.monotonic() < deadline:
            time.sleep(0.5)
        self.assertEqual(verify(plan, runner)["status"], "PASS")
        evidence.update(old_config_drift={"fields": fields, "verify": drift["status"], "apply": stopped["status"]})

        # A divergent spec against the existing topic is a hard stop, never an alter.
        divergent = json.loads(json.dumps(plan))
        divergent["topics"][0]["partitions"] = 3
        diverged = apply(divergent, runner, seal(divergent)[1])
        self.assertEqual((diverged["status"], diverged["mutations"]), ("HARD_STOP", 0))
        self.assertEqual(verify(plan, runner)["status"], "PASS")
        evidence.update(extra_acl="FAIL+HARD_STOP", divergent_spec=diverged["status"])
        out = os.environ.get("QDL_KN_TEST_EVIDENCE_OUT")
        if out:
            Path(out).write_text(json.dumps({"summary": evidence, "first_receipt": first,
                                             "reapply_receipt": second, "verify_receipt": checked},
                                            indent=1, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
