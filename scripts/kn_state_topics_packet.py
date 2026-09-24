#!/usr/bin/env python3
"""KN-3 K3.1: sealed provision packet for the two KN state topics and the
exact least-privilege ACLs of the KN-3 projector principal.

Purpose (plan journal "KN-3 design decisions D1-D5", D5; guide 18.4.2 and
18.10 K3.1): create ``md.latest.v2`` and ``md.bars.v2`` exactly as frozen in
``config/v2/kn-v220-candidate-budget.json`` ``retention.state_topics`` (the only
source of the topic specs) and grant the projector principal exactly::

    TOPIC md.canonical.v2          LITERAL  READ, DESCRIBE
    GROUP <stage-a group>          LITERAL  READ
    GROUP <stage-b group>          LITERAL  READ
    TOPIC md.latest.v2, md.bars.v2 LITERAL  WRITE, DESCRIBE, READ
    TRANSACTIONAL_ID <prefix>      PREFIXED WRITE, DESCRIBE
    CLUSTER kafka-cluster          LITERAL  IDEMPOTENT_WRITE

Modelled on ``scripts/phase103_apply_shared_primary_broker_scope.py``:

* ``review`` (default) is pure offline: prints the plan, the exact command
  allowlist and the sealed confirmation token; exit code 10 (REVIEW_REQUIRED).
* ``apply --confirm TOKEN`` refuses unless TOKEN equals the token sealed over
  the exact plan (topics, ACLs, target, override, budget hash). It first reads
  the broker: a state topic that exists with a different configuration, or an
  extra ACL on a planned resource, is a HARD_STOP before any mutation (nothing
  is ever altered automatically). It then runs only ``--create
  --if-not-exists`` and ``kafka-acls --add`` for what is missing and verifies.
  Re-running on a provisioned broker executes no mutation and verifies PASS.
* ``verify`` is read-only (``--list``/``--describe``): exact partitions, RF,
  per-partition replica count, every planned topic config and no unplanned
  topic override; the exact ACL set of the principal on the planned resources
  (missing and extra entries, including ``User:*`` and wildcard/prefixed
  patterns that cover a planned resource).

Boundary: never alters, deletes or re-partitions any topic, never touches
``md.canonical.v2`` beyond granting READ/DESCRIBE on it, never removes ACLs,
never resets offsets, never uses ``kafka-configs.sh``. Topics and ACLs are
named explicitly (no positional indices; the phaseb bootstrap list drifted
that way). Key material is never read: the production runner only mounts the
certificate directory read-only into the admin container and passes the
``admin.properties`` path.

Runners:

* production: ``--cert-dir DIR`` runs the Kafka CLI in a throwaway container
  equal to the compose ``stable_admin`` service (same image digest, network
  ``stable_internal``, read-only, cert dir mounted ro, ``--command-config``).
  Using it is an owner-approved production packet (D5); not exercised here.
* isolated: ``--bootstrap HOST:PORT --plaintext --exec-container NAME`` runs
  ``docker exec NAME /opt/kafka/bin/<tool>`` against a disposable test broker.
  Only this mode accepts ``--replication-override RF,MIN_ISR`` (single-node
  brokers cannot hold RF 3); the override is part of the sealed plan.

Exit codes: 0 PASS, 1 FAIL (verify mismatch), 3 HARD_STOP, 4 REFUSED,
5 ERROR (a command failed), 10 REVIEW_REQUIRED.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Iterable, Mapping, NamedTuple, Protocol, Sequence


ROOT = Path(__file__).resolve().parents[1]
BUDGET = ROOT / "config/v2/kn-v220-candidate-budget.json"
COMPOSE = ROOT / "docker-compose.v2-stable.yml"
SCHEMA = "qdl.kn.v220.state-topics-packet.v1"

PRODUCTION_PROJECT = "qdl_v2_stable_candidate"
PRODUCTION_NETWORK = f"{PRODUCTION_PROJECT}_stable_internal"
PRODUCTION_BOOTSTRAP = "kafka1:9092,kafka2:9092,kafka3:9092"
ADMIN_CONFIG = "/etc/kafka/secrets/admin.properties"

CANONICAL_TOPIC = "md.canonical.v2"
STATE_TOPICS = ("md.bars.v2", "md.latest.v2")
PROTECTED_TOPICS = frozenset({CANONICAL_TOPIC})
CLUSTER_RESOURCE = "kafka-cluster"

DEFAULT_STAGE_A_GROUP = "kn-projector-v3-a"
DEFAULT_STAGE_B_GROUP = "kn-projector-v3-b"
DEFAULT_TRANSACTIONAL_PREFIX = "kn-projector-v3-"

# Budget topic spec: Kafka topic configs vs structural fields. Any other key in
# the budget is refused so a budget edit is never silently ignored.
_BUDGET_CONFIG_KEYS = (
    "cleanup.policy", "min.insync.replicas", "delete.retention.ms", "min.compaction.lag.ms",
)
_BUDGET_STRUCTURAL_KEYS = ("key", "partitions", "replication_factor")
# Same fixed policy as every existing stable topic (phaseb bootstrap).
_FIXED_TOPIC_CONFIGS = {"unclean.leader.election.enable": "false", "compression.type": "producer"}

_CLI_OPERATION = {"READ": "Read", "WRITE": "Write", "DESCRIBE": "Describe",
                  "IDEMPOTENT_WRITE": "IdempotentWrite"}
_OPERATION_FROM_CLI = {value: key for key, value in _CLI_OPERATION.items()}
_RESOURCE_FLAG = {"TOPIC": "--topic", "GROUP": "--group", "TRANSACTIONAL_ID": "--transactional-id",
                  "CLUSTER": "--cluster"}
_TOOLS = frozenset({"kafka-topics.sh", "kafka-acls.sh"})
_FORBIDDEN_FLAGS = frozenset({
    "--alter", "--delete", "--remove", "--deny-principal", "--deny-host", "--reset-offsets",
    "--add-config", "--delete-config", "--force", "--producer", "--consumer",
})
FORBIDDEN_OPERATIONS = (
    "topic_alter", "topic_delete", "topic_partition_change", "topic_config_alter",
    "canonical_topic_mutation", "kafka_configs_tool", "acl_remove", "acl_deny",
    "acl_operation_all", "acl_alter_delete_create", "acl_wildcard_resource",
    "acl_wildcard_principal", "offset_reset", "service_start_stop",
)
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_PRINCIPAL = re.compile(r"^User:[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_BOOTSTRAP = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}:[0-9]{1,5}$")

EXIT = {"PASS": 0, "FAIL": 1, "HARD_STOP": 3, "REFUSED": 4, "ERROR": 5, "REVIEW_REQUIRED": 10}


class Refused(ValueError):
    """The plan, the command list or the invocation is outside the packet."""


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


# ------------------------------------------------------------------ plan

def load_state_topic_specs(budget_path: Path = BUDGET) -> tuple[dict[str, Any], str]:
    """Return the frozen state topic specs and the budget file SHA-256."""
    raw = budget_path.read_bytes()
    budget = json.loads(raw)
    specs = budget["retention"]["state_topics"]
    names = sorted(name for name, value in specs.items() if isinstance(value, Mapping))
    if tuple(names) != STATE_TOPICS:
        raise Refused(f"budget state topics must be exactly {list(STATE_TOPICS)}, got {names}")
    for name in names:
        unknown = set(specs[name]) - set(_BUDGET_CONFIG_KEYS) - set(_BUDGET_STRUCTURAL_KEYS)
        if unknown:
            raise Refused(f"budget topic {name} has keys this packet does not provision: {sorted(unknown)}")
    return {name: dict(specs[name]) for name in names}, hashlib.sha256(raw).hexdigest()


def production_target() -> dict[str, Any]:
    return {"backend": "admin-container", "bootstrap": PRODUCTION_BOOTSTRAP,
            "network": PRODUCTION_NETWORK, "image": admin_image(),
            "command_config": ADMIN_CONFIG}


def isolated_target(bootstrap: str, container: str | None) -> dict[str, Any]:
    if not _BOOTSTRAP.match(bootstrap):
        raise Refused("--bootstrap must be HOST:PORT")
    if container is not None and (not _NAME.match(container) or container.startswith(PRODUCTION_PROJECT)):
        raise Refused("--exec-container must be a disposable test broker, never a production container")
    return {"backend": "isolated-plaintext", "bootstrap": bootstrap, "exec_container": container}


def expected_acls(principal: str, stage_a_group: str, stage_b_group: str,
                  transactional_prefix: str) -> list[dict[str, str]]:
    """The exact ACL entries of the projector principal, sorted."""
    grants = [
        ("TOPIC", CANONICAL_TOPIC, "LITERAL", ("READ", "DESCRIBE")),
        ("GROUP", stage_a_group, "LITERAL", ("READ",)),
        ("GROUP", stage_b_group, "LITERAL", ("READ",)),
        *[("TOPIC", topic, "LITERAL", ("WRITE", "DESCRIBE", "READ")) for topic in STATE_TOPICS],
        ("TRANSACTIONAL_ID", transactional_prefix, "PREFIXED", ("WRITE", "DESCRIBE")),
        ("CLUSTER", CLUSTER_RESOURCE, "LITERAL", ("IDEMPOTENT_WRITE",)),
    ]
    rows = [
        {"principal": principal, "host": "*", "permission": "ALLOW", "operation": operation,
         "resource_type": rtype, "resource_name": name, "pattern_type": pattern}
        for rtype, name, pattern, operations in grants for operation in operations
    ]
    return sorted(rows, key=_acl_key)


def _acl_key(row: Mapping[str, str]) -> tuple[str, ...]:
    return (row["resource_type"], row["resource_name"], row["pattern_type"], row["principal"],
            row["host"], row["permission"], row["operation"])


def build_plan(*, principal: str, target: Mapping[str, Any],
               stage_a_group: str = DEFAULT_STAGE_A_GROUP,
               stage_b_group: str = DEFAULT_STAGE_B_GROUP,
               transactional_prefix: str = DEFAULT_TRANSACTIONAL_PREFIX,
               replication_override: tuple[int, int] | None = None,
               budget_path: Path = BUDGET) -> dict[str, Any]:
    if not _PRINCIPAL.match(principal or "") or principal == "User:ANONYMOUS":
        raise Refused("--projector-principal must be an explicit User:<name> (no wildcard, not ANONYMOUS)")
    for label, value in (("stage-a group", stage_a_group), ("stage-b group", stage_b_group),
                         ("transactional prefix", transactional_prefix)):
        if not _NAME.match(value or ""):
            raise Refused(f"{label} must match {_NAME.pattern}")
    if stage_a_group == stage_b_group:
        raise Refused("stage A and stage B groups must differ")
    if len(transactional_prefix) < 4 or not transactional_prefix.endswith("-"):
        raise Refused("transactional prefix must be at least 4 characters and end with '-'")
    specs, budget_sha = load_state_topic_specs(budget_path)
    override = None
    if replication_override is not None:
        if target.get("backend") != "isolated-plaintext":
            raise Refused("--replication-override is only allowed in isolated --plaintext mode")
        rf, isr = replication_override
        budget_rf = min(spec["replication_factor"] for spec in specs.values())
        if not 1 <= isr <= rf <= budget_rf:
            raise Refused("replication override needs 1 <= MIN_ISR <= RF <= budget RF")
        override = {"replication_factor": rf, "min.insync.replicas": isr,
                    "budget": {name: {"replication_factor": spec["replication_factor"],
                                      "min.insync.replicas": spec["min.insync.replicas"]}
                               for name, spec in specs.items()}}
    topics = []
    for name, spec in specs.items():
        configs = {key: str(spec[key]) for key in _BUDGET_CONFIG_KEYS if key in spec}
        configs.update(_FIXED_TOPIC_CONFIGS)
        rf = spec["replication_factor"]
        if override is not None:
            rf = override["replication_factor"]
            configs["min.insync.replicas"] = str(override["min.insync.replicas"])
        topics.append({"name": name, "partitions": int(spec["partitions"]), "replication_factor": int(rf),
                       "configs": dict(sorted(configs.items())), "record_key": spec.get("key")})
    plan = {
        "schema": SCHEMA,
        "budget": {"path": str(budget_path.relative_to(ROOT)) if budget_path.is_relative_to(ROOT)
                   else str(budget_path), "sha256": budget_sha},
        "target": dict(target),
        "replication_override": override,
        "principal": principal,
        "stage_a_group": stage_a_group,
        "stage_b_group": stage_b_group,
        "transactional_prefix": transactional_prefix,
        "topics": topics,
        "acls": expected_acls(principal, stage_a_group, stage_b_group, transactional_prefix),
        "protected_topics": sorted(PROTECTED_TOPICS),
        "forbidden_operations": list(FORBIDDEN_OPERATIONS),
    }
    validate_plan(plan)
    return plan


def validate_plan(plan: Mapping[str, Any]) -> None:
    """Refuse any plan that is not the exact least-privilege packet."""
    topics = plan.get("topics") or []
    names = [topic.get("name") for topic in topics]
    if sorted(names) != list(STATE_TOPICS) or len(set(names)) != len(names):
        touched = sorted(set(names) - set(STATE_TOPICS))
        raise Refused(f"plan may only provision {list(STATE_TOPICS)}; refused topics {touched}")
    for topic in topics:
        if topic["name"] in PROTECTED_TOPICS:
            raise Refused(f"plan touches protected topic {topic['name']}")
    if plan.get("replication_override") is not None and plan["target"].get("backend") != "isolated-plaintext":
        raise Refused("replication override outside isolated mode")
    principal = plan.get("principal")
    for row in plan.get("acls") or []:
        if row.get("principal") != principal or row.get("permission") != "ALLOW" or row.get("host") != "*":
            raise Refused("every ACL must ALLOW the plan principal from host *")
        if row.get("operation") not in _CLI_OPERATION:
            raise Refused(f"ACL operation {row.get('operation')} is not permitted")
        if "*" in row.get("resource_name", "*"):
            raise Refused("wildcard ACL resources are not permitted")
        if row["resource_type"] == "TOPIC" and row["resource_name"] in PROTECTED_TOPICS \
                and row["operation"] not in ("READ", "DESCRIBE"):
            raise Refused(f"only READ/DESCRIBE on {row['resource_name']}")
        if row.get("pattern_type") == "PREFIXED" and row["resource_type"] != "TRANSACTIONAL_ID":
            raise Refused("only the transactional id may be a prefixed ACL")
    want = expected_acls(principal, plan["stage_a_group"], plan["stage_b_group"],
                         plan["transactional_prefix"])
    if list(plan.get("acls") or []) != want:
        raise Refused("plan ACL set is not the exact projector set")


def plan_commands(plan: Mapping[str, Any]) -> list[list[str]]:
    """The immutable mutation allowlist derived from the plan (not executed)."""
    commands: list[list[str]] = []
    for topic in plan["topics"]:
        command = ["kafka-topics.sh", "--create", "--if-not-exists", "--topic", topic["name"],
                   "--partitions", str(topic["partitions"]),
                   "--replication-factor", str(topic["replication_factor"])]
        for key, value in topic["configs"].items():
            command.extend(("--config", f"{key}={value}"))
        commands.append(command)
    for resource, operations in _group_acls(plan["acls"]).items():
        commands.append(_acl_add_command(plan["principal"], resource, operations))
    return commands


def _group_acls(rows: Iterable[Mapping[str, str]]) -> dict[tuple[str, str, str], list[str]]:
    grouped: dict[tuple[str, str, str], list[str]] = {}
    for row in rows:
        grouped.setdefault((row["resource_type"], row["resource_name"], row["pattern_type"]), []).append(
            row["operation"])
    return grouped


def _acl_add_command(principal: str, resource: tuple[str, str, str], operations: Sequence[str]) -> list[str]:
    rtype, name, pattern = resource
    command = ["kafka-acls.sh", "--add", "--allow-principal", principal]
    for operation in operations:
        command.extend(("--operation", _CLI_OPERATION[operation]))
    command.append(_RESOURCE_FLAG[rtype])
    if rtype != "CLUSTER":
        command.append(name)
        command.extend(("--resource-pattern-type", pattern.lower()))
    return command


def validate_commands(plan: Mapping[str, Any], commands: Sequence[Sequence[str]]) -> None:
    """Defence in depth: every mutation is a create-if-absent of a state topic
    or an ALLOW add for the plan principal, and the list is the sealed one."""
    for command in commands:
        tool, args = command[0], list(command[1:])
        if tool not in _TOOLS:
            raise Refused(f"tool {tool} is not permitted")
        forbidden = _FORBIDDEN_FLAGS.intersection(args)
        if forbidden:
            raise Refused(f"forbidden operation {sorted(forbidden)} in {tool}")
        if tool == "kafka-topics.sh":
            if args[:3] != ["--create", "--if-not-exists", "--topic"]:
                raise Refused("topic commands may only be --create --if-not-exists")
            if args[3] in PROTECTED_TOPICS or args[3] not in STATE_TOPICS:
                raise Refused(f"topic command on {args[3]} is not permitted")
        else:
            if args[:3] != ["--add", "--allow-principal", plan["principal"]]:
                raise Refused("ACL commands may only --add ALLOW for the plan principal")
            operations = [args[i + 1] for i, token in enumerate(args) if token == "--operation"]
            if any(op not in _OPERATION_FROM_CLI for op in operations):
                raise Refused(f"ACL operations {operations} are not permitted")
            if "*" in args or (CANONICAL_TOPIC in args and set(operations) - {"Read", "Describe"}):
                raise Refused("wildcard resource or canonical write is not permitted")
    if [list(c) for c in commands] != plan_commands(plan):
        raise Refused("commands differ from the sealed plan allowlist")


def seal(plan: Mapping[str, Any]) -> tuple[str, str]:
    """Return (plan_sha256, confirmation token) over plan + exact commands."""
    digest = canonical_sha256({"plan": plan, "commands": plan_commands(plan)})
    return digest, f"APPLY_QDL_KN3_STATE_TOPICS_{digest[:16]}"


# ---------------------------------------------------------------- runners

class CommandResult(NamedTuple):
    returncode: int
    stdout: str
    stderr: str


class Runner(Protocol):
    def run(self, command: Sequence[str]) -> CommandResult: ...

    def describe(self) -> dict[str, Any]: ...

    def preflight(self) -> None: ...


def admin_image(compose: Path = COMPOSE) -> str:
    """The compose ``stable_admin`` image (pinned digest), read not retyped."""
    text = compose.read_text(encoding="utf-8")
    match = re.search(r"^  stable_admin:\n(?:    .*\n)*?    image:\s*(\S+)", text, flags=re.M)
    if not match or "@sha256:" not in match.group(1):
        raise Refused("stable_admin image digest not found in compose")
    return match.group(1)


def _subprocess(argv: Sequence[str], timeout: float) -> CommandResult:
    try:
        done = subprocess.run(list(argv), capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        return CommandResult(124, "", f"timeout after {timeout}s")
    return CommandResult(done.returncode, done.stdout, done.stderr)


class AdminContainerRunner:
    """Production: a throwaway container equal to compose ``stable_admin``.
    The cert directory is bind-mounted read-only and never read here."""

    def __init__(self, cert_dir: Path, *, image: str | None = None, timeout: float = 120.0) -> None:
        self.cert_dir = cert_dir
        self.image = image or admin_image()
        self.timeout = timeout

    def argv(self, command: Sequence[str]) -> list[str]:
        return ["docker", "run", "--rm", "--network", PRODUCTION_NETWORK, "--read-only",
                "--security-opt", "no-new-privileges:true",
                "-v", f"{self.cert_dir}:/etc/kafka/secrets:ro",
                "--entrypoint", f"/opt/kafka/bin/{command[0]}", self.image,
                "--bootstrap-server", PRODUCTION_BOOTSTRAP, "--command-config", ADMIN_CONFIG,
                *command[1:]]

    def run(self, command: Sequence[str]) -> CommandResult:
        return _subprocess(self.argv(command), self.timeout)

    def describe(self) -> dict[str, Any]:
        return {"backend": "admin-container", "image": self.image, "network": PRODUCTION_NETWORK,
                "bootstrap": PRODUCTION_BOOTSTRAP, "command_config": ADMIN_CONFIG,
                "cert_dir_mount": "ro"}

    def preflight(self) -> None:
        if not self.cert_dir.is_dir():
            raise Refused("--cert-dir is not a directory")


class DockerExecRunner:
    """Isolated: ``docker exec`` the Kafka CLI inside a disposable broker."""

    def __init__(self, container: str, bootstrap: str, *, timeout: float = 120.0) -> None:
        isolated_target(bootstrap, container)
        self.container = container
        self.bootstrap = bootstrap
        self.timeout = timeout

    def argv(self, command: Sequence[str]) -> list[str]:
        return ["docker", "exec", self.container, f"/opt/kafka/bin/{command[0]}",
                "--bootstrap-server", self.bootstrap, *command[1:]]

    def run(self, command: Sequence[str]) -> CommandResult:
        return _subprocess(self.argv(command), self.timeout)

    def describe(self) -> dict[str, Any]:
        return {"backend": "isolated-plaintext", "exec_container": self.container,
                "bootstrap": self.bootstrap}

    def preflight(self) -> None:
        result = _subprocess(["docker", "inspect", "--format",
                              '{{index .Config.Labels "com.docker.compose.project"}}', self.container], 30)
        if result.returncode != 0:
            raise Refused(f"isolated broker container {self.container} not found")
        if result.stdout.strip() == PRODUCTION_PROJECT:
            raise Refused("isolated mode refuses a production compose container")


# ---------------------------------------------------------------- parsing

_TOPIC_HEADER = re.compile(
    r"^Topic:\s*(\S+)\s+TopicId:\s*\S+\s+PartitionCount:\s*(\d+)\s+ReplicationFactor:\s*(\d+)\s+Configs:\s*(.*)$")
_TOPIC_PARTITION = re.compile(r"^\s*Topic:\s*(\S+)\s+Partition:\s*(\d+)\s+Leader:\s*\S+\s+Replicas:\s*(\S*)")
_ACL_RESOURCE = re.compile(r"ResourcePattern\(resourceType=(\w+), name=(.+?), patternType=(\w+)\)")
_ACL_ENTRY = re.compile(r"\(principal=(.+?), host=(.+?), operation=(\w+), permissionType=(\w+)\)")


def _parse_configs(text: str) -> dict[str, str]:
    configs: dict[str, str] = {}
    last = None
    for token in (text.strip().split(",") if text.strip() else []):
        if "=" in token:
            last, _, value = token.partition("=")
            configs[last] = value
        elif last is not None:  # a value that itself contains commas
            configs[last] += "," + token
    return configs


def parse_topic_describe(text: str) -> dict[str, dict[str, Any]]:
    topics: dict[str, dict[str, Any]] = {}
    for line in text.splitlines():
        header = _TOPIC_HEADER.match(line.strip())
        if header:
            topics[header.group(1)] = {"partitions": int(header.group(2)),
                                       "replication_factor": int(header.group(3)),
                                       "configs": _parse_configs(header.group(4)),
                                       "replicas_per_partition": {}}
            continue
        part = _TOPIC_PARTITION.match(line)
        if part and part.group(1) in topics:
            replicas = [item for item in part.group(3).split(",") if item]
            topics[part.group(1)]["replicas_per_partition"][int(part.group(2))] = len(replicas)
    return topics


def parse_acl_listing(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    resource = None
    for line in text.splitlines():
        found = _ACL_RESOURCE.search(line)
        if found:
            resource = {"resource_type": found.group(1), "resource_name": found.group(2),
                        "pattern_type": found.group(3)}
            continue
        entry = _ACL_ENTRY.search(line)
        if entry and resource:
            rows.append({**resource, "principal": entry.group(1), "host": entry.group(2),
                         "operation": entry.group(3), "permission": entry.group(4)})
    return rows


# ---------------------------------------------------------------- compare

def topic_mismatches(expected: Mapping[str, Any], actual: Mapping[str, Any]) -> list[dict[str, Any]]:
    out = []
    for field in ("partitions", "replication_factor"):
        if actual[field] != expected[field]:
            out.append({"field": field, "expected": expected[field], "actual": actual[field]})
    bad = sorted(p for p, n in actual["replicas_per_partition"].items() if n != expected["replication_factor"])
    if len(actual["replicas_per_partition"]) != expected["partitions"] or bad:
        out.append({"field": "replicas_per_partition", "expected": expected["replication_factor"],
                    "actual": dict(sorted(actual["replicas_per_partition"].items()))})
    for key in sorted(set(expected["configs"]) | set(actual["configs"])):
        want, got = expected["configs"].get(key), actual["configs"].get(key)
        if want != got:
            out.append({"field": f"config:{key}", "expected": want, "actual": got})
    return out


def _covers(binding: Mapping[str, str], resource: tuple[str, str, str]) -> bool:
    """Does an existing ACL pattern grant on (or inside) a planned resource?"""
    rtype, name, pattern = resource
    if binding["resource_type"] != rtype:
        return False
    bname, bpattern = binding["resource_name"], binding["pattern_type"]
    if bname == "*":
        return True
    if pattern == "LITERAL":
        return bname == name if bpattern == "LITERAL" else name.startswith(bname)
    # planned PREFIXED namespace: any pattern overlapping it counts
    return bname.startswith(name) or (bpattern == "PREFIXED" and name.startswith(bname))


def acl_diff(plan: Mapping[str, Any], listed: Sequence[Mapping[str, str]]) -> dict[str, Any]:
    principal = plan["principal"]
    expected = {_acl_key(row) for row in plan["acls"]}
    resources = list(_group_acls(plan["acls"]))
    relevant, outside = set(), []
    for row in listed:
        if row["principal"] not in (principal, "User:*"):
            continue
        if any(_covers(row, resource) for resource in resources):
            relevant.add(_acl_key(row))
        elif row["principal"] == principal:
            outside.append(dict(row))
    fields = ("resource_type", "resource_name", "pattern_type", "principal", "host", "permission", "operation")
    to_rows = lambda keys: [dict(zip(fields, key)) for key in sorted(keys)]  # noqa: E731
    return {"missing": to_rows(expected - relevant), "extra": to_rows(relevant - expected),
            "present": len(expected & relevant), "expected": len(expected),
            "principal_acls_outside_plan_resources": outside[:50],
            "principal_acls_outside_plan_resources_count": len(outside)}


# ---------------------------------------------------------------- execute

_READ_TOPICS = ["kafka-topics.sh", "--list"]
_READ_ACLS = ["kafka-acls.sh", "--list"]


def _record(command: Sequence[str], result: CommandResult, kind: str) -> dict[str, Any]:
    return {"kind": kind, "command": list(command), "returncode": result.returncode,
            "stdout_sha256": hashlib.sha256(result.stdout.encode()).hexdigest(),
            "stdout_bytes": len(result.stdout.encode()), "stderr_tail": result.stderr.strip()[-400:]}


class _Session:
    """Runs only the fixed read commands and the sealed mutation allowlist."""

    def __init__(self, runner: Runner, allowed_mutations: Sequence[Sequence[str]]) -> None:
        self.runner = runner
        self.allowed = [list(c) for c in allowed_mutations]
        self.log: list[dict[str, Any]] = []
        self.mutations = 0

    def read(self, command: Sequence[str]) -> CommandResult:
        command = list(command)
        is_describe = command[:3] == ["kafka-topics.sh", "--describe", "--topic"] and \
            len(command) == 4 and command[3] in STATE_TOPICS
        if command not in (_READ_TOPICS, _READ_ACLS) and not is_describe:
            raise Refused(f"read command not in allowlist: {command}")
        result = self.runner.run(command)
        self.log.append(_record(command, result, "read"))
        if result.returncode != 0:
            raise RuntimeError(f"read failed: {command[:3]} rc={result.returncode}")
        return result

    def mutate(self, command: Sequence[str]) -> CommandResult:
        if list(command) not in self.allowed:
            raise Refused(f"mutation not in sealed allowlist: {list(command)}")
        result = self.runner.run(command)
        self.mutations += 1
        self.log.append(_record(command, result, "mutation"))
        if result.returncode != 0:
            raise RuntimeError(f"mutation failed: {list(command)[:5]} rc={result.returncode}")
        return result


def _observe(session: _Session, plan: Mapping[str, Any]) -> dict[str, Any]:
    names = set(session.read(_READ_TOPICS).stdout.split())
    topics: dict[str, Any] = {}
    for topic in plan["topics"]:
        if topic["name"] not in names:
            topics[topic["name"]] = {"exists": False, "mismatches": [{"field": "exists", "expected": True,
                                                                      "actual": False}]}
            continue
        described = parse_topic_describe(
            session.read(["kafka-topics.sh", "--describe", "--topic", topic["name"]]).stdout)
        actual = described.get(topic["name"])
        if actual is None:
            raise RuntimeError(f"describe output for {topic['name']} not parseable")
        topics[topic["name"]] = {"exists": True, "actual": actual,
                                 "mismatches": topic_mismatches(topic, actual)}
    acls = acl_diff(plan, parse_acl_listing(session.read(_READ_ACLS).stdout))
    return {"topics": topics, "acls": acls}


def _passes(observed: Mapping[str, Any]) -> bool:
    return (all(not item["mismatches"] for item in observed["topics"].values())
            and not observed["acls"]["missing"] and not observed["acls"]["extra"])


def _receipt(plan: Mapping[str, Any], mode: str, runner: Runner | None) -> dict[str, Any]:
    digest, token = seal(plan)
    return {"schema": SCHEMA + ".receipt", "mode": mode, "generated_at_ns": time.time_ns(),
            "plan": plan, "plan_sha256": digest, "confirmation_token": token,
            "commands": plan_commands(plan), "runner": runner.describe() if runner else None,
            "mutations": 0}


def review(plan: Mapping[str, Any]) -> dict[str, Any]:
    validate_plan(plan)
    validate_commands(plan, plan_commands(plan))
    receipt = _receipt(plan, "review", None)
    receipt["status"] = "REVIEW_REQUIRED"
    return receipt


def verify(plan: Mapping[str, Any], runner: Runner) -> dict[str, Any]:
    validate_plan(plan)
    receipt = _receipt(plan, "verify", runner)
    session = _Session(runner, ())
    try:
        observed = _observe(session, plan)
        receipt.update(verify=observed, status="PASS" if _passes(observed) else "FAIL")
    except (RuntimeError, Refused) as error:
        receipt.update(status="ERROR", error=str(error))
    receipt["executed"] = session.log
    return receipt


def apply(plan: Mapping[str, Any], runner: Runner, confirmation: str | None) -> dict[str, Any]:
    validate_plan(plan)
    commands = plan_commands(plan)
    validate_commands(plan, commands)
    receipt = _receipt(plan, "apply", runner)
    if confirmation != receipt["confirmation_token"]:
        receipt.update(status="REFUSED", error="--confirm must equal the sealed plan confirmation token",
                       executed=[])
        return receipt
    session = _Session(runner, commands)
    actions: list[dict[str, Any]] = []
    try:
        before = _observe(session, plan)
        receipt["preflight"] = before
        different = {name: item["mismatches"] for name, item in before["topics"].items()
                     if item["exists"] and item["mismatches"]}
        if different or before["acls"]["extra"]:
            receipt.update(status="HARD_STOP", error="existing state differs from the plan; nothing altered",
                           hard_stop={"topics": different, "extra_acls": before["acls"]["extra"]},
                           executed=session.log, actions=[])
            return receipt
        missing = {_acl_key(row) for row in before["acls"]["missing"]}
        for topic, command in zip(plan["topics"], commands):
            if before["topics"][topic["name"]]["exists"]:
                actions.append({"topic": topic["name"], "action": "exists_exact"})
            else:
                session.mutate(command)
                actions.append({"topic": topic["name"], "action": "created"})
        for (resource, operations), command in zip(_group_acls(plan["acls"]).items(),
                                                    commands[len(plan["topics"]):]):
            keys = {_acl_key(row) for row in plan["acls"]
                    if (row["resource_type"], row["resource_name"], row["pattern_type"]) == resource}
            label = {"resource": list(resource), "operations": operations}
            if keys & missing:
                session.mutate(command)
                actions.append({**label, "action": "added"})
            else:
                actions.append({**label, "action": "already_present"})
        after = _observe(session, plan)
        receipt.update(verify=after, status="PASS" if _passes(after) else "FAIL")
    except (RuntimeError, Refused) as error:
        receipt.update(status="ERROR", error=str(error))
    receipt.update(actions=actions, executed=session.log, mutations=session.mutations)
    return receipt


# ---------------------------------------------------------------- cli

def _override(text: str | None) -> tuple[int, int] | None:
    if text is None:
        return None
    match = re.fullmatch(r"(\d+),(\d+)", text)
    if not match:
        raise Refused("--replication-override must be RF,MIN_ISR")
    return int(match.group(1)), int(match.group(2))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", nargs="?", default="review", choices=("review", "apply", "verify"))
    parser.add_argument("--projector-principal", required=True)
    parser.add_argument("--stage-a-group", default=DEFAULT_STAGE_A_GROUP)
    parser.add_argument("--stage-b-group", default=DEFAULT_STAGE_B_GROUP)
    parser.add_argument("--transactional-prefix", default=DEFAULT_TRANSACTIONAL_PREFIX)
    parser.add_argument("--confirm")
    parser.add_argument("--cert-dir", type=Path, help="production: host dir mounted ro as /etc/kafka/secrets")
    parser.add_argument("--bootstrap", help="isolated: HOST:PORT of a disposable broker")
    parser.add_argument("--plaintext", action="store_true", help="isolated plaintext test broker")
    parser.add_argument("--exec-container", help="isolated: broker container for docker exec")
    parser.add_argument("--replication-override", help="isolated only: RF,MIN_ISR")
    parser.add_argument("--budget", type=Path, default=BUDGET)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.plaintext != (args.bootstrap is not None):
            raise Refused("isolated mode needs both --bootstrap and --plaintext")
        if args.plaintext:
            target = isolated_target(args.bootstrap, args.exec_container)
            if args.cert_dir is not None:
                raise Refused("--cert-dir is production-only")
        else:
            if args.exec_container or args.replication_override:
                raise Refused("--exec-container/--replication-override need isolated --plaintext mode")
            target = production_target()
        plan = build_plan(principal=args.projector_principal, target=target,
                          stage_a_group=args.stage_a_group, stage_b_group=args.stage_b_group,
                          transactional_prefix=args.transactional_prefix,
                          replication_override=_override(args.replication_override),
                          budget_path=args.budget)
        runner: Runner | None = None
        if args.mode != "review":
            if args.plaintext:
                if not args.exec_container:
                    raise Refused("isolated apply/verify needs --exec-container")
                runner = DockerExecRunner(args.exec_container, args.bootstrap)
            else:
                if args.cert_dir is None:
                    raise Refused("production apply/verify needs --cert-dir")
                runner = AdminContainerRunner(args.cert_dir)
            runner.preflight()
        if args.mode == "review":
            receipt = review(plan)
        elif args.mode == "verify":
            receipt = verify(plan, runner)
        else:
            receipt = apply(plan, runner, args.confirm)
    except Refused as error:
        receipt = {"schema": SCHEMA + ".receipt", "mode": args.mode, "status": "REFUSED",
                   "error": str(error), "mutations": 0}
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    text = json.dumps(receipt, indent=1, sort_keys=True)
    if args.out is not None:
        args.out.write_text(text + "\n", encoding="utf-8")
    print(text)
    return EXIT[receipt["status"]]


if __name__ == "__main__":
    sys.exit(main())
