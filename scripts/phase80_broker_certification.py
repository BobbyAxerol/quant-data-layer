#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import time
import urllib.request
from collections import Counter
from dataclasses import dataclass
from typing import Iterable

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.phase8-kafka.yml"
TOPOLOGY_FILE = ROOT / "config/phase8/broker-topology.yaml"
EVIDENCE_FILE = ROOT / "upgrade/evidence/phase8-broker-topology.json"
FAILOVER_FILE = ROOT / "upgrade/evidence/phase8-broker-failover.json"
SECURITY_FILE = ROOT / "upgrade/evidence/phase8-broker-security.json"
PROJECT = os.environ.get("QDL_PHASE8_PROJECT", "qdl_phase80_certification")
BOOTSTRAP = "kafka1:9092,kafka2:9092,kafka3:9092"
RUST_IMAGE = os.environ.get("QDL_PHASE8_RUST_IMAGE", "qdl-phase8-rust:certification")


@dataclass(frozen=True)
class CommandResult:
    stdout: str
    stderr: str
    returncode: int
    elapsed_seconds: float


def run(
    command: list[str],
    *,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
    check: bool = True,
    timeout: float = 90.0,
) -> CommandResult:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            input=input_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        stdout = error.stdout.decode() if isinstance(error.stdout, bytes) else error.stdout or ""
        stderr = error.stderr.decode() if isinstance(error.stderr, bytes) else error.stderr or ""
        result = CommandResult(
            stdout=stdout,
            stderr=stderr + f"\ncommand timed out after {timeout}s",
            returncode=124,
            elapsed_seconds=time.monotonic() - started,
        )
        if check:
            raise RuntimeError(
                f"command timed out: {' '.join(command)}\n"
                f"stdout={result.stdout[-1200:]}\nstderr={result.stderr[-1200:]}"
            ) from error
        return result
    result = CommandResult(
        stdout=completed.stdout,
        stderr=completed.stderr,
        returncode=completed.returncode,
        elapsed_seconds=time.monotonic() - started,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout={result.stdout[-1200:]}\nstderr={result.stderr[-1200:]}"
        )
    return result


def compose(env: dict[str, str], *arguments: str, **kwargs: object) -> CommandResult:
    return run(
        [
            "docker",
            "compose",
            "--project-name",
            PROJECT,
            "--file",
            str(COMPOSE_FILE),
            *arguments,
        ],
        env=env,
        **kwargs,
    )


def kafka(env: dict[str, str], script: str, *arguments: str, **kwargs: object) -> CommandResult:
    return compose(
        env,
        "exec",
        "-T",
        "kafka1",
        f"/opt/kafka/bin/{script}",
        *arguments,
        **kwargs,
    )


def wait_for_cluster(env: dict[str, str], *, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = "cluster not queried"
    while time.monotonic() < deadline:
        result = kafka(
            env,
            "kafka-broker-api-versions.sh",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/etc/kafka/secrets/admin.properties",
            check=False,
            timeout=15.0,
        )
        if result.returncode == 0:
            return
        last_error = (result.stderr or result.stdout)[-800:]
        time.sleep(2.0)
    raise RuntimeError(f"Kafka cluster did not become ready: {last_error}")


def wait_for_replicas(env: dict[str, str], *, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        result = kafka(
            env,
            "kafka-topics.sh",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/etc/kafka/secrets/admin.properties",
            "--describe",
            "--under-replicated-partitions",
            check=False,
            timeout=20.0,
        )
        last = result.stdout + result.stderr
        if result.returncode == 0 and "Topic:" not in result.stdout:
            return
        time.sleep(2.0)
    raise RuntimeError(f"partitions did not return to full ISR: {last[-1000:]}")


def create_topic(
    env: dict[str, str],
    name: str,
    *,
    partitions: int = 3,
    retention_ms: int = 86_400_000,
) -> None:
    kafka(
        env,
        "kafka-topics.sh",
        "--bootstrap-server",
        BOOTSTRAP,
        "--command-config",
        "/etc/kafka/secrets/admin.properties",
        "--create",
        "--if-not-exists",
        "--topic",
        name,
        "--partitions",
        str(partitions),
        "--replication-factor",
        "3",
        "--config",
        "min.insync.replicas=2",
        "--config",
        f"retention.ms={retention_ms}",
        "--config",
        "compression.type=producer",
        "--config",
        "max.message.bytes=1048576",
    )


def add_acls(env: dict[str, str]) -> None:
    common = (
        "--bootstrap-server",
        BOOTSTRAP,
        "--command-config",
        "/etc/kafka/secrets/admin.properties",
        "--add",
    )
    kafka(
        env,
        "kafka-acls.sh",
        *common,
        "--allow-principal",
        "User:phase8-producer",
        "--operation",
        "WRITE",
        "--operation",
        "DESCRIBE",
        "--topic",
        "qdl.phase8.",
        "--resource-pattern-type",
        "prefixed",
    )
    kafka(
        env,
        "kafka-acls.sh",
        *common,
        "--allow-principal",
        "User:phase8-producer",
        "--operation",
        "IdempotentWrite",
        "--cluster",
    )
    kafka(
        env,
        "kafka-acls.sh",
        *common,
        "--allow-principal",
        "User:phase8-consumer",
        "--operation",
        "READ",
        "--operation",
        "DESCRIBE",
        "--topic",
        "qdl.phase8.",
        "--resource-pattern-type",
        "prefixed",
    )
    kafka(
        env,
        "kafka-acls.sh",
        *common,
        "--allow-principal",
        "User:phase8-consumer",
        "--operation",
        "READ",
        "--group",
        "phase8-",
        "--resource-pattern-type",
        "prefixed",
    )


def produce(
    env: dict[str, str],
    topic: str,
    records: Iterable[str],
    *,
    properties: str = "producer.properties",
    check: bool = True,
    timeout: float = 40.0,
) -> CommandResult:
    payload = "".join(f"{record}\n" for record in records)
    return kafka(
        env,
        "kafka-console-producer.sh",
        "--bootstrap-server",
        BOOTSTRAP,
        "--producer.config",
        f"/etc/kafka/secrets/{properties}",
        "--topic",
        topic,
        input_text=payload,
        check=check,
        timeout=timeout,
    )


def consume(
    env: dict[str, str],
    topic: str,
    count: int,
    group: str,
    *,
    timeout: float = 40.0,
) -> list[str]:
    result = kafka(
        env,
        "kafka-console-consumer.sh",
        "--bootstrap-server",
        BOOTSTRAP,
        "--command-config",
        "/etc/kafka/secrets/consumer.properties",
        "--topic",
        topic,
        "--group",
        group,
        "--from-beginning",
        "--max-messages",
        str(count),
        "--timeout-ms",
        str(int(timeout * 1000) - 2000),
        timeout=timeout,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


def total_end_offset(env: dict[str, str], topic: str) -> int:
    result = kafka(
        env,
        "kafka-get-offsets.sh",
        "--bootstrap-server",
        BOOTSTRAP,
        "--command-config",
        "/etc/kafka/secrets/admin.properties",
        "--topic",
        topic,
        "--time",
        "-1",
    )
    offsets: list[int] = []
    for line in result.stdout.splitlines():
        fields = line.rsplit(":", 1)
        if len(fields) == 2 and fields[1].strip().isdigit():
            offsets.append(int(fields[1].strip()))
    if not offsets:
        raise RuntimeError(f"no end offsets returned for {topic}: {result.stdout!r}")
    return sum(offsets)


def v1_topology() -> dict[str, object]:
    result = run(
        [
            "docker",
            "inspect",
            "data_layer_service",
            "--format",
            "{{json .}}",
        ],
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return {"present": False}
    payload = json.loads(result.stdout)
    return {
        "present": True,
        "id": payload["Id"],
        "image": payload["Image"],
        "started_at": payload["State"]["StartedAt"],
        "restart_count": payload["RestartCount"],
        "mounts": sorted(
            (item["Source"], item["Destination"], item["RW"])
            for item in payload["Mounts"]
        ),
        "networks": sorted(payload["NetworkSettings"]["Networks"]),
    }


def v1_health() -> int:
    try:
        with urllib.request.urlopen("http://127.0.0.1:8100/v1/health", timeout=10) as response:
            return response.status
    except Exception:
        return 0


def project_records(env: dict[str, str], records: list[str]) -> str:
    projected: list[tuple[str, str]] = []
    mset_arguments: list[str] = []
    for record in records:
        record_hash = hashlib.sha256(record.encode()).hexdigest()
        key = f"qdl:phase8:shadow:trade:{record_hash}"
        projected.append((key, record_hash))
        mset_arguments.extend((key, record_hash))
    compose(
        env,
        "exec",
        "-T",
        "phase8_redis",
        "redis-cli",
        "MSET",
        *mset_arguments,
    )
    keys = compose(
        env,
        "exec",
        "-T",
        "phase8_redis",
        "redis-cli",
        "--scan",
        "--pattern",
        "qdl:phase8:shadow:trade:*",
    ).stdout.splitlines()
    if sorted(keys) != sorted(key for key, _ in projected):
        raise RuntimeError("Redis projection key set differs from replay input")
    return hashlib.sha256(
        "\n".join(f"{key}={value}" for key, value in sorted(projected)).encode()
    ).hexdigest()


def cleanup(env: dict[str, str]) -> dict[str, object]:
    compose(env, "down", "--volumes", "--remove-orphans", check=False, timeout=120.0)
    containers = run(
        [
            "docker",
            "ps",
            "-a",
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
            "--format",
            "{{.ID}}",
        ]
    ).stdout.splitlines()
    networks = run(
        [
            "docker",
            "network",
            "ls",
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
            "--format",
            "{{.ID}}",
        ]
    ).stdout.splitlines()
    volumes = run(
        [
            "docker",
            "volume",
            "ls",
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
            "--format",
            "{{.Name}}",
        ]
    ).stdout.splitlines()
    return {
        "containers_after": len(containers),
        "networks_after": len(networks),
        "volumes_after": len(volumes),
    }


def write_json(path: pathlib.Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def rust_transport_smoke(cert_dir: str) -> dict[str, object]:
    nonce = hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:16]
    result = run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            f"{PROJECT}_phase8_shadow",
            "--read-only",
            "--security-opt",
            "no-new-privileges:true",
            "--mount",
            f"type=bind,source={cert_dir},target=/certs,readonly",
            "--env",
            f"QDL_KAFKA_BOOTSTRAP_SERVERS={BOOTSTRAP}",
            "--env",
            "QDL_KAFKA_CERT_ROOT=/certs",
            "--env",
            "QDL_KAFKA_SMOKE_TOPIC=qdl.phase8.audit.v1",
            "--env",
            f"QDL_KAFKA_SMOKE_NONCE={nonce}",
            RUST_IMAGE,
        ],
        timeout=60.0,
    )
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    if payload.get("status") != "PASS" or not payload.get("checkpointed"):
        raise RuntimeError(f"Rust broker transport smoke failed: {payload}")
    payload["elapsed_seconds"] = round(result.elapsed_seconds, 6)
    return payload


def main() -> int:
    topology = yaml.safe_load(TOPOLOGY_FILE.read_text())
    v1_before = v1_topology()
    health_before = v1_health()
    evidence: dict[str, object] = {}
    failover: dict[str, object] = {}
    security: dict[str, object] = {}
    cleanup_result: dict[str, object] = {}

    with tempfile.TemporaryDirectory(prefix="qdl-phase80-certs-") as cert_dir:
        env = os.environ.copy()
        env["QDL_PHASE8_CERT_DIR"] = cert_dir
        run([str(ROOT / "scripts/phase80_generate_tls.sh"), cert_dir], env=env, timeout=120.0)
        try:
            run(
                [
                    "docker",
                    "build",
                    "--provenance=false",
                    "--file",
                    str(ROOT / "Dockerfile.phase8-rust"),
                    "--tag",
                    RUST_IMAGE,
                    str(ROOT),
                ],
                timeout=900.0,
            )
            compose(env, "up", "-d", timeout=180.0)
            wait_for_cluster(env)
            for item in topology["topics"]:
                create_topic(env, item["name"], partitions=item["partitions"])
            add_acls(env)
            wait_for_replicas(env)
            rust_transport = rust_transport_smoke(cert_dir)

            topic = "qdl.phase8.canonical.trade.v2"
            records = [
                json.dumps(
                    {"event_id": f"phase80-{index:04d}", "price": f"{60000 + index}.00"},
                    sort_keys=True,
                    separators=(",", ":"),
                )
                for index in range(64)
            ]
            produced = produce(env, topic, records)
            consumed = consume(env, topic, len(records), "phase8-initial")
            if Counter(consumed) != Counter(records):
                missing = list((Counter(records) - Counter(consumed)).elements())[:3]
                extra = list((Counter(consumed) - Counter(records)).elements())[:3]
                raise RuntimeError(
                    "initial durable replay did not preserve exact records: "
                    f"expected={len(records)} actual={len(consumed)} "
                    f"missing={missing!r} extra={extra!r}"
                )

            first_projection = project_records(env, consumed)
            compose(env, "exec", "-T", "phase8_redis", "redis-cli", "FLUSHDB")
            replayed = consume(env, topic, len(records), "phase8-projector-rebuild")
            rebuilt_projection = project_records(env, replayed)
            if first_projection != rebuilt_projection:
                raise RuntimeError("Redis shadow projection rebuild diverged")

            unauthorized_offset_before = total_end_offset(env, topic)
            unauthorized = produce(
                env,
                topic,
                ["unauthorized-write-must-fail"],
                properties="unauthorized.properties",
                check=False,
                timeout=25.0,
            )
            unauthorized_offset_after = total_end_offset(env, topic)
            unauthorized_failed_closed = unauthorized_offset_after == unauthorized_offset_before
            if not unauthorized_failed_closed:
                raise RuntimeError("unauthorized producer changed the durable end offset")

            compose(env, "stop", "kafka3")
            one_node_offset_before = total_end_offset(env, topic)
            one_node_loss = produce(env, topic, ["one-node-loss-acked"])
            one_node_offset_after = total_end_offset(env, topic)
            if one_node_offset_after != one_node_offset_before + 1:
                raise RuntimeError("one-replica loss write was not durably acknowledged")
            compose(env, "stop", "kafka2")
            min_isr_offset_before = total_end_offset(env, topic)
            min_isr_failure = produce(
                env,
                topic,
                ["min-isr-write-must-fail"],
                check=False,
                timeout=25.0,
            )
            min_isr_offset_after = total_end_offset(env, topic)
            min_isr_failed_closed = min_isr_offset_after == min_isr_offset_before
            if not min_isr_failed_closed:
                raise RuntimeError("producer changed durable offset below min ISR")

            compose(env, "start", "kafka2", "kafka3", timeout=120.0)
            wait_for_cluster(env)
            wait_for_replicas(env)

            compose(env, "restart", "kafka1", "kafka2", "kafka3", timeout=120.0)
            wait_for_cluster(env)
            wait_for_replicas(env)
            recovered = consume(env, topic, len(records) + 1, "phase8-after-restart")
            expected_recovered = records + ["one-node-loss-acked"]
            if Counter(recovered) != Counter(expected_recovered):
                raise RuntimeError("acknowledged records did not survive broker restart")

            compose(env, "stop", "kafka3")
            compose(env, "rm", "-f", "kafka3")
            volume_name = f"{PROJECT}_kafka3_data"
            run(["docker", "volume", "rm", volume_name])
            compose(env, "up", "-d", "kafka3", timeout=120.0)
            wait_for_cluster(env)
            wait_for_replicas(env, timeout=180.0)
            restored = consume(env, topic, len(expected_recovered), "phase8-after-replica-restore")
            if Counter(restored) != Counter(expected_recovered):
                raise RuntimeError("replica restore changed acknowledged records")

            topic_describe = kafka(
                env,
                "kafka-topics.sh",
                "--bootstrap-server",
                BOOTSTRAP,
                "--command-config",
                "/etc/kafka/secrets/admin.properties",
                "--describe",
            ).stdout
            stats = run(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{json .}}",
                    *[
                        f"{PROJECT}-{name}-1"
                        for name in ("kafka1", "kafka2", "kafka3", "phase8_redis")
                    ],
                ],
                check=False,
            ).stdout.splitlines()

            evidence = {
                "schema": "qdl.phase8.broker-topology-evidence.v1",
                "status": "PASS",
                "authority": "RUST_SHADOW",
                "v1_authoritative": True,
                "topology_sha256": hashlib.sha256(TOPOLOGY_FILE.read_bytes()).hexdigest(),
                "topic_count": len(topology["topics"]),
                "records_acked": len(expected_recovered),
                "records_replayed": len(restored),
                "exact_record_parity": Counter(restored) == Counter(expected_recovered),
                "redis_projection_rebuild_equal": first_projection == rebuilt_projection,
                "produce_elapsed_seconds": round(produced.elapsed_seconds, 6),
                "topic_describe_sha256": hashlib.sha256(topic_describe.encode()).hexdigest(),
                "resource_snapshots": [json.loads(line) for line in stats if line.strip()],
                "rust_transport": rust_transport,
                "rust_transport_image_id": run(
                    ["docker", "image", "inspect", RUST_IMAGE, "--format", "{{.Id}}"]
                ).stdout.strip(),
            }
            security = {
                "schema": "qdl.phase8.broker-security-evidence.v1",
                "status": "PASS",
                "transport": "mutual_tls",
                "authorization": "kafka_standard_authorizer",
                "authorized_producer_status": produced.returncode,
                "unauthorized_producer_status": unauthorized.returncode,
                "unauthorized_failed_closed": unauthorized_failed_closed,
                "durable_offset_before": unauthorized_offset_before,
                "durable_offset_after": unauthorized_offset_after,
                "public_ports": 0,
            }
            failover = {
                "schema": "qdl.phase8.broker-failover-evidence.v1",
                "status": "PASS",
                "one_replica_loss_write_status": one_node_loss.returncode,
                "below_min_isr_write_status": min_isr_failure.returncode,
                "below_min_isr_failed_closed": min_isr_failed_closed,
                "one_replica_loss_offset_before": one_node_offset_before,
                "one_replica_loss_offset_after": one_node_offset_after,
                "below_min_isr_offset_before": min_isr_offset_before,
                "below_min_isr_offset_after": min_isr_offset_after,
                "acknowledged_records_survived_full_restart": Counter(recovered)
                == Counter(expected_recovered),
                "acknowledged_records_survived_replica_volume_loss": Counter(restored)
                == Counter(expected_recovered),
                "under_replicated_partitions_after_restore": 0,
            }
        finally:
            cleanup_result = cleanup(env)

    v1_after = v1_topology()
    health_after = v1_health()
    cleanup_result.update(
        {
            "v1_topology_unchanged": v1_before == v1_after,
            "v1_health_before": health_before,
            "v1_health_after": health_after,
        }
    )
    if not all(
        (
            cleanup_result["containers_after"] == 0,
            cleanup_result["networks_after"] == 0,
            cleanup_result["volumes_after"] == 0,
            cleanup_result["v1_topology_unchanged"],
            health_before == 200,
            health_after == 200,
        )
    ):
        raise RuntimeError(f"Phase 8.0 cleanup/V1 invariant failed: {cleanup_result}")
    evidence["cleanup"] = cleanup_result
    failover["cleanup"] = cleanup_result
    security["cleanup"] = cleanup_result
    write_json(EVIDENCE_FILE, evidence)
    write_json(FAILOVER_FILE, failover)
    write_json(SECURITY_FILE, security)
    print(
        json.dumps(
            {
                "status": "PASS",
                "topics": evidence["topic_count"],
                "records": evidence["records_replayed"],
                "cleanup": cleanup_result,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
