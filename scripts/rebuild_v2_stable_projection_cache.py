#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import ssl
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Callable, Sequence


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.v2-stable.yml"
PROJECT_NAME = "qdl_v2_stable_candidate"
CONFIRM_TOKEN = "REBUILD_QDL_V2_STABLE_PROJECTION_CACHE"
CANONICAL_TOPIC = "md.canonical.v2"
PROJECTOR_GROUP = "stable-projector-v1"
KAFKA_BOOTSTRAP = "kafka1:9092,kafka2:9092,kafka3:9092"
KAFKA_ADMIN_CONFIG = "/etc/kafka/secrets/admin.properties"
EXPECTED_CANONICAL_PARTITIONS = 6
MAX_ACCEPTED_LAG = 250
REPLAY_TAIL_RECORDS_PER_PARTITION = 10_000
REQUIRED_BOUNDED_LAG_SAMPLES = 3
STOP_SERVICES = (
    "projector_v2",
    "query_v2_1",
    "query_v2_2",
    "stream_v2_active",
    "stream_v2_passive",
)
STREAM_SERVICES = ("stream_v2_active", "stream_v2_passive")
QUERY_SERVICES = ("query_v2_1", "query_v2_2")
CACHE_FILES = (
    "/var/lib/qdl-stable/shared/canonical-cache.sqlite3",
    "/var/lib/qdl-stable/shared/canonical-cache.sqlite3-wal",
    "/var/lib/qdl-stable/shared/canonical-cache.sqlite3-shm",
)
Run = Callable[..., subprocess.CompletedProcess[str]]


def compose_command(env_file: Path, *arguments: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--env-file",
        str(env_file),
        "-f",
        str(COMPOSE_FILE),
        "--profile",
        "stable-admin",
        *arguments,
    ]


def rebuild_plan(env_file: Path) -> dict[str, object]:
    return {
        "schema": "qdl.v2.stable-projection-cache-rebuild.v1",
        "project": PROJECT_NAME,
        "env_file": str(env_file),
        "authority": "Kafka canonical topic",
        "stop_services": list(STOP_SERVICES),
        "delete_files": list(CACHE_FILES),
        "flush_service": "stable_redis",
        "reset_group": PROJECTOR_GROUP,
        "reset_topic": CANONICAL_TOPIC,
        "replay_tail_records_per_partition": REPLAY_TAIL_RECORDS_PER_PARTITION,
        "lag_gate": {
            "expected_partitions": EXPECTED_CANONICAL_PARTITIONS,
            "max_total_records": MAX_ACCEPTED_LAG,
            "consecutive_samples": REQUIRED_BOUNDED_LAG_SAMPLES,
        },
        "start_order": [
            list(STREAM_SERVICES),
            ["projector_v2"],
            list(QUERY_SERVICES),
        ],
        "touches_v1": False,
        "apply": False,
    }


def require_authorization(*, apply: bool, confirm: str | None) -> None:
    if not apply:
        return
    if confirm != CONFIRM_TOKEN:
        raise ValueError(f"--confirm must equal {CONFIRM_TOKEN}")


def parse_canonical_lag(output: str) -> tuple[int, int]:
    lags: list[int] = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 6 or CANONICAL_TOPIC not in fields:
            continue
        topic_index = fields.index(CANONICAL_TOPIC)
        if topic_index + 4 >= len(fields):
            continue
        partition = fields[topic_index + 1]
        lag = fields[topic_index + 4]
        if partition.isdigit() and lag.lstrip("-").isdigit():
            lags.append(max(0, int(lag)))
    if not lags:
        raise RuntimeError("canonical projector lag output has no partitions")
    return sum(lags), len(lags)


def _run(
    command: Sequence[str],
    *,
    timeout: float,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        cwd=ROOT,
        check=check,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _compose(
    env_file: Path,
    *arguments: str,
    timeout: float = 120,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return _run(
        compose_command(env_file, *arguments),
        timeout=timeout,
        check=check,
    )


def _start_services(env_file: Path, *services: str) -> None:
    if not services:
        raise ValueError("at least one stable service is required")
    _compose(env_file, "up", "-d", "--no-deps", *services)


def _kafka_group(env_file: Path, *arguments: str) -> str:
    result = _compose(
        env_file,
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "stable_admin",
        "--bootstrap-server",
        KAFKA_BOOTSTRAP,
        "--command-config",
        KAFKA_ADMIN_CONFIG,
        *arguments,
        timeout=120,
    )
    return result.stdout


def _reset_projector_to_bounded_tail(env_file: Path) -> None:
    common = (
        "--group",
        PROJECTOR_GROUP,
        "--topic",
        CANONICAL_TOPIC,
        "--reset-offsets",
    )
    _kafka_group(env_file, *common, "--to-latest", "--execute")
    _kafka_group(
        env_file,
        *common,
        "--shift-by",
        f"-{REPLAY_TAIL_RECORDS_PER_PARTITION}",
        "--execute",
    )


def _validate_project(env_file: Path) -> None:
    if not env_file.is_file():
        raise FileNotFoundError(f"stable env file does not exist: {env_file}")
    result = _compose(env_file, "config", "--format", "json")
    document = json.loads(result.stdout)
    if document.get("name") != PROJECT_NAME:
        raise RuntimeError("compose project is not the isolated stable candidate")


def _env_value(env_file: Path, key: str) -> str:
    prefix = f"{key}="
    matches = [
        line[len(prefix):]
        for line in env_file.read_text().splitlines()
        if line.startswith(prefix)
    ]
    if len(matches) != 1 or not matches[0]:
        raise ValueError(f"stable env must define exactly one {key}")
    return matches[0]


def _stable_client_ssl_context(env_file: Path) -> ssl.SSLContext:
    identity_root = Path(
        _env_value(env_file, "QDL_STABLE_TRADING_SYSTEM_CERT_DIR")
    )
    context = ssl.create_default_context(cafile=str(identity_root / "ca.crt"))
    context.load_cert_chain(
        certfile=str(identity_root / "client.crt"),
        keyfile=str(identity_root / "client.key"),
    )
    return context


def _wait_http(
    url: str,
    deadline: float,
    *,
    ssl_context: ssl.SSLContext | None = None,
) -> None:
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                url, timeout=2, context=ssl_context
            ) as response:
                if response.status == 200:
                    return
        except BaseException as error:
            last_error = error
        time.sleep(1)
    raise TimeoutError(f"health endpoint did not become ready: {url}") from last_error


def _wait_projector_ready(env_file: Path, deadline: float) -> None:
    probe = (
        "import urllib.request;"
        "urllib.request.urlopen('http://127.0.0.1:8230/health/ready',timeout=2)"
    )
    while time.monotonic() < deadline:
        result = _compose(
            env_file,
            "exec",
            "-T",
            "projector_v2",
            "python",
            "-c",
            probe,
            timeout=15,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(2)
    raise TimeoutError("stable projector did not become ready")


def lag_sample_acceptable(total_lag: int, partitions: int) -> bool:
    return (
        partitions == EXPECTED_CANONICAL_PARTITIONS
        and 0 <= total_lag <= MAX_ACCEPTED_LAG
    )


def _wait_bounded_lag(env_file: Path, deadline: float) -> dict[str, int]:
    last_lag: int | None = None
    partitions = 0
    consecutive = 0
    observed_bound = 0
    while time.monotonic() < deadline:
        output = _kafka_group(
            env_file,
            "--group",
            PROJECTOR_GROUP,
            "--describe",
        )
        last_lag, partitions = parse_canonical_lag(output)
        if lag_sample_acceptable(last_lag, partitions):
            consecutive += 1
            observed_bound = max(observed_bound, last_lag)
            if consecutive >= REQUIRED_BOUNDED_LAG_SAMPLES:
                return {
                    "lag": last_lag,
                    "partitions": partitions,
                    "observed_bound": observed_bound,
                    "configured_bound": MAX_ACCEPTED_LAG,
                    "consecutive_samples": consecutive,
                }
        else:
            consecutive = 0
            observed_bound = 0
        time.sleep(2)
    raise TimeoutError(
        "stable projector did not enter its bounded live-lag window; "
        f"last_lag={last_lag} partitions={partitions}"
    )


def execute_rebuild(env_file: Path, *, timeout_seconds: float) -> dict[str, object]:
    _validate_project(env_file)
    deadline = time.monotonic() + timeout_seconds
    _compose(env_file, "stop", *STOP_SERVICES)

    running = set(
        _compose(
            env_file, "ps", "--services", "--status", "running"
        ).stdout.split()
    )
    unexpected = running.intersection(STOP_SERVICES)
    if unexpected:
        raise RuntimeError(f"cache users still running: {sorted(unexpected)}")

    cleanup = "rm -f " + " ".join(CACHE_FILES)
    _compose(
        env_file,
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "/bin/sh",
        "stable_state_init",
        "-ec",
        cleanup,
    )
    _compose(
        env_file,
        "exec",
        "-T",
        "stable_redis",
        "redis-cli",
        "FLUSHDB",
    )
    dbsize = _compose(
        env_file,
        "exec",
        "-T",
        "stable_redis",
        "redis-cli",
        "DBSIZE",
    ).stdout.strip()
    if dbsize != "0":
        raise RuntimeError("isolated stable Redis did not reset to zero keys")

    _reset_projector_to_bounded_tail(env_file)
    ssl_context = _stable_client_ssl_context(env_file)
    _start_services(env_file, *STREAM_SERVICES)
    _wait_http(
        "https://localhost:18210/health/live",
        deadline,
        ssl_context=ssl_context,
    )
    _wait_http(
        "https://localhost:18211/health/live",
        deadline,
        ssl_context=ssl_context,
    )

    _start_services(env_file, "projector_v2")
    lag = _wait_bounded_lag(env_file, deadline)
    _wait_projector_ready(env_file, deadline)

    _start_services(env_file, *QUERY_SERVICES)
    _wait_http(
        "https://localhost:18201/health/ready",
        deadline,
        ssl_context=ssl_context,
    )
    _wait_http(
        "https://localhost:18202/health/ready",
        deadline,
        ssl_context=ssl_context,
    )

    final_size = int(
        _compose(
            env_file,
            "exec",
            "-T",
            "stable_redis",
            "redis-cli",
            "DBSIZE",
        ).stdout.strip()
    )
    if final_size <= 0:
        raise RuntimeError("stable Redis projection remained empty after replay")
    return {
        **rebuild_plan(env_file),
        "apply": True,
        "status": "PASS",
        "canonical_lag": lag,
        "redis_keys": final_size,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Atomically rebuild the isolated V2 stable projection cache."
    )
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--timeout-seconds", type=float, default=900)
    args = parser.parse_args()
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    require_authorization(apply=args.apply, confirm=args.confirm)
    if not args.apply:
        print(json.dumps(rebuild_plan(args.env_file), indent=2, sort_keys=True))
        return 0
    result = execute_rebuild(
        args.env_file.resolve(), timeout_seconds=args.timeout_seconds
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
