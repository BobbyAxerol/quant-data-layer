#!/usr/bin/env python3
"""Recover the V2 stable projection cache after a host boot or an equivalent crash.

`stable_redis` is ephemeral by design (tmpfs, no persistence), so after a boot
the projectors find an empty Redis next to a non-empty SQLite spool and stop
fail-closed with ``ProjectionCacheMismatch``. This tool automates exactly the
governed recovery that ``scripts/rebuild_v2_stable_projection_cache.py``
defines (its constants and gates are imported, not copied), but drives the
**existing** containers with ``docker`` instead of Compose, because the
containers were created from sealed overrides that a plain ``compose up``
cannot reproduce and would recreate with drifted config.

Decision, from observation only (``--check``):

* ``WAIT_INFRA``  Kafka quorum or stable_redis not healthy yet (retry later);
* ``HEALTHY``     Redis holds keys and every cache user is running: nothing to do;
* ``RECOVER``     Redis is empty while the spool is non-empty (post-boot state);
* ``START_ONLY``  Redis and spool both empty: cache users just need starting;
* ``MANUAL``      anything else (partial cache, unknown containers); never guessed.

``--apply --confirm REBUILD_QDL_V2_STABLE_PROJECTION_CACHE`` performs RECOVER:
stop cache users -> delete the three canonical cache files -> FLUSHDB
stable_redis -> reset the projector group to now-900 s -> start streams
(healthy) -> projectors -> bar edge -> bounded-lag gate -> projector ready
probe -> queries (healthy) -> Redis non-empty. Every step and gate value is
written to a receipt. ``--simulate-crash`` (also confirm-gated) first
reproduces the post-boot state on a running stack: stops the cache users and
restarts stable_redis so its tmpfs is wiped; the owner-approved rehearsal.

Never touches V1, Kafka topology, other volumes, Trading System or alpha.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_v2_stable_projection_cache import (  # noqa: E402
    BAR_EDGE_SERVICES,
    CACHE_FILES,
    CANONICAL_TOPIC,
    CONFIRM_TOKEN,
    EXPECTED_CANONICAL_PARTITIONS,
    KAFKA_ADMIN_CONFIG,
    KAFKA_BOOTSTRAP,
    MAX_REPLAY_BOOTSTRAP_RECORDS,
    PROJECT_NAME,
    PROJECTOR_GROUP,
    PROJECTOR_SERVICES,
    QUERY_SERVICES,
    REPLAY_LOOKBACK_SECONDS,
    REQUIRED_BOUNDED_LAG_SAMPLES,
    STOP_SERVICES,
    STREAM_SERVICES,
    lag_sample_acceptable,
    parse_canonical_lag,
)

INFRA_SERVICES = ("kafka1", "kafka2", "kafka3", "stable_redis")
REDIS_SERVICE = "stable_redis"
STATE_VOLUME = f"{PROJECT_NAME}_stable_state"
INTERNAL_NETWORK = f"{PROJECT_NAME}_stable_internal"
UTILITY_IMAGE = "alpine:3.20"
DEFAULT_RECEIPT_DIR = Path.home() / ".local/state/qdl-v2/boot-recovery"
Runner = Callable[[list[str]], subprocess.CompletedProcess]


def container(service: str) -> str:
    return f"{PROJECT_NAME}-{service}-1"


def default_runner(command: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(command, capture_output=True, text=True, timeout=300, check=False)


def _out(run: Runner, command: list[str]) -> str:
    result = run(command)
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(command[:4])}… failed rc={result.returncode}: {(result.stderr or '').strip()[:200]}")
    return result.stdout


# ------------------------------------------------------------------ observation
def inspect(run: Runner, service: str) -> dict[str, str]:
    fmt = "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}}|{{.RestartCount}}|{{.Config.Image}}"
    result = run(["docker", "inspect", "--format", fmt, container(service)])
    if result.returncode != 0:
        return {"status": "absent", "health": "-", "restarts": "0", "image": ""}
    status, health, restarts, image = result.stdout.strip().split("|", 3)
    return {"status": status, "health": health, "restarts": restarts, "image": image}


def redis_keys(run: Runner) -> int:
    return int(_out(run, ["docker", "exec", container(REDIS_SERVICE), "redis-cli", "DBSIZE"]).strip() or 0)


def spool_bytes(run: Runner) -> int:
    script = f"stat -c %s {CACHE_FILES[0]} 2>/dev/null || echo 0"
    return int(_out(run, ["docker", "run", "--rm", "--network", "none", "-v", f"{STATE_VOLUME}:/var/lib/qdl-stable:ro", UTILITY_IMAGE, "sh", "-c", script]).strip() or 0)


def kafka_admin(run: Runner) -> tuple[str, str]:
    """(image, host cert dir) taken from the running kafka1 container: no digest or path is hard-coded."""
    image = _out(run, ["docker", "inspect", "--format", "{{.Config.Image}}", container("kafka1")]).strip()
    cert = _out(run, ["docker", "inspect", "--format", '{{range .Mounts}}{{if eq .Destination "/etc/kafka/secrets"}}{{.Source}}{{end}}{{end}}', container("kafka1")]).strip()
    if not image or not cert:
        raise RuntimeError("kafka1 container does not expose its image or secrets mount")
    return image, cert


def kafka_group(run: Runner, image: str, cert: str, *arguments: str) -> str:
    return _out(run, [
        "docker", "run", "--rm", "--network", INTERNAL_NETWORK, "-v", f"{cert}:/etc/kafka/secrets:ro", image,
        "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", KAFKA_BOOTSTRAP,
        "--command-config", KAFKA_ADMIN_CONFIG, *arguments,
    ])


@dataclass
class Assessment:
    decision: str
    infra: dict[str, dict[str, str]]
    cache_users: dict[str, dict[str, str]]
    redis_keys: int | None
    spool_bytes: int | None
    reasons: list[str] = field(default_factory=list)


def assess(run: Runner) -> Assessment:
    infra = {s: inspect(run, s) for s in INFRA_SERVICES}
    users = {s: inspect(run, s) for s in STOP_SERVICES}
    not_ready = [s for s, st in infra.items() if st["health"] != "healthy"]
    if not_ready:
        return Assessment("WAIT_INFRA", infra, users, None, None, [f"not healthy: {', '.join(not_ready)}"])
    keys = redis_keys(run)
    spool = spool_bytes(run)
    running = [s for s, st in users.items() if st["status"] == "running"]
    absent = [s for s, st in users.items() if st["status"] == "absent"]
    if absent:
        return Assessment("MANUAL", infra, users, keys, spool, [f"containers absent: {', '.join(absent)}; recreate is outside this tool"])
    if keys > 0 and len(running) == len(STOP_SERVICES):
        return Assessment("HEALTHY", infra, users, keys, spool, ["redis populated, every cache user running"])
    if keys == 0 and spool > 0:
        return Assessment("RECOVER", infra, users, keys, spool, ["redis empty next to a non-empty spool: post-boot state"])
    if keys == 0 and spool == 0:
        return Assessment("START_ONLY", infra, users, keys, spool, ["redis and spool both empty: cache users only need starting"])
    return Assessment("MANUAL", infra, users, keys, spool, [f"redis keys={keys} spool={spool} running={running}: not a recognised state"])


# ------------------------------------------------------------------ mutation
class Recovery:
    def __init__(self, run: Runner, *, receipt: dict[str, Any], sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic, now: Callable[[], datetime] | None = None) -> None:
        self.run, self.receipt, self.sleep, self.clock = run, receipt, sleep, clock
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.receipt.setdefault("steps", [])

    def step(self, name: str, **detail: Any) -> None:
        self.receipt["steps"].append({"step": name, "at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **detail})

    def _wait(self, predicate: Callable[[], bool], what: str, deadline: float, every: float = 5.0) -> None:
        while self.clock() < deadline:
            if predicate():
                return
            self.sleep(every)
        raise TimeoutError(f"timed out waiting for {what}")

    def wait_healthy(self, services: tuple[str, ...], deadline: float) -> None:
        for s in services:
            self._wait(lambda s=s: inspect(self.run, s)["health"] == "healthy", f"{s} healthy", deadline)
            self.step("healthy", service=s)

    def simulate_crash(self) -> None:
        """Reproduce the post-boot state on a running stack: cache users down, Redis tmpfs wiped."""
        _out(self.run, ["docker", "stop", "-t", "30", *[container(s) for s in STOP_SERVICES]])
        self.step("simulate_crash_stopped_cache_users", services=list(STOP_SERVICES))
        _out(self.run, ["docker", "restart", container(REDIS_SERVICE)])
        self._wait(lambda: inspect(self.run, REDIS_SERVICE)["health"] == "healthy", "stable_redis healthy after restart", self.clock() + 120)
        self.step("simulate_crash_redis_restarted", redis_keys=redis_keys(self.run))

    def recover(self, *, timeout_seconds: float) -> dict[str, Any]:
        deadline = self.clock() + timeout_seconds
        running = [s for s in STOP_SERVICES if inspect(self.run, s)["status"] == "running"]
        if running:
            _out(self.run, ["docker", "stop", "-t", "30", *[container(s) for s in running]])
        self.step("cache_users_stopped", stopped=running)
        cleanup = "rm -f " + " ".join(CACHE_FILES)
        _out(self.run, ["docker", "run", "--rm", "--network", "none", "--user", "0:0", "-v", f"{STATE_VOLUME}:/var/lib/qdl-stable", UTILITY_IMAGE, "sh", "-ec", cleanup])
        self.step("cache_files_deleted", files=list(CACHE_FILES))
        _out(self.run, ["docker", "exec", container(REDIS_SERVICE), "redis-cli", "FLUSHDB"])
        if redis_keys(self.run) != 0:
            raise RuntimeError("stable_redis did not reset to zero keys")
        self.step("redis_flushed")
        image, cert = kafka_admin(self.run)
        start = self.now().astimezone(timezone.utc) - timedelta(seconds=REPLAY_LOOKBACK_SECONDS)
        start_text = start.strftime("%Y-%m-%dT%H:%M:%S.") + f"{start.microsecond // 1000:03d}"
        kafka_group(self.run, image, cert, "--group", PROJECTOR_GROUP, "--topic", CANONICAL_TOPIC, "--reset-offsets", "--to-datetime", start_text, "--execute")
        total, partitions, _ = parse_canonical_lag(kafka_group(self.run, image, cert, "--group", PROJECTOR_GROUP, "--describe"))
        if partitions != EXPECTED_CANONICAL_PARTITIONS:
            raise RuntimeError(f"replay covers {partitions} partitions, expected {EXPECTED_CANONICAL_PARTITIONS}")
        if total > MAX_REPLAY_BOOTSTRAP_RECORDS:
            raise RuntimeError(f"replay budget exceeded: {total} > {MAX_REPLAY_BOOTSTRAP_RECORDS}")
        self.step("group_reset", to_datetime=start_text, replay_records=total, partitions=partitions)
        _out(self.run, ["docker", "start", *[container(s) for s in STREAM_SERVICES]])
        self.wait_healthy(STREAM_SERVICES, deadline)
        _out(self.run, ["docker", "start", *[container(s) for s in PROJECTOR_SERVICES]])
        self.step("projectors_started")
        _out(self.run, ["docker", "start", *[container(s) for s in BAR_EDGE_SERVICES]])
        self.step("bar_edge_started")
        consecutive, last = 0, (None, 0, None)
        while self.clock() < deadline:
            last = parse_canonical_lag(kafka_group(self.run, image, cert, "--group", PROJECTOR_GROUP, "--describe"))
            if lag_sample_acceptable(*last):
                consecutive += 1
                if consecutive >= REQUIRED_BOUNDED_LAG_SAMPLES:
                    break
            else:
                consecutive = 0
            self.sleep(5)
        else:
            raise TimeoutError(f"projector lag never bounded: total={last[0]} max_partition={last[2]}")
        self.step("lag_bounded", total=last[0], partitions=last[1], max_partition=last[2], samples=consecutive)
        probe = "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8230/health/ready',timeout=2)"
        for s in PROJECTOR_SERVICES:
            self._wait(lambda s=s: self.run(["docker", "exec", container(s), "python", "-c", probe]).returncode == 0, f"{s} ready", deadline)
        self.step("projectors_ready")
        _out(self.run, ["docker", "start", *[container(s) for s in QUERY_SERVICES]])
        self.wait_healthy(QUERY_SERVICES, deadline)
        keys = redis_keys(self.run)
        if keys <= 0:
            raise RuntimeError("stable Redis projection remained empty after replay")
        self.step("done", redis_keys=keys)
        return {"status": "PASS", "redis_keys": keys, "lag": {"total": last[0], "partitions": last[1], "max_partition": last[2]}}

    def start_only(self, *, timeout_seconds: float) -> dict[str, Any]:
        deadline = self.clock() + timeout_seconds
        for group in (STREAM_SERVICES, PROJECTOR_SERVICES, BAR_EDGE_SERVICES, QUERY_SERVICES):
            _out(self.run, ["docker", "start", *[container(s) for s in group]])
            self.step("started", services=list(group))
        self.wait_healthy(STREAM_SERVICES + QUERY_SERVICES, deadline)
        return {"status": "PASS", "redis_keys": redis_keys(self.run)}


# ------------------------------------------------------------------ CLI
def write_receipt(receipt_dir: Path, receipt: dict[str, Any]) -> Path:
    receipt_dir.mkdir(parents=True, exist_ok=True)
    path = receipt_dir / f"boot-recovery-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w", encoding="utf-8") as f:
        json.dump(receipt, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    return path


def main(argv: list[str] | None = None, run: Runner = default_runner) -> int:
    parser = argparse.ArgumentParser(description="V2 stable projection cache recovery after boot (governed scope)")
    parser.add_argument("--apply", action="store_true", help="perform RECOVER/START_ONLY; default is assessment only")
    parser.add_argument("--confirm", default=None)
    parser.add_argument("--simulate-crash", action="store_true", help="first reproduce the post-boot state on a running stack (rehearsal)")
    parser.add_argument("--wait-infra-seconds", type=float, default=0, help="keep re-assessing while WAIT_INFRA for this long")
    parser.add_argument("--timeout-seconds", type=float, default=1200)
    parser.add_argument("--receipt-dir", type=Path, default=DEFAULT_RECEIPT_DIR)
    args = parser.parse_args(argv)
    if (args.apply or args.simulate_crash) and args.confirm != CONFIRM_TOKEN:
        parser.error(f"--confirm must equal {CONFIRM_TOKEN}")
    receipt: dict[str, Any] = {"schema": "qdl.v2.stable-boot-recovery.v1", "project": PROJECT_NAME, "apply": args.apply, "simulate_crash": args.simulate_crash}
    recovery = Recovery(run, receipt=receipt)
    if args.simulate_crash:
        before = assess(run)
        if before.decision != "HEALTHY":
            print(json.dumps({**receipt, "refused": f"simulate-crash needs a HEALTHY stack, found {before.decision}", "assessment": asdict(before)}, indent=2, default=str))
            return 2
        recovery.simulate_crash()
    deadline = time.monotonic() + args.wait_infra_seconds
    assessment = assess(run)
    while assessment.decision == "WAIT_INFRA" and time.monotonic() < deadline:
        time.sleep(10)
        assessment = assess(run)
    receipt["assessment"] = asdict(assessment)
    code = {"HEALTHY": 0, "WAIT_INFRA": 3, "RECOVER": 4, "START_ONLY": 4, "MANUAL": 5}[assessment.decision]
    if args.apply and assessment.decision in {"RECOVER", "START_ONLY"}:
        try:
            result = recovery.recover(timeout_seconds=args.timeout_seconds) if assessment.decision == "RECOVER" else recovery.start_only(timeout_seconds=args.timeout_seconds)
            receipt["result"] = result
            code = 0
        except Exception as exc:  # noqa: BLE001 - the receipt must record the failure and the tool must stop fail-closed
            receipt["result"] = {"status": "FAIL", "error": f"{type(exc).__name__}: {exc}"}
            code = 1
        receipt["post_assessment"] = asdict(assess(run))
    receipt["receipt_path"] = str(write_receipt(args.receipt_dir, receipt))
    print(json.dumps(receipt, indent=2, sort_keys=True, default=str))
    return code


if __name__ == "__main__":
    sys.exit(main())
