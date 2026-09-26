#!/usr/bin/env python3
"""Run a bounded, manifest-derived Phase-3 V2 consumer load receipt.

The host half has no SDK imports and launches one disposable client container.
The inner half uses the production SDK against only the existing V2 Query and
Stream endpoints.  It has no V1 URL, provider URL, Docker socket, order client,
or write-capable market-data mount.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
from collections import Counter, defaultdict
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
import hashlib
import json
import math
import os
from pathlib import Path
import re
import random
import resource
import statistics
import subprocess
import sys
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
_IDENTITY_STATE_ROOT = Path("/home/bobby/.local/state/qdl-v2")
_IDENTITY_RE = re.compile(r"[a-z][a-z0-9.-]{2,127}")
_NETWORK_RE = re.compile(r"[A-Za-z0-9_.-]+")
_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_ALLOWED_SESSION_COUNTS = frozenset({5, 20, 35, 50})
_FIVE_LIQUID = {
    "BINANCE": frozenset({"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT"}),
    "OKX": frozenset({"BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "BNB-USDT-SWAP"}),
}
_PHASE3_CONSUMER_MANIFESTS = {
    "trading-system.paper.stable": "/app/consumers/stable/trading-system-paper.yaml",
    "alpha.binance.paper.stable": "/app/consumers/stable/alpha-binance-paper.yaml",
    "alpha.okx.paper.stable": "/app/consumers/stable/alpha-okx-paper.yaml",
    "monitoring.multivenue.stable": "/app/consumers/stable/monitoring-multivenue.yaml",
}
_ISSUER = "https://identity.qdl.stable.internal"
_AUDIENCE = "qdl-v2-stable"
_ROLES = ("historical_reader", "market_data_reader", "stream_consumer")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


_DIAGNOSTIC_FIELDS = (
    "evaluated_at_ns", "state", "freshness_ms", "event_recency_state", "provider_session_state",
    "provider_session_liveness_ms", "execution_eligible", "gap_open", "complete", "reason_codes",
    "source_id", "watermark_offset", "observed_at_ns", "received_at_ns",
)


def _safe_error(error: BaseException) -> dict[str, object]:
    code = getattr(error, "code", None)
    detail = str(getattr(error, "detail", error))
    out: dict[str, object] = {
        "type": type(error).__name__,
        "code": str(code) if isinstance(code, str) and re.fullmatch(r"[A-Z0-9_]{1,80}", code) else None,
        "detail_sha256": _sha256(detail.encode()),
    }
    # The typed reason inside the server's fixed sentence (e.g. EVENT_AGE,
    # SESSION_STATE): vocabulary, never free text.
    reason = re.search(r"\(([A-Z_]{1,40})\)$", detail)
    if reason:
        out["reason"] = reason.group(1)
    # Query's quality at the refusal (KN-4 review): only the known,
    # non-secret fields, bounded.
    diagnostics = getattr(error, "diagnostics", None)
    if isinstance(diagnostics, dict):
        out["diagnostics"] = {
            key: (list(diagnostics[key])[:32] if key == "reason_codes" else diagnostics[key])
            for key in _DIAGNOSTIC_FIELDS if key in diagnostics
        }
    return out


def _stream_frame_quality_diagnostic(event, requirement, *, now_ns: int | None = None) -> dict[str, object]:
    """Return bounded non-secret quality context for a rejected stream frame.

    The load receipt needs enough information to distinguish a stale provider
    event from a stale transport/session without recording raw payloads,
    prices, timestamps, cursor material, or identity secrets.
    """

    from qdl.common.v1 import common_pb2

    envelope = getattr(event, "event", None)
    current_ns = time.time_ns() if now_ns is None else now_ns

    def integer(value: object) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def age_ms(value: object) -> int | None:
        timestamp_ns = integer(value)
        if timestamp_ns is None or timestamp_ns <= 0:
            return None
        return max(0, (current_ns - timestamp_ns) // 1_000_000)

    def policy_name(value: object) -> str | None:
        candidate = getattr(value, "value", None)
        return candidate if isinstance(candidate, str) else None

    flags: list[str] = []
    for value in getattr(envelope, "quality_flags", ()):
        try:
            flags.append(
                common_pb2.QualityFlag.Name(int(value)).removeprefix("QUALITY_FLAG_")
            )
        except (TypeError, ValueError):
            flags.append("UNKNOWN")

    return {
        "logical_offset": integer(getattr(event, "logical_offset", None)),
        "source_event_age_ms": age_ms(getattr(envelope, "source_event_time_ns", None)),
        "receive_age_ms": age_ms(getattr(envelope, "received_at_ns", None)),
        "max_freshness_ms": integer(getattr(requirement, "max_freshness_ms", None)),
        "event_recency_policy": policy_name(
            getattr(requirement, "effective_event_recency_policy", None)
        ),
        "max_session_liveness_ms": integer(
            getattr(requirement, "max_session_liveness_ms", None)
        ),
        "stale_policy": policy_name(getattr(requirement, "stale_policy", None)),
        "gap_policy": policy_name(getattr(requirement, "gap_policy", None)),
        "quality_flags": sorted(set(flags)),
        "connection_generation": integer(
            getattr(envelope, "connection_generation", None)
        ),
        "lease_epoch": integer(getattr(envelope, "lease_epoch", None)),
        "authority_revision": integer(
            getattr(envelope, "authority_revision", None)
        ),
        "config_revision": integer(getattr(envelope, "config_revision", None)),
    }


def _percentiles(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None}
    ordered = sorted(values)
    return {
        "n": len(ordered),
        "p50_ms": round(statistics.median(ordered), 3),
        "p95_ms": round(ordered[math.ceil(len(ordered) * 0.95) - 1], 3),
        "p99_ms": round(ordered[math.ceil(len(ordered) * 0.99) - 1], 3) if len(ordered) >= 100 else None,
        "max_ms": round(ordered[-1], 3),
    }


def _byte_summary(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {
            "n": 0,
            "p50_bytes": None,
            "p95_bytes": None,
            "p99_bytes": None,
            "max_bytes": None,
            "total_bytes": 0,
        }
    ordered = sorted(values)
    return {
        "n": len(ordered),
        "p50_bytes": round(statistics.median(ordered)),
        "p95_bytes": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "p99_bytes": ordered[math.ceil(len(ordered) * 0.99) - 1] if len(ordered) >= 100 else None,
        "max_bytes": ordered[-1],
        "total_bytes": sum(ordered),
    }


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Phase-3 profile is not readable JSON") from error
    if not isinstance(value, dict):
        raise ValueError("Phase-3 profile must be an object")
    return value


def _approved_identity_path(value: object) -> bool:
    """Accept a readable file or a Docker-mountable protected state file.

    The private alpha/monitoring directories are intentionally mode `0700` for
    the runtime UID, so the host launcher cannot stat their leaves. Docker
    mounts them read-only and the disposable bootstrap copies them only into
    tmpfs before dropping privileges. A protected path is therefore accepted
    only below the one managed QDL state root; arbitrary host files never are.
    """

    if not isinstance(value, str) or not value or "\n" in value:
        return False
    path = Path(value)
    if not path.is_absolute() or path.name not in {"ca.crt", "client.crt", "client.key", "private.key"}:
        return False
    try:
        exists_as_file = path.is_file()
    except OSError:
        exists_as_file = False
    if exists_as_file:
        return True
    try:
        path.resolve(strict=False).relative_to(_IDENTITY_STATE_ROOT.resolve())
    except (OSError, ValueError):
        return False
    return True


def validate_profile(raw: dict[str, object]) -> dict[str, object]:
    expected = {
        "image", "network", "runtime_dir", "queries", "stream_targets",
        "query_containers", "identities",
    }
    # KN-4 K4.6: a shadow target names its own containers to watch; the scope
    # is recorded, never used to relax a gate.
    # KN-4 D41: a shadow kn4-matrix names the isolated canonical broker its
    # handoff oracle reads (never a production broker).
    optional = {"monitored_containers", "scope", "oracle_bootstrap"}
    if not expected <= set(raw) or set(raw) - expected - optional:
        raise ValueError("Phase-3 profile fields are incomplete or unknown")
    scope = raw.get("scope", "production")
    if scope not in {"production", "shadow"}:
        raise ValueError("Phase-3 profile scope must be production or shadow")
    monitored = raw.get("monitored_containers")
    if monitored is not None and (
        not isinstance(monitored, list) or not 1 <= len(monitored) <= 32
        or any(not isinstance(item, str) or not _NETWORK_RE.fullmatch(item) for item in monitored)
    ):
        raise ValueError("Phase-3 profile monitored containers are invalid")
    oracle = raw.get("oracle_bootstrap")
    if oracle is not None and (
        scope != "shadow" or not isinstance(oracle, str)
        or not re.fullmatch(r"[A-Za-z0-9_.-]+:[0-9]+", oracle)
        or oracle.split(":", 1)[0] in {"kafka1", "kafka2", "kafka3"}
    ):
        raise ValueError("Phase-3 oracle bootstrap is a shadow-only isolated broker")
    image = raw["image"]
    network = raw["network"]
    runtime_dir = raw["runtime_dir"]
    queries = raw["queries"]
    streams = raw["stream_targets"]
    containers = raw["query_containers"]
    identities = raw["identities"]
    if not isinstance(image, str) or not image.strip():
        raise ValueError("Phase-3 profile requires an existing image")
    if not isinstance(network, str) or not _NETWORK_RE.fullmatch(network):
        raise ValueError("Phase-3 profile network is invalid")
    if not isinstance(runtime_dir, str) or not Path(runtime_dir).is_dir():
        raise ValueError("Phase-3 runtime directory is unavailable")
    if not isinstance(queries, list) or len(queries) != 2 or any(
        not isinstance(item, str) or not re.fullmatch(r"https://[A-Za-z0-9_.-]+:[0-9]+", item)
        for item in queries
    ):
        raise ValueError("Phase-3 profile requires exactly two HTTPS Query replicas")
    if not isinstance(streams, list) or len(streams) != 2 or any(
        not isinstance(item, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+:[0-9]+", item)
        for item in streams
    ):
        raise ValueError("Phase-3 profile requires exactly two Stream replicas")
    if not isinstance(containers, list) or len(containers) != 2 or any(
        not isinstance(item, str) or not _NETWORK_RE.fullmatch(item) for item in containers
    ):
        raise ValueError("Phase-3 profile requires exactly two Query container names")
    if not isinstance(identities, list) or not 1 <= len(identities) <= 4:
        raise ValueError("Phase-3 profile requires one to four identities")
    seen: set[str] = set()
    normalized: list[dict[str, object]] = []
    for item in identities:
        if not isinstance(item, dict) or set(item) != {"id", "tls", "jwt"}:
            raise ValueError("Phase-3 identity fields are invalid")
        consumer_id = item["id"]
        tls = item["tls"]
        jwt = item["jwt"]
        if not isinstance(consumer_id, str) or not _IDENTITY_RE.fullmatch(consumer_id) or consumer_id in seen:
            raise ValueError("Phase-3 identity ID is invalid")
        seen.add(consumer_id)
        if not isinstance(tls, dict) or set(tls) != {"ca_file", "cert_file", "key_file"}:
            raise ValueError("Phase-3 identity TLS fields are invalid")
        if not isinstance(jwt, dict) or set(jwt) != {"private_key_file", "key_id"}:
            raise ValueError("Phase-3 identity JWT fields are invalid")
        paths = [*tls.values(), jwt["private_key_file"]]
        if any(not _approved_identity_path(path) for path in paths):
            raise ValueError("Phase-3 identity file is unavailable")
        if not isinstance(jwt["key_id"], str) or not jwt["key_id"].strip():
            raise ValueError("Phase-3 JWT key ID is invalid")
        normalized.append({"id": consumer_id, "tls": dict(tls), "jwt": dict(jwt)})
    return {
        "image": image,
        "network": network,
        "runtime_dir": str(Path(runtime_dir).resolve()),
        "queries": list(queries),
        "stream_targets": list(streams),
        "query_containers": list(containers),
        "identities": normalized,
        "monitored_containers": list(monitored) if monitored is not None else None,
        "scope": scope,
        "oracle_bootstrap": oracle,
    }


def _docker_image_id(image: str) -> str:
    value = subprocess.run(
        ["docker", "image", "inspect", image, "--format", "{{.Id}}"],
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    ).stdout.strip()
    if not _DIGEST_RE.fullmatch(value):
        raise ValueError("Phase-3 image did not resolve to a local immutable digest")
    return value


def _docker_stats(containers: list[str]) -> list[dict[str, object]]:
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *containers],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    values = []
    for line in result.stdout.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            values.append(value)
    return values


def _runtime_states(containers):
    # `index` tolerates a container without a healthcheck; `.State.Health`
    # fails the whole multi-container inspect ("map has no entry").
    template = '{"name":{{json .Name}},"id":{{json .Id}},"image":{{json .Image}},"running":{{json .State.Running}},"oom":{{json .State.OOMKilled}},"restarts":{{.RestartCount}},"health":{{with index .State "Health"}}{{json .Status}}{{else}}"none"{{end}}}'
    result = subprocess.run(["docker", "inspect", "--format", template, *containers],
                            capture_output=True, text=True, check=True, timeout=20)
    return {value["name"].lstrip("/"): value for value in map(json.loads, result.stdout.splitlines())}


def _runtime_fault(before, current):
    for name, original in before.items():
        now = current.get(name)
        if now is None or not now["running"] or now["oom"] or now["health"] == "unhealthy":
            return f"runtime unhealthy: {name}"
        if now["id"] != original["id"] or now["image"] != original["image"] or now["restarts"] != original["restarts"]:
            return f"runtime restart or deployment changed: {name}"
    return None


def _cpu_throttle(states) -> dict[str, dict[str, int]]:
    """Cumulative cgroup CPU usage/throttling per container (cgroup v2), read-only."""

    values = {}
    for name, state in states.items():
        path = Path(f"/sys/fs/cgroup/system.slice/docker-{state['id']}.scope/cpu.stat")
        try:
            fields = dict(line.split() for line in path.read_text().splitlines())
            values[name] = {key: int(fields[key]) for key in ("usage_usec", "nr_periods", "nr_throttled", "throttled_usec")}
        except (OSError, KeyError, ValueError):
            continue
    return values


def _run_monitored_client(command, containers, *, timeout, observer=None, observations=None):
    baseline = _runtime_states(containers)
    fault = _runtime_fault(baseline, baseline)
    if fault:
        raise RuntimeError(fault)
    telemetry = []
    deadline = time.monotonic() + timeout
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                fault = "client deadline exceeded"
                break
            try:
                stdout, stderr = process.communicate(timeout=min(10.0, remaining))
                current = _runtime_states(containers)
                fault = _runtime_fault(baseline, current)
                return subprocess.CompletedProcess(command, process.returncode, stdout, stderr), telemetry, fault
            except subprocess.TimeoutExpired:
                current = _runtime_states(containers)
                fault = _runtime_fault(baseline, current)
                telemetry.append({"at_ns": time.time_ns(), "states": current, "stats": _docker_stats(containers),
                                  "cpu_throttle": _cpu_throttle(current)})
                if observer is not None:
                    observations.append(observer())
                if fault:
                    break
        process.kill()
        stdout, stderr = process.communicate(timeout=10)
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr), telemetry, fault
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)


def _cleanup_exact_container(name: str) -> None:
    existing = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"name=^/{name}$"],
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    ).stdout.strip()
    if existing:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True, check=True, timeout=30)
    remaining = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"name=^/{name}$"],
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    ).stdout.strip()
    if remaining:
        raise RuntimeError("Phase-3 disposable client cleanup failed")


def _identity_slug(consumer_id: str) -> str:
    return consumer_id.replace(".", "-")


def docker_command(
    profile: dict[str, object],
    *,
    name: str,
    image_id: str,
    mode: str,
    sessions: int,
    duration_seconds: int,
) -> tuple[list[str], dict[str, object]]:
    identities = profile["identities"]
    assert isinstance(identities, list)
    inner_identities = []
    mounts: list[str] = []
    for raw in identities:
        assert isinstance(raw, dict)
        consumer_id = str(raw["id"])
        slug = _identity_slug(consumer_id)
        tls = raw["tls"]
        jwt = raw["jwt"]
        assert isinstance(tls, dict) and isinstance(jwt, dict)
        destination = f"/identity/{slug}"
        fields = {
            "ca_file": (str(tls["ca_file"]), f"{destination}/tls/ca.crt"),
            "cert_file": (str(tls["cert_file"]), f"{destination}/tls/client.crt"),
            "key_file": (str(tls["key_file"]), f"{destination}/tls/client.key"),
            "private_key_file": (str(jwt["private_key_file"]), f"{destination}/jwt/private.key"),
        }
        for source, target in fields.values():
            mounts.extend(["--mount", f"type=bind,src={source},dst={target},readonly"])
        inner_identities.append({
            "id": consumer_id,
            "tls": {
                "ca_file": f"/tmp/identity/{slug}/tls/ca.crt",
                "cert_file": f"/tmp/identity/{slug}/tls/client.crt",
                "key_file": f"/tmp/identity/{slug}/tls/client.key",
            },
            "jwt": {"private_key_file": f"/tmp/identity/{slug}/jwt/private.key", "key_id": str(jwt["key_id"])},
        })
    inner = {
        "mode": mode,
        "logical_sessions": sessions,
        "duration_seconds": duration_seconds,
        "catalog": "/runtime/stable-source-bindings.yaml",
        "acquisition": "/runtime/stable-acquisition-bindings.yaml",
        "queries": profile["queries"],
        "stream_targets": profile["stream_targets"],
        "identities": inner_identities,
    }
    cmd = [
        "docker", "run", "--rm", "--name", name,
        "--network", str(profile["network"]),
        "--user", "0:0",
        "--read-only",
        "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=96m",
        "--security-opt", "no-new-privileges",
        "--pids-limit", "256",
        "--memory", "512m",
        "--cpus", "1.0",
        "--label", "qdl.phase3.disposable-load=true",
        "--env", "PYTHONDONTWRITEBYTECODE=1",
        "--env", "PYTHONPATH=/app:/driver",
        "--env", "QDL_PHASE3_LOAD_CONFIG=" + json.dumps(inner, sort_keys=True, separators=(",", ":")),
        "--mount", f"type=bind,src={profile['runtime_dir']},dst=/runtime,readonly",
        "--mount", f"type=bind,src={ROOT / 'qdl/certification/phase3_consumer_load.py'},dst=/app/qdl/certification/phase3_consumer_load.py,readonly",
        "--mount", f"type=bind,src={Path(__file__).resolve()},dst=/driver/phase3_consumer_load_acceptance.py,readonly",
        "--mount", f"type=bind,src={ROOT / 'scripts/phase3_consumer_load_bootstrap.sh'},dst=/driver/phase3_consumer_load_bootstrap.sh,readonly",
        # The KN-2 oracle/judge the kn4-matrix handoff reuses, from the same source as this driver.
        "--mount", f"type=bind,src={ROOT / 'scripts/kn_native_slice_probe.py'},dst=/driver/kn_native_slice_probe.py,readonly",
        *mounts,
        image_id,
        "/bin/sh", "/driver/phase3_consumer_load_bootstrap.sh",
    ]
    return cmd, inner


def _parse_receipt(stdout: str) -> dict[str, object] | None:
    for line in reversed(stdout.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("schema") == "qdl.phase3.consumer-load.v1":
            return value
    return None


def run_host(args: argparse.Namespace) -> int:
    profile = validate_profile(_read_json(args.profile))
    if {item["id"] for item in profile["identities"]} != set(_PHASE3_CONSUMER_MANIFESTS):
        raise ValueError("Phase-3 requires exactly the four approved crypto consumer identities")
    if args.sessions not in _ALLOWED_SESSION_COUNTS:
        raise ValueError("Phase-3 sessions must be exactly one of 5,20,35,50")
    if args.mode == "final" and (args.sessions != 50 or args.duration_seconds != 300):
        raise ValueError("Phase-3 final acceptance requires exactly 50 sessions for 300 seconds")
    if args.mode == "matrix" and args.duration_seconds != 0:
        raise ValueError("Phase-3 matrix does not accept an observation duration")
    if args.mode == "load" and not 30 <= args.duration_seconds <= 240:
        raise ValueError("Phase-3 staged load duration must be 30..240 seconds")
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    image_id = _docker_image_id(str(profile["image"]))
    name = "qdl-phase3-load-" + uuid.uuid4().hex[:12]
    command, inner = docker_command(
        profile,
        name=name,
        image_id=image_id,
        mode=args.mode,
        sessions=args.sessions,
        duration_seconds=args.duration_seconds,
    )
    before = _docker_stats(list(profile["query_containers"]))
    started = time.monotonic()
    process = None
    cleanup_error = None
    runtime_fault = None
    telemetry = []
    query_names = list(profile["query_containers"])
    compose_prefix = query_names[0].split("-query_v2_", 1)[0]
    monitored = [*query_names, f"{compose_prefix}-stream_v2_active-1", f"{compose_prefix}-stream_v2_passive-1", "market_data_service"]
    try:
        process, telemetry, runtime_fault = _run_monitored_client(
            command, monitored, timeout=max(180, args.duration_seconds + 720),
        )
    finally:
        try:
            _cleanup_exact_container(name)
        except Exception as error:  # cleanup must remain visible in the receipt
            cleanup_error = _safe_error(error)
    after = _docker_stats(list(profile["query_containers"]))
    stdout = process.stdout if process is not None else ""
    stderr = process.stderr if process is not None else ""
    receipt = _parse_receipt(stdout)
    host = {
        "schema": "qdl.phase3.consumer-load-host.v1",
        "status": "PASS" if process is not None and process.returncode == 0 and receipt and receipt.get("status") == "PASS" and cleanup_error is None and runtime_fault is None else "FAIL",
        "image_id": image_id,
        "source_sha": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip(),
        "tool_sha256": _sha256(Path(__file__).read_bytes()),
        "bootstrap_sha256": _sha256((ROOT / "scripts/phase3_consumer_load_bootstrap.sh").read_bytes()),
        "planner_sha256": _sha256((ROOT / "qdl/certification/phase3_consumer_load.py").read_bytes()),
        "mode": args.mode,
        "logical_sessions": args.sessions,
        "duration_seconds": args.duration_seconds,
        "authenticated_identity_count": len(profile["identities"]),
        "query_containers": list(profile["query_containers"]),
        "query_stats_before": before,
        "query_stats_after": after,
        "runtime_fault": runtime_fault,
        "runtime_observation": telemetry,
        "resource_sample_interval_seconds": 10,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "client_returncode": None if process is None else process.returncode,
        "client_stdout_sha256": _sha256(stdout.encode()),
        "client_stderr_sha256": _sha256(stderr.encode()),
        "cleanup_error": cleanup_error,
        "inner_config_sha256": _sha256(json.dumps(inner, sort_keys=True, separators=(",", ":")).encode()),
        "secret_values_recorded": False,
        "order_actions": 0,
    }
    (output / "host.json").write_text(json.dumps(host, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if receipt is not None:
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "schema": host["schema"], "status": host["status"], "output": str(output),
        "client_returncode": host["client_returncode"], "elapsed_seconds": host["elapsed_seconds"],
    }, sort_keys=True))
    return 0 if host["status"] == "PASS" else 1


@dataclass(slots=True)
class _HandoffReservation:
    """Two quota slots reserved for one snapshot-to-stream handoff."""

    stream_at: float
    state: str = "QUERY"


@dataclass(slots=True)
class _Pacer:
    spacing_seconds: float
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    _lock: asyncio.Lock = field(init=False, repr=False)
    _next_at: float | None = field(init=False, default=None)
    _wait_ms: float = field(init=False, default=0.0)
    _operations: Counter[str] = field(init=False, default_factory=Counter)
    _handoff_reservations: int = field(init=False, default=0)
    _incomplete_handoffs: int = field(init=False, default=0)
    _measurement_wait_ms: ContextVar[float] = field(
        init=False,
        repr=False,
        default_factory=lambda: ContextVar("phase3_pacer_measurement_wait_ms", default=0.0),
    )
    _handoff: ContextVar[_HandoffReservation | None] = field(
        init=False,
        repr=False,
        default_factory=lambda: ContextVar("phase3_pacer_handoff", default=None),
    )

    def __post_init__(self) -> None:
        if self.spacing_seconds <= 0:
            raise ValueError("load pacer spacing must be positive")
        self._lock = asyncio.Lock()

    async def acquire(self, operation: str) -> None:
        reservation = self._handoff.get()
        if reservation is not None and reservation.state == "QUERY":
            if operation not in {"snapshot", "warmup"}:
                raise RuntimeError("Phase-3 handoff expected snapshot or warmup")
            async with self._lock:
                reservation.state = "STREAM"
                self._operations[operation] += 1
            return
        if reservation is not None and reservation.state == "STREAM":
            if operation != "stream_subscribe":
                raise RuntimeError("Phase-3 handoff expected stream subscribe")
            async with self._lock:
                wait = max(0.0, reservation.stream_at - self.clock())
                self._operations[operation] += 1
                self._wait_ms += wait * 1000.0
                self._measurement_wait_ms.set(
                    self._measurement_wait_ms.get() + wait * 1000.0
                )
                reservation.state = "COMPLETE"
            if wait:
                await self.sleep(wait)
            return
        async with self._lock:
            now = self.clock()
            target = now if self._next_at is None else max(now, self._next_at)
            self._next_at = target + self.spacing_seconds
            self._operations[operation] += 1
            wait = max(0.0, target - now)
            self._wait_ms += wait * 1000.0
            self._measurement_wait_ms.set(
                self._measurement_wait_ms.get() + wait * 1000.0
            )
        if wait:
            await self.sleep(wait)

    @asynccontextmanager
    async def handoff(self):
        """Reserve consecutive quota slots before a snapshot-to-stream join.

        The signed cursor from a snapshot is only a safe live handoff when its
        matching subscription is not delayed behind unrelated work. Reserving
        both slots first preserves the per-identity leaky-bucket rate while
        limiting the post-snapshot delay to one declared spacing interval.
        """

        if self._handoff.get() is not None:
            raise RuntimeError("Phase-3 handoff reservation cannot be nested")
        async with self._lock:
            now = self.clock()
            snapshot_at = now if self._next_at is None else max(now, self._next_at)
            stream_at = snapshot_at + self.spacing_seconds
            self._next_at = stream_at + self.spacing_seconds
            wait = max(0.0, snapshot_at - now)
            self._wait_ms += wait * 1000.0
            self._measurement_wait_ms.set(
                self._measurement_wait_ms.get() + wait * 1000.0
            )
            self._handoff_reservations += 1
        if wait:
            await self.sleep(wait)
        reservation = _HandoffReservation(stream_at=stream_at)
        token = self._handoff.set(reservation)
        try:
            yield
        finally:
            if reservation.state != "COMPLETE":
                self._incomplete_handoffs += 1
            self._handoff.reset(token)

    def begin_measurement(self):
        return self._measurement_wait_ms.set(0.0)

    def finish_measurement(self, token) -> float:
        wait_ms = self._measurement_wait_ms.get()
        self._measurement_wait_ms.reset(token)
        return wait_ms

    def evidence(self) -> dict[str, object]:
        return {
            "spacing_seconds": round(self.spacing_seconds, 6),
            "queue_wait_ms": round(self._wait_ms, 3),
            "operations": dict(sorted(self._operations.items())),
            "handoff_reservations": self._handoff_reservations,
            "incomplete_handoffs": self._incomplete_handoffs,
        }


class _PacedQueryTransport:
    def __init__(self, delegate, pacer: _Pacer) -> None:
        self._delegate = delegate
        self._pacer = pacer

    async def _call(self, operation: str, *args, **kwargs):
        await self._pacer.acquire(operation)
        return await getattr(self._delegate, operation)(*args, **kwargs)

    async def snapshot(self, *args, **kwargs):
        return await self._call("snapshot", *args, **kwargs)

    async def warmup(self, *args, **kwargs):
        return await self._call("warmup", *args, **kwargs)

    async def warmup_batch(self, *args, **kwargs):
        return await self._call("warmup_batch", *args, **kwargs)

    async def reference_batch(self, *args, **kwargs):
        return await self._call("reference_batch", *args, **kwargs)

    async def feed_status(self, *args, **kwargs):
        return await self._call("feed_status", *args, **kwargs)

    async def instruments(self, *args, **kwargs):
        return await self._call("instruments", *args, **kwargs)

    async def instrument(self, *args, **kwargs):
        return await self._call("instrument", *args, **kwargs)

    async def close(self) -> None:
        await self._delegate.close()


class _PacedStreamTransport:
    def __init__(self, delegate, pacer: _Pacer) -> None:
        self._delegate = delegate
        self._pacer = pacer

    async def subscribe(self, *args, **kwargs):
        await self._pacer.acquire("stream_subscribe")
        async for item in self._delegate.subscribe(*args, **kwargs):
            yield item

    async def close(self) -> None:
        await self._delegate.close()


@dataclass(frozen=True, slots=True)
class _StreamSpec:
    name: str
    consumer_id: str
    product: object
    slow: bool
    purpose: str


@dataclass(frozen=True, slots=True)
class _Identity:
    consumer_id: str
    tls: object
    credential: object
    max_buffer_events: int


def _product_evidence(product) -> dict[str, object]:
    return {
        "consumer_id": product.consumer_id,
        "venue": product.venue,
        "native_symbol": product.native_symbol,
        "feed": product.feed.value,
        "interval": product.interval,
        "delivery": product.delivery.value,
        "source_policy_id": product.source_policy_id,
    }


def _product_group(product, operation: str, replica: str) -> tuple[str, ...]:
    return (
        operation, replica, product.venue, product.native_symbol,
        product.feed.value, product.interval or "",
    )


class _BoundedSamples:
    """Deterministic reservoir per declared route; all-event counts stay exact."""

    def __init__(self, per_group: int = 512):
        self.per_group = per_group
        self.seen = Counter()
        self.groups = defaultdict(list)
        self.random = random.Random(0)

    def append(self, sample):
        key = tuple(sample["group"])
        self.seen[key] += 1
        values = self.groups[key]
        if len(values) < self.per_group:
            values.append(sample)
        else:
            index = self.random.randrange(self.seen[key])
            if index < self.per_group:
                values[index] = sample

    def __iter__(self):
        for values in self.groups.values():
            yield from values


def _summarize_samples(samples: list[dict[str, object]]) -> list[dict[str, object]]:
    groups: dict[tuple[str, ...], dict[str, list[float] | list[int]]] = defaultdict(
        lambda: {"usable": [], "queue": [], "bytes": [], "validation_ms": [], "source_to_usable_ms": [], "host_receive_to_usable_ms": [], "interarrival_ms": []}
    )
    for sample in samples:
        for metric in ("validation_ms", "source_to_usable_ms", "host_receive_to_usable_ms", "interarrival_ms"):
            if isinstance(sample.get(metric), float):
                groups[tuple(sample["group"])][metric].append(sample[metric])
        latency = sample.get("usable_ms")
        if isinstance(latency, float):
            groups[tuple(sample["group"])]["usable"].append(latency)
        queue_wait = sample.get("queue_wait_ms")
        if isinstance(queue_wait, float):
            groups[tuple(sample["group"])]["queue"].append(queue_wait)
        response_bytes = sample.get("response_payload_bytes")
        if isinstance(response_bytes, int):
            groups[tuple(sample["group"])]["bytes"].append(response_bytes)
    return [
        {
            "operation": key[0], "replica": key[1], "venue": key[2],
            "observed_samples": samples.seen[key] if isinstance(samples, _BoundedSamples) else len(values["usable"]) or len(values["validation_ms"]),
            "sampling": "bounded_reservoir" if isinstance(samples, _BoundedSamples) else "all_samples",
            "native_symbol": key[3], "feed": key[4], "interval": key[5] or None,
            "usable_latency": _percentiles(values["usable"]),
            "client_pacing_wait": _percentiles(values["queue"]),
            "response_payload": _byte_summary(values["bytes"]),
            "stream_validation": _percentiles(values["validation_ms"]),
            "source_to_usable": _percentiles(values["source_to_usable_ms"]),
            "host_receive_to_usable": _percentiles(values["host_receive_to_usable_ms"]),
            "stream_interarrival": _percentiles(values["interarrival_ms"]),
        }
        for key, values in sorted(groups.items())
    ]


def _inside_config() -> dict[str, object]:
    try:
        value = json.loads(os.environ["QDL_PHASE3_LOAD_CONFIG"])
    except (KeyError, json.JSONDecodeError) as error:
        raise ValueError("Phase-3 inner configuration is unavailable") from error
    if not isinstance(value, dict):
        raise ValueError("Phase-3 inner configuration is invalid")
    expected = {
        "mode", "logical_sessions", "duration_seconds", "catalog", "acquisition",
        "queries", "stream_targets", "identities",
    }
    if str(value.get("mode", "")).startswith("target") or value.get("mode") == "kn4-matrix":
        expected |= {"budget", "final"}
    if value.get("mode") == "kn4-matrix":
        expected |= {"oracle_bootstrap"}
    if set(value) != expected:
        raise ValueError("Phase-3 inner configuration fields are invalid")
    return value


def _selected_five(products):
    result = tuple(
        item for item in products
        if item.venue in _FIVE_LIQUID and item.native_symbol in _FIVE_LIQUID[item.venue]
    )
    if not result:
        raise ValueError("Phase-3 manifest has no five-liquid V2 products")
    return result


def _identity_map(config: dict[str, object], manifests: dict[str, object]) -> dict[str, _Identity]:
    from qdl_sdk.credentials import RotatingJwtCredentialProvider
    from qdl_sdk.tls import WorkloadTlsConfig

    raw_identities = config["identities"]
    assert isinstance(raw_identities, list)
    values: dict[str, _Identity] = {}
    for raw in raw_identities:
        if not isinstance(raw, dict) or set(raw) != {"id", "tls", "jwt"}:
            raise ValueError("Phase-3 inner identity fields are invalid")
        consumer_id = raw["id"]
        tls_raw = raw["tls"]
        jwt_raw = raw["jwt"]
        if not isinstance(consumer_id, str) or not isinstance(tls_raw, dict) or not isinstance(jwt_raw, dict):
            raise ValueError("Phase-3 inner identity is invalid")
        manifest = manifests.get(consumer_id)
        if manifest is None:
            raise ValueError("Phase-3 identity is not in the sealed release route")
        values[consumer_id] = _Identity(
            consumer_id=consumer_id,
            tls=WorkloadTlsConfig(
                tls_raw["ca_file"], tls_raw["cert_file"], tls_raw["key_file"],
            ),
            credential=RotatingJwtCredentialProvider(
                private_key_file=jwt_raw["private_key_file"],
                key_id=jwt_raw["key_id"], algorithm="RS256", issuer=_ISSUER,
                audience=_AUDIENCE, subject=manifest.subject, environment=manifest.environment,
                roles=_ROLES, venues=("BINANCE", "OKX"),
                consumer_manifest_revision=manifest.manifest_revision,
                lifetime_seconds=300, refresh_before_seconds=60,
            ),
            max_buffer_events=manifest.quotas.max_buffer_events,
        )
    if set(values) != set(manifests):
        raise ValueError("Phase-3 identity set differs from the sealed workload scope")
    return values


def _reference_product(product, *, now_ns: int):
    from qdl.certification.reference_l2_acceptance import (
        ReferenceAcceptanceProduct,
        reference_request_for_requirement,
    )
    return ReferenceAcceptanceProduct(
        consumer_id=product.consumer_id,
        consumer_subject=product.consumer_subject,
        manifest_revision=product.manifest_revision,
        manifest_sha256=product.manifest_sha256,
        instrument_uid=product.instrument_uid,
        instrument_id=product.instrument_id,
        venue=product.venue,
        market=product.market,
        native_symbol=product.native_symbol,
        requirement=product.requirement,
        sdk_requirement=reference_request_for_requirement(product.requirement, now_ns=now_ns),
    )


def _response_payload_bytes(response: object) -> int:
    model_dump = getattr(response, "model_dump", None)
    payload = model_dump(mode="json") if callable(model_dump) else response
    return len(
        json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode()
    )


def _measurement_sample(
    *, product, operation: str, replica: str, pacer: _Pacer, token,
    started: float, response: object,
) -> dict[str, object]:
    queue_wait_ms = pacer.finish_measurement(token)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return {
        "group": _product_group(product, operation, replica),
        "usable_ms": max(0.0, elapsed_ms - queue_wait_ms),
        "queue_wait_ms": queue_wait_ms,
        "response_payload_bytes": _response_payload_bytes(response),
    }


async def _read_product(client, product) -> object:
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
    from qdl.certification.reference_l2_acceptance import reference_evidence

    if product.delivery.value == "ON_DEMAND":
        reference = _reference_product(product, now_ns=time.time_ns())
        response = await client.reference_batch([reference.sdk_requirement], require_all=True)
        if response.partial or len(response.results) != 1:
            raise ValueError("PARTIAL_RESULT")
        reference_evidence(reference, response.results[0], observed_at_ns=time.time_ns())
        return response
    response = await client.snapshot(sdk_requirement(product))
    validate_product_view(product, response.data, require_current_quality=True)
    return response


def _stream_product(products):
    rank = {"TRADE": 0, "QUOTE": 1, "BOOK_DELTA": 2, "BOOK_SNAPSHOT": 3, "BAR": 4}
    values = [item for item in products if item.delivery.value == "DURABLE" and item.feed.value in rank]
    if not values:
        raise ValueError("logical session has no streamable durable product")
    return min(values, key=lambda item: (rank[item.feed.value], item.interval or ""))


def _final_bar_product(products):
    """Return the shortest declared durable BAR, or no final-BAR probe.

    A manifest that only consumes live feeds must not be rejected merely
    because it has no BAR requirement. Its supplemental stream remains a
    continuity probe and is explicitly reported as such.
    """
    from qdl.adapters.intervals import canonical_interval_ms

    values = [
        item for item in products
        if item.delivery.value == "DURABLE" and item.feed.value == "BAR"
    ]
    if not values:
        return None
    if any(not isinstance(item.interval, str) or not item.interval for item in values):
        raise ValueError("Phase-3 durable BAR product is missing its canonical interval")
    return min(
        values,
        key=lambda item: (
            canonical_interval_ms(item.interval),
            item.venue,
            item.native_symbol,
        ),
    )


def _supplemental_stream_spec(consumer_id, products) -> _StreamSpec:
    final_bar = _final_bar_product(products)
    if final_bar is not None:
        return _StreamSpec(
            name=f"final-bar-{consumer_id}",
            consumer_id=consumer_id,
            product=final_bar,
            slow=False,
            purpose="FINAL_BAR",
        )
    return _StreamSpec(
        name=f"continuity-{consumer_id}",
        consumer_id=consumer_id,
        product=_stream_product(products),
        slow=False,
        purpose="CONTINUITY",
    )


def _build_stream_specs(plan, products_by_consumer) -> tuple[_StreamSpec, ...]:
    logical = tuple(
        _StreamSpec(
            name=f"logical-{session.ordinal}",
            consumer_id=session.consumer_id,
            product=_stream_product(session.products),
            slow=session.ordinal % 13 == 0,
            purpose="LOGICAL",
        )
        for session in plan.logical_sessions
    )
    supplemental = tuple(
        _supplemental_stream_spec(consumer_id, products)
        for consumer_id, products in sorted(products_by_consumer.items())
    )
    return (*logical, *supplemental)


def _stream_buffer_bound(identity) -> int:
    value = getattr(identity, "max_buffer_events", None)
    if not isinstance(value, int) or not 1 <= value <= 10_000:
        raise ValueError("Phase-3 identity has an invalid sealed stream buffer quota")
    return value


def _make_client(identity, *, queries, stream_targets, pacer, replicated: bool):
    from qdl_sdk.client import AsyncDataLayerClient
    from qdl_sdk.transport import GrpcStreamTransport, ReplicatedRestQueryTransport, RestQueryTransport

    replicas = [
        RestQueryTransport(url, timeout_seconds=15.0, tls=identity.tls, credential_provider=identity.credential)
        for url in queries
    ]
    query = ReplicatedRestQueryTransport(replicas, max_attempts=2, cooldown_seconds=1.0) if replicated else replicas[0]
    return AsyncDataLayerClient(
        query_transport=_PacedQueryTransport(query, pacer),
        stream_transport=_PacedStreamTransport(
            GrpcStreamTransport(stream_targets, tls=identity.tls, credential_provider=identity.credential), pacer,
        ),
        consumer_id=identity.consumer_id,
        max_buffer_events=_stream_buffer_bound(identity),
        max_reconnect_attempts=1,
    )


@asynccontextmanager
async def _paced_warmup_then_stream(client, pacer: _Pacer, requirement, **kwargs):
    """Keep the signed snapshot and its first stream join within two quota slots.

    Reconnects after the first subscription deliberately return to normal pacing.
    A failed warmup does not refund its reserved stream slot, so the test client
    never exceeds the same per-identity leaky-bucket allowance.
    """

    async with pacer.handoff():
        async with client.warmup_then_stream(requirement, **kwargs) as session:
            yield session


async def _matrix_read(identity, *, product, replica, queries, stream_targets, pacer, samples, errors) -> None:
    client = _make_client(identity, queries=(replica,), stream_targets=stream_targets, pacer=pacer, replicated=False)
    token = pacer.begin_measurement()
    started = time.perf_counter()
    try:
        response = await _read_product(client, product)
        sample = _measurement_sample(
            product=product,
            operation="REFERENCE_BATCH" if product.delivery.value == "ON_DEMAND" else "SNAPSHOT",
            replica=replica,
            pacer=pacer,
            token=token,
            started=started,
            response=response,
        )
        token = None
        samples.append(sample)
    except Exception as error:
        if token is not None:
            pacer.finish_measurement(token)
        errors.append({"operation": "MATRIX", "replica": replica, "product": _product_evidence(product), "error": _safe_error(error)})
    finally:
        await client.close()


def _matrix_selection(*, sessions, execution_products):
    selected: dict[tuple[str, tuple[str, str, str, str, str]], object] = {}
    for session in sessions:
        for product in session.products:
            selected[(session.consumer_id, product.identity)] = product
    # The trading-system manifest is the shared execution-facing consumer. Add
    # exactly one V2 product per required venue/symbol so every staged matrix
    # proves the declared five-liquid universe without replaying all unchanged
    # manifest products.
    feed_rank = {"QUOTE": 0, "TRADE": 1, "BAR": 2, "BOOK_SNAPSHOT": 3, "BOOK_DELTA": 4}
    for venue, symbols in sorted(_FIVE_LIQUID.items()):
        for symbol in sorted(symbols):
            candidates = [
                product for product in execution_products
                if product.venue == venue and product.native_symbol == symbol
            ]
            if not candidates:
                raise ValueError(f"Phase-3 execution scope misses {venue}:{symbol}")
            product = min(
                candidates,
                key=lambda item: (feed_rank.get(item.feed.value, 99), item.interval or ""),
            )
            selected[(product.consumer_id, product.identity)] = product
    return tuple(product for _, product in sorted(selected.items(), key=lambda item: item[0]))


async def _run_matrix(*, products_by_consumer, identities, queries, stream_targets, pacers, sessions):
    selected = _matrix_selection(
        sessions=sessions,
        execution_products=products_by_consumer["trading-system.paper.stable"],
    )
    samples: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    for product in selected:
        for replica in queries:
            await _matrix_read(
                identities[product.consumer_id], product=product, replica=replica,
                queries=queries, stream_targets=stream_targets, pacer=pacers[product.consumer_id],
                samples=samples, errors=errors,
            )
            if errors:
                break
        if errors:
            break
    return samples, errors, selected


async def _poll_worker(*, client, products, pacer, deadline: float, samples, errors) -> None:
    index = 0
    token = None
    try:
        while time.monotonic() < deadline:
            product = products[index % len(products)]
            index += 1
            token = pacer.begin_measurement()
            started = time.perf_counter()
            response = await _read_product(client, product)
            sample = _measurement_sample(
                product=product,
                operation="REFERENCE_BATCH" if product.delivery.value == "ON_DEMAND" else "SNAPSHOT",
                replica="replicated",
                pacer=pacer,
                token=token,
                started=started,
                response=response,
            )
            token = None
            samples.append(sample)
            await asyncio.sleep(0)
    except Exception as error:
        if token is not None:
            pacer.finish_measurement(token)
        errors.append({"operation": "POLL", "product": _product_evidence(product), "error": _safe_error(error)})
    finally:
        await client.close()


async def _reconnect_probe(*, identity, product, queries, stream_targets, pacer) -> None:
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
    from qdl_sdk.models import StreamEvent
    from qdl_sdk.projection import market_data_view_from_stream

    client = _make_client(identity, queries=queries, stream_targets=stream_targets, pacer=pacer, replicated=True)
    try:
        async with _paced_warmup_then_stream(
            client, pacer, sdk_requirement(product)
        ) as first:
            event = await asyncio.wait_for(first.__anext__(), timeout=60.0)
            if not isinstance(event, StreamEvent):
                raise ValueError("reconnect probe did not receive a stream event")
            view = market_data_view_from_stream(event, template=first.warmup.data[-1], requirement=sdk_requirement(product))
            validate_product_view(product, view, require_current_quality=True)
            first.acknowledge(event)
        async with _paced_warmup_then_stream(
            client,
            pacer,
            sdk_requirement(product),
            resume_restored_state=True,
        ) as restored:
            if not restored.state_restored:
                raise ValueError("signed cursor was not restored on reopen")
            event = await asyncio.wait_for(restored.__anext__(), timeout=60.0)
            if not isinstance(event, StreamEvent):
                raise ValueError("restored cursor did not yield a stream event")
            view = market_data_view_from_stream(
                event,
                template=restored.warmup.data[-1],
                requirement=sdk_requirement(product),
            )
            validate_product_view(product, view, require_current_quality=True)
            restored.acknowledge(event)
    finally:
        await client.close()


async def _n_minus_one_probe(*, identity, product, secondary, stream_targets, pacer) -> None:
    from qdl_sdk.client import AsyncDataLayerClient
    from qdl_sdk.transport import GrpcStreamTransport, ReplicatedRestQueryTransport, RestQueryTransport

    dead = RestQueryTransport("https://127.0.0.1:1", timeout_seconds=2.0, tls=identity.tls, credential_provider=identity.credential)
    live = RestQueryTransport(secondary, timeout_seconds=15.0, tls=identity.tls, credential_provider=identity.credential)
    query = _PacedQueryTransport(ReplicatedRestQueryTransport((dead, live), max_attempts=2, cooldown_seconds=1.0), pacer)
    client = AsyncDataLayerClient(
        query_transport=query,
        stream_transport=_PacedStreamTransport(GrpcStreamTransport(stream_targets, tls=identity.tls, credential_provider=identity.credential), pacer),
        consumer_id=identity.consumer_id,
        max_buffer_events=_stream_buffer_bound(identity),
        max_reconnect_attempts=0,
    )
    try:
        await _read_product(client, product)
        stats = query._delegate.stats()
        if not (stats[0]["failures"] == 1 and stats[1]["successes"] == 1):
            raise ValueError("client-side alternate Query reader was not exercised exactly once")
    finally:
        await client.close()


async def _stream_events_until_stop(session, stopped, *, poll_seconds: float = 5.0):
    """Keep one pending read alive across quiet periods and join it on exit."""
    pending = None
    try:
        while not stopped.is_set():
            if pending is None:
                pending = asyncio.create_task(session.__anext__())
            done, _ = await asyncio.wait({pending}, timeout=poll_seconds)
            if not done:
                continue
            event = pending.result()
            pending = None
            yield event, time.perf_counter(), time.time_ns()
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


async def _cold_history_probe(*, identity, product, queries, stream_targets, pacer, samples, counters):
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view

    client = _make_client(identity, queries=queries, stream_targets=stream_targets, pacer=pacer, replicated=True)
    try:
        for rows in (2500, 5000):
            requirement = sdk_requirement(product)
            if requirement.warmup_limit < rows:
                raise ValueError("declared BAR history cannot satisfy the approved cold workload")
            requirement = replace(requirement, warmup_limit=rows)
            token = pacer.begin_measurement()
            started = time.perf_counter()
            try:
                response = await client.warmup(requirement)
                if len(response.data) != rows:
                    raise ValueError("cold history coverage differs from requested rows")
                for item in response.data:
                    validate_product_view(product, item, require_current_quality=False)
                validate_product_view(product, response.data[-1], require_current_quality=True)
                samples.append(_measurement_sample(
                    product=product, operation=f"WARMUP_{rows}", replica="replicated",
                    pacer=pacer, token=token, started=started, response=response,
                ))
                token = None
                counters[f"warmup_rows:{product.venue}:{rows}"] += len(response.data)
            finally:
                if token is not None:
                    pacer.finish_measurement(token)
    finally:
        await client.close()


def _cold_history_selection(products_by_consumer):
    selected = []
    for consumer in ("alpha.binance.paper.stable", "alpha.okx.paper.stable"):
        candidates = [p for p in products_by_consumer.get(consumer, ())
                      if p.feed.value == "BAR" and p.interval == "1m"
                      and p.delivery.value == "DURABLE" and p.requirement.warmup_limit >= 5000]
        if not candidates:
            raise ValueError(f"cold history workload has no declared 5000-row BAR for {consumer}")
        selected.append(min(candidates, key=lambda p: p.native_symbol))
    return selected


async def _run_load(*, plan, products_by_consumer, identities, queries, stream_targets, pacers, duration_seconds: int):
    samples = _BoundedSamples()
    errors: list[dict[str, object]] = []
    counters: Counter[str] = Counter()
    # Each identity has one supplemental stream inside the sealed stream budget.
    # It proves final BAR only where the manifest declares a durable BAR; a
    # live-only consumer instead contributes an explicitly labelled continuity
    # probe and is never misreported as BAR-finality coverage.
    startup: asyncio.Queue = asyncio.Queue()
    stream_specs = _build_stream_specs(plan, products_by_consumer)
    setup_budget = max(
        90.0,
        max(budget.planned_streams * budget.seconds_per_request * 2.0 for budget in plan.identity_budgets) + 60.0,
    )
    setup_deadline = time.monotonic() + setup_budget
    start_observation = asyncio.Event()
    stop_observation = asyncio.Event()
    observation_deadline: float | None = None

    # Establish every signed warmup/cursor and receive one validated event
    # before starting the timed observation. This prevents setup pacing from
    # being reported as load latency, while keeping the first event as evidence
    # that the stream is genuinely live rather than merely constructed.
    async def establish_then_run(spec: _StreamSpec):
        from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
        from qdl_sdk.models import ControlEvent, StreamEvent
        from qdl_sdk.projection import market_data_view_from_stream
        client = _make_client(identities[spec.consumer_id], queries=queries, stream_targets=stream_targets, pacer=pacers[spec.consumer_id], replicated=True)
        started = time.perf_counter()
        admitted = False
        last_stream_event = None
        requirement = sdk_requirement(spec.product)
        try:
            async with _paced_warmup_then_stream(
                client, pacers[spec.consumer_id], requirement
            ) as session:
                slowed = False
                previous_delivery = None
                async for event, delivered_at, delivered_ns in _stream_events_until_stop(
                    session, stop_observation
                ):
                    if isinstance(event, ControlEvent):
                        counters["stream_control_events"] += 1
                        continue
                    if not isinstance(event, StreamEvent):
                        raise ValueError("stream returned an unknown event type")
                    last_stream_event = event
                    view = market_data_view_from_stream(
                        event, template=session.warmup.data[-1], requirement=requirement
                    )
                    validate_product_view(spec.product, view, require_current_quality=True)
                    session.acknowledge(event)
                    validated_at = time.perf_counter()
                    validated_ns = delivered_ns + int((validated_at - delivered_at) * 1_000_000_000)
                    counters[f"stream_event:{spec.name}"] += 1
                    observing = start_observation.is_set()
                    if observing:
                        counters[f"observed_event:{spec.name}"] += 1
                    # A blocking next-event wait includes venue cadence. Report
                    # source/host age and local validation separately from RTT.
                    sample = {
                        "group": _product_group(spec.product, "STREAM", "replicated"),
                        "validation_ms": (validated_at - delivered_at) * 1000.0,
                    }
                    for field, timestamp in (
                        ("source_to_usable_ms", event.event.source_event_time_ns),
                        ("host_receive_to_usable_ms", event.event.received_at_ns),
                    ):
                        if timestamp > 0:
                            sample[field] = max(0.0, (validated_ns - timestamp) / 1_000_000.0)
                    if previous_delivery is not None:
                        sample["interarrival_ms"] = (delivered_at - previous_delivery) * 1000.0
                    previous_delivery = delivered_at
                    samples.append(sample)
                    if not admitted:
                        counters[f"setup_first_usable_ms:{spec.name}"] = round(
                            (validated_at - started) * 1000.0, 3
                        )
                        await startup.put((spec.name, None))
                        admitted = True
                    # Drain while other streams are opening; setup itself must
                    # not turn an already live subscription into a slow reader.
                    if observing and spec.slow and not slowed:
                        slowed = True
                        await asyncio.sleep(5.0)
                        counters["slow_reader_sessions"] += 1
        except asyncio.CancelledError:
            raise
        except Exception as error:
            evidence = {
                "operation": "STREAM",
                "stream": spec.name,
                "product": _product_evidence(spec.product),
                "error": _safe_error(error),
            }
            if last_stream_event is not None:
                evidence["stream_quality"] = _stream_frame_quality_diagnostic(
                    last_stream_event, requirement
                )
            errors.append(evidence)
            if not admitted:
                await startup.put((spec.name, "FAILED"))
        finally:
            await client.close()

    stream_tasks = [asyncio.create_task(establish_then_run(spec)) for spec in stream_specs]
    active_tasks: list[asyncio.Task] = []
    started_names: set[str] = set()
    try:
        while len(started_names) < len(stream_specs):
            timeout = max(0.1, setup_deadline - time.monotonic())
            name, failure = await asyncio.wait_for(startup.get(), timeout=timeout)
            if name in started_names:
                continue
            started_names.add(name)
            if failure:
                raise RuntimeError("stream setup failed")
        if errors:
            raise RuntimeError("stream setup reported a typed failure")
        observation_deadline = time.monotonic() + duration_seconds
        start_observation.set()
        poll_tasks = [
            asyncio.create_task(_poll_worker(
                client=_make_client(identities[session.consumer_id], queries=queries, stream_targets=stream_targets, pacer=pacers[session.consumer_id], replicated=True),
                products=session.products, pacer=pacers[session.consumer_id],
                deadline=observation_deadline, samples=samples, errors=errors,
            ))
            for session in plan.logical_sessions
        ]
        reconnect_tasks = [
            asyncio.create_task(_reconnect_probe(
                identity=identities[consumer_id], product=_stream_product(products), queries=queries,
                stream_targets=stream_targets, pacer=pacers[consumer_id],
            ))
            for consumer_id, products in sorted(products_by_consumer.items())
        ]
        nminusone_tasks = [
            asyncio.create_task(_n_minus_one_probe(
                identity=identities[consumer_id], product=_stream_product(products), secondary=queries[1],
                stream_targets=stream_targets, pacer=pacers[consumer_id],
            ))
            for consumer_id, products in sorted(products_by_consumer.items())
        ]
        cold_tasks = [
            asyncio.create_task(_cold_history_probe(
                identity=identities[product.consumer_id], product=product, queries=queries,
                stream_targets=stream_targets, pacer=pacers[product.consumer_id],
                samples=samples, counters=counters,
            ))
            for product in (_cold_history_selection(products_by_consumer) if products_by_consumer else ())
        ]
        active_tasks = [*poll_tasks, *reconnect_tasks, *nminusone_tasks, *cold_tasks]
        while time.monotonic() < observation_deadline:
            await asyncio.sleep(min(1.0, observation_deadline - time.monotonic()))
            if errors:
                raise RuntimeError("load worker reported a typed failure")
            completed = [task for task in active_tasks if task.done() and not task.cancelled()]
            for task in completed:
                error = task.exception()
                if error is not None:
                    raise RuntimeError("load auxiliary worker failed") from error
        stop_observation.set()
        results = await asyncio.gather(*active_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise RuntimeError("load worker failed after observation") from result
    except Exception as error:
        errors.append({"operation": "LOAD", "error": _safe_error(error)})
        for task in [*active_tasks, *stream_tasks]:
            if not task.done():
                task.cancel()
    finally:
        stop_observation.set()
        start_observation.set()
        await asyncio.gather(*active_tasks, *stream_tasks, return_exceptions=True)
    expected_bars = [spec.name for spec in stream_specs if spec.purpose == "FINAL_BAR"]
    missing_bars = [name for name in expected_bars if counters[f"observed_event:{name}"] < 1]
    if missing_bars:
        errors.append({"operation": "FINAL_BAR", "missing_streams": sorted(missing_bars)})
    supplemental = [
        {
            "consumer_id": spec.consumer_id,
            "kind": spec.purpose,
            "final_bar_status": (
                "FINAL_BAR_DECLARED"
                if spec.purpose == "FINAL_BAR"
                else "FINAL_BAR_NOT_DECLARED"
            ),
            "product": _product_evidence(spec.product),
        }
        for spec in stream_specs
        if spec.purpose != "LOGICAL"
    ]
    return samples, errors, counters, supplemental


async def run_inside() -> dict[str, object]:
    config = _inside_config()
    from qdl.certification.phase103_consumer_acceptance import build_manifest_acceptance_scope
    from qdl.certification.phase3_consumer_load import assert_required_instrument_coverage, build_consumer_load_plan
    from qdl.consumer.manifest import ConsumerManifestLoader
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from qdl.runtime.stable_deployment import StableAcquisitionPlan

    catalog = StableSourceCatalog.load(config["catalog"])
    acquisition = StableAcquisitionPlan.load(config["acquisition"], catalog=catalog)
    declared_identity_ids = {
        str(item["id"])
        for item in config["identities"]
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    if declared_identity_ids != set(_PHASE3_CONSUMER_MANIFESTS):
        raise ValueError("Phase-3 inner identity scope is not the approved crypto set")
    runtime_manifests = {
        consumer_id: ConsumerManifestLoader.load(path)
        for consumer_id, path in _PHASE3_CONSUMER_MANIFESTS.items()
    }
    identities = _identity_map(config, runtime_manifests)
    scope = build_manifest_acceptance_scope(
        tuple(_PHASE3_CONSUMER_MANIFESTS.values()),
        catalog=catalog,
        acquisition=acquisition,
        expected_consumer_ids=frozenset(_PHASE3_CONSUMER_MANIFESTS),
        schema="qdl.phase105.consumer-acceptance-scope.v1",
    )
    products_by_consumer = {
        consumer_id: _selected_five(tuple(item for item in scope.products if item.consumer_id == consumer_id))
        for consumer_id in sorted(identities)
    }
    required_pairs = tuple(
        (venue, symbol)
        for venue, symbols in sorted(_FIVE_LIQUID.items())
        for symbol in sorted(symbols)
    )
    plan = build_consumer_load_plan(
        manifests=runtime_manifests,
        products_by_consumer=products_by_consumer,
        logical_session_count=int(config["logical_sessions"]),
        required_instruments=required_pairs,
    )
    assert_required_instrument_coverage(plan, required_pairs)
    pacers = {
        budget.consumer_id: _Pacer(budget.seconds_per_request)
        for budget in plan.identity_budgets
    }
    mode = config["mode"]
    queries = tuple(config["queries"])
    streams = tuple(config["stream_targets"])
    started = time.monotonic()
    if mode == "matrix":
        samples, errors, selected_products = await _run_matrix(
            products_by_consumer=products_by_consumer, identities=identities,
            queries=queries, stream_targets=streams, pacers=pacers,
            sessions=plan.logical_sessions,
        )
        selected_count = len(selected_products)
        covered_instruments = tuple(sorted({
            (product.venue, product.native_symbol) for product in selected_products
        }))
        counters = Counter()
        supplemental_streams = []
    elif mode in {"load", "final"}:
        samples, errors, counters, supplemental_streams = await _run_load(
            plan=plan, products_by_consumer=products_by_consumer, identities=identities,
            queries=queries, stream_targets=streams, pacers=pacers,
            duration_seconds=int(config["duration_seconds"]),
        )
        selected_count = sum(len(item.products) for item in plan.logical_sessions)
        covered_instruments = plan.covered_instruments
    else:
        raise ValueError("Phase-3 inner mode is invalid")
    status = "PASS" if not errors else "FAIL"
    return {
        "schema": "qdl.phase3.consumer-load.v1",
        "status": status,
        "mode": mode,
        "logical_session_count": len(plan.logical_sessions),
        "authenticated_identity_count": plan.identity_count,
        "planned_stream_count": plan.stream_count,
        "selected_product_count": selected_count,
        "covered_instruments": [{"venue": venue, "native_symbol": symbol} for venue, symbol in covered_instruments],
        "runtime_contract": {
            "catalog_revision": catalog.catalog_revision,
            "acquisition_revision": acquisition.revision,
            "manifests": [
                {
                    "consumer_id": consumer_id,
                    "manifest_revision": manifest.manifest_revision,
                    "manifest_sha256": manifest.manifest_sha256,
                }
                for consumer_id, manifest in sorted(runtime_manifests.items())
            ],
        },
        "identity_budgets": [
            {
                "consumer_id": item.consumer_id,
                "logical_sessions": item.logical_sessions,
                "requests_per_minute": item.requests_per_minute,
                "test_requests_per_minute": item.test_requests_per_minute,
                "seconds_per_request": round(item.seconds_per_request, 6),
                "max_streams": item.max_streams,
                "planned_streams": item.planned_streams,
                "max_buffer_events": identities[item.consumer_id].max_buffer_events,
            }
            for item in plan.identity_budgets
        ],
        "latency": _summarize_samples(samples),
        "pacer": {consumer_id: pacer.evidence() for consumer_id, pacer in sorted(pacers.items())},
        "stream_counters": dict(sorted(counters.items())),
        "supplemental_streams": supplemental_streams,
        "errors": errors[:20],
        "error_count": len(errors),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "provider_connections": 0,
        "v1_fallback_calls": 0,
        "direct_provider_calls": 0,
        "order_actions": 0,
        "secret_values_recorded": False,
        "test_provenance": False,
        "process_cpu_seconds": round(resource.getrusage(resource.RUSAGE_SELF).ru_utime + resource.getrusage(resource.RUSAGE_SELF).ru_stime, 3),
        "process_max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }


# ---------------------------------------------------------------------------
# v2.1.1 target mode (DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md,
# #read-plane-v211-target-closure). The frozen four-class workload runs at its
# declared rate on the two alpha identities; nothing is paced to a quota. Each
# logical alpha owns one SDK client, as an alpha process would. Sessions are
# spread over worker processes so the load client is not the bottleneck, and
# the scheduler lag it reports is the proof of that.
# ---------------------------------------------------------------------------

_TARGET_VENUE_IDENTITY = {
    "BINANCE": "alpha.binance.paper.stable",
    "OKX": "alpha.okx.paper.stable",
}
_TARGET_BUDGET_PATH = ROOT / "config/v2/v211-target-acceptance-budget.json"
_TARGET_LATENCY_CLASS = {
    ("QUOTE", "SNAPSHOT"): "QUOTE_SNAPSHOT",
    ("TRADE", "SNAPSHOT"): "TRADE_SNAPSHOT",
    ("MARK_INDEX_PRICE", "REFERENCE_BATCH"): "MARK_INDEX_REFERENCE",
    ("BOOK_SNAPSHOT", "SNAPSHOT"): "L2_SNAPSHOT",
    ("BAR", "SNAPSHOT"): "BAR_LATEST",
    ("FUNDING_RATE", "REFERENCE_BATCH"): "FUNDING_REFERENCE",
}
_SERVED_BY: ContextVar[str | None] = ContextVar("phase3_served_by", default=None)
_QUERY_METHODS = frozenset({
    "snapshot", "warmup", "warmup_batch", "reference_batch",
    "feed_status", "instruments", "instrument",
})
_TS_HEARTBEAT = """
import asyncio, json, time
import redis.asyncio as redis
from core.config import settings
async def main():
    client = redis.from_url(settings.TRADING_REDIS_URL, decode_responses=True)
    rows = []
    async for key in client.scan_iter(match="service:heartbeat:market_data:*", count=100):
        value = await client.get(key)
        if value:
            rows.append(json.loads(value))
    await client.aclose()
    beat = max(rows, key=lambda row: row.get("last_seen_unix", 0))
    details = beat["details"]
    print(json.dumps({"status": beat["status"], "age_s": round(time.time() - beat["last_seen_unix"], 1),
        "ready": details.get("ready_v2_slices"), "demanded": details.get("demanded_v2_slices"),
        "fallback": details.get("v1_fallback_count"), "v2_error": details.get("v2_error_count"),
        "unhealthy": details.get("reported_unhealthy_slices") or []}))
asyncio.run(main())
"""


def target_worker_count(sessions: int) -> int:
    return min(4, max(1, math.ceil(sessions / 13)))


class _AttributedReplica:
    """Record which Query replica served the current call, for per-replica latency."""

    def __init__(self, delegate, name: str) -> None:
        self._delegate = delegate
        self.base_url = delegate.base_url
        self.name = name

    def __getattr__(self, attribute):
        value = getattr(self._delegate, attribute)
        if attribute not in _QUERY_METHODS:
            return value

        async def call(*args, **kwargs):
            _SERVED_BY.set(self.name)
            return await value(*args, **kwargs)

        return call


def _make_target_client(identity, *, queries, stream_targets):
    from qdl_sdk.client import AsyncDataLayerClient
    from qdl_sdk.transport import GrpcStreamTransport, ReplicatedRestQueryTransport, RestQueryTransport

    replicas = [
        _AttributedReplica(
            RestQueryTransport(url, timeout_seconds=15.0, tls=identity.tls,
                               credential_provider=identity.credential),
            url.split("//", 1)[1].split(":", 1)[0],
        )
        for url in queries
    ]
    return AsyncDataLayerClient(
        query_transport=ReplicatedRestQueryTransport(replicas, max_attempts=2, cooldown_seconds=1.0),
        stream_transport=GrpcStreamTransport(stream_targets, tls=identity.tls,
                                             credential_provider=identity.credential),
        consumer_id=identity.consumer_id,
        max_buffer_events=_stream_buffer_bound(identity),
        max_reconnect_attempts=2,
    )


def _error_code(error: BaseException) -> str:
    return _safe_error(error)["code"] or type(error).__name__


async def _startup_retry(call, *, startup, recorder):
    """Retry only a typed, declared startup code with bounded jittered backoff."""

    attempt = 0
    while True:
        try:
            return await call()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            code = _error_code(error)
            attempt += 1
            if code not in startup["retry_codes"] or attempt >= int(startup["retry_max_attempts"]):
                raise
            recorder.counters[f"startup_retry:{code}"] += 1
            recorder.startup_retries += 1
            delay = min(float(startup["retry_max_seconds"]),
                        float(startup["retry_base_seconds"]) * 2 ** (attempt - 1))
            await asyncio.sleep(delay * (0.5 + random.random() / 2))


async def _target_read(client, operation: str, products) -> object:
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
    from qdl.certification.reference_l2_acceptance import reference_evidence

    if operation == "REFERENCE_BATCH":
        references = [_reference_product(product, now_ns=time.time_ns()) for product in products]
        response = await client.reference_batch([item.sdk_requirement for item in references], require_all=True)
        if response.partial or len(response.results) != len(references):
            raise ValueError("PARTIAL_RESULT")
        for reference, result in zip(references, response.results, strict=True):
            reference_evidence(reference, result, observed_at_ns=time.time_ns())
        return response
    (product,) = products
    requirement = sdk_requirement(product)
    if product.feed.value == "BAR":
        # The alpha runtime's latest_bar (execution_alpha
        # runtime/app/alpha_runtime/orchestration/data_layer_v2.py) sends no
        # warmup. The manifest's 10,000-row BAR warmup belongs to history
        # reads, and would make every latest read judge a 10,000-row horizon.
        requirement = replace(requirement, warmup_limit=0)
    response = await client.snapshot(requirement)
    validate_product_view(product, response.data, require_current_quality=True)
    return response


@dataclass(slots=True)
class _TargetStream:
    name: str
    session: int
    alpha_class: str
    product: object
    slow: bool = False
    reconnect: bool = False
    events: int = 0
    observed_events: int = 0
    bytes: int = 0
    errors: int = 0
    control: int = 0
    final_bars: int = 0
    repeats: int = 0
    reconnects: int = 0
    restored: int = 0
    last_offset: int = 0

    def evidence(self) -> dict[str, object]:
        return {
            "name": self.name, "session": self.session, "class": self.alpha_class,
            "venue": self.product.venue, "native_symbol": self.product.native_symbol,
            "feed": self.product.feed.value, "interval": self.product.interval,
            "events": self.events, "observed_events": self.observed_events, "bytes": self.bytes,
            "errors": self.errors, "control_events": self.control, "final_bars": self.final_bars,
            "repeats": self.repeats, "reconnects": self.reconnects, "restored": self.restored,
            "slow_reader": self.slow,
        }


class _TargetRecorder:
    def __init__(self, *, windows: dict[str, tuple[float, float]]) -> None:
        self.windows = windows
        self.latency: dict[str, list[float]] = defaultdict(list)
        self.groups: dict[str, list[float]] = defaultdict(list)
        self.stream_samples = _BoundedSamples(per_group=256)
        self.ledgers: list[dict[str, object]] = []
        self.lag_ms: list[float] = []
        self.behind_ms: list[float] = []
        self.counters: Counter[str] = Counter()
        self.errors: list[dict[str, object]] = []
        self.error_count = 0
        self.cold: list[dict[str, object]] = []
        self.startup_retries = 0
        # Bounded timeline of slow reads and scheduler lag, in wall-clock
        # milliseconds, so a tail can be matched to server-side samples.
        self.outliers: list[list[object]] = []

    def outlier(self, kind: str, value_ms: float, label: str) -> None:
        if len(self.outliers) < 400:
            self.outliers.append([time.time_ns() // 1_000_000, kind, round(value_ms, 1), label])

    def window(self, at: float) -> str:
        for name in ("BURST", "RECONNECT"):
            start, end = self.windows.get(name, (math.inf, math.inf))
            if start <= at < end:
                return name
        return "STEADY"

    def error(self, **evidence) -> None:
        self.error_count += 1
        if len(self.errors) < 40:
            self.errors.append(evidence)

    def read(self, *, operation: str, products, elapsed_ms: float, window: str) -> None:
        product = products[0]
        name = _TARGET_LATENCY_CLASS.get((product.feed.value, operation), f"{product.feed.value}_{operation}")
        value = round(elapsed_ms, 3)
        self.latency[f"{name}|{product.venue}|{window}"].append(value)
        if value > 250.0:
            self.outlier("READ", value, f"{name}|{product.venue}|{_SERVED_BY.get() or '?'}")
        symbols = "+".join(item.native_symbol for item in products)
        self.groups["|".join((name, product.venue, symbols, _SERVED_BY.get() or "?", window))].append(value)


async def _target_poll_loop(*, client, label, operation, products, period, phase, start, end, recorder,
                            stop=None) -> None:
    from qdl.certification.phase3_consumer_load import DeclaredRateTicker, PollLedger

    ticker = DeclaredRateTicker(start=start, period=period, phase=phase, end=end)
    ledger = PollLedger(offered=ticker.offered)
    try:
        while stop is None or not stop.is_set():
            due, missed = ticker.take(time.monotonic())
            ledger.missed += missed
            if due is None:
                break
            delay = due - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
                sent = time.monotonic()
                # Only a wake-up after an actual sleep measures this client's
                # event loop. A tick that is already due because the previous
                # read was slow is server time, reported separately below.
                recorder.lag_ms.append(round((sent - due) * 1000.0, 3))
                if sent - due > 0.1:
                    recorder.outlier("LAG", (sent - due) * 1000.0, label)
            else:
                sent = time.monotonic()
                recorder.behind_ms.append(round((sent - due) * 1000.0, 3))
            ledger.sent += 1
            window = recorder.window(sent)
            _SERVED_BY.set(None)
            sent_ns = time.time_ns()
            try:
                await _target_read(client, operation, products)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                failed_ns = time.time_ns()
                ledger.record_failure(_error_code(error))
                recorder.error(operation=operation, poll=label, window=window,
                               product=_product_evidence(products[0]), error=_safe_error(error),
                               served_by=_SERVED_BY.get(), sent_ns=sent_ns, failed_ns=failed_ns,
                               elapsed_ms=round((failed_ns - sent_ns) / 1e6, 3))
                continue
            done = time.monotonic()
            ledger.completed += 1
            if done > due + period:
                ledger.late += 1
            recorder.read(operation=operation, products=products, elapsed_ms=(done - sent) * 1000.0,
                          window=window)
        # Ticks never reached because the run stopped are missed, not absent.
        ledger.missed += ticker.take(math.inf)[1]
    finally:
        recorder.ledgers.append({"poll": label, **ledger.evidence()})


class _EitherSet:
    def __init__(self, *events) -> None:
        self._events = events

    def is_set(self) -> bool:
        return any(event.is_set() for event in self._events)


class _ReconnectDue:
    """True once the reconnect window opens for a stream selected for it.

    Read on every check: selection happens after setup, when this stream's
    session was already open, so a value captured at open time never fires.
    """

    def __init__(self, stream: "_TargetStream", reconnect_now: asyncio.Event) -> None:
        self._stream = stream
        self._reconnect_now = reconnect_now

    def is_set(self) -> bool:
        return self._stream.reconnect and self._reconnect_now.is_set()


async def _target_stream(*, client, stream: _TargetStream, series, recorder, observing, stop,
                         reconnect_now, established, startup=None) -> None:
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
    from qdl_sdk.models import ControlEvent, StreamEvent
    from qdl_sdk.projection import market_data_view_from_stream

    product = stream.product
    requirement = sdk_requirement(product)
    if product.feed.value == "BAR":
        # The alpha runtime's bar handoff: one final bar, then the stream.
        requirement = replace(requirement, warmup_limit=1)
    resume = False
    slowed = False
    signalled = False
    attempts = 0
    try:
        while not stop.is_set():
            try:
                async with client.warmup_then_stream(requirement, resume_restored_state=resume) as session:
                    if resume:
                        stream.reconnects += 1
                        stream.restored += int(bool(session.state_restored))
                        if not session.state_restored:
                            raise ValueError("signed cursor was not restored on reconnect")
                    template = session.warmup.data[-1]
                    if series is not None:
                        outcome = series.offer(template.payload.open_time_ns)
                        stream.repeats += outcome == "REPEAT"
                    if not signalled:
                        signalled = True
                        established.set_result(None)
                    interrupt = _EitherSet(stop, _ReconnectDue(stream, reconnect_now)) if not resume else stop
                    async for event, delivered_at, delivered_ns in _stream_events_until_stop(
                        session, interrupt, poll_seconds=1.0
                    ):
                        if isinstance(event, ControlEvent):
                            stream.control += 1
                            recorder.counters[f"stream_control:{event.code}"] += 1
                            continue
                        if not isinstance(event, StreamEvent):
                            raise ValueError("stream returned an unknown event type")
                        view = market_data_view_from_stream(event, template=template, requirement=requirement)
                        validate_product_view(product, view, require_current_quality=True)
                        if event.logical_offset <= stream.last_offset:
                            raise ValueError("stream logical offset regressed")
                        stream.last_offset = event.logical_offset
                        session.acknowledge(event)
                        validated_at = time.perf_counter()
                        validated_ns = delivered_ns + int((validated_at - delivered_at) * 1_000_000_000)
                        stream.events += 1
                        byte_size = getattr(event.event, "ByteSize", None)
                        stream.bytes += byte_size() if callable(byte_size) else 0
                        sample = {
                            "group": _product_group(product, "STREAM", "active"),
                            "validation_ms": (validated_at - delivered_at) * 1000.0,
                        }
                        if event.event.received_at_ns > 0:
                            sample["host_receive_to_usable_ms"] = max(
                                0.0, (validated_ns - event.event.received_at_ns) / 1_000_000.0)
                        if series is not None:
                            outcome = series.offer(view.payload.open_time_ns)
                            stream.repeats += outcome == "REPEAT"
                            if view.payload.lifecycle == "FINAL":
                                stream.final_bars += 1
                                sample["source_to_usable_ms"] = max(
                                    0.0, (validated_ns - view.payload.close_time_ns) / 1_000_000.0)
                        elif event.event.source_event_time_ns > 0:
                            sample["source_to_usable_ms"] = max(
                                0.0, (validated_ns - event.event.source_event_time_ns) / 1_000_000.0)
                        if observing.is_set():
                            stream.observed_events += 1
                            recorder.stream_samples.append(sample)
                            if stream.slow and not slowed:
                                slowed = True
                                recorder.counters["slow_reader_pauses"] += 1
                                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # Before the first handoff completes, a typed startup code (a
                # finite lane refusing more work) is retried with bounded backoff,
                # as an alpha would; after it, every error counts.
                attempts += 1
                if (
                    signalled or startup is None
                    or _error_code(error) not in startup["retry_codes"]
                    or attempts >= int(startup["retry_max_attempts"])
                ):
                    raise
                recorder.counters[f"startup_retry:{_error_code(error)}"] += 1
                recorder.startup_retries += 1
                delay = min(float(startup["retry_max_seconds"]),
                            float(startup["retry_base_seconds"]) * 2 ** (attempts - 1))
                await asyncio.sleep(delay * (0.5 + random.random() / 2))
                continue
            if stop.is_set():
                break
            resume = True
    except asyncio.CancelledError:
        raise
    except Exception as error:
        stream.errors += 1
        recorder.error(operation="STREAM", stream=stream.name, product=_product_evidence(product),
                       error=_safe_error(error))
    finally:
        if not signalled:
            established.set_result("FAILED")


def _target_scope(config: dict[str, object]):
    from qdl.certification.phase103_consumer_acceptance import build_manifest_acceptance_scope
    from qdl.certification.phase3_consumer_load import build_target_workload_plan
    from qdl.consumer.manifest import ConsumerManifestLoader
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from qdl.runtime.stable_deployment import StableAcquisitionPlan

    catalog = StableSourceCatalog.load(config["catalog"])
    acquisition = StableAcquisitionPlan.load(config["acquisition"], catalog=catalog)
    paths = {consumer_id: _PHASE3_CONSUMER_MANIFESTS[consumer_id]
             for consumer_id in _TARGET_VENUE_IDENTITY.values()}
    manifests = {consumer_id: ConsumerManifestLoader.load(path) for consumer_id, path in paths.items()}
    scope = build_manifest_acceptance_scope(
        tuple(paths.values()), catalog=catalog, acquisition=acquisition,
        expected_consumer_ids=frozenset(paths), schema="qdl.phase105.consumer-acceptance-scope.v1",
    )
    products_by_consumer = {
        consumer_id: _selected_five(tuple(item for item in scope.products if item.consumer_id == consumer_id))
        for consumer_id in sorted(paths)
    }
    plan = build_target_workload_plan(
        stage=int(config["logical_sessions"]), manifests=manifests,
        products_by_consumer=products_by_consumer, venue_identity=_TARGET_VENUE_IDENTITY,
        instruments={venue: sorted(symbols) for venue, symbols in _FIVE_LIQUID.items()},
    )
    return catalog, acquisition, manifests, products_by_consumer, plan


def _probe_products(products_by_consumer, feed: str, interval: str | None = None):
    values = {}
    for venue, consumer_id in sorted(_TARGET_VENUE_IDENTITY.items()):
        values[venue] = sorted(
            (item for item in products_by_consumer[consumer_id]
             if item.feed.value == feed and (interval is None or item.interval == interval)),
            key=lambda item: item.native_symbol,
        )
        if len(values[venue]) != len(_FIVE_LIQUID[venue]):
            raise ValueError(f"target probe needs {feed} for every {venue} symbol")
    return values


async def _target_rotating_probe(*, client, venue, feed, products, period, start, end, recorder, stop,
                                 phase=0.0):
    """One latency probe per venue and feed, rotating the five symbols.

    Each probe has its own phase, as the session polls do: six probes firing in
    the same millisecond on one identity queue behind each other in Query's
    per-consumer hot lane and measure that queue, not the read (stage 20,
    2026-09-23: TRADE p50 20-270 ms beside QUOTE 15 ms on the same replica).
    """

    from qdl.certification.phase3_consumer_load import DeclaredRateTicker, PollLedger

    ticker = DeclaredRateTicker(start=start, period=period, phase=phase, end=end)
    ledger = PollLedger(offered=ticker.offered)
    index = 0
    try:
        while not stop.is_set():
            due, missed = ticker.take(time.monotonic())
            ledger.missed += missed
            if due is None:
                break
            if due > time.monotonic():
                await asyncio.sleep(due - time.monotonic())
            product = products[index % len(products)]
            index += 1
            sent = time.monotonic()
            ledger.sent += 1
            window = recorder.window(sent)
            _SERVED_BY.set(None)
            sent_ns = time.time_ns()
            try:
                await _target_read(client, "SNAPSHOT", (product,))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                failed_ns = time.time_ns()
                ledger.record_failure(_error_code(error))
                recorder.error(operation="PROBE", window=window, product=_product_evidence(product),
                               error=_safe_error(error), served_by=_SERVED_BY.get(), sent_ns=sent_ns,
                               failed_ns=failed_ns, elapsed_ms=round((failed_ns - sent_ns) / 1e6, 3))
                continue
            ledger.completed += 1
            recorder.read(operation="SNAPSHOT", products=(product,),
                          elapsed_ms=(time.monotonic() - sent) * 1000.0, window=window)
        ledger.missed += ticker.take(math.inf)[1]
    finally:
        recorder.ledgers.append({"poll": f"probe:{venue}:{feed}", **ledger.evidence()})


async def _target_cold(*, client, product, recorder) -> None:
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view

    for rows in (2500, 5000):
        started = time.monotonic()
        entry = {"venue": product.venue, "native_symbol": product.native_symbol, "rows": rows,
                 "window": recorder.window(started)}
        try:
            response = await client.warmup(replace(sdk_requirement(product), warmup_limit=rows))
            entry["returned"] = len(response.data)
            for item in response.data:
                validate_product_view(product, item, require_current_quality=False)
            validate_product_view(product, response.data[-1], require_current_quality=True)
            opens = [item.payload.open_time_ns for item in response.data]
            step = opens[1] - opens[0] if len(opens) > 1 else 0
            entry["continuous"] = step > 0 and all(b - a == step for a, b in zip(opens, opens[1:]))
            if not entry["continuous"]:
                raise ValueError("cold history is not continuous")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            entry["error"] = _safe_error(error)
            recorder.error(operation="COLD", product=_product_evidence(product), rows=rows,
                           error=_safe_error(error))
        entry["ms"] = round((time.monotonic() - started) * 1000.0, 3)
        recorder.cold.append(entry)


async def _target_worker(config: dict[str, object], worker: int, workers: int) -> dict[str, object]:
    from qdl.adapters.intervals import canonical_interval_ms
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
    from qdl.certification.phase3_consumer_load import BarSeries

    budget = config["budget"]
    final = bool(config["final"])
    duration = float(config["duration_seconds"])
    _, _, manifests, products_by_consumer, plan = _target_scope(config)
    identities = _identity_map(config, manifests)
    queries = tuple(config["queries"])
    stream_targets = tuple(config["stream_targets"])
    # Index ``workers`` is the cold role: the 2500/5000-row warmups run in their
    # own process, as a separate alpha would, so parsing thousands of rows
    # cannot stall the hot readers' event loop and inflate their latency.
    cold_role = worker == workers
    mine = [item for item in plan.sessions if (item.ordinal - 1) % workers == worker]
    windows: dict[str, tuple[float, float]] = {}
    recorder = _TargetRecorder(windows=windows)
    stop = asyncio.Event()
    observing = asyncio.Event()
    reconnect_now = asyncio.Event()
    clients = []
    streams: list[_TargetStream] = []
    stream_tasks: list[asyncio.Task] = []
    series_by_session: dict[int, BarSeries] = {}
    setup_started = time.monotonic()
    setup_failures: list[str] = []

    def new_client(consumer_id: str):
        client = _make_target_client(identities[consumer_id], queries=queries, stream_targets=stream_targets)
        clients.append(client)
        return client

    async def establish(session) -> None:
        client = new_client(session.consumer_id)
        session_clients[session.ordinal] = client
        bar = next((item for item in session.streams if item.feed.value == "BAR"), None)
        if bar is not None:
            rows = 2500 if session.ordinal % 2 else 5000
            requirement = replace(sdk_requirement(bar), warmup_limit=rows)
            response = await _startup_retry(lambda: client.warmup(requirement),
                                            startup=budget["startup"], recorder=recorder)
            if len(response.data) != rows:
                raise ValueError("startup BAR warmup coverage differs from the declared maxlen")
            series = BarSeries(maxlen=rows, interval_ns=canonical_interval_ms(bar.interval) * 1_000_000)
            for item in response.data:
                validate_product_view(bar, item, require_current_quality=False)
                series.offer(item.payload.open_time_ns)
            series_by_session[session.ordinal] = series
        for product in session.startup_snapshots:
            started = time.monotonic()
            _SERVED_BY.set(None)
            await _startup_retry(lambda: _target_read(client, "SNAPSHOT", (product,)),
                                 startup=budget["startup"], recorder=recorder)
            recorder.read(operation="SNAPSHOT", products=(product,),
                          elapsed_ms=(time.monotonic() - started) * 1000.0, window="STARTUP")
        for product in session.streams:
            stream = _TargetStream(
                name=f"s{session.ordinal}-{product.feed.value.lower()}-{product.native_symbol}",
                session=session.ordinal, alpha_class=session.alpha_class, product=product,
            )
            streams.append(stream)
            established = asyncio.get_running_loop().create_future()
            stream_tasks.append(asyncio.create_task(_target_stream(
                client=client, stream=stream,
                series=series_by_session.get(session.ordinal) if product.feed.value == "BAR" else None,
                recorder=recorder, observing=observing, stop=stop, reconnect_now=reconnect_now,
                established=established, startup=budget["startup"],
            )))
            outcome = await asyncio.wait_for(established, timeout=float(budget["startup"]["max_setup_seconds"]))
            if outcome is not None:
                failed_streams.add(stream.name)
                raise RuntimeError("stream handoff failed")

    session_clients: dict[int, object] = {}
    failed_streams: set[str] = set()
    poll_tasks: list[asyncio.Task] = []
    setup_seconds: float | None = None
    start: float | None = None
    try:
        results = await asyncio.gather(*(establish(item) for item in mine), return_exceptions=True)
        for session, result in zip(mine, results, strict=True):
            if isinstance(result, BaseException):
                setup_failures.append(f"s{session.ordinal}")
                recorder.error(operation="SETUP", session=session.ordinal, error=_safe_error(result))
        setup_seconds = round(time.monotonic() - setup_started, 3)
        # Start-up warmups (up to 5,000 rows per BAR session) leave a large
        # heap; freezing the survivors keeps later full collections from
        # sweeping it inside the measured window and stalling this client.
        gc.collect()
        gc.freeze()
        final_cfg = budget["final"]
        if final:
            # Only streams whose handoff completed can be slowed or reconnected.
            ordered = sorted((item for item in streams if item.errors == 0 and item.name not in failed_streams),
                             key=lambda item: item.name)
            live = [item for item in ordered if item.product.feed.value in {"QUOTE", "TRADE"}]
            if worker == 0 and live:
                live[0].slow = True
            for index, item in enumerate(ordered):
                item.reconnect = index % round(1 / float(final_cfg["reconnect"]["fraction"])) == 0
        print(json.dumps({"ready": True, "worker": worker, "setup_seconds": setup_seconds,
                          "setup_failures": setup_failures}), flush=True)
        line = await asyncio.get_running_loop().run_in_executor(None, sys.stdin.readline)
        if not line.startswith("GO "):
            raise RuntimeError("target worker did not receive a start signal")
        start = time.monotonic() + (float(line.split()[1]) - time.time())
        end = start + duration
        if final:
            burst = final_cfg["burst"]
            burst_start = start + duration * float(burst["at_fraction"])
            windows["BURST"] = (burst_start, burst_start + float(burst["seconds"]))
            reconnect_start = start + duration * float(final_cfg["reconnect"]["at_fraction"])
            windows["RECONNECT"] = (reconnect_start, reconnect_start + 15.0)
        if start > time.monotonic():
            await asyncio.sleep(start - time.monotonic())
        observing.set()
        rng = random.Random(1000 + worker)
        for session in mine:
            if f"s{session.ordinal}" in setup_failures:
                continue
            client = session_clients[session.ordinal]
            for number, poll in enumerate(session.polls):
                phase = rng.random() * poll.period_seconds
                label = f"s{session.ordinal}:{session.alpha_class}:{number}"
                poll_tasks.append(asyncio.create_task(_target_poll_loop(
                    client=client, label=label, operation=poll.operation, products=poll.products,
                    period=poll.period_seconds, phase=phase, start=start, end=end, recorder=recorder,
                )))
                if final and session.ordinal % 4 == 0 and poll.period_seconds <= 1.0:
                    burst_start, burst_end = windows["BURST"]
                    poll_tasks.append(asyncio.create_task(_target_poll_loop(
                        client=client, label=f"{label}:burst", operation=poll.operation,
                        products=poll.products, period=poll.period_seconds,
                        phase=(phase + poll.period_seconds / 2) % poll.period_seconds,
                        start=burst_start, end=burst_end, recorder=recorder,
                    )))
        if worker == 0 or cold_role:
            probe_clients = {venue: new_client(consumer_id) for venue, consumer_id in _TARGET_VENUE_IDENTITY.items()}
        if worker == 0:
            probes = budget["probes"]
            for feed in probes["feeds"]:
                by_venue = _probe_products(products_by_consumer, feed, "1m" if feed == "BAR" else None)
                for venue, products in by_venue.items():
                    period = float(probes["period_seconds"])
                    poll_tasks.append(asyncio.create_task(_target_rotating_probe(
                        client=probe_clients[venue], venue=venue, feed=feed, products=products,
                        period=period, start=start + 1.0, end=end,
                        recorder=recorder, stop=stop, phase=rng.random() * period,
                    )))
        if cold_role:
            async def cold_later():
                await asyncio.sleep(max(0.0, start + duration * 0.3 - time.monotonic()))
                cold = _probe_products(products_by_consumer, "BAR", "1m")
                await asyncio.gather(*(
                    _target_cold(client=probe_clients[venue], product=products[0], recorder=recorder)
                    for venue, products in sorted(cold.items())
                ))

            poll_tasks.append(asyncio.create_task(cold_later()))
        if final:
            async def reconnect_later():
                await asyncio.sleep(max(0.0, windows["RECONNECT"][0] - time.monotonic()))
                reconnect_now.set()

            poll_tasks.append(asyncio.create_task(reconnect_later()))
        await asyncio.sleep(max(0.0, end - time.monotonic()))
        await asyncio.wait_for(asyncio.gather(*poll_tasks, return_exceptions=True), timeout=60.0)
    except Exception as error:
        recorder.error(operation="WORKER", error=_safe_error(error))
        if setup_seconds is None:
            setup_seconds = round(time.monotonic() - setup_started, 3)
    finally:
        stop.set()
        observing.set()
        for task in poll_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*poll_tasks, *stream_tasks, return_exceptions=True)
        await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)
    leaked = [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()]
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "worker": worker,
        "sessions": [item.ordinal for item in mine],
        "setup": {"seconds": setup_seconds, "retries": recorder.startup_retries, "failures": setup_failures},
        "latency_series": dict(recorder.latency),
        "latency_groups": dict(recorder.groups),
        "stream_samples": list(recorder.stream_samples),
        "stream_samples_seen": {"|".join(key): count for key, count in recorder.stream_samples.seen.items()},
        "poll_ledgers": recorder.ledgers,
        "scheduler_lag_ms": recorder.lag_ms,
        "started_behind_ms": recorder.behind_ms,
        "streams": [item.evidence() for item in streams],
        "bar_series": {str(key): {"length": len(value), "appended": value.appended, "repeats": value.repeats}
                       for key, value in series_by_session.items()},
        "cold": recorder.cold,
        "outliers": recorder.outliers,
        "counters": dict(recorder.counters),
        "errors": recorder.errors,
        "error_count": recorder.error_count,
        "leaked_tasks": len(leaked),
        "fault_windows": ({} if start is None else
                          {name: [round(a - start, 3), round(b - start, 3)] for name, (a, b) in windows.items()}),
        "cpu_seconds": round(usage.ru_utime + usage.ru_stime, 3),
        "max_rss_kib": usage.ru_maxrss,
    }


def _merge_target(results: list[dict[str, object]], *, plan, final: bool) -> dict[str, object]:
    from qdl.certification.phase3_consumer_load import nearest_rank

    latency: dict[str, list[float]] = defaultdict(list)
    groups: dict[str, list[float]] = defaultdict(list)
    lag: list[float] = []
    behind: list[float] = []
    stream_samples: list[dict[str, object]] = []
    for result in results:
        for key, values in result["latency_series"].items():
            latency[key].extend(values)
        for key, values in result["latency_groups"].items():
            groups[key].extend(values)
        lag.extend(result["scheduler_lag_ms"])
        behind.extend(result.get("started_behind_ms", []))
        stream_samples.extend(result["stream_samples"])
    streams = [item for result in results for item in result["streams"]]
    windows: dict[str, object] = {}
    if final:
        worker0 = next((item for item in results if item["worker"] == 0), {})
        windows = {
            "BURST": worker0.get("fault_windows", {}).get("BURST"),
            "SLOW_READER": sum(item["counters"].get("slow_reader_pauses", 0) for item in results),
            "RECONNECT": sum(item["reconnects"] for item in streams),
            "reconnect_restored": sum(item["restored"] for item in streams),
        }
    stream_summary = []
    by_group: dict[tuple[str, ...], list[dict[str, object]]] = defaultdict(list)
    for sample in stream_samples:
        by_group[tuple(sample["group"])].append(sample)
    for key, values in sorted(by_group.items()):
        stream_summary.append({
            "venue": key[2], "native_symbol": key[3], "feed": key[4], "interval": key[5] or None,
            "reservoir_samples": len(values),
            "validation": _percentiles([item["validation_ms"] for item in values if "validation_ms" in item]),
            "host_receive_to_usable": _percentiles(
                [item["host_receive_to_usable_ms"] for item in values if "host_receive_to_usable_ms" in item]),
            "source_or_close_to_usable": _percentiles(
                [item["source_to_usable_ms"] for item in values if "source_to_usable_ms" in item]),
        })
    return {
        "schema": "qdl.phase3.target-load.v1",
        "stage": plan.stage,
        "final": final,
        "class_counts": dict(zip(("CANDLE", "REALTIME", "GRID", "MULTI"), plan.class_counts)),
        "planned_streams": plan.stream_count,
        "hot_requests_per_second": plan.hot_requests_per_second,
        "identity_demands": [
            {"consumer_id": item.consumer_id, "sessions": item.sessions,
             "required_requests_per_minute": item.required_requests_per_minute,
             "required_streams": item.required_streams, "quota_needed": item.quota_needed,
             "streams_needed": item.streams_needed,
             "sealed_requests_per_minute": item.sealed_requests_per_minute,
             "sealed_max_streams": item.sealed_max_streams}
            for item in plan.demands
        ],
        "workers": len(results),
        "setup": {
            "seconds": max(item["setup"]["seconds"] for item in results),
            "retries": sum(item["setup"]["retries"] for item in results),
            "failures": [name for item in results for name in item["setup"]["failures"]],
        },
        "latency_series": dict(latency),
        "latency_groups": [
            {"group": key, **_percentiles(values)} for key, values in sorted(groups.items())
        ],
        "poll_ledgers": [item for result in results for item in result["poll_ledgers"]],
        "scheduler_lag_ms": {"n": len(lag), "p50": nearest_rank(lag, 0.50), "p95": nearest_rank(lag, 0.95),
                             "p99": nearest_rank(lag, 0.99), "max": max(lag) if lag else None},
        "started_behind_ms": {"n": len(behind), "p50": nearest_rank(behind, 0.50),
                              "p99": nearest_rank(behind, 0.99), "max": max(behind) if behind else None},
        "streams": streams,
        "stream_latency": stream_summary,
        "bar_series": {key: value for result in results for key, value in result["bar_series"].items()},
        "cold": [item for result in results for item in result["cold"]],
        "outliers": sorted(entry for result in results for entry in result.get("outliers", [])),
        "counters": dict(sum((Counter(item["counters"]) for item in results), Counter())),
        "errors": [item for result in results for item in result["errors"]][:40],
        "error_count": sum(item["error_count"] for item in results),
        "leaked_tasks": sum(item["leaked_tasks"] for item in results),
        "fault_windows": windows,
        "worker_cpu_seconds": [item["cpu_seconds"] for item in results],
        "worker_max_rss_kib": [item["max_rss_kib"] for item in results],
        "provider_connections": 0,
        "v1_fallback_calls": 0,
        "order_actions": 0,
        "secret_values_recorded": False,
    }


async def run_target_inside() -> dict[str, object]:
    """Spawn the worker processes, start them together, merge their evidence."""

    config = _inside_config()
    _, _, _, _, plan = _target_scope(config)
    workers = target_worker_count(int(config["logical_sessions"]))
    driver = str(Path(__file__).resolve())
    # One process per session group plus the cold role (index ``workers``).
    processes = [
        await asyncio.create_subprocess_exec(
            sys.executable, "-B", driver, "--inside-worker", str(index), str(workers),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            limit=64 * 1024 * 1024,
        )
        for index in range(workers + 1)
    ]
    ready = []
    results = []
    setup_deadline = float(config["budget"]["startup"]["max_setup_seconds"]) + 60.0
    try:
        for process in processes:
            line = await asyncio.wait_for(process.stdout.readline(), timeout=setup_deadline)
            ready.append(json.loads(line))
        go = time.time() + 2.0
        for process in processes:
            process.stdin.write(f"GO {go}\n".encode())
            await process.stdin.drain()
        for process in processes:
            stdout, _ = await asyncio.wait_for(process.communicate(),
                                               timeout=float(config["duration_seconds"]) + 240.0)
            lines = [line for line in stdout.decode().splitlines() if line.startswith("{")]
            results.append(json.loads(lines[-1]))
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()
    merged = _merge_target(results, plan=plan, final=bool(config["final"]))
    merged["worker_ready"] = ready
    merged["status"] = "COMPLETE" if all(process.returncode == 0 for process in processes) else "WORKER_FAILED"
    return merged


def run_target_worker(index: int, count: int) -> int:
    config = _inside_config()
    result = asyncio.run(_target_worker(config, index, count))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
    return 0


async def run_target_matrix_inside() -> dict[str, object]:
    """Every read product of the stage-50 profile once through each replica.

    Also the bounded longer-interval sample: one 500-row 5m/15m/1h BAR warmup
    per venue and replica. Streams are exercised by the stages themselves.
    """

    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
    from qdl_sdk.client import AsyncDataLayerClient
    from qdl_sdk.transport import GrpcStreamTransport, RestQueryTransport

    config = _inside_config()
    _, _, manifests, products_by_consumer, plan = _target_scope(config)
    identities = _identity_map(config, manifests)
    reads: dict[tuple[str, tuple[object, ...]], tuple[str, tuple[object, ...]]] = {}
    for session in plan.sessions:
        for poll in session.polls:
            for product in poll.products:
                reads[(poll.operation, (product.identity,))] = (poll.operation, (product,))
            if len(poll.products) > 1:
                reads[(poll.operation, tuple(item.identity for item in poll.products))] = (poll.operation, poll.products)
        for product in session.startup_snapshots:
            reads[("SNAPSHOT", (product.identity,))] = ("SNAPSHOT", (product,))
    for feed in config["budget"]["probes"]["feeds"]:
        for products in _probe_products(products_by_consumer, feed, "1m" if feed == "BAR" else None).values():
            for product in products:
                reads[("SNAPSHOT", (product.identity,))] = ("SNAPSHOT", (product,))
    results = []
    for url in config["queries"]:
        replica = url.split("//", 1)[1].split(":", 1)[0]
        clients = {}
        for consumer_id, identity in identities.items():
            clients[consumer_id] = AsyncDataLayerClient(
                query_transport=RestQueryTransport(url, timeout_seconds=15.0, tls=identity.tls,
                                                   credential_provider=identity.credential),
                stream_transport=GrpcStreamTransport(config["stream_targets"], tls=identity.tls,
                                                     credential_provider=identity.credential),
                consumer_id=consumer_id, max_buffer_events=_stream_buffer_bound(identity),
                max_reconnect_attempts=0,
            )
        try:
            for (operation, _), (_, products) in sorted(reads.items(), key=lambda item: str(item[0])):
                started = time.monotonic()
                entry = {"replica": replica, "operation": operation,
                         "venue": products[0].venue, "feed": products[0].feed.value,
                         "symbols": [item.native_symbol for item in products]}
                try:
                    await _target_read(clients[products[0].consumer_id], operation, products)
                    entry["status"] = "PASS"
                except Exception as error:
                    entry.update(status="FAIL", error=_safe_error(error))
                entry["ms"] = round((time.monotonic() - started) * 1000.0, 3)
                results.append(entry)
            for venue, consumer_id in sorted(_TARGET_VENUE_IDENTITY.items()):
                for interval in ("5m", "15m", "1h"):
                    product = _probe_products(products_by_consumer, "BAR", interval)[venue][0]
                    started = time.monotonic()
                    entry = {"replica": replica, "operation": "WARMUP_500", "venue": venue,
                             "feed": "BAR", "interval": interval, "symbols": [product.native_symbol]}
                    try:
                        response = await clients[consumer_id].warmup(
                            replace(sdk_requirement(product), warmup_limit=500))
                        opens = [item.payload.open_time_ns for item in response.data]
                        for item in response.data:
                            validate_product_view(product, item, require_current_quality=False)
                        if len(opens) != 500 or len(set(opens)) != 500 or opens != sorted(opens):
                            raise ValueError("longer-interval sample is not 500 ordered distinct opens")
                        entry["status"] = "PASS"
                    except Exception as error:
                        entry.update(status="FAIL", error=_safe_error(error))
                    entry["ms"] = round((time.monotonic() - started) * 1000.0, 3)
                    results.append(entry)
        finally:
            await asyncio.gather(*(client.close() for client in clients.values()), return_exceptions=True)
    failed = [item for item in results if item["status"] != "PASS"]
    return {
        "schema": "qdl.phase3.target-matrix.v1",
        "status": "PASS" if not failed else "FAIL",
        "reads": len(results),
        "failed": failed,
        "results": results,
        "order_actions": 0,
        "secret_values_recorded": False,
    }


_KN4_HISTORY_ROWS = (2_500, 5_000, 10_000)
_KN4_BATCH_SIZES = (1, 8, 16, 32, 50)
_KN4_HISTORY_INTERVALS = ("1m", "1h", "4h", "1d")
_KN4_HANDOFF_FEEDS = (("TRADE", None), ("QUOTE", None), ("BOOK_DELTA", None), ("BAR", "1m"))


async def _kn4_raw(identity, url: str, method: str, path: str, *, requirement=None, consumer_id: str,
                   params=None, body=None) -> tuple[int, dict]:
    """One public HTTP operation the SDK does not wrap, with the same mTLS
    identity and JWT the SDK sends (history, gaps, readiness)."""

    import httpx

    token = await identity.credential.get_token()
    purpose = "INTERNAL_ALPHA"
    headers = {"Authorization": f"Bearer {token}", "X-QDL-Consumer-ID": consumer_id,
               "X-QDL-Purpose": purpose}
    async with httpx.AsyncClient(base_url=url, verify=identity.tls.ssl_context(), timeout=60.0) as client:
        response = await client.request(method, path, params=params, json=body, headers=headers)
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    return response.status_code, payload if isinstance(payload, dict) else {"items": payload}


def _kn4_http_result(path: str, status: int, payload: dict) -> dict:
    result = {"http": status, "code": payload.get("code")}
    if path != "/v2/data-quality/gaps":
        return result
    complete = (status == 200 and payload.get("schema") == "qdl.data-quality.gaps.v2"
                and isinstance(payload.get("items"), list))
    result.update(scan_complete=complete)
    if not complete:
        result.update(status="FAIL", contract_safe=status in {206, 409, 503},
                      error="GAP_SCAN_NOT_COMPLETE")
    return result


def _kn4_opens_contiguous(opens: list[int], interval_ns: int) -> bool:
    return bool(opens) and all(later - earlier == interval_ns for earlier, later in zip(opens, opens[1:]))


_KN4_HANDOFF_WINDOW_S = 90.0
_KN4_HANDOFF_CONCURRENCY = 4
_KN4_HANDOFF_DRAIN_S = 30.0


def _kn4_probe_module():
    """The KN-2 probe's certified oracle/judge (one owner of exactness)."""

    import importlib.util

    beside = Path(__file__).resolve().with_name("kn_native_slice_probe.py")  # /driver inside the client
    path = beside if beside.is_file() else ROOT / "scripts/kn_native_slice_probe.py"
    spec = importlib.util.spec_from_file_location("kn_native_slice_probe", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(spec.name, module)
    spec.loader.exec_module(module)
    return module


def kn4_handoff_verdict(probe, *, product, watermark: int, delivered: list[int],
                        records: list[tuple[int, int, bytes]], ends: dict[int, int],
                        opened_ns: int, closed_ns: int) -> dict[str, object]:
    """One handoff over a fixed window (D41): every canonical record of the
    product after the snapshot watermark and before the window boundary is
    the expected sequence; the stream's delivered offsets inside the boundary
    are judged by the KN-2 rules (lossless records may not be missing,
    latest-state/lifecycle records only when superseded, no duplicate, no
    reordering, nothing unexpected). A product without a record in the
    window is `quiet_live`, reported separately, never an event PASS."""

    partitions = {partition for partition, _offset, _value in records}
    if len(partitions) > 1:
        raise ValueError("one physical key spans several canonical partitions")
    boundary = ends[next(iter(partitions))] if partitions else None
    row = {"requirement": product.requirement, "feed": product.feed.value, "interval": product.interval}
    expected = probe.expected_delivery(row, records, watermark, opened_ns, closed_ns)
    inside = [offset for offset in delivered if boundary is None or offset < boundary]
    counts = probe.judge_subscription(expected, inside)
    coverage = probe.coverage_class(expected, inside)
    if not expected and not inside:
        coverage = "quiet_live"
    failed = any(counts[name] for name in ("duplicates", "out_of_order", "unexpected", "missing_lossless",
                                           "unsuperseded_drops", "delivered_filtered"))
    failed = failed or coverage == "expected_but_missing" or (delivered and delivered[0] <= watermark)
    return {"status": "FAIL" if failed else "PASS", "watermark": watermark, "boundary": boundary,
            "expected": len(expected), "delivered_inside": len(inside),
            "delivered_after_boundary": len(delivered) - len(inside), "coverage": coverage, **counts}


async def _kn4_fixed_window_handoffs(*, config, identities, products_by_consumer, client_for):
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement

    probe = _kn4_probe_module()
    oracle = config.get("oracle_bootstrap")
    cases = []
    for url in config["queries"]:
        replica = url.split("//", 1)[1].split(":", 1)[0]
        for venue, consumer_id in sorted(_TARGET_VENUE_IDENTITY.items()):
            for feed, interval in _KN4_HANDOFF_FEEDS:
                try:
                    product = _probe_products(products_by_consumer, feed, interval)[venue][0]
                except ValueError:
                    continue
                cases.append((url, replica, venue, consumer_id, product))
    if not oracle:
        return [{"replica": replica, "venue": venue, "feed": product.feed.value, "symbol": product.native_symbol,
                 "status": "FAIL", "error": "no isolated oracle broker: the handoff window cannot be judged"}
                for _url, replica, venue, _consumer_id, product in cases]
    # A handoff proves the snapshot -> stream boundary, not client capacity:
    # waves of a few concurrent sessions, each judged in its own fixed window
    # (16 busy live streams on the 1-CPU client overflowed their buffers).
    results: list[dict[str, object]] = []
    for start in range(0, len(cases), _KN4_HANDOFF_CONCURRENCY):
        results.extend(await _kn4_handoff_wave(cases[start:start + _KN4_HANDOFF_CONCURRENCY], probe=probe,
                                               oracle=oracle, config=config, client_for=client_for))
    return results


async def _kn4_handoff_wave(cases, *, probe, oracle, config, client_for):
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement

    # One client per session: a handoff session must not share its client's
    # stream with another session.
    clients = {index: client_for(url, consumer_id)
               for index, (url, _replica, _venue, consumer_id, _product) in enumerate(cases)}
    opened_ns = time.time_ns()
    close_at = time.monotonic() + _KN4_HANDOFF_WINDOW_S
    closed = asyncio.Event()
    state: dict[int, dict[str, object]] = {}

    async def run(index, url, consumer_id, product):
        entry = state.setdefault(index, {"delivered": [], "watermark": None})
        requirement = replace(sdk_requirement(product), warmup_limit=1 if product.feed.value == "BAR" else 0)
        async with clients[index].warmup_then_stream(requirement) as session:
            entry["watermark"] = session.warmup.watermark_offset
            # A pump owns the stream read; the controller only waits on the
            # queue. Cancelling a pending gRPC read (wait_for on __anext__)
            # ends the call, so a quiet stream died at its first 1 s timeout.
            queue: asyncio.Queue = asyncio.Queue()
            ended = object()

            async def pump():
                try:
                    while True:
                        await queue.put(await session.__anext__())
                except StopAsyncIteration:
                    entry["stream_ended"] = True
                except Exception as error:  # noqa: BLE001 - typed stream failure, judged below
                    entry["stream_error"] = _safe_error(error)
                finally:
                    await queue.put(ended)

            reader = asyncio.create_task(pump())
            try:
                drain_until = None
                while True:
                    if closed.is_set() and drain_until is None:
                        drain_until = time.monotonic() + _KN4_HANDOFF_DRAIN_S
                    limit = (close_at if drain_until is None else drain_until) - time.monotonic()
                    if limit <= 0:
                        if drain_until is None:
                            await closed.wait()
                            continue
                        return
                    if drain_until is not None and entry.get("target") is not None and (
                            entry["delivered"] and entry["delivered"][-1] >= entry["target"]):
                        return
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=min(limit, 1.0))
                    except asyncio.TimeoutError:
                        continue
                    if event is ended:
                        if drain_until is None:
                            await closed.wait()
                        return
                    if hasattr(event, "logical_offset"):
                        entry["delivered"].append(event.logical_offset)
                        session.acknowledge(event)
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)

    from qdl.runtime.stable_catalog import StableSourceCatalog

    catalog = StableSourceCatalog.load(config["catalog"])
    physical = {binding.binding_id: binding.partition_key for binding in catalog.bindings}
    tasks = [asyncio.create_task(run(index, url, consumer_id, product))
             for index, (url, _replica, _venue, consumer_id, product) in enumerate(cases)]
    try:
        await asyncio.sleep(max(0.0, close_at - time.monotonic()))
        # The fixed boundary: every canonical partition's end at the close.
        ends = await asyncio.to_thread(probe.canonical_end_offsets, oracle, "md.canonical.v2")
        closed_ns = time.time_ns()
        # Start well before the window so every record after any snapshot
        # watermark is in the oracle (watermarks are canonical offsets).
        wanted = {physical.get(product.binding_id, "") for _u, _r, _v, _c, product in cases}
        records = await asyncio.to_thread(probe.kafka_oracle_window, oracle, "md.canonical.v2",
                                          since_ms=opened_ns // 1_000_000 - 120_000, ends=ends, keys=wanted)
        for index, (_url, _replica, _venue, _consumer_id, product) in enumerate(cases):
            entry = state.setdefault(index, {"delivered": [], "watermark": None})
            entry["records"] = records.get(physical.get(product.binding_id, ""), [])
            inside = [offset for _partition, offset, _value in entry["records"]]
            entry["target"] = max(inside) if inside else None
        closed.set()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*(client.close() for client in clients.values()), return_exceptions=True)
    results = []
    for index, ((_url, replica, venue, _consumer_id, product), outcome) in enumerate(zip(cases, outcomes)):
        entry = state.get(index, {})
        base = {"replica": replica, "venue": venue, "feed": product.feed.value,
                "interval": product.interval, "symbol": product.native_symbol,
                "window_s": _KN4_HANDOFF_WINDOW_S}
        if isinstance(outcome, BaseException) or entry.get("watermark") is None:
            results.append({**base, "status": "FAIL",
                            "error": _safe_error(outcome) if isinstance(outcome, BaseException)
                            else "no snapshot watermark"})
            continue
        try:
            verdict = kn4_handoff_verdict(
                probe, product=product, watermark=int(entry["watermark"]), delivered=list(entry["delivered"]),
                records=list(entry.get("records", [])), ends=ends, opened_ns=opened_ns, closed_ns=closed_ns)
            if entry.get("stream_ended"):
                verdict["stream_ended"] = True
            if entry.get("stream_error"):
                verdict["stream_error"] = entry["stream_error"]
                verdict["status"] = "FAIL"
            results.append({**base, **verdict})
        except Exception as error:  # noqa: BLE001 - a judge failure is a FAIL, never a skip
            results.append({**base, "status": "FAIL", "error": _safe_error(error)})
    return results


async def run_kn4_matrix_inside() -> dict[str, object]:
    """KN-4 K4-T01..T05/T08 read-plane matrix on both shadow Query replicas.

    The v2.1.1 target matrix (every stage-50 read product once per replica,
    500-row longer-interval warmups) plus: the 2,500/5,000/10,000-row history
    ladder with contiguity and replica parity at the same watermark; strict
    warmup batches of 1/8/16/32/50; snapshot -> stream handoff through the
    real SDK on the paired shadow Stream (offsets strictly after the
    watermark, acknowledged); the freshness verdict evaluated per read (a
    1 ms bound refuses the same product a normal bound serves); and every
    public HTTP operation of the KN-1 inventory. Payload-free evidence.
    """

    from qdl.adapters.intervals import canonical_interval_ms
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
    from qdl_sdk.client import AsyncDataLayerClient
    from qdl_sdk.errors import DataLayerError
    from qdl_sdk.transport import GrpcStreamTransport, RestQueryTransport

    target = await run_target_matrix_inside()
    config = _inside_config()
    _, _, manifests, products_by_consumer, _plan = _target_scope(config)
    identities = _identity_map(config, manifests)
    sections: dict[str, list[dict[str, object]]] = {
        "history": [], "parity": [], "batch": [], "handoff": [], "freshness": [], "http": []}

    def client_for(url: str, consumer_id: str):
        identity = identities[consumer_id]
        return AsyncDataLayerClient(
            query_transport=RestQueryTransport(url, timeout_seconds=60.0, tls=identity.tls,
                                               credential_provider=identity.credential),
            stream_transport=GrpcStreamTransport(config["stream_targets"], tls=identity.tls,
                                                 credential_provider=identity.credential),
            consumer_id=consumer_id, max_buffer_events=_stream_buffer_bound(identity),
            max_reconnect_attempts=2,
        )

    async def timed(section: str, entry: dict[str, object], work) -> dict[str, object]:
        started = time.monotonic()
        try:
            entry.update(await work())
            entry.setdefault("status", "PASS")
        except Exception as error:
            entry.update(status="FAIL", error=_safe_error(error))
        entry["ms"] = round((time.monotonic() - started) * 1000.0, 3)
        sections[section].append(entry)
        return entry

    histories: dict[tuple[str, str, int], dict[str, tuple[int, str]]] = {}
    for url in config["queries"]:
        replica = url.split("//", 1)[1].split(":", 1)[0]
        clients = {consumer_id: client_for(url, consumer_id) for consumer_id in identities}
        try:
            # K4-T03: the history ladder.
            for interval in _KN4_HISTORY_INTERVALS:
                try:
                    by_venue = _probe_products(products_by_consumer, "BAR", interval)
                except ValueError:
                    continue
                for venue, products in sorted(by_venue.items()):
                    product = products[0]
                    interval_ns = canonical_interval_ms(interval) * 1_000_000
                    for rows in _KN4_HISTORY_ROWS:
                        async def ladder(product=product, rows=rows, interval_ns=interval_ns):
                            response = await clients[product.consumer_id].warmup(
                                replace(sdk_requirement(product), warmup_limit=rows))
                            opens = [item.payload.open_time_ns for item in response.data]
                            for item in response.data:
                                validate_product_view(product, item, require_current_quality=False)
                            if not _kn4_opens_contiguous(opens, interval_ns):
                                raise ValueError("history window is not contiguous ordered distinct opens")
                            if len(opens) > rows or (len(opens) < rows and response.coverage != "FULL"):
                                raise ValueError("history returned more rows than asked or a short partial window")
                            digest = hashlib.sha256(json.dumps(
                                [item.payload.model_dump(mode="json") for item in response.data],
                                sort_keys=True).encode()).hexdigest()
                            histories.setdefault((venue, interval, rows), {})[replica] = (
                                response.watermark_offset, digest)
                            return {"returned": len(opens), "coverage": response.coverage,
                                    "watermark": response.watermark_offset,
                                    "short_by_history_start": len(opens) < rows}
                        await timed("history", {"replica": replica, "venue": venue, "interval": interval,
                                                "rows": rows, "symbol": product.native_symbol}, ladder)
            # K4-T02: strict batches (one identity per venue, BAR products).
            for venue, consumer_id in sorted(_TARGET_VENUE_IDENTITY.items()):
                bars = sorted((item for item in products_by_consumer[consumer_id] if item.feed.value == "BAR"),
                              key=lambda item: (item.native_symbol, item.interval or ""))
                for size in _KN4_BATCH_SIZES:
                    async def batch(bars=bars, size=size, consumer_id=consumer_id):
                        if len(bars) < size:
                            raise ValueError(f"only {len(bars)} BAR products for a batch of {size}")
                        response = await clients[consumer_id].warmup_batch(
                            [replace(sdk_requirement(item), warmup_limit=100) for item in bars[:size]],
                            require_all=True)
                        statuses = Counter(item.status for item in response.results)
                        if response.partial or len(response.results) != size or response.error_count:
                            raise ValueError(f"strict batch was partial: {dict(statuses)}")
                        watermarks = {item.data.watermark_offset for item in response.results if item.data}
                        return {"items": size, "statuses": dict(statuses),
                                "distinct_item_watermarks": len(watermarks)}
                    await timed("batch", {"replica": replica, "venue": venue, "size": size}, batch)
            # K4-T04: the verdict is evaluated per read, never cached. A strict
            # (BLOCK) BAR is refused under a 1 ms bound; a quiet-policy
            # (ON_CHANGE + OBSERVE) QUOTE stays served but its event recency
            # turns STALE with LAST_EVENT_STALE - the manifest's own
            # contract - while the manifest bound reads LIVE.
            for feed, interval in (("QUOTE", None), ("BAR", "1m")):
                for venue, products in sorted(_probe_products(products_by_consumer, feed, interval).items()):
                    product = products[0]

                    async def freshness(product=product):
                        client = clients[product.consumer_id]
                        requirement = replace(sdk_requirement(product), warmup_limit=0)
                        served = await client.snapshot(requirement)
                        validate_product_view(product, served.data, require_current_quality=True)
                        normal = served.data.quality.event_recency_state
                        try:
                            strict = await client.snapshot(replace(requirement, max_freshness_ms=1))
                        except DataLayerError as error:
                            if error.code != "DATA_STALE":
                                raise
                            return {"manifest_bound": normal, "one_ms_bound": error.code}
                        quality = strict.data.quality
                        if quality.event_recency_state != "STALE" or "LAST_EVENT_STALE" not in quality.flags:
                            raise ValueError("a 1 ms bound neither refused nor marked the event stale")
                        return {"manifest_bound": normal, "one_ms_bound": "SERVED_EVENT_STALE"}
                    await timed("freshness", {"replica": replica, "venue": venue, "feed": feed,
                                              "symbol": product.native_symbol}, freshness)
            # K4-T01: every public HTTP operation of the KN-1 inventory.
            consumer_id = _TARGET_VENUE_IDENTITY["BINANCE"]
            identity = identities[consumer_id]
            quote = _probe_products(products_by_consumer, "QUOTE")["BINANCE"][0]
            bar = _probe_products(products_by_consumer, "BAR", "1m")["BINANCE"][0]
            # The exact manifest requirement (its recency/session fields are
            # part of the entitlement match), as the SDK sends it.
            common = {**replace(sdk_requirement(bar), warmup_limit=50).query_params()}
            quote_params = {**replace(sdk_requirement(quote), warmup_limit=0).query_params()}
            operations = (
                ("GET /v2/instruments", "GET", "/v2/instruments", {"limit": 5, "consumer_grade": "ALPHA"}, None, {200}),
                ("GET /v2/instruments/{identity}", "GET", f"/v2/instruments/{quote.instrument_uid}",
                 {"consumer_grade": "ALPHA"}, None, {200}),
                ("GET /v2/market-data/{uid}/snapshot", "GET", f"/v2/market-data/{quote.instrument_uid}/snapshot",
                 quote_params, None, {200}),
                ("GET /v2/feeds/{uid}/status", "GET", f"/v2/feeds/{quote.instrument_uid}/status",
                 quote_params, None, {200}),
                ("GET /v2/market-data/{uid}/warmup", "GET", f"/v2/market-data/{bar.instrument_uid}/warmup",
                 common, None, {200}),
                ("GET /v2/market-data/{uid}/history", "GET", f"/v2/market-data/{bar.instrument_uid}/history",
                 common, None, {200}),
                ("GET /v2/system/readiness", "GET", "/v2/system/readiness", None, None, {200}),
                ("GET /v2/data-quality/gaps", "GET", "/v2/data-quality/gaps", None, None, {200, 206, 409, 503}),
            )
            for name, method, path, params, body, accepted in operations:
                async def http(method=method, path=path, params=params, body=body, accepted=accepted):
                    status, payload = await _kn4_raw(identity, url, method, path, consumer_id=consumer_id,
                                                     params=params, body=body)
                    if status not in accepted:
                        raise ValueError(f"HTTP {status} {str(payload.get('code', ''))[:40]}")
                    text = json.dumps(payload)
                    if "kn3-source" in text:
                        raise ValueError("an unsigned cursor placeholder left the process")
                    return _kn4_http_result(path, status, payload)
                await timed("http", {"replica": replica, "operation": name}, http)
            # The three POST operations through the SDK (typed bodies).
            async def sdk_posts():
                client = clients[consumer_id]
                batch = await client.warmup_batch([replace(sdk_requirement(bar), warmup_limit=10)], require_all=True)
                check = await _kn4_raw(identity, url, "POST", "/v2/system/readiness:check", consumer_id=consumer_id,
                                       body={"consumer_id": consumer_id, "require_all": True,
                                             "requirements": [sdk_requirement(quote).to_mapping()]})
                if check[0] != 200:
                    raise ValueError(f"readiness:check HTTP {check[0]} {str(check[1].get('code', ''))[:40]}")
                references = [_reference_product(item, now_ns=time.time_ns())
                              for item in _probe_products(products_by_consumer, "MARK_INDEX_PRICE")["BINANCE"][:2]]
                reference = await client.reference_batch([item.sdk_requirement for item in references],
                                                         require_all=True)
                if batch.partial or reference.partial:
                    raise ValueError("a strict POST batch was partial")
                return {"warmup_batch": len(batch.results), "readiness_check_http": check[0],
                        "reference_batch": len(reference.results)}
            await timed("http", {"replica": replica, "operation": "POST warmup:batch/readiness:check/reference:batch"},
                        sdk_posts)
        finally:
            await asyncio.gather(*(client.close() for client in clients.values()), return_exceptions=True)
    # K4-T01/T03 (D41): snapshot -> stream handoff through the real SDK on
    # both replicas at once, judged over one fixed window against the
    # isolated canonical log.
    sections["handoff"].extend(await _kn4_fixed_window_handoffs(
        config=config, identities=identities, products_by_consumer=products_by_consumer,
        client_for=client_for))
    # Replica parity at the same watermark (guide 18.4.4: never byte-identical
    # at different times; equal content at an equal applied boundary).
    for (venue, interval, rows), by_replica in sorted(histories.items()):
        values = list(by_replica.values())
        entry: dict[str, object] = {"venue": venue, "interval": interval, "rows": rows,
                                    "replicas": len(values)}
        if len(values) == 2 and values[0][0] == values[1][0]:
            entry.update(status="PASS" if values[0][1] == values[1][1] else "FAIL", same_watermark=True)
        else:
            entry.update(status="PASS", same_watermark=False,
                         watermark_delta=abs(values[0][0] - values[1][0]) if len(values) == 2 else None)
        sections["parity"].append(entry)
    failed = {name: [item for item in items if item["status"] != "PASS"] for name, items in sections.items()}
    ok = target["status"] == "PASS" and not any(failed.values())
    return {
        "schema": "qdl.kn4.read-plane-matrix.v1",
        "status": "PASS" if ok else "FAIL",
        "target_matrix": {"status": target["status"], "reads": target["reads"], "failed": target["failed"]},
        "counts": {name: len(items) for name, items in sections.items()},
        "failed": {name: items for name, items in failed.items() if items},
        "sections": sections,
        "order_actions": 0,
        "secret_values_recorded": False,
    }


def _ts_heartbeat() -> dict[str, object] | None:
    try:
        result = subprocess.run(["docker", "exec", "-i", "market_data_service", "python", "-"],
                                input=_TS_HEARTBEAT, capture_output=True, text=True, timeout=20)
        value = json.loads(result.stdout.strip().splitlines()[-1])
        value["at_ns"] = time.time_ns()
        return value
    except Exception as error:  # an unreadable heartbeat is itself evidence
        return {"at_ns": time.time_ns(), "error": _safe_error(error)}


def _ts_disconnects(since: str, until: str) -> tuple[int, Counter]:
    result = subprocess.run(["docker", "logs", "--since", since, "--until", until, "market_data_service"],
                            capture_output=True, text=True, timeout=60)
    codes: Counter[str] = Counter()
    for line in (result.stdout + result.stderr).splitlines():
        if "slice disconnected" in line:
            match = re.search(r"code=([A-Z_]+)", line)
            codes[match.group(1) if match else "UNKNOWN"] += 1
    return sum(codes.values()), codes


def _projector_spans(prefix: str, since: str, until: str) -> dict[str, object]:
    summary = {}
    names = subprocess.run(["docker", "ps", "--format", "{{.Names}}", "--filter", f"name={prefix}-projector"],
                           capture_output=True, text=True, timeout=20).stdout.split()
    for name in sorted(names):
        text = subprocess.run(["docker", "logs", "--since", since, "--until", until, name],
                              capture_output=True, text=True, timeout=60)
        age_max = append_max = 0.0
        windows = 0
        for line in (text.stdout + text.stderr).splitlines():
            if "qdl_stable_projector_spans" not in line:
                continue
            windows += 1
            for key, target in (("canonical_age_ms", "age"), ("durable_append_ms", "append")):
                match = re.search(key + r"=\{[^}]*'max': ([0-9.]+)", line)
                if match:
                    if target == "age":
                        age_max = max(age_max, float(match.group(1)))
                    else:
                        append_max = max(append_max, float(match.group(1)))
        summary[name] = {"span_windows": windows, "canonical_age_max_ms": age_max,
                         "durable_append_max_ms": append_max}
    return summary


def _host_planner():
    """The planner file alone: the host half imports no SDK or package init."""

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "phase3_consumer_load_host", ROOT / "qdl/certification/phase3_consumer_load.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def run_target_host(args: argparse.Namespace) -> int:
    planner = _host_planner()
    TARGET_STAGE_SECONDS = planner.TARGET_STAGE_SECONDS
    evaluate_target_acceptance = planner.evaluate_target_acceptance
    load_target_budget = planner.load_target_budget

    profile = validate_profile(_read_json(args.profile))
    if {item["id"] for item in profile["identities"]} != set(_TARGET_VENUE_IDENTITY.values()):
        raise ValueError("target mode runs exactly the two alpha platform identities")
    budget_bytes = _TARGET_BUDGET_PATH.read_bytes()
    budget = load_target_budget(json.loads(budget_bytes))
    matrix = args.mode in {"target-matrix", "kn4-matrix"}
    if args.sessions not in TARGET_STAGE_SECONDS:
        raise ValueError("target sessions must be exactly one of 5,20,35,50")
    duration = 0 if matrix else TARGET_STAGE_SECONDS[args.sessions]
    if args.duration_seconds != duration:
        raise ValueError(f"target stage {args.sessions} runs exactly {duration} s from the frozen budget")
    final = not matrix and args.sessions == 50
    workers = 1 if matrix else target_worker_count(args.sessions)
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    image_id = _docker_image_id(str(profile["image"]))
    name = "qdl-phase3-target-" + uuid.uuid4().hex[:12]
    command, inner = docker_command(profile, name=name, image_id=image_id, mode=args.mode,
                                    sessions=args.sessions, duration_seconds=duration)
    inner.update(budget=budget, final=final)
    if args.mode == "kn4-matrix":
        inner["oracle_bootstrap"] = profile.get("oracle_bootstrap")
    command[command.index("--cpus") + 1] = f"{float(workers + (0 if matrix else 1)):.1f}"
    command[command.index("--memory") + 1] = f"{512 * (workers + (0 if matrix else 1))}m"
    for index, value in enumerate(command):
        if value.startswith("QDL_PHASE3_LOAD_CONFIG="):
            command[index] = "QDL_PHASE3_LOAD_CONFIG=" + json.dumps(inner, sort_keys=True, separators=(",", ":"))
    query_names = list(profile["query_containers"])
    prefix = query_names[0].split("-query_v2_", 1)[0]
    monitored = profile.get("monitored_containers") or [
        *query_names, f"{prefix}-stream_v2_active-1", f"{prefix}-stream_v2_passive-1",
        "market_data_service",
    ]
    shadow = profile.get("scope") == "shadow"
    # A shadow run still samples the production Trading System: it is the
    # packet's stop condition (TS ready routes must not drop), not a consumer
    # of the shadow targets.
    ts_samples = [] if matrix else [_ts_heartbeat()]
    started_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    baseline_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 600))
    started = time.monotonic()
    process = None
    cleanup_error = None
    runtime_fault = None
    telemetry = []
    try:
        process, telemetry, runtime_fault = _run_monitored_client(
            command, monitored, timeout=duration + 720,
            observer=None if matrix else _ts_heartbeat, observations=ts_samples,
        )
    finally:
        try:
            _cleanup_exact_container(name)
        except Exception as error:
            cleanup_error = _safe_error(error)
    ended_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    if not matrix:
        ts_samples.append(_ts_heartbeat())
    stdout = process.stdout if process is not None else ""
    stderr = process.stderr if process is not None else ""
    receipt = None
    for line in reversed(stdout.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and str(value.get("schema", "")).startswith(("qdl.phase3.target-", "qdl.kn4.")):
            receipt = value
            break
    # A client that failed before its receipt still prints one typed failure
    # line (payload-free); keep it so the failure is attributable.
    client_failure = None
    if receipt is None:
        for line in reversed(stdout.splitlines()):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict) and "failure" in value:
                client_failure = str(value["failure"])[:500]
                break
    trading_system: dict[str, object] = {"samples": [item for item in ts_samples if item and "ready" in item],
                                         "unreadable_samples": [item for item in ts_samples if item and "error" in item]}
    evaluation = None
    if not matrix:
        run_minutes = max(1e-6, (time.monotonic() - started) / 60.0)
        base_count, base_codes = _ts_disconnects(baseline_iso, started_iso)
        run_count, run_codes = _ts_disconnects(started_iso, ended_iso)
        trading_system.update(
            baseline_window=[baseline_iso, started_iso], run_window=[started_iso, ended_iso],
            baseline_disconnects_per_minute=round(base_count / 10.0, 3),
            baseline_codes=dict(base_codes),
            run_disconnects_per_minute=round(run_count / run_minutes, 3),
            disconnect_codes=dict(run_codes),
        )
        if receipt is not None and receipt.get("schema") == "qdl.phase3.target-load.v1":
            evaluation = evaluate_target_acceptance(budget=budget, stage=args.sessions, final=final,
                                                    receipt=receipt, trading_system=trading_system)
    passed = (process is not None and process.returncode == 0 and receipt is not None
              and cleanup_error is None and runtime_fault is None
              and (receipt.get("status") == "PASS" if matrix else
                   evaluation is not None and evaluation["status"] == "PASS"
                   and receipt.get("status") == "COMPLETE"))
    host = {
        "schema": "qdl.phase3.target-host.v1",
        "status": "PASS" if passed else "FAIL",
        "mode": args.mode, "stage": args.sessions, "final": final, "duration_seconds": duration,
        "workers": workers, "image_id": image_id,
        "source_sha": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True,
                                     text=True, check=True).stdout.strip(),
        "tool_sha256": _sha256(Path(__file__).read_bytes()),
        "planner_sha256": _sha256((ROOT / "qdl/certification/phase3_consumer_load.py").read_bytes()),
        "budget_sha256": _sha256(budget_bytes),
        "authenticated_identity_count": len(profile["identities"]),
        "runtime_fault": runtime_fault,
        "runtime_observation": telemetry,
        "trading_system": trading_system,
        "scope": profile.get("scope", "production"),
        "trading_system_scope": ("production TS observed as the shadow stop condition; not a shadow consumer"
                                 if shadow else "consumer of the targets"),
        "monitored_containers": monitored,
        "projector_spans": (None if matrix or shadow
                            else _projector_spans(prefix, started_iso, ended_iso)),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "client_returncode": None if process is None else process.returncode,
        "client_failure": client_failure,
        "client_stderr_tail_sha256": _sha256(stderr.encode()),
        "cleanup_error": cleanup_error,
        "inner_config_sha256": _sha256(json.dumps(inner, sort_keys=True, separators=(",", ":")).encode()),
        "secret_values_recorded": False,
        "order_actions": 0,
    }
    (output / "host.json").write_text(json.dumps(host, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if receipt is not None:
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n",
                                             encoding="utf-8")
    if evaluation is not None:
        (output / "gates.json").write_text(json.dumps(evaluation, indent=2, sort_keys=True) + "\n",
                                           encoding="utf-8")
    if process is not None and process.returncode != 0 and stderr:
        # Payload-free tail for attribution: exception types and codes only.
        lines = [line for line in stderr.splitlines() if re.search(r"(Error|Exception)\b", line)][-20:]
        (output / "stderr-types.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"schema": host["schema"], "status": host["status"], "output": str(output),
                      "failed_gates": None if evaluation is None else evaluation["failed_gates"]},
                     sort_keys=True))
    return 0 if passed else 1


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    value.add_argument("--profile", type=Path)
    value.add_argument("--output", type=Path)
    value.add_argument("--mode", choices=("matrix", "load", "final", "target-matrix", "target", "kn4-matrix"))
    value.add_argument("--inside-worker", nargs=2, type=int, metavar=("INDEX", "COUNT"), help=argparse.SUPPRESS)
    value.add_argument("--sessions", type=int)
    value.add_argument("--duration-seconds", type=int)
    return value


def main() -> int:
    args = parser().parse_args()
    if args.inside_worker is not None:
        return run_target_worker(*args.inside_worker)
    if args.inside:
        if any(value is not None for value in (args.profile, args.output, args.mode, args.sessions, args.duration_seconds)):
            raise SystemExit("inner Phase-3 client accepts configuration only from its mounted environment")
        mode = str(json.loads(os.environ.get("QDL_PHASE3_LOAD_CONFIG", "{}")).get("mode", ""))
        runner = {"target": run_target_inside, "target-matrix": run_target_matrix_inside,
                  "kn4-matrix": run_kn4_matrix_inside}.get(mode, run_inside)
        try:
            result = asyncio.run(runner())
        except Exception as error:
            print(json.dumps({
                "schema": "qdl.phase3.consumer-load.v1", "status": "FAIL",
                "failure": _safe_error(error), "order_actions": 0,
                "secret_values_recorded": False,
            }, sort_keys=True))
            return 1
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0 if result["status"] in {"PASS", "COMPLETE"} else 1
    if None in (args.profile, args.output, args.mode, args.sessions, args.duration_seconds):
        raise SystemExit("host Phase-3 run requires --profile --output --mode --sessions --duration-seconds")
    if args.mode in {"target", "target-matrix", "kn4-matrix"}:
        return run_target_host(args)
    return run_host(args)


if __name__ == "__main__":
    raise SystemExit(main())
