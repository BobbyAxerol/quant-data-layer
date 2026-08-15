#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


_SIZE = re.compile(r"^([0-9.]+)\s*([KMGT]?i?B)$", re.IGNORECASE)


def _bytes(value: str) -> float:
    match = _SIZE.match(value.strip())
    if not match:
        return 0.0
    amount = float(match.group(1))
    unit = match.group(2).upper()
    factors = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "KIB": 1024,
        "MIB": 1024**2,
        "GIB": 1024**3,
        "TIB": 1024**4,
    }
    return amount * factors[unit]


def _stats(path: Path) -> dict:
    peaks: dict[str, dict[str, float]] = {}
    if not path.exists():
        return peaks
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        name = item.get("Name") or item.get("Container") or "unknown"
        current = peaks.setdefault(name, {
            "cpu_percent_peak": 0.0,
            "rss_bytes_peak": 0.0,
            "network_rx_bytes_peak": 0.0,
            "network_tx_bytes_peak": 0.0,
        })
        current["cpu_percent_peak"] = max(
            current["cpu_percent_peak"],
            float(str(item.get("CPUPerc", "0")).rstrip("%") or 0),
        )
        memory = str(item.get("MemUsage", "0B / 0B")).split("/", 1)[0]
        current["rss_bytes_peak"] = max(current["rss_bytes_peak"], _bytes(memory))
        network = str(item.get("NetIO", "0B / 0B")).split("/", 1)
        current["network_rx_bytes_peak"] = max(
            current["network_rx_bytes_peak"], _bytes(network[0])
        )
        if len(network) == 2:
            current["network_tx_bytes_peak"] = max(
                current["network_tx_bytes_peak"], _bytes(network[1])
            )
    return peaks


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--core", type=Path, required=True)
    parser.add_argument("--stats", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--security-output", type=Path, required=True)
    args = parser.parse_args()
    core = json.loads(args.core.read_text(encoding="utf-8"))
    runtime = json.loads(args.runtime.read_text(encoding="utf-8"))
    resources = _stats(args.stats)
    max_rss = max((item["rss_bytes_peak"] for item in resources.values()), default=0)
    max_cpu = max((item["cpu_percent_peak"] for item in resources.values()), default=0)
    store_growth = runtime["durable_store_bytes_after"] - runtime["durable_store_bytes_before"]
    redis_growth = runtime["redis_used_memory_after"] - runtime["redis_used_memory_before"]
    resource_checks = {
        "container_rss_bounded": max_rss <= 512 * 1024 * 1024,
        "container_cpu_bounded": max_cpu <= 100.0,
        "durable_store_growth_bounded": 0 <= store_growth <= 32 * 1024 * 1024,
        "redis_growth_bounded": redis_growth <= 16 * 1024 * 1024,
        "dependency_fail_closed": runtime["dependency_outage_query_status"] == 503,
        "dependency_readiness_degraded": runtime["dependency_outage_ready_status"] == 503,
        "dependency_recovered": runtime["dependency_recovery_ready_status"] == 200,
        "partition_epoch_advanced": (
            runtime["partition_epoch_after"] > runtime["partition_epoch_before"]
        ),
        "v1_fallback_available": runtime["v1_fallback_status"] == 200,
        "v1_topology_unchanged": runtime["v1_topology_unchanged"],
        "production_beta_keys_absent": runtime["production_beta_keys_after"] == 0,
        "cleanup_exact": all((
            runtime["beta_containers_after"] == 0,
            runtime["beta_networks_after"] == 0,
            runtime["beta_volumes_after"] == 0,
            runtime["cursor_files_after"] == 0,
        )),
    }
    checks = dict(core["checks"]) | resource_checks
    result = {
        "schema": "qdl.phase7.capacity.v1",
        "status": "PASS" if core["status"] == "PASS" and all(checks.values()) else "FAIL",
        "decision_scope": "READ_ONLY_PUBLIC_BETA",
        "authority": "V1_SHADOW_READ_ONLY",
        "source": core["source"],
        "generated_market_events": core["generated_market_events"],
        "provenance": runtime["provenance"],
        "normal": core["normal"],
        "burst": core["burst"],
        "stream": core["stream"],
        "end_to_end_freshness_ms": core["end_to_end_freshness_ms"],
        "error_budget_consumption": core["error_budget_consumption"],
        "resources": {
            "containers": resources,
            "max_cpu_percent_of_one_core": max_cpu,
            "max_rss_bytes": max_rss,
            "durable_store_growth_bytes": store_growth,
            "redis_growth_bytes": redis_growth,
        },
        "runtime_failure_and_cleanup": runtime,
        "thresholds": core["thresholds"] | {
            "max_container_rss_bytes": 512 * 1024 * 1024,
            "max_container_cpu_percent_of_one_core": 100.0,
            "max_durable_store_growth_bytes": 32 * 1024 * 1024,
            "max_redis_growth_bytes": 16 * 1024 * 1024,
        },
        "checks": checks,
    }
    security = {
        "schema": "qdl.phase7.security-adversarial.v1",
        "status": "PASS" if all(
            checks[name] for name in (
                "auth_fail_closed", "rotation", "malformed", "oversized",
                "rate_limit", "cursor_tamper", "cursor_expiry", "cursor_scope",
                "dependency_fail_closed", "dependency_readiness_degraded",
                "dependency_recovered", "partition_epoch_advanced",
            )
        ) else "FAIL",
        "authority": "V1_SHADOW_READ_ONLY",
        "tests": core["security"] | {
            "dependency_outage_query_status": runtime["dependency_outage_query_status"],
            "dependency_outage_ready_status": runtime["dependency_outage_ready_status"],
            "dependency_recovery_ready_status": runtime["dependency_recovery_ready_status"],
            "partition_epoch_before": runtime["partition_epoch_before"],
            "partition_epoch_after": runtime["partition_epoch_after"],
        },
    }
    for path, payload in ((args.output, result), (args.security_output, security)):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if result["status"] != "PASS" or security["status"] != "PASS":
        raise SystemExit("Phase 7.3 evidence finalization failed")
    print(json.dumps({
        "capacity": result["status"],
        "security": security["status"],
        "cleanup": checks["cleanup_exact"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
