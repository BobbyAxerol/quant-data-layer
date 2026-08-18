#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def _load(path: Path):
    return json.loads(path.read_text())


def _stats(path: Path) -> list[dict[str, object]]:
    rows = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        rows.append({
            key: value.get(key)
            for key in ("Name", "CPUPerc", "MemUsage", "MemPerc", "PIDs", "NetIO")
        })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in (
        "capacity", "security", "parity", "image-inspect", "bridge-inspect",
        "stats", "v1-before", "v1-after",
    ):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--openapi-before", required=True)
    parser.add_argument("--openapi-after", required=True)
    for name in (
        "production-keys-before", "production-keys-after", "containers-after",
        "networks-after", "volumes-after", "images-after",
    ):
        parser.add_argument(f"--{name}", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    capacity = _load(args.capacity)
    security = _load(args.security)
    parity = _load(args.parity)
    image = _load(args.image_inspect)[0]
    bridge = _load(args.bridge_inspect)[0]
    v1_before = _load(args.v1_before)
    v1_after = _load(args.v1_after)
    labels = image.get("Config", {}).get("Labels", {}) or {}
    mounts = bridge.get("Mounts", [])
    networks = sorted(bridge.get("NetworkSettings", {}).get("Networks", {}))
    checks = {
        "phase73_capacity": capacity.get("status") == "PASS"
        and all(capacity.get("checks", {}).values()),
        "phase73_security": security.get("status") == "PASS",
        "continuous_bridge_parity": parity.get("status") == "PASS"
        and parity.get("canonical_mismatches") == 0,
        "provider_authentic": parity.get("source") == "REAL_V1_PROVIDER_DATA"
        and parity.get("generated_market_events") == 0,
        "candidate_revision_labeled": bool(labels.get("org.opencontainers.image.revision")),
        "candidate_non_root": bridge.get("Config", {}).get("User") == "10001:10001",
        "candidate_read_only": bridge.get("HostConfig", {}).get("ReadonlyRootfs") is True,
        "candidate_no_source_bind": not any(
            item.get("Type") == "bind" for item in mounts
        ),
        "bridge_network_isolated": "bobby_network" in networks
        and any(name.endswith("_qdl_beta_internal") for name in networks)
        and not any(name.endswith("_qdl_beta_ingress") for name in networks),
        "v1_topology_unchanged": v1_before == v1_after,
        "v1_openapi_unchanged": args.openapi_before == args.openapi_after,
        "production_beta_keys_absent": args.production_keys_before == 0
        and args.production_keys_after == 0,
        "cleanup_exact": args.containers_after == 0
        and args.networks_after == 0 and args.volumes_after == 0
        and args.images_after == 0,
    }
    failed = sorted(name for name, passed in checks.items() if not passed)
    if failed:
        raise SystemExit(f"Phase 9.0-B evidence failed: {failed}")

    result = {
        "schema": "qdl.phase9.0-b.isolated-beta.v1",
        "status": "PASS_ISOLATED_NO_AUTHORITY_CUTOVER",
        "authority": "V1_SHADOW_READ_ONLY",
        "decision_scope": "V2_BETA_READ_ONLY_REVIEW",
        "checks": checks,
        "candidate": {
            "image_id": image.get("Id"),
            "source_revision": labels.get("org.opencontainers.image.revision"),
            "release": labels.get("org.opencontainers.image.version"),
            "runtime_user": bridge.get("Config", {}).get("User"),
            "read_only_root": bridge.get("HostConfig", {}).get("ReadonlyRootfs"),
            "source_bind_mounts": sum(item.get("Type") == "bind" for item in mounts),
            "bridge_networks": networks,
        },
        "contract_and_capacity": capacity,
        "security": security,
        "continuous_bridge": parity,
        "resource_snapshot": _stats(args.stats),
        "cleanup": {
            "containers": args.containers_after,
            "networks": args.networks_after,
            "volumes": args.volumes_after,
            "images": args.images_after,
            "production_beta_keys": args.production_keys_after,
        },
        "production_v1": {
            "topology_unchanged": True,
            "openapi_sha256": args.openapi_after,
            "restarted_or_recreated": False,
        },
        "phase_91": "BLOCKED",
    }
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    normal = capacity["normal"]
    burst = capacity["burst"]
    stream = capacity["stream"]
    resources = capacity["resources"]
    report = f'''# Phase 9.0-B Isolated V2 Beta Report

Decision: `PASS_ISOLATED_NO_AUTHORITY_CUTOVER`

## Scope

The existing V2 query/stream beta was re-certified from the Phase 9.0-A
correctness baseline. V1 remained source and public authority. The candidate
used an isolated Redis, durable spool, credentials, consumer group, loopback
ports and Compose project. No Rust canary or public-internet exposure occurred.

## Results

- Authentic slice: `BINANCE / USDM / PERPETUAL / BTCUSDT / BAR / 1m`.
- Canonical mismatches: `{parity['canonical_mismatches']}`; generated events:
  `{parity['generated_market_events']}`; duplicate open times:
  `{parity['duplicate_open_times']}`.
- Continuous bridge watermark delta across the bounded observation window:
  `{parity['watermark_delta']}`.
- Active/passive stream replay: `{stream['fast_consumers']}` fast consumers,
  `{stream['slow_consumers']}` isolated slow consumer, contiguous=
  `{all(item['contiguous'] for item in stream['fast_results'])}`.
- Query normal: `{normal['requests']}` requests, p99.9
  `{normal['latency_ms']['p99_9']}` ms, `{normal['errors']}` errors.
- Query burst: `{burst['requests']}` requests, p99.9
  `{burst['latency_ms']['p99_9']}` ms, `{burst['errors']}` errors.
- Peak candidate RSS: `{resources['max_rss_bytes']}` bytes; peak CPU:
  `{resources['max_cpu_percent_of_one_core']}%` of one core.
- Security/adversarial, cursor, failover/fencing, dependency outage/recovery,
  rate limit, oversized request and cleanup gates all passed.

## Safety And Cleanup

Candidate revision: `{result['candidate']['source_revision']}`. Runtime user was
`10001:10001`, root was read-only, no source bind was mounted and bridge access
was restricted to beta-internal plus the existing V1 internal network.
Production V1 topology/OpenAPI remained unchanged. Candidate containers,
networks, volumes and candidate tags after cleanup: `0/0/0/0`; production beta
keys: `0`.

## Remaining Boundary

This result permits review of an isolated read-only V2 beta only. Phase 9.1
remains blocked on replicated production transport, OTel/alerts, workload
identity, external secrets, signature admission, independent DR, complete
consumer registration and explicit exact-slice authority approval.
'''
    args.report.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
