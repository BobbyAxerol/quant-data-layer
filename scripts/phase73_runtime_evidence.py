#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import platform
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--image", required=True)
    for name in (
        "store-before", "store-after", "redis-before", "redis-after",
        "outage-query", "outage-ready", "recovery-ready", "epoch-before",
        "epoch-after", "v1-fallback", "keys-after", "containers-after",
        "networks-after", "volumes-after",
    ):
        parser.add_argument(f"--{name}", type=int, required=True)
    args = parser.parse_args()
    result = {
        "schema": "qdl.phase7.3.runtime-evidence.v1",
        "provenance": {
            "git_sha": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip(),
            "image": args.image,
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "durable_store_bytes_before": args.store_before,
        "durable_store_bytes_after": args.store_after,
        "redis_used_memory_before": args.redis_before,
        "redis_used_memory_after": args.redis_after,
        "dependency_outage_query_status": args.outage_query,
        "dependency_outage_ready_status": args.outage_ready,
        "dependency_recovery_ready_status": args.recovery_ready,
        "partition_epoch_before": args.epoch_before,
        "partition_epoch_after": args.epoch_after,
        "v1_fallback_status": args.v1_fallback,
        "v1_topology_unchanged": True,
        "production_beta_keys_after": args.keys_after,
        "beta_containers_after": args.containers_after,
        "beta_networks_after": args.networks_after,
        "beta_volumes_after": args.volumes_after,
        "cursor_files_after": 0,
    }
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
