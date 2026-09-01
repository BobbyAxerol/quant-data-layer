#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/qdl-realtime-core-benchmark")
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--minimum-events-per-second", type=int, default=50_000)
    parser.add_argument("--output", type=Path, default=ROOT / "upgrade/evidence/phase-a-realtime-core-capacity.json")
    args = parser.parse_args()
    completed = subprocess.run(
        [str(args.binary), str(args.events), str(args.minimum_events_per_second)],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"Rust core benchmark failed: {completed.stderr[-1000:]}")
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    payload.update({
        "schema": "qdl.phase-a.realtime-core-capacity.v1",
        "provenance": "SYNTHETIC_CAPACITY_ONLY",
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "production_writes": 0,
    })
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "status": payload["status"],
        "events": payload["events"],
        "events_per_second": payload["events_per_second"],
        "p99_ns": payload["p99_ns"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
