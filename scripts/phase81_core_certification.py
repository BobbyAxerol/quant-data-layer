#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import pathlib
import subprocess


ROOT = pathlib.Path(__file__).resolve().parents[1]
BINARY = ROOT / "target/debug/qdl-venue-core-certify"
SESSION_EVIDENCE = ROOT / "upgrade/evidence/phase8-rust-session-chaos.json"
SHARDING_EVIDENCE = ROOT / "upgrade/evidence/phase8-stable-sharding.json"


def write(path: pathlib.Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def main() -> int:
    if not BINARY.is_file():
        raise RuntimeError("build qdl-venue-core-certify before certification")
    result = subprocess.run(
        [str(BINARY)], text=True, capture_output=True, check=True, timeout=30
    )
    payload = json.loads(result.stdout.strip())
    if payload.get("status") != "PASS":
        raise RuntimeError(f"venue core failed: {payload}")
    capability_files = sorted(
        (ROOT / "config/phase8/capabilities").glob("*.yaml")
    )
    capability_digest = hashlib.sha256(
        b"".join(path.read_bytes() for path in capability_files)
    ).hexdigest()
    write(
        SESSION_EVIDENCE,
        {
            "schema": "qdl.phase8.rust-session-chaos.v1",
            "status": "PASS",
            "session": payload["session"],
            "ordering": payload["ordering"],
            "backpressure": payload["backpressure"],
            "capability_count": len(capability_files),
            "capability_digest": capability_digest,
            "authority": "RUST_SHADOW",
        },
    )
    write(
        SHARDING_EVIDENCE,
        {
            "schema": "qdl.phase8.stable-sharding.v1",
            "status": "PASS",
            **payload["sharding"],
            "bounded_churn": 0.25 < payload["sharding"]["churn_ratio"] < 0.40,
            "authority": "RUST_SHADOW",
        },
    )
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
