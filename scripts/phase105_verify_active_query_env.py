#!/usr/bin/env python3
"""Write a payload-free proof that query_v2_1 uses the sealed C2 env."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase105_handoff import (
    _ACTIVE_QUERY_COMMITMENT_SCHEMA,
    active_query_environment_commitment,
    active_runtime_binding,
    load_dotenv,
)


CONFIRM = "VERIFY_QDL_PHASE105_ACTIVE_QUERY_ENV"


def _docker_record(container: str) -> dict[str, object]:
    completed = subprocess.run(
        ("docker", "container", "inspect", container),
        check=True,
        text=True,
        capture_output=True,
    )
    payload = json.loads(completed.stdout)
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise ValueError("Phase 10.5-C active query container inspection is invalid")
    return payload[0]


def _environment(record: dict[str, object]) -> dict[str, str]:
    config = record.get("Config")
    values = config.get("Env") if isinstance(config, dict) else None
    if not isinstance(values, list):
        raise ValueError("Phase 10.5-C active query environment is invalid")
    result: dict[str, str] = {}
    for raw in values:
        if not isinstance(raw, str) or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        if key in result:
            raise ValueError("Phase 10.5-C active query environment repeats a key")
        result[key] = value
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-env", type=Path, required=True)
    parser.add_argument("--active-runtime-packet", type=Path, required=True)
    parser.add_argument("--container", default="qdl_v2_stable_candidate-query_v2_1-1")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    if args.output.exists():
        raise SystemExit("Phase 10.5-C active query commitment output already exists")
    try:
        base = load_dotenv(args.base_env)
        packet = json.loads(args.active_runtime_packet.read_text(encoding="utf-8"))
        if not isinstance(packet, dict):
            raise ValueError("Phase 10.5-C active runtime packet must be an object")
        runtime = active_runtime_binding(base, packet)
        record = _docker_record(args.container)
        image_id = record.get("Image")
        container_id = record.get("Id")
        if not isinstance(image_id, str) or not isinstance(container_id, str) or not container_id:
            raise ValueError("Phase 10.5-C active query identity is invalid")
        commitment = active_query_environment_commitment(base, _environment(record), runtime)
    except (OSError, subprocess.CalledProcessError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(str(error)) from error
    result = {
        "schema": _ACTIVE_QUERY_COMMITMENT_SCHEMA,
        "status": "PASS",
        "service": "query_v2_1",
        "container_image_id": image_id,
        "container_id_sha256": hashlib.sha256(container_id.encode("utf-8")).hexdigest(),
        **commitment,
    }
    print(json.dumps(result, sort_keys=True))
    if not args.apply:
        return 0
    os.umask(0o077)
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    args.output.chmod(0o600)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
