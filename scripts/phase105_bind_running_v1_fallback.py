#!/usr/bin/env python3
"""Bind a frozen V1 attestation to the container serving the C2 fallback read."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase105_handoff import validate_frozen_v1_provenance


CONFIRM = "BIND_QDL_PHASE105_RUNNING_V1_FALLBACK"


def _docker_json(*values: str) -> object:
    completed = subprocess.run(
        ("docker", *values), check=True, text=True, capture_output=True
    )
    return json.loads(completed.stdout)


def _binding(*, provenance: Path, container: str) -> dict[str, object]:
    try:
        provenance_raw = json.loads(provenance.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Phase 10.5 V1 provenance cannot be read") from error
    attestation = validate_frozen_v1_provenance(provenance_raw)
    containers = _docker_json("container", "inspect", container)
    if not isinstance(containers, list) or len(containers) != 1 or not isinstance(containers[0], dict):
        raise ValueError("Phase 10.5 V1 container inspection is invalid")
    record = containers[0]
    image_id = record.get("Image")
    container_id = record.get("Id")
    if not isinstance(image_id, str) or image_id != attestation["image_id"]:
        raise ValueError("Phase 10.5 V1 serving container image differs from frozen provenance")
    if not isinstance(container_id, str) or not container_id:
        raise ValueError("Phase 10.5 V1 serving container identity is invalid")
    images = _docker_json("image", "inspect", image_id)
    if not isinstance(images, list) or len(images) != 1 or not isinstance(images[0], dict):
        raise ValueError("Phase 10.5 V1 serving image is unavailable for attestation")
    if images[0].get("Id") != image_id:
        raise ValueError("Phase 10.5 V1 image inspection differs from serving container")
    return {
        "schema": "qdl.phase105.v1-runtime-binding.v1",
        "status": "PASS",
        "service": "data_layer_service",
        "container_image_id": image_id,
        "container_id_sha256": hashlib.sha256(container_id.encode("utf-8")).hexdigest(),
        "v1_provenance_sha256": attestation["provenance_sha256"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--container", default="data_layer_service")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    if args.output.exists():
        raise SystemExit("Phase 10.5 V1 runtime binding output already exists")
    try:
        binding = _binding(
            provenance=args.provenance,
            container=args.container,
        )
    except (OSError, subprocess.CalledProcessError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(str(error)) from error
    if not args.apply:
        print(json.dumps(binding, sort_keys=True))
        return 0
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    args.output.write_text(json.dumps(binding, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    args.output.chmod(0o600)
    print(json.dumps(binding, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
