#!/usr/bin/env python3
"""Create a secret-free digest-to-source attestation for the frozen V1 image."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase105_handoff import (
    V1_FALLBACK_COMMIT,
    sha256_file,
    v1_image_attestation,
)


CONFIRM = "ATTEST_QDL_PHASE105_V1_FALLBACK"


def _run(*values: str) -> str:
    return subprocess.run(values, check=True, text=True, capture_output=True).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-repo", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    if args.output.exists():
        raise SystemExit("Phase 10.5-C V1 provenance output already exists")
    source = args.source_repo.resolve()
    source_commit = _run("git", "-C", str(source), "rev-parse", "HEAD")
    source_tree = _run("git", "-C", str(source), "rev-parse", "HEAD^{tree}")
    if source_commit != V1_FALLBACK_COMMIT:
        raise SystemExit("V1 source repository is not checked out at the frozen v1.2.2 commit")
    image_payload = _run("docker", "image", "inspect", args.image)
    image = json.loads(image_payload)
    if not isinstance(image, list) or len(image) != 1 or not isinstance(image[0], dict):
        raise SystemExit("Docker did not return one V1 image record")
    evidence = v1_image_attestation(
        image[0],
        source_commit=source_commit,
        source_tree=source_tree,
        dockerfile_sha256=sha256_file(source / "Dockerfile"),
    )
    if not args.apply:
        print(json.dumps(evidence, sort_keys=True))
        return 0
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    args.output.chmod(0o600)
    print(json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
