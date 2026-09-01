#!/usr/bin/env python3
"""Review-only Phase 10.5-D stable-release certification gate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase105_release import (
    certify_stable_release,
)
from qdl.certification.phase105_release_observations import (
    parse_release_observation_bundle,
)
from qdl.consumer import StableReleaseRoutePlan


def _load(path: Path, field: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Phase 10.5-D {field} cannot be read as JSON") from error


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-routing", type=Path, default=ROOT / "config/v2/stable-v2-release-routing.yaml")
    parser.add_argument("--observations", type=Path, required=True)
    parser.add_argument("--v1-provenance", type=Path, required=True)
    parser.add_argument("--runtime-handoff", type=Path, required=True)
    parser.add_argument("--consumer-acceptance", type=Path, required=True)
    parser.add_argument("--fallback-drill", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        plan = StableReleaseRoutePlan.load(args.release_routing, manifest_root=ROOT)
        result = certify_stable_release(
            plan,
            observations=parse_release_observation_bundle(
                plan, _load(args.observations, "observations")
            ),
            v1_provenance=_load(args.v1_provenance, "V1 provenance"),
            runtime_handoff=_load(args.runtime_handoff, "runtime handoff"),
            consumer_acceptance=_load(args.consumer_acceptance, "consumer acceptance"),
            fallback_drill=_load(args.fallback_drill, "fallback drill"),
        )
    except ValueError:
        print(json.dumps({
            "schema": "qdl.phase105.stable-release-certification.v1",
            "status": "BLOCKED",
            "reason": "INVALID_EVIDENCE",
        }, sort_keys=True, separators=(",", ":")))
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
