#!/usr/bin/env python3
"""Build a compact, fresh Phase 10.5-D/B3 observation bundle from C2 evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase105_release_observations import (
    build_release_observation_bundle,
)
from qdl.consumer import StableReleaseRoutePlan


def _load(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Phase 10.5 B3 acceptance receipt cannot be read as JSON") from error


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--release-routing",
        type=Path,
        default=ROOT / "config/v2/stable-v2-release-routing.yaml",
    )
    parser.add_argument("--acceptance", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        plan = StableReleaseRoutePlan.load(args.release_routing, manifest_root=ROOT)
        output = build_release_observation_bundle(plan, _load(args.acceptance))
    except ValueError:
        print(json.dumps({
            "schema": "qdl.phase105.release-observations.v1",
            "status": "BLOCKED",
            "reason": "INVALID_EVIDENCE",
        }, sort_keys=True, separators=(",", ":")))
        return 2
    print(json.dumps(output, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
