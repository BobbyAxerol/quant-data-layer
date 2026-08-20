#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    write_production_core_bundle,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build deterministic Phase C Rust production-core configs."
    )
    parser.add_argument("--source-catalog", type=Path, required=True)
    parser.add_argument("--acquisition-plan", type=Path, required=True)
    parser.add_argument("--raw-authority", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--partition-plan-epoch", type=int, default=1)
    args = parser.parse_args()

    catalog = StableSourceCatalog.load(args.source_catalog)
    acquisition = StableAcquisitionPlan.load(
        args.acquisition_plan, catalog=catalog
    )
    authority = json.loads(args.raw_authority.read_text(encoding="utf-8"))
    digests = write_production_core_bundle(
        args.output_dir,
        catalog=catalog,
        acquisition=acquisition,
        raw_authority=authority,
        partition_plan_epoch=args.partition_plan_epoch,
    )
    print(json.dumps({
        "schema": "qdl.v2.production-core-build-result.v1",
        "status": "PASS",
        "output_dir": str(args.output_dir.resolve()),
        "digests": digests,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
