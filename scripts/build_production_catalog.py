#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from qdl.runtime.production_catalog import (
    ProductionCatalogBuilder,
    ProductionDemandManifest,
    load_binance_exchange_info,
    load_okx_instruments,
)
from qdl.runtime.stable_catalog import StableSourceCatalog


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--demand", action="append", required=True, type=Path)
    parser.add_argument("--binance-usdm-exchange-info", type=Path)
    parser.add_argument("--okx-instruments", type=Path)
    parser.add_argument("--previous-catalog", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--catalog-revision", type=int, required=True)
    parser.add_argument("--source-policy-revision", type=int, required=True)
    parser.add_argument("--authority-revision", type=int, required=True)
    args = parser.parse_args()
    demand = ProductionDemandManifest.load_many(args.demand)
    needs_binance = any(item.venue == "BINANCE" for item in demand.demands)
    needs_okx = any(item.venue == "OKX" for item in demand.demands)
    if needs_binance != (args.binance_usdm_exchange_info is not None):
        raise SystemExit("Binance metadata capture must be supplied exactly when demanded")
    if needs_okx != (args.okx_instruments is not None):
        raise SystemExit("OKX metadata capture must be supplied exactly when demanded")
    binance = (
        load_binance_exchange_info(args.binance_usdm_exchange_info)
        if args.binance_usdm_exchange_info else None
    )
    okx = load_okx_instruments(args.okx_instruments) if args.okx_instruments else []
    previous = (
        StableSourceCatalog.load(args.previous_catalog)
        if args.previous_catalog else None
    )
    metadata = {}
    if args.binance_usdm_exchange_info:
        metadata["binance_exchange_info_sha256"] = sha256(args.binance_usdm_exchange_info)
    if args.okx_instruments:
        metadata["okx_instruments_sha256"] = sha256(args.okx_instruments)
    bundle = ProductionCatalogBuilder(
        catalog_revision=args.catalog_revision,
        source_policy_revision=args.source_policy_revision,
        authority_revision=args.authority_revision,
    ).build(
        demand=demand,
        binance_usdm=binance,
        okx_rows=okx,
        previous_catalog=previous,
        metadata_provenance=metadata,
    )
    print(json.dumps(bundle.write(args.output_dir), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
