from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from datetime import time
from pathlib import Path

import pandas as pd

from qdl.domain.instrument import InstrumentIdentity, ProductType
from qdl.history import (
    AtomicParquetCatalog,
    LocalObjectStore,
    SessionWindow,
    aggregate_bars,
)
from qdl.history.vn_migration import migrate_vn_parquet


INTERVAL_MINUTES = {"1m": 1, "5m": 5, "10m": 10, "15m": 15, "30m": 30, "1h": 60, "4h": 240}


def run(preload_root: Path, output: Path) -> dict:
    identity = InstrumentIdentity.create(
        venue="DNSE", market="VN_DERIVATIVE",
        product_type=ProductType.FUTURE, canonical_symbol="VN30F1M",
    )
    datasets = []
    with tempfile.TemporaryDirectory(prefix="qdl-phase4-vn-shadow-") as directory:
        catalog = AtomicParquetCatalog(LocalObjectStore(Path(directory)))
        canonical_path = preload_root / "1m" / "VN30F1M.parquet"
        if not canonical_path.exists():
            raise RuntimeError(f"real VN preload file missing: {canonical_path}")
        canonical, canonical_migration = migrate_vn_parquet(
            canonical_path,
            instrument_uid=identity.instrument_uid,
            instrument_id=identity.instrument_id,
            interval="1m",
            interval_minutes=1,
            source_id="legacy-vn-preload",
            derivative=True,
            normalizer_version="qdl-vn-migration/phase4",
            config_revision=1,
        )
        sessions = (
            SessionWindow(time(8, 45), time(11, 30)),
            SessionWindow(time(13, 0), time(14, 46)),
        )
        for interval, minutes in INTERVAL_MINUTES.items():
            if interval == "1m":
                records = canonical
                migration = canonical_migration
                legacy_derived = None
            else:
                records = aggregate_bars(
                    canonical,
                    output_interval=interval,
                    interval_minutes=minutes,
                    timezone_name="Asia/Ho_Chi_Minh",
                    sessions=sessions,
                )
                legacy_path = preload_root / interval / "VN30F1M.parquet"
                if not legacy_path.exists():
                    raise RuntimeError(f"real VN derived preload file missing: {legacy_path}")
                legacy_derived = {
                    "path": str(legacy_path),
                    "source_sha256": hashlib.sha256(legacy_path.read_bytes()).hexdigest(),
                    "rows": int(len(pd.read_parquet(legacy_path, columns=["time"]))),
                    "authority": "RECONCILIATION_ONLY",
                }
                migration = {
                    "path": str(canonical_path),
                    "source_sha256": canonical_migration["source_sha256"],
                    "source_rows": canonical_migration["rows"],
                    "rows": len(records),
                    "interpretations": canonical_migration["interpretations"],
                    "exact_duplicate_groups": canonical_migration["exact_duplicate_groups"],
                    "exact_duplicate_rows_removed": canonical_migration["exact_duplicate_rows_removed"],
                    "conflicting_revision_groups": 0,
                    "fabricated_rows": 0,
                    "first_open_time_ns": records[0].open_time_ns if records else None,
                    "last_open_time_ns": records[-1].open_time_ns if records else None,
                }
            snapshot = catalog.commit(
                f"market_data/vn_bar_v2/{interval}", records,
                source_cursor_start=migration["source_sha256"],
                source_cursor_end=migration["source_sha256"],
                normalizer_version="qdl-vn-migration/phase4",
                config_revision=1,
                expected_parent_snapshot_id=None,
            )
            recovered = catalog.read(snapshot)
            if recovered != records:
                raise RuntimeError(f"VN shadow snapshot mismatch interval={interval}")
            datasets.append({
                "interval": interval,
                **migration,
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_data_sha256": snapshot.data_sha256,
                "recovered_rows": len(recovered),
                "legacy_derived": legacy_derived,
            })
    result = {
        "schema": "qdl.phase4.vn-shadow-migration.v1",
        "status": "PASS",
        "provenance": "REAL_EXISTING_PROVIDER_PARQUET_READ_ONLY",
        "production_writes": 0,
        "symbol": "VN30F1M",
        "datasets": datasets,
        "derived_policy": "SESSION_AWARE_AGGREGATION_FROM_CANONICAL_1M",
        "fabricated_rows": sum(item["fabricated_rows"] for item in datasets),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preload-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.preload_root, args.output), sort_keys=True))


if __name__ == "__main__":
    main()
