from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from qdl.history.bars import BarRecord


VN_ZONE = ZoneInfo("Asia/Ho_Chi_Minh")


def _valid_vn_bar_time(value: time, *, derivative: bool) -> bool:
    morning_open = time(8, 45) if derivative else time(9, 15)
    return (
        morning_open <= value <= time(11, 29)
        or time(13, 0) <= value <= time(14, 29)
        or value == time(14, 45)
    )


def interpret_legacy_vn_timestamp(value: pd.Timestamp, *, derivative: bool) -> tuple[int, str]:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        utc = timestamp.tz_convert("UTC")
        return int(utc.value), "SOURCE_TZ_AWARE"
    naive = timestamp.to_pydatetime()
    local_valid = _valid_vn_bar_time(naive.time(), derivative=derivative)
    utc_as_local = naive.replace(tzinfo=timezone.utc).astimezone(VN_ZONE)
    utc_valid = _valid_vn_bar_time(utc_as_local.time(), derivative=derivative)
    if local_valid == utc_valid:
        raise ValueError(
            f"legacy VN timestamp has ambiguous/invalid timezone semantics: {timestamp.isoformat()}"
        )
    if local_valid:
        instant = naive.replace(tzinfo=VN_ZONE).astimezone(timezone.utc)
        interpretation = "LEGACY_NAIVE_ASIA_HO_CHI_MINH"
    else:
        instant = naive.replace(tzinfo=timezone.utc)
        interpretation = "LEGACY_NAIVE_UTC"
    return int(instant.timestamp() * 1_000_000_000), interpretation


def migrate_vn_parquet(
    path: Path,
    *,
    instrument_uid: str,
    instrument_id: str,
    interval: str,
    interval_minutes: int,
    source_id: str,
    derivative: bool,
    normalizer_version: str,
    config_revision: int,
) -> tuple[list[BarRecord], dict]:
    source_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    frame = pd.read_parquet(path)
    required = {"time", "open", "high", "low", "close", "volume"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"legacy VN parquet missing columns: {sorted(missing)}")
    frame = frame.sort_values("time", kind="stable").reset_index(drop=True)
    rows: list[BarRecord] = []
    interpretations: Counter[str] = Counter()
    candidates: dict[int, list[dict]] = defaultdict(list)
    for index, raw in frame.iterrows():
        open_ns, interpretation = interpret_legacy_vn_timestamp(
            pd.Timestamp(raw["time"]), derivative=derivative
        )
        interpretations[interpretation] += 1
        values = tuple(str(Decimal(str(raw[column]))) for column in (
            "open", "high", "low", "close", "volume"
        ))
        candidates[open_ns].append({
            "interpretation": interpretation,
            "values": values,
            "lineage": {
                "legacy_path": str(path),
                "legacy_sha256": source_sha,
                "row_index": int(index),
                "source_time_text": str(raw["time"]),
            },
        })

    duplicate_groups = 0
    duplicate_rows = 0
    for open_ns, group in sorted(candidates.items()):
        distinct_values = {item["values"] for item in group}
        if len(distinct_values) != 1:
            raise ValueError(
                "legacy VN parquet has conflicting OHLCV revisions at canonical timestamp "
                f"{open_ns}"
            )
        if len(group) > 1:
            duplicate_groups += 1
            duplicate_rows += len(group) - 1
        # Prefer the corrected local-time convention when both legacy conventions
        # contain the same provider bar. Every source row remains in lineage.
        selected = max(
            group,
            key=lambda item: item["interpretation"] == "LEGACY_NAIVE_ASIA_HO_CHI_MINH",
        )
        open_, high, low, close, volume = selected["values"]
        lineage = json.dumps(
            {
                "deduplication": "EXACT_OHLCV_ONLY",
                "source_rows": [item["lineage"] for item in group],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        rows.append(BarRecord(
            instrument_uid=instrument_uid,
            instrument_id=instrument_id,
            instrument_revision=1,
            source_id=source_id,
            source_role="BACKFILL",
            interval=interval,
            open_time_ns=open_ns,
            close_time_ns=open_ns + interval_minutes * 60 * 1_000_000_000,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            trade_count=0,
            is_final=True,
            revision=0,
            origin="LEGACY_PROVIDER_PARQUET",
            normalizer_version=normalizer_version,
            config_revision=config_revision,
            raw_lineage_json=lineage,
            timestamp_interpretation=selected["interpretation"],
        ))
    return rows, {
        "path": str(path),
        "source_sha256": source_sha,
        "rows": len(rows),
        "source_rows": len(frame),
        "exact_duplicate_groups": duplicate_groups,
        "exact_duplicate_rows_removed": duplicate_rows,
        "conflicting_revision_groups": 0,
        "interpretations": dict(sorted(interpretations.items())),
        "fabricated_rows": 0,
        "first_open_time_ns": rows[0].open_time_ns if rows else None,
        "last_open_time_ns": rows[-1].open_time_ns if rows else None,
    }
