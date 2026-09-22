#!/usr/bin/env python3
"""Inspect or explicitly create the stable-cache exact final-BAR index.

The default is read-only and prints a bounded JSON preflight. ``--apply`` is
deliberately token-gated because SQLite must scan the existing events table to
build this index. It changes no rows, offsets, topics, Redis keys or V1 state.

    python3 -B scripts/migrate_stable_final_bar_lookup_index.py
    python3 -B scripts/migrate_stable_final_bar_lookup_index.py \
      --apply --confirm CREATE_QDL_FINAL_BAR_LOOKUP_INDEX
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from qdl.transport.sqlite_spool import (  # noqa: E402
    FINAL_BAR_CLOSE_TIME_EXPRESSION,
    FINAL_BAR_LOOKUP_INDEX_NAME,
    FINAL_BAR_LOOKUP_INDEX_SQL,
)


DEFAULT_SPOOL = Path("/var/lib/qdl-stable/shared/canonical-cache.sqlite3")
CONFIRM_TOKEN = "CREATE_QDL_FINAL_BAR_LOOKUP_INDEX"


def _index_present(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
        (FINAL_BAR_LOOKUP_INDEX_NAME,),
    ).fetchone()
    return row is not None


def _snapshot(connection: sqlite3.Connection, *, path: Path) -> dict[str, object]:
    count = int(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])
    events_source = (
        f"events INDEXED BY {FINAL_BAR_LOOKUP_INDEX_NAME}"
        if _index_present(connection)
        else "events"
    )
    plan = connection.execute(
        f"""
        EXPLAIN QUERY PLAN
        SELECT * FROM {events_source}
        WHERE stream = ? AND partition_key = ?
          AND {FINAL_BAR_CLOSE_TIME_EXPRESSION} IN (?, ?)
        ORDER BY logical_offset ASC
        """,
        ("md.canonical.v2", "inspection", 1, 2),
    ).fetchall()
    return {
        "schema": "qdl.v2.stable-final-bar-index-migration.v1",
        "path": str(path),
        "events": count,
        "index": FINAL_BAR_LOOKUP_INDEX_NAME,
        "index_present": _index_present(connection),
        "query_plan": [str(row[3]) for row in plan],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spool", type=Path, default=DEFAULT_SPOOL)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    if args.apply and args.confirm != CONFIRM_TOKEN:
        parser.error(f"--confirm must equal {CONFIRM_TOKEN}")
    if not args.spool.is_file():
        parser.error(f"spool is unavailable: {args.spool}")

    mode = "rwc" if args.apply else "ro"
    connection = sqlite3.connect(
        f"file:{args.spool}?mode={mode}",
        uri=True,
        timeout=args.timeout_seconds,
        isolation_level=None,
    )
    try:
        connection.execute(f"PRAGMA busy_timeout={int(args.timeout_seconds * 1000)}")
        before = _snapshot(connection, path=args.spool)
        if args.apply and not bool(before["index_present"]):
            connection.execute(FINAL_BAR_LOOKUP_INDEX_SQL)
        after = _snapshot(connection, path=args.spool)
    finally:
        connection.close()
    print(json.dumps({"apply": args.apply, "before": before, "after": after}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
