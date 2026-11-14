#!/usr/bin/env python3
"""DL v2.3.0 window gate D5a: every live OKX/Binance lane session accepts the pinned binding revisions.

Purpose. The 2026-11-13 combined window failed at W1 because the rc2 Query refused every retained OKX binding pinned at
config revision 21 under the ingestor's session revision 22 (``qdl/runtime/session_liveness.py`` exact-equality check,
SOURCE_SESSION_CONFIG_MISMATCH -> SESSION_STATE). This check runs the DEPLOYED reader code against the LIVE session
files, independent of the TS writer, right after the ingestor step (D5).

Usage (host): ``docker exec -i <query container> python -B - --min-pinned 21 < scripts/v230_session_revision_check.py``

For every lane record of every venue directory under the session-liveness root it asks the deployed
``StableSessionLivenessReader`` for that record's own session and generation with:

* the record's revision and every revision down to ``--min-pinned`` (the lowest pinned binding revision): must not be
  refused with SOURCE_SESSION_CONFIG_MISMATCH. A lane whose own revision is below ``--min-pinned`` checks only its own
  revision (Binance in window A: session 19 = its pins 19);
* the record's revision + 1 (an event newer than its session): must be refused with SOURCE_SESSION_CONFIG_MISMATCH.

Records whose state is not LIVE or that are older than ``--max-age-s`` are reported and skipped (a lane that is down
is the ingestor gate's business, not this one), but each venue must have at least one LIVE lane checked. Output: one JSON object; exit 0 only when ``verdict`` is PASS.

Boundary: read-only. It reads the session files the Query container already mounts and imports only the Query's own
reader; no network, no write.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time

from qdl.runtime.session_liveness import StableSessionLivenessReader

_VENUES = {"okx-swap": ("OKX", "SWAP"), "binance-usdm": ("BINANCE", "USDM")}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=os.environ.get("QDL_STABLE_SESSION_LIVENESS_ROOT", "/session-liveness"))
    parser.add_argument("--min-pinned", type=int, required=True)
    parser.add_argument("--max-age-s", type=float, default=30.0)
    args = parser.parse_args()
    root = Path(args.root)
    reader = StableSessionLivenessReader(root)
    now_ns = time.time_ns()
    rows, failures = [], []
    for directory, (venue, market) in _VENUES.items():
        for path in sorted((root / directory).glob("*.json")):
            record = json.loads(path.read_text(encoding="utf-8"))
            age_s = (now_ns - int(record["last_transport_at_ns"])) / 1e9
            row = {"lane": f"{directory}/{path.stem}", "state": record["state"],
                   "config_revision": record["config_revision"], "age_s": round(age_s, 1)}
            if record["state"] != "LIVE" or age_s > args.max_age_s:
                row["skipped"] = "not LIVE or stale"
                rows.append(row)
                continue

            def status(revision: int):
                return reader.status(venue=venue, market=market, source_session_id=record["source_session_id"],
                                     connection_generation=int(record["connection_generation"]),
                                     config_revision=revision, now_ns=time.time_ns())

            accepted = {}
            for revision in range(min(args.min_pinned, record["config_revision"]), record["config_revision"] + 1):
                result = status(revision)
                accepted[revision] = result.state
                if "SOURCE_SESSION_CONFIG_MISMATCH" in result.flags:
                    failures.append(f"{row['lane']} refused pinned revision {revision}")
            newer = status(record["config_revision"] + 1)
            row["accepted"] = accepted
            row["newer_refused"] = "SOURCE_SESSION_CONFIG_MISMATCH" in newer.flags
            if not row["newer_refused"]:
                failures.append(f"{row['lane']} accepted an event newer than its session")
            rows.append(row)
    checked = [row for row in rows if "skipped" not in row]
    for prefix, name in (("okx-swap/", "OKX"), ("binance-usdm/", "Binance")):
        if not any(row["lane"].startswith(prefix) for row in checked):
            failures.append(f"no LIVE {name} lane was checked")
    print(json.dumps({"schema": "qdl.v230-session-revision-check.v1", "verdict": "FAIL" if failures else "PASS",
                      "failures": failures, "lanes": rows}, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
