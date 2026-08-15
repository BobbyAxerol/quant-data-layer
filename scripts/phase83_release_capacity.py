#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import pathlib
import resource
import time

import phase82_exact_frame_certification as phase82


ROOT = pathlib.Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "upgrade/evidence/phase8-release-capacity.json"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust-replay", type=pathlib.Path, required=True)
    parser.add_argument("--candidate-image-digest", required=True)
    parser.add_argument("--dnse-input", type=pathlib.Path, required=True)
    parser.add_argument("--dnse-date", required=True)
    parser.add_argument("--live-seconds", type=float, default=20.0)
    parser.add_argument("--retain-per-venue", type=int, default=32)
    parser.add_argument("--repeat", type=int, default=500)
    args = parser.parse_args()
    if not args.candidate_image_digest.startswith("sha256:"):
        raise ValueError("candidate image digest must be immutable")
    if not args.rust_replay.is_file():
        raise FileNotFoundError(args.rust_replay)
    phase82.RUST_REPLAY = args.rust_replay
    started = time.monotonic()
    live_fixtures, _, live_metrics = asyncio.run(
        phase82._collect_live(
            duration_seconds=args.live_seconds,
            retained_per_venue=args.retain_per_venue,
        )
    )
    dnse_fixtures, _ = phase82._collect_dnse(args.dnse_date, args.dnse_input)
    deribit_fixture, _ = phase82._deribit_fixture()
    fixtures = [*live_fixtures, *dnse_fixtures, deribit_fixture]
    replay = phase82._replay(fixtures, args.repeat)
    binary_sha256 = hashlib.sha256(args.rust_replay.read_bytes()).hexdigest()
    venue_counts: dict[str, int] = {}
    for fixture in fixtures:
        venue = fixture["context"]["venue"]
        venue_counts[venue] = venue_counts.get(venue, 0) + 1
    thresholds = {
        "canonical_mismatches_max": 0,
        "python_p99_ms_max": 10.0,
        "rust_release_events_per_second_min": 1000.0,
    }
    passed = (
        replay["record_mismatches"] == 0
        and replay["process_restart_mismatches"] == 0
        and replay["python"]["latency_ms"]["p99"] <= thresholds["python_p99_ms_max"]
        and replay["rust"]["events_per_second_min"]
        >= thresholds["rust_release_events_per_second_min"]
    )
    evidence = {
        "schema": "qdl.phase8.release-capacity.v1",
        "status": "PASS" if passed else "FAIL",
        "authority": "RUST_SHADOW",
        "candidate_image_digest": args.candidate_image_digest,
        "release_binary_sha256": binary_sha256,
        "real_provider_read_only": True,
        "production_writes": 0,
        "public_or_legacy_writes": 0,
        "authentic_venues": ["BINANCE", "DNSE", "OKX"],
        "fixture_only_venues": ["DERIBIT"],
        "venue_fixture_counts": venue_counts,
        "live": live_metrics,
        "replay": replay,
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "wall_seconds": time.monotonic() - started,
        "thresholds": thresholds,
        "thresholds_pass": passed,
    }
    OUTPUT.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "status": evidence["status"],
        "events": replay["events"],
        "rust_release_events_per_second_min": replay["rust"]["events_per_second_min"],
        "python_p99_ms": replay["python"]["latency_ms"]["p99"],
    }, sort_keys=True))
    if not passed:
        raise RuntimeError("Phase 8 release capacity thresholds failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
