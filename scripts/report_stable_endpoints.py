#!/usr/bin/env python3
"""Report every realtime endpoint the stable V2 data layer serves a consumer.

One row per declared requirement: what the consumer asked for, what capability
the contract grants it, and what the delivery path actually measured. The three
quantities the plan insists on keeping apart stay apart here too.

* **delivery lag** - how late an event reached the canonical stream, measured
  against the venue's own clock. For BAR that is publish time minus the bar's
  *close* time, because a bar does not exist until it closes; measuring it from
  `source_event_time_ns` (the bar's open) would report the interval length. For
  every other feed it is publish time minus `source_event_time_ns`.
* **sample count** - how many events each number is taken over, so a p95 from
  five closed candles is never mistaken for a p95 from five thousand trades.
* **contract** - `max_freshness_ms` and the capability fields the consumer
  manifest declares. These are tolerances the consumer enforces, not targets.

The canonical capture is produced separately, so this script stays read-only and
has no broker credentials:

    docker run --rm --network qdl_v2_stable_candidate_stable_internal \\
      -v "$CERTS:/etc/kafka/secrets:ro" --entrypoint \\
      /opt/kafka/bin/kafka-console-consumer.sh "$KAFKA_IMAGE" \\
      --bootstrap-server kafka1:9092 \\
      --command-config /etc/kafka/secrets/admin.properties \\
      --topic md.canonical.v2 --max-messages 200000 --timeout-ms 300000 \\
      --formatter-property print.timestamp=true \\
      --formatter-property print.key=true > capture.bin

    python3 -B scripts/report_stable_endpoints.py --capture capture.bin \\
      --catalog /path/to/core.json
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import statistics
import sys
from pathlib import Path

DEFAULT_MANIFEST = "consumers/stable/trading-system-paper.yaml"

# Capability fields the manifest may carry, in the order a reader wants them.
CAPABILITY_FIELDS = (
    "consumer_grade", "source_policy_id", "recovery", "gap_policy", "stale_policy",
    "bar_revision_policy", "event_recency_policy", "require_final_bars",
    "require_full_coverage", "warmup_limit", "max_session_liveness_ms",
)


def load_requirements(path: Path) -> list[dict]:
    import yaml

    document = yaml.safe_load(path.read_text())
    return document["spec"]["requirements"]


def load_instruments(path: Path) -> dict[str, tuple[str, str]]:
    """instrument_uid -> (venue, native_symbol), read from a deployed core config."""

    bindings = json.loads(path.read_text())["core"]["bindings"]
    return {b["instrument_uid"]: (b["venue"], b["native_symbol"]) for b in bindings}


def measure(capture: Path) -> dict[tuple[str, str, str, str], list[float]]:
    """Delivery lag in seconds per (venue, symbol, feed, interval).

    Keyed by instrument, not just by feed: five symbols of one venue do not
    share a delivery path in any way that makes one number stand for all five,
    and reporting them as if they did hides exactly the per-instrument stall
    this release was written to find.
    """

    from qdl.marketdata.v2 import market_data_pb2

    samples: dict[tuple[str, str, str], list[float]] = collections.defaultdict(list)
    for chunk in capture.read_bytes().split(b"CreateTime:")[1:]:
        match = re.match(rb"(\d+)\t([^\t]*)\t(.*)", chunk, re.S)
        if not match:
            continue
        envelope = market_data_pb2.EventEnvelope()
        try:
            envelope.ParseFromString(match.group(3).rstrip(b"\n"))
        except Exception:                                   # noqa: BLE001 - capture edge
            continue
        published_ms = int(match.group(1))
        payload = envelope.WhichOneof("payload")
        if payload == "bar":
            if not envelope.bar.is_final:
                continue
            reference_ms = envelope.bar.close_time_ns // 1_000_000
            interval = envelope.bar.interval
        else:
            reference_ms = envelope.source_event_time_ns // 1_000_000
            interval = "-"
        if reference_ms <= 0:
            continue
        feed = {
            "trade": "TRADE", "quote": "QUOTE", "bar": "BAR",
            "book_delta": "BOOK_DELTA", "book_snapshot": "BOOK_SNAPSHOT",
            "mark_index_price": "MARK_INDEX_PRICE",
        }.get(payload, (payload or "?").upper())
        samples[(envelope.venue, envelope.native_symbol, feed, interval)].append(
            (published_ms - reference_ms) / 1000
        )
    return samples


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(DEFAULT_MANIFEST))
    parser.add_argument("--catalog", type=Path, required=True,
                        help="a deployed core.json, for instrument_uid -> venue/symbol")
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    requirements = load_requirements(args.manifest)
    instruments = load_instruments(args.catalog)
    samples = measure(args.capture)

    rows = []
    for requirement in requirements:
        venue, symbol = instruments.get(
            requirement["instrument_uid"], ("?", requirement["instrument_uid"][:8])
        )
        feed = requirement["feed"]
        interval = requirement.get("interval") or "-"
        observed = samples.get((venue, symbol, feed, interval), [])
        rows.append({
            "venue": venue, "symbol": symbol, "feed": feed, "interval": interval,
            "contract_max_freshness_ms": requirement["max_freshness_ms"],
            "measured_samples": len(observed),
            "delivery_p50_s": round(statistics.median(observed), 3) if observed else None,
            "delivery_p95_s": round(percentile(observed, 0.95), 3) if observed else None,
            "delivery_max_s": round(max(observed), 3) if observed else None,
            "measured_against": "bar close time" if feed == "BAR" else "source event time",
            "capabilities": {
                field: requirement[field]
                for field in CAPABILITY_FIELDS if field in requirement
            },
        })
    rows.sort(key=lambda r: (r["venue"], r["feed"], r["interval"], r["symbol"]))

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    print(f"{len(rows)} endpoint được khai báo trong {args.manifest.name}\n")
    header = (f"{'venue':8s} {'symbol':16s} {'feed':17s} {'iv':4s} "
              f"{'policy':>9s} {'p50':>8s} {'p95':>8s} {'max':>8s} {'n':>6s}  đo theo")
    print(header)
    print("-" * len(header))
    for row in rows:
        fmt = lambda v: f"{v:8.3f}" if v is not None else f"{'-':>8s}"
        print(f"{row['venue']:8s} {row['symbol']:16s} {row['feed']:17s} "
              f"{row['interval']:4s} {row['contract_max_freshness_ms']:9d} "
              f"{fmt(row['delivery_p50_s'])} {fmt(row['delivery_p95_s'])} "
              f"{fmt(row['delivery_max_s'])} {row['measured_samples']:6d}  "
              f"{row['measured_against']}")

    # Capabilities are declared per feed, not per instrument, so printing them
    # once per feed says the same thing as sixty identical rows and stays
    # readable. Any instrument that diverges gets its own line.
    print("\n\nCapability mỗi endpoint (theo hợp đồng consumer)\n")
    grouped: dict[tuple, list[str]] = collections.defaultdict(list)
    for row in rows:
        key = (row["feed"], row["interval"], row["contract_max_freshness_ms"],
               json.dumps(row["capabilities"], sort_keys=True))
        grouped[key].append(f"{row['venue']}:{row['symbol']}")
    for (feed, interval, policy, capability_json), members in sorted(grouped.items()):
        capabilities = json.loads(capability_json)
        label = feed if interval == "-" else f"{feed} {interval}"
        print(f"  {label}  ({len(members)} endpoint: {', '.join(members[:3])}"
              f"{', …' if len(members) > 3 else ''})")
        print(f"      max_freshness_ms            {policy}")
        for name, value in capabilities.items():
            print(f"      {name:28s}{value}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
