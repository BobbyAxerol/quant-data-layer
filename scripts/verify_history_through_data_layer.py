#!/usr/bin/env python3
"""Does the data layer return *correct* history for past days, and how fast?

Read-only. It asks the data layer for each BAR interval a consumer's manifest
declares, using that consumer's own certificate and credential, times the call,
and then checks what came back against two things that cannot both be wrong: the
interval's own arithmetic, and the venue's REST for the same open times.

Why it exists. R1.31 certified seven of thirteen Binance intervals by waiting at
a live boundary, which proves the realtime lane and says nothing about history.
The owner's question is the other one: for past days, does the data layer answer
correctly and how long does it take. Waiting days for a 1w boundary answers
neither.

What it checks, per interval:

  alignment   every open_time lands on a boundary of its own interval
  span        close_time - open_time is exactly one interval, less a millisecond
  order       strictly increasing opens, no duplicate, no gap
  shape       high >= max(open, close), low <= min(open, close), high >= low
  lifecycle   every bar FINAL, origin VENUE_NATIVE or BACKFILLED
  truth       oldest and newest bar compared field by field against the venue's
              own REST row for the same open time

The venue comparison is the one that matters and the one that is skipped by
every check written only against our own data: our arithmetic agreeing with
itself proves nothing about the numbers.

    docker run --rm --network <stack>_stable_internal \
      -v /home/bobby/data_layer:/src:ro -v <identities>:/id:ro \
      -e QDL_WORKLOAD=alpha-binance -e QDL_MANIFEST=/src/consumers/... \
      --entrypoint python qdl-v2-python:<tag> -B /src/scripts/verify_history_through_data_layer.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import urllib.request
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yaml  # noqa: E402

from qdl.adapters.intervals import (  # noqa: E402
    canonical_interval_ms,
    is_valid_bar_open_ms,
)
from qdl_sdk.client import AsyncDataLayerClient  # noqa: E402
from qdl_sdk.credentials import RotatingJwtCredentialProvider  # noqa: E402
from qdl_sdk.models import DataRequirement, Feed  # noqa: E402
from qdl_sdk.tls import WorkloadTlsConfig  # noqa: E402
from qdl_sdk.transport import GrpcStreamTransport, RestQueryTransport  # noqa: E402

ID = os.environ.get("QDL_ID_DIR", "/id")
BASE_URL = os.environ.get("QDL_QUERY_URL", "https://qdl-v2-query:8200")
GRPC = os.environ.get("QDL_GRPC_TARGET", "qdl-v2-stream-b:8210")
CONSUMER = os.environ.get("QDL_CONSUMER_ID", "alpha.binance.paper.stable")
WORKLOAD = os.environ.get("QDL_WORKLOAD", "alpha-binance")
MANIFEST = Path(os.environ.get(
    "QDL_MANIFEST", str(ROOT / "consumers/stable/alpha-binance-paper.yaml")))
# Kept small on purpose: the point is correctness per interval, not throughput.
LIMIT = int(os.environ.get("QDL_HISTORY_ROWS", "120"))
VENUE_REST = os.environ.get(
    "QDL_VENUE_REST",
    "https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&startTime={start}&limit=1",
)


def transports():
    tls = WorkloadTlsConfig(
        f"{ID}/{WORKLOAD}/ca.crt",
        f"{ID}/{WORKLOAD}/client.crt",
        f"{ID}/{WORKLOAD}/client.key",
    )
    credential = RotatingJwtCredentialProvider(
        private_key_file=f"{ID}/{WORKLOAD}-jwt/private.key",
        key_id=os.environ["QDL_JWT_KEY_ID"],
        algorithm="RS256",
        issuer=os.environ.get("QDL_JWT_ISSUER", "https://identity.qdl.stable.internal"),
        audience=os.environ.get("QDL_JWT_AUDIENCE", "qdl-v2-stable"),
        subject=os.environ["QDL_SUBJECT"],
        environment=os.environ.get("QDL_JWT_ENVIRONMENT", "paper"),
        roles=tuple(os.environ.get(
            "QDL_JWT_ROLES", "market_data_reader,historical_reader,stream_consumer",
        ).split(",")),
        consumer_manifest_revision=int(os.environ["QDL_MANIFEST_REVISION"]),
    )
    return (
        RestQueryTransport(BASE_URL, timeout_seconds=60.0,
                           credential_provider=credential, tls=tls),
        GrpcStreamTransport(GRPC, tls=tls, credential_provider=credential),
    )


def instrument_symbols() -> dict[str, tuple[str, str]]:
    """uid -> (native_symbol, venue), read from the committed catalog.

    The first run compared every interval against `BTCUSDT` because the symbol
    was an environment default. The manifest names an instrument per
    requirement; a probe that ignores it is comparing two different assets and
    calling the difference a fault.
    """
    catalog = yaml.safe_load(
        (ROOT / "config/v2/stable-source-bindings.yaml").read_text(encoding="utf-8"))
    return {
        str(row["instrument_uid"]): (str(row["native_symbol"]), str(row["venue"]))
        for row in catalog["instruments"]
    }


def bar_requirements() -> list[tuple[str, str, DataRequirement]]:
    """One BAR requirement per interval, carrying every manifest policy field."""
    import dataclasses
    import typing

    hints = typing.get_type_hints(DataRequirement)
    spec = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))["spec"]
    names = {f.name for f in dataclasses.fields(DataRequirement)}
    seen: set[str] = set()
    out: list[tuple[str, str, DataRequirement]] = []
    for item in spec["requirements"]:
        if str(item["feed"]) != "BAR":
            continue
        interval = str(item.get("interval") or "")
        if interval in seen:
            continue
        seen.add(interval)
        kwargs: dict[str, object] = {}
        for key, value in item.items():
            if key not in names or value is None:
                continue
            hint = hints.get(key)
            target = getattr(hint, "__args__", (hint,))[0] if hint is not None else None
            if isinstance(target, type) and issubclass(target, __import__("enum").Enum):
                kwargs[key] = target[str(value)]
            else:
                kwargs[key] = value
        # A bounded read: the manifest's own warmup_limit is a startup budget,
        # not a per-check one, and a 10,000-row 1w request measures the transport
        # rather than the answer.
        kwargs["warmup_limit"] = min(int(kwargs.get("warmup_limit") or LIMIT), LIMIT)
        out.append((interval, str(item["instrument_uid"]), DataRequirement(**kwargs)))
    return sorted(out, key=lambda row: canonical_interval_ms(row[0]))


def _decimal(value) -> Decimal:
    """A `DecimalValue` carries its own exact text; trust that, not a float."""
    return Decimal(getattr(value, "source_text", str(value)))


def check_series(interval: str, bars: list, provider: str) -> list[str]:
    """Everything that can be decided from the series itself.

    Alignment goes through `is_valid_bar_open_ms`, not `open % interval`. A
    weekly bar anchors to Monday and the Unix epoch is a Thursday, so the
    modulus is wrong for exactly the two intervals hardest to check by eye -
    which is how a first run of this script produced 130 false faults on 3d and
    1w and none anywhere else.
    """
    faults: list[str] = []
    step_ms = canonical_interval_ms(interval)
    step_ns = step_ms * 1_000_000
    previous_open: int | None = None
    for bar in bars:
        open_ns, close_ns = bar.open_time_ns, bar.close_time_ns
        if not is_valid_bar_open_ms(interval, open_ns // 1_000_000, provider=provider):
            faults.append(f"open {open_ns} is not on a {interval} boundary")
        if close_ns - open_ns != step_ns - 1_000_000:
            faults.append(f"span {(close_ns - open_ns) / 1e6:.0f}ms != {step_ms - 1}ms")
        if previous_open is not None:
            gap = open_ns - previous_open
            if gap == 0:
                faults.append(f"duplicate open at {open_ns}")
            elif gap < 0:
                faults.append(f"out of order at {open_ns}")
            elif gap != step_ns:
                faults.append(f"gap of {gap / step_ns:.0f} bars before {open_ns}")
        previous_open = open_ns
        o, h, l, c = (_decimal(bar.open), _decimal(bar.high),
                      _decimal(bar.low), _decimal(bar.close))
        if h < max(o, c) or l > min(o, c) or h < l:
            faults.append(f"OHLC inconsistent at {open_ns}")
        if str(bar.lifecycle) not in ("FINAL", "BarLifecycle.FINAL"):
            faults.append(f"lifecycle {bar.lifecycle} at {open_ns}")
        if bar.origin not in ("VENUE_NATIVE", "BACKFILLED"):
            faults.append(f"origin {bar.origin} at {open_ns}")
    return faults


def venue_row(symbol: str, interval: str, open_ms: int) -> list | None:
    url = VENUE_REST.format(symbol=symbol, interval=interval, start=open_ms)
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            rows = json.loads(response.read())
    except Exception:
        return None
    for row in rows:
        if int(row[0]) == open_ms:
            return row
    return None


def compare_to_venue(symbol: str, interval: str, bar) -> list[str]:
    """The check our own arithmetic cannot make for us."""
    open_ms = bar.open_time_ns // 1_000_000
    row = venue_row(symbol, interval, open_ms)
    if row is None:
        return [f"venue returned no row for open {open_ms}"]
    faults = []
    for name, ours, theirs in (
        ("open", bar.open, row[1]), ("high", bar.high, row[2]),
        ("low", bar.low, row[3]), ("close", bar.close, row[4]),
        ("volume", bar.volume, row[5]),
    ):
        if _decimal(ours) != Decimal(str(theirs)):
            faults.append(f"{name} ours={ours} venue={theirs} at {open_ms}")
    return faults


async def main() -> int:
    query, stream = transports()
    client = AsyncDataLayerClient(
        query_transport=query, stream_transport=stream, consumer_id=CONSUMER,
    )
    symbols = instrument_symbols()
    rows = []
    try:
        for interval, uid, requirement in bar_requirements():
            symbol, provider = symbols.get(uid, ("", ""))
            started = time.perf_counter()
            try:
                response = await client.warmup(requirement)
            except Exception as error:  # noqa: BLE001 - reported, not raised
                rows.append({"interval": interval, "error": f"{type(error).__name__}: {error}"})
                continue
            elapsed_ms = (time.perf_counter() - started) * 1000
            bars = [view.payload for view in response.data
                    if view.payload.feed is Feed.BAR]
            faults = check_series(interval, bars, provider)
            venue_faults: list[str] = []
            if bars:
                venue_faults += compare_to_venue(symbol, interval, bars[0])
                if len(bars) > 1:
                    venue_faults += compare_to_venue(symbol, interval, bars[-1])
            oldest_days = ((time.time_ns() - bars[0].open_time_ns) / 86_400e9) if bars else 0.0
            rows.append({
                "interval": interval, "symbol": symbol,
                "ms": round(elapsed_ms, 1), "bars": len(bars),
                "coverage": response.coverage, "oldest_days": round(oldest_days, 1),
                "faults": faults, "venue_faults": venue_faults,
            })
    finally:
        await client.close()

    print(f"  {'iv':>4} {'ms':>9} {'bars':>5} {'back':>8} {'coverage':<12} verdict")
    bad = 0
    for row in rows:
        if "error" in row:
            bad += 1
            print(f"  {row['interval']:>4} {'-':>9} {'-':>5} {'-':>8} {'-':<12} {row['error']}")
            continue
        problems = row["faults"] + row["venue_faults"]
        if problems:
            bad += 1
        verdict = "OK" if not problems else f"{len(problems)} fault(s): {problems[0]}"
        print(f"  {row['interval']:>4} {row['ms']:8.1f}ms {row['bars']:5} "
              f"{row['oldest_days']:7.1f}d {row['coverage']:<12} {verdict}")
    print(f"\n  {len(rows) - bad}/{len(rows)} intervals correct against the venue "
          f"and their own arithmetic")
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
