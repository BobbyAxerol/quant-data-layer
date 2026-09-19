#!/usr/bin/env python3
"""How long the data layer takes to answer, timed from where a consumer stands.

Read-only. It issues ordinary reads with the trading system's own identity and
times them; it writes nothing and changes no runtime.

Why it exists, and what it is not. Earlier reports in this program measured the
age of a cached value and called it latency. For a bar those are different
things: a 1m bar sitting 38 s old is correct - the bar closed 38 s ago and the
next one is not due yet. Age follows the interval and always will. What a caller
needs to know is **how long a request takes to come back**, and that is what
this times: the wall clock around each call, repeated, reported as min/p50/p95.

It goes through `AsyncDataLayerClient` over `RestQueryTransport` with the
trading system's mTLS certificate and RS256 credential - the same path
`market_data_service` uses - and reads its requirements out of the consumer
manifest, so it asks for exactly what that consumer asks for rather than
something convenient. Run it on the stack network: that puts it one network hop
away, outside the data layer's containers, where a caller actually sits.

    docker run --rm --network <stack>_stable_internal \
      -v /home/bobby/data_layer:/src:ro -v <identities>:/id:ro \
      -e QDL_ROUNDS=12 --entrypoint python qdl-v2-python:<tag> \
      -B /src/scripts/measure_consumer_request_latency.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yaml                                                            # noqa: E402
from qdl_sdk.client import AsyncDataLayerClient                        # noqa: E402
from qdl_sdk.credentials import RotatingJwtCredentialProvider          # noqa: E402
from qdl_sdk.models import DataRequirement, Feed, Grade                # noqa: E402
from qdl_sdk.tls import WorkloadTlsConfig                              # noqa: E402
from qdl_sdk.transport import GrpcStreamTransport, RestQueryTransport  # noqa: E402

ID = os.environ.get("QDL_ID_DIR", "/id")
BASE_URL = os.environ.get("QDL_QUERY_URL", "https://qdl-v2-query:8200")
GRPC = os.environ.get("QDL_GRPC_TARGET", "qdl-v2-stream-a:8210")
CONSUMER = os.environ.get("QDL_CONSUMER_ID", "trading-system.paper.stable")
MANIFEST = Path(os.environ.get(
    "QDL_MANIFEST", str(ROOT / "consumers/stable/trading-system-paper.yaml")))
ROUNDS = int(os.environ.get("QDL_ROUNDS", "12"))
# One instrument per feed keeps the probe bounded; the manifest decides which.
PER_FEED = int(os.environ.get("QDL_PER_FEED", "2"))


def transports():
    tls = WorkloadTlsConfig(
        f"{ID}/trading-system/ca.crt",
        f"{ID}/trading-system/client.crt",
        f"{ID}/trading-system/client.key",
    )
    credential = RotatingJwtCredentialProvider(
        private_key_file=f"{ID}/trading-system-jwt/private.key",
        key_id=os.environ.get("QDL_JWT_KEY_ID", "stable-trading-system-rs256-v1"),
        algorithm="RS256",
        issuer=os.environ.get("QDL_JWT_ISSUER", "https://identity.qdl.stable.internal"),
        audience=os.environ.get("QDL_JWT_AUDIENCE", "qdl-v2-stable"),
        subject=os.environ.get("QDL_SUBJECT", "spiffe://qdl/paper/trading-system-stable"),
        environment=os.environ.get("QDL_JWT_ENVIRONMENT", "paper"),
        # The three roles `_ROLE_PERMISSIONS` grants a reader
        # (qdl/security/policy.py:24): market data, history, stream. The
        # manifest lists permissions, not roles - a token built from those
        # strings is refused with "unknown or empty roles".
        roles=tuple(os.environ.get(
            "QDL_JWT_ROLES",
            "market_data_reader,historical_reader,stream_consumer",
        ).split(",")),
            # DATA_LAYER_V2_JWT_MANIFEST_REVISION on market_data_service. The data
        # plane compares this exactly and answers 401 on a mismatch, so it is
        # read from the environment rather than defaulted to something tidy.
        consumer_manifest_revision=int(os.environ.get("QDL_MANIFEST_REVISION", "9")),
    )
    return (
        RestQueryTransport(BASE_URL, timeout_seconds=20.0,
                           credential_provider=credential, tls=tls),
        GrpcStreamTransport(GRPC, tls=tls, credential_provider=credential),
    )


def requirements() -> list[tuple[str, DataRequirement]]:
    """Exactly what the consumer manifest asks for, capped per feed.

    Every field is carried across, not a convenient subset: the data plane
    compares a requirement against the registered manifest and answers "data
    requirement is outside the registered consumer manifest" if one policy
    field is missing. The enums are resolved by name from the SDK's own type
    hints, so a field added to the manifest later is carried without editing
    this list.
    """

    import dataclasses
    import typing

    hints = typing.get_type_hints(DataRequirement)
    spec = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))["spec"]
    picked: dict[str, int] = {}
    out: list[tuple[str, DataRequirement]] = []
    names = {f.name for f in dataclasses.fields(DataRequirement)}
    for item in spec["requirements"]:
        feed = str(item["feed"])
        if picked.get(feed, 0) >= PER_FEED:
            continue
        picked[feed] = picked.get(feed, 0) + 1
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
        out.append((f"{feed}{'/' + str(item['interval']) if item.get('interval') else ''}",
                    DataRequirement(**kwargs)))
    return out


def summarise(label: str, call: str, samples: list[float], error: str | None) -> dict:
    if not samples:
        return {"product": label, "call": call, "n": 0, "error": error}
    ordered = sorted(samples)
    return {
        "product": label,
        "call": call,
        "n": len(ordered),
        "min_ms": round(ordered[0], 1),
        "p50_ms": round(statistics.median(ordered), 1),
        "p95_ms": round(ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], 1),
        "max_ms": round(ordered[-1], 1),
        "error": error,
    }


async def time_calls(fn, rounds: int) -> tuple[list[float], str | None]:
    samples: list[float] = []
    error: str | None = None
    for _ in range(rounds):
        started = time.perf_counter()
        try:
            await fn()
        except Exception as exc:                     # noqa: BLE001 - reported, not raised
            error = f"{type(exc).__name__}: {exc}"[:160]
            break
        samples.append((time.perf_counter() - started) * 1000.0)
    return samples, error


async def stream_delivery(client, seconds: float) -> list[dict]:
    """How old a record is when it reaches a consumer over the live stream.

    This is the half a request timing cannot see. A snapshot call says how fast
    the data layer answers when asked; a stream says how long after the venue
    stamped an event the consumer is holding it. The clock is this process's,
    the event time is the venue's, so the number includes every hop between
    them - the wire, the ingestor, Kafka, the core, the projector and the
    stream - and nothing of the consumer's own processing.
    """

    out: list[dict] = []
    for label, requirement in requirements():
        if requirement.feed is Feed.BAR:
            continue                      # a bar's delivery is bounded by its interval
        ages: list[float] = []
        error: str | None = None
        deadline = time.monotonic() + seconds
        try:
            async with client.warmup_then_stream(requirement) as session:
                # The session is the async iterator; `market_data_service`
                # drives it with `session.__anext__()` and nothing else.
                while time.monotonic() < deadline and len(ages) < 400:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    event = await asyncio.wait_for(session.__anext__(), timeout=remaining)
                    envelope = getattr(event, "event", None)
                    stamped = getattr(envelope, "source_event_time_ns", 0) if envelope else 0
                    if stamped:
                        ages.append((time.time() * 1e9 - stamped) / 1e6)
        except (asyncio.TimeoutError, StopAsyncIteration):
            pass                          # the window closed, which is not a fault
        except Exception as exc:          # noqa: BLE001 - reported, not raised
            error = f"{type(exc).__name__}: {exc}"[:160]
        out.append(summarise(label, "stream delivery", ages, error))
    return out


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=ROUNDS)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--stream-seconds", type=float,
                        default=float(os.environ.get("QDL_STREAM_SECONDS", "0")),
                        help="also time live stream delivery per feed, for this long each")
    args = parser.parse_args()

    query, stream = transports()
    client = AsyncDataLayerClient(
        query_transport=query, stream_transport=stream, consumer_id=CONSUMER,
    )
    results: list[dict] = []
    try:
        for label, requirement in requirements():
            is_bar = requirement.feed is Feed.BAR
            call = "warmup" if (is_bar and requirement.warmup_limit) else "snapshot"
            fn = (lambda r=requirement: client.warmup(r)) if call == "warmup" \
                else (lambda r=requirement: client.snapshot(r))
            samples, error = await time_calls(fn, args.rounds)
            results.append(summarise(label, call, samples, error))
        samples, error = await time_calls(
            lambda: client.resolve_instrument(
                venue="BINANCE", product_type="PERPETUAL",
                native_symbol="BTCUSDT", consumer_grade=Grade.EXECUTION, market="USDM",
            ), args.rounds)
        results.append(summarise("instrument", "resolve_instrument", samples, error))
        if args.stream_seconds:
            results.extend(await stream_delivery(client, args.stream_seconds))
    finally:
        await client.close()

    if args.json:
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0
    print(f"  {'product':22} {'call':18} {'n':>3} {'min':>9} {'p50':>9} {'p95':>9} {'max':>9}")
    for row in results:
        if not row["n"]:
            print(f"  {row['product']:22} {row['call']:18}   -   {row.get('error')}")
            continue
        print(f"  {row['product']:22} {row['call']:18} {row['n']:3} "
              f"{row['min_ms']:8.1f}ms {row['p50_ms']:8.1f}ms "
              f"{row['p95_ms']:8.1f}ms {row['max_ms']:8.1f}ms")
    return 0 if all(r["n"] for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
