#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import jwt

from qdl_sdk import (
    AsyncDataLayerClient,
    ControlEvent,
    DataRequirement,
    Feed,
    FileCursorStore,
    GapPolicy,
    Grade,
    GrpcStreamTransport,
    RecoveryPolicy,
    RestQueryTransport,
    StalePolicy,
    StaticBearerCredential,
    StreamEvent,
    market_data_view_from_stream,
)


@dataclass(frozen=True)
class VenueCase:
    venue: str
    instrument_uid: str
    consumer_id: str
    subject: str
    provider: str


CASES = (
    VenueCase(
        "BINANCE",
        "a953e16e-7138-5562-b5e8-c337a44d0b65",
        "alpha.binance.paper.stable",
        "spiffe://qdl/paper/alpha-binance-stable",
        "BINANCE_DIRECT",
    ),
    VenueCase(
        "OKX",
        "fb26214c-7b9b-5961-95b2-55154755af0f",
        "alpha.okx.paper.stable",
        "spiffe://qdl/paper/alpha-okx-stable",
        "OKX_DIRECT",
    ),
)


def token(subject: str, *, issuer: str, audience: str) -> str:
    keys = json.loads(os.environ["QDL_STABLE_JWT_KEYS_JSON"])
    key_id, secret = sorted(keys.items())[0]
    now = int(time.time())
    return jwt.encode(
        {
            "sub": subject,
            "iss": issuer,
            "aud": audience,
            "iat": now,
            "nbf": now - 1,
            "exp": now + 300,
            "jti": str(uuid.uuid4()),
            "environment": "paper",
            "roles": [
                "market_data_reader",
                "historical_reader",
                "stream_consumer",
            ],
            "consumer_manifest_revision": 1,
        },
        secret,
        algorithm="HS256",
        headers={"kid": key_id},
    )


def client(
    *,
    base_url: str,
    grpc_target: str,
    consumer_id: str,
    bearer: str,
    cursor_path: Path,
) -> AsyncDataLayerClient:
    credential = StaticBearerCredential(bearer)
    return AsyncDataLayerClient(
        query_transport=RestQueryTransport(
            base_url,
            timeout_seconds=8,
            credential_provider=credential,
        ),
        stream_transport=GrpcStreamTransport(
            grpc_target,
            allow_insecure_loopback=True,
            credential_provider=credential,
        ),
        consumer_id=consumer_id,
        cursor_store=FileCursorStore(cursor_path),
        max_buffer_events=64,
        max_reconnect_attempts=2,
    )


def bar_fingerprint(response) -> list[dict[str, object]]:
    values = []
    for item in response.data:
        if item.feed is not Feed.BAR:
            raise AssertionError("warmup returned a non-BAR item")
        if item.payload.lifecycle not in {"FINAL", "REVISED"}:
            raise AssertionError("warmup returned a non-final BAR")
        if (
            item.source.authoritative is not True
            or item.quality.complete is not True
            or item.quality.gap_open is not False
        ):
            raise AssertionError("warmup BAR failed authority/coverage gate")
        values.append(
            {
                "instrument_uid": item.instrument_uid,
                "instrument_id": item.instrument_id,
                "observed_at_ns": item.observed_at_ns,
                "payload": item.payload.model_dump(mode="json"),
                "source": item.source.model_dump(mode="json"),
                "contract": item.contract.model_dump(mode="json"),
            }
        )
    return values


async def next_data(session) -> tuple[StreamEvent, list[str]]:
    controls: list[str] = []
    for _ in range(8):
        item = await asyncio.wait_for(session.__anext__(), timeout=12)
        if isinstance(item, ControlEvent):
            controls.append(item.code)
            continue
        if not isinstance(item, StreamEvent):
            raise AssertionError("stream returned an unknown SDK event")
        return item, controls
    raise AssertionError("stream did not reach a market-data event")


async def certify_case(
    case: VenueCase,
    *,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    issuer: str,
    audience: str,
    state_dir: Path,
) -> dict[str, object]:
    bearer = token(case.subject, issuer=issuer, audience=audience)
    bar_requirement = DataRequirement(
        case.instrument_uid,
        Feed.BAR,
        Grade.ALPHA,
        "crypto_primary_v2",
        interval="1m",
        warmup_limit=5,
        max_freshness_ms=180_000,
        stale_policy=StalePolicy.BLOCK,
        gap_policy=GapPolicy.BLOCK,
        recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY,
    )
    primary = client(
        base_url=primary_url,
        grpc_target=grpc_target,
        consumer_id=case.consumer_id,
        bearer=bearer,
        cursor_path=state_dir / f"{case.venue.lower()}-query-primary.json",
    )
    secondary = client(
        base_url=secondary_url,
        grpc_target=grpc_target,
        consumer_id=case.consumer_id,
        bearer=bearer,
        cursor_path=state_dir / f"{case.venue.lower()}-query-secondary.json",
    )
    try:
        first = await primary.warmup(bar_requirement)
        second = await secondary.warmup(bar_requirement)
    finally:
        await primary.close()
        await secondary.close()
    first_rows = bar_fingerprint(first)
    second_rows = bar_fingerprint(second)
    if first_rows != second_rows:
        raise AssertionError(f"{case.venue} query replicas diverged")

    trade_requirement = DataRequirement(
        case.instrument_uid,
        Feed.TRADE,
        Grade.ALPHA,
        "crypto_primary_v2",
        warmup_limit=0,
        max_freshness_ms=15_000,
        stale_policy=StalePolicy.BLOCK,
        gap_policy=GapPolicy.BLOCK,
        recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY,
    )
    cursor_path = state_dir / f"{case.venue.lower()}-stream.json"
    first_client = client(
        base_url=primary_url,
        grpc_target=grpc_target,
        consumer_id=case.consumer_id,
        bearer=bearer,
        cursor_path=cursor_path,
    )
    try:
        async with first_client.warmup_then_stream(trade_requirement) as session:
            first_event, first_controls = await next_data(session)
            first_view = market_data_view_from_stream(
                first_event,
                template=session.warmup.data[-1],
                requirement=trade_requirement,
            )
            if (
                first_view.source.provider != case.provider
                or not first_view.source.authoritative
                or not first_view.quality.complete
                or first_view.quality.gap_open
            ):
                raise AssertionError(f"{case.venue} first stream event failed quality")
            session.acknowledge(first_event)
    finally:
        await first_client.close()

    resumed_client = client(
        base_url=secondary_url,
        grpc_target=grpc_target,
        consumer_id=case.consumer_id,
        bearer=bearer,
        cursor_path=cursor_path,
    )
    try:
        async with resumed_client.warmup_then_stream(
            trade_requirement,
            resume_restored_state=True,
        ) as session:
            resumed_event, resumed_controls = await next_data(session)
            resumed_view = market_data_view_from_stream(
                resumed_event,
                template=session.warmup.data[-1],
                requirement=trade_requirement,
            )
            if resumed_event.logical_offset != first_event.logical_offset + 1:
                raise AssertionError(
                    f"{case.venue} cursor resume was not contiguous: "
                    f"{first_event.logical_offset}->{resumed_event.logical_offset}"
                )
            if resumed_view.source.provider != case.provider:
                raise AssertionError(f"{case.venue} resumed source changed")
            session.acknowledge(resumed_event)
    finally:
        await resumed_client.close()

    persisted = json.loads(cursor_path.read_text(encoding="utf-8"))
    offsets = [
        int(item["offset"])
        for item in persisted["items"].values()
    ]
    if offsets != [resumed_event.logical_offset]:
        raise AssertionError(f"{case.venue} durable cursor was not ACKed")

    return {
        "venue": case.venue,
        "provider": case.provider,
        "warmup_rows": len(first_rows),
        "coverage": first.coverage,
        "final_bar_close_time_ns": first_rows[-1]["payload"]["close_time_ns"],
        "first_stream_offset": first_event.logical_offset,
        "resumed_stream_offset": resumed_event.logical_offset,
        "first_controls": first_controls,
        "resumed_controls": resumed_controls,
        "cursor_persisted": True,
        "secret_values_recorded": False,
    }


async def run(args: argparse.Namespace) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="qdl-c1-sdk-") as temporary:
        state_dir = Path(temporary)
        results = []
        for case in CASES:
            results.append(
                await certify_case(
                    case,
                    primary_url=args.primary_url,
                    secondary_url=args.secondary_url,
                    grpc_target=args.grpc_target,
                    issuer=args.issuer,
                    audience=args.audience,
                    state_dir=state_dir,
                )
            )
    return {
        "schema": "qdl.phase-c1.isolated-consumer-acceptance.v1",
        "status": "PASS",
        "contract_version": "2.0.0",
        "authority_expected": "RUST_SHADOW",
        "cases": results,
        "secret_values_recorded": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary-url", default="http://127.0.0.1:18201")
    parser.add_argument("--secondary-url", default="http://127.0.0.1:18202")
    parser.add_argument("--grpc-target", required=True)
    parser.add_argument(
        "--issuer", default="https://identity.qdl.stable.internal"
    )
    parser.add_argument("--audience", default="qdl-v2-stable")
    args = parser.parse_args()
    if "QDL_STABLE_JWT_KEYS_JSON" not in os.environ:
        raise SystemExit("QDL_STABLE_JWT_KEYS_JSON is required")
    print(json.dumps(asyncio.run(run(args)), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
