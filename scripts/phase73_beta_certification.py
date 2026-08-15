#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
import uuid
from pathlib import Path

import httpx
import jwt

from qdl.canary import sdk_requirement
from qdl.consumer import ConsumerManifestLoader
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.runtime.canary_bridge import (
    CanonicalV1Bridge,
    V1ReadOnlyBarSource,
    V1ReadOnlyBridgeConfig,
)
from qdl.runtime.canary_source import CanarySourceCatalog
from qdl.transport import Cursor, SQLiteDurableSpool, SpoolConfig
from qdl_sdk import GrpcStreamTransport, StaticBearerCredential, StreamEvent
from qdl_sdk.errors import CursorExpiredError, DataLayerError, SlowConsumerError


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, math.ceil(percentile * len(ordered)) - 1)
    return round(ordered[rank], 3)


def _token(
    manifest,
    *,
    key_id: str,
    secret: str,
    issuer: str,
    audience: str,
    environment: str | None = None,
    roles: tuple[str, ...] = (
        "market_data_reader",
        "historical_reader",
        "stream_consumer",
    ),
) -> str:
    now = int(time.time())
    return jwt.encode(
        {
            "sub": manifest.subject,
            "iss": issuer,
            "aud": audience,
            "iat": now,
            "nbf": now - 1,
            "exp": now + 600,
            "jti": str(uuid.uuid4()),
            "environment": environment or manifest.environment,
            "roles": list(roles),
            "consumer_manifest_revision": manifest.manifest_revision,
        },
        secret,
        algorithm="HS256",
        headers={"kid": key_id},
    )


def _headers(manifest, token: str, *, purpose: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "X-QDL-Consumer-ID": manifest.consumer_id,
        "X-QDL-Purpose": purpose,
    }


async def _query_profile(
    client: httpx.AsyncClient,
    *,
    path: str,
    params: dict,
    headers: dict[str, str],
    requests: int,
    concurrency: int,
) -> dict:
    semaphore = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    response_bytes = 0
    errors = 0

    async def one() -> None:
        nonlocal errors, response_bytes
        async with semaphore:
            started = time.perf_counter_ns()
            response = await client.get(path, params=params, headers=headers)
            latencies.append((time.perf_counter_ns() - started) / 1_000_000)
            response_bytes += len(response.content)
            if response.status_code != 200:
                errors += 1

    started = time.perf_counter()
    await asyncio.gather(*(one() for _ in range(requests)))
    elapsed = max(time.perf_counter() - started, 0.000001)
    return {
        "requests": requests,
        "concurrency": concurrency,
        "requests_per_second": round(requests / elapsed, 3),
        "response_bytes_per_second": round(response_bytes / elapsed, 3),
        "errors": errors,
        "latency_ms": {
            "p50": _percentile(latencies, 0.50),
            "p95": _percentile(latencies, 0.95),
            "p99": _percentile(latencies, 0.99),
            "p99_9": _percentile(latencies, 0.999),
        },
    }


async def _first_control(iterator) -> None:
    item = await asyncio.wait_for(iterator.__anext__(), timeout=10.0)
    if getattr(item, "code", "") != "REPLAYING":
        raise AssertionError("stream did not begin with an explicit replay control")


async def _consume(iterator, expected: int) -> dict:
    latencies: list[float] = []
    event_bytes = 0
    offsets: list[int] = []
    while len(offsets) < expected:
        item = await asyncio.wait_for(iterator.__anext__(), timeout=20.0)
        if not isinstance(item, StreamEvent):
            continue
        offsets.append(item.logical_offset)
        event_bytes += item.event.ByteSize()
        latencies.append(max(0, time.time_ns() - item.event.published_at_ns) / 1_000_000)
    return {
        "events": len(offsets),
        "first_offset": offsets[0],
        "last_offset": offsets[-1],
        "contiguous": offsets == list(range(offsets[0], offsets[-1] + 1)),
        "event_bytes": event_bytes,
        "delivery_latency_ms": {
            "p50": _percentile(latencies, 0.50),
            "p95": _percentile(latencies, 0.95),
            "p99": _percentile(latencies, 0.99),
            "p99_9": _percentile(latencies, 0.999),
        },
    }


async def certify(args) -> dict:
    catalog = CanarySourceCatalog.load(args.source_bindings)
    binding = catalog.bindings[0]
    monitoring = ConsumerManifestLoader.load(args.monitoring_manifest)
    paper = ConsumerManifestLoader.load(args.paper_manifest)
    capacity = ConsumerManifestLoader.load(args.capacity_manifest)
    keys: dict[str, str] = json.loads(args.jwt_keys_json)
    key_ids = sorted(keys)
    if len(key_ids) < 2:
        raise ValueError("Phase 7.3 requires two workload verification keys")

    bridge = CanonicalV1Bridge(
        config=V1ReadOnlyBridgeConfig(
            source_catalog_path=args.source_bindings,
            v1_base_url=args.v1_base_url,
            ingest_urls=tuple(json.loads(args.ingest_urls_json)),
            ingest_secret=args.ingest_secret.encode(),
        ),
        catalog=catalog,
        source=V1ReadOnlyBarSource(args.v1_base_url),
    )
    spool = SQLiteDurableSpool(SpoolConfig(
        path=Path(args.spool_path),
        min_free_disk_bytes=0,
        consumer_ttl_seconds=args.cursor_ttl_seconds,
    ))
    handoff = GapFreeHandoff(
        spool,
        SignedHandoffCursorCodec(
            {key: value.encode() for key, value in json.loads(args.cursor_keys_json).items()},
            active_key_id=args.cursor_active_key_id,
        ),
        checkpoint_ttl_seconds=args.cursor_ttl_seconds,
    )
    clients: list[httpx.AsyncClient] = []
    transports: list[GrpcStreamTransport] = []
    iterators = []
    try:
        rows, envelopes = await bridge.prepare(binding, warmup=True)
        if len(envelopes) < 64:
            raise RuntimeError("real provider returned fewer than 64 closed bars")
        tail_count = min(2, len(envelopes) - 32)
        initial = envelopes[:-tail_count]
        tail = envelopes[-tail_count:]
        seeded = await bridge.submit(initial)
        if seeded["accepted"] != len(initial):
            raise AssertionError("isolated spool was not clean before Phase 7.3")

        high_before = spool.high_watermark(binding.stream, binding.partition_key)
        cursor_offset = max(1, high_before - min(64, high_before - 1))
        expected_events = high_before - cursor_offset + len(tail)
        if expected_events < 32:
            raise AssertionError("fan-out replay window is too small")

        stream_specs = (
            (monitoring, key_ids[0]),
            (monitoring, key_ids[1]),
            (paper, key_ids[0]),
            (paper, key_ids[1]),
        )
        for manifest, key_id in stream_specs:
            grant = handoff.issue(
                consumer_id=manifest.consumer_id,
                snapshot_id=f"phase73-{manifest.consumer_id}",
                snapshot_watermark=Cursor(
                    binding.stream, binding.partition_key, cursor_offset
                ),
                ttl_seconds=args.cursor_ttl_seconds,
            )
            credential = StaticBearerCredential(_token(
                manifest,
                key_id=key_id,
                secret=keys[key_id],
                issuer=args.issuer,
                audience=args.audience,
            ))
            transport = GrpcStreamTransport(
                args.grpc_target,
                allow_insecure_loopback=True,
                credential_provider=credential,
            )
            iterator = transport.subscribe(
                sdk_requirement(manifest),
                consumer_id=manifest.consumer_id,
                cursor_token=grant.token,
                max_buffer_events=(1 if len(iterators) == 3 else 500),
            ).__aiter__()
            await _first_control(iterator)
            transports.append(transport)
            iterators.append(iterator)

        started = time.perf_counter()
        fast_tasks = [
            asyncio.create_task(_consume(iterator, expected_events))
            for iterator in iterators[:3]
        ]
        await asyncio.sleep(0.1)
        tail_result = await bridge.submit(tail)
        if tail_result["accepted"] != len(tail):
            raise AssertionError("real provider tail was not committed exactly once")
        fast_results = await asyncio.gather(*fast_tasks)
        stream_elapsed = max(time.perf_counter() - started, 0.000001)

        slow_events = 0
        slow_disconnected = False
        try:
            while True:
                item = await asyncio.wait_for(iterators[3].__anext__(), timeout=20.0)
                if isinstance(item, StreamEvent):
                    slow_events += 1
        except SlowConsumerError:
            slow_disconnected = True
        if not slow_disconnected:
            raise AssertionError("intentionally slow consumer was not explicitly disconnected")

        requirement = sdk_requirement(capacity)
        capacity_token = _token(
            capacity,
            key_id=key_ids[0],
            secret=keys[key_ids[0]],
            issuer=args.issuer,
            audience=args.audience,
        )
        query_client = httpx.AsyncClient(
            base_url=args.query_url,
            timeout=httpx.Timeout(10.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
        clients.append(query_client)
        path = f"/v2/market-data/{requirement.instrument_uid}/warmup"
        query_headers = _headers(capacity, capacity_token, purpose="INTERNAL_RESEARCH")
        normal = await _query_profile(
            query_client,
            path=path,
            params=requirement.query_params(),
            headers=query_headers,
            requests=args.normal_requests,
            concurrency=args.normal_concurrency,
        )
        burst = await _query_profile(
            query_client,
            path=path,
            params=requirement.query_params(),
            headers=query_headers,
            requests=args.burst_requests,
            concurrency=args.burst_concurrency,
        )

        auth_results = {}
        probe_path = "/v2/instruments"
        no_auth = await query_client.get(probe_path)
        auth_results["missing_token"] = no_auth.status_code
        wrong_audience = _token(
            capacity,
            key_id=key_ids[0], secret=keys[key_ids[0]], issuer=args.issuer,
            audience="wrong-audience",
        )
        auth_results["wrong_audience"] = (await query_client.get(
            probe_path,
            headers=_headers(capacity, wrong_audience, purpose="INTERNAL_RESEARCH"),
        )).status_code
        wrong_environment = _token(
            capacity,
            key_id=key_ids[0], secret=keys[key_ids[0]], issuer=args.issuer,
            audience=args.audience, environment="live",
        )
        auth_results["wrong_environment"] = (await query_client.get(
            probe_path,
            headers=_headers(capacity, wrong_environment, purpose="INTERNAL_RESEARCH"),
        )).status_code
        no_scope = _token(
            capacity,
            key_id=key_ids[0], secret=keys[key_ids[0]], issuer=args.issuer,
            audience=args.audience, roles=(),
        )
        auth_results["missing_scope"] = (await query_client.get(
            probe_path,
            headers=_headers(capacity, no_scope, purpose="INTERNAL_RESEARCH"),
        )).status_code
        mismatch_headers = _headers(capacity, capacity_token, purpose="INTERNAL_RESEARCH")
        mismatch_headers["X-QDL-Consumer-ID"] = paper.consumer_id
        auth_results["consumer_mismatch"] = (await query_client.get(
            probe_path, headers=mismatch_headers,
        )).status_code
        for key_id in key_ids[:2]:
            rotated = _token(
                capacity, key_id=key_id, secret=keys[key_id], issuer=args.issuer,
                audience=args.audience,
            )
            auth_results[f"rotation_{key_id}"] = (await query_client.get(
                probe_path,
                headers=_headers(capacity, rotated, purpose="INTERNAL_RESEARCH"),
            )).status_code

        malformed = await query_client.get(
            path,
            params=requirement.query_params() | {"feed": "NOT_A_FEED"},
            headers=query_headers,
        )
        oversized = await query_client.post(
            probe_path,
            content=b"x" * (args.max_request_bytes + 1),
            headers=query_headers,
        )

        monitor_token = _token(
            monitoring,
            key_id=key_ids[0], secret=keys[key_ids[0]], issuer=args.issuer,
            audience=args.audience,
        )
        monitor_headers = _headers(
            monitoring, monitor_token, purpose="INTERNAL_RESEARCH"
        )
        rate_codes = []
        for _ in range(monitoring.quotas.requests_per_minute + 2):
            response = await query_client.get(probe_path, headers=monitor_headers)
            rate_codes.append(response.status_code)
            if response.status_code == 429:
                break

        capacity_grant = handoff.issue(
            consumer_id=capacity.consumer_id,
            snapshot_id="phase73-capacity-security",
            snapshot_watermark=Cursor(
                binding.stream, binding.partition_key, high_before
            ),
            ttl_seconds=args.cursor_ttl_seconds,
        )
        tampered = capacity_grant.token[:-1] + (
            "A" if capacity_grant.token[-1] != "A" else "B"
        )
        tamper_transport = GrpcStreamTransport(
            args.grpc_target,
            allow_insecure_loopback=True,
            credential_provider=StaticBearerCredential(capacity_token),
        )
        transports.append(tamper_transport)
        try:
            tamper_iter = tamper_transport.subscribe(
                requirement,
                consumer_id=capacity.consumer_id,
                cursor_token=tampered,
                max_buffer_events=10,
            ).__aiter__()
            await tamper_iter.__anext__()
            cursor_tamper = "FAIL_OPEN"
        except DataLayerError as error:
            cursor_tamper = error.code

        expired = handoff.issue(
            consumer_id=capacity.consumer_id,
            snapshot_id="phase73-expired",
            snapshot_watermark=Cursor(
                binding.stream, binding.partition_key, high_before
            ),
            ttl_seconds=1,
        ).token
        await asyncio.sleep(1.05)
        expiry_transport = GrpcStreamTransport(
            args.grpc_target,
            allow_insecure_loopback=True,
            credential_provider=StaticBearerCredential(capacity_token),
        )
        transports.append(expiry_transport)
        try:
            expiry_iter = expiry_transport.subscribe(
                requirement,
                consumer_id=capacity.consumer_id,
                cursor_token=expired,
                max_buffer_events=10,
            ).__aiter__()
            await expiry_iter.__anext__()
            cursor_expiry = "FAIL_OPEN"
        except CursorExpiredError as error:
            cursor_expiry = error.code

        cursor_mismatch_transport = GrpcStreamTransport(
            args.grpc_target,
            allow_insecure_loopback=True,
            credential_provider=StaticBearerCredential(_token(
                paper, key_id=key_ids[0], secret=keys[key_ids[0]],
                issuer=args.issuer, audience=args.audience,
            )),
        )
        transports.append(cursor_mismatch_transport)
        try:
            mismatch_iter = cursor_mismatch_transport.subscribe(
                sdk_requirement(paper),
                consumer_id=paper.consumer_id,
                cursor_token=capacity_grant.token,
                max_buffer_events=10,
            ).__aiter__()
            await mismatch_iter.__anext__()
            cursor_scope = "FAIL_OPEN"
        except DataLayerError as error:
            cursor_scope = error.code

        latest_close_ns = int(tail[-1].bar.close_time_ns)
        final_high = spool.high_watermark(binding.stream, binding.partition_key)
        stream_events = sum(item["events"] for item in fast_results)
        stream_bytes = sum(item["event_bytes"] for item in fast_results)
        stream = {
            "fanout_consumers": 4,
            "fast_consumers": 3,
            "slow_consumers": 1,
            "replayed_events_per_fast_consumer": expected_events,
            "events_per_second": round(stream_events / stream_elapsed, 3),
            "bytes_per_second": round(stream_bytes / stream_elapsed, 3),
            "cursor_lag_before": high_before - cursor_offset,
            "cursor_lag_after": 0,
            "replay_lag_after": 0,
            "subscriber_peak": 4,
            "disconnect_count": 1,
            "replay_count": 4,
            "slow_consumer_events_before_disconnect": slow_events,
            "slow_consumer_explicit_disconnect": slow_disconnected,
            "fast_results": fast_results,
        }
        security = {
            "auth_status_codes": auth_results,
            "malformed_request_status": malformed.status_code,
            "oversized_request_status": oversized.status_code,
            "rate_limit_status": rate_codes[-1],
            "rate_limit_requests_until_reject": len(rate_codes),
            "cursor_tamper": cursor_tamper,
            "cursor_expiry": cursor_expiry,
            "cursor_consumer_scope": cursor_scope,
        }
        thresholds = {
            "normal_min_requests_per_second": args.normal_min_rps,
            "burst_min_requests_per_second": args.burst_min_rps,
            "query_max_p99_9_ms": args.max_query_p999_ms,
            "max_end_to_end_freshness_ms": args.max_freshness_ms,
            "max_error_budget_fraction": 0.0,
        }
        checks = {
            "normal_rate": normal["requests_per_second"] >= args.normal_min_rps,
            "burst_rate": burst["requests_per_second"] >= args.burst_min_rps,
            "query_tail": max(
                normal["latency_ms"]["p99_9"], burst["latency_ms"]["p99_9"]
            ) <= args.max_query_p999_ms,
            "query_errors": normal["errors"] == 0 and burst["errors"] == 0,
            "stream_contiguous": all(item["contiguous"] for item in fast_results),
            "stream_drained": all(
                item["last_offset"] == final_high for item in fast_results
            ),
            "slow_consumer_isolated": slow_disconnected,
            "freshness": max(0, time.time_ns() - latest_close_ns) / 1_000_000
            <= args.max_freshness_ms,
            "auth_fail_closed": all(
                code in {401, 403}
                for name, code in auth_results.items()
                if not name.startswith("rotation_")
            ),
            "rotation": all(
                code == 200 for name, code in auth_results.items()
                if name.startswith("rotation_")
            ),
            "malformed": malformed.status_code in {400, 422},
            "oversized": oversized.status_code == 413,
            "rate_limit": rate_codes[-1] == 429,
            "cursor_tamper": cursor_tamper == "CURSOR_INVALID",
            "cursor_expiry": cursor_expiry == "CURSOR_EXPIRED",
            "cursor_scope": cursor_scope == "CURSOR_INVALID",
        }
        result = {
            "schema": "qdl.phase7.3.beta-certification.v1",
            "status": "PASS" if all(checks.values()) else "FAIL",
            "authority": "V1_SHADOW_READ_ONLY",
            "source": "REAL_V1_PROVIDER_DATA",
            "generated_market_events": 0,
            "provider_rows": len(rows),
            "latest_closed_bar_ns": latest_close_ns,
            "end_to_end_freshness_ms": round(
                max(0, time.time_ns() - latest_close_ns) / 1_000_000, 3
            ),
            "normal": normal,
            "burst": burst,
            "stream": stream,
            "security": security,
            "thresholds": thresholds,
            "checks": checks,
            "error_budget_consumption": 0.0,
        }
        Path(args.output).write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        if result["status"] != "PASS":
            raise RuntimeError(f"Phase 7.3 certification gates failed: {checks}")
        return result
    finally:
        for iterator in iterators:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()
        for transport in transports:
            await transport.close()
        for client in clients:
            await client.aclose()
        spool.close()
        await bridge.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-bindings", required=True)
    parser.add_argument("--monitoring-manifest", required=True)
    parser.add_argument("--paper-manifest", required=True)
    parser.add_argument("--capacity-manifest", required=True)
    parser.add_argument("--spool-path", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--v1-base-url", default="http://127.0.0.1:8100")
    parser.add_argument("--query-url", default="http://127.0.0.1:18100")
    parser.add_argument("--grpc-target", default="127.0.0.1:18110")
    parser.add_argument("--ingest-urls-json", required=True)
    parser.add_argument("--ingest-secret", required=True)
    parser.add_argument("--jwt-keys-json", required=True)
    parser.add_argument("--cursor-keys-json", required=True)
    parser.add_argument("--cursor-active-key-id", default="beta-k1")
    parser.add_argument("--cursor-ttl-seconds", type=int, default=3600)
    parser.add_argument("--issuer", required=True)
    parser.add_argument("--audience", required=True)
    parser.add_argument("--max-request-bytes", type=int, default=1048576)
    parser.add_argument("--normal-requests", type=int, default=30)
    parser.add_argument("--normal-concurrency", type=int, default=5)
    parser.add_argument("--burst-requests", type=int, default=60)
    parser.add_argument("--burst-concurrency", type=int, default=20)
    parser.add_argument("--normal-min-rps", type=float, default=10.0)
    parser.add_argument("--burst-min-rps", type=float, default=20.0)
    parser.add_argument("--max-query-p999-ms", type=float, default=1000.0)
    parser.add_argument("--max-freshness-ms", type=int, default=240000)
    args = parser.parse_args()
    result = asyncio.run(certify(args))
    print(json.dumps({
        "status": result["status"],
        "normal_rps": result["normal"]["requests_per_second"],
        "burst_rps": result["burst"]["requests_per_second"],
        "stream_events_per_second": result["stream"]["events_per_second"],
        "freshness_ms": result["end_to_end_freshness_ms"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
