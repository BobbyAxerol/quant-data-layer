#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid
from pathlib import Path

import jwt

from qdl.canary import PaperAlphaCanary, sdk_requirement
from qdl.consumer import ConsumerManifestLoader
from qdl.runtime.canary_bridge import (
    CanonicalV1Bridge,
    V1ReadOnlyBarSource,
    V1ReadOnlyBridgeConfig,
)
from qdl.runtime.canary_source import CanarySourceCatalog
from qdl_sdk import (
    AsyncDataLayerClient,
    FileCursorStore,
    GrpcStreamTransport,
    RestQueryTransport,
    StaticBearerCredential,
    StreamEvent,
)


def _token(manifest, *, key_id: str, secret: str, issuer: str, audience: str) -> str:
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
            "environment": manifest.environment,
            "roles": [
                "market_data_reader",
                "historical_reader",
                "stream_consumer",
            ],
            "consumer_manifest_revision": manifest.manifest_revision,
        },
        secret,
        algorithm="HS256",
        headers={"kid": key_id},
    )


def _client(
    manifest,
    *,
    query_url: str,
    grpc_target: str,
    key_id: str,
    secret: str,
    issuer: str,
    audience: str,
    cursor_path: Path,
) -> AsyncDataLayerClient:
    credential = StaticBearerCredential(_token(
        manifest,
        key_id=key_id,
        secret=secret,
        issuer=issuer,
        audience=audience,
    ))
    return AsyncDataLayerClient(
        query_transport=RestQueryTransport(
            query_url, credential_provider=credential
        ),
        stream_transport=GrpcStreamTransport(
            grpc_target,
            allow_insecure_loopback=True,
            credential_provider=credential,
        ),
        consumer_id=manifest.consumer_id,
        cursor_store=FileCursorStore(cursor_path),
        max_buffer_events=manifest.quotas.max_buffer_events,
    )


def _assert_v1_v2_parity(warmup, expected_rows, binding) -> None:
    expected = {int(row[0]) * 1_000_000: row for row in expected_rows}
    if not warmup.data:
        raise AssertionError("monitoring warmup returned no canonical rows")
    previous = -1
    for item in warmup.data:
        payload = item.payload
        row = expected.get(int(payload.open_time_ns))
        if row is None:
            raise AssertionError("V2 bar does not correspond to a V1 source row")
        checks = {
            "instrument_uid": item.instrument_uid == binding.instrument.instrument_uid,
            "instrument_id": item.instrument_id == binding.instrument.instrument_id,
            "interval": item.interval == binding.interval,
            "open": payload.open.source_text == str(row[1]),
            "high": payload.high.source_text == str(row[2]),
            "low": payload.low.source_text == str(row[3]),
            "close": payload.close.source_text == str(row[4]),
            "volume": payload.volume.source_text == str(row[5]),
            "close_time": int(payload.close_time_ns) == int(row[6]) * 1_000_000,
            "trade_count": int(payload.trade_count) == int(row[8]),
            "final": str(payload.lifecycle) in {"FINAL", "BarLifecycle.FINAL"},
            "source_id": item.source.source_id == binding.source_id,
            "source_role": item.source.source_role == binding.source_role,
            "authority": item.source.authoritative is binding.authoritative,
            "policy": item.quality.policy_id == binding.source_policy_id,
            "execution_forbidden": item.quality.execution_eligible is False,
        }
        failed = [name for name, passed in checks.items() if not passed]
        if failed:
            raise AssertionError(f"V1/V2 canonical parity failed: {failed}")
        if int(payload.open_time_ns) <= previous:
            raise AssertionError("V2 warmup bars are not strictly ordered")
        previous = int(payload.open_time_ns)


async def _observe_one(client, manifest) -> int:
    requirement = sdk_requirement(manifest)
    async with client.warmup_then_stream(requirement) as session:
        while True:
            item = await asyncio.wait_for(session.__anext__(), timeout=15.0)
            if not isinstance(item, StreamEvent):
                continue
            session.acknowledge(item)
            return item.logical_offset


async def _initial(args) -> dict:
    catalog = CanarySourceCatalog.load(args.source_bindings)
    binding = catalog.bindings[0]
    monitoring = ConsumerManifestLoader.load(args.monitoring_manifest)
    paper = ConsumerManifestLoader.load(args.paper_manifest)
    keys = json.loads(args.jwt_keys_json)
    key_ids = sorted(keys)
    if len(key_ids) < 2:
        raise ValueError("Phase 7.2 credential rotation requires two JWT keys")
    bridge_config = V1ReadOnlyBridgeConfig(
        source_catalog_path=args.source_bindings,
        v1_base_url=args.v1_base_url,
        ingest_urls=tuple(json.loads(args.ingest_urls_json)),
        ingest_secret=args.ingest_secret.encode(),
    )
    source = V1ReadOnlyBarSource(args.v1_base_url)
    bridge = CanonicalV1Bridge(
        config=bridge_config, catalog=catalog, source=source
    )
    monitoring_client = None
    paper_client = None
    rotated_client = None
    try:
        rows, envelopes = await bridge.prepare(binding, warmup=True)
        if len(envelopes) < 32:
            raise RuntimeError("real V1 source returned fewer than 32 closed bars")
        seed, monitor_event, paper_event = envelopes[:-2], envelopes[-2], envelopes[-1]
        seeded = await bridge.submit(seed)
        if seeded["accepted"] < 30:
            raise AssertionError("canonical bridge did not seed the required warmup")

        monitoring_client = _client(
            monitoring,
            query_url=args.query_url,
            grpc_target=args.grpc_target,
            key_id=key_ids[0],
            secret=keys[key_ids[0]],
            issuer=args.issuer,
            audience=args.audience,
            cursor_path=Path(args.state_dir) / "monitoring-cursor.json",
        )
        monitoring_warmup = await monitoring_client.warmup(
            sdk_requirement(monitoring)
        )
        _assert_v1_v2_parity(monitoring_warmup, rows, binding)
        monitor_task = asyncio.create_task(_observe_one(monitoring_client, monitoring))
        await asyncio.sleep(0.25)
        await bridge.submit((monitor_event,))
        monitoring_offset = await monitor_task

        paper_client = _client(
            paper,
            query_url=args.query_url,
            grpc_target=args.grpc_target,
            key_id=key_ids[0],
            secret=keys[key_ids[0]],
            issuer=args.issuer,
            audience=args.audience,
            cursor_path=Path(args.state_dir) / "paper-cursor.json",
        )
        paper_canary = PaperAlphaCanary(
            manifest=paper,
            client=paper_client,
            state_path=Path(args.state_dir) / "paper-state.json",
        )
        paper_task = asyncio.create_task(paper_canary.run(
            event_count=1,
            timeout_seconds=15.0,
        ))
        await asyncio.sleep(0.25)
        await bridge.submit((paper_event,))
        paper_result = await paper_task

        rotated_client = _client(
            monitoring,
            query_url=args.query_url,
            grpc_target=args.grpc_target,
            key_id=key_ids[1],
            secret=keys[key_ids[1]],
            issuer=args.issuer,
            audience=args.audience,
            cursor_path=Path(args.state_dir) / "rotated-cursor.json",
        )
        rotated = await rotated_client.warmup(sdk_requirement(monitoring))
        _assert_v1_v2_parity(rotated, rows, binding)
        result = {
            "schema": "qdl.phase7.2.initial-canary.v1",
            "authority": "V1_SHADOW_READ_ONLY",
            "source": "REAL_V1_PROVIDER_DATA",
            "monitoring_offset": monitoring_offset,
            "paper_checkpointed_offset": paper_result.checkpointed_offset,
            "paper_state_sha256": paper_result.signal_state_sha256,
            "paper_signal": paper_result.signal,
            "execution_dependency": paper_result.execution_dependency,
            "credential_rotation": f"{key_ids[0]}->{key_ids[1]}",
            "last_open_time_ns": int(paper_event.bar.open_time_ns),
            "seeded_events": seeded["accepted"],
            "v1_v2_mismatches": 0,
        }
        Path(args.output).write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return result
    finally:
        for client in (monitoring_client, paper_client, rotated_client):
            if client is not None:
                await client.close()
        await bridge.close()


async def _post_failover(args) -> dict:
    previous = json.loads(Path(args.initial_result).read_text(encoding="utf-8"))
    catalog = CanarySourceCatalog.load(args.source_bindings)
    binding = catalog.bindings[0]
    paper = ConsumerManifestLoader.load(args.paper_manifest)
    keys = json.loads(args.jwt_keys_json)
    key_id = sorted(keys)[-1]
    bridge_config = V1ReadOnlyBridgeConfig(
        source_catalog_path=args.source_bindings,
        v1_base_url=args.v1_base_url,
        ingest_urls=tuple(json.loads(args.ingest_urls_json)),
        ingest_secret=args.ingest_secret.encode(),
    )
    bridge = CanonicalV1Bridge(
        config=bridge_config,
        catalog=catalog,
        source=V1ReadOnlyBarSource(args.v1_base_url),
    )
    client = _client(
        paper,
        query_url=args.query_url,
        grpc_target=args.grpc_target,
        key_id=key_id,
        secret=keys[key_id],
        issuer=args.issuer,
        audience=args.audience,
        cursor_path=Path(args.state_dir) / "paper-cursor.json",
    )
    try:
        canary = PaperAlphaCanary(
            manifest=paper,
            client=client,
            state_path=Path(args.state_dir) / "paper-state-after-failover.json",
        )
        task = asyncio.create_task(canary.run(
            event_count=1,
            timeout_seconds=args.next_bar_timeout_seconds,
            resume_restored_state=True,
        ))
        deadline = time.monotonic() + args.next_bar_timeout_seconds
        next_event = None
        while time.monotonic() < deadline:
            _rows, envelopes = await bridge.prepare(binding, warmup=False)
            candidates = [
                item for item in envelopes
                if int(item.bar.open_time_ns) > int(previous["last_open_time_ns"])
            ]
            if candidates:
                next_event = candidates[-1]
                break
            await asyncio.sleep(2.0)
        if next_event is None:
            task.cancel()
            raise TimeoutError("no new real closed provider bar arrived after failover")
        await bridge.submit((next_event,))
        resumed = await task
        fresh_client = _client(
            paper,
            query_url=args.query_url,
            grpc_target=args.grpc_target,
            key_id=key_id,
            secret=keys[key_id],
            issuer=args.issuer,
            audience=args.audience,
            cursor_path=Path(args.state_dir) / "fresh-cursor.json",
        )
        try:
            fresh = PaperAlphaCanary(
                manifest=paper,
                client=fresh_client,
                state_path=Path(args.state_dir) / "paper-state-fresh.json",
            )
            rebuilt = await fresh.run(event_count=0, timeout_seconds=5.0)
        finally:
            await fresh_client.close()
        if resumed.signal_state_sha256 != rebuilt.signal_state_sha256:
            raise AssertionError("paper alpha restart reconstructed a different signal state")
        result = {
            "schema": "qdl.phase7.2.failover-canary.v1",
            "authority": "V1_SHADOW_READ_ONLY",
            "source": "REAL_V1_PROVIDER_DATA",
            "checkpoint_before": previous["paper_checkpointed_offset"],
            "checkpoint_after": resumed.checkpointed_offset,
            "state_sha256_after_resume": resumed.signal_state_sha256,
            "state_sha256_fresh_rebuild": rebuilt.signal_state_sha256,
            "state_mismatch": 0,
            "new_open_time_ns": int(next_event.bar.open_time_ns),
            "execution_dependency": resumed.execution_dependency,
        }
        Path(args.output).write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return result
    finally:
        await client.close()
        await bridge.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=("initial", "post-failover"))
    parser.add_argument("--source-bindings", required=True)
    parser.add_argument("--monitoring-manifest", required=True)
    parser.add_argument("--paper-manifest", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--initial-result")
    parser.add_argument("--v1-base-url", default="http://127.0.0.1:8100")
    parser.add_argument("--query-url", default="http://127.0.0.1:18100")
    parser.add_argument("--grpc-target", default="127.0.0.1:18110")
    parser.add_argument("--ingest-urls-json", required=True)
    parser.add_argument("--ingest-secret", required=True)
    parser.add_argument("--jwt-keys-json", required=True)
    parser.add_argument("--issuer", required=True)
    parser.add_argument("--audience", required=True)
    parser.add_argument("--next-bar-timeout-seconds", type=float, default=90.0)
    args = parser.parse_args()
    Path(args.state_dir).mkdir(parents=True, exist_ok=True)
    if args.stage == "post-failover" and not args.initial_result:
        parser.error("post-failover requires --initial-result")
    result = asyncio.run(_initial(args) if args.stage == "initial" else _post_failover(args))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
