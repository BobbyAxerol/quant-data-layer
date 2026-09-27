#!/usr/bin/env python3
"""Strict no-order Query/Stream acceptance for separately pinned consumer realms."""
import argparse
import asyncio
import json
import time
from pathlib import Path

import yaml
from qdl_sdk import DataRequirement, Feed, Grade, StalePolicy, StreamEvent
from qdl_sdk.client import AsyncDataLayerClient
from qdl_sdk.credentials import RotatingJwtCredentialProvider
from qdl_sdk.errors import DataLayerError
from qdl_sdk.models import GapPolicy, RecoveryPolicy, BarRevisionPolicy
from qdl_sdk.tls import WorkloadTlsConfig
from qdl_sdk.transport import RestQueryTransport, GrpcStreamTransport


def requirement(row):
    return DataRequirement(instrument_uid=row["instrument_uid"], feed=Feed(row["feed"]),
        consumer_grade=Grade(row["consumer_grade"]), source_policy_id=row["source_policy_id"],
        interval=row.get("interval"), warmup_limit=0, max_freshness_ms=row.get("max_freshness_ms"),
        event_recency_policy=StalePolicy(row["event_recency_policy"]) if row.get("event_recency_policy") else None,
        max_session_liveness_ms=row.get("max_session_liveness_ms"),
        require_full_coverage=row.get("require_full_coverage", True),
        require_final_bars=row.get("require_final_bars", True),
        stale_policy=StalePolicy(row.get("stale_policy", "BLOCK")),
        gap_policy=GapPolicy(row.get("gap_policy", "BLOCK")),
        recovery=RecoveryPolicy(row.get("recovery", "SNAPSHOT_AND_REPLAY")),
        bar_revision_policy=BarRevisionPolicy(row.get("bar_revision_policy", "LATEST")))


async def run(args):
    packet = Path(args.packet)
    policy = json.loads((packet / "reader-public/jwt-config.json").read_text())
    reads, negatives, streams = [], [], []
    for path in sorted((packet / "manifests-crypto").glob("*.yaml")):
        payload = yaml.safe_load(path.read_text())
        meta, declared = payload["metadata"], payload["spec"]["requirements"]
        cid, realm = meta["id"], meta["environment"]
        identity = packet / "identities" / cid
        tls = WorkloadTlsConfig(identity / "ca.crt", identity / "client.crt", identity / "client.key")
        def credential(**overrides):
            values = dict(private_key_file=identity / "private.key", key_id=cid + "-rs256-v1",
                algorithm="RS256", issuer=policy["issuer"], audience=policy["audience"],
                subject=meta["subject"], environment=realm,
                roles=("market_data_reader", "historical_reader", "stream_consumer"),
                consumer_manifest_revision=meta["revision"], lifetime_seconds=300, refresh_before_seconds=30)
            values.update(overrides)
            return RotatingJwtCredentialProvider(**values)
        selected = list(declared) if cid.startswith("trading-system.") else []
        if not selected:
            for feed in ("TRADE", "QUOTE", "BAR", "MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA"):
                row = next((r for r in declared if r["feed"] == feed and (feed != "BAR" or r.get("interval") == "1m")), None)
                if row:
                    selected.append(row)
        for replica in (1, 2):
            creds = credential()
            url = f"https://query_v2_{replica}:8200"
            query = RestQueryTransport(url, credential_provider=creds, tls=tls)
            transport = GrpcStreamTransport(f"qdl-v2-stream-{'a' if replica == 1 else 'b'}:8210", credential_provider=creds, tls=tls)
            client = AsyncDataLayerClient(query_transport=query, stream_transport=transport,
                                          consumer_id=cid, max_warmup_attempts=1)
            try:
                for raw in selected:
                    req = requirement(raw)
                    start = time.perf_counter()
                    row = dict(consumer_id=cid, realm=realm, replica=replica,
                        instrument_uid=req.instrument_uid, feed=req.feed.value, interval=req.interval)
                    try:
                        response = await client.snapshot(req)
                        quality = response.data.quality
                        row.update(status="TYPED_RESPONSE", state=quality.state,
                            execution_eligible=quality.execution_eligible,
                            event_recency_state=quality.event_recency_state,
                            provider_session_state=quality.provider_session_state,
                            gap_open=quality.gap_open, complete=quality.complete,
                            sampled_at_ns=time.time_ns(),
                            observed_at_ns=response.data.observed_at_ns,
                            received_at_ns=response.data.received_at_ns,
                            watermark_offset=response.data.watermark_offset,
                            quality=quality.model_dump(mode="json"),
                            source=response.data.source.model_dump(mode="json"),
                            contract=response.data.contract.model_dump(mode="json"))
                    except Exception as error:
                        row.update(status="REFUSED", code=getattr(error, "code", type(error).__name__), detail=str(error)[:180], diagnostics=getattr(error, "diagnostics", None))
                    row["call_to_result_ms"] = (time.perf_counter() - start) * 1000
                    reads.append(row)
                    await asyncio.sleep(.1)
                probe = requirement(next(r for r in selected if r["feed"] == "TRADE"))
                for name, kwargs, consumer in (
                    ("wrong-realm", {"environment": "paper"}, cid),
                    ("wrong-revision", {"consumer_manifest_revision": meta["revision"] + 1}, cid),
                    ("wrong-consumer", {}, cid.replace("." + realm + ".", ".paper.")),
                ):
                    bad = RestQueryTransport(url, credential_provider=credential(**kwargs), tls=tls)
                    try:
                        await bad.snapshot(probe, consumer_id=consumer)
                        negatives.append(dict(consumer_id=cid, replica=replica, case=name, refused=False))
                    except DataLayerError as error:
                        negatives.append(dict(consumer_id=cid, replica=replica, case=name,
                            refused=error.code in {"UNAUTHENTICATED", "CONSUMER_MISMATCH", "PERMISSION_DENIED"}, code=error.code))
                    finally:
                        await bad.close()
                try:
                    async with asyncio.timeout(20):
                        async with client.warmup_then_stream(probe) as session:
                            async for event in session:
                                if isinstance(event, StreamEvent):
                                    await session.acknowledge_async(event)
                                    streams.append(dict(consumer_id=cid, replica=replica, status="STREAM_ACK_PASS"))
                                    break
                except Exception as error:
                    streams.append(dict(consumer_id=cid, replica=replica, status="FAILED",
                        code=getattr(error, "code", type(error).__name__), detail=str(error)[:180]))
            finally:
                await client.close()
    # No DATA_STALE exemption and no retry seeking a green sample.
    failures = [r for r in reads if r["status"] != "TYPED_RESPONSE"]
    usable = [r for r in reads if r.get("execution_eligible") is True]
    report = dict(schema="qdl.consumer-realm-acceptance.v1", order_actions=0,
        snapshots=reads, negatives=negatives, streams=streams,
        typed_response_gate=bool(reads) and not failures,
        execution_usable_count=len(usable), total_reads=len(reads),
        negative_gate=bool(negatives) and all(r["refused"] for r in negatives),
        stream_gate=len(streams) == 12 and all(r["status"] == "STREAM_ACK_PASS" for r in streams))
    report["all_reads_execution_eligible"] = bool(reads) and len(usable) == len(reads)
    Path(args.output).write_text(json.dumps(report, indent=2))
    print(json.dumps({k:v for k,v in report.items() if k not in {"snapshots", "negatives", "streams"}}))
    return report["typed_response_gate"] and report["negative_gate"] and report["stream_gate"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", required=True)
    parser.add_argument("--output", required=True)
    raise SystemExit(0 if asyncio.run(run(parser.parse_args())) else 1)
