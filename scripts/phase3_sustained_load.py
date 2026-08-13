from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import tempfile
import time
import tracemalloc
from pathlib import Path

from qdl.ingestion.contracts import DeliveryPolicy
from qdl.ingestion.queue import FeedQueue
from qdl.transport.contracts import DurableEvent
from qdl.transport.sqlite_spool import SQLiteDurableSpool, SpoolConfig


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * quantile) - 1)
    return ordered[max(0, index)]


async def exercise(
    *, events: int, partitions: int, target_rate: int, output: Path
) -> dict:
    if events < partitions or partitions < 1 or target_rate < 1:
        raise ValueError("events must cover partitions and target_rate must be positive")
    queue = FeedQueue[DurableEvent](capacity=512, policy=DeliveryPolicy.LOSSLESS)
    durable_latency_ms: list[float] = []
    pulse_size = max(1, target_rate // 10)
    started = time.monotonic()
    tracemalloc.start()
    with tempfile.TemporaryDirectory(prefix="qdl-phase3-sustained-") as directory:
        path = Path(directory) / "sustained.sqlite3"
        config = SpoolConfig(
            path=path,
            max_records=events + 100,
            max_payload_bytes=max(32 * 1024 * 1024, events * 256),
            max_event_bytes=4096,
            max_partitions=partitions + 10,
            min_free_disk_bytes=0,
            replay_retention_seconds=3600,
        )
        spool = SQLiteDurableSpool(config)

        async def producer() -> None:
            produced = 0
            while produced < events:
                target = started + produced / target_rate
                delay = target - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                for index in range(produced, min(events, produced + pulse_size)):
                    partition = f"instrument-{index % partitions}/trade/source"
                    payload = json.dumps(
                        {"index": index, "provenance": "TEST_SYNTHETIC_LOAD"},
                        separators=(",", ":"),
                    ).encode()
                    await queue.put(
                        partition,
                        DurableEvent(
                            stream="md.raw.v1.test.sustained.trade",
                            partition_key=partition,
                            event_id=hashlib.blake2b(
                                index.to_bytes(8, "big"), digest_size=16
                            ).digest(),
                            payload=payload,
                            accepted_at_ns=time.time_ns(),
                            content_type="application/json",
                            headers={"provenance": "TEST_SYNTHETIC_LOAD"},
                        ),
                    )
                produced += min(pulse_size, events - produced)

        async def consumer() -> None:
            consumed = 0
            while consumed < events:
                batch = [
                    await queue.get()
                    for _ in range(min(100, events - consumed))
                ]
                results = spool.append_many(batch)
                committed_at_ns = time.time_ns()
                if any(result.duplicate for result in results):
                    raise RuntimeError("unexpected duplicate during sustained load")
                durable_latency_ms.extend(
                    (committed_at_ns - event.accepted_at_ns) / 1_000_000
                    for event in batch
                )
                for _ in batch:
                    queue.task_done()
                consumed += len(batch)

        await asyncio.gather(producer(), consumer())
        first_stats = spool.stats()
        spool.close()
        reopened = SQLiteDurableSpool(config)
        second_stats = reopened.stats()
        reopened.close()
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    elapsed = time.monotonic() - started
    achieved_rate = events / elapsed
    result = {
        "schema": "qdl.phase3.sustained-load.v1",
        "status": "PASS",
        "provenance": "TEST_SYNTHETIC_LOAD",
        "events": events,
        "partitions": partitions,
        "target_events_per_second": target_rate,
        "achieved_events_per_second": achieved_rate,
        "elapsed_seconds": elapsed,
        "durable_latency_p95_ms": percentile(durable_latency_ms, 0.95),
        "durable_latency_p99_ms": percentile(durable_latency_ms, 0.99),
        "queue_high_watermark": queue.stats().high_watermark,
        "queue_rejected": queue.stats().rejected,
        "records_before_restart": first_stats.records,
        "records_after_restart": second_stats.records,
        "peak_traced_memory_bytes": peak_bytes,
    }
    if (
        first_stats.records != events
        or second_stats.records != events
        or queue.stats().rejected != 0
        or achieved_rate < target_rate * 0.9
        or result["durable_latency_p99_ms"] > 1_000
        or peak_bytes > 64 * 1024 * 1024
    ):
        result["status"] = "FAIL"
        raise RuntimeError(f"sustained load gate failed: {result}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=5_000)
    parser.add_argument("--partitions", type=int, default=80)
    parser.add_argument("--target-rate", type=int, default=500)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(exercise(
        events=args.events,
        partitions=args.partitions,
        target_rate=args.target_rate,
        output=args.output,
    )), sort_keys=True))


if __name__ == "__main__":
    main()
