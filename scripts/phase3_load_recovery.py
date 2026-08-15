from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import tempfile
import time
import tracemalloc
from pathlib import Path

from qdl.ingestion.contracts import DeliveryPolicy
from qdl.ingestion.queue import FeedQueue
from qdl.transport.contracts import DurableEvent
from qdl.transport.sqlite_spool import SQLiteDurableSpool, SpoolConfig


async def exercise(events: int, partitions: int, output: Path) -> dict:
    if events < partitions or partitions < 1:
        raise ValueError("events must cover every partition")
    queue = FeedQueue[DurableEvent](capacity=512, policy=DeliveryPolicy.LOSSLESS)
    accepted_ids: list[bytes] = []
    started = time.perf_counter()
    tracemalloc.start()
    with tempfile.TemporaryDirectory(prefix="qdl-phase3-load-") as directory:
        path = Path(directory) / "load.sqlite3"
        config = SpoolConfig(
            path=path, max_records=events + 100,
            max_payload_bytes=max(32 * 1024 * 1024, events * 256),
            max_event_bytes=4096, max_partitions=partitions + 10,
            min_free_disk_bytes=0, replay_retention_seconds=3600,
        )
        spool = SQLiteDurableSpool(config)

        async def producer() -> None:
            for index in range(events):
                partition = f"instrument-{index % partitions}/trade/source"
                payload = json.dumps(
                    {"index": index, "provenance": "TEST_SYNTHETIC_LOAD"},
                    separators=(",", ":"),
                ).encode()
                event = DurableEvent(
                    stream="md.raw.v1.test.load.trade", partition_key=partition,
                    event_id=hashlib.blake2b(index.to_bytes(8, "big"), digest_size=16).digest(),
                    payload=payload, accepted_at_ns=time.time_ns(),
                    content_type="application/json",
                    headers={"provenance": "TEST_SYNTHETIC_LOAD"},
                )
                await queue.put(partition, event)

        async def consumer() -> None:
            consumed = 0
            while consumed < events:
                batch = [
                    await queue.get()
                    for _ in range(min(100, events - consumed))
                ]
                results = spool.append_many(batch)
                if any(result.duplicate for result in results):
                    raise RuntimeError("unexpected duplicate during load")
                accepted_ids.extend(event.event_id for event in batch)
                for _ in batch:
                    queue.task_done()
                consumed += len(batch)

        await asyncio.gather(producer(), consumer())
        first_stats = spool.stats()
        spool.close()

        reopened = SQLiteDurableSpool(config)
        second_stats = reopened.stats()
        replayed = 0
        for partition_index in range(partitions):
            partition = f"instrument-{partition_index}/trade/source"
            replayed += len(reopened.read(
                stream="md.raw.v1.test.load.trade", partition_key=partition, limit=10_000
            ))
        reopened.close()
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    elapsed = time.perf_counter() - started
    result = {
        "schema": "qdl.phase3.load-recovery.v1",
        "status": "PASS",
        "provenance": "TEST_SYNTHETIC_LOAD",
        "events": events,
        "partitions": partitions,
        "accepted": len(accepted_ids),
        "replayed_after_restart": replayed,
        "records_before_restart": first_stats.records,
        "records_after_restart": second_stats.records,
        "queue_high_watermark": queue.stats().high_watermark,
        "queue_rejected": queue.stats().rejected,
        "peak_traced_memory_bytes": peak_bytes,
        "events_per_second": events / elapsed,
    }
    if not all(value == events for value in (
        result["accepted"], result["replayed_after_restart"],
        result["records_before_restart"], result["records_after_restart"],
    )):
        raise RuntimeError(f"load/recovery event loss: {result}")
    if result["queue_rejected"] != 0 or peak_bytes > 64 * 1024 * 1024:
        raise RuntimeError(f"load/recovery resource bound failed: {result}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--partitions", type=int, default=80)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(exercise(args.events, args.partitions, args.output)), sort_keys=True))


if __name__ == "__main__":
    main()
