from __future__ import annotations

import argparse
import hashlib
import json
import resource
import statistics
import tempfile
import time
import tracemalloc
from pathlib import Path

from qdl.domain.event_id import deterministic_event_id
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig


def percentile(sorted_values: list[int], percentile_value: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, int((len(sorted_values) - 1) * percentile_value))
    return sorted_values[index] / 1_000_000


def run(
    event_count: int,
    partition_count: int,
    payload_size: int,
    batch_size: int,
    consumer_groups: int,
) -> dict:
    with tempfile.TemporaryDirectory(prefix="qdl-phase2-benchmark.") as directory:
        path = Path(directory) / "bridge.sqlite3"
        payload_body = b"x" * payload_size
        config = SpoolConfig(
            path=path,
            max_records=event_count + 100,
            max_payload_bytes=(event_count + 100) * payload_size * 2,
            max_event_bytes=max(payload_size * 2, 1024),
            max_storage_bytes=max(64 * 1024 * 1024, event_count * payload_size * 8),
            max_partitions=partition_count + 10,
            min_free_disk_bytes=0,
        )
        process_start = time.process_time()
        wall_start = time.perf_counter()
        tracemalloc.start()
        latencies = []
        payload_bytes = 0
        with SQLiteDurableSpool(config) as spool:
            for batch_start in range(0, event_count, batch_size):
                batch = []
                for index in range(batch_start, min(event_count, batch_start + batch_size)):
                    partition = f"instrument-{index % partition_count}/trade/binance-shadow"
                    batch.append(
                        DurableEvent(
                            stream="md.canonical.v2.trade",
                            partition_key=partition,
                            event_id=deterministic_event_id(["phase2-benchmark", index]),
                            payload=payload_body,
                            accepted_at_ns=time.time_ns(),
                        )
                    )
                started = time.perf_counter_ns()
                spool.append_many(batch)
                batch_latency = time.perf_counter_ns() - started
                latencies.extend([batch_latency] * len(batch))
                payload_bytes += len(payload_body) * len(batch)

            duplicate_started = time.perf_counter()
            for index in range(min(event_count, 1000)):
                spool.append(
                    DurableEvent(
                        stream="md.canonical.v2.trade",
                        partition_key=f"instrument-{index % partition_count}/trade/binance-shadow",
                        event_id=deterministic_event_id(["phase2-benchmark", index]),
                        payload=payload_body,
                        accepted_at_ns=time.time_ns(),
                    )
                )
            duplicate_seconds = time.perf_counter() - duplicate_started

            checkpoint_start = time.perf_counter()
            checkpoint_rows = 0
            for consumer_index in range(consumer_groups):
                consumer_id = f"benchmark-consumer-{consumer_index}"
                for partition_index in range(partition_count):
                    partition = (
                        f"instrument-{partition_index}/trade/binance-shadow"
                    )
                    high = spool.high_watermark("md.canonical.v2.trade", partition)
                    spool.register_consumer(
                        consumer_id=consumer_id,
                        stream="md.canonical.v2.trade",
                        partition_key=partition,
                        after_offset=high,
                    )
                    checkpoint_rows += 1
            checkpoint_seconds = time.perf_counter() - checkpoint_start

            replay_start = time.perf_counter()
            replayed = 0
            replay_digest = hashlib.sha256()
            for partition_index in range(partition_count):
                rows = spool.read(
                    stream="md.canonical.v2.trade",
                    partition_key=f"instrument-{partition_index}/trade/binance-shadow",
                    limit=10_000,
                )
                replayed += len(rows)
                for row in rows:
                    replay_digest.update(row.event.event_id)
                    replay_digest.update(bytes.fromhex(row.payload_sha256))
            replay_seconds = time.perf_counter() - replay_start
            stats = spool.stats()
        _current, peak_allocation = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        wall_seconds = time.perf_counter() - wall_start
        process_seconds = time.process_time() - process_start
        ordered = sorted(latencies)
        return {
            "schema": "qdl.phase2.benchmark.v1",
            "status": "PASS",
            "config": {
                "event_count": event_count,
                "partition_count": partition_count,
                "payload_size_bytes": payload_size,
                "batch_size": batch_size,
                "consumer_groups": consumer_groups,
                "sqlite_journal": "WAL",
                "sqlite_synchronous": "FULL",
            },
            "append": {
                "throughput_events_per_second": event_count / wall_seconds,
                "latency_ms": {
                    "p50": percentile(ordered, 0.50),
                    "p95": percentile(ordered, 0.95),
                    "p99": percentile(ordered, 0.99),
                    "p99_9": percentile(ordered, 0.999),
                    "max": max(ordered) / 1_000_000,
                    "mean": statistics.mean(ordered) / 1_000_000,
                },
                "duplicate_retry_events_per_second": min(event_count, 1000)
                / duplicate_seconds,
            },
            "replay": {
                "events": replayed,
                "events_per_second": replayed / replay_seconds,
                "checksum": replay_digest.hexdigest(),
            },
            "resources": {
                "process_cpu_seconds": process_seconds,
                "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "python_peak_allocation_bytes": peak_allocation,
                "payload_bytes": payload_bytes,
                "storage_bytes": stats.storage_bytes,
                "disk_amplification": stats.storage_bytes / payload_bytes,
                "network_amplification": 0.0,
            },
            "capacity": {
                "logical_utilization": stats.utilization,
                "replay_horizon_seconds_at_100_events_per_second": config.max_records / 100,
                "benchmark_consumer_groups": consumer_groups,
                "checkpoint_rows": checkpoint_rows,
                "checkpoint_writes_per_second": checkpoint_rows / checkpoint_seconds,
            },
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--partitions", type=int, default=10)
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--consumer-groups", type=int, default=8)
    parser.add_argument("--min-throughput", type=float, default=0.0)
    parser.add_argument("--max-p99-ms", type=float, default=0.0)
    parser.add_argument("--max-disk-amplification", type=float, default=0.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.events <= 0 or args.partitions <= 0 or args.events < args.partitions:
        raise SystemExit("invalid benchmark event/partition count")
    if args.batch_size <= 0 or args.batch_size > 1000:
        raise SystemExit("invalid benchmark batch size")
    if args.consumer_groups <= 0:
        raise SystemExit("invalid benchmark consumer group count")
    result = run(
        args.events,
        args.partitions,
        args.payload_bytes,
        args.batch_size,
        args.consumer_groups,
    )
    failures = []
    if (
        args.min_throughput > 0
        and result["append"]["throughput_events_per_second"] < args.min_throughput
    ):
        failures.append("append_throughput_below_gate")
    if args.max_p99_ms > 0 and result["append"]["latency_ms"]["p99"] > args.max_p99_ms:
        failures.append("append_p99_above_gate")
    if (
        args.max_disk_amplification > 0
        and result["resources"]["disk_amplification"]
        > args.max_disk_amplification
    ):
        failures.append("disk_amplification_above_gate")
    if result["replay"]["events"] != args.events:
        failures.append("replay_event_count_mismatch")
    result["gates"] = {
        "min_throughput": args.min_throughput,
        "max_p99_ms": args.max_p99_ms,
        "max_disk_amplification": args.max_disk_amplification,
        "failures": failures,
    }
    result["status"] = "FAIL" if failures else "PASS"
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    print(rendered, end="")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
