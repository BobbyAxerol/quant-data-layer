from __future__ import annotations

import argparse
import asyncio
import json
import platform
import tempfile
from pathlib import Path

from scripts.phase3_sustained_load import exercise


async def certify(
    *,
    events_per_window: int,
    partitions: int,
    normal_rate: int,
    burst_rate: int,
    output: Path,
) -> dict:
    if burst_rate <= normal_rate:
        raise ValueError("burst rate must exceed normal rate")
    windows = []
    with tempfile.TemporaryDirectory(prefix="qdl-phase6-capacity-") as directory:
        root = Path(directory)
        for name, rate in (
            ("normal-1", normal_rate),
            ("normal-2", normal_rate),
            ("burst", burst_rate),
        ):
            result = await exercise(
                events=events_per_window,
                partitions=partitions,
                target_rate=rate,
                output=root / f"{name}.json",
            )
            windows.append({"name": name, **result})
    normal_peaks = [item["peak_traced_memory_bytes"] for item in windows[:2]]
    memory_growth = normal_peaks[1] - normal_peaks[0]
    dropped = sum(item["queue_rejected"] for item in windows)
    replay_mismatch = any(
        item["records_before_restart"] != item["records_after_restart"]
        for item in windows
    )
    result = {
        "schema": "qdl.phase6.capacity-certification.v1",
        "status": "PASS",
        "provenance": "TEST_SYNTHETIC_LOAD",
        "runtime": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "events_per_window": events_per_window,
        "partitions": partitions,
        "normal_target_events_per_second": normal_rate,
        "burst_target_events_per_second": burst_rate,
        "memory_growth_between_normal_windows_bytes": memory_growth,
        "canonical_queue_rejected": dropped,
        "replay_mismatch": replay_mismatch,
        "windows": windows,
    }
    if dropped != 0 or replay_mismatch or memory_growth > 8 * 1024 * 1024:
        result["status"] = "FAIL"
        raise RuntimeError(f"capacity certification failed: {result}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-per-window", type=int, default=5_000)
    parser.add_argument("--partitions", type=int, default=80)
    parser.add_argument("--normal-rate", type=int, default=500)
    parser.add_argument("--burst-rate", type=int, default=1_500)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(certify(
        events_per_window=args.events_per_window,
        partitions=args.partitions,
        normal_rate=args.normal_rate,
        burst_rate=args.burst_rate,
        output=args.output,
    ))
    print(json.dumps({
        "status": result["status"],
        "normal_rps": [round(item["achieved_events_per_second"], 2) for item in result["windows"][:2]],
        "burst_rps": round(result["windows"][2]["achieved_events_per_second"], 2),
        "max_p999_ms": round(max(item["durable_latency_p999_ms"] for item in result["windows"]), 3),
        "queue_rejected": result["canonical_queue_rejected"],
        "memory_growth_bytes": result["memory_growth_between_normal_windows_bytes"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
