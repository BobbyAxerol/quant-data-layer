#!/usr/bin/env python3
"""Deadline-bounded KN-5 resource journal; emergency stop affects new roles only."""
import argparse
import json
from pathlib import Path
import subprocess
import time

PROJECT = "qdl_v2_stable_candidate"
NATIVE = ("market_cache", "market_projector_1", "market_projector_2", "query_kn_1", "query_kn_2", "stream_kn_1", "stream_kn_2")


def read_values(path):
    try:
        return {p[0]: int(p[1]) for p in (line.split() for line in path.read_text().splitlines())}
    except (OSError, ValueError):
        return {}


def sample():
    ids = subprocess.check_output(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={PROJECT}"], text=True).split()
    extra = subprocess.run(["docker", "inspect", "market_data_service"], capture_output=True, text=True)
    containers = json.loads(subprocess.check_output(["docker", "inspect", *ids])) if ids else []
    if extra.returncode == 0:
        containers.extend(json.loads(extra.stdout))
    rows = {}
    for item in containers:
        cg = Path("/sys/fs/cgroup/system.slice") / ("docker-" + item["Id"] + ".scope")
        try:
            memory = int((cg / "memory.current").read_text())
        except OSError:
            memory = None
        rows[item["Name"].lstrip("/")] = {"id": item["Id"], "cpu": read_values(cg / "cpu.stat"),
            "memory": memory, "memory_events": read_values(cg / "memory.events"),
            "restarts": item["RestartCount"], "oom": item["State"]["OOMKilled"],
            "image": item["Image"], "cpu_cap": item["HostConfig"]["NanoCpus"],
            "memory_cap": item["HostConfig"]["Memory"]}
    available = int(next(x for x in Path("/proc/meminfo").read_text().splitlines() if x.startswith("MemAvailable:")).split()[1]) * 1024
    return {"time_ns": time.time_ns(), "available_memory": available, "containers": rows}


def stop_reasons(row, low_idle_seconds=0):
    reasons = []
    if row["available_memory"] < 2 * 1024**3:
        reasons.append("HOST_AVAILABLE_RAM_BELOW_2GIB")
    if low_idle_seconds >= 60:
        reasons.append("HOST_IDLE_BELOW_5_PERCENT_60S")
    if row.get("ts_mark_index_unavailable_60s", 0) >= 10:
        reasons.append("TS_MARK_INDEX_SOURCE_UNAVAILABLE_10_PER_MINUTE")
    for role in NATIVE:
        value = row["containers"].get(f"{PROJECT}-{role}-1", {})
        if value.get("oom") or value.get("memory_events", {}).get("oom_kill", 0):
            reasons.append("NATIVE_OOM:" + role)
    return reasons


def cpu_ticks():
    values = [int(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]
    return sum(values), values[3] + values[4]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=7200)
    parser.add_argument("--stop-file", type=Path, required=True)
    args = parser.parse_args()
    end = time.monotonic() + args.seconds
    previous_cpu = cpu_ticks()
    low_idle_seconds = 0
    tick = 0
    ts_count = 0
    with args.out.open("x") as handle:
        while time.monotonic() < end and not args.stop_file.exists():
            row = sample()
            now_cpu = cpu_ticks()
            total_delta = now_cpu[0] - previous_cpu[0]
            idle = 100 * (now_cpu[1] - previous_cpu[1]) / total_delta if total_delta else 100
            previous_cpu = now_cpu
            low_idle_seconds = low_idle_seconds + 5 if idle < 5 else 0
            if tick % 6 == 0:
                logs = subprocess.run(["docker", "logs", "--since", "60s", "--tail", "1000", "market_data_service"],
                                      capture_output=True, text=True, timeout=10)
                ts_count = sum("MARK_INDEX" in line and "SOURCE_UNAVAILABLE" in line
                               for line in (logs.stdout + logs.stderr).splitlines())
            tick += 1
            row["ts_mark_index_unavailable_60s"] = ts_count
            row["host_idle_percent"] = idle
            row["stop_reasons"] = stop_reasons(row, low_idle_seconds)
            danger = bool(row["stop_reasons"])
            row["guard_stop"] = danger
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")
            handle.flush()
            if danger:
                names = [f"{PROJECT}-{r}-1" for r in NATIVE if f"{PROJECT}-{r}-1" in row["containers"]]
                if names:
                    subprocess.run(["docker", "stop", "-t", "15", *names], check=True)
                print("GUARD_STOP_NEW_KN_ROLES", flush=True)
                return 3
            time.sleep(5)
    print("WATCH_COMPLETE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
