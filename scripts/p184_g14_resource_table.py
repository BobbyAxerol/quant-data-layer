#!/usr/bin/env python3
"""P18.4 gate G14: per-role CPU and memory table from a stage-100 driver run (offline, read-only).

Input: the driver's host.json (scripts/phase3_consumer_load_acceptance.py run_target_host), whose
`runtime_observation` holds one sample about every 10 s with `docker stats --no-stream` rows and the cumulative cgroup
`cpu_throttle` counters of every monitored container (profile `monitored_containers`; the final campaign adds
binance_bar_edge and stable_redis, which the v2.2.3 scorecard listed as not sampled).

Per role: cpu_mean% = cgroup usage_usec delta / wall delta between the first and last sample that carry it (exact
mean; 100 % = one core), cpu_p95% / cpu_max% from the instantaneous docker stats CPUPerc samples (nearest rank),
mem_max_MiB from MemUsage, the memory limit from MemUsage's denominator, the CPU limit (NanoCpus) and throttled time
share when --inspect is given (read-only docker inspect). Roles missing from the samples are listed, never filled.

G14 is a recording gate: the owner sets the budget from this measurement. The table is PASS-eligible only when every
expected role has samples, every max RSS is under its limit and the run met the protocol (decided by the scorer).
usage: p184_g14_resource_table.py HOST_JSON [--expect name,...] [--inspect] [--out PATH]
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

PREFIX = "qdl_v2_stable_candidate-"
UNITS = {"B": 1 / 2**20, "KiB": 1 / 1024, "MiB": 1.0, "GiB": 1024.0, "TiB": 1024.0 ** 2,
         "kB": 1000 / 2**20, "MB": 1e6 / 2**20, "GB": 1e9 / 2**20}


def role_name(container: str) -> str:
    name = container[len(PREFIX):] if container.startswith(PREFIX) else container
    return name[:-2] if name.endswith("-1") else name


def mib(text: str) -> float | None:
    match = re.fullmatch(r"\s*([0-9.]+)\s*([A-Za-z]+)\s*", text or "")
    if not match or match.group(2) not in UNITS:
        return None
    return float(match.group(1)) * UNITS[match.group(2)]


def percent(text: str) -> float | None:
    try:
        return float(str(text).strip().rstrip("%"))
    except ValueError:
        return None


def nearest(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))] if ordered else None


def table(host: dict, *, expect=(), limits=None) -> dict:
    samples = host.get("runtime_observation") or []
    cpu, mem, mem_limit, usage = {}, {}, {}, {}
    for sample in samples:
        for row in sample.get("stats", []):
            name = row.get("Name") or row.get("Container")
            if not name:
                continue
            value = percent(row.get("CPUPerc"))
            if value is not None:
                cpu.setdefault(name, []).append(value)
            used, _, limit = str(row.get("MemUsage", "")).partition("/")
            if mib(used) is not None:
                mem.setdefault(name, []).append(mib(used))
            if mib(limit) is not None:
                mem_limit[name] = mib(limit)
        for name, counters in (sample.get("cpu_throttle") or {}).items():
            usage.setdefault(name, []).append((sample["at_ns"], counters))
    roles = {}
    for name in sorted(set(cpu) | set(mem) | set(usage)):
        series = usage.get(name, [])
        mean = throttled = None
        if len(series) >= 2:
            (t0, c0), (t1, c1) = series[0], series[-1]
            wall_us = (t1 - t0) / 1000
            if wall_us > 0:
                mean = round(100.0 * (c1["usage_usec"] - c0["usage_usec"]) / wall_us, 1)
                throttled = round(100.0 * (c1["throttled_usec"] - c0["throttled_usec"]) / wall_us, 1)
        row = {"container": name, "samples": len(cpu.get(name, [])), "cpu_mean_pct": mean,
               "cpu_p95_pct": nearest(cpu.get(name, []), .95), "cpu_max_pct": max(cpu.get(name, []), default=None),
               "mem_max_mib": round(max(mem.get(name, [])), 1) if mem.get(name) else None,
               "mem_limit_mib": round(mem_limit[name], 1) if name in mem_limit else None,
               "throttled_pct_of_wall": throttled}
        if limits and name in limits:
            row.update(limits[name])
        row["mem_headroom_pct"] = (round(100.0 * (1 - row["mem_max_mib"] / row["mem_limit_mib"]), 1)
                                   if row["mem_max_mib"] and row["mem_limit_mib"] else None)
        roles[role_name(name)] = row
    expected = {role_name(name) for name in (expect or host.get("monitored_containers") or [])}
    missing = sorted(expected - {r for r, v in roles.items() if v["samples"]})
    over = sorted(r for r, v in roles.items() if v["mem_max_mib"] and v["mem_limit_mib"]
                  and v["mem_max_mib"] >= v["mem_limit_mib"])
    means = [v["cpu_mean_pct"] for r, v in roles.items() if v["cpu_mean_pct"] is not None
             and not r.startswith("market_data_service")]
    return {"roles": roles, "missing_roles": missing, "mem_at_or_over_limit": over,
            "samples": len(samples), "dl_cpu_mean_sum_vcpu": round(sum(means) / 100.0, 2) if means else None}


def inspect_limits(containers) -> dict:
    result = subprocess.run(["docker", "inspect", "--format",
                             '{"name":{{json .Name}},"nano_cpus":{{.HostConfig.NanoCpus}},"memory":{{.HostConfig.Memory}}}',
                             *containers], capture_output=True, text=True, timeout=30)
    out = {}
    for line in result.stdout.splitlines():
        value = json.loads(line)
        out[value["name"].lstrip("/")] = {"cpu_limit_vcpu": value["nano_cpus"] / 1e9 or None,
                                          "mem_limit_bytes_inspect": value["memory"] or None}
    return out


def render(result: dict) -> str:
    lines = [f"{'role':<28}{'cpu_mean%':>10}{'cpu_p95%':>9}{'cpu_max%':>9}{'thr%':>6}{'mem_max_MiB':>12}"
             f"{'limit_MiB':>11}{'headroom%':>10}"]
    fmt = lambda v, w: f"{'-' if v is None else v:>{w}}"  # noqa: E731
    for role, row in sorted(result["roles"].items()):
        lines.append(f"{role:<28}{fmt(row['cpu_mean_pct'], 10)}{fmt(row['cpu_p95_pct'], 9)}"
                     f"{fmt(row['cpu_max_pct'], 9)}{fmt(row['throttled_pct_of_wall'], 6)}"
                     f"{fmt(row['mem_max_mib'], 12)}{fmt(row['mem_limit_mib'], 11)}{fmt(row['mem_headroom_pct'], 10)}")
    lines.append(f"samples {result['samples']}; DL CPU mean sum {result['dl_cpu_mean_sum_vcpu']} vCPU (writer "
                 f"excluded); missing roles {result['missing_roles'] or 'none'}; at/over memory limit "
                 f"{result['mem_at_or_over_limit'] or 'none'}")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("host_json", type=Path)
    parser.add_argument("--expect", default="", help="comma list of containers that must have samples")
    parser.add_argument("--inspect", action="store_true", help="add CPU limits via read-only docker inspect")
    parser.add_argument("--out", type=Path, help="write JSON here (exclusive) and the text table beside it")
    args = parser.parse_args(argv)
    host = json.loads(args.host_json.read_text())
    expect = [x for x in args.expect.split(",") if x]
    limits = inspect_limits(expect or host.get("monitored_containers") or []) if args.inspect else None
    result = table(host, expect=expect, limits=limits)
    text = render(result)
    if args.out:
        with args.out.open("x") as handle:
            handle.write(json.dumps(result, indent=1, sort_keys=True) + "\n")
        with args.out.with_suffix(".txt").open("x") as handle:
            handle.write(text)
    print(text, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
