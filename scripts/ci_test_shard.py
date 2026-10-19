#!/usr/bin/env python3
"""Run one deterministic shard of the Python unit suite (`python -m unittest discover -s tests`).

Purpose: the unit suite took 1583 s of a 30-minute CI run (GitHub run 39201550345) in one container. CI now runs
it as N parallel shards; this module builds exactly the suite `discover -s tests` builds (same loader, same start
directory, same cwd), splits it by top-level test module with a deterministic greedy balance, and runs one shard.

Partition: modules sorted by (weight descending, name); each goes to the currently lightest shard (lowest index on
a tie). Weight = number of test cases of the module (minimum 1), or the seconds recorded for that module in the
optional checked-in WEIGHTS_FILE. A module that fails to import is kept: discover yields a `_FailedTest` named after
the module, it is assigned like any module, and it fails its shard. `--verify-partition` (always done by shard 1)
asserts that the N shards cover every discovered module exactly once and that none is empty.

Boundary: no test selection, skipping or retry; it does not change test order inside a module. Run it from the
repository root as a module (`python -B -m scripts.ci_test_shard`) so sys.path[0] is the cwd, as with
`python -m unittest`.

usage: python -B -m scripts.ci_test_shard --shard I --of N [--list] [--verify-partition] [--start-dir tests]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import unittest
from pathlib import Path

WEIGHTS_FILE = Path(__file__).with_name("ci_test_shard_weights.json")


def leaves(suite):
    if isinstance(suite, unittest.TestSuite):
        for item in suite:
            yield from leaves(item)
    else:
        yield suite


def module_of(test):
    """Top-level module name of a leaf test; a load failure is named after the module that failed."""
    cls = type(test)
    if cls.__module__ == "unittest.loader" and cls.__name__ in ("_FailedTest", "ModuleImportFailure"):
        return test._testMethodName
    return cls.__module__


def group_by_module(suite):
    """Ordered {module: [top-level suite items]} from a discovered suite, keeping the original suite objects."""
    groups = {}
    for item in suite:
        names = [module_of(test) for test in leaves(item)]
        if not names:
            continue  # a module without tests contributes nothing to run
        groups.setdefault(names[0], []).append(item)
    return groups


def module_weights(groups, recorded=None):
    recorded = recorded or {}
    weights = {}
    for name, items in groups.items():
        if name in recorded:
            weights[name] = max(float(recorded[name]), 0.001)
        else:
            weights[name] = max(sum(item.countTestCases() for item in items), 1)
    return weights


def partition(weights, shards):
    """Deterministic greedy balance: list of `shards` sorted module lists."""
    if shards < 1:
        raise ValueError("--of must be >= 1")
    loads = [0.0] * shards
    buckets = [[] for _ in range(shards)]
    for name in sorted(weights, key=lambda n: (-weights[n], n)):
        index = min(range(shards), key=lambda i: (loads[i], i))
        buckets[index].append(name)
        loads[index] += weights[name]
    return [sorted(bucket) for bucket in buckets]


def verify_partition(buckets, modules):
    problems = []
    seen = {}
    for index, bucket in enumerate(buckets, start=1):
        if not bucket:
            problems.append(f"shard {index} is empty")
        for name in bucket:
            if name in seen:
                problems.append(f"{name} in shards {seen[name]} and {index}")
            seen[name] = index
    missing = sorted(set(modules) - set(seen))
    extra = sorted(set(seen) - set(modules))
    if missing:
        problems.append(f"modules in no shard: {missing}")
    if extra:
        problems.append(f"shard modules not discovered: {extra}")
    return problems


def load_recorded(path=WEIGHTS_FILE):
    if not path.is_file():
        return {}
    data = json.loads(path.read_text())
    return {str(k): float(v) for k, v in (data.get("seconds") or {}).items()}


class TimedSuite(unittest.TestSuite):
    """A module's suite items, run unchanged, recording wall seconds into `seconds[name]`."""

    def __init__(self, name, items, seconds):
        super().__init__(items)
        self.name, self.seconds = name, seconds

    def run(self, result, debug=False):
        begin = time.perf_counter()
        try:
            return super().run(result, debug)
        finally:
            self.seconds[self.name] = round(time.perf_counter() - begin, 3)


def run_modules(groups, names, verbosity):
    """Run the selected modules in discovery order with the runner settings `python -m unittest` uses."""
    seconds = {}
    suite = unittest.TestSuite(TimedSuite(name, groups[name], seconds) for name in names)
    # unittest.main (python -m unittest) passes warnings="default" unless -W options were given.
    runner = unittest.TextTestRunner(verbosity=verbosity, warnings=None if sys.warnoptions else "default")
    return runner.run(suite), seconds


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--shard", type=int, required=True, help="1-based shard index")
    parser.add_argument("--of", type=int, required=True, help="number of shards")
    parser.add_argument("--start-dir", default="tests")
    parser.add_argument("--list", action="store_true", help="print the shard's modules and exit")
    parser.add_argument("--verify-partition", action="store_true")
    parser.add_argument("--verbosity", type=int, default=1)
    args = parser.parse_args(argv)
    if not 1 <= args.shard <= args.of:
        parser.error("--shard must be within 1..--of")
    # A fresh loader behaves as defaultTestLoader on its first discover; the shared one keeps _top_level_dir
    # (Python 3.12), which would leak into a later discover in the same process.
    suite = unittest.TestLoader().discover(args.start_dir)
    groups = group_by_module(suite)
    weights = module_weights(groups, load_recorded())
    buckets = partition(weights, args.of)
    if args.verify_partition or args.shard == 1:
        problems = verify_partition(buckets, groups)
        if problems:
            print("partition check FAILED: " + "; ".join(problems), file=sys.stderr)
            return 2
        print(f"partition check PASS: {len(groups)} modules in {args.of} shards, loads "
              + ", ".join(f"{sum(weights[n] for n in b):g}" for b in buckets), file=sys.stderr)
    chosen = buckets[args.shard - 1]
    if args.list:
        print("\n".join(chosen))
        return 0
    order = [name for name in groups if name in set(chosen)]  # discovery order within the shard
    print(f"shard {args.shard}/{args.of}: {len(order)} modules, "
          f"{sum(i.countTestCases() for n in order for i in groups[n])} tests", file=sys.stderr)
    result, seconds = run_modules(groups, order, args.verbosity)
    slowest = sorted(seconds.items(), key=lambda kv: -kv[1])
    print("shard-timing " + json.dumps({"shard": args.shard, "of": args.of, "modules": len(order),
                                        "tests_run": result.testsRun, "seconds": seconds,
                                        "slowest": slowest[:10]}, sort_keys=True), file=sys.stderr)
    if result.testsRun == 0:
        print("shard ran no tests", file=sys.stderr)
        return 1
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
