#!/usr/bin/env python3
"""Offline TEST_ONLY allocation microbenchmark, not a serving-stack CPU gate.

Compare the same L2 workload against a Git baseline and current source using an
existing compiler image. No image builds, credentials, networks or provider I/O.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import uuid

SOURCE = Path("rust/qdl-core/src/l2_book.rs")
IMAGE = "sha256:540c902e99c384163b688bbd8b5b8520e94e7731b27f7bd0eaa56ae1960627ab"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    if output.is_relative_to(root):
        parser.error("evidence output must be outside the checkout")
    output.mkdir(parents=True, exist_ok=False)
    baseline_sha = subprocess.check_output(
        ["git", "rev-parse", "--verify", "--end-of-options", f"{args.baseline_ref}^{{commit}}"], cwd=root, text=True
    ).strip()
    sources = {
        "baseline": subprocess.check_output(["git", "show", f"{baseline_sha}:{SOURCE}"], cwd=root),
        "candidate": (root / SOURCE).read_bytes(),
    }
    harness = (root / "scripts/support/l2_apply_probe.rs").read_bytes()
    receipt = {
        "provenance": "TEST_ONLY_SYNTHETIC_MICROBENCH",
        "baseline_sha": baseline_sha,
        "compiler_image": IMAGE,
        "source_sha256": {k: hashlib.sha256(v).hexdigest() for k, v in sources.items()},
        "harness_sha256": hashlib.sha256(harness).hexdigest(),
        "cpu_vcpu": "NOT_MEASURED",
        "full_stack_budget": "NOT_MEASURED",
        "measurements": [],
        "status": "INCOMPLETE",
    }
    scratch = Path(tempfile.mkdtemp(prefix="l2-apply-", dir=output))
    try:
        def run(variant, command):
            name = f"dr07-l2-{uuid.uuid4().hex}"
            try:
                return subprocess.run(
                    ["docker", "run", "--rm", "--name", name, "--network", "none", "--cpus", "1",
                     "--memory", "512m", "--user", f"{os.getuid()}:{os.getgid()}",
                     "-v", f"{scratch}:/work", "-w", f"/work/{variant}", IMAGE, *command],
                    capture_output=True, text=True, timeout=120, check=True,
                )
            except BaseException:
                subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=15)
                raise

        for variant, source in sources.items():
            directory = scratch / variant
            directory.mkdir()
            (directory / "book.rs").write_bytes(source)
            (directory / "bench.rs").write_bytes(harness)
            result = run(variant, ["rustc", "--edition=2021", "-O", "-Awarnings", "bench.rs", "-o", "bench"])
            (output / f"{variant}-compile.log").write_text(result.stdout + result.stderr)
        for depth in (20, 400, 1000):
            hashes = set()
            for pair in range(5):
                order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
                for variant in order:
                    result = run(variant, ["./bench", str(depth), "2000"])
                    row = json.loads(result.stdout)
                    digest = hashlib.sha256(result.stderr.encode()).hexdigest()
                    hashes.add(digest)
                    row.update(variant=variant, pair=pair, state_sha256=digest)
                    receipt["measurements"].append(row)
            if len(hashes) != 1:
                raise RuntimeError(f"baseline/candidate final state differs at depth {depth}")
        receipt["status"] = "PASS_MICROBENCH_ONLY"
    finally:
        shutil.rmtree(scratch)
        receipt["scratch_removed"] = True
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"status": receipt["status"], "receipt": str(output / "receipt.json")}))


if __name__ == "__main__":
    main()
