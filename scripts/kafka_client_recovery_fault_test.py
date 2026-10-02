#!/usr/bin/env python3
"""Pause only an isolated test broker; run the captured-data recovery gate."""
import argparse
import json
import subprocess
import time
from pathlib import Path

BROKER = "qdl-kafka-recovery-test"
NETWORK = "qdl-kafka-recovery-test"
RUNNER = "qdl-recovery-fault-runner"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    for name in ("ready", "paused", "restored", "receipt.json"):
        if (evidence / name).exists():
            raise SystemExit("Use a fresh evidence directory; preserve failed runs")
    broker = json.loads(subprocess.check_output(["docker", "inspect", BROKER]))[0]
    if set(broker["NetworkSettings"]["Networks"]) != {NETWORK}:
        raise SystemExit("Refusing non-isolated broker")
    if broker["HostConfig"]["PortBindings"] or any(m["Type"] != "tmpfs" for m in broker["Mounts"]):
        raise SystemExit("Refusing published ports or persistent/shared storage")
    target = evidence.parent / "target"
    command = ["docker", "run", "--rm", "--name", RUNNER, "--network", NETWORK,
        "--cpus", "2", "--memory", "3g", "-v", f"{root}:/work:ro",
        "-v", "qdl-cargo-home:/usr/local/cargo/registry", "-v", f"{target}:/target",
        "-v", f"{evidence}:/evidence", "-w", "/work/rust",
        "-e", "CARGO_BUILD_JOBS=2", "-e", "CARGO_TARGET_DIR=/target",
        "-e", f"QDL_KN_TEST_KAFKA={BROKER}:9092",
        "-e", "QDL_KN_TEST_REDIS=redis://qdl-kafka-recovery-redis:6379/0",
        "-e", "QDL_RECOVERY_FAULT_DIR=/evidence", "qdl-rust-builder:r134-test",
        "cargo", "test", "--offline", "--locked", "-p", "qdl-projector",
        "--test", "stage_a_kafka", "captured_frames_recover", "--", "--ignored", "--nocapture"]
    paused = False
    started = time.monotonic()
    with (evidence / "test.log").open("w") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        try:
            while not (evidence / "ready").exists():
                if process.poll() is not None or time.monotonic() - started > 180:
                    raise RuntimeError("test did not reach fault boundary")
                time.sleep(.1)
            subprocess.run(["docker", "pause", BROKER], check=True, capture_output=True)
            paused = True
            fault_start = time.monotonic()
            (evidence / "paused").write_text("test-only broker paused")
            time.sleep(10)
            subprocess.run(["docker", "unpause", BROKER], check=True, capture_output=True)
            paused = False
            outage = time.monotonic() - fault_start
            (evidence / "restored").write_text("test-only broker restored")
            code = process.wait(timeout=180)
            controller = {"broker": BROKER, "fault_seconds": outage, "exit_code": code,
                "elapsed_seconds": time.monotonic() - started, "production_mutations": 0}
            (evidence / "controller.json").write_text(json.dumps(controller, indent=2))
            print(json.dumps(controller))
            if code:
                raise SystemExit(code)
        finally:
            if paused:
                subprocess.run(["docker", "unpause", BROKER], check=True, capture_output=True)
            if process.poll() is None:
                subprocess.run(["docker", "stop", "-t", "5", RUNNER], capture_output=True)
                process.wait(timeout=15)


if __name__ == "__main__":
    main()
