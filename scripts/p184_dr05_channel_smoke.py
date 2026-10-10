"""Opt-in disposable JSONL handoff smoke; not TS acceptance evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import selectors
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import p184_dr05_dl_fault as dr


def exercise(output, exported, core_binary):
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    receipt = {"schema": "p184.dr05.channel-smoke.v1", "acceptance": False,
               "provenance": "TEST_ONLY", "resets": [], "steps": [],
               "disk_free_before": shutil.disk_usage(output).free}
    channel = watcher = None
    passed = False
    with tempfile.TemporaryDirectory(prefix="p184-dr05-channel-") as tmp:
        work = Path(tmp) / "work"
        ns = dr.init(work, exported)["namespace"]
        receipt["namespace"] = ns
        with (output / "channel.stderr").open("w") as errors, (output / "reset.stderr").open("w") as reset_errors:
            try:
                receipt["startup"] = dr.up(work, core_binary=core_binary)
                argv = dr.helper_command(ns, work, ["--channel", "/work"], network="container:" + ns + "-kafka")
                argv.insert(2, "-i")
                argv[argv.index("scripts/p184_dr05_dl_fault.py")] = "scripts/p184_dr05_channel.py"
                pos = argv.index(dr.image_set(work)["python"])
                argv[pos:pos] = ["-e", "P184_DR05_ISOLATED_NAMESPACE=" + ns]
                channel = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=errors, text=True, bufsize=1)
                watcher = subprocess.Popen([sys.executable, "-B", str(ROOT / "scripts/p184_dr05_reset.py"),
                    "--serve-reset-host", str(work), "--duration", "300"], stdout=reset_errors, stderr=reset_errors)

                def rpc(op, args, timeout):
                    started = time.monotonic()
                    channel.stdin.write(json.dumps({"op": op, "args": args, "timeout": timeout}) + "\n")
                    channel.stdin.flush()
                    with selectors.DefaultSelector() as selector:
                        selector.register(channel.stdout, selectors.EVENT_READ)
                        if not selector.select(timeout + 2):
                            raise TimeoutError("JSONL reply deadline")
                    line = channel.stdout.readline()
                    if not line:
                        raise RuntimeError("JSONL channel exited")
                    result = json.loads(line)
                    if result.get("ok") is not True:
                        raise RuntimeError("channel reply " + str(result))
                    return result["result"], (time.monotonic() - started) * 1000

                for venue in dr.VENUES:
                    fault = "GAP_REFUSAL"
                    reset, elapsed = rpc("reset_case", {"venue": venue, "fault": fault}, 30)
                    receipt["resets"].append({**reset, "elapsed_ms": elapsed})
                    for index, step in enumerate(dr.schedule(venue, fault)):
                        values = {k: v for k, v in step.items() if k != "phase"}
                        frame = dr.frame(**values, at_ns=time.time_ns())
                        raw, publish_ms = rpc("publish", {"frame": frame}, 5)
                        observed, observe_ms = rpc("canonical_observe", {"receipt": raw}, 10)
                        if index == 0 and raw["offset"] != 0:
                            raise AssertionError("fresh raw topic did not start at zero")
                        if observed.get("observed") is not True:
                            raise AssertionError("actual committed observation missing")
                        receipt["steps"].append({"venue": venue, "phase": step["phase"],
                            "raw": raw, "observed": observed, "publish_ms": publish_ms, "observe_ms": observe_ms})
                if len({r["broker_id"] for r in receipt["resets"]}) != 1:
                    raise AssertionError("broker namespace changed")
                if len({r["topic_id"] for r in receipt["resets"]}) != 2:
                    raise AssertionError("topic UUID reused")
                passed = True
            except Exception as error:
                receipt["error"] = type(error).__name__ + ":" + str(error)[:1000]
            finally:
                if channel:
                    channel.stdin.close()
                    try:
                        channel.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        channel.terminate()
                        channel.wait(timeout=10)
                if watcher:
                    watcher.terminate()
                    try:
                        watcher.wait(timeout=35)
                    except subprocess.TimeoutExpired:
                        watcher.kill()
                        watcher.wait(timeout=5)
                try:
                    receipt["cleanup"] = dr.cleanup(ns)
                finally:
                    dr.erase_private(work)
                    receipt["private_files_remaining"] = sum((work / f).exists() for f in dr.PRIVATE_FILES)
    receipt.update(passed=passed, disk_free_after=shutil.disk_usage(output).free)
    wire = json.dumps(receipt, indent=2) + "\n"
    (output / "receipt.json").write_text(wire)
    print(json.dumps({"passed": passed, "acceptance": False, "output": str(output),
                      "sha256": hashlib.sha256(wire.encode()).hexdigest(), "error": receipt.get("error")}))
    return passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--exported", type=Path, required=True)
    parser.add_argument("--core-binary", type=Path, required=True)
    args = parser.parse_args()
    raise SystemExit(0 if exercise(args.output, args.exported, args.core_binary) else 1)
