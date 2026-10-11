"""Disposable DR05 observer rehearsal; no shared runtime or certification."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts import p184_dr05_dl_fault as dr


def exercise(output, exported, venue, fault, core_binary=None):
    override = dr.core_override(core_binary)
    output = output.resolve()
    dr.topology("p184-dr05-dl-0123456789abcdef", output)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    started, free_before = time.monotonic(), shutil.disk_usage(output).free
    receipt = {"schema": "p184.dr05.stack-observation.v1", "acceptance": False,
               "venue": venue, "fault": fault, "images": dr.IMAGES, "source_files": {},
               "test_core_binary": override}
    for name in ("p184_dr05_observer.py", "p184_dr05_raw_publisher.py", "p184_dr05_dl_fault.py"):
        receipt["source_files"][name] = hashlib.sha256((ROOT / "scripts" / name).read_bytes()).hexdigest()
    ok = False
    with tempfile.TemporaryDirectory(prefix="p184-dr05-observe-") as tmp:
        work = Path(tmp) / "work"
        initial = dr.init(work, exported)
        ns = initial["namespace"]
        receipt["namespace"] = ns
        try:
            receipt["startup"] = dr.up(work, core_binary=override)
            ts_image = "sha256:a6548ee36aeb0242c1734c071678c525c1506d05c10a70b533eb7110e7eaf16a"
            live_args = ["docker", "run", "-d", "--rm", "--pull=never", "--name", ns + "-tsvalidator",
                "--label", dr.LABEL + "=" + ns, "--network", "container:" + ns + "-kafka", "--read-only",
                "--user", "1001:1001", "--memory", "512m", "--cpus", "0.5", "--pids-limit", "128",
                "--log-opt", "max-size=4m", "--log-opt", "max-file=1",
                "--tmpfs", "/tmp:rw,size=64m,mode=1777", "-e", "PYTHONDONTWRITEBYTECODE=1",
                "-e", "PYTHONPATH=/app", "-e", "P184_DR05_ISOLATED_NAMESPACE=" + ns,
                "-v", str(ROOT / "scripts/p184_dr05_ts_validate.py") + ":/probe.py:ro",
                "--entrypoint", "python", ts_image, "-B", "/probe.py", "--serve", "--namespace", ns]
            dr.bounded(live_args)
            argv = dr.helper_command(ns, work, [], network="container:" + ns + "-kafka")
            index = argv.index(dr.image_set(work)["python"])
            argv[index:index] = ["-e", "P184_DR05_ISOLATED_NAMESPACE=" + ns]
            argv[-1:] = ["scripts/p184_dr05_observer.py", "--work", "/work", "--venue", venue, "--fault", fault]
            receipt["observation"] = json.loads(dr.bounded(argv, 120))
            from scripts.p184_dr05_verdict import evaluate
            receipt["case_verdict"] = evaluate(receipt["observation"])
            ts_image = "sha256:a6548ee36aeb0242c1734c071678c525c1506d05c10a70b533eb7110e7eaf16a"
            ts_args = ["docker", "run", "--rm", "--pull=never", "--name", ns + "-helper",
                "--label", dr.LABEL + "=" + ns, "--network", "none", "--read-only",
                "--user", "1001:1001", "--memory", "512m", "--cpus", "0.5", "--pids-limit", "128",
                "--tmpfs", "/tmp:rw,size=64m,mode=1777", "-e", "PYTHONDONTWRITEBYTECODE=1",
                "-e", "PYTHONPATH=/app", "-v", str(ROOT / "scripts/p184_dr05_ts_validate.py") + ":/probe.py:ro",
                "-i", "--entrypoint", "python", ts_image, "-B", "/probe.py"]
            ts = subprocess.run(ts_args, input=(work / "ts-input.json").read_text(), text=True,
                                capture_output=True, timeout=45)
            receipt["ts_validation"] = {"image": ts_image, "exit_code": ts.returncode,
                "output": json.loads(ts.stdout), "stderr": ts.stderr[-2000:]}
            ok = ts.returncode == 0
        except Exception as error:
            receipt["error"] = {"type": type(error).__name__, "detail": str(error)[:2000],
                                "stderr": getattr(error, "stderr", "")[-3000:]}
            partial = work / "observations.json"
            if partial.exists():
                receipt["partial_observations"] = json.loads(partial.read_text())
        finally:
            try:
                for role in (*dr.ROLES, "tsvalidator"):
                    row = dr.owned(ns, role)
                    if row:
                        logs = subprocess.run(["docker", "logs", "--tail", "100", row["Id"]],
                                              capture_output=True, text=True, timeout=10)
                        (output / (role + ".log")).write_text((logs.stdout + logs.stderr)[-32000:])
            except Exception as error:
                receipt["log_capture_error"] = type(error).__name__
            try:
                receipt["cleanup"] = dr.cleanup(ns)
            finally:
                dr.erase_private(work)
                receipt["private_files_remaining"] = sum((work / name).exists() for name in dr.PRIVATE_FILES)
    receipt.update(elapsed_seconds=time.monotonic() - started, disk_free_before=free_before,
                   disk_free_after=shutil.disk_usage(output).free)
    def redact(value):
        if isinstance(value, dict):
            return {key: ("sha256:" + hashlib.sha256(item.encode()).hexdigest()
                          if key in {"stream_cursor", "cursor"} and isinstance(item, str) else redact(item))
                    for key, item in value.items()}
        if isinstance(value, list):
            return [redact(item) for item in value]
        return value
    data = json.dumps(redact(receipt), indent=2) + "\n"
    (output / "receipt.json").write_text(data)
    print(json.dumps({"output": str(output), "sha256": hashlib.sha256(data.encode()).hexdigest(),
                      "observer_completed": ok, "acceptance": False}))
    return ok


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--exported", type=Path, required=True)
    parser.add_argument("--venue", choices=dr.VENUES, required=True)
    parser.add_argument("--fault", choices=dr.FAULTS, required=True)
    parser.add_argument("--core-binary", type=Path, help="isolated test-only compiled binary; no image build")
    args = parser.parse_args()
    return 0 if exercise(args.output, args.exported, args.venue, args.fault, args.core_binary) else 1


if __name__ == "__main__":
    raise SystemExit(main())
