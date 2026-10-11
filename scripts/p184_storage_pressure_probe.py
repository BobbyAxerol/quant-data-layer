"""Test-only ENOSPC probe: dedicated tiny /fault tmpfs, never host storage."""
import errno
import json
import os
from pathlib import Path
import time
from scripts.kn_disk_pressure import DiskPressure, sample_disk


def main():
    root = Path("/fault")
    mounts = Path("/proc/self/mountinfo").read_text().splitlines()
    if os.environ.get("P184_ISOLATED_STORAGE_FAULT") != "1" or not any(
        line.split()[4] == str(root) and line.split(" - ")[1].split()[0] == "tmpfs"
        for line in mounts
    ):
        raise RuntimeError("DEDICATED_TEST_TMPFS_REQUIRED")
    fs = os.statvfs(root)
    if not 0 < fs.f_blocks * fs.f_frsize <= 32 * 1024**2 or not 0 < fs.f_files <= 128 or list(root.iterdir()):
        raise RuntimeError("EMPTY_BOUNDED_TEST_TMPFS_REQUIRED")
    # Test-only capacities; not deployment policy or Kafka recovery budgets.
    policy = DiskPressure(1024**2, 2 * 1024**2, 4, 8, 1, 2)
    observations = []
    started = time.monotonic()
    def observe(label):
        row = policy.observe(str(root), sample_disk(root))
        observations.append({"stage": label, **row})
        return row
    def check_deadline():
        if time.monotonic() - started > 10:
            raise TimeoutError("ISOLATED_FAULT_BUDGET")
    try:
        observe("baseline")
        time.sleep(.01)
        assert not observe("initial_ready")["pressure"]
        with (root / "bytes").open("wb", buffering=0) as output:
            try:
                while True:
                    check_deadline()
                    output.write(b"x" * 65536)
            except OSError as error:
                if error.errno != errno.ENOSPC:
                    raise
        row = observe("bytes_exhausted")
        assert "DISK_BYTES_RESERVE" in row["reasons"]
        (root / "bytes").unlink()
        time.sleep(.01)
        assert not observe("bytes_recovered")["pressure"]
        try:
            for i in range(256):
                check_deadline()
                (root / ("inode-" + str(i))).touch(exist_ok=False)
            raise AssertionError("inode fault not reached")
        except OSError as error:
            if error.errno != errno.ENOSPC:
                raise
        row = observe("inodes_exhausted")
        assert "DISK_INODES_RESERVE" in row["reasons"]
        for path in root.glob("inode-*"):
            path.unlink()
        time.sleep(.01)
        assert not observe("inodes_recovered")["pressure"]
        (root / "verification").write_bytes(b"test-only-after-recovery")
        assert (root / "verification").read_bytes() == b"test-only-after-recovery"
        print(json.dumps({"status": "ISOLATED_FILESYSTEM_PASS", "seconds": time.monotonic() - started,
                          "provider_data": False, "kafka_recovery": "NOT_TESTED", "observations": observations}))
    finally:
        for path in root.iterdir():
            if path.name in {"bytes", "verification"} or path.name.startswith("inode-"):
                path.unlink()


if __name__ == "__main__":
    main()
