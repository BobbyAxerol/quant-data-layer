"""Read-only filesystem observations and explicit, operator-owned disk policy."""
import math
import os
import time


def sample_disk(path):
    try:
        device = os.stat(path).st_dev
        fs = os.statvfs(path)
        return {"device": device, "monotonic": time.monotonic(),
                "total_bytes": fs.f_blocks * fs.f_frsize,
                "available_bytes": fs.f_bavail * fs.f_frsize,
                "total_inodes": fs.f_files, "available_inodes": fs.f_favail}
    except OSError:
        return None


class DiskPressure:
    """Per-path hysteresis; no lifecycle authority and no production defaults.

    First/missing/non-comparable observations fail closed. Recovery requires both
    capacity recovery floors and time-to-reserve above the recovery horizon.
    Rates measure net available-capacity depletion, not cumulative write volume;
    a null time-to-reserve with zero growth means no observed depletion.
    State is process-local and does not certify sustained application recovery.
    """

    def __init__(self, reserve_bytes, recovery_bytes, reserve_inodes,
                 recovery_inodes, reserve_seconds, recovery_seconds):
        values = (reserve_bytes, recovery_bytes, reserve_inodes,
                  recovery_inodes, reserve_seconds, recovery_seconds)
        if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError("disk thresholds must be finite and positive")
        if any(high <= low for low, high in zip(values[::2], values[1::2])):
            raise ValueError("disk recovery thresholds must exceed reserve thresholds")
        self.reserve = (reserve_bytes, reserve_inodes)
        self.recovery = (recovery_bytes, recovery_inodes)
        self.reserve_seconds = reserve_seconds
        self.recovery_seconds = recovery_seconds
        self.previous = {}
        self.pressured = set()

    def observe(self, path, observation):
        previous = self.previous.pop(path, None)
        result = {"observation": None, "reasons": []}
        reasons = result["reasons"]
        try:
            if not isinstance(observation, dict):
                raise ValueError("missing observation")
            for key in ("device", "total_bytes", "available_bytes", "total_inodes", "available_inodes"):
                if type(observation.get(key)) is not int or observation[key] < 0:
                    raise ValueError("invalid capacity")
            stamp = observation.get("monotonic")
            if type(stamp) not in (int, float) or not math.isfinite(stamp) or stamp < 0:
                raise ValueError("invalid clock")
            for unit in ("bytes", "inodes"):
                if not 0 <= observation["available_" + unit] <= observation["total_" + unit] or observation["total_" + unit] == 0:
                    raise ValueError("invalid capacity")
            # Journal only the validated schema, never arbitrary malformed input.
            observation = {key: observation[key] for key in (
                "device", "monotonic", "total_bytes", "available_bytes",
                "total_inodes", "available_inodes")}
            result["observation"] = observation
            self.previous[path] = dict(observation)
            for unit, reserve in zip(("bytes", "inodes"), self.reserve):
                if observation["available_" + unit] <= reserve:
                    reasons.append("DISK_" + unit.upper() + "_RESERVE")
            if previous is None or any(observation[k] != previous[k] for k in ("device", "total_bytes", "total_inodes")):
                raise ValueError("missing comparable baseline")
            elapsed = stamp - previous["monotonic"]
            if elapsed <= 0 or not math.isfinite(elapsed):
                raise ValueError("non-increasing clock")
            recovering = path in self.pressured
            horizon = self.recovery_seconds if recovering else self.reserve_seconds
            metrics = {}
            for unit, reserve, recovery in zip(("bytes", "inodes"), self.reserve, self.recovery):
                available = observation["available_" + unit]
                growth = max(0.0, (previous["available_" + unit] - available) / elapsed)
                seconds = max(0, available - reserve) / growth if growth else None
                if not math.isfinite(growth) or (seconds is not None and not math.isfinite(seconds)):
                    raise ValueError("invalid rate")
                metrics[unit] = {"growth_per_second": growth, "seconds_to_reserve": seconds}
                if seconds is not None and seconds <= horizon:
                    reasons.append("DISK_" + unit.upper() + "_TIME_TO_RESERVE")
                if recovering and available < recovery:
                    reasons.append("DISK_" + unit.upper() + "_RECOVERY_PENDING")
            result["metrics"] = metrics
        except (ValueError, TypeError, OverflowError):
            reasons.append("DISK_OBSERVATION_UNKNOWN")
        if reasons:
            self.pressured.add(path)
        else:
            self.pressured.discard(path)
        result["pressure"] = bool(reasons)
        return result
