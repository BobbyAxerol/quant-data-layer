"""Bounded KN recovery orchestration with explicit exact-container start authority.

The probe is an operator-selected executable returning a current typed observation.
It must measure actual disk/quorum/progress and production consumer validation;
heartbeat is not consumer eligibility. Defaults to observation only. Never deletes,
flushes, resets offsets, recreates, stops or restarts running containers.
"""
import argparse
import json
import math
import os
from pathlib import Path
import subprocess
import time

if __package__:
    from scripts.kn_recovery_state import RecoveryController, RecoveryPolicy
else:
    from kn_recovery_state import RecoveryController, RecoveryPolicy


MAX_BYTES = 262144


def read_packet(path):
    with open(path, "rb") as handle:
        data = handle.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("packet too large")
    value = json.loads(data)
    if value.get("schema") != "qdl.kn.recovery-packet.v1":
        raise ValueError("packet schema")
    command = value.get("probe_command")
    if (not isinstance(command, list) or not command or len(command) > 32
            or any(not isinstance(v, str) or not v or len(v) > 4096 for v in command)
            or not Path(command[0]).is_absolute()):
        raise ValueError("absolute bounded probe executable required")
    return value


def bounded_probe(command, timeout):
    # Temporary output files avoid unbounded PIPE buffering from a broken probe.
    import tempfile
    import resource
    def limit_output():
        resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_BYTES, MAX_BYTES))
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        result = subprocess.run(command, stdout=output, stderr=errors, timeout=timeout, check=False,
                                preexec_fn=limit_output)
        output.seek(0)
        raw = output.read(MAX_BYTES + 1)
    if result.returncode or len(raw) > MAX_BYTES:
        raise ValueError("PROBE_FAILED_OR_TOO_LARGE")
    return json.loads(raw)


def start_exact(role, expected, approved_ids, timeout, run=subprocess.run, clock=time.monotonic):
    if expected["id"] not in approved_ids:
        return {"action": "PROPOSED_START", "role": role, "executed": False}
    deadline = clock() + timeout
    command = ["docker", "inspect", "--format",
               "{{.Id}}|{{.Image}}|{{.Name}}|{{.State.Status}}|{{.State.OOMKilled}}", expected["id"]]
    result = run(command, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        raise ValueError("PRE_START_INSPECT_FAILED")
    parts = result.stdout.strip().split("|")
    if len(parts) != 5 or parts[:3] != [expected["id"], expected["image"], "/" + role]:
        raise ValueError("PRE_START_IDENTITY_DRIFT")
    if parts[3] != "exited" or parts[4] != "false":
        raise ValueError("PRE_START_STATE_NOT_AUTHORIZED")
    remaining = deadline - clock()
    if remaining <= 0:
        raise ValueError("PRE_START_DEADLINE_EXPIRED")
    result = run(["docker", "start", expected["id"]], capture_output=True, text=True,
                 timeout=remaining, check=False)
    return {"action": "START", "role": role, "executed": True,
            "accepted": result.returncode == 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--approve-start-id", action="append", default=[])
    args = parser.parse_args(argv)
    packet = read_packet(args.packet)
    policy = RecoveryPolicy(**packet["policy"])
    controller = RecoveryController(policy, packet["roles"])
    approved = set(args.approve_start_id)
    if len(approved) != len(args.approve_start_id) or not approved <= {r["id"] for r in packet["roles"].values()}:
        raise ValueError("start approval must match exact packet IDs")
    interval = packet["poll_seconds"]
    if type(interval) not in (int, float) or not math.isfinite(interval) or not .1 <= interval <= 60:
        raise ValueError("invalid polling interval")
    if policy.deadline_seconds > 3600 or len(packet["roles"]) > 64:
        raise ValueError("bounded run requires <= 3600 seconds and <= 64 roles")
    end = time.monotonic() + policy.deadline_seconds
    descriptor = os.open(args.out, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as output:
        while True:
            remaining = end - time.monotonic()
            try:
                observation = bounded_probe(packet["probe_command"], max(.001, min(10, remaining))) if remaining > 0 else {}
            except (ValueError, OSError, subprocess.TimeoutExpired):
                observation = {}
            decision = controller.step(observation)
            actions = []
            for action in decision.get("actions", []):
                remaining = end - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    actions.append(start_exact(action["role"], packet["roles"][action["role"]], approved,
                                               min(10, remaining)))
                except (ValueError, OSError, subprocess.TimeoutExpired):
                    actions.append({"role": action["role"], "action": "START_REFUSED_OR_UNKNOWN", "executed": None})
            output.write(json.dumps({"at_ns": time.time_ns(), "decision": decision, "actions": actions}, allow_nan=False) + "\n")
            output.flush()
            if decision["state"] == "SERVING":
                return 0
            if decision["state"] == "BLOCKED":
                return 3
            time.sleep(min(interval, max(0, end - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
