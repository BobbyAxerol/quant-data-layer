"""Pure, process-local KN recovery decisions; never performs lifecycle operations."""

from dataclasses import dataclass
import math
import re
import time


SCHEMA = "qdl.kn.recovery-observation.v1"
GATES = ("disk_safe", "quorum_isr_ready", "projector_progress_ready", "consumer_eligible")
STAGES = ("infra", "projector", "reader")
ROLE_STATES = frozenset(("created", "running", "paused", "restarting", "removing", "exited", "dead"))


def _number(value):
    try:
        return type(value) in (int, float) and math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def _identity(role):
    return (isinstance(role, dict)
            and isinstance(role.get("id"), str)
            and re.fullmatch(r"[0-9a-f]{64}", role["id"]) is not None
            and isinstance(role.get("image"), str)
            and re.fullmatch(r"sha256:[0-9a-f]{64}", role["image"]) is not None)


@dataclass(frozen=True)
class RecoveryPolicy:
    deadline_seconds: float
    max_observation_age_seconds: float
    stable_samples: int
    retry_seconds: float
    max_start_attempts: int

    def __post_init__(self):
        for name in ("deadline_seconds", "max_observation_age_seconds", "retry_seconds"):
            value = getattr(self, name)
            if not _number(value) or value <= 0:
                raise ValueError(name + " must be finite and positive")
        for name in ("stable_samples", "max_start_attempts"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(name + " must be a positive integer")


class RecoveryController:
    """One absolute deadline per episode, including unknown/pressure/backoff time.

    step returns a JSON-compatible dict. actions is empty or contains one
    {action: 'start', role, id, image, attempt} proposal, not an execution receipt.
    A proposal consumes an attempt even if an external adapter declines it.
    Retry delay doubles up to deadline_seconds. BLOCKED is sticky; an operator
    must create a new controller after review. SERVING keeps monitoring, and
    the first unhealthy/unknown poll starts a new bounded episode.

    Identical fresh timestamps preserve stability but never add credit or retry.
    Changed data at an identical timestamp and regressing timestamps are unknown.
    Only validated gate/role fields participate in duplicate comparison.
    """

    def __init__(self, policy, expected_roles, clock=time.monotonic):
        if not isinstance(policy, RecoveryPolicy):
            raise ValueError("policy must be RecoveryPolicy")
        if not isinstance(expected_roles, dict) or not expected_roles:
            raise ValueError("expected_roles must be a nonempty roster")
        self._roles = {}
        for name, role in expected_roles.items():
            if (not isinstance(name, str) or not name or not _identity(role)
                    or role.get("stage") not in STAGES):
                raise ValueError("invalid expected role")
            self._roles[name] = {key: role[key] for key in ("id", "image", "stage")}
        if len({role["id"] for role in self._roles.values()}) != len(self._roles):
            raise ValueError("duplicate expected container ID")
        self.policy = policy
        self._clock = clock
        now = clock()
        if not _number(now) or not math.isfinite(now + policy.deadline_seconds):
            raise ValueError("invalid clock/deadline")
        self._last_now = now
        self._deadline = now + policy.deadline_seconds
        self._last_observation = None
        self._stable = 0
        self._attempts = {}
        self._retry_at = {}
        self._delay = {}
        self.state = "HOLD_UNKNOWN"
        self._reason = "awaiting observation"

    def _result(self, state, reason, actions=None):
        self.state, self._reason = state, reason
        return {"state": state, "reason": reason, "actions": actions or [],
                "stable_samples": self._stable, "deadline_monotonic": self._deadline}

    def _validate(self, observation, now):
        if not isinstance(observation, dict) or observation.get("schema") != SCHEMA:
            return None, "invalid schema"
        stamp = observation.get("observed_monotonic")
        if not _number(stamp) or stamp > now or now - stamp > self.policy.max_observation_age_seconds:
            return None, "invalid, future or stale observation time"
        if any(type(observation.get(gate)) is not bool for gate in GATES):
            return None, "missing or non-boolean gate"
        roles = observation.get("roles")
        if not isinstance(roles, dict) or roles.keys() != self._roles.keys():
            return None, "role roster mismatch"
        normalized = {"observed_monotonic": stamp, "roles": {}}
        normalized.update({gate: observation[gate] for gate in GATES})
        for name, role in roles.items():
            if (not _identity(role) or not isinstance(role.get("state"), str)
                    or role["state"] not in ROLE_STATES):
                return None, "malformed role"
            normalized["roles"][name] = {key: role[key] for key in ("id", "image", "state")}
        return normalized, None

    def _start(self, stage, roles, now, duplicate):
        if duplicate:
            return [], None
        for name, expected in self._roles.items():
            if expected["stage"] != stage or roles[name]["state"] != "exited":
                continue
            if now < self._retry_at.get(name, 0):
                continue
            attempts = self._attempts.get(name, 0)
            if attempts >= self.policy.max_start_attempts:
                return [], "start attempts exhausted: " + name
            attempts += 1
            self._attempts[name] = attempts
            delay = min(self.policy.deadline_seconds,
                        self._delay[name] * 2 if name in self._delay else self.policy.retry_seconds)
            self._delay[name] = delay
            self._retry_at[name] = now + delay
            return [{"action": "start", "role": name, "id": expected["id"],
                     "image": expected["image"], "attempt": attempts}], None
        return [], None

    def step(self, observation):
        if self.state == "BLOCKED":
            return self._result("BLOCKED", self._reason)
        now = self._clock()
        if not _number(now) or now < self._last_now:
            self._stable = 0
            return self._result("BLOCKED", "invalid controller clock")
        self._last_now = now
        was_serving = self.state == "SERVING"
        if not was_serving and now >= self._deadline:
            self._stable = 0
            return self._result("BLOCKED", "recovery deadline exceeded")
        sample, error = self._validate(observation, now)
        duplicate = False
        if sample is not None:
            for name, role in sample["roles"].items():
                if any(role[key] != self._roles[name][key] for key in ("id", "image")):
                    self._stable = 0
                    return self._result("BLOCKED", "role identity drift: " + name)
            previous = self._last_observation
            if previous is not None:
                delta = sample["observed_monotonic"] - previous["observed_monotonic"]
                if delta < 0 or (delta == 0 and sample != previous):
                    error = "regressing or conflicting observation"
                duplicate = delta == 0
        healthy = (sample is not None and error is None
                   and all(sample[gate] for gate in GATES)
                   and all(role["state"] == "running" for role in sample["roles"].values()))
        if was_serving and not healthy:
            self._deadline = now + self.policy.deadline_seconds
            self._attempts.clear()
            self._retry_at.clear()
            self._delay.clear()
        if error is not None:
            self._stable = 0
            return self._result("HOLD_UNKNOWN", error)
        self._last_observation = sample
        if not healthy:
            self._stable = 0
        if not sample["disk_safe"]:
            return self._result("HOLD_PRESSURE", "disk pressure holds all starts")
        roles = sample["roles"]
        for stage, gate, state in (
                ("infra", "quorum_isr_ready", "WAIT_QUORUM"),
                ("projector", "projector_progress_ready", "WAIT_PROJECTOR"),
                ("reader", "consumer_eligible", "WAIT_CONSUMER")):
            running = all(roles[name]["state"] == "running" for name, role in self._roles.items()
                          if role["stage"] == stage)
            if not running or not sample[gate]:
                actions, blocked = self._start(stage, roles, now, duplicate)
                return self._result("BLOCKED" if blocked else state,
                                    blocked or "waiting for " + stage + " readiness", actions)
        if not duplicate:
            self._stable = min(self.policy.stable_samples, self._stable + 1)
        if self._stable >= self.policy.stable_samples:
            return self._result("SERVING", "all gates stable")
        return self._result("WAIT_CONSUMER", "waiting for stable observations")
