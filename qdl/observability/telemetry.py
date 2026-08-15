from __future__ import annotations

import hashlib
import math
import re
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Mapping


_METRIC_NAME = re.compile(r"^qdl_[a-z0-9_]+$")
_SAFE_LABELS = frozenset(
    {
        "environment",
        "service_name",
        "runtime_role",
        "venue",
        "market",
        "feed_type",
        "source_id",
        "state",
        "result",
        "error_code",
        "consumer_grade",
        "instrument_bucket",
    }
)
_CONTEXT_FIELDS = (
    "environment",
    "service_name",
    "service_version",
    "instance_id",
    "runtime_role",
    "venue",
    "market",
    "instrument_id",
    "feed_type",
    "source_id",
    "source_session_id",
    "subscription_shard",
    "source_sequence",
    "event_id",
    "schema_version",
    "normalizer_version",
    "config_revision",
    "broker_topic",
    "broker_partition",
    "broker_offset",
    "consumer_group",
    "request_id",
    "trace_id",
)


@dataclass(frozen=True, slots=True)
class CorrelationContext:
    environment: str
    service_name: str
    service_version: str
    instance_id: str
    runtime_role: str
    venue: str | None = None
    market: str | None = None
    instrument_id: str | None = None
    feed_type: str | None = None
    source_id: str | None = None
    source_session_id: str | None = None
    subscription_shard: str | None = None
    source_sequence: str | None = None
    event_id: str | None = None
    schema_version: str | None = None
    normalizer_version: str | None = None
    config_revision: str | None = None
    broker_topic: str | None = None
    broker_partition: int | None = None
    broker_offset: int | None = None
    consumer_group: str | None = None
    request_id: str | None = None
    trace_id: str | None = None

    def fields(self) -> dict[str, str | int]:
        values = asdict(self)
        return {name: values[name] for name in _CONTEXT_FIELDS if values[name] is not None}

    def metric_labels(self) -> dict[str, str]:
        labels = {
            "environment": self.environment,
            "service_name": self.service_name,
            "runtime_role": self.runtime_role,
        }
        for name in ("venue", "market", "feed_type", "source_id"):
            value = getattr(self, name)
            if value is not None:
                labels[name] = value
        if self.instrument_id is not None:
            digest = hashlib.sha256(self.instrument_id.encode("utf-8")).digest()
            labels["instrument_bucket"] = str(int.from_bytes(digest[:2], "big") % 64)
        return labels


@dataclass(frozen=True, slots=True)
class TelemetrySnapshot:
    counters: Mapping[str, float]
    gauges: Mapping[str, float]
    histograms: Mapping[str, tuple[float, ...]]
    rejected_series: int
    created_at_ns: int


class BoundedTelemetry:
    """OTel-friendly bounded telemetry buffer with controlled label cardinality."""

    def __init__(self, *, max_series: int = 2048, max_points_per_histogram: int = 4096):
        if max_series <= 0 or max_points_per_histogram <= 0:
            raise ValueError("telemetry bounds must be positive")
        self._max_series = max_series
        self._max_points = max_points_per_histogram
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._histograms: dict[tuple[str, tuple[tuple[str, str], ...]], deque[float]] = {}
        self._rejected_series = 0
        self._lock = threading.RLock()

    @staticmethod
    def _key(name: str, labels: Mapping[str, str] | None) -> tuple[str, tuple[tuple[str, str], ...]]:
        if not _METRIC_NAME.fullmatch(name):
            raise ValueError("metric names must use the qdl_ snake-case namespace")
        normalized = tuple(sorted((str(key), str(value)) for key, value in (labels or {}).items()))
        unknown = {key for key, _ in normalized} - _SAFE_LABELS
        if unknown:
            raise ValueError(f"unbounded or unsupported metric labels: {sorted(unknown)}")
        if any(len(value) > 120 for _, value in normalized):
            raise ValueError("metric label values must be bounded")
        return name, normalized

    def _admit(self, key: tuple[str, tuple[tuple[str, str], ...]]) -> bool:
        all_keys = set(self._counters) | set(self._gauges) | set(self._histograms)
        if key in all_keys or len(all_keys) < self._max_series:
            return True
        self._rejected_series += 1
        return False

    def add(self, name: str, value: float = 1.0, *, labels: Mapping[str, str] | None = None) -> bool:
        if not math.isfinite(value) or value < 0:
            raise ValueError("counter increments must be finite and non-negative")
        key = self._key(name, labels)
        with self._lock:
            if not self._admit(key):
                return False
            self._counters[key] += value
        return True

    def set(self, name: str, value: float, *, labels: Mapping[str, str] | None = None) -> bool:
        if not math.isfinite(value):
            raise ValueError("gauge values must be finite")
        key = self._key(name, labels)
        with self._lock:
            if not self._admit(key):
                return False
            self._gauges[key] = value
        return True

    def observe(self, name: str, value: float, *, labels: Mapping[str, str] | None = None) -> bool:
        if not math.isfinite(value) or value < 0:
            raise ValueError("histogram observations must be finite and non-negative")
        key = self._key(name, labels)
        with self._lock:
            if not self._admit(key):
                return False
            self._histograms.setdefault(key, deque(maxlen=self._max_points)).append(value)
        return True

    @staticmethod
    def _display_key(key: tuple[str, tuple[tuple[str, str], ...]]) -> str:
        name, labels = key
        suffix = ",".join(f"{label}={value}" for label, value in labels)
        return f"{name}{{{suffix}}}" if suffix else name

    def snapshot(self) -> TelemetrySnapshot:
        with self._lock:
            return TelemetrySnapshot(
                counters={self._display_key(key): value for key, value in self._counters.items()},
                gauges={self._display_key(key): value for key, value in self._gauges.items()},
                histograms={self._display_key(key): tuple(values) for key, values in self._histograms.items()},
                rejected_series=self._rejected_series,
                created_at_ns=time.time_ns(),
            )


@dataclass(frozen=True, slots=True)
class SLOPolicy:
    availability_min: float
    completeness_min: float
    latency_p99_ms_max: float
    canonical_drops_max: int = 0

    def __post_init__(self) -> None:
        if not 0 <= self.availability_min <= 1 or not 0 <= self.completeness_min <= 1:
            raise ValueError("availability and completeness thresholds must be ratios")
        if self.latency_p99_ms_max <= 0 or self.canonical_drops_max < 0:
            raise ValueError("invalid latency or drop threshold")


@dataclass(frozen=True, slots=True)
class SLOEvidence:
    availability: float
    completeness: float
    latency_p99_ms: float
    canonical_drops: int
    sample_count: int


class AlertDecision(str, Enum):
    PASS = "PASS"
    SEV1 = "SEV1"
    SEV2 = "SEV2"


@dataclass(frozen=True, slots=True)
class SLOEvaluation:
    decision: AlertDecision
    violations: tuple[str, ...]
    availability_budget_remaining: float
    completeness_budget_remaining: float


def evaluate_slo(policy: SLOPolicy, evidence: SLOEvidence) -> SLOEvaluation:
    if evidence.sample_count <= 0:
        return SLOEvaluation(AlertDecision.SEV2, ("NO_EVIDENCE",), 0.0, 0.0)
    violations = []
    if evidence.availability < policy.availability_min:
        violations.append("AVAILABILITY")
    if evidence.completeness < policy.completeness_min:
        violations.append("COMPLETENESS")
    if evidence.latency_p99_ms > policy.latency_p99_ms_max:
        violations.append("LATENCY_P99")
    if evidence.canonical_drops > policy.canonical_drops_max:
        violations.append("CANONICAL_DROP")
    decision = (
        AlertDecision.SEV1
        if "CANONICAL_DROP" in violations or "COMPLETENESS" in violations
        else AlertDecision.SEV2 if violations else AlertDecision.PASS
    )
    return SLOEvaluation(
        decision,
        tuple(violations),
        evidence.availability - policy.availability_min,
        evidence.completeness - policy.completeness_min,
    )
