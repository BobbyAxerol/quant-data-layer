from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
from typing import Any, Mapping

from qdl.demand.v1 import demand_pb2
from qdl.warmup.contracts import (
    IntervalSourcePolicy,
    WarmupSpecification,
    WarmupTimeRange,
)


class _StringEnum(str, Enum):
    pass


class DemandPurpose(_StringEnum):
    EXECUTION = "EXECUTION"
    ALPHA = "ALPHA"
    RESEARCH = "RESEARCH"
    OBSERVABILITY = "OBSERVABILITY"


class DemandFeed(_StringEnum):
    TRADE = "TRADE"
    QUOTE = "QUOTE"
    BAR = "BAR"
    BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
    BOOK_DELTA = "BOOK_DELTA"
    FUNDING_RATE = "FUNDING_RATE"
    OPEN_INTEREST = "OPEN_INTEREST"
    LONG_SHORT_RATIO = "LONG_SHORT_RATIO"
    TAKER_FLOW = "TAKER_FLOW"
    BASIS = "BASIS"
    MARK_PRICE = "MARK_PRICE"
    INDEX_PRICE = "INDEX_PRICE"


_SAMPLED_FRESHNESS_FEEDS = frozenset(
    {
        DemandFeed.LONG_SHORT_RATIO,
        DemandFeed.TAKER_FLOW,
        DemandFeed.BASIS,
    }
)
_INTERVAL_CAPABLE_FEEDS = _SAMPLED_FRESHNESS_FEEDS | frozenset(
    {DemandFeed.OPEN_INTEREST}
)
_POINT_FRESHNESS_MAX_MS = 86_400_000
# A final monthly bar may legitimately remain the newest valid sample for more
# than one day.  Keep that sampled-data bound explicit without relaxing point
# feeds such as trade/quote into an unbounded stale-data acceptance window.
_SAMPLED_FRESHNESS_MAX_MS = 366 * _POINT_FRESHNESS_MAX_MS


class DemandState(_StringEnum):
    REQUESTED = "REQUESTED"
    CONNECTING = "CONNECTING"
    WARMING = "WARMING"
    LIVE = "LIVE"
    DEGRADED = "DEGRADED"
    MARKET_CLOSED = "MARKET_CLOSED"
    UNSUPPORTED = "UNSUPPORTED"
    EXPIRED = "EXPIRED"


class CapabilityAvailability(_StringEnum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    TIER_GATED = "TIER_GATED"
    REGION_GATED = "REGION_GATED"
    UNVERIFIED = "UNVERIFIED"


class UniverseSelectorKind(_StringEnum):
    EXPLICIT = "EXPLICIT"
    UNIVERSE_REF = "UNIVERSE_REF"
    SEGMENT = "SEGMENT"
    CONTINUOUS = "CONTINUOUS"


_ALLOWED_DEMAND_TRANSITIONS: dict[DemandState, frozenset[DemandState]] = {
    DemandState.REQUESTED: frozenset({DemandState.CONNECTING, DemandState.UNSUPPORTED, DemandState.EXPIRED}),
    DemandState.CONNECTING: frozenset({DemandState.WARMING, DemandState.DEGRADED, DemandState.EXPIRED}),
    DemandState.WARMING: frozenset({DemandState.LIVE, DemandState.DEGRADED, DemandState.MARKET_CLOSED, DemandState.EXPIRED}),
    DemandState.LIVE: frozenset({DemandState.DEGRADED, DemandState.MARKET_CLOSED, DemandState.EXPIRED}),
    DemandState.DEGRADED: frozenset({DemandState.CONNECTING, DemandState.MARKET_CLOSED, DemandState.EXPIRED}),
    DemandState.MARKET_CLOSED: frozenset({DemandState.CONNECTING, DemandState.EXPIRED}),
    DemandState.UNSUPPORTED: frozenset({DemandState.REQUESTED, DemandState.EXPIRED}),
    DemandState.EXPIRED: frozenset({DemandState.REQUESTED}),
}


def demand_transition_allowed(previous: DemandState, current: DemandState) -> bool:
    """Shared control-plane lifecycle guard; adapters cannot skip continuity states."""
    return DemandState(current) in _ALLOWED_DEMAND_TRANSITIONS[DemandState(previous)]


_HEX = frozenset("0123456789abcdef")


def _enum(value: Any, kind: type[_StringEnum], field: str) -> _StringEnum:
    if isinstance(value, kind):
        return value
    try:
        return kind(str(value).upper())
    except ValueError as error:
        raise ValueError(f"{field} is invalid") from error


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{field} is required")
    return result


def _sha256(value: str | None, field: str) -> str | None:
    if value is None:
        return None
    result = str(value).strip().lower()
    if len(result) != 64 or any(character not in _HEX for character in result):
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")
    return result


@dataclass(frozen=True, slots=True)
class UniverseSelector:
    selector_id: str
    kind: UniverseSelectorKind
    venue: str
    market: str
    product_type: str
    native_symbols: tuple[str, ...] = ()
    universe_ref: str | None = None
    segment_id: str | None = None
    continuous_family: str | None = None
    continuous_roll_policy: str | None = None
    continuous_minimum_days_to_expiry: int = 0
    expected_universe_sha256: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "selector_id", _text(self.selector_id, "selector_id"))
        object.__setattr__(self, "kind", _enum(self.kind, UniverseSelectorKind, "kind"))
        object.__setattr__(self, "venue", _text(self.venue, "venue").upper())
        object.__setattr__(self, "market", _text(self.market, "market").upper())
        object.__setattr__(self, "product_type", _text(self.product_type, "product_type").upper())
        normalized_symbols = tuple(
            sorted({_text(value, "native_symbol").upper() for value in self.native_symbols})
        )
        object.__setattr__(self, "native_symbols", normalized_symbols)
        object.__setattr__(self, "universe_ref", self._optional(self.universe_ref))
        object.__setattr__(self, "segment_id", self._optional(self.segment_id))
        object.__setattr__(self, "continuous_family", self._optional(self.continuous_family))
        object.__setattr__(self, "continuous_roll_policy", self._optional(self.continuous_roll_policy))
        object.__setattr__(
            self,
            "expected_universe_sha256",
            _sha256(self.expected_universe_sha256, "expected_universe_sha256"),
        )
        if not 0 <= self.continuous_minimum_days_to_expiry <= 3650:
            raise ValueError("continuous_minimum_days_to_expiry is outside bounds")
        if self.kind is UniverseSelectorKind.EXPLICIT:
            if not normalized_symbols or any(
                value is not None
                for value in (
                    self.universe_ref,
                    self.segment_id,
                    self.continuous_family,
                    self.continuous_roll_policy,
                )
            ):
                raise ValueError("explicit selector needs only native_symbols")
        elif self.kind is UniverseSelectorKind.UNIVERSE_REF:
            if not self.universe_ref or normalized_symbols or self.segment_id or self.continuous_family:
                raise ValueError("universe selector needs only universe_ref")
        elif self.kind is UniverseSelectorKind.SEGMENT:
            if not self.universe_ref or not self.segment_id or normalized_symbols or self.continuous_family:
                raise ValueError("segment selector needs universe_ref and segment_id")
        elif self.kind is UniverseSelectorKind.CONTINUOUS:
            if (
                not self.continuous_family
                or not self.continuous_roll_policy
                or normalized_symbols
                or self.universe_ref
                or self.segment_id
            ):
                raise ValueError("continuous selector needs family and roll policy")

    @staticmethod
    def _optional(value: str | None) -> str | None:
        result = str(value or "").strip()
        return result or None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "UniverseSelector":
        allowed = {
            "selector_id", "kind", "venue", "market", "product_type",
            "native_symbols", "universe_ref", "segment_id", "continuous_family",
            "continuous_roll_policy", "continuous_minimum_days_to_expiry",
            "expected_universe_sha256",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown universe selector fields: {sorted(unknown)}")
        symbols = value.get("native_symbols", ())
        if not isinstance(symbols, (list, tuple)):
            raise ValueError("native_symbols must be a list")
        return cls(
            selector_id=value.get("selector_id", ""),
            kind=value.get("kind", ""),
            venue=value.get("venue", ""),
            market=value.get("market", ""),
            product_type=value.get("product_type", ""),
            native_symbols=tuple(str(item) for item in symbols),
            universe_ref=value.get("universe_ref"),
            segment_id=value.get("segment_id"),
            continuous_family=value.get("continuous_family"),
            continuous_roll_policy=value.get("continuous_roll_policy"),
            continuous_minimum_days_to_expiry=int(
                value.get("continuous_minimum_days_to_expiry", 0)
            ),
            expected_universe_sha256=value.get("expected_universe_sha256"),
        )

    def canonical_mapping(self) -> dict[str, Any]:
        return {
            "selector_id": self.selector_id,
            "kind": self.kind.value,
            "venue": self.venue,
            "market": self.market,
            "product_type": self.product_type,
            "native_symbols": list(self.native_symbols),
            "universe_ref": self.universe_ref,
            "segment_id": self.segment_id,
            "continuous_family": self.continuous_family,
            "continuous_roll_policy": self.continuous_roll_policy,
            "continuous_minimum_days_to_expiry": self.continuous_minimum_days_to_expiry,
            "expected_universe_sha256": self.expected_universe_sha256,
        }

    def to_proto(self) -> demand_pb2.UniverseSelector:
        result = demand_pb2.UniverseSelector(
            selector_id=self.selector_id,
            venue=self.venue,
            market=self.market,
            product_type=self.product_type,
            universe_ref=self.universe_ref or "",
            expected_universe_sha256=(
                bytes.fromhex(self.expected_universe_sha256)
                if self.expected_universe_sha256 is not None
                else b""
            ),
        )
        if self.kind is UniverseSelectorKind.EXPLICIT:
            result.explicit_symbols.native_symbols.extend(self.native_symbols)
        elif self.kind is UniverseSelectorKind.UNIVERSE_REF:
            result.universe_ref = self.universe_ref
        elif self.kind is UniverseSelectorKind.SEGMENT:
            result.segment_id = self.segment_id
        else:
            result.continuous_contract.family = self.continuous_family
            result.continuous_contract.roll_policy = self.continuous_roll_policy
            result.continuous_contract.minimum_days_to_expiry = (
                self.continuous_minimum_days_to_expiry
            )
        return result

    @classmethod
    def from_proto(cls, value: demand_pb2.UniverseSelector) -> "UniverseSelector":
        selector = value.WhichOneof("selector")
        common = {
            "selector_id": value.selector_id,
            "venue": value.venue,
            "market": value.market,
            "product_type": value.product_type,
            "expected_universe_sha256": (
                bytes(value.expected_universe_sha256).hex()
                if value.expected_universe_sha256
                else None
            ),
        }
        if selector == "explicit_symbols":
            return cls(
                kind=UniverseSelectorKind.EXPLICIT,
                native_symbols=tuple(value.explicit_symbols.native_symbols),
                **common,
            )
        if selector is None and value.universe_ref:
            return cls(
                kind=UniverseSelectorKind.UNIVERSE_REF,
                universe_ref=value.universe_ref,
                **common,
            )
        if selector == "segment_id":
            return cls(
                kind=UniverseSelectorKind.SEGMENT,
                universe_ref=value.universe_ref,
                segment_id=value.segment_id,
                **common,
            )
        if selector == "continuous_contract":
            return cls(
                kind=UniverseSelectorKind.CONTINUOUS,
                continuous_family=value.continuous_contract.family,
                continuous_roll_policy=value.continuous_contract.roll_policy,
                continuous_minimum_days_to_expiry=value.continuous_contract.minimum_days_to_expiry,
                **common,
            )
        raise ValueError("universe selector target is required")


@dataclass(frozen=True, slots=True)
class DataRequirement:
    consumer_id: str
    purpose: DemandPurpose
    universe: UniverseSelector
    feed: DemandFeed
    source_policy_id: str
    interval: str | None = None
    warmup_limit: int = 0
    max_freshness_ms: int | None = None
    priority: int = 100
    ttl_seconds: int = 180
    require_final_bars: bool = False
    require_live: bool = True
    execution_grade: bool = False
    depth_levels: int = 0
    configuration_revision: int = 1
    warmup: WarmupSpecification | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "consumer_id", _text(self.consumer_id, "consumer_id"))
        object.__setattr__(self, "purpose", _enum(self.purpose, DemandPurpose, "purpose"))
        object.__setattr__(self, "feed", _enum(self.feed, DemandFeed, "feed"))
        object.__setattr__(self, "source_policy_id", _text(self.source_policy_id, "source_policy_id"))
        interval = UniverseSelector._optional(self.interval)
        object.__setattr__(self, "interval", interval)
        if not 0 <= self.warmup_limit <= 100_000:
            raise ValueError("warmup_limit is outside bounds")
        if self.warmup is not None and not isinstance(
            self.warmup, WarmupSpecification
        ):
            raise TypeError("warmup must use WarmupSpecification")
        if self.warmup is not None and self.warmup.rows is not None:
            if self.warmup_limit not in {0, self.warmup.rows}:
                raise ValueError("warmup_limit conflicts with warmup.rows")
        if self.warmup is not None and self.warmup.time_range is not None:
            if self.warmup_limit:
                raise ValueError("time-range warmup cannot also declare warmup_limit")
        if self.max_freshness_ms is not None:
            maximum_freshness = (
                _SAMPLED_FRESHNESS_MAX_MS
                if self.feed is DemandFeed.BAR or self.feed in _SAMPLED_FRESHNESS_FEEDS
                else _POINT_FRESHNESS_MAX_MS
            )
            if not 1 <= self.max_freshness_ms <= maximum_freshness:
                raise ValueError("max_freshness_ms is outside bounds for feed semantics")
        if not 0 <= self.priority <= 1_000:
            raise ValueError("priority is outside bounds")
        if not 30 <= self.ttl_seconds <= 3_600:
            raise ValueError("ttl_seconds must be between 30 and 3600")
        if not 0 <= self.depth_levels <= 10_000:
            raise ValueError("depth_levels is outside bounds")
        if self.configuration_revision < 1:
            raise ValueError("configuration_revision must be positive")
        if self.feed is DemandFeed.BAR:
            if not interval:
                raise ValueError("BAR requirement needs interval")
        elif self.feed not in _INTERVAL_CAPABLE_FEEDS and interval is not None:
            raise ValueError("interval is valid only for BAR and interval-capable metric requirements")
        if self.require_final_bars and self.feed is not DemandFeed.BAR:
            raise ValueError("require_final_bars is valid only for BAR")
        if self.depth_levels and self.feed not in {
            DemandFeed.BOOK_SNAPSHOT,
            DemandFeed.BOOK_DELTA,
        }:
            raise ValueError("depth_levels is valid only for book feeds")
        if self.purpose is DemandPurpose.EXECUTION:
            if not self.execution_grade or not self.require_live:
                raise ValueError("execution demand must require execution-grade live data")
        elif self.execution_grade:
            raise ValueError("execution_grade is reserved for EXECUTION purpose")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "DataRequirement":
        allowed = {
            "consumer_id", "purpose", "universe", "feed", "source_policy_id",
            "interval", "warmup_limit", "max_freshness_ms", "priority",
            "ttl_seconds", "require_final_bars", "require_live", "execution_grade",
            "depth_levels", "configuration_revision",
            "warmup",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown data requirement fields: {sorted(unknown)}")
        universe = value.get("universe")
        if not isinstance(universe, Mapping):
            raise ValueError("universe must be a mapping")
        return cls(
            consumer_id=value.get("consumer_id", ""),
            purpose=value.get("purpose", ""),
            universe=UniverseSelector.from_mapping(universe),
            feed=value.get("feed", ""),
            source_policy_id=value.get("source_policy_id", ""),
            interval=value.get("interval"),
            warmup_limit=int(value.get("warmup_limit", 0)),
            max_freshness_ms=(
                int(value["max_freshness_ms"])
                if value.get("max_freshness_ms") is not None
                else None
            ),
            priority=int(value.get("priority", 100)),
            ttl_seconds=int(value.get("ttl_seconds", 180)),
            require_final_bars=bool(value.get("require_final_bars", False)),
            require_live=bool(value.get("require_live", True)),
            execution_grade=bool(value.get("execution_grade", False)),
            depth_levels=int(value.get("depth_levels", 0)),
            configuration_revision=int(value.get("configuration_revision", 1)),
            warmup=(
                WarmupSpecification.from_mapping(value["warmup"])
                if isinstance(value.get("warmup"), Mapping)
                else None
            ),
        )

    def canonical_mapping(self) -> dict[str, Any]:
        mapping = {
            "consumer_id": self.consumer_id,
            "purpose": self.purpose.value,
            "universe": self.universe.canonical_mapping(),
            "feed": self.feed.value,
            "source_policy_id": self.source_policy_id,
            "interval": self.interval,
            "warmup_limit": self.warmup_limit,
            "max_freshness_ms": self.max_freshness_ms,
            "priority": self.priority,
            "ttl_seconds": self.ttl_seconds,
            "require_final_bars": self.require_final_bars,
            "require_live": self.require_live,
            "execution_grade": self.execution_grade,
            "depth_levels": self.depth_levels,
            "configuration_revision": self.configuration_revision,
        }
        # Preserve requirement IDs produced before the additive Phase 10.2
        # contract. An absent optional field is not a semantic change and must
        # not churn manifests, leases or consumer checkpoints.
        if self.warmup is not None:
            mapping["warmup"] = self.warmup.canonical_mapping()
        return mapping

    @property
    def requirement_id(self) -> str:
        encoded = json.dumps(
            self.canonical_mapping(), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(encoded).hexdigest()

    @property
    def warmup_specification(self) -> WarmupSpecification | None:
        if self.warmup is not None:
            return self.warmup
        if self.warmup_limit:
            return WarmupSpecification.for_rows(self.warmup_limit)
        return None

    def to_proto(self) -> demand_pb2.DataRequirement:
        result = demand_pb2.DataRequirement(
            consumer_id=self.consumer_id,
            purpose=getattr(demand_pb2, f"DEMAND_PURPOSE_{self.purpose.value}"),
            universe=self.universe.to_proto(),
            feed=getattr(demand_pb2, f"FEED_TYPE_{self.feed.value}"),
            interval=self.interval or "",
            warmup_limit=self.warmup_limit,
            max_freshness_ms=self.max_freshness_ms or 0,
            priority=self.priority,
            ttl_seconds=self.ttl_seconds,
            require_final_bars=self.require_final_bars,
            require_live=self.require_live,
            execution_grade=self.execution_grade,
            source_policy_id=self.source_policy_id,
            depth_levels=self.depth_levels,
            configuration_revision=self.configuration_revision,
        )
        if self.warmup is not None:
            proto = demand_pb2.WarmupSpecification(
                interval_source_policy=getattr(
                    demand_pb2,
                    f"INTERVAL_SOURCE_POLICY_{self.warmup.interval_source_policy.value}",
                ),
                max_cache_age_ms=self.warmup.max_cache_age_ms,
                deadline_ms=self.warmup.deadline_ms,
            )
            if self.warmup.rows is not None:
                proto.rows = self.warmup.rows
            else:
                assert self.warmup.time_range is not None
                proto.time_range.CopyFrom(demand_pb2.WarmupTimeRange(
                    start_time_ns=self.warmup.time_range.start_time_ns,
                    end_time_ns=self.warmup.time_range.end_time_ns,
                ))
            result.warmup.CopyFrom(proto)
        return result

    @classmethod
    def from_proto(cls, value: demand_pb2.DataRequirement) -> "DataRequirement":
        if value.purpose == demand_pb2.DEMAND_PURPOSE_UNSPECIFIED:
            raise ValueError("demand purpose is required")
        if value.feed == demand_pb2.FEED_TYPE_UNSPECIFIED:
            raise ValueError("demand feed is required")
        warmup = None
        if value.HasField("warmup"):
            horizon = value.warmup.WhichOneof("horizon")
            if horizon is None:
                raise ValueError("warmup horizon is required")
            if value.warmup.interval_source_policy == demand_pb2.INTERVAL_SOURCE_POLICY_UNSPECIFIED:
                raise ValueError("warmup interval source policy is required")
            warmup = WarmupSpecification(
                rows=value.warmup.rows if horizon == "rows" else None,
                time_range=(
                    WarmupTimeRange(
                        value.warmup.time_range.start_time_ns,
                        value.warmup.time_range.end_time_ns,
                    )
                    if horizon == "time_range"
                    else None
                ),
                interval_source_policy=IntervalSourcePolicy(
                    demand_pb2.IntervalSourcePolicy.Name(
                        value.warmup.interval_source_policy
                    ).removeprefix("INTERVAL_SOURCE_POLICY_")
                ),
                max_cache_age_ms=value.warmup.max_cache_age_ms,
                deadline_ms=value.warmup.deadline_ms,
            )
        return cls(
            consumer_id=value.consumer_id,
            purpose=demand_pb2.DemandPurpose.Name(value.purpose).removeprefix("DEMAND_PURPOSE_"),
            universe=UniverseSelector.from_proto(value.universe),
            feed=demand_pb2.FeedType.Name(value.feed).removeprefix("FEED_TYPE_"),
            source_policy_id=value.source_policy_id,
            interval=value.interval or None,
            warmup_limit=value.warmup_limit,
            max_freshness_ms=value.max_freshness_ms or None,
            priority=value.priority,
            ttl_seconds=value.ttl_seconds,
            require_final_bars=value.require_final_bars,
            require_live=value.require_live,
            execution_grade=value.execution_grade,
            depth_levels=value.depth_levels,
            configuration_revision=value.configuration_revision,
            warmup=warmup,
        )


@dataclass(frozen=True, slots=True)
class FeedCapability:
    capability_id: str
    venue: str
    market: str
    product_type: str
    feed: DemandFeed
    availability: CapabilityAvailability
    rest_history: bool = False
    live: bool = False
    snapshot: bool = False
    delta: bool = False
    sequence: bool = False
    checksum: bool = False
    resubscribe: bool = False
    resnapshot_on_gap: bool = False
    native_intervals: tuple[str, ...] = ()
    constraint: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "capability_id", _text(self.capability_id, "capability_id"))
        object.__setattr__(self, "venue", _text(self.venue, "venue").upper())
        object.__setattr__(self, "market", _text(self.market, "market").upper())
        object.__setattr__(self, "product_type", _text(self.product_type, "product_type").upper())
        object.__setattr__(self, "feed", _enum(self.feed, DemandFeed, "feed"))
        object.__setattr__(
            self,
            "availability",
            _enum(self.availability, CapabilityAvailability, "availability"),
        )
        object.__setattr__(
            self,
            "native_intervals",
            tuple(sorted({_text(value, "native_interval") for value in self.native_intervals})),
        )
        object.__setattr__(self, "constraint", UniverseSelector._optional(self.constraint))

    @property
    def enabled(self) -> bool:
        return self.availability is CapabilityAvailability.AVAILABLE

    def to_proto(self) -> demand_pb2.FeedCapability:
        return demand_pb2.FeedCapability(
            capability_id=self.capability_id,
            venue=self.venue,
            market=self.market,
            product_type=self.product_type,
            feed=getattr(demand_pb2, f"FEED_TYPE_{self.feed.value}"),
            availability=getattr(
                demand_pb2,
                f"CAPABILITY_AVAILABILITY_{self.availability.value}",
            ),
            rest_history=self.rest_history,
            live=self.live,
            snapshot=self.snapshot,
            delta=self.delta,
            sequence=self.sequence,
            checksum=self.checksum,
            resubscribe=self.resubscribe,
            resnapshot_on_gap=self.resnapshot_on_gap,
            native_intervals=list(self.native_intervals),
            constraint=self.constraint or "",
        )


@dataclass(frozen=True, slots=True)
class ResolvedRequirement:
    requirement: DataRequirement
    instrument_uid: str
    instrument_id: str
    native_symbol: str
    capability: FeedCapability
    binding_id: str | None
    state: DemandState
    provisioned: bool
    catalog_revision: int
    demand_revision: int
    consumer_ids: tuple[str, ...] = ()
    effective_priority: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument_uid", _text(self.instrument_uid, "instrument_uid"))
        object.__setattr__(self, "instrument_id", _text(self.instrument_id, "instrument_id").upper())
        object.__setattr__(self, "native_symbol", _text(self.native_symbol, "native_symbol").upper())
        object.__setattr__(self, "binding_id", UniverseSelector._optional(self.binding_id))
        object.__setattr__(self, "state", _enum(self.state, DemandState, "state"))
        owners = tuple(sorted({_text(value, "consumer_id") for value in self.consumer_ids}))
        object.__setattr__(self, "consumer_ids", owners or (self.requirement.consumer_id,))
        priority = self.requirement.priority if self.effective_priority is None else self.effective_priority
        if not 0 <= priority <= 1_000:
            raise ValueError("effective_priority is outside bounds")
        object.__setattr__(self, "effective_priority", priority)
        if self.catalog_revision < 1 or self.demand_revision < 1:
            raise ValueError("catalog and demand revisions must be positive")
        if self.capability.feed is not self.requirement.feed:
            raise ValueError("resolved capability feed differs from requirement")
        if self.provisioned != bool(self.binding_id):
            raise ValueError("provisioned state must match binding_id")
        if self.state is DemandState.UNSUPPORTED and self.capability.enabled:
            raise ValueError("unsupported state requires unavailable capability")
        if self.state is not DemandState.UNSUPPORTED and not self.capability.enabled:
            raise ValueError("unavailable capability must resolve as unsupported")

    @property
    def requirement_id(self) -> str:
        """Resolved identity is per instrument, never just the selector request.

        A selector can expand to hundreds of instruments. Reusing the source
        requirement hash for all of them would collapse independent TTL leases
        and silently drop all but one demanded slice.
        """
        interval = self.requirement.interval or ""
        return hashlib.sha256(
            (
                f"qdl-resolved-demand-v1\0{self.instrument_uid}\0"
                f"{self.requirement.feed.value}\0{interval}\0"
                f"{self.requirement.source_policy_id}"
            ).encode()
        ).hexdigest()

    @property
    def resolution_key(self) -> tuple[str, DemandFeed, str | None, str]:
        return (
            self.instrument_uid,
            self.requirement.feed,
            self.requirement.interval,
            self.requirement.source_policy_id,
        )

    def to_proto(self) -> demand_pb2.ResolvedRequirement:
        return demand_pb2.ResolvedRequirement(
            requirement_id=self.requirement_id,
            requirement=self.requirement.to_proto(),
            instrument_uid=self.instrument_uid,
            instrument_id=self.instrument_id,
            native_symbol=self.native_symbol,
            binding_id=self.binding_id or "",
            capability=self.capability.to_proto(),
            state=getattr(demand_pb2, f"DEMAND_STATE_{self.state.value}"),
            provisioned=self.provisioned,
            catalog_revision=self.catalog_revision,
            demand_revision=self.demand_revision,
            consumer_ids=list(self.consumer_ids),
            effective_priority=self.effective_priority,
        )


@dataclass(frozen=True, slots=True)
class DemandLease:
    lease_id: str
    owner_id: str
    requirement_id: str
    demand_revision: int
    renewed_at_ns: int
    expires_at_ns: int
    state: DemandState

    def __post_init__(self) -> None:
        object.__setattr__(self, "lease_id", _text(self.lease_id, "lease_id"))
        object.__setattr__(self, "owner_id", _text(self.owner_id, "owner_id"))
        object.__setattr__(self, "requirement_id", _text(self.requirement_id, "requirement_id"))
        object.__setattr__(self, "state", _enum(self.state, DemandState, "state"))
        if self.demand_revision < 1:
            raise ValueError("demand_revision must be positive")
        if self.renewed_at_ns <= 0 or self.expires_at_ns <= self.renewed_at_ns:
            raise ValueError("demand lease timestamps are invalid")

    def to_proto(self) -> demand_pb2.DemandLease:
        return demand_pb2.DemandLease(
            lease_id=self.lease_id,
            owner_id=self.owner_id,
            requirement_id=self.requirement_id,
            demand_revision=self.demand_revision,
            renewed_at_ns=self.renewed_at_ns,
            expires_at_ns=self.expires_at_ns,
            state=getattr(demand_pb2, f"DEMAND_STATE_{self.state.value}"),
        )


@dataclass(frozen=True, slots=True)
class DemandTransition:
    requirement_id: str
    previous: DemandState
    current: DemandState
    reason: str
    changed_at_ns: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "requirement_id", _text(self.requirement_id, "requirement_id"))
        object.__setattr__(self, "previous", _enum(self.previous, DemandState, "previous"))
        object.__setattr__(self, "current", _enum(self.current, DemandState, "current"))
        object.__setattr__(self, "reason", _text(self.reason, "reason"))
        if self.changed_at_ns <= 0:
            raise ValueError("changed_at_ns must be positive")

    def to_proto(self) -> demand_pb2.DemandTransition:
        return demand_pb2.DemandTransition(
            requirement_id=self.requirement_id,
            previous=getattr(demand_pb2, f"DEMAND_STATE_{self.previous.value}"),
            current=getattr(demand_pb2, f"DEMAND_STATE_{self.current.value}"),
            reason=self.reason,
            changed_at_ns=self.changed_at_ns,
        )
