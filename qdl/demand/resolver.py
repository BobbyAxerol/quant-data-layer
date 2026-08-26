from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Iterable, Mapping

import yaml

from qdl.demand.contracts import (
    CapabilityAvailability,
    DataRequirement,
    DemandFeed,
    DemandLease,
    DemandPurpose,
    DemandState,
    DemandTransition,
    FeedCapability,
    demand_transition_allowed,
    ResolvedRequirement,
    UniverseSelector,
    UniverseSelectorKind,
)
from qdl.domain.capabilities import (
    CapabilityAvailability as DomainCapabilityAvailability,
    FeedCapability as DomainFeedCapability,
    VenueCapabilityProfile,
    binance_spot_capabilities,
    binance_usdm_capabilities,
    dnse_capabilities,
    okx_global_capabilities,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.warmup.contracts import (
    IntervalSourcePolicy,
    WarmupSpecification,
    WarmupTimeRange,
)


_UNIVERSE_SCHEMA = "qdl.v2.universe-registry.v1"
_DEMAND_SCHEMA = "qdl.v2.universal-demand.v1"


@dataclass(frozen=True, slots=True)
class UniverseMember:
    native_symbol: str
    segments: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        symbol = str(self.native_symbol or "").strip().upper()
        if not symbol:
            raise ValueError("universe native_symbol is required")
        object.__setattr__(self, "native_symbol", symbol)
        object.__setattr__(
            self,
            "segments",
            tuple(sorted({str(item).strip() for item in self.segments if str(item).strip()})),
        )


@dataclass(frozen=True, slots=True)
class UniverseDefinition:
    universe_id: str
    venue: str
    market: str
    product_type: str
    members: tuple[UniverseMember, ...]

    def __post_init__(self) -> None:
        values = {
            "universe_id": str(self.universe_id or "").strip(),
            "venue": str(self.venue or "").strip().upper(),
            "market": str(self.market or "").strip().upper(),
            "product_type": str(self.product_type or "").strip().upper(),
        }
        if any(not value for value in values.values()):
            raise ValueError("universe identity is incomplete")
        object.__setattr__(self, "universe_id", values["universe_id"])
        object.__setattr__(self, "venue", values["venue"])
        object.__setattr__(self, "market", values["market"])
        object.__setattr__(self, "product_type", values["product_type"])
        members = tuple(sorted(self.members, key=lambda item: item.native_symbol))
        if not members or len({item.native_symbol for item in members}) != len(members):
            raise ValueError("universe members must be non-empty and unique")
        object.__setattr__(self, "members", members)

    def canonical_mapping(self) -> dict[str, Any]:
        return {
            "universe_id": self.universe_id,
            "venue": self.venue,
            "market": self.market,
            "product_type": self.product_type,
            "symbols": [
                {"native_symbol": item.native_symbol, "segments": list(item.segments)}
                for item in self.members
            ],
        }


class UniverseRegistry:
    def __init__(self, *, revision: int, universes: Iterable[UniverseDefinition], source_path: str) -> None:
        if revision < 1:
            raise ValueError("universe registry revision must be positive")
        rows = tuple(sorted(universes, key=lambda item: item.universe_id))
        if not rows or len({item.universe_id for item in rows}) != len(rows):
            raise ValueError("universe registry needs unique non-empty universes")
        self.revision = revision
        self.universes = rows
        self.source_path = source_path
        self._by_id = {item.universe_id: item for item in rows}
        encoded = json.dumps(
            {
                "schema": _UNIVERSE_SCHEMA,
                "revision": revision,
                "universes": [item.canonical_mapping() for item in rows],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        self.sha256 = hashlib.sha256(encoded).hexdigest()

    @classmethod
    def load(cls, path: str | Path) -> "UniverseRegistry":
        source = Path(path).resolve()
        raw = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or set(raw) != {"schema", "revision", "universes"}:
            raise ValueError("universe registry fields are incomplete or unknown")
        if raw["schema"] != _UNIVERSE_SCHEMA:
            raise ValueError("unsupported universe registry schema")
        values = raw["universes"]
        if not isinstance(values, list) or not 1 <= len(values) <= 10_000:
            raise ValueError("universe registry must contain 1..10000 universes")
        universes: list[UniverseDefinition] = []
        for raw_universe in values:
            expected = {"universe_id", "venue", "market", "product_type", "symbols"}
            if not isinstance(raw_universe, dict) or set(raw_universe) != expected:
                raise ValueError("universe definition fields are incomplete or unknown")
            symbols = raw_universe["symbols"]
            if not isinstance(symbols, list) or not symbols:
                raise ValueError("universe symbols are required")
            members: list[UniverseMember] = []
            for raw_symbol in symbols:
                if not isinstance(raw_symbol, dict) or set(raw_symbol) != {"native_symbol", "segments"}:
                    raise ValueError("universe symbol fields are incomplete or unknown")
                segments = raw_symbol["segments"]
                if not isinstance(segments, list):
                    raise ValueError("universe symbol segments must be a list")
                members.append(
                    UniverseMember(
                        native_symbol=str(raw_symbol["native_symbol"]),
                        segments=tuple(str(value) for value in segments),
                    )
                )
            universes.append(
                UniverseDefinition(
                    universe_id=str(raw_universe["universe_id"]),
                    venue=str(raw_universe["venue"]),
                    market=str(raw_universe["market"]),
                    product_type=str(raw_universe["product_type"]),
                    members=tuple(members),
                )
            )
        return cls(
            revision=int(raw["revision"]),
            universes=tuple(universes),
            source_path=str(source),
        )

    def resolve(self, selector: UniverseSelector) -> tuple[str, ...]:
        if selector.kind is UniverseSelectorKind.EXPLICIT:
            return selector.native_symbols
        if selector.kind is UniverseSelectorKind.CONTINUOUS:
            raise RuntimeError("continuous contract resolution requires the dedicated derivatives resolver")
        try:
            universe = self._by_id[selector.universe_ref or ""]
        except KeyError as error:
            raise KeyError(f"unknown universe_ref: {selector.universe_ref}") from error
        if (
            universe.venue != selector.venue
            or universe.market != selector.market
            or universe.product_type != selector.product_type
        ):
            raise ValueError("selector identity differs from referenced universe")
        if (
            selector.expected_universe_sha256 is not None
            and selector.expected_universe_sha256 != self.sha256
        ):
            raise ValueError("universe registry digest differs from selector expectation")
        if selector.kind is UniverseSelectorKind.UNIVERSE_REF:
            return tuple(item.native_symbol for item in universe.members)
        selected = tuple(
            item.native_symbol
            for item in universe.members
            if selector.segment_id in item.segments
        )
        if not selected:
            raise ValueError("selector segment resolves to an empty universe")
        return selected


@dataclass(frozen=True, slots=True)
class DemandManifest:
    revision: int
    requirements: tuple[DataRequirement, ...]
    source_paths: tuple[str, ...]
    sha256: str

    @classmethod
    def load_many(cls, paths: Iterable[str | Path]) -> "DemandManifest":
        requirements: list[DataRequirement] = []
        revisions: list[int] = []
        normalized_paths: list[str] = []
        digest = hashlib.sha256()
        for raw_path in paths:
            path = Path(raw_path).resolve()
            encoded = path.read_bytes()
            digest.update(path.name.encode())
            digest.update(b"\0")
            digest.update(encoded)
            raw = yaml.safe_load(encoded)
            if not isinstance(raw, dict) or set(raw) != {"schema", "revision", "requirements"}:
                raise ValueError("universal demand manifest fields are incomplete or unknown")
            if raw["schema"] != _DEMAND_SCHEMA or int(raw["revision"]) < 1:
                raise ValueError("universal demand manifest schema/revision is invalid")
            rows = raw["requirements"]
            if not isinstance(rows, list) or not 1 <= len(rows) <= 100_000:
                raise ValueError("universal demand manifest needs 1..100000 requirements")
            requirements.extend(DataRequirement.from_mapping(item) for item in rows)
            revisions.append(int(raw["revision"]))
            normalized_paths.append(str(path))
        if not requirements:
            raise ValueError("universal demand manifest cannot be empty")
        return cls(
            revision=max(revisions),
            requirements=tuple(requirements),
            source_paths=tuple(sorted(normalized_paths)),
            sha256=digest.hexdigest(),
        )


class CapabilityRegistry:
    """Converts provider capability profiles into one demand-contract view."""

    _FEED_MAP = {
        DemandFeed.TRADE: "trade",
        DemandFeed.QUOTE: "bbo",
        DemandFeed.BAR: "bar",
        DemandFeed.BOOK_SNAPSHOT: "l2",
        DemandFeed.BOOK_DELTA: "l2",
        DemandFeed.FUNDING_RATE: "funding_rate",
        DemandFeed.OPEN_INTEREST: "open_interest",
        DemandFeed.LONG_SHORT_RATIO: "long_short_ratio",
        DemandFeed.TAKER_FLOW: "taker_flow",
        DemandFeed.BASIS: "basis",
        DemandFeed.MARK_PRICE: "mark_index_price",
        DemandFeed.INDEX_PRICE: "mark_index_price",
    }

    def __init__(self, profiles: Mapping[tuple[str, str], VenueCapabilityProfile]) -> None:
        self._profiles = {
            (venue.upper(), market.upper()): profile
            for (venue, market), profile in profiles.items()
        }

    @classmethod
    def defaults(cls) -> "CapabilityRegistry":
        profiles: dict[tuple[str, str], VenueCapabilityProfile] = {
            ("BINANCE", "USDM"): binance_usdm_capabilities(),
            ("BINANCE", "SPOT"): binance_spot_capabilities(),
            ("OKX", "SWAP"): okx_global_capabilities("SWAP"),
            ("OKX", "SPOT"): okx_global_capabilities("SPOT"),
        }
        vn = dnse_capabilities()
        profiles[("HNX", "VN_DERIVATIVES")] = vn
        profiles[("HOSE", "EQUITIES")] = vn
        return cls(profiles)

    def resolve(
        self,
        *,
        venue: str,
        market: str,
        product_type: str,
        feed: DemandFeed,
    ) -> FeedCapability:
        venue_value = venue.upper()
        market_value = market.upper()
        profile = self._profiles.get((venue_value, market_value))
        key = self._FEED_MAP.get(feed)
        if profile is None or key is None:
            return FeedCapability(
                capability_id=f"{venue_value}:{market_value}:{product_type}:{feed.value}",
                venue=venue_value,
                market=market_value,
                product_type=product_type,
                feed=feed,
                availability=CapabilityAvailability.UNAVAILABLE,
                constraint="feed is not implemented in the current V2 capability registry",
            )
        try:
            capability = profile.capability(key)
        except KeyError:
            capability = DomainFeedCapability(DomainCapabilityAvailability.UNAVAILABLE)
        return FeedCapability(
            capability_id=f"{profile.provider}:{venue_value}:{market_value}:{product_type}:{feed.value}",
            venue=venue_value,
            market=market_value,
            product_type=product_type,
            feed=feed,
            availability=CapabilityAvailability(capability.availability.value),
            rest_history=capability.rest_history,
            live=capability.live,
            snapshot=capability.snapshot,
            delta=capability.delta,
            sequence=capability.sequence,
            checksum=capability.checksum,
            resubscribe=capability.resubscribe,
            resnapshot_on_gap=capability.resnapshot_on_gap,
            native_intervals=capability.native_intervals,
            constraint=capability.constraint,
        )


class DemandResolver:
    """Resolves a universal manifest against catalog identity and capabilities."""

    def __init__(
        self,
        *,
        catalog: StableSourceCatalog,
        universes: UniverseRegistry,
        capabilities: CapabilityRegistry | None = None,
    ) -> None:
        self.catalog = catalog
        self.universes = universes
        self.capabilities = capabilities or CapabilityRegistry.defaults()
        self._by_native = {
            (
                item.identity.venue,
                item.identity.market,
                item.identity.product_type.value,
                item.native_symbol,
            ): item
            for item in catalog.instruments
        }
        self._bindings = {
            (
                item.instrument.instrument_uid,
                item.feed.value,
                item.interval,
                item.source_policy_id,
            ): item
            for item in catalog.bindings
        }

    def resolve_requirement(
        self,
        requirement: DataRequirement,
        *,
        demand_revision: int,
    ) -> tuple[ResolvedRequirement, ...]:
        if demand_revision < 1:
            raise ValueError("demand_revision must be positive")
        symbols = self.universes.resolve(requirement.universe)
        resolved: list[ResolvedRequirement] = []
        for native_symbol in symbols:
            key = (
                requirement.universe.venue,
                requirement.universe.market,
                requirement.universe.product_type,
                native_symbol,
            )
            try:
                instrument = self._by_native[key]
            except KeyError as error:
                raise KeyError(f"catalog is missing selected instrument: {key}") from error
            capability = self.capabilities.resolve(
                venue=instrument.identity.venue,
                market=instrument.identity.market,
                product_type=instrument.identity.product_type.value,
                feed=requirement.feed,
            )
            binding = self._bindings.get(
                (
                    instrument.instrument_uid,
                    requirement.feed.value,
                    requirement.interval,
                    requirement.source_policy_id,
                )
            )
            state = (
                DemandState.REQUESTED
                if capability.enabled
                else DemandState.UNSUPPORTED
            )
            resolved.append(
                ResolvedRequirement(
                    requirement=requirement,
                    instrument_uid=instrument.instrument_uid,
                    instrument_id=instrument.instrument_id,
                    native_symbol=instrument.native_symbol,
                    capability=capability,
                    binding_id=binding.binding_id if binding is not None else None,
                    state=state,
                    provisioned=binding is not None,
                    catalog_revision=self.catalog.catalog_revision,
                    demand_revision=demand_revision,
                )
            )
        return tuple(sorted(resolved, key=lambda item: item.resolution_key))

    def resolve_manifest(self, manifest: DemandManifest) -> tuple[ResolvedRequirement, ...]:
        rows: list[ResolvedRequirement] = []
        for requirement in manifest.requirements:
            rows.extend(self.resolve_requirement(requirement, demand_revision=manifest.revision))
        return self._merge(rows)

    @staticmethod
    def _merge(rows: Iterable[ResolvedRequirement]) -> tuple[ResolvedRequirement, ...]:
        grouped: dict[tuple[str, DemandFeed, str | None, str], list[ResolvedRequirement]] = {}
        for row in rows:
            grouped.setdefault(row.resolution_key, []).append(row)
        merged: list[ResolvedRequirement] = []
        for key, values in sorted(grouped.items(), key=lambda item: item[0]):
            values = sorted(values, key=lambda item: (item.requirement.priority, item.requirement.consumer_id))
            first = values[0]
            if len({item.requirement.source_policy_id for item in values}) != 1:
                raise ValueError(f"conflicting source policies for resolved demand: {key}")
            if len({item.capability.capability_id for item in values}) != 1:
                raise ValueError(f"conflicting capabilities for resolved demand: {key}")
            purposes = {item.requirement.purpose for item in values}
            purpose = (
                DemandPurpose.EXECUTION
                if DemandPurpose.EXECUTION in purposes
                else DemandPurpose.ALPHA
                if DemandPurpose.ALPHA in purposes
                else DemandPurpose.RESEARCH
                if DemandPurpose.RESEARCH in purposes
                else DemandPurpose.OBSERVABILITY
            )
            freshness = [
                item.requirement.max_freshness_ms
                for item in values
                if item.requirement.max_freshness_ms is not None
            ]
            explicit_warmup = any(
                item.requirement.warmup is not None for item in values
            )
            merged_warmup = (
                DemandResolver._merge_warmup(values) if explicit_warmup else None
            )
            effective = replace(
                first.requirement,
                consumer_id=first.requirement.consumer_id,
                purpose=purpose,
                warmup_limit=(
                    merged_warmup.rows
                    if merged_warmup is not None and merged_warmup.rows is not None
                    else 0
                    if merged_warmup is not None
                    else max(item.requirement.warmup_limit for item in values)
                ),
                warmup=merged_warmup,
                max_freshness_ms=min(freshness) if freshness else None,
                priority=min(item.requirement.priority for item in values),
                ttl_seconds=max(item.requirement.ttl_seconds for item in values),
                require_final_bars=any(item.requirement.require_final_bars for item in values),
                require_live=any(item.requirement.require_live for item in values),
                execution_grade=purpose is DemandPurpose.EXECUTION,
                depth_levels=max(item.requirement.depth_levels for item in values),
            )
            binding_ids = {item.binding_id for item in values if item.binding_id}
            if len(binding_ids) > 1:
                raise ValueError(f"conflicting stable bindings for resolved demand: {key}")
            states = {item.state for item in values}
            state = DemandState.UNSUPPORTED if DemandState.UNSUPPORTED in states else DemandState.REQUESTED
            merged.append(
                ResolvedRequirement(
                    requirement=effective,
                    instrument_uid=first.instrument_uid,
                    instrument_id=first.instrument_id,
                    native_symbol=first.native_symbol,
                    capability=first.capability,
                    binding_id=next(iter(binding_ids), None),
                    state=state,
                    provisioned=bool(binding_ids),
                    catalog_revision=first.catalog_revision,
                    demand_revision=max(item.demand_revision for item in values),
                    consumer_ids=tuple(
                        sorted({consumer for item in values for consumer in item.consumer_ids})
                    ),
                    effective_priority=effective.priority,
                )
            )
        return tuple(merged)

    @staticmethod
    def _merge_warmup(values: list[ResolvedRequirement]) -> WarmupSpecification:
        specifications = tuple(
            item.requirement.warmup_specification
            for item in values
            if item.requirement.warmup_specification is not None
        )
        if not specifications:
            raise ValueError("explicit warmup merge has no warmup specification")
        row_horizons = tuple(item for item in specifications if item.rows is not None)
        range_horizons = tuple(
            item for item in specifications if item.time_range is not None
        )
        if row_horizons and range_horizons:
            raise ValueError(
                "resolved demand cannot merge row and time-range warmup horizons"
            )
        source_policy = (
            IntervalSourcePolicy.NATIVE_ONLY
            if any(
                item.interval_source_policy is IntervalSourcePolicy.NATIVE_ONLY
                for item in specifications
            )
            else IntervalSourcePolicy.NATIVE_OR_EXACT_RESAMPLE
        )
        common = {
            "interval_source_policy": source_policy,
            "max_cache_age_ms": min(item.max_cache_age_ms for item in specifications),
            "deadline_ms": min(item.deadline_ms for item in specifications),
        }
        if row_horizons:
            return WarmupSpecification(
                rows=max(int(item.rows or 0) for item in row_horizons),
                **common,
            )
        ranges = tuple(item.time_range for item in range_horizons)
        assert all(item is not None for item in ranges)
        return WarmupSpecification(
            time_range=WarmupTimeRange(
                min(item.start_time_ns for item in ranges if item is not None),
                max(item.end_time_ns for item in ranges if item is not None),
            ),
            **common,
        )


class DemandLeaseRegistry:
    """In-memory lifecycle reference implementation for one topology revision.

    Runtime persistence/leader fencing stays in the existing control-plane store.
    This registry deliberately has no Redis or Kafka dependency so it can be used
    in config compilation and deterministic tests without changing live state.
    """

    def __init__(self, *, clock_ns=time.time_ns) -> None:
        self._clock_ns = clock_ns
        self._leases: dict[tuple[str, str], DemandLease] = {}
        self._requirements: dict[str, ResolvedRequirement] = {}
        self._states: dict[str, DemandState] = {}
        self._transitions: list[DemandTransition] = []

    @staticmethod
    def _lease_id(owner_id: str, requirement_id: str, revision: int) -> str:
        return hashlib.sha256(
            f"qdl-demand-lease-v1\0{owner_id}\0{requirement_id}\0{revision}".encode()
        ).hexdigest()

    def renew(
        self,
        owner_id: str,
        requirements: Iterable[ResolvedRequirement],
        *,
        ttl_seconds: int | None = None,
        now_ns: int | None = None,
    ) -> tuple[DemandLease, ...]:
        owner = str(owner_id or "").strip()
        if not owner:
            raise ValueError("owner_id is required")
        now = self._clock_ns() if now_ns is None else now_ns
        if now <= 0:
            raise ValueError("now_ns must be positive")
        values = tuple(requirements)
        requested = {item.requirement_id: item for item in values}
        for key in [key for key in self._leases if key[0] == owner and key[1] not in requested]:
            del self._leases[key]
            self._expire_if_unowned(key[1], now, "owner_released_requirement")
        leases: list[DemandLease] = []
        for requirement_id, item in sorted(requested.items()):
            ttl = item.requirement.ttl_seconds if ttl_seconds is None else ttl_seconds
            if not 30 <= ttl <= 3_600:
                raise ValueError("ttl_seconds must be between 30 and 3600")
            previous = self._states.get(requirement_id)
            state = item.state if previous is None else previous
            if previous is DemandState.EXPIRED and item.state is not DemandState.EXPIRED:
                if not demand_transition_allowed(previous, item.state):
                    raise ValueError("expired demand cannot be renewed to this state")
                state = item.state
                self._states[requirement_id] = state
                self._transitions.append(
                    DemandTransition(
                        requirement_id=requirement_id,
                        previous=previous,
                        current=state,
                        reason="lease_renewed_after_expiry",
                        changed_at_ns=now,
                    )
                )
            lease = DemandLease(
                lease_id=self._lease_id(owner, requirement_id, item.demand_revision),
                owner_id=owner,
                requirement_id=requirement_id,
                demand_revision=item.demand_revision,
                renewed_at_ns=now,
                expires_at_ns=now + ttl * 1_000_000_000,
                state=state,
            )
            self._leases[(owner, requirement_id)] = lease
            self._requirements[requirement_id] = item
            self._states.setdefault(requirement_id, state)
            leases.append(lease)
        return tuple(leases)

    def release(self, owner_id: str, *, now_ns: int | None = None) -> int:
        owner = str(owner_id or "").strip()
        now = self._clock_ns() if now_ns is None else now_ns
        keys = [key for key in self._leases if key[0] == owner]
        for key in keys:
            del self._leases[key]
            self._expire_if_unowned(key[1], now, "owner_released")
        return len(keys)

    def expire(self, *, now_ns: int | None = None) -> tuple[DemandLease, ...]:
        now = self._clock_ns() if now_ns is None else now_ns
        expired = [lease for lease in self._leases.values() if lease.expires_at_ns <= now]
        for lease in expired:
            self._leases.pop((lease.owner_id, lease.requirement_id), None)
            self._expire_if_unowned(lease.requirement_id, now, "lease_ttl_expired")
        return tuple(sorted(expired, key=lambda item: (item.owner_id, item.requirement_id)))

    def transition(
        self,
        requirement_id: str,
        state: DemandState,
        *,
        reason: str,
        now_ns: int | None = None,
    ) -> DemandTransition:
        if requirement_id not in self._requirements:
            raise KeyError("cannot transition an unknown demand requirement")
        current = DemandState(state)
        previous = self._states.get(requirement_id, DemandState.REQUESTED)
        now = self._clock_ns() if now_ns is None else now_ns
        if not demand_transition_allowed(previous, current):
            raise ValueError(
                f"invalid demand transition: {previous.value} -> {current.value}"
            )
        transition = DemandTransition(
            requirement_id=requirement_id,
            previous=previous,
            current=current,
            reason=reason,
            changed_at_ns=now,
        )
        self._states[requirement_id] = current
        self._transitions.append(transition)
        return transition

    def desired(self, *, now_ns: int | None = None) -> tuple[ResolvedRequirement, ...]:
        self.expire(now_ns=now_ns)
        active_ids = {lease.requirement_id for lease in self._leases.values()}
        result = []
        for requirement_id in sorted(active_ids):
            original = self._requirements[requirement_id]
            result.append(replace(original, state=self._states[requirement_id]))
        return tuple(result)

    def transitions(self) -> tuple[DemandTransition, ...]:
        return tuple(self._transitions)

    def _expire_if_unowned(self, requirement_id: str, now_ns: int, reason: str) -> None:
        if any(lease.requirement_id == requirement_id for lease in self._leases.values()):
            return
        if requirement_id not in self._requirements:
            return
        previous = self._states.get(requirement_id, DemandState.REQUESTED)
        if previous is DemandState.EXPIRED:
            return
        self._states[requirement_id] = DemandState.EXPIRED
        self._transitions.append(
            DemandTransition(
                requirement_id=requirement_id,
                previous=previous,
                current=DemandState.EXPIRED,
                reason=reason,
                changed_at_ns=now_ns,
            )
        )
