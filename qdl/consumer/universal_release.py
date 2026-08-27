"""Checksum-bound Phase 11 universal V2 release planning.

The existing Phase 10.5 release manifest intentionally covers a small stable
consumer set.  This module materializes a separate, deterministic manifest
from the admitted Phase 11 active-demand inventory.  It is control-plane code:
it does not open a provider connection, mutate a route, issue an order, or
create a runtime role.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

from qdl.demand import (
    ActiveDemandConvergence,
    ActiveDemandInventory,
    DemandFeed,
    ProviderAdmission,
    source_requirement_for_admission,
)
from qdl.demand.inventory import ProviderAdmissionRow
from qdl.runtime.l2_demand import L2DemandPlan
from qdl.runtime.universal_realtime import UniversalRealtimePlan


_SCHEMA = "qdl.v2.universal-release-manifest.v1"
_POLICY_SCHEMA = "qdl.v2.universal-release-policy.v1"
_COVERAGE_SCHEMA = "qdl.v2.universal-release-coverage.v1"
_V2_VENUES = frozenset({"BINANCE", "OKX"})
_REALTIME_FEEDS = frozenset({DemandFeed.TRADE, DemandFeed.QUOTE, DemandFeed.BAR})
_BOOK_FEEDS = frozenset({DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA})


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(value: object) -> str:
    return _sha256_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def _sha256(value: object, field: str) -> str:
    result = str(value or "").strip().lower()
    if len(result) != 64 or any(character not in "0123456789abcdef" for character in result):
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return result


def _text(value: object, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{field} is required")
    return result


def _relative_path(root: Path, value: object, field: str) -> Path:
    candidate = Path(_text(value, field))
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"{field} must be a safe relative path")
    resolved_root = root.resolve()
    resolved = (resolved_root / candidate).resolve()
    if resolved_root not in resolved.parents and resolved != resolved_root:
        raise ValueError(f"{field} escapes manifest root")
    return resolved


class UniversalConsumerClass(StrEnum):
    MONITORING = "MONITORING"
    TRADING_SYSTEM = "TRADING_SYSTEM"
    SINGLE_SYMBOL_ALPHA = "SINGLE_SYMBOL_ALPHA"
    PORTFOLIO_MULTI_SYMBOL = "PORTFOLIO_MULTI_SYMBOL"
    GRID_REACTIVE_BRACKET = "GRID_REACTIVE_BRACKET"
    BASIS_ARB = "BASIS_ARB"


@dataclass(frozen=True, slots=True)
class ConsumerClassRule:
    consumer_class: UniversalConsumerClass
    consumer_prefixes: tuple[str, ...]

    def __post_init__(self) -> None:
        prefixes = tuple(sorted({_text(value, "consumer prefix") for value in self.consumer_prefixes}))
        if not prefixes:
            raise ValueError("consumer class needs at least one prefix")
        object.__setattr__(self, "consumer_prefixes", prefixes)


@dataclass(frozen=True, slots=True)
class FallbackRule:
    rule_id: str
    venue: str
    market: str
    product_type: str
    feed: DemandFeed
    interval: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "rule_id", _text(self.rule_id, "fallback rule_id"))
        for field in ("venue", "market", "product_type"):
            object.__setattr__(self, field, _text(getattr(self, field), field).upper())
        object.__setattr__(self, "feed", DemandFeed(self.feed))
        interval = str(self.interval).strip() if self.interval is not None else None
        object.__setattr__(self, "interval", interval or None)

    def matches(self, row: ProviderAdmissionRow) -> bool:
        return (
            self.venue == row.venue
            and self.market == row.market
            and self.product_type == row.product_type
            and self.feed.value == row.feed
            and self.interval == row.interval
        )

    def canonical_mapping(self) -> dict[str, object]:
        return {
            "rule_id": self.rule_id,
            "venue": self.venue,
            "market": self.market,
            "product_type": self.product_type,
            "feed": self.feed.value,
            "interval": self.interval,
        }


@dataclass(frozen=True, slots=True)
class UniversalV1Rollback:
    release_tag: str
    source_commit: str
    image_reference: str
    manifest_revision: str

    def __post_init__(self) -> None:
        for field in ("release_tag", "source_commit", "image_reference", "manifest_revision"):
            object.__setattr__(self, field, _text(getattr(self, field), f"v1 rollback {field}"))

    def canonical_mapping(self) -> dict[str, str]:
        return {
            "release_tag": self.release_tag,
            "source_commit": self.source_commit,
            "image_reference": self.image_reference,
            "manifest_revision": self.manifest_revision,
        }


@dataclass(frozen=True, slots=True)
class UniversalResourceBudget:
    max_consumer_lag: int
    max_cpu_millicores: int
    max_rss_bytes: int

    def __post_init__(self) -> None:
        if (
            not 1 <= self.max_consumer_lag <= 1_000_000
            or not 1 <= self.max_cpu_millicores <= 100_000
            or not 1 <= self.max_rss_bytes <= 2**63 - 1
        ):
            raise ValueError("universal release resource budget is outside bounds")

    def canonical_mapping(self) -> dict[str, int]:
        return {
            "max_consumer_lag": self.max_consumer_lag,
            "max_cpu_millicores": self.max_cpu_millicores,
            "max_rss_bytes": self.max_rss_bytes,
        }


@dataclass(frozen=True, slots=True)
class UniversalReleasePolicy:
    revision: int
    contract_version: str
    capability_matrix_path: Path
    capability_matrix_sha256: str
    capability_matrix_revision: int
    v1_rollback: UniversalV1Rollback
    resource_budget: UniversalResourceBudget
    allowed_non_admitted_states: frozenset[str]
    consumer_classes: tuple[ConsumerClassRule, ...]
    fallback_rules: tuple[FallbackRule, ...]
    policy_sha256: str

    @classmethod
    def load(cls, path: str | Path, *, manifest_root: str | Path) -> "UniversalReleasePolicy":
        root = Path(manifest_root).resolve()
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        expected = {
            "schema", "revision", "contract_version", "capability_matrix", "v1_rollback",
            "resource_budget", "allowed_non_admitted_states", "consumer_classes", "fallback_rules",
        }
        if not isinstance(raw, dict) or set(raw) != expected or raw.get("schema") != _POLICY_SCHEMA:
            raise ValueError("universal release policy schema or fields are invalid")
        revision = int(raw["revision"])
        if revision < 1 or str(raw["contract_version"]) != "2.0.0":
            raise ValueError("universal release policy revision or contract version is invalid")
        matrix = raw["capability_matrix"]
        if not isinstance(matrix, dict) or set(matrix) != {"path", "sha256", "revision"}:
            raise ValueError("universal release capability matrix reference is invalid")
        matrix_path = _relative_path(root, matrix["path"], "capability_matrix.path")
        matrix_sha256 = _sha256(matrix["sha256"], "capability_matrix.sha256")
        if not matrix_path.is_file() or _sha256_bytes(matrix_path.read_bytes()) != matrix_sha256:
            raise ValueError("universal release capability matrix checksum differs")
        matrix_raw = yaml.safe_load(matrix_path.read_text(encoding="utf-8"))
        if (
            not isinstance(matrix_raw, dict)
            or int(matrix["revision"]) < 1
            or int(matrix_raw.get("revision", 0)) != int(matrix["revision"])
            or matrix_raw.get("public_contract_version") != "2.0.0"
            or matrix_raw.get("realtime_core_target") != "RUST"
            or matrix_raw.get("equal_source_contract") is not True
        ):
            raise ValueError("universal release capability matrix is not V2 Rust-core compatible")
        v1 = raw["v1_rollback"]
        budget = raw["resource_budget"]
        if not isinstance(v1, dict) or set(v1) != {
            "release_tag", "source_commit", "image_reference", "manifest_revision"
        }:
            raise ValueError("universal release V1 rollback reference is invalid")
        if not isinstance(budget, dict) or set(budget) != {
            "max_consumer_lag", "max_cpu_millicores", "max_rss_bytes"
        }:
            raise ValueError("universal release resource budget fields are invalid")
        allowed = raw["allowed_non_admitted_states"]
        if not isinstance(allowed, list) or not allowed:
            raise ValueError("universal release allowed non-admitted states are required")
        allowed_states = frozenset(_text(value, "allowed_non_admitted_state") for value in allowed)
        classes_raw = raw["consumer_classes"]
        if not isinstance(classes_raw, list) or not classes_raw:
            raise ValueError("universal release consumer classes are required")
        classes = []
        for item in classes_raw:
            if not isinstance(item, dict) or set(item) != {"class_id", "consumer_prefixes"}:
                raise ValueError("universal release consumer class fields are invalid")
            prefixes = item["consumer_prefixes"]
            if not isinstance(prefixes, list):
                raise ValueError("universal release consumer prefixes must be a list")
            classes.append(ConsumerClassRule(
                UniversalConsumerClass(str(item["class_id"])), tuple(str(value) for value in prefixes)
            ))
        if len({item.consumer_class for item in classes}) != len(classes):
            raise ValueError("universal release consumer classes must be unique")
        prefixes = [prefix for item in classes for prefix in item.consumer_prefixes]
        if len(prefixes) != len(set(prefixes)) or any(
            left != right and (left.startswith(right) or right.startswith(left))
            for index, left in enumerate(prefixes)
            for right in prefixes[index + 1 :]
        ):
            raise ValueError("universal release consumer prefixes overlap")
        fallback_raw = raw["fallback_rules"]
        if not isinstance(fallback_raw, list):
            raise ValueError("universal release fallback rules must be a list")
        fallback = []
        for item in fallback_raw:
            if not isinstance(item, dict) or set(item) != {
                "rule_id", "venue", "market", "product_type", "feed", "interval"
            }:
                raise ValueError("universal release fallback rule fields are invalid")
            fallback.append(FallbackRule(
                rule_id=str(item["rule_id"]),
                venue=str(item["venue"]),
                market=str(item["market"]),
                product_type=str(item["product_type"]),
                feed=DemandFeed(str(item["feed"])),
                interval=(str(item["interval"]) if item["interval"] is not None else None),
            ))
        keys = [
            (item.venue, item.market, item.product_type, item.feed.value, item.interval)
            for item in fallback
        ]
        if len({item.rule_id for item in fallback}) != len(fallback) or len(set(keys)) != len(keys):
            raise ValueError("universal release fallback rules overlap")
        return cls(
            revision=revision,
            contract_version=str(raw["contract_version"]),
            capability_matrix_path=matrix_path,
            capability_matrix_sha256=matrix_sha256,
            capability_matrix_revision=int(matrix["revision"]),
            v1_rollback=UniversalV1Rollback(**v1),
            resource_budget=UniversalResourceBudget(**{key: int(value) for key, value in budget.items()}),
            allowed_non_admitted_states=allowed_states,
            consumer_classes=tuple(classes),
            fallback_rules=tuple(fallback),
            policy_sha256=_sha256_bytes(Path(path).read_bytes()),
        )

    def classify(self, consumer_id: str) -> UniversalConsumerClass:
        matches = [
            rule.consumer_class
            for rule in self.consumer_classes
            if any(consumer_id.startswith(prefix) for prefix in rule.consumer_prefixes)
        ]
        if len(matches) != 1:
            raise ValueError(f"universal release consumer is unclassified: {consumer_id}")
        return matches[0]

    def fallback_for(self, row: ProviderAdmissionRow) -> FallbackRule | None:
        matches = tuple(item for item in self.fallback_rules if item.matches(row))
        if len(matches) > 1:
            raise ValueError("universal release fallback rule is ambiguous")
        return matches[0] if matches else None


@dataclass(frozen=True, slots=True)
class UniversalReleaseCoverage:
    inventory_sha256: str
    realtime_plan_sha256: str
    realtime_evidence_sha256: str
    realtime_requirement_ids: tuple[str, ...]
    reference_evidence_sha256: str
    reference_requirement_ids: tuple[str, ...]
    l2_plan_sha256: str | None
    l2_evidence_sha256: str | None
    l2_requirement_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "inventory_sha256", _sha256(self.inventory_sha256, "coverage inventory_sha256"))
        object.__setattr__(self, "realtime_plan_sha256", _sha256(self.realtime_plan_sha256, "coverage realtime_plan_sha256"))
        object.__setattr__(self, "realtime_evidence_sha256", _sha256(self.realtime_evidence_sha256, "coverage realtime_evidence_sha256"))
        object.__setattr__(self, "reference_evidence_sha256", _sha256(self.reference_evidence_sha256, "coverage reference_evidence_sha256"))
        if self.l2_plan_sha256 is not None:
            object.__setattr__(self, "l2_plan_sha256", _sha256(self.l2_plan_sha256, "coverage l2_plan_sha256"))
        if self.l2_evidence_sha256 is not None:
            object.__setattr__(self, "l2_evidence_sha256", _sha256(self.l2_evidence_sha256, "coverage l2_evidence_sha256"))
        if (self.l2_plan_sha256 is None) != (self.l2_evidence_sha256 is None):
            raise ValueError("coverage L2 plan/evidence bindings must agree")
        for field in ("realtime_requirement_ids", "reference_requirement_ids", "l2_requirement_ids"):
            values = tuple(sorted({_sha256(value, field) for value in getattr(self, field)}))
            if len(values) != len(getattr(self, field)):
                raise ValueError(f"coverage {field} has duplicate requirement IDs")
            object.__setattr__(self, field, values)

    @classmethod
    def from_phase_plans(
        cls,
        *,
        inventory: ActiveDemandInventory,
        admission: ProviderAdmission,
        convergence: ActiveDemandConvergence,
        realtime_plan: UniversalRealtimePlan,
        realtime_evidence_sha256: str,
        reference_evidence_sha256: str,
        l2_plan: L2DemandPlan | None,
        l2_evidence_sha256: str | None,
    ) -> "UniversalReleaseCoverage":
        if (
            admission.inventory_sha256 != inventory.manifest_sha256
            or convergence.inventory_sha256 != inventory.manifest_sha256
            or realtime_plan.inventory_sha256 != inventory.manifest_sha256
        ):
            raise ValueError("universal release coverage inputs have different inventories")
        realtime_bindings = {
            (
                str(item["instrument_uid"]),
                str(item["feed"]),
                str(item["interval"]) if item["interval"] is not None else None,
                str(item["source"]["source_policy_id"]),
            )
            for item in realtime_plan.bundle.source_catalog["bindings"]
        }
        readiness = {item.requirement_id: item for item in convergence.readiness}
        if len(readiness) != len(convergence.readiness):
            raise ValueError("universal release convergence contains duplicate IDs")
        realtime_ids = []
        reference_ids = []
        book_ids = []
        for row in admission.rows:
            if row.state != "ADMITTED":
                continue
            requirement = source_requirement_for_admission(inventory, row)
            if readiness[row.requirement_id].state.value != "WARMING":
                raise ValueError("universal release admitted requirement is not converged")
            if requirement.feed in _REALTIME_FEEDS:
                key = (row.instrument_uid, row.feed, row.interval, requirement.source_policy_id)
                if key not in realtime_bindings:
                    raise ValueError("universal release realtime plan missed an admitted requirement")
                realtime_ids.append(row.requirement_id)
            elif requirement.feed in _BOOK_FEEDS:
                book_ids.append(row.requirement_id)
            else:
                reference_ids.append(row.requirement_id)
        if book_ids:
            if l2_plan is None or l2_plan.inventory_sha256 != inventory.manifest_sha256:
                raise ValueError("universal release active book demand lacks an L2 plan")
            l2_requirement_ids = {
                requirement_id
                for binding in l2_plan.bindings
                for requirement_id in binding.requirement_ids
            }
            if set(book_ids) != l2_requirement_ids:
                raise ValueError("universal release L2 plan differs from admitted book demand")
        elif l2_plan is not None:
            raise ValueError("universal release has an undeclared L2 plan")
        return cls(
            inventory_sha256=inventory.manifest_sha256,
            realtime_plan_sha256=_digest(realtime_plan.report_payload()),
            realtime_evidence_sha256=realtime_evidence_sha256,
            realtime_requirement_ids=tuple(realtime_ids),
            reference_evidence_sha256=reference_evidence_sha256,
            reference_requirement_ids=tuple(reference_ids),
            l2_plan_sha256=_digest(l2_plan.report_payload()) if l2_plan is not None else None,
            l2_evidence_sha256=l2_evidence_sha256,
            l2_requirement_ids=tuple(book_ids),
        )

    def plane_for(self, requirement_id: str) -> str:
        if requirement_id in self.realtime_requirement_ids:
            return "REALTIME"
        if requirement_id in self.reference_requirement_ids:
            return "REFERENCE"
        if requirement_id in self.l2_requirement_ids:
            return "L2"
        raise ValueError("universal release coverage has no provider plane for requirement")

    def canonical_mapping(self) -> dict[str, object]:
        return {
            "schema": _COVERAGE_SCHEMA,
            "inventory_sha256": self.inventory_sha256,
            "realtime_plan_sha256": self.realtime_plan_sha256,
            "realtime_evidence_sha256": self.realtime_evidence_sha256,
            "realtime_requirement_ids": list(self.realtime_requirement_ids),
            "reference_evidence_sha256": self.reference_evidence_sha256,
            "reference_requirement_ids": list(self.reference_requirement_ids),
            "l2_plan_sha256": self.l2_plan_sha256,
            "l2_evidence_sha256": self.l2_evidence_sha256,
            "l2_requirement_ids": list(self.l2_requirement_ids),
        }


@dataclass(frozen=True, slots=True)
class UniversalReleaseProduct:
    consumer_id: str
    consumer_class: UniversalConsumerClass
    requirement_id: str
    instrument_uid: str
    instrument_id: str
    venue: str
    market: str
    product_type: str
    native_symbol: str
    feed: str
    interval: str | None
    source_policy_id: str
    provider_plane: str
    max_freshness_ms: int | None
    require_final_bars: bool
    require_live: bool
    execution_grade: bool
    route: str
    fallback: str
    fallback_rule_id: str | None
    blocked_reason: str | None

    def __post_init__(self) -> None:
        for field in (
            "consumer_id", "requirement_id", "instrument_uid", "instrument_id", "venue",
            "market", "product_type", "native_symbol", "feed", "source_policy_id", "provider_plane",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(self, "requirement_id", _sha256(self.requirement_id, "requirement_id"))
        if self.venue not in _V2_VENUES or self.route != "V2_PRIMARY":
            raise ValueError("universal release product is not an admitted V2 route")
        if self.provider_plane not in {"REALTIME", "REFERENCE", "L2"}:
            raise ValueError("universal release provider plane is invalid")
        if self.fallback not in {"V1", "BLOCKED"}:
            raise ValueError("universal release fallback route is invalid")
        if self.fallback == "V1":
            if not self.fallback_rule_id or self.blocked_reason is not None:
                raise ValueError("V1 fallback needs one explicit compatibility rule")
        elif self.fallback_rule_id is not None or not self.blocked_reason:
            raise ValueError("blocked fallback needs a reason and no compatibility rule")
        if self.max_freshness_ms is not None and self.max_freshness_ms <= 0:
            raise ValueError("universal release freshness must be positive")

    def canonical_mapping(self) -> dict[str, object]:
        return {
            "consumer_id": self.consumer_id,
            "consumer_class": self.consumer_class.value,
            "requirement_id": self.requirement_id,
            "instrument_uid": self.instrument_uid,
            "instrument_id": self.instrument_id,
            "venue": self.venue,
            "market": self.market,
            "product_type": self.product_type,
            "native_symbol": self.native_symbol,
            "feed": self.feed,
            "interval": self.interval,
            "source_policy_id": self.source_policy_id,
            "provider_plane": self.provider_plane,
            "max_freshness_ms": self.max_freshness_ms,
            "require_final_bars": self.require_final_bars,
            "require_live": self.require_live,
            "execution_grade": self.execution_grade,
            "route": self.route,
            "fallback": self.fallback,
            "fallback_rule_id": self.fallback_rule_id,
            "blocked_reason": self.blocked_reason,
            "gap_policy": "BLOCK",
        }


@dataclass(frozen=True, slots=True)
class UniversalReleaseExclusion:
    consumer_id: str
    consumer_class: UniversalConsumerClass
    requirement_id: str
    venue: str
    market: str
    product_type: str
    native_symbol: str
    feed: str
    interval: str | None
    state: str
    reason: str

    def __post_init__(self) -> None:
        for field in (
            "consumer_id", "requirement_id", "venue", "market", "product_type", "native_symbol",
            "feed", "state", "reason",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(self, "requirement_id", _sha256(self.requirement_id, "requirement_id"))

    def canonical_mapping(self) -> dict[str, object]:
        return {
            "consumer_id": self.consumer_id,
            "consumer_class": self.consumer_class.value,
            "requirement_id": self.requirement_id,
            "venue": self.venue,
            "market": self.market,
            "product_type": self.product_type,
            "native_symbol": self.native_symbol,
            "feed": self.feed,
            "interval": self.interval,
            "state": self.state,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class UniversalReleaseManifest:
    revision: int
    policy_sha256: str
    capability_matrix_sha256: str
    capability_matrix_revision: int
    inventory_sha256: str
    provider_admission_sha256: str
    convergence_sha256: str
    coverage: UniversalReleaseCoverage
    v1_rollback: UniversalV1Rollback
    resource_budget: UniversalResourceBudget
    products: tuple[UniversalReleaseProduct, ...]
    exclusions: tuple[UniversalReleaseExclusion, ...]

    def __post_init__(self) -> None:
        if self.revision < 1 or self.capability_matrix_revision < 1:
            raise ValueError("universal release revision is invalid")
        for field in (
            "policy_sha256", "capability_matrix_sha256", "inventory_sha256",
            "provider_admission_sha256", "convergence_sha256",
        ):
            object.__setattr__(self, field, _sha256(getattr(self, field), field))
        if self.coverage.inventory_sha256 != self.inventory_sha256:
            raise ValueError("universal release coverage inventory differs")
        product_keys = [(item.consumer_id, item.requirement_id) for item in self.products]
        exclusion_keys = [(item.consumer_id, item.requirement_id) for item in self.exclusions]
        if not self.products or len(product_keys) != len(set(product_keys)):
            raise ValueError("universal release products are missing or duplicate")
        if len(exclusion_keys) != len(set(exclusion_keys)) or set(product_keys) & set(exclusion_keys):
            raise ValueError("universal release exclusions overlap products")

    def canonical_mapping(self) -> dict[str, object]:
        return {
            "schema": _SCHEMA,
            "revision": self.revision,
            "contract_version": "2.0.0",
            "policy_sha256": self.policy_sha256,
            "capability_matrix": {
                "sha256": self.capability_matrix_sha256,
                "revision": self.capability_matrix_revision,
            },
            "inventory_sha256": self.inventory_sha256,
            "provider_admission_sha256": self.provider_admission_sha256,
            "convergence_sha256": self.convergence_sha256,
            "coverage": self.coverage.canonical_mapping(),
            "v1_rollback": self.v1_rollback.canonical_mapping(),
            "resource_budget": self.resource_budget.canonical_mapping(),
            "products": [item.canonical_mapping() for item in self.products],
            "exclusions": [item.canonical_mapping() for item in self.exclusions],
        }

    @property
    def digest(self) -> str:
        return _digest(self.canonical_mapping())

    def report_payload(self) -> dict[str, object]:
        by_class = Counter(item.consumer_class.value for item in self.products)
        by_feed = Counter(item.feed for item in self.products)
        by_plane = Counter(item.provider_plane for item in self.products)
        by_fallback = Counter(item.fallback for item in self.products)
        by_exclusion = Counter(item.state for item in self.exclusions)
        return {
            "schema": "qdl.phase115.universal-release-preflight.v1",
            "status": "PREPARED",
            "manifest_sha256": self.digest,
            "inventory_sha256": self.inventory_sha256,
            "product_count": len(self.products),
            "exclusion_count": len(self.exclusions),
            "by_consumer_class": dict(sorted(by_class.items())),
            "by_feed": dict(sorted(by_feed.items())),
            "by_provider_plane": dict(sorted(by_plane.items())),
            "by_fallback": dict(sorted(by_fallback.items())),
            "exclusions_by_state": dict(sorted(by_exclusion.items())),
            "runtime_mutations": 0,
            "order_actions": 0,
        }


def build_universal_release_manifest(
    *,
    policy: UniversalReleasePolicy,
    inventory: ActiveDemandInventory,
    admission: ProviderAdmission,
    convergence: ActiveDemandConvergence,
    coverage: UniversalReleaseCoverage,
    release_revision: int,
) -> UniversalReleaseManifest:
    """Materialize one complete, fail-closed V2 candidate manifest."""

    if release_revision < 1:
        raise ValueError("universal release revision must be positive")
    if (
        admission.inventory_sha256 != inventory.manifest_sha256
        or convergence.inventory_sha256 != inventory.manifest_sha256
        or coverage.inventory_sha256 != inventory.manifest_sha256
    ):
        raise ValueError("universal release inputs have different inventory digests")
    admission_ids = {item.requirement_id for item in admission.rows}
    readiness = {item.requirement_id: item for item in convergence.readiness}
    if len(admission_ids) != len(admission.rows) or set(readiness) != admission_ids:
        raise ValueError("universal release admission/convergence identities differ")
    products = []
    exclusions = []
    for row in sorted(admission.rows, key=lambda item: (item.consumer_id, item.requirement_id)):
        requirement = source_requirement_for_admission(inventory, row)
        consumer_class = policy.classify(requirement.consumer_id)
        if row.state != "ADMITTED":
            if row.state not in policy.allowed_non_admitted_states:
                raise ValueError(f"universal release has unapproved admission state: {row.state}")
            exclusions.append(UniversalReleaseExclusion(
                consumer_id=row.consumer_id,
                consumer_class=consumer_class,
                requirement_id=row.requirement_id,
                venue=row.venue,
                market=row.market,
                product_type=row.product_type,
                native_symbol=row.native_symbol,
                feed=row.feed,
                interval=row.interval,
                state=row.state,
                reason=row.reason or "EXPLICIT_NON_ADMITTED_PROVIDER_STATE",
            ))
            continue
        if row.venue not in _V2_VENUES or row.instrument_uid is None or row.instrument_id is None:
            raise ValueError("admitted universal release row lacks V2 identity")
        if readiness[row.requirement_id].state.value != "WARMING":
            raise ValueError("admitted universal release row is not converged")
        plane = coverage.plane_for(row.requirement_id)
        fallback = policy.fallback_for(row)
        products.append(UniversalReleaseProduct(
            consumer_id=row.consumer_id,
            consumer_class=consumer_class,
            requirement_id=row.requirement_id,
            instrument_uid=row.instrument_uid,
            instrument_id=row.instrument_id,
            venue=row.venue,
            market=row.market,
            product_type=row.product_type,
            native_symbol=row.native_symbol,
            feed=row.feed,
            interval=row.interval,
            source_policy_id=requirement.source_policy_id,
            provider_plane=plane,
            max_freshness_ms=requirement.max_freshness_ms,
            require_final_bars=requirement.require_final_bars,
            require_live=requirement.require_live,
            execution_grade=requirement.execution_grade,
            route="V2_PRIMARY",
            fallback="V1" if fallback else "BLOCKED",
            fallback_rule_id=fallback.rule_id if fallback else None,
            blocked_reason=None if fallback else "V1_EQUIVALENCE_UNPROVEN",
        ))
    manifest = UniversalReleaseManifest(
        revision=release_revision,
        policy_sha256=policy.policy_sha256,
        capability_matrix_sha256=policy.capability_matrix_sha256,
        capability_matrix_revision=policy.capability_matrix_revision,
        inventory_sha256=inventory.manifest_sha256,
        provider_admission_sha256=_digest(admission.report_payload()),
        convergence_sha256=_digest(convergence.report_payload()),
        coverage=coverage,
        v1_rollback=policy.v1_rollback,
        resource_budget=policy.resource_budget,
        products=tuple(sorted(products, key=lambda item: (item.consumer_id, item.requirement_id))),
        exclusions=tuple(sorted(exclusions, key=lambda item: (item.consumer_id, item.requirement_id))),
    )
    if len(manifest.products) + len(manifest.exclusions) != len(admission.rows):
        raise ValueError("universal release did not cover every admitted inventory row")
    return manifest
