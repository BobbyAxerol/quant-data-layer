"""Frozen, per-requirement routing contract for a stable V2 release.

The existing shared-primary plan is intentionally consumer-level because it
seals a Rust-authority handoff.  A release needs one additional distinction:
some requirements within one consumer can safely fall back to V1, while a
different requirement must block rather than silently change market-data
semantics.  This module is pure and has no provider, broker or runtime I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

from qdl.consumer.manifest import ConsumerManifest, ConsumerManifestLoader
from qdl.consumer.realtime_route import RealtimeRoute, requirement_key
from qdl.runtime.stable_catalog import StableSourceCatalog


_HEX = frozenset("0123456789abcdef")
_V2_VENUES = frozenset({"BINANCE", "OKX"})
_V2_ROUTE = RealtimeRoute.V2_PRIMARY.value
_V1_ROUTE = RealtimeRoute.V1_PRIMARY.value
_FALLBACKS = frozenset({"V1", "BLOCKED", "NONE"})


def _pass_through_eligible(catalog: StableSourceCatalog, requirement: object) -> bool:
    """Defer the provider-history import to avoid the security/stream cycle."""
    from qdl.runtime.provider_history import pass_through_eligible

    return pass_through_eligible(catalog, requirement)  # type: ignore[arg-type]


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require_sha256(value: object, field: str) -> str:
    result = str(value).strip().lower()
    if len(result) != 64 or any(character not in _HEX for character in result):
        raise ValueError(f"{field} must be a SHA-256 digest")
    return result


def _require_git_commit(value: object, field: str) -> str:
    result = str(value).strip().lower()
    if len(result) not in {40, 64} or any(character not in _HEX for character in result):
        raise ValueError(f"{field} must be a full Git object ID")
    return result


def _resolve_under_root(path: object, *, root: Path, field: str) -> Path:
    declared = Path(str(path))
    relative = (
        Path(*declared.parts[2:])
        if declared.is_absolute() and len(declared.parts) > 1
        and declared.parts[1] == "app"
        else declared
    )
    target = (root / relative).resolve()
    try:
        target.relative_to(root)
    except ValueError as error:
        raise ValueError(f"{field} escapes repository root") from error
    return target


@dataclass(frozen=True, slots=True)
class ReleaseArtifactReference:
    path: Path
    sha256: str
    revision: int

    @classmethod
    def load(
        cls,
        raw: object,
        *,
        root: Path,
        field: str,
        revision_field: str = "revision",
    ) -> "ReleaseArtifactReference":
        if not isinstance(raw, dict) or set(raw) != {"path", "sha256", "revision"}:
            raise ValueError(f"{field} reference fields are incomplete or unknown")
        target = _resolve_under_root(raw["path"], root=root, field=field)
        if not target.is_file():
            raise FileNotFoundError(f"{field} reference is unavailable: {target}")
        contents = target.read_bytes()
        expected = _require_sha256(raw["sha256"], f"{field}.sha256")
        if _sha256_bytes(contents) != expected:
            raise ValueError(f"{field} reference checksum differs")
        document = yaml.safe_load(contents)
        revision = document.get(revision_field) if isinstance(document, dict) else None
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise ValueError(f"{field} reference has no valid {revision_field}")
        if int(raw["revision"]) != revision:
            raise ValueError(f"{field} reference revision differs")
        return cls(path=target, sha256=expected, revision=revision)


@dataclass(frozen=True, slots=True)
class V1FallbackReference:
    release_tag: str
    source_commit: str
    image_reference: str
    api_contract: str
    health_endpoint: str
    runtime_image_verification_required: bool

    @classmethod
    def from_mapping(cls, raw: object) -> "V1FallbackReference":
        expected = {
            "release_tag", "source_commit", "image_reference", "api_contract",
            "health_endpoint", "runtime_image_verification_required",
        }
        if not isinstance(raw, dict) or set(raw) != expected:
            raise ValueError("V1 fallback reference fields are incomplete or unknown")
        source_commit = _require_git_commit(raw["source_commit"], "V1 fallback source_commit")
        result = cls(
            release_tag=str(raw["release_tag"]).strip(),
            source_commit=source_commit,
            image_reference=str(raw["image_reference"]).strip(),
            api_contract=str(raw["api_contract"]).strip().upper(),
            health_endpoint=str(raw["health_endpoint"]).strip(),
            runtime_image_verification_required=raw["runtime_image_verification_required"],
        )
        if (
            not result.release_tag
            or not result.image_reference
            or result.api_contract != "V1"
            or result.health_endpoint != "/v1/health"
            or not isinstance(result.runtime_image_verification_required, bool)
            or not result.runtime_image_verification_required
        ):
            raise ValueError("V1 fallback reference is not exact and runtime-verifiable")
        return result


@dataclass(frozen=True, slots=True)
class ReleaseResourceBudget:
    max_consumer_lag: int
    max_cpu_millicores: int
    max_rss_bytes: int

    @classmethod
    def from_mapping(cls, raw: object) -> "ReleaseResourceBudget":
        expected = {"max_consumer_lag", "max_cpu_millicores", "max_rss_bytes"}
        if not isinstance(raw, dict) or set(raw) != expected:
            raise ValueError("stable release resource budget fields are incomplete or unknown")
        values = tuple(raw[name] for name in sorted(expected))
        if any(not isinstance(value, int) or isinstance(value, bool) or value <= 0 for value in values):
            raise ValueError("stable release resource budget values must be positive integers")
        return cls(
            max_consumer_lag=int(raw["max_consumer_lag"]),
            max_cpu_millicores=int(raw["max_cpu_millicores"]),
            max_rss_bytes=int(raw["max_rss_bytes"]),
        )


@dataclass(frozen=True, slots=True)
class StableReleaseProductRoute:
    requirement_key: str
    route: str
    fallback: str
    reason: str | None

    @classmethod
    def from_mapping(cls, raw: object) -> "StableReleaseProductRoute":
        expected = {"requirement_key", "route", "fallback", "reason"}
        if not isinstance(raw, dict) or set(raw) != expected:
            raise ValueError("stable release product route fields are incomplete or unknown")
        result = cls(
            requirement_key=str(raw["requirement_key"]).strip(),
            route=str(raw["route"]).strip().upper(),
            fallback=str(raw["fallback"]).strip().upper(),
            reason=(str(raw["reason"]).strip() if raw["reason"] is not None else None),
        )
        if not result.requirement_key or result.route not in {_V2_ROUTE, _V1_ROUTE}:
            raise ValueError("stable release product route identity or route is invalid")
        if result.fallback not in _FALLBACKS:
            raise ValueError("stable release product fallback is invalid")
        if result.route == _V2_ROUTE:
            if result.fallback not in {"V1", "BLOCKED"}:
                raise ValueError("V2 primary product needs V1 or BLOCKED fallback")
            if result.fallback == "BLOCKED" and not result.reason:
                raise ValueError("blocked V2 product requires an explicit reason")
            if result.fallback == "V1" and result.reason is not None:
                raise ValueError("V1 fallback product cannot carry a block reason")
        elif result.fallback != "NONE" or not result.reason:
            raise ValueError("V1 primary product needs NONE fallback and an exclusion reason")
        return result


@dataclass(frozen=True, slots=True)
class StableReleaseConsumerRoute:
    consumer_id: str
    manifest_path: Path
    manifest: ConsumerManifest
    demand_revision: int
    products: tuple[StableReleaseProductRoute, ...]


@dataclass(frozen=True, slots=True)
class StableReleaseRoutePlan:
    """Immutable source contract for the Phase 10.5 consumer release path."""

    schema: str
    revision: int
    contract_version: str
    source_catalog: ReleaseArtifactReference
    crypto_demand: ReleaseArtifactReference
    capability_matrix: ReleaseArtifactReference
    v1_fallback: V1FallbackReference
    resource_budget: ReleaseResourceBudget
    consumers: tuple[StableReleaseConsumerRoute, ...]

    def __post_init__(self) -> None:
        if (
            self.schema != "qdl.v2.stable-release-routing.v1"
            or self.revision < 1
            or self.contract_version != "2.0.0"
            or not self.consumers
        ):
            raise ValueError("stable release route schema/revision is invalid")
        identifiers = [item.consumer_id for item in self.consumers]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("stable release consumers must be unique")

    @classmethod
    def load(cls, path: str | Path, *, manifest_root: str | Path) -> "StableReleaseRoutePlan":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls.from_mapping(payload, manifest_root=manifest_root)

    @classmethod
    def from_mapping(
        cls,
        payload: object,
        *,
        manifest_root: str | Path,
    ) -> "StableReleaseRoutePlan":
        expected = {
            "schema", "revision", "contract_version", "source_catalog",
            "crypto_demand", "capability_matrix", "v1_fallback", "consumers",
            "resource_budget",
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            raise ValueError("stable release route fields are incomplete or unknown")
        root = Path(manifest_root).resolve()
        catalog_ref = ReleaseArtifactReference.load(
            payload["source_catalog"], root=root, field="source_catalog",
            revision_field="catalog_revision",
        )
        demand_ref = ReleaseArtifactReference.load(
            payload["crypto_demand"], root=root, field="crypto_demand",
        )
        capabilities_ref = ReleaseArtifactReference.load(
            payload["capability_matrix"], root=root, field="capability_matrix",
        )
        catalog = StableSourceCatalog.load(catalog_ref.path)
        demand_keys = cls._load_crypto_demand(demand_ref.path)
        capabilities = yaml.safe_load(capabilities_ref.path.read_text(encoding="utf-8"))
        cls._validate_capability_matrix(capabilities)
        v1_fallback = V1FallbackReference.from_mapping(payload["v1_fallback"])
        resource_budget = ReleaseResourceBudget.from_mapping(payload["resource_budget"])
        consumers_raw = payload["consumers"]
        if not isinstance(consumers_raw, list) or not 1 <= len(consumers_raw) <= 10_000:
            raise ValueError("stable release requires 1..10000 consumers")
        consumers = tuple(
            cls._consumer(
                item,
                root=root,
                catalog=catalog,
                demand_revision=demand_ref.revision,
                demand_keys=demand_keys,
            )
            for item in consumers_raw
        )
        return cls(
            schema=str(payload["schema"]),
            revision=int(payload["revision"]),
            contract_version=str(payload["contract_version"]),
            source_catalog=catalog_ref,
            crypto_demand=demand_ref,
            capability_matrix=capabilities_ref,
            v1_fallback=v1_fallback,
            resource_budget=resource_budget,
            consumers=consumers,
        )

    @staticmethod
    def _validate_capability_matrix(raw: object) -> None:
        if not isinstance(raw, dict) or (
            raw.get("public_contract_version") != "2.0.0"
            or raw.get("realtime_core_target") != "RUST"
            or raw.get("equal_source_contract") is not True
        ):
            raise ValueError("stable release capability matrix is not V2 Rust-core compatible")
        gates = raw.get("capability_gates")
        if not isinstance(gates, dict):
            raise ValueError("stable release capability gates are unavailable")
        for name in ("VN_TRADE", "VN_BAR"):
            gate = gates.get(name)
            if not isinstance(gate, dict) or gate.get("stable") is not False:
                raise ValueError("uncertified VN capability must remain excluded from V2 release")

    @staticmethod
    def _load_crypto_demand(path: Path) -> frozenset[tuple[str, str, str, str, str, str | None, str]]:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if (
            not isinstance(raw, dict)
            or raw.get("schema") != "qdl.v2.production-demand.v1"
            or not isinstance(raw.get("consumers"), list)
        ):
            raise ValueError("stable release crypto demand is invalid")
        keys = set()
        for consumer in raw["consumers"]:
            if not isinstance(consumer, dict) or not isinstance(consumer.get("requirements"), list):
                raise ValueError("stable release crypto demand consumer is invalid")
            for requirement in consumer["requirements"]:
                if not isinstance(requirement, dict):
                    raise ValueError("stable release crypto demand requirement is invalid")
                required = {
                    "venue", "market", "product_type", "native_symbol", "feed",
                    "interval", "source_policy_id",
                }
                if set(requirement) != required:
                    raise ValueError("stable release crypto demand requirement fields differ")
                keys.add((
                    str(requirement["venue"]).upper(),
                    str(requirement["market"]).upper(),
                    str(requirement["product_type"]).upper(),
                    str(requirement["native_symbol"]).upper(),
                    str(requirement["feed"]).upper(),
                    str(requirement["interval"]) if requirement["interval"] is not None else None,
                    str(requirement["source_policy_id"]),
                ))
        if not keys:
            raise ValueError("stable release crypto demand cannot be empty")
        return frozenset(keys)

    @staticmethod
    def _consumer(
        raw: object,
        *,
        root: Path,
        catalog: StableSourceCatalog,
        demand_revision: int,
        demand_keys: frozenset[tuple[str, str, str, str, str, str | None, str]],
    ) -> StableReleaseConsumerRoute:
        expected = {
            "consumer_id", "manifest", "manifest_revision", "manifest_sha256",
            "demand_revision", "products",
        }
        if not isinstance(raw, dict) or set(raw) != expected:
            raise ValueError("stable release consumer fields are incomplete or unknown")
        path = _resolve_under_root(raw["manifest"], root=root, field="consumer manifest")
        manifest = ConsumerManifestLoader.load(path)
        consumer_id = str(raw["consumer_id"]).strip()
        if (
            not consumer_id
            or consumer_id != manifest.consumer_id
            or int(raw["manifest_revision"]) != manifest.manifest_revision
            or _require_sha256(raw["manifest_sha256"], "consumer manifest_sha256")
            != manifest.manifest_sha256
            or int(raw["demand_revision"]) != demand_revision
        ):
            raise ValueError("stable release consumer manifest/demand binding differs")
        products_raw = raw["products"]
        if not isinstance(products_raw, list) or not products_raw:
            raise ValueError("stable release consumer needs product routes")
        products = tuple(StableReleaseProductRoute.from_mapping(item) for item in products_raw)
        product_keys = [item.requirement_key for item in products]
        if len(product_keys) != len(set(product_keys)):
            raise ValueError("stable release consumer product routes must be unique")
        requirements = {requirement_key(item): item for item in manifest.requirements}
        if set(product_keys) != set(requirements):
            raise ValueError("stable release product routes differ from consumer manifest")
        for product in products:
            StableReleaseRoutePlan._validate_product(
                product,
                requirement=requirements[product.requirement_key],
                catalog=catalog,
                demand_keys=demand_keys,
            )
        return StableReleaseConsumerRoute(
            consumer_id=consumer_id,
            manifest_path=path,
            manifest=manifest,
            demand_revision=demand_revision,
            products=products,
        )

    @staticmethod
    def _validate_product(
        product: StableReleaseProductRoute,
        *,
        requirement,
        catalog: StableSourceCatalog,
        demand_keys: frozenset[tuple[str, str, str, str, str, str | None, str]],
    ) -> None:
        instrument = catalog.instrument_for(requirement.instrument_uid)
        venue = instrument.identity.venue
        binding = None
        try:
            binding = catalog.binding_for(requirement)
        except (KeyError, ValueError):
            if not _pass_through_eligible(catalog, requirement):
                raise ValueError("stable release product has no V2 source")
        if product.route == _V2_ROUTE:
            if venue not in _V2_VENUES:
                raise ValueError("only certified Binance/OKX products may be V2 primary")
            if binding is not None:
                identity = instrument.identity
                demand_key = (
                    identity.venue,
                    identity.market,
                    identity.product_type.value,
                    instrument.native_symbol,
                    requirement.feed.value,
                    requirement.interval,
                    requirement.source_policy_id,
                )
                if demand_key not in demand_keys:
                    raise ValueError("materialized V2 release product is absent from crypto demand")
            if product.fallback == "V1":
                if binding is None or binding.v1_compatibility == "NONE":
                    raise ValueError("V1 fallback lacks proven binding compatibility")
        elif venue in _V2_VENUES:
            raise ValueError("certified crypto product cannot remain V1 primary in release manifest")

    def _canonical_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "revision": self.revision,
            "contract_version": self.contract_version,
            "source_catalog": {
                "sha256": self.source_catalog.sha256,
                "revision": self.source_catalog.revision,
            },
            "crypto_demand": {
                "sha256": self.crypto_demand.sha256,
                "revision": self.crypto_demand.revision,
            },
            "capability_matrix": {
                "sha256": self.capability_matrix.sha256,
                "revision": self.capability_matrix.revision,
            },
            "v1_fallback": {
                "release_tag": self.v1_fallback.release_tag,
                "source_commit": self.v1_fallback.source_commit,
                "image_reference": self.v1_fallback.image_reference,
                "api_contract": self.v1_fallback.api_contract,
                "health_endpoint": self.v1_fallback.health_endpoint,
                "runtime_image_verification_required": self.v1_fallback.runtime_image_verification_required,
            },
            "resource_budget": {
                "max_consumer_lag": self.resource_budget.max_consumer_lag,
                "max_cpu_millicores": self.resource_budget.max_cpu_millicores,
                "max_rss_bytes": self.resource_budget.max_rss_bytes,
            },
            "consumers": [
                {
                    "consumer_id": item.consumer_id,
                    "manifest_revision": item.manifest.manifest_revision,
                    "manifest_sha256": item.manifest.manifest_sha256,
                    "demand_revision": item.demand_revision,
                    "products": [
                        {
                            "requirement_key": product.requirement_key,
                            "route": product.route,
                            "fallback": product.fallback,
                            "reason": product.reason,
                        }
                        for product in item.products
                    ],
                }
                for item in self.consumers
            ],
        }

    @property
    def digest(self) -> str:
        return _sha256_bytes(json.dumps(
            self._canonical_payload(), sort_keys=True, separators=(",", ":")
        ).encode())

    def products(self) -> tuple[tuple[str, StableReleaseProductRoute], ...]:
        return tuple(
            (consumer.consumer_id, product)
            for consumer in self.consumers
            for product in consumer.products
        )


@dataclass(frozen=True, slots=True)
class ReleaseRouteObservation:
    """Bounded health evidence for one declared release product route."""

    consumer_id: str
    requirement_key: str
    route: str
    reason: str
    v2_source_age_ms: int | None
    v2_receive_age_ms: int | None
    v2_gap_open: bool
    v1_source_age_ms: int | None
    v1_receive_age_ms: int | None
    consumer_lag: int
    cpu_millicores: int
    rss_bytes: int

    def __post_init__(self) -> None:
        if (
            not self.consumer_id
            or not self.requirement_key
            or self.route not in {item.value for item in RealtimeRoute}
            or not self.reason
        ):
            raise ValueError("release route observation identity is invalid")
        if any(
            value is not None and (isinstance(value, bool) or value < 0)
            for value in (
                self.v2_source_age_ms, self.v2_receive_age_ms,
                self.v1_source_age_ms, self.v1_receive_age_ms,
            )
        ):
            raise ValueError("release route freshness must be non-negative")
        if any(
            isinstance(value, bool) or value < 0
            for value in (self.consumer_lag, self.cpu_millicores, self.rss_bytes)
        ):
            raise ValueError("release route resource metrics must be non-negative")

    def public_record(self) -> dict[str, object]:
        return {
            "consumer_id": self.consumer_id,
            "requirement_key": self.requirement_key,
            "route": self.route,
            "reason": self.reason,
            "v2_source_age_ms": self.v2_source_age_ms,
            "v2_receive_age_ms": self.v2_receive_age_ms,
            "v2_gap_open": self.v2_gap_open,
            "v1_source_age_ms": self.v1_source_age_ms,
            "v1_receive_age_ms": self.v1_receive_age_ms,
            "consumer_lag": self.consumer_lag,
            "cpu_millicores": self.cpu_millicores,
            "rss_bytes": self.rss_bytes,
        }


@dataclass(frozen=True, slots=True)
class ReleaseReadinessSummary:
    status: str
    ready: bool
    route_plan_sha256: str
    product_count: int
    v2_primary_count: int
    v1_primary_count: int
    fallback_count: int
    blocked_count: int
    fallback_rate: float
    max_consumer_lag: int
    max_cpu_millicores: int
    max_rss_bytes: int
    budget_violations: tuple[str, ...]


def is_explicit_v1_exclusion(
    product: StableReleaseProductRoute,
    observation: ReleaseRouteObservation,
) -> bool:
    """Return whether a V1-only product is honestly recorded as excluded.

    A V1-primary route in the frozen V2 release plan is not V2 evidence and
    must never be made to look current by borrowing stale or invented V1/V2
    ages.  Its declared plan reason is the sole release evidence; a separate
    venue certificate can later replace that exclusion through a new manifest
    revision.
    """
    return (
        product.route == _V1_ROUTE
        and observation.route == _V1_ROUTE
        and observation.reason == product.reason
        and observation.v2_source_age_ms is None
        and observation.v2_receive_age_ms is None
        and observation.v1_source_age_ms is None
        and observation.v1_receive_age_ms is None
        and not observation.v2_gap_open
    )


def evaluate_release_readiness(
    plan: StableReleaseRoutePlan,
    observations: Iterable[ReleaseRouteObservation],
) -> ReleaseReadinessSummary:
    """Validate an exact release manifest observation set without hiding drift.

    `READY` means every release V2 product remains V2-primary and every
    explicitly excluded product remains V1-primary. `DEGRADED` means an
    allowed V1 fallback is active. `NOT_READY` means a product is blocked or
    the observation set does not match the frozen manifest.
    """

    expected = {
        (consumer.consumer_id, product.requirement_key): (
            product,
            {
                requirement_key(item): item
                for item in consumer.manifest.requirements
            }[product.requirement_key],
        )
        for consumer in plan.consumers
        for product in consumer.products
    }
    values = tuple(observations)
    actual = {(item.consumer_id, item.requirement_key): item for item in values}
    if len(actual) != len(values) or set(actual) != set(expected):
        raise ValueError("release readiness observations differ from frozen manifest")
    fallback_count = 0
    blocked_count = 0
    v2_primary_count = 0
    v1_primary_count = 0
    budget_violations = set()
    for identity, (product, requirement) in expected.items():
        observed = actual[identity]
        if observed.consumer_lag > plan.resource_budget.max_consumer_lag:
            budget_violations.add(f"CONSUMER_LAG:{identity[0]}")
        if observed.cpu_millicores > plan.resource_budget.max_cpu_millicores:
            budget_violations.add(f"CPU_MILLICORES:{identity[0]}")
        if observed.rss_bytes > plan.resource_budget.max_rss_bytes:
            budget_violations.add(f"RSS_BYTES:{identity[0]}")
        if product.route == _V1_ROUTE:
            if not is_explicit_v1_exclusion(product, observed):
                raise ValueError(
                    "V1-primary release product is not an explicit V2 exclusion"
                )
            v1_primary_count += 1
            continue
        if observed.route == _V2_ROUTE:
            if (
                observed.v2_gap_open
                or observed.v2_source_age_ms is None
                or (
                    requirement.max_freshness_ms is not None
                    and observed.v2_source_age_ms > requirement.max_freshness_ms
                )
            ):
                blocked_count += 1
            else:
                v2_primary_count += 1
        elif observed.route == RealtimeRoute.V1_FALLBACK.value and product.fallback == "V1":
            if (
                observed.v1_source_age_ms is None
                or (
                    requirement.max_freshness_ms is not None
                    and observed.v1_source_age_ms > requirement.max_freshness_ms
                )
            ):
                blocked_count += 1
            else:
                fallback_count += 1
        elif observed.route == RealtimeRoute.BLOCKED.value:
            blocked_count += 1
        else:
            raise ValueError("release product used an undeclared fallback route")
    product_count = len(expected)
    v2_products = sum(
        product.route == _V2_ROUTE
        for product, _requirement in expected.values()
    )
    if blocked_count or budget_violations:
        status = "NOT_READY"
    elif fallback_count:
        status = "DEGRADED"
    else:
        status = "READY"
    return ReleaseReadinessSummary(
        status=status,
        ready=status == "READY",
        route_plan_sha256=plan.digest,
        product_count=product_count,
        v2_primary_count=v2_primary_count,
        v1_primary_count=v1_primary_count,
        fallback_count=fallback_count,
        blocked_count=blocked_count,
        fallback_rate=(fallback_count / v2_products) if v2_products else 0.0,
        max_consumer_lag=max(item.consumer_lag for item in actual.values()),
        max_cpu_millicores=max(item.cpu_millicores for item in actual.values()),
        max_rss_bytes=max(item.rss_bytes for item in actual.values()),
        budget_violations=tuple(sorted(budget_violations)),
    )
