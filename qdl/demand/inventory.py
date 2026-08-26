"""Compile declared consumer configuration into a dark universal-demand manifest.

This module is deliberately a control-plane compiler.  It reads versioned
deployment declarations, never runtime process state, logs, Redis or Docker,
and it never changes subscriptions.  The output is an auditable manifest that
Phase 11.2 can admit into the dynamic Rust/Python runtime after a separate
approval.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field, replace
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any, Callable, Iterable, Mapping

import yaml

from qdl.adapters.binance_spot import parse_spot_exchange_info
from qdl.adapters.binance_usdm import parse_exchange_info
from qdl.adapters.okx.instruments import parse_public_instrument
from qdl.demand.contracts import (
    CapabilityAvailability,
    DataRequirement,
    DemandFeed,
    DemandPurpose,
    DemandState,
    ResolvedRequirement,
    UniverseSelector,
    UniverseSelectorKind,
)
from qdl.demand.resolver import CapabilityRegistry, DemandLeaseRegistry, DemandResolver
from qdl.demand.topology import DemandTopology, DemandTopologyPlanner
from qdl.domain.instrument import InstrumentRecord, ProductType


SOURCE_REGISTRY_SCHEMA = "qdl.v2.active-demand-source-registry.v1"
INVENTORY_SCHEMA = "qdl.v2.active-demand-inventory.v1"
MANIFEST_SCHEMA = "qdl.v2.universal-demand.v1"

_INTERVAL_MILLISECONDS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
    "1M": 2_592_000_000,
}
_SUPPORTED_PARSERS = frozenset({"ALPHA_COMPOSE_V1", "PRODUCTION_DEMAND_V1"})
_TARGET_VENUES = frozenset({"BINANCE", "OKX"})


class InventoryError(ValueError):
    """A declared consumer source cannot be compiled truthfully."""


@dataclass(frozen=True, slots=True)
class SourceDocument:
    source_id: str
    root_id: str
    relative_path: str
    sha256: str
    byte_count: int

    def canonical_mapping(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "root_id": self.root_id,
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "byte_count": self.byte_count,
        }


@dataclass(frozen=True, slots=True)
class InventoryExclusion:
    source_id: str
    relative_path: str
    owner_id: str | None
    code: str
    detail: str

    def canonical_mapping(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "relative_path": self.relative_path,
            "owner_id": self.owner_id,
            "code": self.code,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class InventoryCandidate:
    requirement: DataRequirement
    source_refs: tuple[str, ...]
    source_kind: str
    detail: str

    @property
    def key(self) -> str:
        return json.dumps(
            self.requirement.canonical_mapping(), sort_keys=True, separators=(",", ":")
        )


@dataclass(frozen=True, slots=True)
class SourceSpec:
    source_id: str
    root_id: str
    parser: str
    path: str | None
    glob: str | None
    source_policy_id: str
    priority: int
    ttl_seconds: int
    default_warmup_limit: int
    bar_freshness_multiplier: int

    def __post_init__(self) -> None:
        if not self.source_id.strip() or not self.root_id.strip():
            raise InventoryError("source_id and root are required")
        if self.parser not in _SUPPORTED_PARSERS:
            raise InventoryError(f"unsupported source parser: {self.parser}")
        if bool(self.path) == bool(self.glob):
            raise InventoryError("source requires exactly one of path or glob")
        if not self.source_policy_id.strip():
            raise InventoryError("source_policy_id is required")
        if not 0 <= self.priority <= 1_000:
            raise InventoryError("source priority is outside bounds")
        if not 30 <= self.ttl_seconds <= 3_600:
            raise InventoryError("source ttl_seconds is outside bounds")
        if not 1 <= self.default_warmup_limit <= 100_000:
            raise InventoryError("source default_warmup_limit is outside bounds")
        if not 1 <= self.bar_freshness_multiplier <= 100:
            raise InventoryError("source bar_freshness_multiplier is outside bounds")


@dataclass(frozen=True, slots=True)
class AdmissionBudget:
    venue: str
    market: str
    feed: DemandFeed
    max_slices: int

    def __post_init__(self) -> None:
        venue = str(self.venue or "").strip().upper()
        market = str(self.market or "").strip().upper()
        if not venue or not market:
            raise InventoryError("admission budget venue and market are required")
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "market", market)
        object.__setattr__(self, "feed", DemandFeed(self.feed))
        if not 1 <= self.max_slices <= 100_000:
            raise InventoryError("admission budget max_slices is outside bounds")

    @property
    def key(self) -> tuple[str, str, DemandFeed]:
        return self.venue, self.market, self.feed


@dataclass(frozen=True, slots=True)
class AdmissionPolicy:
    max_subscriptions_per_connection: int
    max_total_slices: int
    budgets: tuple[AdmissionBudget, ...]

    def __post_init__(self) -> None:
        if not 1 <= self.max_subscriptions_per_connection <= 1_024:
            raise InventoryError("admission max_subscriptions_per_connection is outside bounds")
        if not 1 <= self.max_total_slices <= 100_000:
            raise InventoryError("admission max_total_slices is outside bounds")
        if not self.budgets or len({item.key for item in self.budgets}) != len(self.budgets):
            raise InventoryError("admission budgets must be non-empty and unique")

    def limit_for(self, venue: str, market: str, feed: DemandFeed) -> int:
        key = str(venue).upper(), str(market).upper(), DemandFeed(feed)
        for budget in self.budgets:
            if budget.key == key:
                return budget.max_slices
        raise InventoryError(
            "active-demand policy has no budget for "
            f"{key[0]}/{key[1]}/{key[2].value}"
        )


@dataclass(frozen=True, slots=True)
class ActiveDemandSourceRegistry:
    revision: int
    owner_registry_root: str
    owner_registry_path: str
    admission_policy: AdmissionPolicy
    sources: tuple[SourceSpec, ...]
    source_path: Path

    @classmethod
    def load(cls, path: str | Path) -> "ActiveDemandSourceRegistry":
        source_path = Path(path).resolve()
        raw = yaml.safe_load(source_path.read_text(encoding="utf-8"))
        expected = {"schema", "revision", "owner_registry", "admission", "sources"}
        if not isinstance(raw, Mapping) or set(raw) != expected:
            raise InventoryError("active-demand source registry fields are incomplete or unknown")
        if raw.get("schema") != SOURCE_REGISTRY_SCHEMA or int(raw.get("revision") or 0) < 1:
            raise InventoryError("active-demand source registry schema/revision is invalid")
        owner = raw.get("owner_registry")
        if not isinstance(owner, Mapping) or set(owner) != {"root", "path"}:
            raise InventoryError("owner_registry requires root and path")
        admission = raw.get("admission")
        admission_fields = {
            "max_subscriptions_per_connection", "max_total_slices", "budgets",
        }
        if not isinstance(admission, Mapping) or set(admission) != admission_fields:
            raise InventoryError("admission policy fields are incomplete or unknown")
        budget_rows = admission.get("budgets")
        if not isinstance(budget_rows, list):
            raise InventoryError("admission budgets must be a list")
        if any(
            not isinstance(row, Mapping)
            or set(row) != {"venue", "market", "feed", "max_slices"}
            for row in budget_rows
        ):
            raise InventoryError("admission budget fields are incomplete or unknown")
        policy = AdmissionPolicy(
            max_subscriptions_per_connection=int(admission["max_subscriptions_per_connection"]),
            max_total_slices=int(admission["max_total_slices"]),
            budgets=tuple(
                AdmissionBudget(
                    venue=str(row["venue"]),
                    market=str(row["market"]),
                    feed=str(row["feed"]),
                    max_slices=int(row["max_slices"]),
                )
                for row in budget_rows
            ),
        )
        rows = raw.get("sources")
        if not isinstance(rows, list) or not rows:
            raise InventoryError("active-demand source registry needs sources")
        specs: list[SourceSpec] = []
        for row in rows:
            expected_row = {
                "source_id", "root", "parser", "path", "glob", "source_policy_id",
                "priority", "ttl_seconds", "default_warmup_limit",
                "bar_freshness_multiplier",
            }
            if not isinstance(row, Mapping) or set(row) != expected_row:
                raise InventoryError("active-demand source fields are incomplete or unknown")
            specs.append(
                SourceSpec(
                    source_id=str(row["source_id"]),
                    root_id=str(row["root"]),
                    parser=str(row["parser"]),
                    path=(str(row["path"]) if row["path"] is not None else None),
                    glob=(str(row["glob"]) if row["glob"] is not None else None),
                    source_policy_id=str(row["source_policy_id"]),
                    priority=int(row["priority"]),
                    ttl_seconds=int(row["ttl_seconds"]),
                    default_warmup_limit=int(row["default_warmup_limit"]),
                    bar_freshness_multiplier=int(row["bar_freshness_multiplier"]),
                )
            )
        if len({item.source_id for item in specs}) != len(specs):
            raise InventoryError("active-demand source ids must be unique")
        return cls(
            revision=int(raw["revision"]),
            owner_registry_root=str(owner["root"]),
            owner_registry_path=str(owner["path"]),
            admission_policy=policy,
            sources=tuple(specs),
            source_path=source_path,
        )


@dataclass(frozen=True, slots=True)
class ActiveDemandInventory:
    revision: int
    requirements: tuple[DataRequirement, ...]
    source_documents: tuple[SourceDocument, ...]
    candidates: tuple[InventoryCandidate, ...]
    exclusions: tuple[InventoryExclusion, ...]
    input_sha256: str

    def manifest_payload(self) -> dict[str, Any]:
        return {
            "schema": MANIFEST_SCHEMA,
            "revision": self.revision,
            "requirements": [
                item.canonical_mapping() for item in self.requirements
            ],
        }

    @property
    def manifest_sha256(self) -> str:
        return _digest(self.manifest_payload())

    def report_payload(self) -> dict[str, Any]:
        candidate_sources: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for candidate in self.candidates:
            candidate_sources[candidate.key].append(
                {
                    "source_refs": list(candidate.source_refs),
                    "source_kind": candidate.source_kind,
                    "detail": candidate.detail,
                }
            )
        return {
            "schema": INVENTORY_SCHEMA,
            "status": "PASS",
            "revision": self.revision,
            "input_sha256": self.input_sha256,
            "manifest_sha256": self.manifest_sha256,
            "requirement_count": len(self.requirements),
            "source_documents": [
                item.canonical_mapping() for item in self.source_documents
            ],
            "requirements": [
                {
                    "requirement": requirement.canonical_mapping(),
                    "sources": sorted(candidate_sources.get(json.dumps(
                        requirement.canonical_mapping(), sort_keys=True, separators=(",", ":")
                    ), []), key=lambda value: (value["source_kind"], value["detail"])),
                }
                for requirement in self.requirements
            ],
            "exclusions": [item.canonical_mapping() for item in self.exclusions],
        }


@dataclass(frozen=True, slots=True)
class ProviderAdmissionRow:
    requirement_id: str
    consumer_id: str
    venue: str
    market: str
    product_type: str
    native_symbol: str
    feed: str
    interval: str | None
    instrument_uid: str | None
    instrument_id: str | None
    capability: str
    state: str
    reason: str | None

    def canonical_mapping(self) -> dict[str, Any]:
        return {
            "requirement_id": self.requirement_id,
            "consumer_id": self.consumer_id,
            "venue": self.venue,
            "market": self.market,
            "product_type": self.product_type,
            "native_symbol": self.native_symbol,
            "feed": self.feed,
            "interval": self.interval,
            "instrument_uid": self.instrument_uid,
            "instrument_id": self.instrument_id,
            "capability": self.capability,
            "state": self.state,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class ProviderAdmission:
    inventory_sha256: str
    metadata_sha256: Mapping[str, str]
    rows: tuple[ProviderAdmissionRow, ...]
    records: Mapping[tuple[str, str, str, str], InstrumentRecord] = field(
        repr=False,
        compare=False,
    )

    @property
    def passed(self) -> bool:
        return all(item.state == "ADMITTED" for item in self.rows)

    def report_payload(self) -> dict[str, Any]:
        return {
            "schema": "qdl.v2.active-demand-provider-admission.v1",
            "status": "PASS" if self.passed else "FAIL",
            "provenance": "REAL_PROVIDER_METADATA_READ_ONLY",
            "inventory_sha256": self.inventory_sha256,
            "metadata_sha256": dict(sorted(self.metadata_sha256.items())),
            "row_count": len(self.rows),
            "pass_count": sum(item.state == "ADMITTED" for item in self.rows),
            "failure_count": sum(item.state != "ADMITTED" for item in self.rows),
            "rows": [item.canonical_mapping() for item in self.rows],
        }


@dataclass(frozen=True, slots=True)
class DemandInventoryReadiness:
    requirement_id: str
    consumer_id: str
    venue: str
    market: str
    product_type: str
    native_symbol: str
    feed: str
    interval: str | None
    state: DemandState
    execution_eligible: bool
    reason: str | None

    def canonical_mapping(self) -> dict[str, Any]:
        return {
            "requirement_id": self.requirement_id,
            "consumer_id": self.consumer_id,
            "venue": self.venue,
            "market": self.market,
            "product_type": self.product_type,
            "native_symbol": self.native_symbol,
            "feed": self.feed,
            "interval": self.interval,
            "state": self.state.value,
            "execution_eligible": self.execution_eligible,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class AdmissionBudgetUsage:
    venue: str
    market: str
    feed: str
    limit: int
    selected_slices: int

    def canonical_mapping(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "market": self.market,
            "feed": self.feed,
            "limit": self.limit,
            "selected_slices": self.selected_slices,
        }


@dataclass(frozen=True, slots=True)
class ActiveDemandConvergence:
    inventory_sha256: str
    policy: AdmissionPolicy
    topology: DemandTopology
    readiness: tuple[DemandInventoryReadiness, ...]
    budget_usage: tuple[AdmissionBudgetUsage, ...]
    selected_slice_count: int

    @property
    def passed(self) -> bool:
        return all(item.state is DemandState.WARMING for item in self.readiness)

    def report_payload(self) -> dict[str, Any]:
        return {
            "schema": "qdl.v2.active-demand-convergence.v1",
            "status": "PASS" if self.passed else "FAIL",
            "provenance": "DARK_CONTROL_PLANE_PLAN",
            "inventory_sha256": self.inventory_sha256,
            "selected_slice_count": self.selected_slice_count,
            "topology": {
                "subscription_count": len(self.topology.subscriptions),
                "connection_count": self.topology.connection_count,
                "service_role_count": self.topology.service_role_count,
                "runtime_roles": [list(item) for item in self.topology.runtime_roles],
                "provisioning_required_count": len(self.topology.provisioning_required),
            },
            "budget": {
                "max_total_slices": self.policy.max_total_slices,
                "max_subscriptions_per_connection": self.policy.max_subscriptions_per_connection,
                "usage": [item.canonical_mapping() for item in self.budget_usage],
            },
            "readiness": [item.canonical_mapping() for item in self.readiness],
        }


def _digest(value: Any) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _safe_child(root: Path, relative_path: str) -> Path:
    value = Path(relative_path)
    if value.is_absolute() or ".." in value.parts:
        raise InventoryError("source path must be a safe relative path")
    resolved_root = root.resolve()
    resolved = (resolved_root / value).resolve()
    if resolved_root not in resolved.parents and resolved != resolved_root:
        raise InventoryError("source path escapes its configured root")
    return resolved


def _load_mapping(path: Path) -> Mapping[str, Any]:
    text = path.read_text(encoding="utf-8")
    value = json.loads(text) if path.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(value, Mapping):
        raise InventoryError(f"{path} must contain an object")
    return value


def _environment(raw: Any) -> dict[str, str]:
    if isinstance(raw, Mapping):
        return {str(key): str(value) for key, value in raw.items() if value is not None}
    if isinstance(raw, list):
        result: dict[str, str] = {}
        for item in raw:
            if not isinstance(item, str) or "=" not in item:
                raise InventoryError("compose environment list requires KEY=VALUE entries")
            key, value = item.split("=", 1)
            result[key] = value
        return result
    if raw is None:
        return {}
    raise InventoryError("compose environment must be a mapping or KEY=VALUE list")


def _truthy(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise InventoryError("boolean environment value is invalid")


def _interval_ms(interval: str) -> int:
    try:
        return _INTERVAL_MILLISECONDS[interval]
    except KeyError as error:
        raise InventoryError(f"unsupported declared interval: {interval}") from error


def _symbol_list(value: Any, *, field: str) -> tuple[str, ...]:
    if isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, list):
        values = value
    else:
        raise InventoryError(f"{field} must be a comma-separated string or list")
    result = tuple(sorted({str(item).strip().upper() for item in values if str(item).strip()}))
    if not result:
        raise InventoryError(f"{field} cannot be empty")
    return result


def _maxlen(value: Any) -> int | None:
    values: list[int] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in {"maxlen", "warmup_limit", "lookback_limit", "bootstrap_limit"}:
                try:
                    number = int(item)
                except (TypeError, ValueError) as error:
                    raise InventoryError(f"{key} must be an integer") from error
                if number > 0:
                    values.append(number)
            values.extend(item_value for item_value in (_maxlen(item),) if item_value is not None)
    elif isinstance(value, list):
        for item in value:
            values.extend(item_value for item_value in (_maxlen(item),) if item_value is not None)
    return max(values) if values else None


def _document_ref(root_id: str, root: Path, path: Path) -> str:
    return f"{root_id}:{path.resolve().relative_to(root.resolve()).as_posix()}"


class ActiveDemandCompiler:
    """Compiles declared alpha/Trading System demand without runtime mutation."""

    def __init__(
        self,
        *,
        registry: ActiveDemandSourceRegistry,
        repository_root: str | Path,
        execution_alpha_root: str | Path,
        trading_system_root: str | Path,
    ) -> None:
        self.registry = registry
        self.roots = {
            "repository": Path(repository_root).resolve(),
            "execution_alpha": Path(execution_alpha_root).resolve(),
            "trading_system": Path(trading_system_root).resolve(),
        }
        if any(not root.is_dir() for root in self.roots.values()):
            raise InventoryError("each configured inventory root must exist")
        self._documents: dict[str, SourceDocument] = {}
        self._document_paths: dict[Path, str] = {}
        self._exclusions: list[InventoryExclusion] = []
        self._owner_venues: dict[str, frozenset[str]] = {}

    def compile(self) -> ActiveDemandInventory:
        self._load_owner_registry()
        candidates: list[InventoryCandidate] = []
        for source in self.registry.sources:
            root = self._root(source.root_id)
            paths = self._source_paths(source, root)
            if not paths:
                raise InventoryError(f"source {source.source_id} has no declared files")
            for path in paths:
                document_ref = self._record_document(source.source_id, source.root_id, root, path)
                if source.parser == "ALPHA_COMPOSE_V1":
                    candidates.extend(self._compile_alpha_compose(source, root, path, document_ref))
                elif source.parser == "PRODUCTION_DEMAND_V1":
                    candidates.extend(self._compile_production_demand(source, path, document_ref))
                else:  # guarded by SourceSpec; kept fail-closed for future changes.
                    raise InventoryError(f"unsupported source parser: {source.parser}")
        merged: dict[str, list[InventoryCandidate]] = defaultdict(list)
        for candidate in candidates:
            merged[candidate.key].append(candidate)
        requirements = tuple(
            sorted(
                (items[0].requirement for _, items in merged.items()),
                key=lambda item: item.requirement_id,
            )
        )
        if not requirements:
            raise InventoryError("active demand inventory has no Binance/OKX requirements")
        documents = tuple(sorted(self._documents.values(), key=lambda item: (item.root_id, item.relative_path)))
        input_sha256 = _digest(
            {
                "registry_revision": self.registry.revision,
                "documents": [item.canonical_mapping() for item in documents],
                "requirements": [item.canonical_mapping() for item in requirements],
                "exclusions": [item.canonical_mapping() for item in sorted(
                    self._exclusions,
                    key=lambda item: (item.source_id, item.relative_path, item.owner_id or "", item.code),
                )],
            }
        )
        return ActiveDemandInventory(
            revision=self.registry.revision,
            requirements=requirements,
            source_documents=documents,
            candidates=tuple(candidates),
            exclusions=tuple(sorted(
                self._exclusions,
                key=lambda item: (item.source_id, item.relative_path, item.owner_id or "", item.code),
            )),
            input_sha256=input_sha256,
        )

    def _root(self, root_id: str) -> Path:
        try:
            return self.roots[root_id]
        except KeyError as error:
            raise InventoryError(f"unknown configured root: {root_id}") from error

    def _source_paths(self, source: SourceSpec, root: Path) -> tuple[Path, ...]:
        if source.path is not None:
            path = _safe_child(root, source.path)
            if not path.is_file():
                raise InventoryError(f"declared source file is missing: {path}")
            return (path,)
        assert source.glob is not None
        pattern = Path(source.glob)
        if pattern.is_absolute() or ".." in pattern.parts:
            raise InventoryError("source glob must be a safe relative path")
        return tuple(sorted(path.resolve() for path in root.glob(source.glob) if path.is_file()))

    def _record_document(self, source_id: str, root_id: str, root: Path, path: Path) -> str:
        path = path.resolve()
        existing = self._document_paths.get(path)
        if existing is not None:
            return existing
        relative = path.relative_to(root.resolve()).as_posix()
        ref = _document_ref(root_id, root, path)
        content = path.read_bytes()
        self._documents[ref] = SourceDocument(
            source_id=source_id,
            root_id=root_id,
            relative_path=relative,
            sha256=sha256(content).hexdigest(),
            byte_count=len(content),
        )
        self._document_paths[path] = ref
        return ref

    def _load_owner_registry(self) -> None:
        root = self._root(self.registry.owner_registry_root)
        path = _safe_child(root, self.registry.owner_registry_path)
        document_ref = self._record_document("trading-alpha-owner-registry", self.registry.owner_registry_root, root, path)
        del document_ref
        raw = _load_mapping(path)
        rows = raw.get("alphas")
        if not isinstance(rows, list) or not rows:
            raise InventoryError("Trading System alpha registry has no alphas")
        owners: dict[str, frozenset[str]] = {}
        for row in rows:
            if not isinstance(row, Mapping):
                raise InventoryError("Trading System alpha registry row must be a mapping")
            alpha_id = str(row.get("alpha_id") or "").strip()
            venues = row.get("allowed_venues")
            if not alpha_id or not isinstance(venues, list) or not venues:
                raise InventoryError("Trading System alpha registry row is incomplete")
            normalized = frozenset(str(value).upper() for value in venues if str(value).strip())
            if not normalized or alpha_id in owners:
                raise InventoryError("Trading System alpha registry contains an invalid duplicate")
            owners[alpha_id] = normalized
        self._owner_venues = owners

    def _compile_alpha_compose(
        self,
        source: SourceSpec,
        root: Path,
        compose_path: Path,
        document_ref: str,
    ) -> list[InventoryCandidate]:
        raw = _load_mapping(compose_path)
        services = raw.get("services")
        if not isinstance(services, Mapping):
            raise InventoryError(f"compose source has no services mapping: {compose_path}")
        candidates: list[InventoryCandidate] = []
        alpha_root = compose_path.parent
        for service_name, service in sorted(services.items()):
            if not isinstance(service, Mapping):
                raise InventoryError(f"compose service must be a mapping: {service_name}")
            environment = _environment(service.get("environment"))
            alpha_id = environment.get("TRADING_ALPHA_ID", "").strip()
            if not alpha_id:
                continue
            venue = environment.get("TRADING_VENUE", "").strip().upper()
            if not venue:
                raise InventoryError(f"compose service {service_name} has TRADING_ALPHA_ID but no TRADING_VENUE")
            if venue not in _TARGET_VENUES:
                self._exclusions.append(InventoryExclusion(
                    source_id=source.source_id,
                    relative_path=compose_path.relative_to(root).as_posix(),
                    owner_id=alpha_id,
                    code="OUTSIDE_PHASE11_BINANCE_OKX_SCOPE",
                    detail=f"service={service_name} venue={venue}",
                ))
                continue
            self._validate_owner(alpha_id, venue, compose_path)
            candidates.extend(
                self._compile_alpha_service(
                    source=source,
                    root=root,
                    compose_path=compose_path,
                    alpha_root=alpha_root,
                    service_name=str(service_name),
                    environment=environment,
                    document_ref=document_ref,
                )
            )
        return candidates

    def _validate_owner(self, alpha_id: str, venue: str, path: Path) -> None:
        allowed = self._owner_venues.get(alpha_id)
        if allowed is None:
            raise InventoryError(f"compose alpha_id is absent from Trading System registry: {alpha_id}")
        if venue not in allowed:
            raise InventoryError(
                f"compose venue is not allowed by Trading System registry: {alpha_id}/{venue} ({path})"
            )

    def _compile_alpha_service(
        self,
        *,
        source: SourceSpec,
        root: Path,
        compose_path: Path,
        alpha_root: Path,
        service_name: str,
        environment: Mapping[str, str],
        document_ref: str,
    ) -> list[InventoryCandidate]:
        alpha_id = environment["TRADING_ALPHA_ID"].strip()
        declared_venue = environment["TRADING_VENUE"].strip().upper()
        references = [document_ref]
        if environment.get("DEEP_MOMENTUM_CONFIG"):
            symbols, interval, warmup, refs, market = self._deep_momentum_config(alpha_root, environment)
            references.extend(refs)
            venue, market, product_type = self._identity_from_contract(declared_venue, market)
            return self._bar_candidates(
                source=source,
                alpha_id=alpha_id,
                venue=venue,
                market=market,
                product_type=product_type,
                symbols=symbols,
                interval=interval,
                warmup=warmup,
                references=references,
                detail=f"compose={service_name}; profile=deep_momentum",
                include_trade=False,
            )
        if environment.get("BASIS_ARB_CONFIG"):
            venue, market, product_type = self._identity_from_contract(declared_venue, "USDM")
            return self._basis_arb_candidates(
                source=source,
                alpha_id=alpha_id,
                venue=venue,
                market=market,
                product_type=product_type,
                alpha_root=alpha_root,
                environment=environment,
                references=references,
                detail=f"compose={service_name}; profile=basis_arb",
            )
        interval = str(environment.get("ALPHA_INTERVAL") or "").strip()
        if not interval:
            raise InventoryError(f"compose service {service_name} is missing ALPHA_INTERVAL")
        venue, market, product_type, identity_detail, identity_refs = self._identity_from_environment(
            environment
        )
        references.extend(identity_refs)
        symbols, companion_refs = self._service_symbols(
            alpha_root,
            alpha_id,
            interval,
            environment,
        )
        references.extend(companion_refs)
        warmup = self._service_warmup(alpha_root, environment, source.default_warmup_limit, companion_refs)
        include_trade = _truthy(environment.get("ALPHA_ENABLE_REALTIME_STREAM"), True)
        return self._bar_candidates(
            source=source,
            alpha_id=alpha_id,
            venue=venue,
            market=market,
            product_type=product_type,
            symbols=symbols,
            interval=interval,
            warmup=warmup,
            references=references,
            detail=(
                f"compose={service_name}; {identity_detail}; "
                f"realtime_trade={str(include_trade).lower()}"
            ),
            include_trade=include_trade,
            freshness_override_ms=self._freshness_override(environment),
        )

    def _identity_from_environment(
        self, environment: Mapping[str, str]
    ) -> tuple[str, str, str, str, list[str]]:
        venue = environment.get("TRADING_VENUE", "").strip().upper()
        contract = str(
            environment.get("ALPHA_CONTRACT_TYPE")
            or environment.get("DATA_LAYER_BINANCE_MARKET")
            or ""
        ).strip().upper()
        if contract:
            market_venue, market, product_type = self._identity_from_contract(venue, contract)
            return market_venue, market, product_type, "identity=explicit_contract", []

        # Legacy portfolio runners call the generic Binance history route with
        # ``market=auto``.  That route is deterministic: USD-M is attempted
        # before Spot.  For this V2 inventory we bind the declared Binance
        # futures owner to that first authoritative market and record both
        # code paths as hashed sources.  Provider admission still fails closed
        # if any declared symbol is not an active USD-M instrument; V2 never
        # carries the legacy silent Spot fallback forward.
        legacy_market = str(environment.get("ALPHA_MARKET") or "").strip().lower()
        provider = str(environment.get("ALPHA_PROVIDER") or "").strip().lower()
        if venue == "BINANCE" and legacy_market in {"crypto", "binance"} and provider in {"", "binance"}:
            repository_root = self._root("repository")
            execution_root = self._root("execution_alpha")
            route = _safe_child(repository_root, "app/providers/binance/rest.py")
            runtime = _safe_child(
                execution_root,
                "runtime/app/alpha_runtime/legacy/handler.py",
            )
            if not route.is_file() or not runtime.is_file():
                raise InventoryError("legacy Binance auto-market semantics source is missing")
            return (
                "BINANCE",
                "USDM",
                "PERPETUAL",
                "identity=legacy_auto_usdm_first",
                [
                    self._record_document(
                        "legacy-binance-auto-market-semantics",
                        "repository",
                        repository_root,
                        route,
                    ),
                    self._record_document(
                        "legacy-alpha-runtime-market-semantics",
                        "execution_alpha",
                        execution_root,
                        runtime,
                    ),
                ],
            )
        raise InventoryError(
            "declared venue/contract identity is not explicit or supported by recorded legacy semantics: "
            f"{venue}/{contract or legacy_market or 'missing'}"
        )

    @staticmethod
    def _identity_from_contract(venue: str, contract: str) -> tuple[str, str, str]:
        mapping = {
            ("BINANCE", "USDM"): ("BINANCE", "USDM", "PERPETUAL"),
            ("BINANCE", "PERPETUAL"): ("BINANCE", "USDM", "PERPETUAL"),
            ("BINANCE", "SPOT"): ("BINANCE", "SPOT", "SPOT"),
            ("OKX", "SWAP"): ("OKX", "SWAP", "PERPETUAL"),
            ("OKX", "PERPETUAL"): ("OKX", "SWAP", "PERPETUAL"),
            ("OKX", "SPOT"): ("OKX", "SPOT", "SPOT"),
        }
        try:
            return mapping[(venue, contract)]
        except KeyError as error:
            raise InventoryError(
                f"declared venue/contract identity is not explicit or unsupported: {venue}/{contract}"
            ) from error

    def _service_symbols(
        self,
        alpha_root: Path,
        alpha_id: str,
        interval: str,
        environment: Mapping[str, str],
    ) -> tuple[tuple[str, ...], list[str]]:
        if environment.get("ALPHA_SYMBOLS"):
            return _symbol_list(environment["ALPHA_SYMBOLS"], field="ALPHA_SYMBOLS"), []
        refs: list[str] = []
        if environment.get("ALPHA_SYMBOLS_FILE"):
            path = self._app_path(alpha_root, environment["ALPHA_SYMBOLS_FILE"])
            refs.append(self._record_external_document("execution_alpha", alpha_root, path))
            raw = json.loads(path.read_text(encoding="utf-8"))
            return _symbol_list(raw, field="ALPHA_SYMBOLS_FILE"), refs
        deployment = alpha_root / "config" / "deployment.yaml"
        if deployment.is_file():
            refs.append(self._record_external_document("execution_alpha", alpha_root, deployment))
            raw = _load_mapping(deployment)
            for row in raw.get("deployments", []) if isinstance(raw.get("deployments"), list) else []:
                if isinstance(row, Mapping) and str(row.get("alpha_id") or "") == alpha_id:
                    symbols = row.get("symbols")
                    if symbols:
                        return _symbol_list(symbols, field=f"deployment symbols for {alpha_id}"), refs
            strategy = raw.get("strategy")
            if isinstance(strategy, Mapping) and str(strategy.get("alpha_id") or "") == alpha_id:
                symbols = strategy.get("symbols")
                if symbols:
                    return _symbol_list(symbols, field=f"strategy symbols for {alpha_id}"), refs
                universe_source = str(strategy.get("universe_source") or "").strip()
                if universe_source:
                    universe = _safe_child(alpha_root, universe_source)
                    refs.append(self._record_external_document("execution_alpha", alpha_root, universe))
                    return _symbol_list(json.loads(universe.read_text(encoding="utf-8")), field="universe_source"), refs
        config = alpha_root / "config.yaml"
        if config.is_file():
            refs.append(self._record_external_document("execution_alpha", alpha_root, config))
            raw = _load_mapping(config)
            symbols = self._symbols_from_interval_group(raw, alpha_id=alpha_id, interval=interval)
            if symbols is not None:
                return symbols, refs
        raise InventoryError(f"could not resolve declared symbols for alpha {alpha_id}")

    @staticmethod
    def _symbols_from_interval_group(
        raw: Mapping[str, Any], *, alpha_id: str, interval: str
    ) -> tuple[str, ...] | None:
        """Read the shared ``symbols_<interval>`` alpha config convention.

        This is intentionally a small generic convention rather than a list of
        alpha names.  A group is accepted only when exactly one top-level
        strategy mapping declares the interval.  A second matching group is an
        ambiguity in source declarations and must be corrected by its owner.
        """
        suffix = interval.lower()
        candidates: list[tuple[str, Any]] = []
        for group_name, group in raw.items():
            if not isinstance(group, Mapping):
                continue
            value = group.get(f"symbols_{suffix}")
            if value:
                candidates.append((str(group_name), value))
        if not candidates:
            return None
        if len(candidates) != 1:
            names = ",".join(sorted(name for name, _ in candidates))
            raise InventoryError(
                f"config.yaml symbol groups are ambiguous for {alpha_id}/{interval}: {names}"
            )
        group_name, symbols = candidates[0]
        return _symbol_list(symbols, field=f"config.yaml {group_name}.symbols_{suffix}")

    def _service_warmup(
        self,
        alpha_root: Path,
        environment: Mapping[str, str],
        default: int,
        refs: list[str],
    ) -> int:
        explicit = environment.get("ALPHA_MAXLEN") or environment.get("DATA_LAYER_WARMUP_REQUEST_LIMIT")
        values = [int(explicit)] if explicit else []
        for path in (alpha_root / "config.yaml", alpha_root / "config" / "deployment.yaml"):
            if path.is_file():
                ref = self._record_external_document("execution_alpha", alpha_root, path)
                if ref not in refs:
                    refs.append(ref)
                detected = _maxlen(_load_mapping(path))
                if detected is not None:
                    values.append(detected)
        return max(values) if values else default

    def _deep_momentum_config(
        self, alpha_root: Path, environment: Mapping[str, str]
    ) -> tuple[tuple[str, ...], str, int, list[str], str]:
        path = self._app_path(alpha_root, environment["DEEP_MOMENTUM_CONFIG"])
        refs = [self._record_external_document("execution_alpha", alpha_root, path)]
        raw = _load_mapping(path)
        data = raw.get("data_layer")
        if not isinstance(data, Mapping):
            raise InventoryError("deep momentum config has no data_layer section")
        interval = str(data.get("interval") or "").strip()
        universe_path = str(data.get("universe_symbols_path") or "").strip()
        if not interval or not universe_path:
            raise InventoryError("deep momentum data_layer interval/universe is required")
        universe = self._app_path(alpha_root, universe_path)
        refs.append(self._record_external_document("execution_alpha", alpha_root, universe))
        provider = str(data.get("provider") or "").strip().upper()
        market = str(data.get("market") or "").strip().upper()
        if provider != "BINANCE" or not market:
            raise InventoryError("deep momentum provider/market declaration is incomplete")
        return (
            _symbol_list(json.loads(universe.read_text(encoding="utf-8")), field="deep momentum universe"),
            interval,
            int(data.get("lookback_limit") or 0),
            refs,
            market,
        )

    def _basis_arb_candidates(
        self,
        *,
        source: SourceSpec,
        alpha_id: str,
        venue: str,
        market: str,
        product_type: str,
        alpha_root: Path,
        environment: Mapping[str, str],
        references: list[str],
        detail: str,
    ) -> list[InventoryCandidate]:
        path = self._app_path(alpha_root, environment["BASIS_ARB_CONFIG"])
        references.append(self._record_external_document("execution_alpha", alpha_root, path))
        raw = _load_mapping(path)
        data = raw.get("data")
        if not isinstance(data, Mapping):
            raise InventoryError("basis arb config has no data section")
        perp = str(data.get("perp_symbol") or "").strip().upper()
        base = str(data.get("quarterly_base_symbol") or "").strip().upper()
        contract_type = str(data.get("contract_type") or "").strip().upper()
        interval = str(data.get("interval") or data.get("resolution") or "").strip()
        runtime = raw.get("runtime") if isinstance(raw.get("runtime"), Mapping) else {}
        warmup = int(runtime.get("min_history_rows") or data.get("bootstrap_limit") or 0)
        depth = int(data.get("depth_limit") or 0)
        if not perp or not base or contract_type not in {"CURRENT_QUARTER", "NEXT_QUARTER"} or not interval or warmup <= 0:
            raise InventoryError("basis arb data declaration is incomplete")
        common = dict(
            consumer_id=alpha_id,
            purpose=DemandPurpose.ALPHA,
            source_policy_id=source.source_policy_id,
            warmup_limit=warmup,
            max_freshness_ms=self._bar_freshness(interval, source),
            priority=source.priority,
            ttl_seconds=source.ttl_seconds,
            require_live=True,
            execution_grade=False,
            configuration_revision=self.registry.revision,
        )
        perp_selector = UniverseSelector(
            selector_id=f"{alpha_id}:perpetual:{perp}",
            kind=UniverseSelectorKind.EXPLICIT,
            venue=venue,
            market=market,
            product_type=product_type,
            native_symbols=(perp,),
        )
        family = f"{base.removesuffix('USDT')}-USDT"
        quarterly_selector = UniverseSelector(
            selector_id=f"{alpha_id}:continuous:{family}:{contract_type}",
            kind=UniverseSelectorKind.CONTINUOUS,
            venue=venue,
            market=market,
            product_type="FUTURE",
            continuous_family=family,
            continuous_roll_policy=contract_type,
        )
        requirements = [
            DataRequirement(universe=perp_selector, feed=DemandFeed.BAR, interval=interval, require_final_bars=True, **common),
            DataRequirement(universe=quarterly_selector, feed=DemandFeed.BAR, interval=interval, require_final_bars=True, **common),
            DataRequirement(universe=perp_selector, feed=DemandFeed.FUNDING_RATE, max_freshness_ms=86_400_000, **{key: value for key, value in common.items() if key not in {"max_freshness_ms"}}),
            DataRequirement(universe=quarterly_selector, feed=DemandFeed.BASIS, interval="1d", max_freshness_ms=86_400_000, **{key: value for key, value in common.items() if key not in {"max_freshness_ms"}}),
        ]
        if bool(data.get("include_depth", False)):
            if depth <= 0:
                raise InventoryError("basis arb include_depth requires depth_limit")
            for selector in (perp_selector, quarterly_selector):
                requirements.append(DataRequirement(
                    universe=selector,
                    feed=DemandFeed.BOOK_SNAPSHOT,
                    max_freshness_ms=3_600_000,
                    depth_levels=depth,
                    require_live=False,
                    **{key: value for key, value in common.items() if key not in {"max_freshness_ms", "require_live"}},
                ))
        return [
            InventoryCandidate(
                requirement=item,
                source_refs=tuple(sorted(set(references))),
                source_kind="BASIS_ARB_CONFIG_V1",
                detail=detail,
            )
            for item in requirements
        ]

    def _bar_candidates(
        self,
        *,
        source: SourceSpec,
        alpha_id: str,
        venue: str,
        market: str,
        product_type: str,
        symbols: tuple[str, ...],
        interval: str,
        warmup: int,
        references: list[str],
        detail: str,
        include_trade: bool,
        freshness_override_ms: int | None = None,
    ) -> list[InventoryCandidate]:
        if warmup <= 0:
            raise InventoryError(f"alpha {alpha_id} has no positive warmup limit")
        selected = self._target_symbols(venue, market, symbols, alpha_id, detail)
        selector = UniverseSelector(
            selector_id=f"{alpha_id}:{venue}:{market}:{product_type}:{interval}",
            kind=UniverseSelectorKind.EXPLICIT,
            venue=venue,
            market=market,
            product_type=product_type,
            native_symbols=selected,
        )
        common = dict(
            consumer_id=alpha_id,
            purpose=DemandPurpose.ALPHA,
            source_policy_id=source.source_policy_id,
            warmup_limit=warmup,
            priority=source.priority,
            ttl_seconds=source.ttl_seconds,
            require_live=True,
            execution_grade=False,
            configuration_revision=self.registry.revision,
        )
        rows = [DataRequirement(
            universe=selector,
            feed=DemandFeed.BAR,
            interval=interval,
            max_freshness_ms=freshness_override_ms or self._bar_freshness(interval, source),
            require_final_bars=True,
            **common,
        )]
        if include_trade:
            rows.append(DataRequirement(
                universe=selector,
                feed=DemandFeed.TRADE,
                max_freshness_ms=15_000,
                require_final_bars=False,
                **common,
            ))
        return [
            InventoryCandidate(
                requirement=item,
                source_refs=tuple(sorted(set(references))),
                source_kind="ALPHA_COMPOSE_V1",
                detail=detail,
            )
            for item in rows
        ]

    def _target_symbols(
        self,
        venue: str,
        market: str,
        symbols: tuple[str, ...],
        alpha_id: str,
        detail: str,
    ) -> tuple[str, ...]:
        # This is an explicitly constrained current alpha domain: every active
        # Binance USD-M strategy declares USDT-margined symbols.  Mixed DNSE
        # deployments share config files, so their non-USDT values are recorded
        # as excluded rather than incorrectly assigned to Binance.
        if venue == "BINANCE" and market == "USDM":
            accepted = tuple(symbol for symbol in symbols if re.fullmatch(r"[A-Z0-9]+USDT(?:_[0-9]{6})?", symbol))
            rejected = sorted(set(symbols) - set(accepted))
            for symbol in rejected:
                self._exclusions.append(InventoryExclusion(
                    source_id="alpha-compose",
                    relative_path="declared-symbol-filter",
                    owner_id=alpha_id,
                    code="NON_USDM_SYMBOL_IN_MIXED_DECLARATION",
                    detail=f"symbol={symbol}; {detail}",
                ))
            if not accepted:
                raise InventoryError(f"alpha {alpha_id} has no symbols in declared Binance USD-M scope")
            return accepted
        return symbols

    def _bar_freshness(self, interval: str, source: SourceSpec) -> int:
        return _interval_ms(interval) * source.bar_freshness_multiplier

    @staticmethod
    def _freshness_override(environment: Mapping[str, str]) -> int | None:
        value = environment.get("ALPHA_CANDLE_MAX_LAG_SECONDS")
        if value is None:
            return None
        try:
            seconds = int(value)
        except ValueError as error:
            raise InventoryError("ALPHA_CANDLE_MAX_LAG_SECONDS must be an integer") from error
        if seconds <= 0:
            raise InventoryError("ALPHA_CANDLE_MAX_LAG_SECONDS must be positive")
        return seconds * 1_000

    def _app_path(self, alpha_root: Path, container_path: str) -> Path:
        value = Path(container_path)
        if not value.is_absolute() or value.parts[:2] != ("/", "app"):
            raise InventoryError(f"unsupported alpha container path: {container_path}")
        relative = Path(*value.parts[2:])
        return _safe_child(alpha_root, relative.as_posix())

    def _record_external_document(self, root_id: str, alpha_root: Path, path: Path) -> str:
        root = self._root(root_id)
        try:
            path.resolve().relative_to(root)
        except ValueError:
            # This compiler is intentionally portable: a test may provide a
            # standalone alpha root.  Record it under the configured alpha root
            # while preserving its relative source path.
            root = alpha_root
        return self._record_document("alpha-compose-dependency", root_id, root, path)

    def _compile_production_demand(
        self, source: SourceSpec, path: Path, document_ref: str
    ) -> list[InventoryCandidate]:
        raw = _load_mapping(path)
        if raw.get("schema") != "qdl.v2.production-demand.v1":
            raise InventoryError("production demand source schema is invalid")
        consumers = raw.get("consumers")
        if not isinstance(consumers, list) or not consumers:
            raise InventoryError("production demand source has no consumers")
        values: list[InventoryCandidate] = []
        for consumer in consumers:
            if not isinstance(consumer, Mapping):
                raise InventoryError("production demand consumer must be a mapping")
            owner = str(consumer.get("consumer_id") or "").strip()
            grade = str(consumer.get("consumer_grade") or "").strip().upper()
            requirements = consumer.get("requirements")
            if not owner or grade not in {"EXECUTION", "ALPHA", "RESEARCH"} or not isinstance(requirements, list):
                raise InventoryError("production demand consumer is incomplete")
            for row in requirements:
                if not isinstance(row, Mapping):
                    raise InventoryError("production demand requirement must be a mapping")
                venue = str(row.get("venue") or "").strip().upper()
                if venue not in _TARGET_VENUES:
                    continue
                market = str(row.get("market") or "").strip().upper()
                product_type = str(row.get("product_type") or "").strip().upper()
                symbol = str(row.get("native_symbol") or "").strip().upper()
                feed = DemandFeed(str(row.get("feed") or "").strip().upper())
                interval = str(row["interval"]).strip() if row.get("interval") is not None else None
                if feed is DemandFeed.BAR and not interval:
                    raise InventoryError("production BAR demand needs interval")
                purpose = DemandPurpose.EXECUTION if grade == "EXECUTION" else DemandPurpose(grade)
                freshness = (
                    self._bar_freshness(interval, source)
                    if feed is DemandFeed.BAR and interval
                    else 5_000 if feed is DemandFeed.QUOTE else 15_000
                )
                requirement = DataRequirement(
                    consumer_id=owner,
                    purpose=purpose,
                    universe=UniverseSelector(
                        selector_id=f"{owner}:{venue}:{market}:{product_type}:{symbol}:{feed.value}:{interval or 'point'}",
                        kind=UniverseSelectorKind.EXPLICIT,
                        venue=venue,
                        market=market,
                        product_type=product_type,
                        native_symbols=(symbol,),
                    ),
                    feed=feed,
                    source_policy_id=str(row.get("source_policy_id") or source.source_policy_id),
                    interval=interval,
                    warmup_limit=0,
                    max_freshness_ms=freshness,
                    priority=source.priority,
                    ttl_seconds=source.ttl_seconds,
                    require_final_bars=feed is DemandFeed.BAR,
                    require_live=True,
                    execution_grade=purpose is DemandPurpose.EXECUTION,
                    configuration_revision=self.registry.revision,
                )
                values.append(InventoryCandidate(
                    requirement=requirement,
                    source_refs=(document_ref,),
                    source_kind="PRODUCTION_DEMAND_V1",
                    detail=f"consumer_grade={grade}",
                ))
        return values


def _record_lookup(records: Iterable[InstrumentRecord]) -> dict[tuple[str, str, str, str], InstrumentRecord]:
    result: dict[tuple[str, str, str, str], InstrumentRecord] = {}
    for item in records:
        key = (
            item.identity.venue,
            item.identity.market,
            item.identity.product_type.value,
            item.native_symbol,
        )
        if key in result:
            raise InventoryError(f"provider metadata contains duplicate instrument identity: {key}")
        result[key] = item
    return result


def _selected_metadata_payload(
    venue: str,
    market: str,
    payload: Any,
    requirements: tuple[DataRequirement, ...] | None,
) -> Any:
    """Limit parsing to declared selectors without mutating provider provenance.

    Provider instrument endpoints enumerate far more products than the active
    manifest.  An unrelated prelisting or incomplete provider row must not
    poison a demanded slice; a selected row remains strictly parsed and fails
    closed.  The caller still hashes the complete authentic response before
    this narrowing, so evidence retains the exact metadata provenance.
    """
    if requirements is None:
        return payload
    selected = tuple(
        item for item in requirements
        if item.universe.venue == venue and item.universe.market == market
    )
    if not selected:
        return payload
    explicit_symbols = {
        symbol
        for item in selected
        if item.universe.kind is UniverseSelectorKind.EXPLICIT
        for symbol in item.universe.native_symbols
    }
    continuous_families = {
        (str(item.universe.continuous_family or "").upper(), str(item.universe.continuous_roll_policy or "").upper())
        for item in selected
        if item.universe.kind is UniverseSelectorKind.CONTINUOUS
    }

    def selected_row(row: Mapping[str, Any]) -> bool:
        native_symbol = str(row.get("symbol") or row.get("instId") or "").upper()
        if native_symbol in explicit_symbols:
            return True
        if venue == "BINANCE":
            family = f"{str(row.get('baseAsset') or '').upper()}-{str(row.get('quoteAsset') or '').upper()}"
            contract_type = str(row.get("contractType") or "").upper()
        else:
            family = str(row.get("instFamily") or "").upper()
            contract_type = ""
        return (family, contract_type) in continuous_families

    if venue == "BINANCE":
        if not isinstance(payload, Mapping) or not isinstance(payload.get("symbols"), list):
            raise InventoryError(f"{venue}/{market} metadata must contain a symbols list")
        return {
            **payload,
            "symbols": [
                row for row in payload["symbols"]
                if isinstance(row, Mapping) and selected_row(row)
            ],
        }
    if not isinstance(payload, list):
        raise InventoryError(f"{venue}/{market} metadata must be a list")
    return [
        row for row in payload
        if isinstance(row, Mapping) and selected_row(row)
    ]


def parse_provider_metadata(
    payloads: Mapping[tuple[str, str], Any],
    *,
    requirements: Iterable[DataRequirement] | None = None,
) -> tuple[dict[tuple[str, str, str, str], InstrumentRecord], dict[str, str]]:
    """Parse authentic provider metadata captures for active demand admission."""
    selected_requirements = tuple(requirements) if requirements is not None else None
    records: list[InstrumentRecord] = []
    digests: dict[str, str] = {}
    for (venue_raw, market_raw), payload in sorted(payloads.items()):
        venue, market = venue_raw.upper(), market_raw.upper()
        key = f"{venue}:{market}"
        digests[key] = _digest(payload)
        selected_payload = _selected_metadata_payload(
            venue,
            market,
            payload,
            selected_requirements,
        )
        if venue == "BINANCE" and market == "USDM":
            if not isinstance(selected_payload, Mapping):
                raise InventoryError("Binance USD-M metadata capture must be an object")
            if selected_payload["symbols"]:
                records.extend(parse_exchange_info(selected_payload, valid_from_ns=1).records)
        elif venue == "BINANCE" and market == "SPOT":
            if not isinstance(selected_payload, Mapping):
                raise InventoryError("Binance Spot metadata capture must be an object")
            if selected_payload["symbols"]:
                records.extend(parse_spot_exchange_info(selected_payload, valid_from_ns=1).records)
        elif venue == "OKX" and market in {"SWAP", "SPOT"}:
            if not isinstance(selected_payload, list):
                raise InventoryError("OKX metadata capture must be a data list")
            for row in selected_payload:
                if not isinstance(row, Mapping):
                    raise InventoryError("OKX instrument metadata row must be an object")
                record, _ = parse_public_instrument(row, metadata_revision=1, valid_from_ns=1)
                if record.identity.market == market:
                    records.append(record)
        else:
            raise InventoryError(f"provider metadata capture is not supported: {venue}/{market}")
    return _record_lookup(records), digests


def admit_provider_metadata(
    inventory: ActiveDemandInventory,
    payloads: Mapping[tuple[str, str], Any],
    *,
    capabilities: CapabilityRegistry | None = None,
) -> ProviderAdmission:
    records, metadata_sha256 = parse_provider_metadata(
        payloads,
        requirements=inventory.requirements,
    )
    capability_registry = capabilities or CapabilityRegistry.defaults()
    rows: list[ProviderAdmissionRow] = []
    for requirement in inventory.requirements:
        symbols: list[str] = []
        selector = requirement.universe
        if selector.kind is UniverseSelectorKind.EXPLICIT:
            symbols.extend(selector.native_symbols)
        elif selector.kind is UniverseSelectorKind.CONTINUOUS:
            family = str(selector.continuous_family or "").upper()
            policy = str(selector.continuous_roll_policy or "").upper()
            candidates = [
                item for item in records.values()
                if item.identity.venue == selector.venue
                and item.identity.market == selector.market
                and item.identity.product_type is ProductType.FUTURE
                and f"{item.base_asset}-{item.quote_asset}" == family
                and item.attributes.get("contractType", "").upper() == policy
            ]
            if len(candidates) == 1:
                symbols.append(candidates[0].native_symbol)
            elif not candidates:
                rows.append(
                    _admission_row(
                        requirement,
                        None,
                        "MISSING_CONTINUOUS_CONTRACT",
                        f"{family}/{policy}",
                    )
                )
                continue
            else:
                rows.append(
                    _admission_row(
                        requirement,
                        None,
                        "AMBIGUOUS_CONTINUOUS_CONTRACT",
                        f"{family}/{policy}",
                    )
                )
                continue
        else:
            rows.append(_admission_row(requirement, None, "UNSUPPORTED_SELECTOR", selector.kind.value))
            continue
        for native_symbol in symbols:
            record = records.get((selector.venue, selector.market, selector.product_type, native_symbol))
            if record is None:
                rows.append(
                    _admission_row(
                        requirement,
                        None,
                        "MISSING_INSTRUMENT",
                        native_symbol,
                        native_symbol=native_symbol,
                    )
                )
                continue
            capability = capability_registry.resolve(
                venue=record.identity.venue,
                market=record.identity.market,
                product_type=record.identity.product_type.value,
                feed=requirement.feed,
            )
            if capability.availability is not CapabilityAvailability.AVAILABLE:
                rows.append(_admission_row(
                    requirement,
                    record,
                    "UNSUPPORTED_CAPABILITY",
                    capability.constraint or capability.availability.value,
                ))
                continue
            rows.append(_admission_row(requirement, record, "ADMITTED", None))
    return ProviderAdmission(
        inventory_sha256=inventory.manifest_sha256,
        metadata_sha256=metadata_sha256,
        rows=tuple(sorted(rows, key=lambda item: (item.requirement_id, item.native_symbol, item.feed))),
        records=records,
    )


def _admission_requirement_id(
    requirement: DataRequirement,
    native_symbol: str,
) -> str:
    """Return the stable per-instrument identity used by admission evidence."""
    return sha256(
        (
            "qdl-active-demand-admission-v1\0"
            f"{requirement.requirement_id}\0{native_symbol}\0"
            f"{requirement.feed.value}\0{requirement.interval or ''}"
        ).encode()
    ).hexdigest()


def _source_requirement_for_admission(
    inventory: ActiveDemandInventory,
    row: ProviderAdmissionRow,
) -> DataRequirement:
    """Recover one declared selector from its stable provider-admission row."""
    candidates: list[DataRequirement] = []
    for requirement in inventory.requirements:
        selector = requirement.universe
        if (
            selector.venue != row.venue
            or selector.market != row.market
            or selector.product_type != row.product_type
            or requirement.feed.value != row.feed
            or requirement.interval != row.interval
        ):
            continue
        if _admission_requirement_id(requirement, row.native_symbol) == row.requirement_id:
            candidates.append(requirement)
    if len(candidates) != 1:
        raise InventoryError(
            "provider admission row does not resolve to exactly one declared selector: "
            f"{row.requirement_id}"
        )
    return candidates[0]


def _admission_row(
    requirement: DataRequirement,
    record: InstrumentRecord | None,
    state: str,
    reason: str | None,
    *,
    native_symbol: str | None = None,
) -> ProviderAdmissionRow:
    resolved_native_symbol = native_symbol or (
        record.native_symbol if record is not None else (
            requirement.universe.native_symbols[0] if requirement.universe.native_symbols else ""
        )
    )
    requirement_id = _admission_requirement_id(requirement, resolved_native_symbol)
    return ProviderAdmissionRow(
        requirement_id=requirement_id,
        consumer_id=requirement.consumer_id,
        venue=requirement.universe.venue,
        market=requirement.universe.market,
        product_type=requirement.universe.product_type,
        native_symbol=resolved_native_symbol,
        feed=requirement.feed.value,
        interval=requirement.interval,
        instrument_uid=record.instrument_uid if record is not None else None,
        instrument_id=record.instrument_id if record is not None else None,
        capability="AVAILABLE" if state == "ADMITTED" else "NOT_EVALUATED",
        state=state,
        reason=reason,
    )


_PURPOSE_RANK = {
    DemandPurpose.EXECUTION: 0,
    DemandPurpose.ALPHA: 1,
    DemandPurpose.RESEARCH: 2,
    DemandPurpose.OBSERVABILITY: 3,
}


def _physical_slice_key(
    row: ProviderAdmissionRow,
) -> tuple[str, str, str, str, str]:
    return (
        row.venue,
        row.market,
        row.feed,
        row.interval or "",
        row.native_symbol,
    )


def _resolved_requirement_for_admission(
    requirement: DataRequirement,
    row: ProviderAdmissionRow,
    record: InstrumentRecord,
    capability_registry: CapabilityRegistry,
    *,
    demand_revision: int,
) -> ResolvedRequirement:
    """Turn one admitted provider row into a single-instrument demand slice."""
    selector = UniverseSelector(
        selector_id=(
            f"{requirement.universe.selector_id}:resolved:{row.native_symbol}"
        ),
        kind=UniverseSelectorKind.EXPLICIT,
        venue=row.venue,
        market=row.market,
        product_type=row.product_type,
        native_symbols=(row.native_symbol,),
    )
    effective_requirement = replace(requirement, universe=selector)
    capability = capability_registry.resolve(
        venue=row.venue,
        market=row.market,
        product_type=row.product_type,
        feed=effective_requirement.feed,
    )
    if capability.availability is not CapabilityAvailability.AVAILABLE:
        raise InventoryError(
            "provider admission reported ADMITTED with an unavailable capability: "
            f"{row.venue}/{row.market}/{row.product_type}/{row.feed}"
        )
    return ResolvedRequirement(
        requirement=effective_requirement,
        instrument_uid=record.instrument_uid,
        instrument_id=record.instrument_id,
        native_symbol=row.native_symbol,
        capability=capability,
        binding_id=None,
        state=DemandState.WARMING,
        provisioned=False,
        catalog_revision=record.metadata_revision,
        demand_revision=demand_revision,
        consumer_ids=(requirement.consumer_id,),
        effective_priority=requirement.priority,
    )


def _merge_active_slice(
    values: list[ResolvedRequirement],
) -> ResolvedRequirement:
    """Merge owners of one physical subscription without changing its identity."""
    if not values:
        raise InventoryError("cannot merge an empty active demand slice")
    ordered = sorted(
        values,
        key=lambda item: (
            _PURPOSE_RANK[item.requirement.purpose],
            item.requirement.priority,
            item.requirement.requirement_id,
        ),
    )
    first = ordered[0]
    if len({item.requirement.source_policy_id for item in ordered}) != 1:
        raise InventoryError(
            "conflicting source policies for active physical slice: "
            f"{first.requirement.universe.venue}/{first.requirement.universe.market}/"
            f"{first.native_symbol}/{first.requirement.feed.value}/"
            f"{first.requirement.interval or 'point'}"
        )
    if len({item.capability.capability_id for item in ordered}) != 1:
        raise InventoryError("conflicting capabilities for active physical slice")
    if len({item.instrument_uid for item in ordered}) != 1:
        raise InventoryError("physical subscription resolves to multiple instrument identities")
    purposes = {item.requirement.purpose for item in ordered}
    purpose = min(purposes, key=lambda item: _PURPOSE_RANK[item])
    explicit_warmup = any(item.requirement.warmup is not None for item in ordered)
    merged_warmup = (
        DemandResolver._merge_warmup(ordered) if explicit_warmup else None
    )
    freshness = [
        item.requirement.max_freshness_ms
        for item in ordered
        if item.requirement.max_freshness_ms is not None
    ]
    requirement = replace(
        first.requirement,
        purpose=purpose,
        warmup_limit=(
            int(merged_warmup.rows or 0)
            if merged_warmup is not None
            else max(item.requirement.warmup_limit for item in ordered)
        ),
        warmup=merged_warmup,
        max_freshness_ms=min(freshness) if freshness else None,
        priority=min(item.requirement.priority for item in ordered),
        ttl_seconds=max(item.requirement.ttl_seconds for item in ordered),
        require_final_bars=any(item.requirement.require_final_bars for item in ordered),
        require_live=any(item.requirement.require_live for item in ordered),
        execution_grade=purpose is DemandPurpose.EXECUTION,
        depth_levels=max(item.requirement.depth_levels for item in ordered),
    )
    return replace(
        first,
        requirement=requirement,
        catalog_revision=max(item.catalog_revision for item in ordered),
        consumer_ids=tuple(
            sorted({owner for item in ordered for owner in item.consumer_ids})
        ),
        effective_priority=requirement.priority,
    )


def converge_active_demand(
    inventory: ActiveDemandInventory,
    admission: ProviderAdmission,
    policy: AdmissionPolicy,
    *,
    capabilities: CapabilityRegistry | None = None,
) -> ActiveDemandConvergence:
    """Build a dark, bounded topology/readiness plan from admitted demand.

    This is intentionally deterministic and in-memory.  It creates no provider
    session, Docker role, image, consumer route, lease store, or runtime state.
    The returned plan is the sole Phase 11.1 evidence for a later approved
    runtime handoff.
    """
    if admission.inventory_sha256 != inventory.manifest_sha256:
        raise InventoryError("provider admission inventory digest does not match active demand")
    capability_registry = capabilities or CapabilityRegistry.defaults()
    resolved_by_slice: dict[
        tuple[str, str, str, str, str],
        list[ResolvedRequirement],
    ] = defaultdict(list)
    admission_ids_by_slice: dict[tuple[str, str, str, str, str], list[str]] = defaultdict(list)
    for row in admission.rows:
        if row.state != "ADMITTED":
            continue
        source_requirement = _source_requirement_for_admission(inventory, row)
        try:
            record = admission.records[
                (row.venue, row.market, row.product_type, row.native_symbol)
            ]
        except KeyError as error:
            raise InventoryError(
                "admitted provider row has no canonical instrument record: "
                f"{row.venue}/{row.market}/{row.product_type}/{row.native_symbol}"
            ) from error
        key = _physical_slice_key(row)
        resolved_by_slice[key].append(
            _resolved_requirement_for_admission(
                source_requirement,
                row,
                record,
                capability_registry,
                demand_revision=inventory.revision,
            )
        )
        admission_ids_by_slice[key].append(row.requirement_id)

    merged_by_slice = {
        key: _merge_active_slice(values)
        for key, values in resolved_by_slice.items()
    }
    selected_keys: set[tuple[str, str, str, str, str]] = set()
    exhausted_keys: set[tuple[str, str, str, str, str]] = set()
    selected_by_budget: dict[tuple[str, str, DemandFeed], int] = defaultdict(int)
    for key, resolved in sorted(
        merged_by_slice.items(),
        key=lambda item: (
            _PURPOSE_RANK[item[1].requirement.purpose],
            item[1].effective_priority,
            item[1].requirement.requirement_id,
            item[0],
        ),
    ):
        budget_key = (
            resolved.requirement.universe.venue,
            resolved.requirement.universe.market,
            resolved.requirement.feed,
        )
        budget_limit = policy.limit_for(*budget_key)
        if (
            len(selected_keys) >= policy.max_total_slices
            or selected_by_budget[budget_key] >= budget_limit
        ):
            exhausted_keys.add(key)
            continue
        selected_keys.add(key)
        selected_by_budget[budget_key] += 1

    leases = DemandLeaseRegistry(clock_ns=lambda: 1_000_000_000)
    owners: dict[str, list[ResolvedRequirement]] = defaultdict(list)
    for key in sorted(selected_keys):
        resolved = merged_by_slice[key]
        for owner in resolved.consumer_ids:
            owners[owner].append(resolved)
    for owner, values in sorted(owners.items()):
        leases.renew(owner, values, now_ns=1_000_000_000)
    desired = leases.desired(now_ns=1_000_000_000)
    topology = DemandTopologyPlanner(
        max_subscriptions_per_connection=policy.max_subscriptions_per_connection,
    ).build(desired, demand_revision=inventory.revision)

    selected_admission_ids = {
        row_id
        for key in selected_keys
        for row_id in admission_ids_by_slice[key]
    }
    exhausted_admission_ids = {
        row_id
        for key in exhausted_keys
        for row_id in admission_ids_by_slice[key]
    }
    readiness: list[DemandInventoryReadiness] = []
    for row in admission.rows:
        if row.state != "ADMITTED":
            state = DemandState.UNSUPPORTED
            reason = f"{row.state}:{row.reason or row.capability}"
        elif row.requirement_id in exhausted_admission_ids:
            state = DemandState.DEGRADED
            reason = "ADMISSION_BUDGET_EXHAUSTED"
        elif row.requirement_id in selected_admission_ids:
            state = DemandState.WARMING
            reason = "DARK_PLAN_NOT_APPLIED"
        else:
            raise InventoryError("admitted provider row is absent from convergence selection")
        readiness.append(
            DemandInventoryReadiness(
                requirement_id=row.requirement_id,
                consumer_id=row.consumer_id,
                venue=row.venue,
                market=row.market,
                product_type=row.product_type,
                native_symbol=row.native_symbol,
                feed=row.feed,
                interval=row.interval,
                state=state,
                execution_eligible=False,
                reason=reason,
            )
        )
    budget_usage = tuple(
        AdmissionBudgetUsage(
            venue=item.venue,
            market=item.market,
            feed=item.feed.value,
            limit=item.max_slices,
            selected_slices=selected_by_budget[item.key],
        )
        for item in sorted(
            policy.budgets,
            key=lambda item: (item.venue, item.market, item.feed.value),
        )
    )
    return ActiveDemandConvergence(
        inventory_sha256=inventory.manifest_sha256,
        policy=policy,
        topology=topology,
        readiness=tuple(sorted(
            readiness,
            key=lambda item: (item.requirement_id, item.native_symbol, item.feed),
        )),
        budget_usage=budget_usage,
        selected_slice_count=len(selected_keys),
    )
