#!/usr/bin/env python3
"""Bounded read-only V2 warmup/reference admission for active Binance/OKX demand.

The verifier compiles the same active-demand inventory as Phase 11.1/11.2,
performs one fresh public instrument-metadata admission, and builds the
resulting catalog only in memory.  It then uses the V2 query service itself to
exercise every admitted final-BAR binding through bounded history batches plus
the declared funding/basis requests and a bounded representative reference
matrix.  It never starts a role, writes Kafka/Redis/SQLite/PostgreSQL, changes
consumer routing, or retains raw provider bytes.

The optional evidence file contains only canonical identities, response
digests, counts, latency/resource aggregates and typed status.  It is useful
for acceptance review but is not a runtime configuration input.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, replace
import hashlib
import json
import math
from pathlib import Path
import resource
import time
from typing import Any, Iterable, Mapping, Sequence

from qdl.demand import (
    DemandFeed,
    DemandPurpose,
    InventoryError,
    source_requirement_for_admission,
)
from qdl.query import (
    AccessPurpose,
    BatchRequirement,
    ConsumerGrade,
    DataRequirement,
    FeedType,
    InstrumentQuery,
    QueryBackendError,
    RecoveryPolicy,
    V2QueryService,
)
from qdl.query.reference import (
    ReferenceBatchRequirement,
    ReferenceDataRequirement,
)
from qdl.query.results import HistoryResult
from qdl.reference import (
    BasisSeries,
    LongShortKind,
    ReferenceProduct,
    ReferenceStatus,
)
from qdl.reference.runtime import build_default_reference_runtime
from qdl.runtime.provider_history import (
    ProviderBarHistorySource,
    ProviderHistoryUnavailable,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.universal_realtime import ProviderRealtimeBinding
from qdl.warmup import WarmupSpecification
from scripts.phase111_active_demand_inventory import ROOT as QDL_ROOT
from scripts.phase111_active_demand_inventory import ProviderMetadataError
from scripts.phase112_universal_realtime_provider_admission import (
    DEFAULT_SOURCE_REGISTRY,
    ProviderAdmissionError,
    ProviderAdmissionPlan,
    _build_plan,
    _related_workspace_root,
)


DEFAULT_OUTPUT = QDL_ROOT / "upgrade/evidence/phase113-universal-warmup-reference-admission.json"
_MAX_BATCH_ITEMS = 100
_MAX_REFERENCE_SYMBOLS_PER_VENUE = 4
_MIN_WARMUP_ROWS = 2
_MAX_WARMUP_ROWS = 30
_SCHEMA_DIGEST = hashlib.sha256(b"qdl.phase113.provider-only-query.v1").hexdigest()
_DAY_MS = 86_400_000
_HOUR_MS = 3_600_000
_FUNDING_MS = 8 * _HOUR_MS
_FUNDING_SETTLEMENT_GRACE_MS = 60_000
_METRIC_HISTORY_HOURS = 30 * 24


class Phase113AdmissionError(RuntimeError):
    """A real provider or V2 contract failed the Phase 11.3 gate."""


@dataclass(frozen=True, slots=True)
class _BarWork:
    binding: ProviderRealtimeBinding
    requirement: DataRequirement


@dataclass(frozen=True, slots=True)
class _ReferenceWork:
    requirement: ReferenceDataRequirement
    expected_blocked: bool = False
    allow_typed_partial: bool = False


class _ProviderOnlyBackend:
    """Read-only V2 backend for admission only, with no durable position.

    This intentionally exposes the existing provider-history implementation as
    a V2 query backend.  Every answer remains non-authoritative and has the
    explicit no-replay cursor carried by :class:`ProviderBarHistorySource`.
    The class is local to the verifier so production routing cannot mistake a
    provider read for the Rust canonical path.
    """

    def __init__(self, source: ProviderBarHistorySource) -> None:
        self._source = source

    def history(self, requirement: DataRequirement) -> HistoryResult | None:
        try:
            return self._source.history_result(requirement, schema_digest=_SCHEMA_DIGEST)
        except ProviderHistoryUnavailable as error:
            raise QueryBackendError(error.problem) from error

    def latest(self, requirement: DataRequirement):
        specification = requirement.warmup_specification
        rows = max(_MIN_WARMUP_ROWS, specification.rows if specification and specification.rows else 1)
        history = self.history(
            replace(
                requirement,
                warmup_limit=0,
                warmup=WarmupSpecification.for_rows(rows, max_cache_age_ms=0),
            )
        )
        return history.items[-1] if history and history.items else None

    def feed_status(self, requirement: DataRequirement):
        item = self.latest(requirement)
        return item.quality if item is not None else None

    @staticmethod
    def open_gaps() -> tuple[object, ...]:
        return ()

    def warmup_stats(self) -> dict[str, int]:
        return self._source.stats()


def _chunks[T](values: Sequence[T], size: int) -> tuple[tuple[T, ...], ...]:
    if not 1 <= size <= _MAX_BATCH_ITEMS:
        raise ValueError("batch size is outside the V2 contract bound")
    return tuple(tuple(values[index : index + size]) for index in range(0, len(values), size))


def _percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, math.ceil(len(ordered) * fraction) - 1))]


def _purpose_for_demand(purpose: DemandPurpose) -> tuple[ConsumerGrade, AccessPurpose]:
    if purpose is DemandPurpose.ALPHA:
        return ConsumerGrade.ALPHA, AccessPurpose.INTERNAL_ALPHA
    if purpose in {DemandPurpose.RESEARCH, DemandPurpose.OBSERVABILITY}:
        return ConsumerGrade.RESEARCH, AccessPurpose.INTERNAL_RESEARCH
    raise Phase113AdmissionError(
        "reference data cannot be promoted from an execution-grade active demand"
    )


def _binding_policy(plan: ProviderAdmissionPlan) -> dict[str, str]:
    return {
        str(item["binding_id"]): str(item["source"]["source_policy_id"])
        for item in plan.plan.bundle.source_catalog["bindings"]
    }


def _policy_for_record(catalog: StableSourceCatalog, instrument_uid: str) -> str:
    policies = {
        binding.source_policy_id
        for binding in catalog.bindings
        if binding.instrument.instrument_uid == instrument_uid
    }
    if len(policies) != 1:
        raise Phase113AdmissionError(
            "representative reference instrument has no unique active source policy"
        )
    return next(iter(policies))


def _bar_work(
    plan: ProviderAdmissionPlan,
    *,
    rows: int,
    deadline_ms: int,
) -> tuple[_BarWork, ...]:
    if not _MIN_WARMUP_ROWS <= rows <= _MAX_WARMUP_ROWS:
        raise ValueError(
            f"warmup rows must be between {_MIN_WARMUP_ROWS} and {_MAX_WARMUP_ROWS}"
        )
    policy_by_binding = _binding_policy(plan)
    result = []
    for binding in sorted(plan.bindings, key=lambda item: item.binding_id):
        if binding.feed is not FeedType.BAR or binding.interval is None:
            continue
        try:
            policy = policy_by_binding[binding.binding_id]
        except KeyError as error:
            raise Phase113AdmissionError(
                f"active BAR binding has no source-policy declaration: {binding.binding_id}"
            ) from error
        result.append(
            _BarWork(
                binding=binding,
                requirement=DataRequirement(
                    instrument_uid=binding.instrument_uid,
                    feed=FeedType.BAR,
                    interval=binding.interval,
                    consumer_grade=ConsumerGrade.ALPHA,
                    source_policy_id=policy,
                    max_freshness_ms=binding.stale_after_ms,
                    require_final_bars=True,
                    recovery=RecoveryPolicy.FRESH_SNAPSHOT,
                    warmup=WarmupSpecification.for_rows(
                        rows,
                        max_cache_age_ms=0,
                        deadline_ms=deadline_ms,
                    ),
                ),
            )
        )
    if not result:
        raise Phase113AdmissionError("active demand has no admitted BAR binding")
    identities = {(item.requirement.instrument_uid, item.requirement.interval) for item in result}
    if len(identities) != len(result):
        raise Phase113AdmissionError("active BAR projection contains duplicate V2 requirements")
    return tuple(result)


def _last_closed_daily_open_ms(now_ms: int) -> int:
    return (now_ms // _DAY_MS - 1) * _DAY_MS


def _last_closed_funding_ms(now_ms: int) -> int:
    """Return the latest safely settled nominal funding boundary.

    Binance timestamps the funding observation at the completed 8-hour event,
    but its public settlement clock can lag the nominal boundary slightly.
    Keep a bounded grace before treating the new nominal event as available;
    callers extend the provider window by the same grace so the raw timestamp
    remains visible without being rounded or rewritten.
    """

    scheduled_ms = (now_ms // _FUNDING_MS) * _FUNDING_MS
    if now_ms < scheduled_ms + _FUNDING_SETTLEMENT_GRACE_MS:
        return scheduled_ms - _FUNDING_MS
    return scheduled_ms


def _demand_reference_work(
    plan: ProviderAdmissionPlan,
    *,
    now_ms: int,
    deadline_ms: int,
) -> tuple[_ReferenceWork, ...]:
    if plan.inventory is None or plan.admission is None:
        raise Phase113AdmissionError("Phase 11.2 admission plan lacks its authenticated inventory")
    result = []
    for row in plan.admission.rows:
        if row.state != "ADMITTED" or row.feed not in {
            DemandFeed.FUNDING_RATE.value,
            DemandFeed.BASIS.value,
        }:
            continue
        if row.instrument_uid is None:
            raise Phase113AdmissionError("admitted reference demand lacks canonical instrument identity")
        declared = source_requirement_for_admission(plan.inventory, row)
        grade, _purpose = _purpose_for_demand(declared.purpose)
        if declared.feed is DemandFeed.FUNDING_RATE:
            settled_end_ms = _last_closed_funding_ms(now_ms)
            # Ask through the bounded provider settlement grace, while the
            # historical horizon itself remains exactly 365 calendar days.
            start_ms = settled_end_ms - 365 * _DAY_MS
            end_ms = settled_end_ms + _FUNDING_SETTLEMENT_GRACE_MS
            result.append(_ReferenceWork(ReferenceDataRequirement(
                instrument_uid=row.instrument_uid,
                product=ReferenceProduct.FUNDING_RATE,
                consumer_grade=grade,
                source_policy_id=declared.source_policy_id,
                start_time_ns=start_ms * 1_000_000,
                end_time_ns=end_ms * 1_000_000,
                limit=1_200,
                page_size=1_000,
                max_pages=4,
                max_freshness_ms=86_400_000,
                deadline_ms=deadline_ms,
            )))
            continue
        if declared.feed is DemandFeed.BASIS:
            end_ms = _last_closed_daily_open_ms(now_ms)
            start_ms = end_ms - 364 * _DAY_MS
            result.append(_ReferenceWork(ReferenceDataRequirement(
                instrument_uid=row.instrument_uid,
                product=ReferenceProduct.BASIS,
                consumer_grade=grade,
                source_policy_id=declared.source_policy_id,
                start_time_ns=start_ms * 1_000_000,
                end_time_ns=end_ms * 1_000_000,
                interval="1d",
                limit=365,
                page_size=365,
                max_pages=1,
                basis_series=BasisSeries(str(declared.basis_series or "CONTINUOUS")),
                basis_contract_type=declared.basis_contract_type,
                max_freshness_ms=86_400_000,
                deadline_ms=deadline_ms,
            )))
            continue
        raise Phase113AdmissionError("reference admission selected an unsupported demand feed")
    feeds = {item.requirement.product for item in result}
    # A bounded consumer handoff may legitimately declare only trade/BAR
    # routes. Keep the representative reference matrix below, but do not
    # invent a funding/basis requirement that the sealed consumer binding did
    # not request. A partial declared reference set is still a contract error.
    if not feeds:
        return ()
    if not {ReferenceProduct.FUNDING_RATE, ReferenceProduct.BASIS} <= feeds:
        raise Phase113AdmissionError(
            "active demand did not resolve both funding and basis reference contracts"
        )
    return tuple(result)


def _representative_reference_work(
    catalog: StableSourceCatalog,
    *,
    now_ms: int,
    deadline_ms: int,
) -> tuple[_ReferenceWork, ...]:
    """Exercise the common adapters broadly without fabricating unavailable data.

    Active reference demand is currently one Binance basis/funding pair.  This
    bounded matrix tests the remaining declared wrappers across multiple
    actually admitted instruments, while retaining explicit `BLOCKED` status
    for provider products that OKX does not expose.
    """

    records = sorted(
        catalog.instruments,
        key=lambda item: (
            item.identity.venue,
            item.identity.market,
            item.native_symbol,
        ),
    )
    binance = [
        item for item in records
        if (item.identity.venue, item.identity.market, item.identity.product_type.value)
        == ("BINANCE", "USDM", "PERPETUAL")
    ][:_MAX_REFERENCE_SYMBOLS_PER_VENUE]
    okx = [
        item for item in records
        if (item.identity.venue, item.identity.market, item.identity.product_type.value)
        == ("OKX", "SWAP", "PERPETUAL")
    ][:_MAX_REFERENCE_SYMBOLS_PER_VENUE]
    if not binance or not okx:
        raise Phase113AdmissionError("active catalog lacks Binance USD-M or OKX Swap reference representatives")
    end_hour_ms = (now_ms // _HOUR_MS - 1) * _HOUR_MS
    # Binance's 30-day hourly metric retention is 720 observations.  The
    # window is inclusive at both ends, so subtract 719 hours rather than
    # accidentally requesting a 721st sample outside the declared retention.
    start_30d_ms = end_hour_ms - (_METRIC_HISTORY_HOURS - 1) * _HOUR_MS
    result = []
    for record in binance:
        common = dict(
            instrument_uid=record.instrument_uid,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id=_policy_for_record(catalog, record.instrument_uid),
            deadline_ms=deadline_ms,
        )
        result.extend((
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.OPEN_INTEREST,
                start_time_ns=start_30d_ms * 1_000_000,
                end_time_ns=end_hour_ms * 1_000_000,
                interval="1h", limit=_METRIC_HISTORY_HOURS, page_size=500, max_pages=2,
                require_full_coverage=True, **common,
            ), allow_typed_partial=True),
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.LONG_SHORT_RATIO,
                start_time_ns=start_30d_ms * 1_000_000,
                end_time_ns=end_hour_ms * 1_000_000,
                interval="1h", limit=_METRIC_HISTORY_HOURS, page_size=500, max_pages=2,
                long_short_kind=LongShortKind.GLOBAL_ACCOUNT,
                require_full_coverage=True, **common,
            ), allow_typed_partial=True),
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.TAKER_FLOW,
                start_time_ns=start_30d_ms * 1_000_000,
                end_time_ns=end_hour_ms * 1_000_000,
                interval="1h", limit=_METRIC_HISTORY_HOURS, page_size=500, max_pages=2,
                require_full_coverage=True, **common,
            ), allow_typed_partial=True),
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.MARK_INDEX_PRICE, **common,
            )),
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.CONTRACT_METADATA, **common,
            )),
        ))
    for record in okx:
        common = dict(
            instrument_uid=record.instrument_uid,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id=_policy_for_record(catalog, record.instrument_uid),
            deadline_ms=deadline_ms,
        )
        result.extend((
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.OPEN_INTEREST, **common,
            )),
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.MARK_INDEX_PRICE, **common,
            )),
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.CONTRACT_METADATA, **common,
            )),
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.LONG_SHORT_RATIO,
                start_time_ns=start_30d_ms * 1_000_000,
                end_time_ns=end_hour_ms * 1_000_000,
                interval="1h", limit=_METRIC_HISTORY_HOURS, page_size=500, max_pages=2,
                long_short_kind=LongShortKind.GLOBAL_ACCOUNT,
                **common,
            ), expected_blocked=True),
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.TAKER_FLOW,
                start_time_ns=start_30d_ms * 1_000_000,
                end_time_ns=end_hour_ms * 1_000_000,
                interval="1h", limit=_METRIC_HISTORY_HOURS, page_size=500, max_pages=2,
                **common,
            ), expected_blocked=True),
            _ReferenceWork(ReferenceDataRequirement(
                product=ReferenceProduct.BASIS,
                start_time_ns=start_30d_ms * 1_000_000,
                end_time_ns=end_hour_ms * 1_000_000,
                interval="1h", limit=_METRIC_HISTORY_HOURS, page_size=500, max_pages=2,
                basis_contract_type="CURRENT_QUARTER",
                **common,
            ), expected_blocked=True),
        ))
    return tuple(result)


def _history_digest(history: HistoryResult) -> str:
    material = [
        {
            "instrument_uid": item.instrument_uid,
            "interval": item.interval,
            "open_time_ns": item.payload.get("open_time_ns"),
            "close_time_ns": item.payload.get("close_time_ns"),
            "close": item.payload.get("close"),
            "is_final": item.payload.get("is_final"),
            "source_id": item.source.source_id,
            "authoritative": item.source.authoritative,
        }
        for item in history.items
    ]
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _reference_digest(result) -> str:
    material = {
        "instrument_uid": result.request.instrument.instrument_uid,
        "product": result.request.product.value,
        "status": result.status.value,
        "coverage": {
            "complete_left": result.coverage.complete_left,
            "complete_right": result.coverage.complete_right,
            "truncated": result.coverage.truncated,
            "terminal_reason": result.coverage.terminal_reason,
        },
        "observations": [
            {
                "at_ns": item.observed_at_ns,
                "fields": [
                    (field.name, field.value.source_text, field.unit)
                    for field in item.fields
                ],
                "labels": item.labels,
            }
            for item in result.observations
        ],
    }
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _service(catalog: StableSourceCatalog, *, timeout_seconds: float) -> tuple[V2QueryService, ProviderBarHistorySource]:
    source = ProviderBarHistorySource(
        catalog,
        fetch_timeout_seconds=timeout_seconds,
    )
    reference_runtime = build_default_reference_runtime()
    entitlements = catalog.entitlements(include_unbound=True).with_grants(
        reference_runtime.entitlement_grants()
    )
    service = V2QueryService(
        instruments=InstrumentQuery(catalog.instrument_registry(include_unbound=True)),
        backend=_ProviderOnlyBackend(source),
        entitlements=entitlements,
        reference_batch=reference_runtime.batch,
        reference_source_id=reference_runtime.source_id_for,
    )
    return service, source


async def _admit_bars(
    service: V2QueryService,
    works: Sequence[_BarWork],
) -> tuple[dict[str, object], ...]:
    evidence = []
    by_key = {
        (work.requirement.instrument_uid, work.requirement.interval): work.binding
        for work in works
    }
    for chunk_index, chunk in enumerate(_chunks(tuple(works), _MAX_BATCH_ITEMS), start=1):
        batch = BatchRequirement(
            consumer_id=f"qdl.phase113.warmup.batch.{chunk_index}",
            requirements=tuple(item.requirement for item in chunk),
            require_all=True,
        )
        result = await service.warmup_batch_async(
            batch,
            purpose=AccessPurpose.INTERNAL_ALPHA,
        )
        if result.partial:
            failures = [
                {
                    "binding_id": work.binding.binding_id,
                    "instrument_uid": work.binding.instrument_uid,
                    "interval": work.binding.interval,
                    "code": item.problem.code.value,
                    "detail": item.problem.detail,
                }
                for work, item in zip(chunk, result.results, strict=True)
                if item.problem is not None
            ]
            raise Phase113AdmissionError(
                "V2 warmup batch returned typed failures: "
                + json.dumps(failures, sort_keys=True, separators=(",", ":"))
            )
        for item in result.results:
            assert item.result is not None
            history = item.result.history
            key = (item.instrument_uid, history.items[-1].interval)
            try:
                binding = by_key[key]
            except KeyError as error:
                raise Phase113AdmissionError("V2 warmup result cross-mixed its active BAR binding") from error
            opens = [int(value.payload["open_time_ns"]) for value in history.items]
            if (
                len(history.items) != len(opens)
                or opens != sorted(set(opens))
                or history.coverage.value != "FULL"
                or any(
                    value.instrument_uid != binding.instrument_uid
                    or value.interval != binding.interval
                    or value.payload.get("is_final") is not True
                    or value.source.authoritative
                    or value.quality.execution_eligible
                    for value in history.items
                )
            ):
                raise Phase113AdmissionError(
                    f"V2 provider warmup violated identity/finality/authority contract: {binding.binding_id}"
                )
            evidence.append({
                "binding_id": binding.binding_id,
                "venue": binding.venue,
                "market": binding.market,
                "native_symbol": binding.native_symbol,
                "instrument_uid": binding.instrument_uid,
                "interval": binding.interval,
                "rows": len(history.items),
                "first_open_ns": opens[0],
                "last_open_ns": opens[-1],
                "window_sha256": _history_digest(history),
            })
    return tuple(sorted(evidence, key=lambda item: str(item["binding_id"])))


async def _admit_references(
    service: V2QueryService,
    works: Sequence[_ReferenceWork],
) -> tuple[dict[str, object], ...]:
    evidence = []
    grouped: dict[ConsumerGrade, list[_ReferenceWork]] = {}
    for work in works:
        grouped.setdefault(work.requirement.consumer_grade, []).append(work)
    chunk_index = 0
    for grade, values in sorted(grouped.items(), key=lambda item: item[0].value):
        for chunk in _chunks(tuple(values), _MAX_BATCH_ITEMS):
            chunk_index += 1
            purpose = (
                AccessPurpose.INTERNAL_ALPHA
                if grade is ConsumerGrade.ALPHA
                else AccessPurpose.INTERNAL_RESEARCH
            )
            result = await service.reference_data_batch_async(
                ReferenceBatchRequirement(
                    consumer_id=f"qdl.phase113.reference.batch.{chunk_index}",
                    requirements=tuple(item.requirement for item in chunk),
                    require_all=not any(
                        item.expected_blocked or item.allow_typed_partial for item in chunk
                    ),
                ),
                purpose=purpose,
            )
            for work, item in zip(chunk, result.results, strict=True):
                if work.expected_blocked:
                    if (
                        item.problem is None
                        or item.problem.code.value != "UNSUPPORTED_FEED"
                        or item.result is None
                        or item.result.status is not ReferenceStatus.UNAVAILABLE
                    ):
                        raise Phase113AdmissionError(
                            "provider-unavailable reference product was not explicitly blocked"
                        )
                    evidence.append({
                        "instrument_uid": work.requirement.instrument_uid,
                        "product": work.requirement.product.value,
                        "expected": "BLOCKED",
                        "status": item.status,
                        "error_code": item.problem.code.value,
                    })
                    continue
                if item.problem is not None or item.result is None:
                    if (
                        work.allow_typed_partial
                        and item.problem is not None
                        and item.problem.code.value == "PARTIAL_RESULT"
                        and item.result is not None
                        and item.result.status is ReferenceStatus.OK
                        and item.result.observations
                    ):
                        evidence.append({
                            "instrument_uid": work.requirement.instrument_uid,
                            "product": work.requirement.product.value,
                            "expected": "PARTIAL_TYPED",
                            "status": item.status,
                            "error_code": item.problem.code.value,
                            "observation_count": len(item.result.observations),
                            "coverage": item.result.coverage.terminal_reason,
                            "semantic_sha256": _reference_digest(item.result),
                        })
                        continue
                    detail = item.problem.detail if item.problem is not None else "missing result"
                    raise Phase113AdmissionError(
                        "V2 reference request failed: "
                        + json.dumps(
                            {
                                "instrument_uid": work.requirement.instrument_uid,
                                "product": work.requirement.product.value,
                                "code": (
                                    item.problem.code.value
                                    if item.problem is not None
                                    else "MISSING_RESULT"
                                ),
                                "detail": detail,
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                    )
                data = item.result
                if (
                    data.status is not ReferenceStatus.OK
                    or not data.observations
                    or (data.request.is_history and (
                        not data.coverage.complete_left
                        or not data.coverage.complete_right
                        or data.coverage.truncated
                    ))
                ):
                    raise Phase113AdmissionError(
                        f"V2 reference result is incomplete: {work.requirement.product.value}"
                    )
                evidence.append({
                    "instrument_uid": work.requirement.instrument_uid,
                    "product": work.requirement.product.value,
                    "expected": "AVAILABLE",
                    "status": data.status.value,
                    "observation_count": len(data.observations),
                    "observed_min_ns": min(value.observed_at_ns for value in data.observations),
                    "observed_max_ns": max(value.observed_at_ns for value in data.observations),
                    "coverage": data.coverage.terminal_reason,
                    "semantic_sha256": _reference_digest(data),
                })
    return tuple(sorted(evidence, key=lambda item: (
        str(item["instrument_uid"]), str(item["product"]), str(item["expected"])
    )))


async def _run_async(
    *,
    admission_plan: ProviderAdmissionPlan,
    warmup_rows: int,
    provider_timeout_seconds: float,
    deadline_ms: int,
    now_ms: int,
) -> dict[str, object]:
    catalog = StableSourceCatalog.from_mapping(admission_plan.plan.bundle.source_catalog)
    service, source = _service(catalog, timeout_seconds=provider_timeout_seconds)
    bar_work = _bar_work(admission_plan, rows=warmup_rows, deadline_ms=deadline_ms)
    reference_work = _demand_reference_work(
        admission_plan, now_ms=now_ms, deadline_ms=deadline_ms
    ) + _representative_reference_work(
        catalog, now_ms=now_ms, deadline_ms=deadline_ms
    )
    started = time.monotonic()
    cpu_before = time.process_time()
    bar_evidence = await _admit_bars(service, bar_work)
    reference_evidence = await _admit_references(service, reference_work)
    elapsed = time.monotonic() - started
    stats = source.stats()
    if stats["provider_source_failures"]:
        raise Phase113AdmissionError("provider warmup source recorded a failure")
    expected_blocked = sum(1 for item in reference_evidence if item["expected"] == "BLOCKED")
    partial_count = sum(1 for item in reference_evidence if item["expected"] == "PARTIAL_TYPED")
    return {
        "schema": "qdl.phase113.universal-warmup-reference-admission.v1",
        "status": "PASS",
        "provenance": "REAL_PROVIDER_READ_ONLY",
        "inventory_sha256": admission_plan.plan.inventory_sha256,
        "metadata_sha256": dict(sorted(admission_plan.admission.metadata_sha256.items())),
        "source_catalog_sha256": admission_plan.plan.bundle.provenance["source_catalog_sha256"],
        "production_writes": 0,
        "provider_writes": 0,
        "raw_payload_persisted": False,
        "runtime_mutations": 0,
        "bar_binding_count": len(bar_evidence),
        "bar_batch_count": len(_chunks(bar_work, _MAX_BATCH_ITEMS)),
        "warmup_rows": warmup_rows,
        "reference_request_count": len(reference_evidence),
        "reference_expected_blocked_count": expected_blocked,
        "reference_partial_count": partial_count,
        "reference_available_count": len(reference_evidence) - expected_blocked - partial_count,
        "elapsed_seconds": elapsed,
        "cpu_seconds": time.process_time() - cpu_before,
        "rss_max_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        "warmup": service.last_batch_evidence,
        "reference": service.last_reference_batch_evidence,
        "provider_source": stats,
        "bar_slices": bar_evidence,
        "reference_results": reference_evidence,
    }


def run(
    *,
    source_registry: Path = DEFAULT_SOURCE_REGISTRY,
    repository_root: Path = QDL_ROOT,
    execution_alpha_root: Path | None = None,
    trading_system_root: Path | None = None,
    warmup_rows: int = 3,
    provider_timeout_seconds: float = 20.0,
    deadline_ms: int = 60_000,
    metadata_timeout_seconds: float = 20.0,
    metadata_attempts: int = 3,
    now_ms: int | None = None,
) -> dict[str, object]:
    if not 1.0 <= provider_timeout_seconds <= 30.0:
        raise ValueError("provider timeout must be between 1 and 30 seconds")
    if not 1 <= metadata_attempts <= 5:
        raise ValueError("metadata attempts must be between 1 and 5")
    if not 1_000 <= deadline_ms <= 120_000:
        raise ValueError("deadline must be between 1000 and 120000 milliseconds")
    alpha_root = execution_alpha_root or _related_workspace_root("execution_alpha")
    trading_root = trading_system_root or _related_workspace_root("trading_system")
    admission_plan = _build_plan(
        source_registry=source_registry,
        repository_root=repository_root,
        execution_alpha_root=alpha_root,
        trading_system_root=trading_root,
        metadata_timeout_seconds=metadata_timeout_seconds,
        metadata_attempts=metadata_attempts,
    )
    return asyncio.run(_run_async(
        admission_plan=admission_plan,
        warmup_rows=warmup_rows,
        provider_timeout_seconds=provider_timeout_seconds,
        deadline_ms=deadline_ms,
        now_ms=now_ms if now_ms is not None else time.time_ns() // 1_000_000,
    ))


def _write(path: Path, report: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _console_summary(report: Mapping[str, object]) -> dict[str, object]:
    return {
        key: report[key]
        for key in (
            "schema",
            "status",
            "provenance",
            "bar_binding_count",
            "bar_batch_count",
            "warmup_rows",
            "reference_request_count",
            "reference_available_count",
            "reference_expected_blocked_count",
            "reference_partial_count",
            "elapsed_seconds",
            "cpu_seconds",
            "rss_max_kib",
            "production_writes",
            "runtime_mutations",
        )
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-registry", type=Path, default=DEFAULT_SOURCE_REGISTRY)
    parser.add_argument("--repository-root", type=Path, default=QDL_ROOT)
    parser.add_argument("--execution-alpha-root", type=Path)
    parser.add_argument("--trading-system-root", type=Path)
    parser.add_argument("--warmup-rows", type=int, default=3)
    parser.add_argument("--provider-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--deadline-ms", type=int, default=60_000)
    parser.add_argument("--metadata-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--metadata-attempts", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        report = run(
            source_registry=args.source_registry,
            repository_root=args.repository_root,
            execution_alpha_root=args.execution_alpha_root,
            trading_system_root=args.trading_system_root,
            warmup_rows=args.warmup_rows,
            provider_timeout_seconds=args.provider_timeout_seconds,
            deadline_ms=args.deadline_ms,
            metadata_timeout_seconds=args.metadata_timeout_seconds,
            metadata_attempts=args.metadata_attempts,
        )
    except (
        Phase113AdmissionError,
        ProviderAdmissionError,
        InventoryError,
        ProviderMetadataError,
        OSError,
        ValueError,
    ) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, sort_keys=True))
        return 1
    if args.output is not None:
        _write(args.output, report)
    print(json.dumps(_console_summary(report), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
