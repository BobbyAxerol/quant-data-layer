from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, replace

from qdl.query.contracts import (
    BatchRequirement,
    BarRevisionPolicy,
    CanonicalErrorCode,
    CoverageStatus,
    DataRequirement,
    FeedType,
    QueryProblem,
    evaluate_requirement,
)
from qdl.query.lifecycle import BarLifecycle
from qdl.query.entitlement import AccessPurpose, DataProduct, EntitlementPolicy
from qdl.query.results import (
    HistoryResult,
    InstrumentPage,
    InstrumentQuery,
    MarketDataItem,
    MarketDataQueryBackend,
    QualityMetadata,
)


class QueryServiceError(RuntimeError):
    def __init__(
        self,
        problem: QueryProblem,
        *,
        request_id: str,
        instrument_uid: str | None = None,
        quality_state: str | None = None,
    ) -> None:
        super().__init__(problem.detail)
        self.problem = problem
        self.request_id = request_id
        self.instrument_uid = instrument_uid
        self.quality_state = quality_state


@dataclass(frozen=True)
class QueryResult:
    request_id: str
    item: MarketDataItem


@dataclass(frozen=True)
class WarmupResult:
    request_id: str
    history: HistoryResult


@dataclass(frozen=True)
class BatchItemResult:
    instrument_uid: str
    status: str
    result: WarmupResult | None = None
    problem: QueryProblem | None = None


@dataclass(frozen=True)
class BatchQueryResult:
    request_id: str
    results: tuple[BatchItemResult, ...]

    @property
    def partial(self) -> bool:
        return any(item.problem is not None for item in self.results)

    @property
    def success_count(self) -> int:
        return sum(item.problem is None for item in self.results)

    @property
    def error_count(self) -> int:
        return len(self.results) - self.success_count


@dataclass(frozen=True)
class ReadinessItemResult:
    instrument_uid: str
    status: str
    quality: QualityMetadata | None = None
    problem: QueryProblem | None = None


@dataclass(frozen=True)
class ReadinessResult:
    request_id: str
    ready: bool
    results: tuple[ReadinessItemResult, ...]


class V2QueryService:
    """Provider-neutral policy boundary shared by REST, gRPC and SDK."""

    def __init__(
        self,
        *,
        instruments: InstrumentQuery,
        backend: MarketDataQueryBackend,
        entitlements: EntitlementPolicy,
        clock_ns=time.time_ns,
    ) -> None:
        self.instruments = instruments
        self.backend = backend
        self.entitlements = entitlements
        self._clock_ns = clock_ns

    @staticmethod
    def request_id() -> str:
        return str(uuid.uuid4())

    def list_instruments(self, *, cursor: str | None, limit: int) -> InstrumentPage:
        return self.instruments.list(cursor=cursor, limit=limit)

    def get_instrument(self, identity: str):
        return self.instruments.get(identity)

    def snapshot(
        self,
        requirement: DataRequirement,
        *,
        purpose: AccessPurpose,
        request_id: str | None = None,
    ) -> QueryResult:
        request_id = request_id or self.request_id()
        item = self.backend.latest(requirement)
        if item is None:
            self._raise_not_ready(requirement, request_id)
        self._enforce_content(requirement, (item,), request_id)
        self._enforce(
            requirement,
            item.quality,
            item.source.source_id,
            purpose,
            DataProduct.CANONICAL_SNAPSHOT,
            CoverageStatus.FULL,
            request_id,
            authoritative=item.source.authoritative,
        )
        return QueryResult(
            request_id,
            self._with_execution_eligibility(
                requirement, item, DataProduct.CANONICAL_SNAPSHOT
            ),
        )

    def warmup(
        self,
        requirement: DataRequirement,
        *,
        purpose: AccessPurpose,
        request_id: str | None = None,
    ) -> WarmupResult:
        request_id = request_id or self.request_id()
        history = self.backend.history(requirement)
        if history is None or not history.items:
            self._raise_not_ready(requirement, request_id)
        quality = history.items[-1].quality
        source_id = history.items[-1].source.source_id
        self._enforce_content(requirement, history.items, request_id)
        self._enforce(
            requirement,
            quality,
            source_id,
            purpose,
            DataProduct.CANONICAL_HISTORY,
            history.coverage,
            request_id,
            authoritative=history.items[-1].source.authoritative,
        )
        return WarmupResult(
            request_id,
            replace(
                history,
                items=tuple(
                    self._with_execution_eligibility(
                        requirement, item, DataProduct.CANONICAL_HISTORY
                    )
                    for item in history.items
                ),
            ),
        )

    def warmup_batch(
        self,
        batch: BatchRequirement,
        *,
        purpose: AccessPurpose,
        request_id: str | None = None,
    ) -> BatchQueryResult:
        request_id = request_id or self.request_id()
        results = []
        for requirement in batch.requirements:
            try:
                result = self.warmup(requirement, purpose=purpose, request_id=request_id)
                results.append(BatchItemResult(requirement.instrument_uid, "OK", result=result))
            except QueryServiceError as error:
                results.append(
                    BatchItemResult(
                        requirement.instrument_uid,
                        error.problem.code.value,
                        problem=error.problem,
                    )
                )
        return BatchQueryResult(request_id, tuple(results))

    def status(self, requirement: DataRequirement) -> QualityMetadata:
        item = self.backend.latest(requirement)
        if item is None:
            history = self.backend.history(requirement)
            item = history.items[-1] if history and history.items else None
        if item is None:
            raise QueryServiceError(
                QueryProblem(CanonicalErrorCode.DATA_NOT_READY, "feed status is unavailable", True),
                request_id=self.request_id(),
                instrument_uid=requirement.instrument_uid,
            )
        return self._with_execution_eligibility(
            requirement,
            item,
            DataProduct.CANONICAL_SNAPSHOT,
        ).quality

    def open_gaps(self):
        return self.backend.open_gaps()

    def readiness(
        self,
        batch: BatchRequirement,
        *,
        purpose: AccessPurpose,
    ) -> ReadinessResult:
        request_id = self.request_id()
        results = []
        for requirement in batch.requirements:
            try:
                if requirement.warmup_limit > 0:
                    checked = self.warmup(
                        requirement, purpose=purpose, request_id=request_id
                    ).history.items[-1]
                else:
                    checked = self.snapshot(
                        requirement, purpose=purpose, request_id=request_id
                    ).item
                results.append(ReadinessItemResult(
                    requirement.instrument_uid, "READY", quality=checked.quality
                ))
            except QueryServiceError as error:
                results.append(ReadinessItemResult(
                    requirement.instrument_uid,
                    error.problem.code.value,
                    problem=error.problem,
                ))
        return ReadinessResult(
            request_id,
            ready=not any(item.problem is not None for item in results),
            results=tuple(results),
        )

    def _enforce(
        self,
        requirement: DataRequirement,
        quality: QualityMetadata,
        source_id: str,
        purpose: AccessPurpose,
        product: DataProduct,
        coverage: CoverageStatus,
        request_id: str,
        *,
        authoritative: bool,
    ) -> None:
        if quality.policy_id != requirement.source_policy_id:
            raise QueryServiceError(
                QueryProblem(
                    CanonicalErrorCode.CONFLICT,
                    "resolved source policy does not match the data requirement",
                    False,
                ),
                request_id=request_id,
                instrument_uid=requirement.instrument_uid,
                quality_state=quality.state,
            )
        entitlement = self.entitlements.authorize(
            source_id=source_id,
            purpose=purpose,
            product=product,
            at_ns=self._clock_ns(),
        )
        problem = evaluate_requirement(
            requirement,
            entitled=entitlement.allowed,
            available=quality.state not in {"OFFLINE", "UNAVAILABLE"},
            fresh=(
                quality.state not in {"STALE", "OFFLINE"}
                and (
                    requirement.max_freshness_ms is None
                    or quality.freshness_ms <= requirement.max_freshness_ms
                )
            ),
            authoritative=(
                authoritative and not quality.gap_open
            ),
            gap_open=quality.gap_open,
            coverage=(
                coverage if quality.complete else CoverageStatus.PARTIAL
            ),
        )
        if problem is not None:
            raise QueryServiceError(
                problem,
                request_id=request_id,
                instrument_uid=requirement.instrument_uid,
                quality_state=quality.state,
            )

    def _with_execution_eligibility(
        self,
        requirement: DataRequirement,
        item: MarketDataItem,
        product: DataProduct,
    ) -> MarketDataItem:
        execution_entitlement = self.entitlements.authorize(
            source_id=item.source.source_id,
            purpose=AccessPurpose.INTERNAL_EXECUTION,
            product=product,
            at_ns=self._clock_ns(),
        )
        quality = item.quality
        eligible = (
            execution_entitlement.allowed
            and item.source.authoritative
            and quality.policy_id == requirement.source_policy_id
            and quality.state == "LIVE"
            and quality.complete
            and not quality.gap_open
            and (
                requirement.max_freshness_ms is None
                or quality.freshness_ms <= requirement.max_freshness_ms
            )
        )
        return replace(
            item,
            quality=replace(quality, execution_eligible=eligible),
        )

    @staticmethod
    def _enforce_content(
        requirement: DataRequirement,
        items: tuple[MarketDataItem, ...],
        request_id: str,
    ) -> None:
        if requirement.feed is not FeedType.BAR:
            return
        if requirement.require_final_bars and any(
            item.bar_lifecycle not in {BarLifecycle.FINAL, BarLifecycle.REVISED}
            for item in items
        ):
            raise QueryServiceError(
                QueryProblem(
                    CanonicalErrorCode.DATA_NOT_READY,
                    "required final bar is not available",
                    True,
                ),
                request_id=request_id,
                instrument_uid=requirement.instrument_uid,
            )
        if requirement.bar_revision_policy is BarRevisionPolicy.INITIAL_ONLY and any(
            item.revision != 0 for item in items
        ):
            raise QueryServiceError(
                QueryProblem(
                    CanonicalErrorCode.DATA_NOT_READY,
                    "initial bar revision is not available from this result",
                    True,
                ),
                request_id=request_id,
                instrument_uid=requirement.instrument_uid,
            )

    @staticmethod
    def _raise_not_ready(requirement: DataRequirement, request_id: str) -> None:
        raise QueryServiceError(
            QueryProblem(CanonicalErrorCode.DATA_NOT_READY, "required data is not available", True),
            request_id=request_id,
            instrument_uid=requirement.instrument_uid,
        )
