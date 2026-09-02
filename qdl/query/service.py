from __future__ import annotations

import asyncio
import math
import uuid
import time
from dataclasses import dataclass, replace
from typing import Callable

from qdl.adapters.intervals import canonical_interval_ms
from qdl.domain.calendar import trading_calendar_for_id
from qdl.domain.instrument import InstrumentRecord
from qdl.query.contracts import (
    BatchRequirement,
    BarRevisionPolicy,
    CanonicalErrorCode,
    CoverageStatus,
    ConsumerGrade,
    DataRequirement,
    FeedType,
    QueryProblem,
    StalePolicy,
    evaluate_requirement,
)
from qdl.query.lifecycle import BarLifecycle
from qdl.query.entitlement import AccessPurpose, DataProduct, EntitlementPolicy
from qdl.query.reference import (
    ReferenceBatchRequirement,
    ReferenceDataRequirement,
)
from qdl.query.results import (
    HistoryResult,
    InstrumentPage,
    InstrumentQuery,
    MarketDataItem,
    MarketDataQueryBackend,
    QualityMetadata,
    QueryBackendError,
)
from qdl.warmup.executor import BoundedWarmupExecutor, RetryableWarmupError
from qdl.reference.batch import ReferenceBatch
from qdl.reference.contracts import (
    ReferenceBatchResult,
    ReferenceRequest,
    ReferenceStatus,
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
class ReferenceBatchItemResult:
    requirement: ReferenceDataRequirement
    status: str
    result: ReferenceBatchResult | None = None
    problem: QueryProblem | None = None


@dataclass(frozen=True)
class ReferenceBatchQueryResult:
    request_id: str
    results: tuple[ReferenceBatchItemResult, ...]

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
        warmup_executor: BoundedWarmupExecutor | None = None,
        reference_batch: ReferenceBatch | None = None,
        reference_source_id: Callable[[InstrumentRecord], str] | None = None,
    ) -> None:
        if reference_batch is not None and reference_source_id is None:
            raise ValueError("reference batch requires an explicit source-id resolver")
        self.instruments = instruments
        self.backend = backend
        self.entitlements = entitlements
        self._clock_ns = clock_ns
        self.warmup_executor = warmup_executor or BoundedWarmupExecutor()
        self.reference_batch = reference_batch
        self._reference_source_id = reference_source_id
        self.last_batch_evidence: dict[str, object] = {}
        self.last_reference_batch_evidence: dict[str, object] = {}

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
        try:
            item = self.backend.latest(requirement)
        except QueryBackendError as error:
            raise QueryServiceError(
                error.problem,
                request_id=request_id,
                instrument_uid=requirement.instrument_uid,
            ) from error
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
        try:
            history = self.backend.history(requirement)
        except QueryBackendError as error:
            raise QueryServiceError(
                error.problem,
                request_id=request_id,
                instrument_uid=requirement.instrument_uid,
            ) from error
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

    async def warmup_async(
        self,
        requirement: DataRequirement,
        *,
        purpose: AccessPurpose,
        request_id: str | None = None,
    ) -> WarmupResult:
        """Run one warmup through the same bounded policy as a batch item."""
        request_id = request_id or self.request_id()
        batch = await self.warmup_batch_async(
            BatchRequirement(
                consumer_id="qdl.v2.single-warmup",
                requirements=(requirement,),
                require_all=True,
            ),
            purpose=purpose,
            request_id=request_id,
        )
        item = batch.results[0]
        if item.problem is not None:
            raise QueryServiceError(
                item.problem,
                request_id=request_id,
                instrument_uid=requirement.instrument_uid,
            )
        assert item.result is not None
        return item.result

    async def warmup_batch_async(
        self,
        batch: BatchRequirement,
        *,
        purpose: AccessPurpose,
        request_id: str | None = None,
    ) -> BatchQueryResult:
        """Execute a batch concurrently without hiding item-level failures."""
        request_id = request_id or self.request_id()
        executor_before = self.warmup_executor.stats()
        backend_before = self._warmup_backend_stats()

        async def work(requirement: DataRequirement) -> WarmupResult:
            try:
                return await asyncio.to_thread(
                    self.warmup,
                    requirement,
                    purpose=purpose,
                    request_id=request_id,
                )
            except QueryServiceError as error:
                if error.problem.retryable:
                    raise RetryableWarmupError(
                        error.problem.detail,
                        retry_after_ms=error.problem.retry_after_ms,
                        cause=error,
                    ) from error
                raise

        def provider(requirement: DataRequirement) -> str:
            try:
                return self.instruments.get(requirement.instrument_uid).identity.venue
            except KeyError:
                return "UNKNOWN"

        def deadline(requirement: DataRequirement) -> int:
            specification = requirement.warmup_specification
            return specification.deadline_ms if specification else 20_000

        executions = await self.warmup_executor.execute(
            batch.requirements,
            work=work,
            identity=lambda requirement: requirement,
            provider=provider,
            deadline_ms=deadline,
        )
        results = []
        for execution in executions:
            requirement = execution.item
            if execution.ok:
                assert execution.value is not None
                results.append(
                    BatchItemResult(
                        requirement.instrument_uid,
                        "OK",
                        result=execution.value,
                    )
                )
                continue
            error = execution.error
            if isinstance(error, RetryableWarmupError) and isinstance(
                error.cause, QueryServiceError
            ):
                problem = error.cause.problem
            elif isinstance(error, QueryServiceError):
                problem = error.problem
            elif isinstance(error, RetryableWarmupError):
                problem = QueryProblem(
                    CanonicalErrorCode.DEPENDENCY_UNAVAILABLE,
                    str(error),
                    True,
                    error.retry_after_ms,
                )
            else:
                problem = QueryProblem(
                    CanonicalErrorCode.INTERNAL_ERROR,
                    "warmup batch item failed inside the bounded executor",
                    False,
                )
            results.append(
                BatchItemResult(
                    requirement.instrument_uid,
                    problem.code.value,
                    problem=problem,
                )
            )
        elapsed = sorted(execution.elapsed_ms for execution in executions)
        percentile = lambda fraction: (
            elapsed[min(len(elapsed) - 1, max(0, math.ceil(len(elapsed) * fraction) - 1))]
            if elapsed
            else 0.0
        )
        executor_after = self.warmup_executor.stats()
        backend_after = self._warmup_backend_stats()
        executor_delta = {
            f"executor_{key}": executor_after.get(key, 0) - executor_before.get(key, 0)
            for key in executor_after
        }
        backend_delta = {
            key: backend_after.get(key, 0) - backend_before.get(key, 0)
            for key in backend_after
            if key != "cache_entries"
        }
        cache_lookups = backend_delta.get("cache_hits", 0) + backend_delta.get(
            "cache_misses", 0
        )
        self.last_batch_evidence = {
            "request_id": request_id,
            "item_count": len(executions),
            "success_count": sum(item.problem is None for item in results),
            "error_count": sum(item.problem is not None for item in results),
            "p50_ms": percentile(0.50),
            "p95_ms": percentile(0.95),
            "cache_hit_rate": (
                backend_delta.get("cache_hits", 0) / cache_lookups
                if cache_lookups
                else 0.0
            ),
            **executor_delta,
            **backend_delta,
        }
        return BatchQueryResult(request_id, tuple(results))

    def _warmup_backend_stats(self) -> dict[str, int]:
        stats = getattr(self.backend, "warmup_stats", None)
        if not callable(stats):
            return {}
        return {str(key): int(value) for key, value in stats().items()}

    async def reference_data_batch_async(
        self,
        batch: ReferenceBatchRequirement,
        *,
        purpose: AccessPurpose,
        request_id: str | None = None,
    ) -> ReferenceBatchQueryResult:
        """Fetch bounded provider reference data through the V2 policy boundary.

        A caller can only reach an adapter after its canonical ``instrument_uid``
        resolves in the active catalog and the same source/purpose entitlement
        as a normal V2 history request has been granted.  The shared warmup
        executor supplies provider lanes, token budgets, deadlines and
        singleflight; ``ReferenceBatch`` supplies reference-specific cache,
        capability and decimal/coverage validation.
        """

        request_id = request_id or self.request_id()
        if self.reference_batch is None or self._reference_source_id is None:
            raise QueryServiceError(
                QueryProblem(
                    CanonicalErrorCode.DEPENDENCY_UNAVAILABLE,
                    "V2 reference-data batch is not enabled for this runtime",
                    True,
                ),
                request_id=request_id,
            )

        executor_before = self.warmup_executor.stats()
        reference_before = self.reference_batch.stats()
        results: list[ReferenceBatchItemResult | None] = [None] * len(batch.requirements)
        admitted: list[tuple[int, ReferenceDataRequirement, ReferenceRequest]] = []
        for index, requirement in enumerate(batch.requirements):
            try:
                instrument = self.instruments.get(requirement.instrument_uid)
            except KeyError:
                results[index] = ReferenceBatchItemResult(
                    requirement,
                    CanonicalErrorCode.INSTRUMENT_NOT_FOUND.value,
                    problem=QueryProblem(
                        CanonicalErrorCode.INSTRUMENT_NOT_FOUND,
                        "reference instrument is not present in the active catalog",
                        False,
                    ),
                )
                continue
            try:
                request = requirement.to_reference_request(instrument)
                source_id = self._reference_source_id(instrument)
            except (TypeError, ValueError) as error:
                results[index] = ReferenceBatchItemResult(
                    requirement,
                    CanonicalErrorCode.INVALID_ARGUMENT.value,
                    problem=QueryProblem(CanonicalErrorCode.INVALID_ARGUMENT, str(error), False),
                )
                continue
            product = (
                DataProduct.CANONICAL_HISTORY
                if request.is_history
                else DataProduct.CANONICAL_SNAPSHOT
            )
            decision = self.entitlements.authorize(
                source_id=source_id,
                purpose=purpose,
                product=product,
                at_ns=self._clock_ns(),
            )
            if not decision.allowed:
                results[index] = ReferenceBatchItemResult(
                    requirement,
                    CanonicalErrorCode.SOURCE_NOT_ALLOWED.value,
                    problem=QueryProblem(
                        CanonicalErrorCode.SOURCE_NOT_ALLOWED,
                        "reference source entitlement denied this consumer purpose",
                        False,
                    ),
                )
                continue
            admitted.append((index, requirement, request))

        async def work(
            candidate: tuple[int, ReferenceDataRequirement, ReferenceRequest]
        ) -> ReferenceBatchResult:
            _index, _requirement, request = candidate
            result = await self.reference_batch.fetch_one(request)
            initial_problem = self._reference_problem(_requirement, request, result)
            if (
                initial_problem is not None
                and initial_problem.code is CanonicalErrorCode.DATA_STALE
                and result.cache_hit
            ):
                # A cache hit may have crossed this caller's governed
                # freshness boundary after it was stored. Refresh the same
                # catalog-bound provider request once; a genuinely stale
                # provider observation remains DATA_STALE below.
                result = await self.reference_batch.fetch_one(
                    request,
                    bypass_cache=True,
                )
            # Rust provider admission deliberately communicates bounded
            # pressure through a typed retry delay.  Keep Rust as the only
            # admission authority and let the shared executor honor that
            # delay, rather than treating a deferred result as completed.
            if (
                result.status is ReferenceStatus.ERROR
                and result.retry_after_ms is not None
            ):
                raise RetryableWarmupError(
                    result.error_detail or "reference provider deferred by bounded admission",
                    retry_after_ms=result.retry_after_ms,
                )
            return result

        executions = await self.warmup_executor.execute(
            admitted,
            work=work,
            identity=lambda candidate: candidate[2].cache_key,
            provider=lambda candidate: candidate[2].instrument.identity.venue,
            deadline_ms=lambda candidate: candidate[1].deadline_ms,
        )
        for execution in executions:
            index, requirement, request = execution.item
            if execution.error is not None:
                retry_after_ms = getattr(execution.error, "retry_after_ms", None)
                results[index] = ReferenceBatchItemResult(
                    requirement,
                    CanonicalErrorCode.SOURCE_UNAVAILABLE.value,
                    problem=QueryProblem(
                        CanonicalErrorCode.SOURCE_UNAVAILABLE,
                        "reference batch provider lane did not complete",
                        True,
                        retry_after_ms,
                    ),
                )
                continue
            assert execution.value is not None
            result = execution.value
            problem = self._reference_problem(requirement, request, result)
            results[index] = ReferenceBatchItemResult(
                requirement,
                result.status.value if problem is None else problem.code.value,
                result=result,
                problem=problem,
            )

        resolved = tuple(item for item in results if item is not None)
        if len(resolved) != len(batch.requirements):
            raise RuntimeError("reference batch lost a result during bounded scheduling")
        executor_after = self.warmup_executor.stats()
        reference_after = self.reference_batch.stats()
        self.last_reference_batch_evidence = {
            "request_id": request_id,
            "item_count": len(resolved),
            "success_count": sum(item.problem is None for item in resolved),
            "error_count": sum(item.problem is not None for item in resolved),
            **{
                f"executor_{key}": executor_after.get(key, 0) - executor_before.get(key, 0)
                for key in executor_after
            },
            **{
                f"reference_{key}": reference_after.get(key, 0) - reference_before.get(key, 0)
                for key in reference_after
                if key != "cache_entries" and key != "inflight"
            },
        }
        return ReferenceBatchQueryResult(request_id, resolved)

    def _reference_problem(
        self,
        requirement: ReferenceDataRequirement,
        request: ReferenceRequest,
        result: ReferenceBatchResult,
    ) -> QueryProblem | None:
        if result.request != request:
            return QueryProblem(
                CanonicalErrorCode.CONFLICT,
                "reference batch returned a result for a different request identity",
                False,
            )
        if result.request.instrument.instrument_uid != requirement.instrument_uid:
            return QueryProblem(
                CanonicalErrorCode.CONFLICT,
                "reference batch result instrument differs from the request",
                False,
            )
        if result.request.product is not requirement.product:
            return QueryProblem(
                CanonicalErrorCode.CONFLICT,
                "reference batch result product differs from the request",
                False,
            )
        if result.status is ReferenceStatus.OK:
            if result.request.is_history and requirement.require_full_coverage:
                coverage = result.coverage
                if not coverage.complete_left or not coverage.complete_right or coverage.truncated:
                    return QueryProblem(
                        CanonicalErrorCode.PARTIAL_RESULT,
                        "reference provider history did not cover the requested complete window",
                        False,
                    )
            if requirement.max_freshness_ms is not None:
                newest = max(item.observed_at_ns for item in result.observations)
                freshness_ms = max(0, (self._clock_ns() - newest) // 1_000_000)
                if freshness_ms > requirement.max_freshness_ms:
                    return QueryProblem(
                        CanonicalErrorCode.DATA_STALE,
                        "reference provider result exceeds the declared freshness bound",
                        True,
                    )
            return None
        if result.status is ReferenceStatus.MISSING:
            return QueryProblem(
                CanonicalErrorCode.DATA_NOT_READY,
                "reference provider returned no observation for the requested identity/window",
                False,
            )
        if result.status is ReferenceStatus.UNAVAILABLE:
            return QueryProblem(
                CanonicalErrorCode.UNSUPPORTED_FEED,
                result.error_detail or "reference product is unavailable at this provider",
                False,
            )
        return QueryProblem(
            CanonicalErrorCode.SOURCE_UNAVAILABLE,
            result.error_detail or "reference provider request failed",
            result.error_code not in {"PROVIDER_PROTOCOL"},
            result.retry_after_ms,
        )

    def status(self, requirement: DataRequirement) -> QualityMetadata:
        request_id = self.request_id()
        try:
            item = self.backend.latest(requirement)
            if item is None:
                history = self.backend.history(requirement)
                item = history.items[-1] if history and history.items else None
        except QueryBackendError as error:
            raise QueryServiceError(
                error.problem,
                request_id=request_id,
                instrument_uid=requirement.instrument_uid,
            ) from error
        if item is None:
            raise QueryServiceError(
                QueryProblem(CanonicalErrorCode.DATA_NOT_READY, "feed status is unavailable", True),
                request_id=request_id,
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
                if requirement.warmup_specification is not None:
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
            available=(
                quality.state not in {"OFFLINE", "UNAVAILABLE"}
                and not (
                    requirement.consumer_grade is ConsumerGrade.EXECUTION
                    and quality.state == "MARKET_CLOSED"
                )
            ),
            fresh=(
                quality.state == "MARKET_CLOSED"
                or (
                    quality.state not in {"STALE", "OFFLINE", "UNAVAILABLE"}
                    and quality.provider_session_state
                    not in {"STALE", "DISCONNECTED", "UNKNOWN"}
                    and (
                        requirement.max_session_liveness_ms is None
                        or (
                            quality.provider_session_state == "LIVE"
                            and quality.provider_session_liveness_ms is not None
                            and quality.provider_session_liveness_ms
                            <= requirement.max_session_liveness_ms
                        )
                    )
                    and (
                        requirement.max_freshness_ms is None
                        or quality.freshness_ms <= requirement.max_freshness_ms
                        or requirement.effective_event_recency_policy
                        not in {StalePolicy.BLOCK, StalePolicy.PAUSE}
                    )
                )
            ),
            # Source authority and continuity are distinct facts.  Keeping
            # them separate lets execution callers receive OPEN_SEQUENCE_GAP
            # for a real continuity defect instead of a misleading lineage
            # rejection.
            authoritative=authoritative,
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
            and quality.event_recency_state != "STALE"
            and quality.provider_session_state
            not in {"STALE", "DISCONNECTED", "UNKNOWN"}
            and (
                requirement.max_session_liveness_ms is None
                or (
                    quality.provider_session_state == "LIVE"
                    and quality.provider_session_liveness_ms is not None
                    and quality.provider_session_liveness_ms
                    <= requirement.max_session_liveness_ms
                )
            )
            and (
                requirement.max_freshness_ms is None
                or quality.freshness_ms <= requirement.max_freshness_ms
            )
        )
        return replace(
            item,
            quality=replace(quality, execution_eligible=eligible),
        )

    def _enforce_content(
        self,
        requirement: DataRequirement,
        items: tuple[MarketDataItem, ...],
        request_id: str,
    ) -> None:
        if any(
            item.instrument_uid != requirement.instrument_uid
            or item.feed is not requirement.feed
            or item.interval != requirement.interval
            for item in items
        ):
            raise QueryServiceError(
                QueryProblem(
                    CanonicalErrorCode.CONFLICT,
                    "query result identity differs from the data requirement",
                    False,
                ),
                request_id=request_id,
                instrument_uid=requirement.instrument_uid,
            )
        if requirement.feed is not FeedType.BAR:
            return
        opens = tuple(int(item.payload["open_time_ns"]) for item in items)
        if opens != tuple(sorted(set(opens))):
            raise QueryServiceError(
                QueryProblem(
                    CanonicalErrorCode.OPEN_SEQUENCE_GAP,
                    "BAR result is duplicate or out of order",
                    True,
                ),
                request_id=request_id,
                instrument_uid=requirement.instrument_uid,
            )
        specification = requirement.warmup_specification
        if specification is not None and specification.rows is not None:
            if len(items) != specification.rows:
                raise QueryServiceError(
                    QueryProblem(
                        CanonicalErrorCode.PARTIAL_RESULT,
                        "BAR result row count differs from the warmup horizon",
                        True,
                    ),
                    request_id=request_id,
                    instrument_uid=requirement.instrument_uid,
                )
        if specification is not None and specification.time_range is not None:
            interval_ns = canonical_interval_ms(requirement.interval or "") * 1_000_000
            start_ns = specification.time_range.start_time_ns
            end_ns = specification.time_range.end_time_ns
            instrument = self.instruments.get(requirement.instrument_uid)
            if instrument.session_calendar_id == "CRYPTO_24X7":
                duration_ns = end_ns - start_ns
                if duration_ns % interval_ns:
                    raise QueryServiceError(
                        QueryProblem(
                            CanonicalErrorCode.INVALID_ARGUMENT,
                            "BAR warmup time range is not aligned to the interval",
                            False,
                        ),
                        request_id=request_id,
                        instrument_uid=requirement.instrument_uid,
                    )
                if duration_ns // interval_ns > 10_000:
                    raise QueryServiceError(
                        QueryProblem(
                            CanonicalErrorCode.INVALID_ARGUMENT,
                            "BAR warmup time range exceeds the public row bound",
                            False,
                        ),
                        request_id=request_id,
                        instrument_uid=requirement.instrument_uid,
                    )
                expected = tuple(range(start_ns, end_ns, interval_ns))
            else:
                try:
                    expected = trading_calendar_for_id(
                        instrument.session_calendar_id
                    ).bar_opens_between_ns(
                        start_ns=start_ns,
                        end_ns=end_ns,
                        interval_ns=interval_ns,
                        max_rows=10_000,
                    )
                except ValueError as error:
                    raise QueryServiceError(
                        QueryProblem(
                            CanonicalErrorCode.INVALID_ARGUMENT,
                            f"invalid governed BAR warmup range: {error}",
                            False,
                        ),
                        request_id=request_id,
                        instrument_uid=requirement.instrument_uid,
                    ) from error
            if opens != expected:
                raise QueryServiceError(
                    QueryProblem(
                        CanonicalErrorCode.PARTIAL_RESULT,
                        "BAR result does not exactly cover the requested session range",
                        True,
                    ),
                    request_id=request_id,
                    instrument_uid=requirement.instrument_uid,
                )
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
