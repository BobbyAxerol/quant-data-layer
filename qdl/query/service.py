from __future__ import annotations

import asyncio
import math
import uuid
import time
from dataclasses import dataclass, replace

from qdl.adapters.intervals import canonical_interval_ms
from qdl.domain.calendar import trading_calendar_for_id
from qdl.query.contracts import (
    BatchRequirement,
    BarRevisionPolicy,
    CanonicalErrorCode,
    CoverageStatus,
    ConsumerGrade,
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
    QueryBackendError,
)
from qdl.warmup.executor import BoundedWarmupExecutor, RetryableWarmupError


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
        warmup_executor: BoundedWarmupExecutor | None = None,
    ) -> None:
        self.instruments = instruments
        self.backend = backend
        self.entitlements = entitlements
        self._clock_ns = clock_ns
        self.warmup_executor = warmup_executor or BoundedWarmupExecutor()
        self.last_batch_evidence: dict[str, object] = {}

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
                    quality.state not in {"STALE", "OFFLINE"}
                    and (
                        requirement.max_freshness_ms is None
                        or quality.freshness_ms <= requirement.max_freshness_ms
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
