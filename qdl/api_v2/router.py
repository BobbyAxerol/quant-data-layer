from __future__ import annotations

from dataclasses import asdict
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from qdl.api_v2.models import (
    BatchItemResponse,
    BatchRequirementModel,
    BatchResponse,
    FeedStatusResponse,
    GapListResponse,
    InstrumentPageResponse,
    InstrumentResponse,
    MarketDataView,
    ProblemDetails,
    QualityView,
    ReadinessItemResponse,
    ReadinessResponse,
    SnapshotResponse,
    SourceView,
    SystemReadinessSummary,
    WarmupResponse,
)
from qdl.query import (
    AccessPurpose,
    BatchRequirement,
    BarRevisionPolicy,
    CanonicalErrorCode,
    ConsumerGrade,
    DataRequirement,
    FeedType,
    GapPolicy,
    QueryProblem,
    QueryServiceError,
    RecoveryPolicy,
    StalePolicy,
    V2QueryService,
)


router = APIRouter(prefix="/v2", tags=["market-data-v2"])


def _service(request: Request) -> V2QueryService:
    return request.app.state.v2_query_service


def _purpose(value: Annotated[str, Header(alias="X-QDL-Purpose")] = "INTERNAL_ALPHA"):
    try:
        return AccessPurpose(value.strip().upper())
    except ValueError as error:
        raise QueryServiceError(
            QueryProblem(CanonicalErrorCode.INVALID_ARGUMENT, "invalid X-QDL-Purpose", False),
            request_id=V2QueryService.request_id(),
        ) from error


def _requirement(model) -> DataRequirement:
    return DataRequirement(**model.model_dump())


def _market_item(item) -> MarketDataView:
    return MarketDataView(
        instrument_uid=item.instrument_uid,
        instrument_id=item.instrument_id,
        instrument_revision=item.instrument_revision,
        feed=item.feed.value,
        interval=item.interval,
        observed_at_ns=item.observed_at_ns,
        revision=item.revision,
        payload=item.payload,
        source=SourceView(**asdict(item.source)),
        quality=QualityView(**{**asdict(item.quality), "flags": list(item.quality.flags)}),
        cursor=item.cursor,
        snapshot_id=item.snapshot_id,
        watermark_offset=item.watermark_offset,
    )


def _warmup(result) -> WarmupResponse:
    history = result.history
    return WarmupResponse(
        request_id=result.request_id,
        snapshot_id=history.snapshot_id,
        data_as_of_ns=history.data_as_of_ns,
        stream_cursor=history.stream_cursor,
        watermark_offset=history.watermark_offset,
        coverage=history.coverage.value,
        count=len(history.items),
        data=[_market_item(item) for item in history.items],
    )


_STATUS = {
    CanonicalErrorCode.INVALID_ARGUMENT: 400,
    CanonicalErrorCode.INSTRUMENT_NOT_FOUND: 404,
    CanonicalErrorCode.UNSUPPORTED_FEED: 422,
    CanonicalErrorCode.SCHEMA_NOT_SUPPORTED: 422,
    CanonicalErrorCode.DATA_NOT_READY: 503,
    CanonicalErrorCode.DATA_STALE: 503,
    CanonicalErrorCode.SOURCE_UNAVAILABLE: 503,
    CanonicalErrorCode.SOURCE_NOT_ALLOWED: 403,
    CanonicalErrorCode.SOURCE_NON_AUTHORITATIVE: 503,
    CanonicalErrorCode.OPEN_SEQUENCE_GAP: 503,
    CanonicalErrorCode.CURSOR_EXPIRED: 410,
    CanonicalErrorCode.CURSOR_INVALID: 400,
    CanonicalErrorCode.RATE_LIMITED: 429,
    CanonicalErrorCode.DEPENDENCY_UNAVAILABLE: 503,
    CanonicalErrorCode.PARTIAL_RESULT: 409,
    CanonicalErrorCode.CONFLICT: 409,
    CanonicalErrorCode.INTERNAL_ERROR: 500,
}


def _problem(error: QueryServiceError) -> ProblemDetails:
    code = error.problem.code
    return ProblemDetails(
        type=f"urn:qdl:error:{code.value.lower().replace('_', '-')}",
        title=code.value.replace("_", " ").title(),
        status=_STATUS[code],
        code=code.value,
        detail=error.problem.detail,
        request_id=error.request_id,
        retryable=error.problem.retryable,
        retry_after_ms=error.problem.retry_after_ms,
        instrument_uid=error.instrument_uid,
        quality_state=error.quality_state,
    )


@router.get("/instruments", response_model=InstrumentPageResponse)
async def list_instruments(
    cursor: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    service: V2QueryService = Depends(_service),
):
    page = service.list_instruments(cursor=cursor, limit=limit)
    return {
        "schema": "qdl.instruments.page.v2",
        "items": [item.identity.__dict__ | {
            "metadata_revision": item.metadata_revision,
            "asset_class": item.asset_class.value,
            "native_symbol": item.native_symbol,
            "status": item.status.value,
        } for item in page.items],
        "next_cursor": page.next_cursor,
    }


@router.get("/instruments/{identity}", response_model=InstrumentResponse)
async def get_instrument(identity: str, service: V2QueryService = Depends(_service)):
    try:
        item = service.get_instrument(identity)
    except KeyError as error:
        raise QueryServiceError(
            QueryProblem(CanonicalErrorCode.INSTRUMENT_NOT_FOUND, str(error), False),
            request_id=service.request_id(),
        ) from error
    return {
        "schema": "qdl.instrument.v2",
        **item.identity.__dict__,
        "metadata_revision": item.metadata_revision,
        "asset_class": item.asset_class.value,
        "native_symbol": item.native_symbol,
        "status": item.status.value,
    }


def _query_requirement(
    instrument_uid: str,
    feed: FeedType,
    consumer_grade: ConsumerGrade,
    source_policy_id: str,
    interval: str | None,
    warmup_limit: int,
    max_freshness_ms: int | None,
    require_full_coverage: bool,
    require_final_bars: bool,
    stale_policy: StalePolicy,
    gap_policy: GapPolicy,
    recovery: RecoveryPolicy,
    bar_revision_policy: BarRevisionPolicy,
) -> DataRequirement:
    return DataRequirement(
        instrument_uid=instrument_uid,
        feed=feed,
        consumer_grade=consumer_grade,
        source_policy_id=source_policy_id,
        interval=interval,
        warmup_limit=warmup_limit,
        max_freshness_ms=max_freshness_ms,
        require_full_coverage=require_full_coverage,
        require_final_bars=require_final_bars,
        stale_policy=stale_policy,
        gap_policy=gap_policy,
        recovery=recovery,
        bar_revision_policy=bar_revision_policy,
    )


@router.get("/market-data/{instrument_uid}/snapshot", response_model=SnapshotResponse)
async def snapshot(
    instrument_uid: str,
    feed: FeedType,
    source_policy_id: str,
    consumer_grade: ConsumerGrade = ConsumerGrade.ALPHA,
    interval: str | None = None,
    max_freshness_ms: int | None = Query(None, gt=0, le=86_400_000),
    require_full_coverage: bool = True,
    require_final_bars: bool = True,
    stale_policy: StalePolicy = StalePolicy.BLOCK,
    gap_policy: GapPolicy = GapPolicy.BLOCK,
    recovery: RecoveryPolicy = RecoveryPolicy.SNAPSHOT_AND_REPLAY,
    bar_revision_policy: BarRevisionPolicy = BarRevisionPolicy.LATEST,
    purpose: AccessPurpose = Depends(_purpose),
    service: V2QueryService = Depends(_service),
):
    result = service.snapshot(
        _query_requirement(
            instrument_uid, feed, consumer_grade, source_policy_id,
            interval, 0, max_freshness_ms, require_full_coverage,
            require_final_bars, stale_policy, gap_policy, recovery,
            bar_revision_policy,
        ),
        purpose=purpose,
    )
    return SnapshotResponse(request_id=result.request_id, data=_market_item(result.item))


@router.get("/market-data/{instrument_uid}/warmup", response_model=WarmupResponse)
async def warmup(
    instrument_uid: str,
    feed: FeedType,
    source_policy_id: str,
    consumer_grade: ConsumerGrade = ConsumerGrade.ALPHA,
    interval: str | None = None,
    limit: int = Query(1000, ge=1, le=10_000),
    max_freshness_ms: int | None = Query(None, gt=0, le=86_400_000),
    require_full_coverage: bool = True,
    require_final_bars: bool = True,
    stale_policy: StalePolicy = StalePolicy.BLOCK,
    gap_policy: GapPolicy = GapPolicy.BLOCK,
    recovery: RecoveryPolicy = RecoveryPolicy.SNAPSHOT_AND_REPLAY,
    bar_revision_policy: BarRevisionPolicy = BarRevisionPolicy.LATEST,
    purpose: AccessPurpose = Depends(_purpose),
    service: V2QueryService = Depends(_service),
):
    result = service.warmup(
        _query_requirement(
            instrument_uid, feed, consumer_grade, source_policy_id,
            interval, limit, max_freshness_ms, require_full_coverage,
            require_final_bars, stale_policy, gap_policy, recovery,
            bar_revision_policy,
        ),
        purpose=purpose,
    )
    return _warmup(result)


@router.get("/market-data/{instrument_uid}/history", response_model=WarmupResponse)
async def history(
    instrument_uid: str,
    feed: FeedType,
    source_policy_id: str,
    consumer_grade: ConsumerGrade = ConsumerGrade.RESEARCH,
    interval: str | None = None,
    limit: int = Query(1000, ge=1, le=10_000),
    max_freshness_ms: int | None = Query(None, gt=0, le=86_400_000),
    require_full_coverage: bool = True,
    require_final_bars: bool = True,
    stale_policy: StalePolicy = StalePolicy.BLOCK,
    gap_policy: GapPolicy = GapPolicy.BLOCK,
    recovery: RecoveryPolicy = RecoveryPolicy.SNAPSHOT_AND_REPLAY,
    bar_revision_policy: BarRevisionPolicy = BarRevisionPolicy.LATEST,
    purpose: AccessPurpose = Depends(_purpose),
    service: V2QueryService = Depends(_service),
):
    return await warmup(
        instrument_uid,
        feed,
        source_policy_id,
        consumer_grade,
        interval,
        limit,
        max_freshness_ms,
        require_full_coverage,
        require_final_bars,
        stale_policy,
        gap_policy,
        recovery,
        bar_revision_policy,
        purpose,
        service,
    )


@router.post("/market-data/warmup:batch", response_model=BatchResponse)
async def warmup_batch(
    body: BatchRequirementModel,
    purpose: AccessPurpose = Depends(_purpose),
    service: V2QueryService = Depends(_service),
):
    batch = BatchRequirement(
        body.consumer_id,
        tuple(_requirement(item) for item in body.requirements),
        require_all=body.require_all,
    )
    result = service.warmup_batch(batch, purpose=purpose)
    items = []
    for item in result.results:
        problem = None
        if item.problem is not None:
            problem = _problem(QueryServiceError(
                item.problem,
                request_id=result.request_id,
                instrument_uid=item.instrument_uid,
            ))
        items.append(BatchItemResponse(
            instrument_uid=item.instrument_uid,
            status=item.status,
            data=_warmup(item.result) if item.result else None,
            problem=problem,
        ))
    return BatchResponse(
        request_id=result.request_id,
        partial=result.partial,
        success_count=result.success_count,
        error_count=result.error_count,
        results=items,
    )


@router.get("/feeds/{instrument_uid}/status", response_model=FeedStatusResponse)
async def feed_status(
    instrument_uid: str,
    feed: FeedType,
    source_policy_id: str,
    consumer_grade: ConsumerGrade = ConsumerGrade.ALPHA,
    interval: str | None = None,
    require_full_coverage: bool = True,
    require_final_bars: bool = True,
    stale_policy: StalePolicy = StalePolicy.BLOCK,
    gap_policy: GapPolicy = GapPolicy.BLOCK,
    recovery: RecoveryPolicy = RecoveryPolicy.SNAPSHOT_AND_REPLAY,
    bar_revision_policy: BarRevisionPolicy = BarRevisionPolicy.LATEST,
    service: V2QueryService = Depends(_service),
):
    requirement = _query_requirement(
        instrument_uid, feed, consumer_grade, source_policy_id, interval, 0, None,
        require_full_coverage, require_final_bars, stale_policy, gap_policy,
        recovery, bar_revision_policy,
    )
    return {
        "schema": "qdl.feed-status.v2",
        "instrument_uid": instrument_uid,
        "feed": feed.value,
        "quality": asdict(service.status(requirement)),
    }


@router.post("/system/readiness:check", response_model=ReadinessResponse)
async def readiness(
    body: BatchRequirementModel,
    purpose: AccessPurpose = Depends(_purpose),
    service: V2QueryService = Depends(_service),
):
    batch = BatchRequirement(
        body.consumer_id,
        tuple(_requirement(item) for item in body.requirements),
        require_all=body.require_all,
    )
    result = service.readiness(batch, purpose=purpose)
    items = []
    for item in result.results:
        problem = None
        if item.problem is not None:
            problem = _problem(QueryServiceError(
                item.problem,
                request_id=result.request_id,
                instrument_uid=item.instrument_uid,
            ))
        items.append(ReadinessItemResponse(
            instrument_uid=item.instrument_uid,
            status=item.status,
            quality=(
                QualityView(**{**asdict(item.quality), "flags": list(item.quality.flags)})
                if item.quality else None
            ),
            problem=problem,
        ))
    return ReadinessResponse(
        request_id=result.request_id,
        ready=result.ready,
        results=items,
    )


@router.get("/system/readiness", response_model=SystemReadinessSummary)
async def system_readiness():
    return {
        "schema": "qdl.system-readiness.v2",
        "status": "SHADOW_READY",
        "authority": "V1",
        "v2_consumer_activation": "MANIFEST_CONTROLLED",
    }


@router.get("/data-quality/gaps", response_model=GapListResponse)
async def data_quality_gaps(service: V2QueryService = Depends(_service)):
    return {
        "schema": "qdl.data-quality.gaps.v2",
        "items": [
            {**asdict(item), "feed": item.feed.value}
            for item in service.open_gaps()
        ],
    }


def create_v2_app(service: V2QueryService) -> FastAPI:
    app = FastAPI(title="Quant Data Layer V2", version="2.0.0-shadow")
    app.state.v2_query_service = service
    app.state.runtime_manifest = {
        "role": "api_v2",
        "owns_live_ingestion": False,
        "owns_venue_connections": False,
        "authority": "SHADOW",
    }
    app.include_router(router)

    @app.exception_handler(QueryServiceError)
    async def query_error_handler(_request: Request, error: QueryServiceError):
        problem = _problem(error)
        return JSONResponse(
            status_code=problem.status,
            content=problem.model_dump(exclude_none=True),
            media_type="application/problem+json",
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, error: RequestValidationError):
        request_id = service.request_id()
        problem = ProblemDetails(
            type="urn:qdl:error:invalid-argument",
            title="Invalid Argument",
            status=400,
            code=CanonicalErrorCode.INVALID_ARGUMENT.value,
            detail="; ".join(item["msg"] for item in error.errors()),
            request_id=request_id,
            retryable=False,
        )
        return JSONResponse(
            status_code=400,
            content=problem.model_dump(exclude_none=True),
            media_type="application/problem+json",
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(_request: Request, error: ValueError):
        problem = ProblemDetails(
            type="urn:qdl:error:invalid-argument",
            title="Invalid Argument",
            status=400,
            code=CanonicalErrorCode.INVALID_ARGUMENT.value,
            detail=str(error),
            request_id=service.request_id(),
            retryable=False,
        )
        return JSONResponse(
            status_code=400,
            content=problem.model_dump(exclude_none=True),
            media_type="application/problem+json",
        )

    return app
