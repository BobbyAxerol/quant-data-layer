from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal, InvalidOperation
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

from qdl.api_v2.models import (
    BatchItemResponse,
    BatchRequirementModel,
    BatchResponse,
    DecimalValue,
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
from qdl.security import (
    DataPlaneAccess,
    DataPlaneAccessError,
    DataPlaneIdentityService,
    DataPlanePermission,
)
from qdl.runtime.bounds import BoundedRequestMiddleware, RequestBounds
from qdl.runtime.readiness import FailClosedReadiness


router = APIRouter(prefix="/v2", tags=["market-data-v2"])
_bearer_scheme = HTTPBearer(auto_error=False, scheme_name="QDLWorkloadBearer")
_consumer_scheme = APIKeyHeader(
    name="X-QDL-Consumer-ID",
    auto_error=False,
    scheme_name="QDLConsumerIdentity",
)


def _service(request: Request) -> V2QueryService:
    return request.app.state.v2_query_service


def _data_access(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(_bearer_scheme)
    ],
    consumer_id: Annotated[str | None, Depends(_consumer_scheme)],
) -> DataPlaneAccess:
    identity = getattr(request.app.state, "v2_identity_service", None)
    if identity is None:
        raise DataPlaneAccessError(
            "DEPENDENCY_UNAVAILABLE",
            "V2 data-plane identity service is unavailable",
            status_code=503,
        )
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise DataPlaneAccessError(
            "UNAUTHENTICATED", "workload bearer token is required", status_code=401
        )
    if not consumer_id:
        raise DataPlaneAccessError(
            "UNAUTHENTICATED", "X-QDL-Consumer-ID is required", status_code=401
        )
    access = identity.authenticate(
        credentials.credentials,
        consumer_id=consumer_id,
    )
    request.state.qdl_data_access = access
    return access


def _purpose(value: Annotated[str, Header(alias="X-QDL-Purpose")]):
    try:
        return AccessPurpose(value.strip().upper())
    except ValueError as error:
        raise QueryServiceError(
            QueryProblem(CanonicalErrorCode.INVALID_ARGUMENT, "invalid X-QDL-Purpose", False),
            request_id=V2QueryService.request_id(),
        ) from error


def _requirement(model) -> DataRequirement:
    return DataRequirement(**model.model_dump())


def _decimal(value: object) -> DecimalValue:
    if isinstance(value, dict):
        return DecimalValue.model_validate(value)
    if isinstance(value, float) or isinstance(value, bool):
        raise ValueError("public V2 decimal fields cannot originate from binary float/bool")
    try:
        source = str(value)
        parsed = Decimal(source)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("canonical decimal field is invalid") from error
    if not parsed.is_finite():
        raise ValueError("canonical decimal field must be finite")
    sign, digits, exponent = parsed.as_tuple()
    coefficient = int("".join(str(digit) for digit in digits) or "0")
    if sign:
        coefficient = -coefficient
    return DecimalValue(
        coefficient=str(coefficient),
        scale=-exponent,
        source_text=source,
    )


def _book_levels(values: object) -> list[dict]:
    if not isinstance(values, list):
        raise ValueError("order-book levels must be a list")
    return [
        {
            "side": item["side"],
            "price": _decimal(item["price"]),
            "quantity": _decimal(item["quantity"]),
            "quantity_unit": item["quantity_unit"],
            "order_count": int(item.get("order_count", 0)),
        }
        for item in values
    ]


def _typed_payload(item) -> dict:
    value = item.payload
    if item.feed is FeedType.TRADE:
        return {
            "feed": item.feed,
            "native_trade_id": value["native_trade_id"],
            "price": _decimal(value["price"]),
            "quantity": _decimal(value["quantity"]),
            "quantity_unit": value["quantity_unit"],
            "aggressor_side": value["aggressor_side"],
            "identity_kind": value["identity_kind"],
            "is_block_trade": bool(value.get("is_block_trade", False)),
            "is_buyer_maker": bool(value.get("is_buyer_maker", False)),
        }
    if item.feed is FeedType.QUOTE:
        return {
            "feed": item.feed,
            "bid_price": _decimal(value["bid_price"]),
            "bid_quantity": _decimal(value["bid_quantity"]),
            "ask_price": _decimal(value["ask_price"]),
            "ask_quantity": _decimal(value["ask_quantity"]),
            "quantity_unit": value["quantity_unit"],
            "level": int(value.get("level", 1)),
        }
    if item.feed is FeedType.BAR:
        return {
            "feed": item.feed,
            "interval": item.interval,
            "open_time_ns": int(value["open_time_ns"]),
            "close_time_ns": int(value["close_time_ns"]),
            "open": _decimal(value["open"]),
            "high": _decimal(value["high"]),
            "low": _decimal(value["low"]),
            "close": _decimal(value["close"]),
            "volume": _decimal(value["volume"]),
            "volume_unit": value["volume_unit"],
            "base_volume": (
                _decimal(value["base_volume"])
                if value.get("base_volume") is not None
                else None
            ),
            "quote_volume": (
                _decimal(value["quote_volume"])
                if value.get("quote_volume") is not None
                else None
            ),
            "contract_volume": (
                _decimal(value["contract_volume"])
                if value.get("contract_volume") is not None
                else None
            ),
            "trade_count": int(value.get("trade_count", 0)),
            "lifecycle": item.bar_lifecycle,
            "revision": item.revision,
            "origin": value["origin"],
            "supersedes_event_id": item.supersedes_event_id,
        }
    if item.feed is FeedType.BOOK_SNAPSHOT:
        return {
            "feed": item.feed,
            "native_sequence": value["native_sequence"],
            "checksum": value.get("checksum"),
            "levels": _book_levels(value["levels"]),
            "depth": int(value["depth"]),
        }
    if item.feed is FeedType.BOOK_DELTA:
        return {
            "feed": item.feed,
            "native_sequence_start": value["native_sequence_start"],
            "native_sequence_end": value["native_sequence_end"],
            "snapshot_sequence": value["snapshot_sequence"],
            "checksum": value.get("checksum"),
            "updates": _book_levels(value["updates"]),
            "reset": bool(value.get("reset", False)),
        }
    if item.feed is FeedType.FUNDING_RATE:
        return {
            "feed": item.feed,
            "rate": _decimal(value["rate"]),
            "funding_time_ns": int(value["funding_time_ns"]),
            "next_funding_time_ns": value.get("next_funding_time_ns"),
        }
    if item.feed is FeedType.OPEN_INTEREST:
        return {
            "feed": item.feed,
            "quantity": _decimal(value["quantity"]),
            "quantity_unit": value["quantity_unit"],
            "notional": _decimal(value["notional"]) if value.get("notional") is not None else None,
        }
    if item.feed is FeedType.MARK_INDEX_PRICE:
        return {
            "feed": item.feed,
            "mark_price": _decimal(value["mark_price"]),
            "index_price": _decimal(value["index_price"]),
        }
    if item.feed is FeedType.TICKER:
        result = {"feed": item.feed, "last_price": _decimal(value["last_price"])}
        for field in ("last_quantity", "open_24h", "high_24h", "low_24h", "volume_24h"):
            result[field] = _decimal(value[field]) if value.get(field) is not None else None
        result["last_quantity_unit"] = value.get("last_quantity_unit")
        result["volume_24h_unit"] = value.get("volume_24h_unit")
        return result
    raise ValueError(f"public typed payload is undefined for {item.feed.value}")


def _market_item(item) -> MarketDataView:
    return MarketDataView(
        instrument_uid=item.instrument_uid,
        instrument_id=item.instrument_id,
        instrument_revision=item.instrument_revision,
        feed=item.feed.value,
        interval=item.interval,
        observed_at_ns=item.observed_at_ns,
        revision=item.revision,
        payload=_typed_payload(item),
        source=SourceView(**asdict(item.source)),
        quality=QualityView(**{**asdict(item.quality), "flags": list(item.quality.flags)}),
        contract=asdict(item.contract),
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


def _bind_item_cursor(request: Request, access, requirement, item):
    issuer = getattr(request.app.state, "v2_cursor_issuer", None)
    if issuer is None:
        return item
    return issuer.bind_item(
        requirement, item, consumer_id=access.consumer_id
    )


def _bind_history_cursor(request: Request, access, requirement, history):
    issuer = getattr(request.app.state, "v2_cursor_issuer", None)
    if issuer is None:
        return history
    return issuer.bind_history(
        requirement, history, consumer_id=access.consumer_id
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
    access: DataPlaneAccess = Depends(_data_access),
):
    access.require_permission(DataPlanePermission.INSTRUMENTS_READ)
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
async def get_instrument(
    identity: str,
    service: V2QueryService = Depends(_service),
    access: DataPlaneAccess = Depends(_data_access),
):
    access.require_permission(DataPlanePermission.INSTRUMENTS_READ)
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
    request: Request,
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
    access: DataPlaneAccess = Depends(_data_access),
):
    requirement = _query_requirement(
        instrument_uid, feed, consumer_grade, source_policy_id,
        interval, 0, max_freshness_ms, require_full_coverage,
        require_final_bars, stale_policy, gap_policy, recovery,
        bar_revision_policy,
    )
    access.require_permission(DataPlanePermission.SNAPSHOT_READ)
    access.require_purpose(purpose)
    access.require_requirement(requirement)
    result = service.snapshot(
        requirement,
        purpose=purpose,
    )
    item = _bind_item_cursor(request, access, requirement, result.item)
    return SnapshotResponse(request_id=result.request_id, data=_market_item(item))


@router.get("/market-data/{instrument_uid}/warmup", response_model=WarmupResponse)
async def warmup(
    request: Request,
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
    access: DataPlaneAccess = Depends(_data_access),
):
    requirement = _query_requirement(
        instrument_uid, feed, consumer_grade, source_policy_id,
        interval, limit, max_freshness_ms, require_full_coverage,
        require_final_bars, stale_policy, gap_policy, recovery,
        bar_revision_policy,
    )
    access.require_permission(DataPlanePermission.HISTORY_READ)
    access.require_purpose(purpose)
    access.require_requirement(requirement)
    result = service.warmup(
        requirement,
        purpose=purpose,
    )
    result = type(result)(
        result.request_id,
        _bind_history_cursor(request, access, requirement, result.history),
    )
    return _warmup(result)


@router.get("/market-data/{instrument_uid}/history", response_model=WarmupResponse)
async def history(
    request: Request,
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
    access: DataPlaneAccess = Depends(_data_access),
):
    requirement = _query_requirement(
        instrument_uid, feed, consumer_grade, source_policy_id,
        interval, limit, max_freshness_ms, require_full_coverage,
        require_final_bars, stale_policy, gap_policy, recovery,
        bar_revision_policy,
    )
    access.require_permission(DataPlanePermission.HISTORY_READ)
    access.require_purpose(purpose)
    access.require_requirement(requirement)
    result = service.warmup(requirement, purpose=purpose)
    result = type(result)(
        result.request_id,
        _bind_history_cursor(request, access, requirement, result.history),
    )
    return _warmup(result)


@router.post("/market-data/warmup:batch", response_model=BatchResponse)
async def warmup_batch(
    request: Request,
    body: BatchRequirementModel,
    purpose: AccessPurpose = Depends(_purpose),
    service: V2QueryService = Depends(_service),
    access: DataPlaneAccess = Depends(_data_access),
):
    access.require_consumer(body.consumer_id)
    access.require_permission(DataPlanePermission.HISTORY_READ)
    access.require_purpose(purpose)
    access.require_batch_size(len(body.requirements))
    requirements = tuple(_requirement(item) for item in body.requirements)
    for requirement in requirements:
        access.require_requirement(requirement)
    batch = BatchRequirement(
        body.consumer_id,
        requirements,
        require_all=body.require_all,
    )
    result = service.warmup_batch(batch, purpose=purpose)
    items = []
    for item, requirement in zip(result.results, requirements, strict=True):
        problem = None
        if item.problem is not None:
            problem = _problem(QueryServiceError(
                item.problem,
                request_id=result.request_id,
                instrument_uid=item.instrument_uid,
            ))
        warmup_data = None
        if item.result is not None:
            bound = type(item.result)(
                item.result.request_id,
                _bind_history_cursor(
                    request,
                    access,
                    requirement,
                    item.result.history,
                ),
            )
            warmup_data = _warmup(bound)
        items.append(BatchItemResponse(
            instrument_uid=item.instrument_uid,
            status=item.status,
            data=warmup_data,
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
    purpose: AccessPurpose = Depends(_purpose),
    service: V2QueryService = Depends(_service),
    access: DataPlaneAccess = Depends(_data_access),
):
    requirement = _query_requirement(
        instrument_uid, feed, consumer_grade, source_policy_id, interval, 0, None,
        require_full_coverage, require_final_bars, stale_policy, gap_policy,
        recovery, bar_revision_policy,
    )
    access.require_permission(DataPlanePermission.STATUS_READ)
    access.require_purpose(purpose)
    access.require_requirement(requirement)
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
    access: DataPlaneAccess = Depends(_data_access),
):
    access.require_consumer(body.consumer_id)
    access.require_permission(DataPlanePermission.STATUS_READ)
    access.require_purpose(purpose)
    access.require_batch_size(len(body.requirements))
    requirements = tuple(_requirement(item) for item in body.requirements)
    for requirement in requirements:
        access.require_requirement(requirement)
    batch = BatchRequirement(
        body.consumer_id,
        requirements,
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
async def system_readiness(
    request: Request,
    access: DataPlaneAccess = Depends(_data_access),
):
    access.require_permission(DataPlanePermission.STATUS_READ)
    return await request.app.state.v2_runtime_readiness.public_summary()


@router.get("/data-quality/gaps", response_model=GapListResponse)
async def data_quality_gaps(
    service: V2QueryService = Depends(_service),
    access: DataPlaneAccess = Depends(_data_access),
):
    access.require_permission(DataPlanePermission.QUALITY_READ)
    return {
        "schema": "qdl.data-quality.gaps.v2",
        "items": [
            {**asdict(item), "feed": item.feed.value}
            for item in service.open_gaps()
        ],
    }


def create_v2_app(
    service: V2QueryService,
    *,
    identity_service: DataPlaneIdentityService | None = None,
    readiness_service=None,
    request_bounds: RequestBounds | None = None,
    cursor_issuer=None,
    contract_version: str = "2.0.0-shadow",
    authority: str = "SHADOW",
) -> FastAPI:
    if not contract_version.startswith("2.0.0") or authority not in {
        "SHADOW", "INTERNAL_STABLE", "PRIMARY"
    }:
        raise ValueError("V2 app contract version/authority is invalid")
    app = FastAPI(title="Quant Data Layer V2", version=contract_version)
    app.state.v2_query_service = service
    app.state.v2_identity_service = identity_service
    app.state.v2_runtime_readiness = readiness_service or FailClosedReadiness()
    app.state.v2_cursor_issuer = cursor_issuer
    app.state.runtime_manifest = {
        "role": "api_v2",
        "owns_live_ingestion": False,
        "owns_venue_connections": False,
        "authority": authority,
        "contract_version": contract_version,
    }
    app.include_router(router)
    if request_bounds is not None:
        app.add_middleware(BoundedRequestMiddleware, bounds=request_bounds)
    default_openapi = app.openapi

    def data_plane_openapi():
        schema = default_openapi()
        required_security = [{
            "QDLWorkloadBearer": [],
            "QDLConsumerIdentity": [],
        }]
        for path, operations in schema.get("paths", {}).items():
            if not path.startswith("/v2"):
                continue
            for method, operation in operations.items():
                if method.lower() in {"get", "post", "put", "patch", "delete"}:
                    operation["security"] = required_security
        return schema

    app.openapi = data_plane_openapi

    @app.exception_handler(DataPlaneAccessError)
    async def data_access_error_handler(_request: Request, error: DataPlaneAccessError):
        request_id = service.request_id() if service is not None else "unavailable"
        problem = ProblemDetails(
            type=f"urn:qdl:error:{error.code.lower().replace('_', '-')}",
            title=error.code.replace("_", " ").title(),
            status=error.status_code,
            code=error.code,
            detail=error.detail,
            request_id=request_id,
            retryable=error.status_code in {429, 503},
        )
        return JSONResponse(
            status_code=error.status_code,
            content=problem.model_dump(exclude_none=True),
            media_type="application/problem+json",
        )

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
