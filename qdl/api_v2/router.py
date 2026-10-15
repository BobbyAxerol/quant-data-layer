from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import re
from dataclasses import asdict, fields as dataclass_fields
from functools import lru_cache
from decimal import Decimal, InvalidOperation, getcontext
from typing import Annotated, get_args

from fastapi import APIRouter, Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from pydantic import create_model

from qdl.query.cold_work import await_in_thread, cold_yield
from qdl.domain.decimal import PLAIN_DECIMAL_TEXT
from qdl_sdk.models import ResponseModel
from qdl.api_v2.models import (
    BarPayload,
    BatchItemResponse,
    BatchRequirementModel,
    BatchResponse,
    FeedStatusResponse,
    GapListResponse,
    InstrumentPageResponse,
    InstrumentResponse,
    MarketDataView,
    MAX_REQUIREMENT_FRESHNESS_MS,
    ProblemDetails,
    QualityView,
    QuantityUnit,
    ReadinessItemResponse,
    ReadinessResponse,
    ReferenceBatchRequest,
    ReferenceBatchResponse,
    SnapshotResponse,
    SourceView,
    SystemReadinessSummary,
    WarmupResponse,
)
from qdl.query import (
    AccessPurpose,
    BarLifecycle,
    BatchRequirement,
    BarRevisionPolicy,
    CanonicalErrorCode,
    ConsumerGrade,
    DataRequirement,
    EXECUTION_PRICE_VALIDATION_FEEDS,
    FeedType,
    GapPolicy,
    QueryProblem,
    QueryServiceError,
    RecoveryPolicy,
    StalePolicy,
    V2QueryService,
)
from qdl.query.reference import ReferenceBatchRequirement, ReferenceDataRequirement
from qdl.reference.contracts import (
    BasisSeries as DomainBasisSeries,
    LongShortKind as DomainLongShortKind,
    MarkIndexKind as DomainMarkIndexKind,
    ReferenceProduct as DomainReferenceProduct,
)
from qdl.security import (
    DataPlaneAccess,
    DataPlaneAccessError,
    DataPlaneIdentityService,
    DataPlanePermission,
)
from qdl.runtime.bounds import BoundedRequestMiddleware, RequestBounds
from qdl.runtime.readiness import FailClosedReadiness
from qdl.runtime.refusal_log import REFUSALS
from qdl.warmup.contracts import (
    IntervalSourcePolicy,
    WarmupSpecification,
    WarmupTimeRange,
)


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


_REPLICA_REDIRECT_HEADER = "X-QDL-Replica-Redirect"


def _replica_redirect(
    value: Annotated[
        str | None,
        Header(
            alias=_REPLICA_REDIRECT_HEADER,
            description=(
                "Optional. `1`: if this replica's large-warmup lane would make the "
                "request wait, refuse it at once with retryable RATE_LIMITED "
                "(retry_after_ms 0, detail and `X-QDL-Refusal-Reason` "
                "REPLICA_LANE_BUSY) so the client can retry once on another "
                "replica. Absent, `0` or any other value: queue as before."
            ),
        ),
    ] = None,
) -> bool:
    return value is not None and value.strip() == "1"


def _requirement(model) -> DataRequirement:
    return DataRequirement.from_mapping(model.model_dump())


def _reference_requirement(model) -> ReferenceDataRequirement:
    """Translate the public SDK contract into the catalog-bound domain input.

    The public SDK owns its own generated enums.  Keeping this conversion at
    the HTTP boundary prevents those types from leaking into the query core
    while preserving every selector exactly (notably dated/continuous BASIS).
    """

    return ReferenceDataRequirement(
        instrument_uid=model.instrument_uid,
        product=DomainReferenceProduct(model.product.value),
        consumer_grade=ConsumerGrade(model.consumer_grade.value),
        source_policy_id=model.source_policy_id,
        start_time_ns=model.start_time_ns,
        end_time_ns=model.end_time_ns,
        interval=model.interval,
        limit=model.limit,
        page_size=model.page_size,
        max_pages=model.max_pages,
        long_short_kind=(
            DomainLongShortKind(model.long_short_kind.value)
            if model.long_short_kind is not None
            else None
        ),
        mark_index_kind=DomainMarkIndexKind(model.mark_index_kind.value),
        basis_series=DomainBasisSeries(model.basis_series.value),
        basis_contract_type=model.basis_contract_type,
        max_freshness_ms=model.max_freshness_ms,
        event_recency_policy=(
            StalePolicy(model.event_recency_policy.value)
            if model.event_recency_policy is not None
            else None
        ),
        max_session_liveness_ms=model.max_session_liveness_ms,
        require_full_coverage=model.require_full_coverage,
        deadline_ms=model.deadline_ms,
    )


def _decimal_parts(source: str) -> tuple[str, int]:
    """``(coefficient, scale)`` of a decimal text, as ``Decimal`` reads it."""

    plain = PLAIN_DECIMAL_TEXT.fullmatch(source)
    if plain is not None:
        # ASCII ``-?digits[.digits]``: exactly the digits/exponent Decimal yields.
        fraction = plain.group(2) or ""
        return str(int(plain.group(1) + fraction)), len(fraction)
    try:
        parsed = Decimal(source)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("canonical decimal field is invalid") from error
    if not parsed.is_finite():
        raise ValueError("canonical decimal field must be finite")
    sign, digits, exponent = parsed.as_tuple()
    coefficient = int("".join(map(str, digits)) or "0")
    if sign:
        coefficient = -coefficient
    return str(coefficient), -exponent


def _decimal(value: object) -> dict:
    """The ``DecimalValue`` input of a canonical decimal text.

    Returned as model input, not as a model: the enclosing public view
    validates it with the same ``DecimalValue`` schema and validator in one
    pydantic-core call, instead of one Python model construction per value
    (P18.4 D3: 8 per BAR row)."""

    if isinstance(value, dict):
        return value
    if isinstance(value, float) or isinstance(value, bool):
        raise ValueError("public V2 decimal fields cannot originate from binary float/bool")
    try:
        source = str(value)
    except (TypeError, ValueError) as error:
        raise ValueError("canonical decimal field is invalid") from error
    coefficient, scale = _decimal_parts(source)
    return {"coefficient": coefficient, "scale": scale, "source_text": source}


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
            "resample_lineage": (
                asdict(item.resample_lineage) if item.resample_lineage else None
            ),
        }
    if item.feed is FeedType.BOOK_SNAPSHOT:
        return {
            "feed": item.feed,
            "native_sequence": value["native_sequence"],
            "checksum": value.get("checksum"),
            "levels": _book_levels(value["levels"]),
            "depth": int(value["depth"]),
            "book_generation": int(value.get("book_generation", 0)),
            "sequence_verified": bool(value.get("sequence_verified", False)),
            "truncated": bool(value.get("truncated", False)),
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
            "book_generation": int(value.get("book_generation", 0)),
            "sequence_verified": bool(value.get("sequence_verified", False)),
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
            "sampling_interval": value.get("sampling_interval"),
        }
    if item.feed is FeedType.MARK_INDEX_PRICE:
        return {
            "feed": item.feed,
            "mark_price": _decimal(value["mark_price"]),
            "index_price": _decimal(value["index_price"]),
        }
    if item.feed is FeedType.LONG_SHORT_RATIO:
        return {
            "feed": item.feed,
            "population": value["population"],
            "sampling_interval": value["sampling_interval"],
            "long_value": _decimal(value["long_value"]),
            "short_value": _decimal(value["short_value"]),
            "long_short_ratio": _decimal(value["long_short_ratio"]),
            "value_unit": value["value_unit"],
        }
    if item.feed is FeedType.TAKER_FLOW:
        return {
            "feed": item.feed,
            "sampling_interval": value["sampling_interval"],
            "buy_volume": _decimal(value["buy_volume"]),
            "sell_volume": _decimal(value["sell_volume"]),
            "buy_sell_ratio": _decimal(value["buy_sell_ratio"]),
            "quantity_unit": value["quantity_unit"],
        }
    if item.feed is FeedType.BASIS:
        return {
            "feed": item.feed,
            "kind": value["kind"],
            "sampling_interval": value["sampling_interval"],
            "basis": _decimal(value["basis"]),
            "basis_unit": value["basis_unit"],
            "annualized_basis": (
                _decimal(value["annualized_basis"])
                if value.get("annualized_basis") is not None
                else None
            ),
            "reference_instrument_uid": value.get("reference_instrument_uid", ""),
            "formula_id": value.get("formula_id", ""),
            "input_instrument_uids": list(value.get("input_instrument_uids", [])),
        }
    if item.feed is FeedType.CONTRACT_METADATA:
        return {
            "feed": item.feed,
            "contract_kind": value["contract_kind"],
            "settlement_asset": value["settlement_asset"],
            "contract_multiplier": _decimal(value["contract_multiplier"]),
            "price_tick": _decimal(value["price_tick"]),
            "quantity_step": _decimal(value["quantity_step"]),
            "expiry_time_ns": value.get("expiry_time_ns"),
            "funding_interval_ns": value.get("funding_interval_ns"),
            "continuous": bool(value.get("continuous", False)),
            "underlying_instrument_uid": value.get("underlying_instrument_uid", ""),
        }
    if item.feed is FeedType.TICKER:
        result = {"feed": item.feed, "last_price": _decimal(value["last_price"])}
        for field in ("last_quantity", "open_24h", "high_24h", "low_24h", "volume_24h"):
            result[field] = _decimal(value[field]) if value.get(field) is not None else None
        result["last_quantity_unit"] = value.get("last_quantity_unit")
        result["volume_24h_unit"] = value.get("volume_24h_unit")
        return result
    raise ValueError(f"public typed payload is undefined for {item.feed.value}")


_SCALARS = (str, int, float, type(None))


@lru_cache(maxsize=None)
def _field_names(kind: type) -> tuple[str, ...]:
    return tuple(field.name for field in dataclass_fields(kind))


def _flat_asdict(value) -> dict:
    """``dataclasses.asdict`` for a dataclass of scalars and tuples of scalars.

    ``asdict`` recurses and copies every value (~15 us for the three metadata
    objects of a view, P18.4 D3); for these values its result is this shallow
    dict. Anything else takes ``asdict`` itself."""

    result = {}
    for name in _field_names(type(value)):
        member = getattr(value, name)
        if not isinstance(member, _SCALARS) and not (
            type(member) is tuple and all(isinstance(element, _SCALARS) for element in member)
        ):
            return asdict(value)
        result[name] = member
    return result


def _quality_fields(item) -> dict:
    quality = {**_flat_asdict(item.quality), "flags": list(item.quality.flags)}
    if item.feed not in EXECUTION_PRICE_VALIDATION_FEEDS:
        quality["execution_eligible"] = False
    return quality


def _market_item(item) -> MarketDataView:
    quality = _quality_fields(item)
    return MarketDataView(
        instrument_uid=item.instrument_uid,
        instrument_id=item.instrument_id,
        instrument_revision=item.instrument_revision,
        feed=item.feed.value,
        interval=item.interval,
        observed_at_ns=item.observed_at_ns,
        received_at_ns=item.received_at_ns,
        revision=item.revision,
        payload=_typed_payload(item),
        # Model inputs: validated by the view with the same schemas.
        source=_flat_asdict(item.source),
        quality=quality,
        contract=_flat_asdict(item.contract),
        cursor=item.cursor,
        snapshot_id=item.snapshot_id,
        watermark_offset=item.watermark_offset,
        event_id=item.event_id,
    )


# MarketDataView fields that depend on the request (time, cursor, view);
# every other field is a function of the row's immutable content (KN-4 D30).
_VIEW_DYNAMIC_FIELDS = ("quality", "cursor", "snapshot_id", "watermark_offset")
# The same field definitions (types, bounds) as MarketDataView, validated
# alone once a row's static fields were validated and encoded.
_MarketDataViewDynamic = create_model(
    "MarketDataViewDynamic",
    __base__=ResponseModel,
    **{name: (MarketDataView.model_fields[name].annotation, MarketDataView.model_fields[name])
       for name in _VIEW_DYNAMIC_FIELDS},
)


# What ``json.dumps(value, ensure_ascii=False, allow_nan=False, indent=None,
# separators=(",", ":"))`` builds per call (the ``JSONResponse`` encoding);
# one stateless instance, without a constructor per row.
_JSON_ENCODER = json.JSONEncoder(ensure_ascii=False, allow_nan=False, indent=None, separators=(",", ":"))


def _encode_json(value) -> bytes:
    return _JSON_ENCODER.encode(value).encode("utf-8")


_VIEW_DYNAMIC_PREFIX = {name: _encode_json(name) + b":" for name in _VIEW_DYNAMIC_FIELDS}
_RENDERED_ONCE = object()


def _view_layout(dump: dict) -> tuple:
    """A view's members in field order: each run of static members encoded
    once as ``"k":v,...`` bytes, each dynamic member as its field name."""

    parts, run = [], {}
    for key, value in dump.items():
        if key in _VIEW_DYNAMIC_FIELDS:
            if run:
                parts.append(_encode_json(run)[1:-1])
                run = {}
            parts.append(key)
        else:
            run[key] = value
    if run:
        parts.append(_encode_json(run)[1:-1])
    return tuple(parts)


def _splice_view(layout: tuple, dynamic: dict) -> bytes:
    return b"{" + b",".join(
        part if isinstance(part, bytes) else _VIEW_DYNAMIC_PREFIX[part] + _encode_json(dynamic[part])
        for part in layout
    ) + b"}"


class _ModelPath(Exception):
    """A row the BAR fast render cannot prove valid; the model path decides."""


# BAR fast render (P18.4 G5 cold): the public rules a BAR view's fields must
# meet (same Literal sets as the models; lengths/patterns pinned by tests).
_JSON_TEXT = json.encoder.encode_basestring
_QUALITY_STATES = frozenset(get_args(QualityView.model_fields["state"].annotation))
_RECENCY_STATES = frozenset(get_args(QualityView.model_fields["event_recency_state"].annotation))
_SESSION_STATES = frozenset(get_args(QualityView.model_fields["provider_session_state"].annotation))
_SOURCE_ROLES = frozenset(get_args(SourceView.model_fields["source_role"].annotation))
_BAR_ORIGINS = frozenset(get_args(BarPayload.model_fields["origin"].annotation))
_QUANTITY_UNITS = frozenset(unit.value for unit in QuantityUnit)
_SCHEMA_DIGEST = re.compile(r"[0-9a-f]{64}")
_EVENT_ID = re.compile(r"(?:[0-9a-f]{2}){1,64}")
# ``_quality_fields`` keeps a BAR row's own ``execution_eligible`` only while BAR is one of these.
_BAR_KEEPS_ELIGIBILITY = FeedType.BAR in EXECUTION_PRICE_VALIDATION_FEEDS


def _text(value, low: int = 0, high: int | None = None) -> str:
    if type(value) is not str or len(value) < low or (high is not None and len(value) > high):
        raise _ModelPath
    return _JSON_TEXT(value)


def _integer(value, low: int) -> str:
    if type(value) is not int or value < low:
        raise _ModelPath
    return str(value)


def _flag(value) -> str:
    if type(value) is not bool:
        raise _ModelPath
    return "true" if value else "false"


def _member(value, allowed: frozenset) -> str:
    # Literal/enum members are ASCII identifiers: no escaping applies.
    if type(value) is not str or value not in allowed:
        raise _ModelPath
    return '"' + value + '"'


def _bar_decimal(value) -> str:
    """``DecimalValue`` JSON of a plain decimal text, or the model path for any other."""

    if type(value) is not str or len(value) > 128:
        raise _ModelPath
    plain = PLAIN_DECIMAL_TEXT.fullmatch(value)
    if plain is None:
        raise _ModelPath
    fraction = plain.group(2) or ""
    coefficient = str(int(plain.group(1) + fraction))
    # The model's check ``Decimal(coefficient).scaleb(-scale) == Decimal(text)``
    # is exact (always true) while the coefficient fits the context precision.
    if len(fraction) > 38 or len(coefficient) - (coefficient[0] == "-") > getcontext().prec:
        raise _ModelPath
    return f'{{"coefficient":"{coefficient}","scale":{len(fraction)},"source_text":"{value}"}}'


def _optional_decimal(value) -> str:
    return "null" if value is None else _bar_decimal(value)


def _bar_quality(quality) -> str:
    flags = quality.flags
    if type(flags) not in (tuple, list):
        raise _ModelPath
    liveness = quality.provider_session_liveness_ms
    proof = quality.proof_observed_at_ns
    return (
        '{"state":' + _member(quality.state, _QUALITY_STATES)
        + ',"freshness_ms":' + _integer(quality.freshness_ms, 0)
        + ',"event_recency_state":' + _member(quality.event_recency_state, _RECENCY_STATES)
        + ',"provider_session_state":' + _member(quality.provider_session_state, _SESSION_STATES)
        + ',"provider_session_liveness_ms":' + ("null" if liveness is None else _integer(liveness, 0))
        + ',"gap_open":' + _flag(quality.gap_open)
        + ',"complete":' + _flag(quality.complete)
        + ',"execution_eligible":' + _flag(quality.execution_eligible)
        + ',"policy_id":' + _text(quality.policy_id, 1, 200)
        + ',"flags":[' + ",".join(_text(flag) for flag in flags) + "]"
        + ("" if proof is None else ',"proof_observed_at_ns":' + _integer(proof, 1))
        + "}"
    )


def _bar_view_parts(item) -> tuple[tuple, dict] | None:
    """A BAR view's ``(layout, encoded dynamic members)`` without pydantic, or
    ``None`` when any field is outside the shapes proven here (model path)."""

    try:
        value = item.payload
        lifecycle = item.bar_lifecycle
        supersedes = item.supersedes_event_id
        if (item.feed is not FeedType.BAR or not _BAR_KEEPS_ELIGIBILITY or item.resample_lineage is not None
                or type(lifecycle) is not BarLifecycle or lifecycle is BarLifecycle.UNSPECIFIED
                or (lifecycle is BarLifecycle.REVISED and not supersedes)):
            raise _ModelPath
        interval = _text(item.interval, 1, 20)
        revision = _integer(item.revision, 0)
        opened, closed = value["open_time_ns"], value["close_time_ns"]
        if type(closed) is not int or type(opened) is not int or not 0 < opened < closed:
            raise _ModelPath
        payload = (
            '{"feed":"BAR","interval":' + interval
            + ',"open_time_ns":' + str(opened) + ',"close_time_ns":' + str(closed)
            + ',"open":' + _bar_decimal(value["open"]) + ',"high":' + _bar_decimal(value["high"])
            + ',"low":' + _bar_decimal(value["low"]) + ',"close":' + _bar_decimal(value["close"])
            + ',"volume":' + _bar_decimal(value["volume"])
            + ',"volume_unit":' + _member(value["volume_unit"], _QUANTITY_UNITS)
            + ',"base_volume":' + _optional_decimal(value.get("base_volume"))
            + ',"quote_volume":' + _optional_decimal(value.get("quote_volume"))
            + ',"contract_volume":' + _optional_decimal(value.get("contract_volume"))
            + ',"trade_count":' + _integer(value.get("trade_count", 0), 0)
            + ',"lifecycle":"' + lifecycle.value + '","revision":' + revision
            + ',"origin":' + _member(value["origin"], _BAR_ORIGINS)
            + ',"supersedes_event_id":' + ("null" if supersedes is None else _text(supersedes))
            + ',"resample_lineage":null}'
        )
        source = item.source
        contract = item.contract
        received = item.received_at_ns
        if type(contract.schema_digest) is not str or not _SCHEMA_DIGEST.fullmatch(contract.schema_digest):
            raise _ModelPath
        head = (
            '"instrument_uid":' + _text(item.instrument_uid)
            + ',"instrument_id":' + _text(item.instrument_id)
            + ',"instrument_revision":' + _integer(item.instrument_revision, 1)
            + ',"feed":"BAR","interval":' + interval
            + ',"observed_at_ns":' + _integer(item.observed_at_ns, 1)
            + ',"received_at_ns":' + ("null" if received is None else _integer(received, 1))
            + ',"revision":' + revision + ',"payload":' + payload
            + ',"source":{"venue":' + _text(source.venue, 1, 40)
            + ',"provider":' + _text(source.provider, 1, 80)
            + ',"source_id":' + _text(source.source_id, 1, 120)
            + ',"source_role":' + _member(source.source_role, _SOURCE_ROLES)
            + ',"authoritative":' + _flag(source.authoritative) + "}"
        )
        tail = (
            '"contract":{"schema_digest":"' + contract.schema_digest
            + '","contract_version":' + _text(contract.contract_version, 1, 40)
            + ',"normalizer_version":' + _text(contract.normalizer_version, 1, 80)
            + ',"adapter_version":' + _text(contract.adapter_version, 1, 80)
            + ',"instrument_catalog_revision":' + _integer(contract.instrument_catalog_revision, 1)
            + ',"source_policy_revision":' + _integer(contract.source_policy_revision, 1)
            + ',"authority_revision":' + _integer(contract.authority_revision, 1)
            + ',"config_revision":' + _integer(contract.config_revision, 1)
            + ',"correlation_id":' + _text(contract.correlation_id, 1, 200) + "}"
        )
        event_id = item.event_id
        if event_id is not None and (type(event_id) is not str or not _EVENT_ID.fullmatch(event_id)):
            raise _ModelPath
        cursor, snapshot_id = item.cursor, item.snapshot_id
        dynamic = {
            "quality": _bar_quality(item.quality).encode("utf-8"),
            "cursor": ("null" if cursor is None else _text(cursor)).encode("utf-8"),
            "snapshot_id": ("null" if snapshot_id is None else _text(snapshot_id)).encode("utf-8"),
            "watermark_offset": _integer(item.watermark_offset, 0).encode("utf-8"),
        }
        layout = (
            head.encode("utf-8"), "quality", tail.encode("utf-8"), "cursor", "snapshot_id", "watermark_offset",
            ('"event_id":' + ("null" if event_id is None else '"' + event_id + '"')).encode("utf-8"),
        )
    except (_ModelPath, KeyError, TypeError, ValueError, AttributeError):
        # Includes an unencodable (lone surrogate) text: the model path raises its own error.
        return None
    return layout, dynamic


def _market_item_json(item) -> bytes:
    """``_encode_json(_market_item(item).model_dump(mode="json", by_alias=True))``.

    P18.4 D3 (measured): building, validating and dumping a ``MarketDataView``
    was ~170 us per row, two thirds of a warm 2,500-row warmup, and most of it
    re-derives the row's immutable fields (payload decimals, source, contract)
    on every request. An item from the market-cache backend carries its row's
    ``render_slot``: a render validates the whole view as before, and from
    the row's second render on the encoded static members are kept there
    (~1.7 KB per row, bounded and evicted with the row cache); later renders
    validate only the request-dependent fields (same field definitions) and
    splice them in. The view's cross-field rule reads static
    fields only. Bytes are identical; items without a slot take the full path.
    """

    slot = item.render_slot
    layout = slot.value if slot is not None else None
    if (layout is None or layout is _RENDERED_ONCE) and item.feed is FeedType.BAR:
        # G5 cold: a BAR row proven valid field by field is encoded directly,
        # byte-identical to the model path below (tests cover every BAR shape).
        parts = _bar_view_parts(item)
        if parts is not None:
            fast, dynamic = parts
            if slot is not None:
                slot.value = fast if layout is _RENDERED_ONCE else _RENDERED_ONCE
            return b"{" + b",".join(
                part if isinstance(part, bytes) else _VIEW_DYNAMIC_PREFIX[part] + dynamic[part] for part in fast
            ) + b"}"
    if layout is None:
        # A row's first render keeps no layout: a row read once pays no
        # layout encoding. Pydantic's own JSON serializer writes exactly the
        # bytes of ``_encode_json(model_dump(mode="json"))`` (compact, UTF-8,
        # same escapes; checked over every BAR shape and all BMP characters
        # in tests) at half the cost of a Python dump plus ``json.dumps``.
        if slot is not None:
            slot.value = _RENDERED_ONCE
        return _market_item(item).model_dump_json(by_alias=True).encode("utf-8")
    if layout is _RENDERED_ONCE:
        dump = _market_item(item).model_dump(mode="json", by_alias=True)
        slot.value = layout = _view_layout(dump)
        return _splice_view(layout, dump)
    return _splice_view(layout, _MarketDataViewDynamic(
        quality=_quality_fields(item),
        cursor=item.cursor,
        snapshot_id=item.snapshot_id,
        watermark_offset=item.watermark_offset,
    ).model_dump(mode="json", by_alias=True))


def _warmup(result) -> WarmupResponse:
    return _warmup_envelope(result).model_copy(
        update={"data": [_market_item(item) for item in _cold_rows(result.history.items)]}
    )


def _warmup_envelope(result) -> WarmupResponse:
    """The warmup response without its rows (``data`` is rendered per chunk)."""

    history = result.history
    return WarmupResponse(
        request_id=result.request_id,
        snapshot_id=history.snapshot_id,
        data_as_of_ns=history.data_as_of_ns,
        stream_cursor=history.stream_cursor,
        watermark_offset=history.watermark_offset,
        coverage=history.coverage.value,
        count=len(history.items),
        data=[],
    )


async def _json_off_loop(build) -> JSONResponse:
    """Build, dump and encode a large public response on a worker thread.

    A 5,000-row BAR warmup is about 10 MB and seconds of Pydantic work. Done on
    the event loop it stalls every other request on the replica: measured on
    2026-09-23 (v2.1.1 Phase-3 stage 5), QUOTE snapshots reached 6.5 s while
    ``query_v2_1`` sat at its 1.0 CPU cap during two cold warmups. The bytes
    are exactly those of ``JSONResponse(content=model.model_dump(mode="json",
    by_alias=True))``; only the thread that computes them changes.
    """

    # The render thread holds the caller's local lease until it returns, also
    # when the request is cancelled (KN-4 D39): an abandoned render may not
    # overlap the next admitted one.
    return await await_in_thread(
        _RENDER_EXECUTOR,
        lambda: JSONResponse(content=build().model_dump(mode="json", by_alias=True)),
    )


_RENDER_CHUNK_ROWS = 250
# Large responses render on two fixed threads, not on the loop's default
# executor (up to cpu_count + 4 threads): each thread that renders a 10k-row
# warmup grows its own malloc arena to the render's peak and keeps it, so the
# number of render threads bounds the replica's retained memory (KN-4 D39,
# measured). The local batch lease already admits one large render at a time.
_RENDER_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="qdl-query-render")


def _cold_rows(values):
    for value in values:
        cold_yield()
        yield value


def _render_warmup_chunked(model: WarmupResponse) -> bytes:
    """Encode a warmup model in bounded pieces, byte-identical to ``JSONResponse``."""

    rows = model.data
    return _render_chunked(model, len(rows), lambda start, stop: _cold_rows(rows[start:stop]))


def _render_warmup_result(result) -> bytes:
    """Encode a warmup result directly, one chunk of public views at a time.

    Only ``_RENDER_CHUNK_ROWS`` views exist at once: building every
    ``MarketDataView`` first held ~11 KB per row (110 MB for 10,000 rows,
    measured KN-4 D39) next to the rendered text. Bytes are identical to
    ``_render_warmup_chunked(_warmup(result))``.
    """

    items = result.history.items
    return _render_encoded_chunks(
        _warmup_envelope(result),
        len(items),
        lambda start, stop: b",".join(_market_item_json(item) for item in _cold_rows(items[start:stop])),
    )


def _render_chunked(envelope: WarmupResponse, count: int, views) -> bytes:
    """Encode a warmup in bounded pieces, byte-identical to ``JSONResponse``.

    One ``model_dump`` and one ``json.dumps`` over 5,000 rows are two C-level
    calls that each hold the GIL for most of a second, and no switch interval
    can preempt them: on 2026-09-23 (v2.1.1 Phase-3 stage 5) every probe on
    the rendering replica took ~1.4 s at once. Per-chunk calls hand the GIL
    back between pieces. ``data`` is the model's last field, so the envelope
    is rendered with an empty list and the rows are spliced into it. Each
    piece is encoded to UTF-8 as it is made, so the text of the whole body
    never exists next to its bytes.
    """

    def rows(start: int, stop: int) -> bytes:
        return _encode_json([view.model_dump(mode="json", by_alias=True) for view in views(start, stop)])[1:-1]

    return _render_encoded_chunks(envelope, count, rows)


def _render_encoded_chunks(envelope: WarmupResponse, count: int, rows) -> bytes:
    """Splice ``rows(start, stop)`` (the comma-joined encoded rows) into the envelope."""

    suffix = b',"data":[]}'
    head = _encode_json(envelope.model_copy(update={"data": []}).model_dump(mode="json", by_alias=True))
    if not head.endswith(suffix):
        raise RuntimeError("warmup response layout no longer ends with its data list")
    parts = [head[: -len(suffix)], b',"data":[']
    for start in range(0, count, _RENDER_CHUNK_ROWS):
        if start:
            parts.append(b",")
        parts.append(rows(start, start + _RENDER_CHUNK_ROWS))
    parts.append(b"]}")
    return b"".join(parts)


async def _warmup_json_off_loop(build) -> Response:
    """A warmup response rendered off the loop and in chunks (see above).

    ``build`` returns the (cursor-bound) warmup result, not its model.
    """

    # The render thread holds the local lease until it returns, also when the
    # request is cancelled (KN-4 K4-T06).
    body = await await_in_thread(
        _RENDER_EXECUTOR, lambda: _render_warmup_result(build()), cold=True
    )
    return Response(content=body, media_type="application/json")


async def _single_warmup_response(
    request, access, service, requirement, purpose, *, redirect: bool = False
):
    """One warmup, rendered inside the local lease exactly like ``warmup:batch``.

    Holding the lease through rendering keeps at most one large response being
    built per replica, so moving the work off the loop does not also remove its
    memory bound.
    """

    async def render(batch_result):
        (item,) = batch_result.results
        if item.problem is not None:
            raise QueryServiceError(
                item.problem,
                request_id=batch_result.request_id,
                instrument_uid=requirement.instrument_uid,
            )
        # Cursor binding copies every item and reads the durable watermark, so
        # it belongs off the loop with the rest of the response.
        return await _warmup_json_off_loop(lambda: type(item.result)(
            item.result.request_id,
            _bind_history_cursor(request, access, requirement, item.result.history),
        ))

    complete = getattr(service, "warmup_batch_completed_async", None)
    if callable(complete):
        # The opt-in is passed only when requested, so service doubles that
        # predate it keep working for every request without the header.
        return await complete(
            BatchRequirement(access.consumer_id, (requirement,), require_all=True),
            purpose=purpose,
            completion=render,
            **({"redirect_when_busy": True} if redirect else {}),
        )
    # Service doubles predating the completion hook keep the original path.
    result = await service.warmup_async(
        requirement,
        purpose=purpose,
        consumer_id=access.consumer_id,
    )
    result = type(result)(
        result.request_id,
        _bind_history_cursor(request, access, requirement, result.history),
    )
    return _warmup(result)


def _warmup_batch_response(
    request: Request,
    access: DataPlaneAccess,
    result,
    requirements: tuple[DataRequirement, ...],
) -> BatchResponse:
    """Build the public batch contract before a local batch lease is released."""

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


_WARMUP_BATCH_ROUTE = "/v2/market-data/warmup:batch"
_REFERENCE_BATCH_ROUTE = "/v2/market-data/reference:batch"


def _record_batch_item_refusal(
    origin: str,
    problem: ProblemDetails,
    *,
    feed: object,
    route: str,
    extra_reasons: tuple[object, ...] = (),
) -> None:
    """Astra R6 extension: a per-item failure inside a 200 batch leaves the same line.

    The batch envelope succeeds, so the Query problem handler never sees these
    item problems; without this line a ``require_all`` client's PARTIAL_RESULT
    (stage 100g, 2026-10-10) could not be joined to the item that failed.
    """

    REFUSALS.record(
        origin=origin,
        code=problem.code,
        detail=problem.detail,
        status=problem.status,
        retryable=problem.retryable,
        request_id=problem.request_id,
        instrument_uid=problem.instrument_uid,
        route=route,
        feed=str(getattr(feed, "value", feed)) if feed is not None else None,
        extra_reasons=extra_reasons,
    )


def _render_warmup_batch(request, access, result, requirements) -> bytes:
    """Render bounded row chunks per item, preserving the existing batch bytes."""
    def encode(value):
        return json.dumps(value, ensure_ascii=False, allow_nan=False,
                          separators=(",", ":")).encode("utf-8")

    bodies, item_models = [], []
    for item, requirement in zip(result.results, requirements, strict=True):
        cold_yield()
        problem = None if item.problem is None else _problem(QueryServiceError(
            item.problem, request_id=result.request_id, instrument_uid=item.instrument_uid))
        if problem is not None:
            _record_batch_item_refusal("warmup_batch_item", problem,
                                       feed=getattr(requirement, "feed", None),
                                       route=_WARMUP_BATCH_ROUTE,
                                       extra_reasons=(getattr(item, "reason", None),))
        item_model = BatchItemResponse(instrument_uid=item.instrument_uid,
            status=item.status, data=None, problem=problem)
        item_models.append(item_model)
        metadata = item_model.model_dump(mode="json", by_alias=True)
        data = b"null"
        if item.result is not None:
            bound = type(item.result)(item.result.request_id,
                _bind_history_cursor(request, access, requirement, item.result.history))
            data = _render_warmup_result(bound)
        bodies.append(b"{" + b",".join(encode(key) + b":" + (data if key == "data" else encode(value))
            for key, value in metadata.items()) + b"}")
    envelope = BatchResponse(request_id=result.request_id, partial=result.partial,
        success_count=result.success_count, error_count=result.error_count, results=item_models)
    return b"{" + b",".join(encode(key) + b":" +
        (b"[" + b",".join(bodies) + b"]" if key == "results" else encode(value))
        for key, value in envelope.model_dump(mode="json", by_alias=True).items()) + b"}"


async def _warmup_batch_json_off_loop(request, access, result, requirements) -> Response:
    body = await await_in_thread(_RENDER_EXECUTOR,
        lambda: _render_warmup_batch(request, access, result, requirements), cold=True)
    return Response(content=body, media_type="application/json")


def _reference_data(result) -> dict:
    """Serialize provider-authentic reference data without float coercion."""

    return {
        "instrument_uid": result.request.instrument.instrument_uid,
        "product": result.request.product.value,
        "status": result.status.value,
        "lineage": [asdict(item) for item in result.lineage],
        "coverage": asdict(result.coverage),
        "received_at_ns": result.received_at_ns,
        "observations": [
            {
                "instrument_uid": item.instrument_uid,
                "instrument_revision": item.instrument_revision,
                "product": item.product.value,
                "observed_at_ns": item.observed_at_ns,
                "fields": [
                    {
                        "name": field.name,
                        "value": _decimal(field.value.source_text),
                        "unit": field.unit,
                    }
                    for field in item.fields
                ],
                "labels": dict(item.labels),
            }
            for item in result.observations
        ],
        "error_code": result.error_code,
        "error_detail": result.error_detail,
        "cache_hit": result.cache_hit,
        "coalesced": result.coalesced,
    }


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
        diagnostics=error.diagnostics,
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
    event_recency_policy: StalePolicy | None,
    max_session_liveness_ms: int | None,
    require_full_coverage: bool,
    require_final_bars: bool,
    stale_policy: StalePolicy,
    gap_policy: GapPolicy,
    recovery: RecoveryPolicy,
    bar_revision_policy: BarRevisionPolicy,
    warmup: WarmupSpecification | None = None,
) -> DataRequirement:
    return DataRequirement(
        instrument_uid=instrument_uid,
        feed=feed,
        consumer_grade=consumer_grade,
        source_policy_id=source_policy_id,
        interval=interval,
        warmup_limit=warmup_limit,
        max_freshness_ms=max_freshness_ms,
        event_recency_policy=event_recency_policy,
        max_session_liveness_ms=max_session_liveness_ms,
        require_full_coverage=require_full_coverage,
        require_final_bars=require_final_bars,
        stale_policy=stale_policy,
        gap_policy=gap_policy,
        recovery=recovery,
        bar_revision_policy=bar_revision_policy,
        warmup=warmup,
    )


def _warmup_specification(
    *,
    limit: int | None,
    start_time_ns: int | None,
    end_time_ns: int | None,
    interval_source_policy: IntervalSourcePolicy,
    max_cache_age_ms: int,
    deadline_ms: int,
) -> WarmupSpecification:
    if (start_time_ns is None) != (end_time_ns is None):
        raise ValueError("start_time_ns and end_time_ns must be provided together")
    if start_time_ns is not None:
        if limit is not None:
            raise ValueError("time-range warmup cannot also declare limit")
        return WarmupSpecification(
            time_range=WarmupTimeRange(start_time_ns, end_time_ns),
            interval_source_policy=interval_source_policy,
            max_cache_age_ms=max_cache_age_ms,
            deadline_ms=deadline_ms,
        )
    return WarmupSpecification.for_rows(
        limit or 1000,
        interval_source_policy=interval_source_policy,
        max_cache_age_ms=max_cache_age_ms,
        deadline_ms=deadline_ms,
    )


@router.get("/market-data/{instrument_uid}/snapshot", response_model=SnapshotResponse)
async def snapshot(
    request: Request,
    instrument_uid: str,
    feed: FeedType,
    source_policy_id: str,
    consumer_grade: ConsumerGrade = ConsumerGrade.ALPHA,
    interval: str | None = None,
    max_freshness_ms: int | None = Query(
        None, gt=0, le=MAX_REQUIREMENT_FRESHNESS_MS
    ),
    event_recency_policy: StalePolicy | None = None,
    max_session_liveness_ms: int | None = Query(None, gt=0, le=86_400_000),
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
        interval, 0, max_freshness_ms, event_recency_policy,
        max_session_liveness_ms, require_full_coverage,
        require_final_bars, stale_policy, gap_policy, recovery,
        bar_revision_policy,
    )
    access.require_permission(DataPlanePermission.SNAPSHOT_READ)
    access.require_purpose(purpose)
    access.require_requirement(requirement)
    result = await service.snapshot_async(
        requirement,
        purpose=purpose,
        consumer_id=access.consumer_id,
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
    limit: int | None = Query(None, ge=1, le=10_000),
    start_time_ns: int | None = Query(None, gt=0),
    end_time_ns: int | None = Query(None, gt=0),
    interval_source_policy: IntervalSourcePolicy = (
        IntervalSourcePolicy.NATIVE_OR_EXACT_RESAMPLE
    ),
    max_cache_age_ms: int = Query(60_000, ge=0, le=86_400_000),
    deadline_ms: int = Query(20_000, ge=100, le=120_000),
    max_freshness_ms: int | None = Query(
        None, gt=0, le=MAX_REQUIREMENT_FRESHNESS_MS
    ),
    event_recency_policy: StalePolicy | None = None,
    max_session_liveness_ms: int | None = Query(None, gt=0, le=86_400_000),
    require_full_coverage: bool = True,
    require_final_bars: bool = True,
    stale_policy: StalePolicy = StalePolicy.BLOCK,
    gap_policy: GapPolicy = GapPolicy.BLOCK,
    recovery: RecoveryPolicy = RecoveryPolicy.SNAPSHOT_AND_REPLAY,
    bar_revision_policy: BarRevisionPolicy = BarRevisionPolicy.LATEST,
    purpose: AccessPurpose = Depends(_purpose),
    service: V2QueryService = Depends(_service),
    access: DataPlaneAccess = Depends(_data_access),
    redirect: bool = Depends(_replica_redirect),
):
    try:
        warmup_spec = _warmup_specification(
            limit=limit,
            start_time_ns=start_time_ns,
            end_time_ns=end_time_ns,
            interval_source_policy=interval_source_policy,
            max_cache_age_ms=max_cache_age_ms,
            deadline_ms=deadline_ms,
        )
    except ValueError as error:
        raise QueryServiceError(
            QueryProblem(CanonicalErrorCode.INVALID_ARGUMENT, str(error), False),
            request_id=service.request_id(),
            instrument_uid=instrument_uid,
        ) from error
    requirement = _query_requirement(
        instrument_uid, feed, consumer_grade, source_policy_id,
        interval, 0, max_freshness_ms, event_recency_policy,
        max_session_liveness_ms, require_full_coverage,
        require_final_bars, stale_policy, gap_policy, recovery,
        bar_revision_policy, warmup_spec,
    )
    access.require_permission(DataPlanePermission.HISTORY_READ)
    access.require_purpose(purpose)
    access.require_requirement(requirement)
    return await _single_warmup_response(
        request, access, service, requirement, purpose, redirect=redirect
    )


@router.get("/market-data/{instrument_uid}/history", response_model=WarmupResponse)
async def history(
    request: Request,
    instrument_uid: str,
    feed: FeedType,
    source_policy_id: str,
    consumer_grade: ConsumerGrade = ConsumerGrade.RESEARCH,
    interval: str | None = None,
    limit: int | None = Query(None, ge=1, le=10_000),
    start_time_ns: int | None = Query(None, gt=0),
    end_time_ns: int | None = Query(None, gt=0),
    interval_source_policy: IntervalSourcePolicy = (
        IntervalSourcePolicy.NATIVE_OR_EXACT_RESAMPLE
    ),
    max_cache_age_ms: int = Query(60_000, ge=0, le=86_400_000),
    deadline_ms: int = Query(20_000, ge=100, le=120_000),
    max_freshness_ms: int | None = Query(
        None, gt=0, le=MAX_REQUIREMENT_FRESHNESS_MS
    ),
    event_recency_policy: StalePolicy | None = None,
    max_session_liveness_ms: int | None = Query(None, gt=0, le=86_400_000),
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
    try:
        warmup_spec = _warmup_specification(
            limit=limit,
            start_time_ns=start_time_ns,
            end_time_ns=end_time_ns,
            interval_source_policy=interval_source_policy,
            max_cache_age_ms=max_cache_age_ms,
            deadline_ms=deadline_ms,
        )
    except ValueError as error:
        raise QueryServiceError(
            QueryProblem(CanonicalErrorCode.INVALID_ARGUMENT, str(error), False),
            request_id=service.request_id(),
            instrument_uid=instrument_uid,
        ) from error
    requirement = _query_requirement(
        instrument_uid, feed, consumer_grade, source_policy_id,
        interval, 0, max_freshness_ms, event_recency_policy,
        max_session_liveness_ms, require_full_coverage,
        require_final_bars, stale_policy, gap_policy, recovery,
        bar_revision_policy, warmup_spec,
    )
    access.require_permission(DataPlanePermission.HISTORY_READ)
    access.require_purpose(purpose)
    access.require_requirement(requirement)
    return await _single_warmup_response(request, access, service, requirement, purpose)


@router.post("/market-data/warmup:batch", response_model=BatchResponse)
async def warmup_batch(
    request: Request,
    body: BatchRequirementModel,
    purpose: AccessPurpose = Depends(_purpose),
    service: V2QueryService = Depends(_service),
    access: DataPlaneAccess = Depends(_data_access),
    redirect: bool = Depends(_replica_redirect),
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

    async def render(result):
        # Returning an already-rendered Response prevents FastAPI from doing a
        # second Pydantic walk after the fully-local service lease is released;
        # rendering off the loop keeps other requests on this replica moving.
        return await _warmup_batch_json_off_loop(request, access, result, requirements)

    complete = getattr(service, "warmup_batch_completed_async", None)
    if callable(complete):
        return await complete(
            batch,
            purpose=purpose,
            completion=render,
            **({"redirect_when_busy": True} if redirect else {}),
        )
    # Focused service doubles predating the internal completion hook retain the
    # old public service protocol. Production V2QueryService always uses it.
    return await render(await service.warmup_batch_async(batch, purpose=purpose))


@router.post("/market-data/reference:batch", response_model=ReferenceBatchResponse)
async def reference_data_batch(
    body: ReferenceBatchRequest,
    purpose: AccessPurpose = Depends(_purpose),
    service: V2QueryService = Depends(_service),
    access: DataPlaneAccess = Depends(_data_access),
):
    """Return bounded provider reference data for declared alpha/research use.

    Unlike the canonical spool route, this path is deliberately a typed,
    bounded provider query.  It carries coverage and lineage on every result,
    and never fabricates a missing metric as zero or silently substitutes a
    different venue.
    """

    access.require_consumer(body.consumer_id)
    access.require_permission(DataPlanePermission.HISTORY_READ)
    access.require_purpose(purpose)
    access.require_batch_size(len(body.requirements))
    requirements = tuple(_reference_requirement(item) for item in body.requirements)
    for requirement in requirements:
        access.require_requirement(requirement.data_requirement)
    batch = ReferenceBatchRequirement(
        body.consumer_id,
        requirements,
        require_all=body.require_all,
    )
    result = await service.reference_data_batch_async(batch, purpose=purpose)
    items = []
    for item in result.results:
        problem = None
        if item.problem is not None:
            problem = _problem(
                QueryServiceError(
                    item.problem,
                    request_id=result.request_id,
                    instrument_uid=item.requirement.instrument_uid,
                )
            )
            provider = item.result
            _record_batch_item_refusal(
                "reference_batch_item",
                problem,
                feed=item.requirement.product,
                route=_REFERENCE_BATCH_ROUTE,
                # Typed provider evidence only: the adapter's error code and
                # the coverage terminal reason (e.g. PROVIDER_PROTOCOL,
                # REACHED_REQUEST_START), never observations or prices.
                extra_reasons=(
                    getattr(provider, "error_code", None),
                    getattr(getattr(provider, "coverage", None), "terminal_reason", None),
                ) if provider is not None else (),
            )
        items.append(
            {
                "instrument_uid": item.requirement.instrument_uid,
                "product": item.requirement.product.value,
                "status": item.status,
                "data": _reference_data(item.result) if item.result is not None else None,
                "problem": problem,
            }
        )
    return ReferenceBatchResponse(
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
    max_freshness_ms: int | None = Query(
        None, gt=0, le=MAX_REQUIREMENT_FRESHNESS_MS
    ),
    event_recency_policy: StalePolicy | None = None,
    max_session_liveness_ms: int | None = Query(None, gt=0, le=86_400_000),
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
        instrument_uid, feed, consumer_grade, source_policy_id, interval, 0,
        max_freshness_ms, event_recency_policy, max_session_liveness_ms,
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
        "quality": asdict(
            await service.status_async(
                requirement,
                consumer_id=access.consumer_id,
            )
        ),
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
    result = await service.readiness_async(batch, purpose=purpose)
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


@router.get("/data-quality/gaps", response_model=GapListResponse, response_model_exclude_unset=True)
async def data_quality_gaps(
    response: Response,
    include_coverage: bool = Query(False),
    service: V2QueryService = Depends(_service),
    access: DataPlaneAccess = Depends(_data_access),
):
    access.require_permission(DataPlanePermission.QUALITY_READ)
    result = await service.open_gaps_async()
    coverage = getattr(result, "coverage_document", None)
    document = coverage() if callable(coverage) else None
    response.headers["X-QDL-Gap-Scope"] = "RETAINED_WINDOW"
    if document is not None and result.unavailable and not include_coverage:
        sample = ",".join(item["binding_id"] for item in result.unavailable[:8])
        raise QueryServiceError(QueryProblem(CanonicalErrorCode.PARTIAL_RESULT,
            f"retained scan has {len(result.unavailable)} unavailable product views: {sample}; "
            "request include_coverage=true for exact coverage; history boundaries are not assessed",
            True, retry_after_ms=1000), request_id=service.request_id())
    body = {"schema": "qdl.data-quality.gaps.v2", "items": [
        {**asdict(item), "feed": item.feed.value} for item in result]}
    if include_coverage:
        if document is None:
            raise QueryServiceError(QueryProblem(CanonicalErrorCode.PARTIAL_RESULT,
                "backend does not expose product coverage", False), request_id=service.request_id())
        body["coverage"] = document
    return body


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

    def _route_of(request: Request) -> str | None:
        route = request.scope.get("route")
        return getattr(route, "path", None)

    @app.exception_handler(DataPlaneAccessError)
    async def data_access_error_handler(_request: Request, error: DataPlaneAccessError):
        request_id = service.request_id() if service is not None else "unavailable"
        # Astra R6: the identity/quota layer's refusal, bounded and payload-free.
        REFUSALS.record(origin="data_plane_access", code=error.code, detail=error.detail,
                        status=error.status_code, retryable=error.status_code in {429, 503},
                        request_id=request_id, route=_route_of(_request))
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
        # Astra R6: one bounded, payload-free line per Query refusal (typed reasons + detail digest).
        reason = getattr(error, "reason", None)
        REFUSALS.record(origin="query", code=problem.code, detail=problem.detail, status=problem.status,
                        retryable=problem.retryable, request_id=problem.request_id,
                        instrument_uid=problem.instrument_uid, route=_route_of(_request),
                        extra_reasons=(reason,))
        headers = None
        if reason:
            # Additive response headers for a typed refusal reason (P18.4 G5:
            # REPLICA_LANE_BUSY); the problem body schema is unchanged.
            headers = {"X-QDL-Refusal-Reason": reason}
            if problem.retry_after_ms is not None:
                headers["Retry-After"] = str(-(-problem.retry_after_ms // 1000))
        return JSONResponse(
            status_code=problem.status,
            content=problem.model_dump(exclude_none=True),
            media_type="application/problem+json",
            headers=headers,
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
