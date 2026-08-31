"""Strict bridge from Phase 10.4-B reference observations to V2 envelopes.

The batch adapters retain their provider-native field set.  This module only
projects a product when the existing public protobuf can represent its unit and
semantics without an invented default.  It deliberately has no provider I/O,
durable write, cache or routing side effect.
"""

from __future__ import annotations

import hashlib

from qdl.canonical.trade import (
    TradeContext,
    _decimal,
    _set_canonical_payload_hash,
    _validate_shadow_context,
    source_role_proto,
)
from qdl.common.v1 import common_pb2
from qdl.domain.event_id import deterministic_event_id
from qdl.marketdata.v2 import market_data_pb2
from qdl.reference.contracts import (
    ReferenceBatchResult,
    ReferenceField,
    ReferenceObservation,
    ReferenceProduct,
    ReferenceStatus,
)


class ReferenceProjectionError(ValueError):
    """A reference observation cannot faithfully enter the public V2 model."""


_QUANTITY_UNITS = {
    "BASE_ASSET_QUANTITY": common_pb2.QUANTITY_UNIT_BASE_ASSET,
    "QUOTE_ASSET_QUANTITY": common_pb2.QUANTITY_UNIT_QUOTE_ASSET,
    "CONTRACTS": common_pb2.QUANTITY_UNIT_CONTRACT,
    "SHARES": common_pb2.QUANTITY_UNIT_SHARE,
}

_METRIC_UNITS = {
    "RATIO": market_data_pb2.METRIC_UNIT_RATIO,
    "QUOTE_PRICE": market_data_pb2.METRIC_UNIT_PRICE,
    "BASE_ASSET_QUANTITY": market_data_pb2.METRIC_UNIT_BASE_ASSET,
    "QUOTE_ASSET_QUANTITY": market_data_pb2.METRIC_UNIT_QUOTE_ASSET,
    "CONTRACTS": market_data_pb2.METRIC_UNIT_CONTRACT,
    "QUOTE_NOTIONAL": market_data_pb2.METRIC_UNIT_NOTIONAL,
    "PERCENT": market_data_pb2.METRIC_UNIT_PERCENT,
    "BASIS_POINTS": market_data_pb2.METRIC_UNIT_BASIS_POINTS,
}

_POPULATIONS = {
    "GLOBAL_ACCOUNT": market_data_pb2.LONG_SHORT_RATIO_POPULATION_GLOBAL_ACCOUNT,
    "TOP_ACCOUNT": market_data_pb2.LONG_SHORT_RATIO_POPULATION_TOP_ACCOUNT,
    "TOP_POSITION": market_data_pb2.LONG_SHORT_RATIO_POPULATION_TOP_POSITION,
}


def canonicalize_reference_observation(
    *,
    result: ReferenceBatchResult,
    observation: ReferenceObservation,
    context: TradeContext,
) -> market_data_pb2.EventEnvelope:
    """Build one auditable V2 reference event or fail closed.

    ``TradeContext`` is reused only as the established canonical provenance
    carrier.  The caller must bind it to the exact provider capture before
    calling this function; this bridge never synthesizes a raw capture or a
    missing value.
    """

    _validate_result(result, observation, context)
    fields = {field.name: field for field in observation.fields}
    labels = dict(observation.labels)
    semantic = _semantic_digest(observation)
    sequence = f"{observation.product.value.lower()}:{observation.observed_at_ns}:{semantic[:24]}"
    envelope = market_data_pb2.EventEnvelope(
        schema_name=f"qdl.marketdata.{_feed_name(observation.product)}",
        schema_major=2,
        schema_minor=0,
        event_id=deterministic_event_id(
            (
                2,
                context.venue,
                context.market,
                context.instrument_uid,
                observation.product.value,
                context.source_id,
                sequence,
                context.raw_capture_id,
            )
        ),
        instrument_uid=context.instrument_uid,
        instrument_id=context.instrument_id,
        instrument_revision=context.instrument_revision,
        venue=context.venue,
        market=context.market,
        product_type=context.product_type,
        native_symbol=context.native_symbol,
        provider=context.provider,
        source_id=context.source_id,
        source_role=source_role_proto(context.source_role),
        lease_epoch=context.lease_epoch,
        source_event_time_ns=observation.observed_at_ns,
        received_at_ns=context.received_at_ns,
        normalized_at_ns=context.normalized_at_ns,
        published_at_ns=context.published_at_ns,
        source_sequence=sequence,
        partition_sequence=context.partition_sequence,
        normalizer_version=context.normalizer_version,
        adapter_version=context.adapter_version,
        raw_payload_hash=context.raw_frame_sha256,
        correlation_id=context.correlation_id,
        config_revision=context.config_revision,
        source_session_id=context.source_session_id,
        connection_generation=context.connection_generation,
        authority_revision=context.authority_revision,
        partition_plan_epoch=context.partition_plan_epoch,
        raw_capture_id=context.raw_capture_id,
    )
    envelope.quality_flags.append(common_pb2.QUALITY_FLAG_SOURCE_REFERENCE_ONLY)

    if observation.product is ReferenceProduct.FUNDING_RATE:
        rate = _field(fields, "funding_rate", "DIMENSIONLESS_RATE")
        envelope.funding_rate.CopyFrom(
            market_data_pb2.FundingRate(
                rate=_decimal(rate.value.source_text),
                funding_time_ns=observation.observed_at_ns,
            )
        )
    elif observation.product is ReferenceProduct.OPEN_INTEREST:
        quantity = _one_of_quantity(
            fields,
            (
                ("open_interest_contracts", "CONTRACTS"),
                ("open_interest_ccy", "BASE_ASSET_QUANTITY"),
            ),
        )
        message = market_data_pb2.OpenInterest(
            quantity=_decimal(quantity.value.source_text),
            quantity_unit=_quantity_unit(quantity.unit),
            sampling_interval=result.request.interval or "",
        )
        notional = fields.get("open_interest_quote_notional")
        if notional is not None:
            _require_unit(notional, "QUOTE_NOTIONAL")
            message.notional.CopyFrom(_decimal(notional.value.source_text))
        envelope.open_interest.CopyFrom(message)
    elif observation.product is ReferenceProduct.MARK_INDEX_PRICE:
        mark = _field(fields, "mark_price", "QUOTE_PRICE")
        index = _field(fields, "index_price", "QUOTE_PRICE")
        envelope.mark_index_price.CopyFrom(
            market_data_pb2.MarkIndexPrice(
                mark_price=_decimal(mark.value.source_text),
                index_price=_decimal(index.value.source_text),
            )
        )
    elif observation.product is ReferenceProduct.LONG_SHORT_RATIO:
        population = _POPULATIONS.get(labels.get("ratio_kind", ""))
        if population is None:
            raise ReferenceProjectionError("long-short ratio has no supported population")
        interval = _sampling_interval(result)
        envelope.long_short_ratio.CopyFrom(
            market_data_pb2.LongShortRatio(
                population=population,
                sampling_interval=interval,
                long_value=_decimal(
                    _field(fields, "long_account_ratio", "RATIO").value.source_text
                ),
                short_value=_decimal(
                    _field(fields, "short_account_ratio", "RATIO").value.source_text
                ),
                long_short_ratio=_decimal(
                    _field(fields, "long_short_ratio", "RATIO").value.source_text
                ),
                value_unit=market_data_pb2.METRIC_UNIT_RATIO,
            )
        )
    elif observation.product is ReferenceProduct.TAKER_FLOW:
        interval = _sampling_interval(result)
        buy = _field_present(fields, "buy_volume")
        sell = _field_present(fields, "sell_volume")
        if buy.unit != sell.unit:
            raise ReferenceProjectionError("taker buy/sell volume units differ")
        envelope.taker_flow.CopyFrom(
            market_data_pb2.TakerFlow(
                sampling_interval=interval,
                buy_volume=_decimal(buy.value.source_text),
                sell_volume=_decimal(sell.value.source_text),
                buy_sell_ratio=_decimal(
                    _field(fields, "buy_sell_ratio", "RATIO").value.source_text
                ),
                quantity_unit=_quantity_unit(buy.unit),
            )
        )
    elif observation.product is ReferenceProduct.BASIS:
        basis = _field_present(fields, "basis")
        annualized = fields.get("annualized_basis_rate")
        message = market_data_pb2.Basis(
            kind=market_data_pb2.BASIS_KIND_PROVIDER_NATIVE,
            sampling_interval=_sampling_interval(result),
            basis=_decimal(basis.value.source_text),
            basis_unit=_metric_unit(basis.unit),
        )
        if annualized is not None:
            _require_unit(annualized, "DIMENSIONLESS_RATE")
            message.annualized_basis.CopyFrom(_decimal(annualized.value.source_text))
        envelope.basis.CopyFrom(message)
    elif observation.product is ReferenceProduct.CONTRACT_METADATA:
        record = result.request.instrument
        contract_kind = labels.get("contract_type") or record.identity.product_type.value
        message = market_data_pb2.ContractMetadata(
            contract_kind=contract_kind,
            settlement_asset=record.settlement_asset,
            contract_multiplier=_decimal(record.contract_multiplier.source_text),
            price_tick=_decimal(
                _field(fields, "price_tick", "QUOTE_PRICE").value.source_text
            ),
            quantity_step=_decimal(
                _field(fields, "quantity_step", "CONTRACTS").value.source_text
            ),
            continuous=record.attributes.get("continuous_series", "false").lower()
            == "true",
            underlying_instrument_uid=record.underlying_instrument_uid or "",
        )
        expiry = _epoch_ms(fields.get("delivery_time_ms") or fields.get("expiry_time_ms"))
        if expiry is None:
            expiry = record.expiry_time_ns
        if expiry is not None:
            message.expiry_time_ns = expiry
        envelope.contract_metadata.CopyFrom(message)
    else:  # pragma: no cover - guarded by ReferenceProduct completeness test.
        raise ReferenceProjectionError(
            f"reference product {observation.product.value} has no V2 projection"
        )

    _set_canonical_payload_hash(envelope, enabled=bool(context.source_session_id))
    return envelope


def _validate_result(
    result: ReferenceBatchResult,
    observation: ReferenceObservation,
    context: TradeContext,
) -> None:
    _validate_shadow_context(context)
    request = result.request
    if result.status is not ReferenceStatus.OK:
        raise ReferenceProjectionError("only an OK reference result can be canonicalized")
    if (
        observation.product is not request.product
        or observation.instrument_uid != request.instrument.instrument_uid
        or observation.instrument_revision != request.instrument.metadata_revision
    ):
        raise ReferenceProjectionError("reference observation differs from its bounded request")
    if (
        context.instrument_uid != request.instrument.instrument_uid
        or context.instrument_id != request.instrument.instrument_id
        or context.instrument_revision != request.instrument.metadata_revision
        or context.venue != request.instrument.identity.venue
        or context.market != request.instrument.identity.market
        or context.product_type != request.instrument.identity.product_type.value
        or context.native_symbol != request.instrument.native_symbol
    ):
        raise ReferenceProjectionError("reference context differs from registry identity")
    if context.source_role != "REFERENCE":
        raise ReferenceProjectionError("reference canonical events require REFERENCE source role")
    if not result.lineage or any(item.source_role != "REFERENCE" for item in result.lineage):
        raise ReferenceProjectionError("reference result lineage is incomplete")
    if any(item.provider != context.provider for item in result.lineage):
        raise ReferenceProjectionError("reference lineage provider differs from context")
    if any(item.adapter_version != context.adapter_version for item in result.lineage):
        raise ReferenceProjectionError("reference lineage adapter differs from context")
    if observation.observed_at_ns > context.received_at_ns:
        raise ReferenceProjectionError("reference observation is later than receipt time")


def _field(
    fields: dict[str, ReferenceField], name: str, unit: str
) -> ReferenceField:
    field = _field_present(fields, name)
    _require_unit(field, unit)
    return field


def _field_present(fields: dict[str, ReferenceField], name: str) -> ReferenceField:
    try:
        return fields[name]
    except KeyError as error:
        raise ReferenceProjectionError(f"reference field {name} is required") from error


def _require_unit(field: ReferenceField, expected: str) -> None:
    if field.unit != expected:
        raise ReferenceProjectionError(
            f"reference field {field.name} has unit {field.unit}, expected {expected}"
        )


def _one_of_quantity(
    fields: dict[str, ReferenceField], candidates: tuple[tuple[str, str], ...]
) -> ReferenceField:
    for name, unit in candidates:
        field = fields.get(name)
        if field is not None:
            _require_unit(field, unit)
            return field
    names = ", ".join(name for name, _ in candidates)
    raise ReferenceProjectionError(f"reference quantity requires one of {names}")


def _quantity_unit(unit: str) -> int:
    try:
        return _QUANTITY_UNITS[unit]
    except KeyError as error:
        raise ReferenceProjectionError(
            f"reference quantity unit {unit} is not representable by V2"
        ) from error


def _metric_unit(unit: str) -> int:
    try:
        return _METRIC_UNITS[unit]
    except KeyError as error:
        raise ReferenceProjectionError(
            f"reference metric unit {unit} is not representable by V2"
        ) from error


def _sampling_interval(result: ReferenceBatchResult) -> str:
    interval = str(result.request.interval or "").strip()
    if not interval:
        raise ReferenceProjectionError("sampled reference product lacks interval")
    return interval


def _epoch_ms(field: ReferenceField | None) -> int | None:
    if field is None:
        return None
    _require_unit(field, "EPOCH_MILLISECONDS")
    value = field.value.as_decimal()
    if value != value.to_integral_value() or value <= 0:
        raise ReferenceProjectionError("reference expiry timestamp is invalid")
    return int(value) * 1_000_000


def _feed_name(product: ReferenceProduct) -> str:
    return {
        ReferenceProduct.FUNDING_RATE: "funding_rate",
        ReferenceProduct.OPEN_INTEREST: "open_interest",
        ReferenceProduct.MARK_INDEX_PRICE: "mark_index_price",
        ReferenceProduct.LONG_SHORT_RATIO: "long_short_ratio",
        ReferenceProduct.TAKER_FLOW: "taker_flow",
        ReferenceProduct.BASIS: "basis",
        ReferenceProduct.CONTRACT_METADATA: "contract_metadata",
    }[product]


def _semantic_digest(observation: ReferenceObservation) -> str:
    fields = "|".join(
        f"{field.name}={field.value.source_text}:{field.unit}"
        for field in sorted(observation.fields, key=lambda item: item.name)
    )
    labels = "|".join(f"{name}={value}" for name, value in observation.labels)
    return hashlib.sha256(
        f"{observation.instrument_uid}|{observation.instrument_revision}|"
        f"{observation.product.value}|{observation.observed_at_ns}|{fields}|{labels}".encode()
    ).hexdigest()
