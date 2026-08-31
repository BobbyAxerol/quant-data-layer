from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation

from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentAlias,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentStatus,
    OptionType,
    ProductType,
)


_PRODUCT_TYPES = {
    "SPOT": ProductType.SPOT,
    "SWAP": ProductType.PERPETUAL,
    "FUTURES": ProductType.FUTURE,
    "OPTION": ProductType.OPTION,
    "EVENTS": ProductType.EVENT_CONTRACT,
}
_ASSET_CLASSES = {
    "SPOT": AssetClass.CRYPTO,
    "SWAP": AssetClass.DERIVATIVE,
    "FUTURES": AssetClass.DERIVATIVE,
    "OPTION": AssetClass.OPTION,
    "EVENTS": AssetClass.DERIVATIVE,
}
_STATUS = {
    "live": InstrumentStatus.ACTIVE,
    "suspend": InstrumentStatus.HALTED,
    "preopen": InstrumentStatus.PRELISTED,
    "test": InstrumentStatus.PRELISTED,
}


def _required(payload: Mapping[str, str], name: str) -> str:
    value = str(payload.get(name) or "").strip()
    if not value:
        raise ValueError(f"OKX /public/instruments missing {name}")
    return value


def _milliseconds_to_ns(value: str | None) -> int | None:
    text = str(value or "").strip()
    return int(text) * 1_000_000 if text else None


def _canonical_symbol(payload: Mapping[str, str], inst_type: str) -> str:
    inst_id = _required(payload, "instId")
    if inst_type == "SWAP":
        return _required(payload, "instFamily")
    # Dated, option and event identities preserve exact registry-provided IDs;
    # no code constructs a provider-native instrument ID from date/strike text.
    return inst_id


def parse_public_instrument(
    payload: Mapping[str, str],
    *,
    metadata_revision: int,
    valid_from_ns: int,
) -> tuple[InstrumentRecord, InstrumentAlias]:
    inst_type = _required(payload, "instType").upper()
    if inst_type not in _PRODUCT_TYPES:
        raise ValueError(f"unsupported OKX instType: {inst_type}")
    inst_id = _required(payload, "instId")
    identity = InstrumentIdentity.create(
        venue="OKX",
        market=inst_type,
        product_type=_PRODUCT_TYPES[inst_type],
        canonical_symbol=_canonical_symbol(payload, inst_type),
    )
    expiry_ns = _milliseconds_to_ns(payload.get("expTime"))
    strike_text = str(payload.get("stk") or "").strip()
    option_value = str(payload.get("optType") or "").strip().upper()
    option_type = {"C": OptionType.CALL, "CALL": OptionType.CALL, "P": OptionType.PUT, "PUT": OptionType.PUT}.get(option_value)
    ct_val_text = str(payload.get("ctVal") or "1")
    ct_mult_text = str(payload.get("ctMult") or "1")
    try:
        multiplier_text = format(Decimal(ct_val_text) * Decimal(ct_mult_text), "f")
    except InvalidOperation as exc:
        raise ValueError("OKX contract multiplier fields must be exact decimals") from exc
    family_parts = identity.canonical_symbol.split("-")
    family_base = family_parts[0] if len(family_parts) >= 2 else ""
    family_quote = family_parts[-1] if len(family_parts) >= 2 else ""
    record = InstrumentRecord(
        identity=identity,
        metadata_revision=metadata_revision,
        asset_class=_ASSET_CLASSES[inst_type],
        native_symbol=inst_id,
        base_asset=str(
            payload.get("baseCcy") or payload.get("ctValCcy") or family_base
        ).upper(),
        quote_asset=str(payload.get("quoteCcy") or family_quote).upper(),
        settlement_asset=str(
            payload.get("settleCcy") or payload.get("quoteCcy") or family_quote
        ).upper(),
        price_tick=CanonicalDecimal.from_text(_required(payload, "tickSz")),
        quantity_step=CanonicalDecimal.from_text(_required(payload, "lotSz")),
        contract_multiplier=CanonicalDecimal.from_text(multiplier_text),
        session_calendar_id="CRYPTO_24X7",
        status=_STATUS.get(str(payload.get("state") or "live").lower(), InstrumentStatus.PRELISTED),
        expiry_time_ns=expiry_ns,
        strike_price=CanonicalDecimal.from_text(strike_text) if strike_text else None,
        option_type=option_type,
        valid_from_ns=valid_from_ns,
        attributes={
            key: str(value)
            for key, value in payload.items()
            if key in {
                "instFamily",
                "uly",
                "groupId",
                "seriesId",
                "ctType",
                "ctVal",
                "ctMult",
                "ctValCcy",
                "instIdCode",
                "alias",
            }
            and value not in (None, "")
        },
    )
    alias = InstrumentAlias(
        provider="OKX_DIRECT",
        market=inst_type,
        native_symbol=inst_id,
        instrument_uid=record.instrument_uid,
        instrument_revision=metadata_revision,
        valid_from_ns=valid_from_ns,
    )
    return record, alias
