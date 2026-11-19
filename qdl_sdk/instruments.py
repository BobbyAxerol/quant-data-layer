"""SDK 2.10.0 canonical instrument helpers (DL v2.4.1 `qdl.instrument.canonical-metadata.v1`).

Data Layer prices, quantities and bars stay venue-native. These helpers convert one native value to its underlying
unit using the instrument's served canonical metadata, exactly in `Decimal`:

- `to_underlying_price(view, price)`: native price / price_multiplier (Binance `1000PEPEUSDT` 0.0038 -> PEPE
  0.0000038; OKX `PEPE-USDT-SWAP` is already per PEPE, multiplier 1);
- `to_underlying_quantity(view, quantity)`: native quantity * quantity_multiplier (one OKX PEPE contract = 10,000,000
  PEPE; one Binance 1000PEPE unit = 1,000 PEPE);
- `same_underlying(a, b)`: both instruments name the same underlying (Binance `LUNA2USDT` and OKX `LUNA-USDT-SWAP`).

Every helper refuses with `CanonicalMetadataUnavailable` when the metadata is absent or `UNVERIFIED`; none ever
assumes 1:1.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

from qdl_sdk.errors import CanonicalMetadataUnavailable
from qdl_sdk.models import CanonicalInstrumentMetadataView, InstrumentView

__all__ = ["same_underlying", "to_underlying_price", "to_underlying_quantity", "verified_metadata"]


def verified_metadata(view: InstrumentView) -> CanonicalInstrumentMetadataView:
    """The instrument's VERIFIED canonical metadata, or a typed refusal."""
    canonical = getattr(view, "canonical", None)
    if canonical is None:
        raise CanonicalMetadataUnavailable(
            "CANONICAL_METADATA_MISSING", f"{view.instrument_id} has no canonical instrument metadata")
    if canonical.status != "VERIFIED":
        raise CanonicalMetadataUnavailable(
            "CANONICAL_METADATA_UNVERIFIED",
            f"{view.instrument_id} canonical metadata is {canonical.status} (rule {canonical.rule_id})")
    if not (canonical.underlying_asset and canonical.underlying_instrument_uid
            and canonical.price_multiplier and canonical.quantity_multiplier):
        raise CanonicalMetadataUnavailable(
            "CANONICAL_METADATA_INCOMPLETE", f"{view.instrument_id} canonical metadata lacks a required field")
    return canonical


def _decimal(value: Decimal | str | int, field: str) -> Decimal:
    if isinstance(value, float) or isinstance(value, bool):
        raise TypeError(f"{field} must be Decimal, str or int, not {type(value).__name__}")
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError) as error:
        raise ValueError(f"{field} is not a decimal") from error
    if not result.is_finite():
        raise ValueError(f"{field} must be finite")
    return result


def to_underlying_price(view: InstrumentView, price: Decimal | str | int) -> Decimal:
    canonical = verified_metadata(view)
    return _decimal(price, "price") / Decimal(canonical.price_multiplier)


def to_underlying_quantity(view: InstrumentView, quantity: Decimal | str | int) -> Decimal:
    canonical = verified_metadata(view)
    return _decimal(quantity, "quantity") * Decimal(canonical.quantity_multiplier)


def same_underlying(first: InstrumentView, second: InstrumentView) -> bool:
    return (verified_metadata(first).underlying_instrument_uid
            == verified_metadata(second).underlying_instrument_uid)
