from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


@dataclass(frozen=True)
class CanonicalDecimal:
    """Exact finite decimal with an auditable venue-native spelling."""

    coefficient: int | str
    scale: int
    source_text: str

    @classmethod
    def from_text(cls, value: str) -> "CanonicalDecimal":
        source_text = str(value).strip()
        if not source_text:
            raise ValueError("decimal value is required")
        try:
            parsed = Decimal(source_text)
        except InvalidOperation as exc:
            raise ValueError(f"invalid decimal: {source_text}") from exc
        if not parsed.is_finite():
            raise ValueError("canonical decimals must be finite")

        sign, digits, exponent = parsed.as_tuple()
        coefficient_value = int("".join(str(digit) for digit in digits) or "0")
        if sign:
            coefficient_value = -coefficient_value
        scale = max(0, -exponent)
        if exponent > 0:
            coefficient_value *= 10**exponent
        coefficient: int | str = coefficient_value
        if coefficient_value < INT64_MIN or coefficient_value > INT64_MAX:
            coefficient = str(coefficient_value)
        return cls(coefficient=coefficient, scale=scale, source_text=source_text)

    def as_decimal(self) -> Decimal:
        return Decimal(str(self.coefficient)).scaleb(-self.scale)

    @property
    def uses_text_coefficient(self) -> bool:
        return isinstance(self.coefficient, str)

