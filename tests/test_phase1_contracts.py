from __future__ import annotations

import hashlib
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "generated" / "python"))

from qdl.domain.decimal import CanonicalDecimal  # noqa: E402
from qdl.domain.event_id import deterministic_event_id  # noqa: E402

try:
    from qdl.common.v1 import common_pb2
    from qdl.marketdata.v2 import market_data_pb2

    HAS_PROTOBUF = True
except (ImportError, ModuleNotFoundError):
    HAS_PROTOBUF = False


class CanonicalDecimalTests(unittest.TestCase):
    def test_preserves_scale_and_source_spelling_without_float(self):
        value = CanonicalDecimal.from_text("61234.10")
        self.assertEqual(value.coefficient, 6123410)
        self.assertEqual(value.scale, 2)
        self.assertEqual(str(value.as_decimal()), "61234.10")
        self.assertEqual(value.source_text, "61234.10")

    def test_uses_text_coefficient_instead_of_overflowing_int64(self):
        value = CanonicalDecimal.from_text("123456789012345678901.123")
        self.assertTrue(value.uses_text_coefficient)
        self.assertEqual(value.coefficient, "123456789012345678901123")
        self.assertEqual(value.scale, 3)

    def test_rejects_non_finite_and_invalid_values(self):
        for invalid in ("", "NaN", "Infinity", "not-a-number"):
            with self.subTest(value=invalid), self.assertRaises(ValueError):
                CanonicalDecimal.from_text(invalid)

    def test_plain_text_fast_path_equals_the_decimal_parse(self):
        # P18.4 D3: ASCII ``-?digits[.digits]`` is read off the text; every
        # other spelling takes the Decimal path. Both must agree exactly.
        from decimal import Decimal

        def by_decimal(text: str) -> CanonicalDecimal:
            sign, digits, exponent = Decimal(text).as_tuple()
            value = int("".join(map(str, digits)) or "0") * (-1 if sign else 1)
            if exponent > 0:
                value *= 10**exponent
            coefficient = value if -(2**63) <= value <= 2**63 - 1 else str(value)
            return CanonicalDecimal(coefficient, max(0, -exponent), text)

        texts = ["61234.10", "0", "-0", "-0.000", "000.10", "00", "1.0", "-12.50",
                 "123456789012345678901.123", "9223372036854775807", "9223372036854775808",
                 "-9223372036854775808", "-9223372036854775809", "0.000000000000000000000000000000001",
                 "1E+3", "-12.5e-2", "+1.5", "1_000.5", ".5", "5.", "٣.1"]
        texts += [f"{whole}.{fraction}" for whole in ("0", "7", "-31", "100000000000")
                  for fraction in ("0", "05", "1234567890", "9" * 30)]
        for text in texts:
            with self.subTest(text=text):
                parsed = CanonicalDecimal.from_text(text)
                self.assertEqual(parsed, by_decimal(text.strip()))
                self.assertEqual(type(parsed.coefficient), type(by_decimal(text.strip()).coefficient))
        for invalid in ("", "  ", "NaN", "-Infinity", "x1", "1.2.3", "--1", "1-"):
            with self.subTest(value=invalid), self.assertRaises(ValueError):
                CanonicalDecimal.from_text(invalid)


class DeterministicEventIdTests(unittest.TestCase):
    def test_is_stable_and_length_delimited(self):
        first = deterministic_event_id(["AB", "C", 1])
        self.assertEqual(first, deterministic_event_id(["AB", "C", 1]))
        self.assertNotEqual(first, deterministic_event_id(["A", "BC", 1]))
        self.assertEqual(len(first), 16)

    def test_only_approved_lengths_are_supported(self):
        with self.assertRaises(ValueError):
            deterministic_event_id(["event"], size=20)


@unittest.skipUnless(HAS_PROTOBUF, "generated contract test requires protobuf runtime")
class GeneratedContractGoldenTests(unittest.TestCase):
    def test_python_decodes_and_reencodes_golden_bytes(self):
        golden_dir = ROOT / "contracts" / "golden" / "canonical"
        payload = (golden_dir / "trade-envelope.bin").read_bytes()
        metadata = json.loads((golden_dir / "trade-envelope.json").read_text())
        message = market_data_pb2.EventEnvelope.FromString(payload)

        self.assertEqual(hashlib.sha256(payload).hexdigest(), metadata["sha256"])
        self.assertEqual(message.instrument_id, "BINANCE.USDM.PERPETUAL.BTC-USDT")
        self.assertEqual(message.event_id, bytes(range(16)))
        self.assertEqual(message.source_event_time_ns, 1_786_352_400_123_000_000)
        self.assertEqual(message.received_at_ns, 1_786_352_400_123_456_000)
        self.assertEqual(message.normalized_at_ns, 1_786_352_400_123_500_000)
        self.assertEqual(message.published_at_ns, 1_786_352_400_123_700_000)
        self.assertEqual(message.trade.native_trade_id, "184467440737095516160")
        self.assertEqual(message.trade.price.mantissa, 6_123_410)
        self.assertEqual(message.trade.price.scale, 2)
        self.assertEqual(message.trade.price.source_text, "61234.10")
        self.assertEqual(message.SerializeToString(deterministic=True), payload)

    def test_decimal_representation_has_no_float_fields(self):
        decimal_fields = common_pb2.DecimalValue.DESCRIPTOR.fields
        float_types = {
            field.TYPE_DOUBLE
            for field in decimal_fields
        } | {field.TYPE_FLOAT for field in decimal_fields}
        self.assertFalse(any(field.type in float_types for field in decimal_fields))


if __name__ == "__main__":
    unittest.main()
