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
