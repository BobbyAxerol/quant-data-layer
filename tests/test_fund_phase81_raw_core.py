from __future__ import annotations

import hashlib
import pathlib
import unittest

import yaml

from qdl.canonical.trade import TradeContext, canonicalize_binance_usdm_trade
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.envelope import build_raw_envelope, validate_raw_envelope


ROOT = pathlib.Path(__file__).resolve().parents[1]


class Phase81RawCoreTests(unittest.TestCase):
    def raw(self):
        return build_raw_envelope(
            capture_id=bytes(range(16)), provider="BINANCE_DIRECT",
            venue="BINANCE", market="USDM", product_type="PERPETUAL",
            native_symbol="BTCUSDT", native_channel="btcusdt@aggTrade",
            subscription_id="sub-1", source_session_id="session-1",
            connection_generation=2, lease_epoch=3, authority_revision=4,
            partition_plan_epoch=5, received_at_ns=1_000_000,
            transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_WEBSOCKET,
            transport_compression=raw_provider_pb2.TRANSPORT_COMPRESSION_NONE,
            capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_POST_DECOMPRESSION,
            raw_frame_bytes=b'{"e":"aggTrade","s":"BTCUSDT","a":1}',
            adapter_version="binance/2.0.0", config_revision=6,
            instrument_catalog_revision=7, correlation_id="corr-1",
            test_provenance=True,
        )

    def test_python_raw_envelope_matches_cross_language_golden(self):
        value = self.raw()
        validate_raw_envelope(value)
        actual = value.SerializeToString(deterministic=True)
        self.assertEqual(
            actual,
            (ROOT / "contracts/golden/phase8/raw-provider-envelope.bin").read_bytes(),
        )
        self.assertEqual(value.raw_frame_sha256, hashlib.sha256(value.raw_frame_bytes).digest())

    def test_raw_contract_rejects_missing_hash_and_plausible_defaults(self):
        value = self.raw()
        value.source_session_id = ""
        with self.assertRaisesRegex(ValueError, "source_session_id"):
            validate_raw_envelope(value)
        value = self.raw()
        value.raw_frame_sha256 = b"\x00" * 32
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            validate_raw_envelope(value)

    def test_required_binance_provider_boolean_is_not_defaulted(self):
        context = TradeContext(
            instrument_uid="i", instrument_id="id", instrument_revision=1,
            venue="BINANCE", market="USDM", product_type="PERPETUAL",
            native_symbol="BTCUSDT", provider="BINANCE_DIRECT", source_id="s",
            lease_epoch=1, received_at_ns=1, normalized_at_ns=2, published_at_ns=3,
            partition_sequence=1, normalizer_version="n", adapter_version="a",
            config_revision=1,
        )
        with self.assertRaisesRegex(ValueError, "boolean"):
            canonicalize_binance_usdm_trade(
                {"s": "BTCUSDT", "a": 1, "p": "1", "q": "1", "T": 1},
                context,
            )

    def test_phase8_metadata_and_canonical_hash_are_populated(self):
        context = TradeContext(
            instrument_uid="i", instrument_id="id", instrument_revision=1,
            venue="BINANCE", market="USDM", product_type="PERPETUAL",
            native_symbol="BTCUSDT", provider="BINANCE_DIRECT", source_id="s",
            lease_epoch=1, received_at_ns=1, normalized_at_ns=2, published_at_ns=3,
            partition_sequence=1, normalizer_version="n", adapter_version="a",
            config_revision=1, source_session_id="session-1",
            connection_generation=2, authority_revision=3, partition_plan_epoch=4,
        )
        event = canonicalize_binance_usdm_trade(
            {"s": "BTCUSDT", "a": 1, "p": "1", "q": "1", "T": 1, "m": False},
            context,
        )
        self.assertEqual(event.source_session_id, "session-1")
        self.assertEqual(event.connection_generation, 2)
        self.assertEqual(len(event.canonical_payload_hash), 32)

    def test_all_capability_manifests_are_fail_closed_and_shadow_only(self):
        paths = sorted((ROOT / "config/phase8/capabilities").glob("*.yaml"))
        self.assertEqual(len(paths), 4)
        for path in paths:
            value = yaml.safe_load(path.read_text())
            self.assertEqual(value["schema_version"], 1)
            self.assertFalse(value["authority_eligible"])
            self.assertTrue(value["supports_raw_exact_frame"])
            self.assertTrue(value["native_sequence_field"])
            self.assertTrue(value["reconnect_sequence_continuity"])


if __name__ == "__main__":
    unittest.main()
