from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qdl.adapters.vn import (
    VnRawBinding,
    build_dnse_bar_raw_envelope,
    build_dnse_trade_raw_envelope,
)
from qdl.canonical.trade import TradeContext, canonicalize_dnse_trade
from qdl.common.v1 import common_pb2
from qdl.raw.capture import bind_capture_context
from qdl.raw.envelope import validate_raw_envelope
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


class FakeMessage:
    def __init__(self, partition=1, offset=7):
        self._partition = partition
        self._offset = offset

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class FakeProducer:
    def __init__(self, config, *, fail=False):
        self.config = config
        self.fail = fail
        self.pending = []
        self.records = []

    def produce(self, topic, **kwargs):
        self.records.append((topic, kwargs))
        self.pending.append(kwargs["on_delivery"])

    def poll(self, _):
        return 0

    def flush(self, _):
        callbacks, self.pending = self.pending, []
        for callback in callbacks:
            callback(RuntimeError("delivery failed") if self.fail else None, FakeMessage())
        return 0


class VnRawEdgeTests(unittest.TestCase):
    def binding(self):
        return VnRawBinding(
            venue="HNX",
            market="VN_DERIVATIVES",
            product_type="FUTURE",
            native_symbol="VN30F1M",
            subscription_id="dnse-vn30-trades",
            source_session_id="dnse-session-1",
            connection_generation=1,
            lease_epoch=7,
            authority_revision=1,
            partition_plan_epoch=1,
            adapter_version="dnse-sdk/2.0.0",
            config_revision=1,
            instrument_catalog_revision=3,
        )

    def envelope(self):
        return build_dnse_trade_raw_envelope(
            {
                "symbol": "VN30F1M",
                "price": 1820.7,
                "quantity": 12,
                "totalVolumeTraded": 12000,
                "marketId": "G3",
                "boardId": "DERIVATIVES",
                "tradingSessionId": 2,
            },
            self.binding(),
            received_at_ns=1_786_352_400_123_456_000,
            test_provenance=True,
        )

    def test_dnse_sdk_delivery_preserves_honest_missing_trade_semantics(self):
        raw = self.envelope()
        validate_raw_envelope(raw)
        self.assertEqual(raw.capture_boundary, 3)
        payload = json.loads(raw.raw_frame_bytes)
        self.assertEqual(payload["price"], "1820.7")
        self.assertEqual(payload["quantity"], "12")
        context = bind_capture_context(
            TradeContext(
                instrument_uid="uid-vn30f1m",
                instrument_id="HNX.VN_DERIVATIVES.FUTURE.VN30F1M",
                instrument_revision=1,
                venue="HNX",
                market="VN_DERIVATIVES",
                product_type="FUTURE",
                native_symbol="VN30F1M",
                provider="DNSE_DIRECT",
                source_id="dnse-vn30-trade",
                source_role="PRIMARY",
                lease_epoch=7,
                received_at_ns=1,
                normalized_at_ns=2,
                published_at_ns=3,
                partition_sequence=1,
                normalizer_version="qdl-normalizer/2.0.0",
                adapter_version="dnse-sdk/2.0.0",
                config_revision=1,
            ),
            raw,
        )
        event = canonicalize_dnse_trade(payload, context)
        self.assertEqual(event.trade.quantity_unit, common_pb2.QUANTITY_UNIT_CONTRACT)
        self.assertEqual(event.trade.aggressor_side, common_pb2.AGGRESSOR_SIDE_UNSPECIFIED)
        self.assertTrue(event.trade.native_trade_id.startswith("derived:"))

    def test_rest_and_websocket_closed_bar_share_core_binding_with_honest_transport(self):
        row = {
            "t": 1_786_352_340,
            "o": "1820.7",
            "h": "1821.2",
            "l": "1820.2",
            "c": "1820.9",
            "v": "0",
        }
        rest = build_dnse_bar_raw_envelope(
            row, self.binding(), received_at_ns=1_786_352_400_000_000_000
        )
        websocket = build_dnse_bar_raw_envelope(
            row,
            self.binding(),
            received_at_ns=1_786_352_400_000_000_001,
            acquisition_origin="WEBSOCKET_CLOSED",
        )
        validate_raw_envelope(rest)
        validate_raw_envelope(websocket)
        self.assertEqual(rest.native_channel, websocket.native_channel)
        self.assertEqual(rest.raw_frame_bytes, websocket.raw_frame_bytes)
        self.assertNotEqual(rest.transport_protocol, websocket.transport_protocol)
        self.assertNotEqual(rest.capture_boundary, websocket.capture_boundary)

    def test_invalid_or_incomplete_dnse_delivery_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "symbol"):
            build_dnse_trade_raw_envelope(
                {"symbol": "FPT", "price": 1, "quantity": 1, "totalVolumeTraded": 1},
                self.binding(),
                received_at_ns=1,
            )
        with self.assertRaisesRegex(ValueError, "quantity"):
            build_dnse_trade_raw_envelope(
                {"symbol": "VN30F1M", "price": 1, "quantity": 0, "totalVolumeTraded": 1},
                self.binding(),
                received_at_ns=1,
            )

    def test_python_vendor_edge_uses_idempotent_tls_durable_ack(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("ca.crt", "client.crt", "client.key"):
                (root / name).write_text("test")
            created = []

            def factory(config):
                value = FakeProducer(config)
                created.append(value)
                return value

            publisher = KafkaRawPublisher(
                KafkaRawPublisherConfig(
                    bootstrap_servers="kafka1:9092,kafka2:9092,kafka3:9092",
                    client_id="qdl-vn-edge",
                    topic="qdl.raw.vn.v1",
                    ca_path=root / "ca.crt",
                    certificate_path=root / "client.crt",
                    key_path=root / "client.key",
                ),
                producer_factory=factory,
            )
            ack = publisher.publish_many((self.envelope(),))
            self.assertEqual(len(ack), 1)
            self.assertEqual(ack[0].offset, 7)
            self.assertTrue(created[0].config["enable.idempotence"])
            self.assertEqual(created[0].config["acks"], "all")
            self.assertEqual(created[0].records[0][0], "qdl.raw.vn.v1")
            publisher.close()

    def test_delivery_failure_is_not_reported_as_acknowledged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("ca.crt", "client.crt", "client.key"):
                (root / name).write_text("test")
            publisher = KafkaRawPublisher(
                KafkaRawPublisherConfig(
                    bootstrap_servers="kafka:9092",
                    client_id="qdl-vn-edge",
                    topic="qdl.raw.vn.v1",
                    ca_path=root / "ca.crt",
                    certificate_path=root / "client.crt",
                    key_path=root / "client.key",
                ),
                producer_factory=lambda config: FakeProducer(config, fail=True),
            )
            with self.assertRaisesRegex(RuntimeError, "durable ACK failed"):
                publisher.publish_many((self.envelope(),))


if __name__ == "__main__":
    unittest.main()
