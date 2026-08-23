from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from qdl.marketdata.v2 import market_data_pb2
from qdl.raw.envelope import build_raw_envelope
from qdl.provider.v1 import raw_provider_pb2
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    AuthorityPromotionScope,
    StableAcquisitionPlan,
)
from scripts.phasec40_collect_live_core_parity import (
    LiveSample,
    _bounded_window,
    _canonicalize,
    _context,
    _provider_frames,
    _scan_tail,
    verify_sample,
)

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
SCOPE_PATH = ROOT / "config/v2/stable-authority-promotion-scope.yaml"
FIXTURES = ROOT / "tests/fixtures/phase2"


class C40LiveCoreParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(CATALOG_PATH)
        cls.acquisition = StableAcquisitionPlan.load(
            ACQUISITION_PATH, catalog=cls.catalog
        )
        cls.bindings = {
            item.binding_id: item for item in cls.catalog.bindings
        }
        cls.acquisitions = {
            item.binding_id: item for item in cls.acquisition.bindings
        }

    def _sample(
        self,
        binding_id: str,
        fixture_name: str,
        *,
        test_provenance: bool = False,
    ):
        fixture = json.loads((FIXTURES / fixture_name).read_text())
        binding = self.bindings[binding_id]
        acquisition = self.acquisitions[binding_id]
        provider_payload = fixture["raw"]
        if acquisition.provider_kind.startswith("binance_") and (
            acquisition.provider_kind != "binance_usdm_rest_bar"
        ):
            provider_payload = {
                "stream": acquisition.native_channel,
                "data": provider_payload,
            }
        elif acquisition.provider_kind == "okx_trade":
            provider_payload = {
                "arg": {
                    "channel": "trades",
                    "instId": binding.instrument.native_symbol,
                },
                "data": [provider_payload],
            }
        raw_bytes = json.dumps(
            provider_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        received_at_ns = int(fixture["context"]["received_at_ns"])
        raw = build_raw_envelope(
            capture_id=hashlib.sha256(raw_bytes).digest()[:16],
            provider=binding.provider,
            venue=binding.instrument.identity.venue,
            market=binding.instrument.identity.market,
            product_type=binding.instrument.identity.product_type.value,
            native_symbol=binding.instrument.native_symbol,
            native_channel=acquisition.native_channel,
            subscription_id=binding.source_id,
            source_session_id=f"c40-{binding_id}-session",
            connection_generation=3,
            lease_epoch=1,
            authority_revision=self.catalog.authority_revision,
            partition_plan_epoch=1,
            received_at_ns=received_at_ns,
            transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_WEBSOCKET,
            transport_compression=raw_provider_pb2.TRANSPORT_COMPRESSION_NONE,
            capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_POST_DECOMPRESSION,
            raw_frame_bytes=raw_bytes,
            adapter_version=binding.adapter_version,
            config_revision=1,
            instrument_catalog_revision=self.catalog.catalog_revision,
            correlation_id=f"c40-{binding_id}",
            test_provenance=test_provenance,
        )
        seed = market_data_pb2.EventEnvelope(partition_sequence=7_000_001)
        context = _context(binding, raw, seed)
        expected = _canonicalize(
            acquisition,
            _provider_frames(acquisition, provider_payload)[0],
            context,
        )
        sample = LiveSample(
            payload=expected.SerializeToString(deterministic=True),
            raw_envelope=raw.SerializeToString(deterministic=True),
            event_id_header=bytes(expected.event_id),
            partition=2,
            offset=7,
        )
        return binding, acquisition, sample

    def test_all_six_crypto_provider_paths_match_exact_bytes(self) -> None:
        cases = (
            (
                "binance-usdm-btcusdt-trade",
                "binance_usdm_trade.json",
            ),
            (
                "binance-usdm-btcusdt-quote",
                "binance_usdm_bbo.json",
            ),
            (
                "binance-usdm-btcusdt-bar-1m",
                "binance_usdm_rest_bar.json",
            ),
            (
                "okx-swap-btcusdt-trade",
                "okx_trade.json",
            ),
            (
                "okx-swap-btcusdt-quote",
                "okx_bbo.json",
            ),
            (
                "okx-swap-btcusdt-bar-1m",
                "okx_bar.json",
            ),
        )
        for binding_id, fixture_name in cases:
            with self.subTest(binding_id=binding_id):
                binding, acquisition, sample = self._sample(
                    binding_id, fixture_name
                )
                result = verify_sample(
                    sample,
                    catalog=self.catalog,
                    binding=binding,
                    acquisition=acquisition,
                )
                self.assertEqual(result["offset"], 7)
                self.assertEqual(
                    result["payload_sha256"],
                    hashlib.sha256(sample.payload).hexdigest(),
                )

    def test_mutated_canonical_payload_is_rejected(self) -> None:
        binding, acquisition, sample = self._sample(
            "binance-usdm-btcusdt-trade",
            "binance_usdm_trade.json",
        )
        mutated = market_data_pb2.EventEnvelope.FromString(sample.payload)
        mutated.trade.price.source_text = "999999"
        broken = LiveSample(
            payload=mutated.SerializeToString(deterministic=True),
            raw_envelope=sample.raw_envelope,
            event_id_header=sample.event_id_header,
            partition=sample.partition,
            offset=sample.offset,
        )
        with self.assertRaisesRegex(ValueError, "canonical bytes differ"):
            verify_sample(
                broken,
                catalog=self.catalog,
                binding=binding,
                acquisition=acquisition,
            )

    def test_test_provenance_is_rejected(self) -> None:
        binding, acquisition, sample = self._sample(
            "okx-swap-btcusdt-trade",
            "okx_trade.json",
            test_provenance=True,
        )
        with self.assertRaisesRegex(ValueError, "test provenance"):
            verify_sample(
                sample,
                catalog=self.catalog,
                binding=binding,
                acquisition=acquisition,
            )

    def test_scan_freezes_highs_and_stops_at_complete_sample_floor(self) -> None:
        class Message:
            def __init__(self, partition: int, offset: int) -> None:
                self._partition = partition
                self._offset = offset

            def error(self):
                return None

            def partition(self) -> int:
                return self._partition

            def offset(self) -> int:
                return self._offset

        class Consumer:
            def __init__(self) -> None:
                self.messages = iter((Message(0, 10), Message(1, 10)))
                self.closed = False

            def list_topics(self, *, topic, timeout):
                del timeout
                return SimpleNamespace(
                    topics={
                        topic: SimpleNamespace(
                            error=None,
                            partitions={0: object(), 1: object()},
                        )
                    }
                )

            def offsets_for_times(self, values, *, timeout):
                del timeout
                return [
                    SimpleNamespace(partition=value.partition, offset=10)
                    for value in values
                ]

            def get_watermark_offsets(self, value, *, timeout):
                del value, timeout
                return 0, 100

            def assign(self, values):
                self.assignments = values

            def poll(self, timeout):
                del timeout
                return next(self.messages)

            def close(self):
                self.closed = True

        consumer = Consumer()
        accepted = []
        args = SimpleNamespace(
            canonical_topic="md.canonical.v2",
            candidate_deployed_at_ns=1_787_469_659_369_240_158,
            records_per_partition=250,
            timeout_seconds=2.0,
        )
        with patch(
            "scripts.phasec40_collect_live_core_parity._consumer",
            return_value=consumer,
        ):
            result = _scan_tail(
                args,
                accept=lambda message: accepted.append(
                    (message.partition(), message.offset())
                ),
                complete=lambda: len(accepted) == 2,
            )
        self.assertEqual(accepted, [(0, 10), (1, 10)])
        self.assertTrue(result["completed_when_sample_floor_met"])
        self.assertEqual(result["captured_high_offsets"], {"0": 100, "1": 100})
        self.assertEqual(result["records"], 2)
        self.assertTrue(consumer.closed)

    def test_scope_and_tail_windows_are_strictly_bounded(self) -> None:
        scope = AuthorityPromotionScope.load(
            SCOPE_PATH,
            catalog=self.catalog,
        )
        self.assertEqual(len(scope.binding_ids), 12)
        self.assertEqual(_bounded_window(100, 1000, 250), (750, 1000))
        self.assertEqual(_bounded_window(900, 1000, 250), (900, 1000))
        for values in ((-1, 1, 1), (2, 1, 1), (0, 1, 0)):
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    _bounded_window(*values)


if __name__ == "__main__":
    unittest.main()
