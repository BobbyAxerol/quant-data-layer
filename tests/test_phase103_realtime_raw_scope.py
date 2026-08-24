from __future__ import annotations

import hashlib
from pathlib import Path
import unittest

from qdl.raw.envelope import build_raw_envelope
from qdl.provider.v1 import raw_provider_pb2
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from scripts.phase103_inspect_realtime_raw_scope import ScopeTally


ROOT = Path(__file__).resolve().parents[1]
CATALOG = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
ACQUISITION = StableAcquisitionPlan.load(
    ROOT / "config/v2/stable-acquisition-bindings.yaml", catalog=CATALOG
)
ACQUISITION_BY_ID = {item.binding_id: item for item in ACQUISITION.bindings}


def raw_for(
    binding_id: str,
    *,
    subscription_id: str | None = None,
    test_provenance: bool = False,
    **updates,
):
    binding = next(item for item in CATALOG.bindings if item.binding_id == binding_id)
    acquisition = ACQUISITION_BY_ID[binding_id]
    frame = b'{"e":"trade","s":"BTCUSDT","t":1,"p":"1","q":"1","T":2,"m":false}'
    value = build_raw_envelope(
        capture_id=hashlib.sha256(binding_id.encode()).digest()[:16],
        provider=binding.provider,
        venue=binding.instrument.identity.venue,
        market=binding.instrument.identity.market,
        product_type=binding.instrument.identity.product_type.value,
        native_symbol=binding.instrument.native_symbol,
        native_channel=acquisition.native_channel,
        subscription_id=subscription_id or binding.source_id,
        source_session_id="phase103-test-session",
        connection_generation=1,
        lease_epoch=1,
        authority_revision=CATALOG.authority_revision,
        partition_plan_epoch=1,
        received_at_ns=1_786_352_400_123_456_000,
        transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_WEBSOCKET,
        transport_compression=raw_provider_pb2.TRANSPORT_COMPRESSION_NONE,
        capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_POST_DECOMPRESSION,
        raw_frame_bytes=frame,
        adapter_version=binding.adapter_version,
        config_revision=1,
        instrument_catalog_revision=CATALOG.catalog_revision,
        correlation_id="phase103-test",
        test_provenance=test_provenance,
    )
    for field, value_update in updates.items():
        setattr(value, field, value_update)
    return value.SerializeToString(deterministic=True)


class RealtimeRawScopeTests(unittest.TestCase):
    def test_declared_raw_is_accepted_with_no_raw_frame_disclosure(self):
        tally = ScopeTally.from_catalog(catalog=CATALOG, acquisition=ACQUISITION)
        tally.observe(raw_for("binance-usdm-btcusdt-trade"))
        evidence = tally.evidence(required_bindings=["binance-usdm-btcusdt-trade"])
        self.assertEqual(evidence["accepted_by_binding"]["binance-usdm-btcusdt-trade"], 1)
        self.assertEqual(evidence["missing_required_bindings"], [])
        self.assertEqual(evidence["out_of_scope_count"], 0)
        self.assertNotIn("raw_frame", str(evidence))

    def test_test_provenance_is_never_accepted_as_live_scope_evidence(self):
        tally = ScopeTally.from_catalog(catalog=CATALOG, acquisition=ACQUISITION)
        tally.observe(raw_for("binance-usdm-btcusdt-trade", test_provenance=True))
        evidence = tally.evidence(required_bindings=["binance-usdm-btcusdt-trade"])
        self.assertEqual(evidence["test_provenance_count"], 1)
        self.assertEqual(
            evidence["missing_required_bindings"], ["binance-usdm-btcusdt-trade"]
        )

    def test_unknown_subscription_is_counted_by_hash_only(self):
        tally = ScopeTally.from_catalog(catalog=CATALOG, acquisition=ACQUISITION)
        tally.observe(raw_for("binance-usdm-btcusdt-trade", subscription_id="legacy-source"))
        evidence = tally.evidence(required_bindings=[])
        self.assertEqual(evidence["out_of_scope_count"], 1)
        self.assertEqual(
            evidence["unknown_subscription_hashes"],
            {hashlib.sha256(b"legacy-source").hexdigest(): 1},
        )
        self.assertNotIn("legacy-source", str(evidence))

    def test_identity_and_revision_mismatches_fail_the_declared_scope(self):
        tally = ScopeTally.from_catalog(catalog=CATALOG, acquisition=ACQUISITION)
        tally.observe(raw_for("binance-usdm-btcusdt-trade", native_symbol="ETHUSDT"))
        tally.observe(raw_for("binance-usdm-btcusdt-trade", authority_revision=99))
        evidence = tally.evidence(required_bindings=[])
        self.assertEqual(evidence["identity_mismatch_count"], 1)
        self.assertEqual(evidence["revision_mismatch_count"], 1)

    def test_malformed_payload_is_aggregated_not_emitted(self):
        tally = ScopeTally.from_catalog(catalog=CATALOG, acquisition=ACQUISITION)
        tally.observe(b"not-a-protobuf")
        evidence = tally.evidence(required_bindings=[])
        self.assertEqual(evidence["malformed_count"], 1)

    def test_required_binding_reports_absence_without_any_frame_disclosure(self):
        tally = ScopeTally.from_catalog(catalog=CATALOG, acquisition=ACQUISITION)
        evidence = tally.evidence(required_bindings=["okx-swap-eth-usdt-swap-bar-1m"])
        self.assertEqual(
            evidence["missing_required_bindings"], ["okx-swap-eth-usdt-swap-bar-1m"]
        )
        self.assertNotIn("raw_frame_bytes", str(evidence))


if __name__ == "__main__":
    unittest.main()
