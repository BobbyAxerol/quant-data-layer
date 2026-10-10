"""IDR-082 (v2.2.3, additive): the public market-data view carries the event identity.

A Query snapshot of a canonical event and the Stream delivery of the same event
must expose the same ``event_id`` (lowercase hex of ``EventEnvelope.event_id``),
so a consumer can tell a re-read of the last trade from a new trade. These
cases use the real canonical TRADE records of the KN codec golden, the stable
spool Query backend (whose ``_item`` the KN market-cache backend inherits),
the public HTTP renderer and the SDK stream projection. The KN Redis path is
covered by ``tests.test_kn_query_backend`` (needs ``QDL_KN_TEST_REDIS``).
"""
from __future__ import annotations

import importlib
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from pydantic import ValidationError

from qdl.marketdata.v2 import market_data_pb2
from qdl.query import ConsumerGrade, DataRequirement, FeedType
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_source import StableSpoolQueryBackend
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig
from qdl_sdk import DataRequirement as SdkRequirement, Feed, Grade as SdkGrade, StreamEvent, market_data_view_from_stream
from qdl_sdk.models import MarketDataView, ResponseModel, SnapshotResponse
from tests.test_kn_bar_legacy_import import golden_records
from tests.test_qdl_sdk_stream_projection import template

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
router = importlib.import_module("qdl.api_v2.router")


def _golden_trades():
    return [
        (pk, payload) for pk, payload in golden_records(bar=False)
        if market_data_pb2.EventEnvelope.FromString(payload).WhichOneof("payload") == "trade"
    ]


class ViewEventIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(CATALOG_PATH)
        cls.trades = _golden_trades()
        if not cls.trades:
            raise AssertionError("the KN codec golden has no real TRADE record")

    def binding(self, partition_key: str):
        return next(
            item for item in self.catalog.bindings
            if item.partition_key == partition_key and item.feed is FeedType.TRADE
        )

    def query_backend(self, binding, payloads, now_ns: int) -> StableSpoolQueryBackend:
        directory = tempfile.TemporaryDirectory(prefix="idr082-spool-")
        self.addCleanup(directory.cleanup)
        spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(directory.name) / "spool.sqlite3", max_records=1_000, max_payload_bytes=10_000_000,
            max_event_bytes=64_000, max_storage_bytes=40_000_000, min_free_disk_bytes=0,
            max_partition_records=1_000))
        self.addCleanup(spool.close)
        spool.append_many([
            DurableEvent(stream=binding.canonical_stream, partition_key=binding.partition_key,
                         event_id=bytes(market_data_pb2.EventEnvelope.FromString(payload).event_id),
                         payload=payload, accepted_at_ns=1_000 + index)
            for index, payload in enumerate(payloads)
        ])
        return StableSpoolQueryBackend(spool, self.catalog, schema_digest="a" * 64, clock_ns=lambda: now_ns)

    def snapshot_and_stream(self, partition_key: str, payload: bytes, *, offset: int = 41):
        envelope = market_data_pb2.EventEnvelope.FromString(payload)
        binding = self.binding(partition_key)
        now_ns = int(envelope.received_at_ns) + 1_000_000
        item = self.query_backend(binding, [payload], now_ns).latest(DataRequirement(
            instrument_uid=binding.instrument.instrument_uid, feed=FeedType.TRADE,
            consumer_grade=ConsumerGrade.ALPHA, source_policy_id=binding.source_policy_id,
        ))
        self.assertIsNotNone(item)
        # The public HTTP bytes, read back by the SDK as a consumer does.
        body = b'{"request_id":"idr082","data":' + router._market_item_json(item) + b"}"
        snapshot = SnapshotResponse.model_validate_json(body).data
        stream = market_data_view_from_stream(
            StreamEvent(offset, "signed", envelope),
            template=snapshot,
            requirement=SdkRequirement(
                instrument_uid=binding.instrument.instrument_uid, feed=Feed.TRADE,
                consumer_grade=SdkGrade.ALPHA, source_policy_id=binding.source_policy_id,
            ),
            now_ns=now_ns,
        )
        return envelope, item, snapshot, stream

    def test_query_snapshot_and_stream_delivery_of_one_trade_share_its_event_id(self):
        seen = set()
        for partition_key, payload in self.trades:
            with self.subTest(partition_key=partition_key):
                envelope, item, snapshot, stream = self.snapshot_and_stream(partition_key, payload)
                expected = bytes(envelope.event_id).hex()
                self.assertTrue(expected)
                self.assertEqual(item.event_id, expected)
                self.assertEqual(snapshot.event_id, expected)
                self.assertEqual(stream.event_id, snapshot.event_id)
                # Same trade, same identity; the transport fields are what differ.
                self.assertEqual(stream.payload.native_trade_id, snapshot.payload.native_trade_id)
                seen.add(expected)
        self.assertEqual(len(seen), len(self.trades), "distinct trades have distinct identities")

    def test_a_later_snapshot_of_the_same_event_keeps_its_identity(self):
        partition_key, payload = self.trades[0]
        _, _, first, _ = self.snapshot_and_stream(partition_key, payload, offset=41)
        _, _, again, stream = self.snapshot_and_stream(partition_key, payload, offset=97)
        self.assertEqual(again.event_id, first.event_id)
        self.assertEqual(stream.watermark_offset, 97)
        self.assertEqual(stream.event_id, first.event_id)

    def test_render_slot_paths_emit_identical_bytes_with_the_identity(self):
        partition_key, payload = self.trades[0]
        _, item, _, _ = self.snapshot_and_stream(partition_key, payload)

        class Slot:
            value = None

        slotted = replace(item, render_slot=Slot())
        full = router._market_item(item).model_dump_json(by_alias=True).encode("utf-8")
        # First render (full model), second (layout built), third (static splice).
        renders = [router._market_item_json(slotted) for _ in range(3)]
        self.assertEqual(renders, [full] * 3)
        self.assertIn(b'"event_id":"' + item.event_id.encode() + b'"', full)
        # Additive: the new member is appended; every existing member is unchanged.
        without = router._market_item(replace(item, event_id=None)).model_dump_json(by_alias=True)
        self.assertEqual(full.decode(), without[:-len(',"event_id":null}')] + f',"event_id":"{item.event_id}"}}')

    def test_sdk_view_tolerates_absence_and_validates_the_identity(self):
        view = template(Feed.TRADE).model_dump(mode="json")
        self.assertNotIn("event_id", {k for k, v in view.items() if v is not None})
        self.assertIsNone(MarketDataView.model_validate(view).event_id, "older server: absent")
        self.assertEqual(MarketDataView.model_validate({**view, "event_id": "0a" * 16}).event_id, "0a" * 16)
        for bad in ("", "ABCD", "abc", "zz" * 16, "0a" * 65):
            with self.subTest(bad=bad), self.assertRaises(ValidationError):
                MarketDataView.model_validate({**view, "event_id": bad})

    def test_a_reader_without_the_field_ignores_it(self):
        # SDK 2.1.0 response models are ``extra="ignore"``: the same view model
        # without ``event_id`` reads a v2.2.3 body to the same known fields.
        fields = {name: (info.annotation, info) for name, info in MarketDataView.model_fields.items()
                  if name != "event_id"}
        from pydantic import create_model
        old_view = create_model("OldMarketDataView", __base__=ResponseModel, **fields)
        body = {**template(Feed.TRADE).model_dump(mode="json"), "event_id": "0a" * 16}
        old = old_view.model_validate(body)
        self.assertFalse(hasattr(old, "event_id"))
        self.assertEqual(old.model_dump(), {k: v for k, v in MarketDataView.model_validate(body).model_dump().items()
                                            if k != "event_id"})

    def test_stable_query_always_has_an_identity_and_stream_tolerates_none(self):
        # The stable catalog admits only a 16-byte canonical event id
        # (``binding_for_envelope``), so every stable Query row has one.
        partition_key, trade = self.trades[0]
        _, item, snapshot, _ = self.snapshot_and_stream(partition_key, trade)
        self.assertRegex(item.event_id, r"\A[0-9a-f]{32}\Z")
        # Stream projection adds no new refusal: an id-less frame projects with none.
        stripped = market_data_pb2.EventEnvelope.FromString(trade)
        stripped.ClearField("event_id")
        stream = market_data_view_from_stream(
            StreamEvent(42, "signed", stripped), template=snapshot,
            requirement=SdkRequirement(
                instrument_uid=snapshot.instrument_uid, feed=Feed.TRADE,
                consumer_grade=SdkGrade.ALPHA, source_policy_id=snapshot.quality.policy_id,
            ),
            now_ns=int(stripped.received_at_ns) + 1_000_000,
        )
        self.assertIsNone(stream.event_id)

    def test_resample_clears_the_constituent_identity(self):
        from qdl.query.lifecycle import BarLifecycle
        from qdl.query.results import ContractMetadata, MarketDataItem, QualityMetadata, SourceMetadata
        from qdl.warmup.handoff import resample_final_bars

        def bar(open_ns: int, event_id: str) -> MarketDataItem:
            return MarketDataItem(
                instrument_uid="uid-1", instrument_id="BINANCE.USDM.PERPETUAL.BTC-USDT", instrument_revision=1,
                feed=FeedType.BAR, interval="1m", observed_at_ns=open_ns + 59_999_999_999,
                payload={"open_time_ns": open_ns, "close_time_ns": open_ns + 59_999_999_999, "open": "1",
                         "high": "2", "low": "1", "close": "2", "volume": "3", "volume_unit": "BASE_ASSET",
                         "trade_count": 1, "origin": "PROVIDER", "is_final": True},
                source=SourceMetadata(venue="BINANCE", provider="BINANCE_DIRECT", source_id="s",
                                      source_role="PRIMARY", authoritative=True),
                quality=QualityMetadata(state="LIVE", freshness_ms=0, gap_open=False, complete=True,
                                        execution_eligible=False, policy_id="p", flags=()),
                contract=ContractMetadata(schema_digest="a" * 64, contract_version="2.0.0",
                                          normalizer_version="n", adapter_version="a",
                                          instrument_catalog_revision=1, source_policy_revision=1,
                                          authority_revision=1, config_revision=1, correlation_id=event_id),
                bar_lifecycle=BarLifecycle.FINAL, event_id=event_id,
            )

        base = 1_700_000_100 // 300 * 300 * 1_000_000_000
        items = [bar(base + index * 60_000_000_000, f"{index:02x}" * 16) for index in range(5)]
        (aggregate,) = resample_final_bars(items, target_interval="5m")
        self.assertIsNone(aggregate.event_id)
        self.assertEqual(items[-1].event_id, "04" * 16)
        with self.assertRaisesRegex(ValueError, "event_id"):
            replace(items[0], event_id="NOT-HEX")


if __name__ == "__main__":
    unittest.main()
