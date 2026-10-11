"""TEST_ONLY DR05 book fences through the actual Python read path."""
import base64
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import grpc
from google.protobuf.wrappers_pb2 import BytesValue
from qdl.common.v1 import common_pb2 as common
from qdl.marketdata.v2 import market_data_pb2 as md
from qdl.query import ConsumerGrade, DataRequirement
from qdl.query.contracts import CanonicalErrorCode
from qdl.query.results import QueryBackendError
from qdl.runtime.kn_bar_readback import binding_product_key
from qdl.runtime.kn_hot_view import CanonicalHotClient, HotViewGap, HotViewUnavailable, SCHEMA
from qdl.runtime.kn_market_cache import CacheRow, ProductView, SourceBoundary, KnCacheError, KnCacheNotReady
from qdl.runtime.kn_query_backend import KnMarketCacheQueryBackend
from qdl.runtime.stable_catalog import StableSourceCatalog


class Refused(grpc.RpcError):
    def __init__(self, reason, code=grpc.StatusCode.FAILED_PRECONDITION):
        self.reason, self.status = reason, code

    def code(self):
        return self.status

    def details(self):
        return self.reason


class BookInvalidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = StableSourceCatalog.load(Path(__file__).resolve().parents[1] /
                                               "config/v2/stable-source-bindings.yaml")
        cls.bindings = [b for b in cls.catalog.bindings if b.feed.value in
                       {"BOOK_SNAPSHOT", "BOOK_DELTA"} and
                       b.instrument.native_symbol in {"ETHUSDT", "ETH-USDT-SWAP"}]
        assert len(cls.bindings) == 4

    def setup_binding(self, binding):
        self.binding = binding
        self.lpk = binding_product_key(binding, "paper")
        self.now = 1800000000000000000
        self.requirement = DataRequirement(instrument_uid=binding.instrument.instrument_uid,
            feed=binding.feed, consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id=binding.source_policy_id, max_freshness_ms=1000)
        self.hot = Mock()
        self.reader = SimpleNamespace(environment="paper", latest=lambda key: self.view)
        self.backend = KnMarketCacheQueryBackend(self.reader, self.catalog, schema_digest="a"*64,
            topic_id="topic", clock_ns=lambda: self.now, hot_client=self.hot)
        self.set_view(self.event(), 10)

    def event(self, fence=False):
        b = self.binding
        e = md.EventEnvelope(schema_major=2, event_id=b"e"*16,
            instrument_uid=b.instrument.instrument_uid, instrument_id=b.instrument.instrument_id,
            instrument_revision=b.instrument.metadata_revision, venue=b.instrument.identity.venue,
            market=b.instrument.identity.market, product_type=b.instrument.identity.product_type.value,
            native_symbol=b.instrument.native_symbol, provider=b.provider, source_id=b.source_id,
            source_role=getattr(common, "SOURCE_ROLE_"+b.source_role),
            authority_revision=self.catalog.authority_revision, adapter_version=b.adapter_version,
            normalizer_version=b.normalizer_version, raw_capture_id=b"r"*16, raw_payload_hash=b"h"*32,
            source_session_id="TEST_ONLY.session", connection_generation=2, lease_epoch=1,
            partition_sequence=20, source_sequence="g2:invalid:20" if fence else "g2:42",
            received_at_ns=self.now, normalized_at_ns=self.now, published_at_ns=self.now,
            source_event_time_ns=self.now, correlation_id="TEST_ONLY.DR05")
        book = getattr(e, b.feed.value.lower())
        book.book_generation = 2
        book.sequence_verified = not fence
        if fence:
            e.quality_flags.extend([common.QUALITY_FLAG_SEQUENCE_GAP_BEFORE,
                                    common.QUALITY_FLAG_RESYNC_REQUIRED])
            if b.feed.value == "BOOK_DELTA":
                book.reset = True
        elif b.feed.value == "BOOK_SNAPSHOT":
            book.native_sequence = "42"
            book.depth = 1
            for side, price in ((common.BOOK_SIDE_BID, 100), (common.BOOK_SIDE_ASK, 101)):
                level = book.levels.add(side=side, quantity_unit=common.QUANTITY_UNIT_BASE_ASSET)
                level.price.CopyFrom(common.DecimalValue(mantissa=price, source_text=str(price)))
                level.quantity.CopyFrom(common.DecimalValue(mantissa=1, source_text="1"))
        else:
            book.native_sequence_start = book.native_sequence_end = "42"
            book.snapshot_sequence = "41"
        return e

    def set_view(self, event, offset):
        self.view = ProductView(self.lpk, 1, 1, (CacheRow(event.SerializeToString(), offset),),
                                SourceBoundary("topic", 0, offset))

    def assert_gap(self, reason):
        with self.assertRaises(QueryBackendError) as caught:
            self.backend.latest(self.requirement)
        self.assertEqual(caught.exception.problem.code, CanonicalErrorCode.OPEN_SEQUENCE_GAP)
        self.assertIn(reason, caught.exception.problem.detail)

    def test_primary_empty_fence_and_valid_anchor_recovery(self):
        for binding in self.bindings:
            with self.subTest(binding=binding.binding_id):
                self.setup_binding(binding)
                self.assertTrue(self.backend.latest(self.requirement).quality.execution_eligible)
                self.set_view(self.event(fence=True), 20)
                self.assert_gap("BOOK_INVALIDATED_RESYNC_REQUIRED")
                self.hot.latest.assert_not_called()
                self.set_view(self.event(), 30)
                self.assertTrue(self.backend.latest(self.requirement).quality.execution_eligible)

    def test_invalid_identity_and_unflagged_empty_book_are_not_typed_gap(self):
        for binding in self.bindings:
            for mutation in ("source_id", "raw_capture_id", "flags"):
                with self.subTest(binding=binding.binding_id, mutation=mutation):
                    self.setup_binding(binding)
                    event = self.event(fence=True)
                    if mutation == "flags":
                        del event.quality_flags[:]
                    else:
                        setattr(event, mutation, "wrong" if mutation == "source_id" else b"")
                    self.set_view(event, 20)
                    with self.assertRaises(ValueError):
                        self.backend.latest(self.requirement)
                    self.hot.latest.assert_not_called()

    def test_rpc_gap_never_retries_replica_or_masks_with_primary_error(self):
        for reason in ("HOT_RECORD_GAPPED", "HOT_RECORD_GENERATION_MISMATCH", "HOT_BOOK_UNVERIFIED"):
            for primary in ("stale", "unavailable", "missing"):
                with self.subTest(reason=reason, primary=primary):
                    self.setup_binding(self.bindings[0])
                    self.now += 10000000000
                    if primary == "missing":
                        self.reader.latest = Mock(side_effect=KnCacheNotReady(self.lpk, "MISSING"))
                    elif primary == "unavailable":
                        self.reader.latest = Mock(side_effect=KnCacheError("offline"))
                    second = Mock(side_effect=AssertionError("old healthy replica must not be read"))
                    client = CanonicalHotClient(calls=(Mock(side_effect=Refused(reason)), second),
                                               expectation={}, secret=b"x"*32)
                    self.backend.hot_client = client
                    self.assert_gap(reason)
                    second.assert_not_called()
                    self.assertTrue(client._slots.acquire(False))
                    client._slots.release()

    def test_hot_canonical_empty_fence_does_not_become_lineage_error(self):
        for binding in self.bindings:
            with self.subTest(binding=binding.binding_id):
                self.setup_binding(binding)
                self.now += 10000000000
                reply = dict(schema=SCHEMA, binding_id=binding.binding_id,
                    product_key=self.lpk.encode(), physical_key=binding.partition_key,
                    source_topic_id="topic", source_partition=0, source_offset=20, record_offset=20,
                    canonical=base64.b64encode(self.event(fence=True).SerializeToString()).decode())
                transport = Mock(return_value=BytesValue(value=json.dumps(reply).encode()))
                self.backend.hot_client = CanonicalHotClient(calls=(transport,),
                    expectation={"source_topic_id":"topic"}, secret=b"x"*32)
                self.assert_gap("BOOK_INVALIDATED_RESYNC_REQUIRED")

    def test_gap_text_under_auth_failure_is_not_trusted(self):
        client = CanonicalHotClient(calls=(Mock(side_effect=Refused("HOT_RECORD_GAPPED",
            grpc.StatusCode.PERMISSION_DENIED)),), expectation={}, secret=b"x"*32)
        self.setup_binding(self.bindings[0])
        with self.assertRaises(HotViewUnavailable) as caught:
            client.latest(self.binding, self.lpk)
        self.assertNotIsInstance(caught.exception, HotViewGap)
        self.assertEqual(str(caught.exception), "HOT_AUTHORITY_OR_PROTOCOL_REFUSED")
