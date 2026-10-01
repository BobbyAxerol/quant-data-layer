"""Private hot-view protocol: exact identity and bounded failures, no runtime I/O."""
import base64
import hmac
import json
from types import SimpleNamespace
import unittest

import grpc
from google.protobuf.wrappers_pb2 import BytesValue

from qdl.runtime.kn_hot_view import CanonicalHotClient, DOMAIN, SCHEMA, HotViewUnavailable


class HotClientTests(unittest.TestCase):
    def setUp(self):
        self.binding = SimpleNamespace(binding_id="quote-okx", feed=SimpleNamespace(value="QUOTE"),
            interval=None, partition_key="physical")
        self.lpk = SimpleNamespace(encode=lambda: "logical")
        self.expectation = {"environment":"paper", "stream":"canonical", "source_topic_id":"topic",
            "partition_plan_epoch":1,"source_policy_revision":1,"catalog_revision":10,
            "route_generation":"r1","schema_major":2}
        self.reply = {**self.expectation,"schema":SCHEMA,"binding_id":"quote-okx", "product_key":"logical",
            "physical_key":"physical","source_partition":0,"source_offset":0,"record_offset":0,
            "canonical":base64.b64encode(b"test-only").decode()}

    def client(self, call=None):
        def valid(request, *, metadata, timeout, wait_for_ready):
            self.assertEqual(metadata[0][1], hmac.digest(b"x"*32, DOMAIN+request.value,"sha256"))
            self.assertFalse(wait_for_ready)
            self.assertLessEqual(timeout, .25)
            self.assertEqual(json.loads(request.value)["issued_at_ns"],123)
            return BytesValue(value=json.dumps(self.reply).encode())
        return CanonicalHotClient(calls=(call or valid,), expectation=self.expectation,
            secret=b"x"*32, clock_ns=lambda:123)

    def test_preserves_bytes_zero_offset_and_no_cache_generation(self):
        view=self.client().latest(self.binding,self.lpk)
        self.assertEqual(view.rows[0].canonical,b"test-only")
        self.assertEqual(view.boundary.offset,0)
        self.assertFalse(hasattr(view,"generation"))
        self.assertFalse(hasattr(view,"fence"))

    def test_mismatched_identity_authority_and_offsets_are_rejected(self):
        for key in (*self.expectation,"binding_id","product_key","physical_key","schema"):
            with self.subTest(key=key):
                saved=self.reply[key];self.reply[key]="different"
                with self.assertRaisesRegex(HotViewUnavailable,"HOT_REPLY_INVALID"):
                    self.client().latest(self.binding,self.lpk)
                self.reply[key]=saved
        for bad in (-1,True,"0",2**63):
            self.reply["source_offset"]=bad
            with self.assertRaises(HotViewUnavailable):self.client().latest(self.binding,self.lpk)

    def test_payload_and_reply_shape_are_bounded(self):
        for raw in (b"[]",b"null",b"{",b"x"*(512*1024+1)):
            with self.assertRaises(HotViewUnavailable):
                self.client(lambda *a,**k:BytesValue(value=raw)).latest(self.binding,self.lpk)
        self.reply["canonical"]="!bad!"
        with self.assertRaises(HotViewUnavailable):self.client().latest(self.binding,self.lpk)

    def test_unsupported_feed_and_saturation_do_not_call_transport(self):
        def forbidden(*args,**kwargs):self.fail("must not call transport")
        client=self.client(forbidden)
        self.binding.feed.value="BAR"
        with self.assertRaisesRegex(HotViewUnavailable,"HOT_FEED_UNSUPPORTED"):client.latest(self.binding,self.lpk)
        self.binding.feed.value="QUOTE"
        for _ in range(8):self.assertTrue(client._slots.acquire(False))
        with self.assertRaisesRegex(HotViewUnavailable,"HOT_READ_CAPACITY"):client.latest(self.binding,self.lpk)
        for _ in range(8):client._slots.release()

    def test_hard_refusal_does_not_fall_back_to_older_replica(self):
        class Refused(grpc.RpcError):
            def code(self):return grpc.StatusCode.FAILED_PRECONDITION
        def failed(*args,**kwargs):raise Refused()
        def forbidden(*args,**kwargs):self.fail("must not retry integrity/authority")
        client=CanonicalHotClient(calls=(failed,forbidden),expectation=self.expectation,secret=b"x"*32)
        with self.assertRaisesRegex(HotViewUnavailable,"HOT_AUTHORITY_OR_PROTOCOL_REFUSED"):
            client.latest(self.binding,self.lpk)
        self.assertTrue(client._slots.acquire(False));client._slots.release()

    def test_transient_failure_can_try_second_replica_with_same_total_deadline(self):
        class Unavailable(grpc.RpcError):
            def code(self):return grpc.StatusCode.UNAVAILABLE
        def failed(*args,**kwargs):raise Unavailable()
        client=self.client();client._calls=(failed,client._calls[0])
        self.assertEqual(client.latest(self.binding,self.lpk).boundary.offset,0)

    def test_cursor_boundary_is_not_the_record_offset(self):
        self.reply["source_offset"]=100
        view=self.client().latest(self.binding,self.lpk)
        self.assertEqual(view.boundary.offset,100)
        self.assertEqual(view.rows[0].source_offset,0)
        self.reply["record_offset"]=101
        with self.assertRaises(HotViewUnavailable):self.client().latest(self.binding,self.lpk)


class HotQuerySelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pathlib import Path
        from qdl.runtime.stable_catalog import StableSourceCatalog
        from tests.test_kn_bar_legacy_import import golden_records
        from tests.test_kn_query_backend import binding_of
        from qdl.marketdata.v2 import market_data_pb2
        cls.catalog = StableSourceCatalog.load(Path(__file__).resolve().parents[1]/"config/v2/stable-source-bindings.yaml")
        pk, payload = next((pk,payload) for pk,payload in golden_records(bar=False)
            if market_data_pb2.EventEnvelope.FromString(payload).WhichOneof("payload")=="quote")
        cls.binding=binding_of(cls.catalog,pk,payload)
        cls.original=payload

    def setUp(self):
        from qdl.runtime.kn_query_backend import KnMarketCacheQueryBackend
        from qdl.runtime.kn_bar_readback import binding_product_key
        from qdl.runtime.kn_hot_view import CanonicalHotView
        from qdl.runtime.kn_market_cache import CacheRow,SourceBoundary,ProductView
        from qdl.marketdata.v2 import market_data_pb2
        from qdl.query import DataRequirement,ConsumerGrade
        envelope=market_data_pb2.EventEnvelope.FromString(self.original)
        self.now=envelope.source_event_time_ns+10_000_000_000
        self.lpk=binding_product_key(self.binding,"paper")
        self.primary=ProductView(self.lpk,1,1,(CacheRow(self.original,10),),SourceBoundary("topic",0,20))
        envelope.source_event_time_ns=self.now-10_000_000
        envelope.received_at_ns=self.now-9_000_000
        envelope.normalized_at_ns=self.now-8_000_000
        envelope.published_at_ns=self.now-7_000_000
        self.backup=CanonicalHotView(self.lpk,(CacheRow(envelope.SerializeToString(),30),),SourceBoundary("topic",0,40))
        self.calls=0
        def backup(*args):
            self.calls+=1
            if isinstance(self.backup,Exception):raise self.backup
            return self.backup
        self.reader=SimpleNamespace(environment="paper",latest=lambda key:self.primary)
        self.backend=KnMarketCacheQueryBackend(self.reader,self.catalog,schema_digest="a"*64,
            topic_id="topic",clock_ns=lambda:self.now,hot_client=SimpleNamespace(latest=backup))
        self.requirement=DataRequirement(instrument_uid=self.binding.instrument.instrument_uid,
            feed=self.binding.feed,consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id=self.binding.source_policy_id,max_freshness_ms=1000)

    def test_stale_primary_uses_newer_verified_record_without_timestamp_changes(self):
        from qdl.runtime.kn_query_backend import parse_placeholder
        result=self.backend.latest(self.requirement)
        self.assertEqual(self.calls,1)
        self.assertTrue(result.quality.execution_eligible)
        self.assertEqual(parse_placeholder(result.cursor),("topic",0,40))
        self.assertEqual(result.quality.freshness_ms,10)

    def test_recovery_never_returns_older_primary_when_backup_disappears(self):
        from qdl.query.results import QueryBackendError
        self.assertTrue(self.backend.latest(self.requirement).quality.execution_eligible)
        self.backup=HotViewUnavailable("offline")
        with self.assertRaisesRegex(QueryBackendError,"HOT_BACKUP_UNAVAILABLE_PRIMARY_BEHIND"):
            self.backend.latest(self.requirement)

    def test_switches_back_only_when_primary_catches_up_and_stays_valid(self):
        from qdl.runtime.kn_market_cache import ProductView
        self.backend.latest(self.requirement)
        self.primary=ProductView(self.lpk,2,3,self.backup.rows,self.backup.boundary)
        self.backup=HotViewUnavailable("offline")
        self.assertTrue(self.backend.latest(self.requirement).quality.execution_eligible)
        self.assertEqual(self.calls,1)

    def test_stale_backup_does_not_turn_old_price_usable(self):
        self.now+=10_000_000_000
        result=self.backend.latest(self.requirement)
        self.assertFalse(result.quality.execution_eligible)

    def test_equal_record_offset_cannot_change_payload_even_with_newer_boundary(self):
        from dataclasses import replace
        from qdl.runtime.kn_market_cache import CacheRow
        from qdl.query.results import QueryBackendError
        self.backup=replace(self.backup,rows=(CacheRow(self.backup.rows[0].canonical,10),))
        with self.assertRaisesRegex(QueryBackendError,"HOT_SOURCE_PAYLOAD_MISMATCH"):
            self.backend.latest(self.requirement)

    def test_corrupt_backup_lineage_is_a_typed_internal_refusal(self):
        from dataclasses import replace
        from qdl.marketdata.v2 import market_data_pb2
        from qdl.runtime.kn_market_cache import CacheRow
        from qdl.query.results import QueryBackendError
        from qdl.query.contracts import CanonicalErrorCode
        envelope=market_data_pb2.EventEnvelope.FromString(self.backup.rows[0].canonical)
        envelope.source_id="wrong-provider-lineage"
        self.backup=replace(self.backup,rows=(CacheRow(envelope.SerializeToString(),30),))
        with self.assertRaises(QueryBackendError) as rejected:self.backend.latest(self.requirement)
        self.assertEqual(rejected.exception.problem.code,CanonicalErrorCode.INTERNAL_ERROR)
        self.assertIn("HOT_BACKUP_LINEAGE_INVALID",str(rejected.exception))


    def _quiet_primary(self):
        # Selection-layer fixture: the separate quality oracle has admitted an
        # ON_CHANGE quiet session. It cannot prove projector progress.
        from dataclasses import replace
        original = self.backend._items
        def items(requirement, records):
            values = original(requirement, records)
            return tuple(replace(item, quality=replace(item.quality,
                state="LIVE", execution_eligible=True, event_recency_state="STALE",
                provider_session_state="LIVE", provider_session_liveness_ms=10))
                if item.quality.freshness_ms > 1000 else item for item in values)
        self.backend._items = items

    def test_quiet_primary_checks_independent_latest_before_admission(self):
        self._quiet_primary()
        item = self.backend.latest(self.requirement)
        self.assertEqual(self.calls, 1)
        self.assertEqual(item.quality.freshness_ms, 10)

    def test_quiet_primary_cannot_mask_backup_outage(self):
        from qdl.query.results import QueryBackendError
        self._quiet_primary()
        self.backup = HotViewUnavailable("stopped reader")
        with self.assertRaisesRegex(QueryBackendError, "HOT_QUIET_PRIMARY_UNVERIFIED"):
            self.backend.latest(self.requirement)

    def test_verified_quiet_canonical_event_keeps_original_timestamp(self):
        from qdl.runtime.kn_hot_view import CanonicalHotView
        from qdl.runtime.kn_market_cache import SourceBoundary
        self._quiet_primary()
        self.backup = CanonicalHotView(self.lpk, self.primary.rows, SourceBoundary("topic",0,40))
        item = self.backend.latest(self.requirement)
        self.assertEqual(self.calls, 1)
        self.assertEqual(item.quality.freshness_ms, 10000)
        self.assertTrue(item.quality.execution_eligible)
