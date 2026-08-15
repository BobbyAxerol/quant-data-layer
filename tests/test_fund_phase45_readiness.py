from __future__ import annotations

import unittest
from dataclasses import dataclass

from qdl.query import (
    AccessPurpose,
    BarRevisionPolicy,
    BatchRequirement,
    CanonicalErrorCode,
    ConsumerGrade,
    CoverageStatus,
    DataProduct,
    DataRequirement,
    EntitlementGrant,
    EntitlementPolicy,
    FeedType,
    GapPolicy,
    RecoveryPolicy,
    StalePolicy,
    evaluate_requirement,
)
from qdl.query.contracts import LEGACY_ERROR_ALIASES
from qdl.replay import (
    GapFreeHandoff,
    HandoffStore,
    SignedHandoffCursorCodec,
    SigningKeySet,
    SnapshotHandoffCoordinator,
    SnapshotWatermarkMismatch,
)
from qdl.transport import Cursor


STREAM = "md.canonical.v2.bar"
PARTITION = "uid/bar/binance"


class RequirementContractTests(unittest.TestCase):
    def requirement(self, **overrides) -> DataRequirement:
        values = {
            "instrument_uid": "uid",
            "feed": FeedType.BAR,
            "consumer_grade": ConsumerGrade.EXECUTION,
            "source_policy_id": "execution_binance_usdm_v1",
            "interval": "1m",
            "warmup_limit": 1000,
            "max_freshness_ms": 1000,
        }
        values.update(overrides)
        return DataRequirement(**values)

    def test_execution_requirement_is_strict_and_bounded(self):
        self.assertEqual(self.requirement().recovery, RecoveryPolicy.SNAPSHOT_AND_REPLAY)
        self.assertEqual(self.requirement().bar_revision_policy, BarRevisionPolicy.LATEST)
        for field, value in (
            ("stale_policy", StalePolicy.OBSERVE),
            ("gap_policy", GapPolicy.OBSERVE),
            ("require_full_coverage", False),
        ):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.requirement(**{field: value})
        with self.assertRaisesRegex(ValueError, "between 0 and 10000"):
            self.requirement(warmup_limit=10_001)
        with self.assertRaisesRegex(ValueError, "need an interval"):
            self.requirement(interval=None)
        with self.assertRaisesRegex(ValueError, "only for bar"):
            self.requirement(feed=FeedType.TRADE, interval="1m")

    def test_batch_partial_policy_cannot_weaken_execution_grade(self):
        first = self.requirement()
        second = self.requirement(instrument_uid="uid-2")
        batch = BatchRequirement("alpha-a", (first, second))
        self.assertTrue(batch.require_all)
        with self.assertRaisesRegex(ValueError, "must require all"):
            BatchRequirement("alpha-a", (first,), require_all=False)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            BatchRequirement("alpha-a", (first, first))

    def test_evaluator_has_one_fail_closed_precedence(self):
        requirement = self.requirement()
        cases = (
            ({"entitled": False}, CanonicalErrorCode.SOURCE_NOT_ALLOWED),
            ({"available": False}, CanonicalErrorCode.DATA_NOT_READY),
            ({"authoritative": False}, CanonicalErrorCode.SOURCE_NON_AUTHORITATIVE),
            ({"gap_open": True}, CanonicalErrorCode.OPEN_SEQUENCE_GAP),
            ({"fresh": False}, CanonicalErrorCode.DATA_STALE),
            ({"coverage": CoverageStatus.PARTIAL}, CanonicalErrorCode.PARTIAL_RESULT),
        )
        defaults = {
            "coverage": CoverageStatus.FULL,
            "entitled": True,
            "available": True,
            "fresh": True,
            "authoritative": True,
            "gap_open": False,
        }
        for changed, expected in cases:
            with self.subTest(expected=expected):
                problem = evaluate_requirement(requirement, **{**defaults, **changed})
                self.assertEqual(problem.code, expected)
        self.assertIsNone(evaluate_requirement(requirement, **defaults))

    def test_alpha_and_research_can_only_relax_explicit_policies(self):
        defaults = {
            "coverage": CoverageStatus.PARTIAL,
            "entitled": True,
            "available": True,
            "fresh": False,
            "authoritative": False,
            "gap_open": True,
        }
        for grade in (ConsumerGrade.ALPHA, ConsumerGrade.RESEARCH):
            with self.subTest(grade=grade):
                requirement = self.requirement(
                    consumer_grade=grade,
                    require_full_coverage=False,
                    stale_policy=StalePolicy.OBSERVE,
                    gap_policy=GapPolicy.OBSERVE,
                )
                self.assertIsNone(evaluate_requirement(requirement, **defaults))
        blocked = self.requirement(
            consumer_grade=ConsumerGrade.ALPHA,
            require_full_coverage=False,
            stale_policy=StalePolicy.BLOCK,
            gap_policy=GapPolicy.OBSERVE,
        )
        self.assertEqual(
            evaluate_requirement(blocked, **defaults).code,
            CanonicalErrorCode.DATA_STALE,
        )

    def test_error_aliases_collapse_to_one_canonical_vocabulary(self):
        self.assertEqual(
            LEGACY_ERROR_ALIASES,
            {
                "INVALID_REQUEST": CanonicalErrorCode.INVALID_ARGUMENT,
                "UNKNOWN_INSTRUMENT": CanonicalErrorCode.INSTRUMENT_NOT_FOUND,
                "UPSTREAM_UNAVAILABLE": CanonicalErrorCode.SOURCE_UNAVAILABLE,
                "SCHEMA_UNSUPPORTED": CanonicalErrorCode.SCHEMA_NOT_SUPPORTED,
            },
        )


class EntitlementTests(unittest.TestCase):
    def setUp(self):
        self.policy = EntitlementPolicy((EntitlementGrant(
            source_id="BINANCE_DIRECT",
            license_revision="binance-public-2026-08",
            purposes=frozenset({AccessPurpose.INTERNAL_ALPHA}),
            products=frozenset({DataProduct.CANONICAL_HISTORY}),
            valid_from_ns=100,
            valid_to_ns=200,
        ),))

    def test_absent_expired_raw_and_redistribution_access_fail_closed(self):
        cases = (
            ({"source_id": "UNKNOWN"}, "NO_ACTIVE_ENTITLEMENT"),
            ({"at_ns": 200}, "NO_ACTIVE_ENTITLEMENT"),
            ({"product": DataProduct.RAW_EVENTS}, "PRODUCT_NOT_ALLOWED"),
            ({"purpose": AccessPurpose.EXTERNAL_REDISTRIBUTION}, "PURPOSE_NOT_ALLOWED"),
        )
        defaults = {
            "source_id": "BINANCE_DIRECT",
            "purpose": AccessPurpose.INTERNAL_ALPHA,
            "product": DataProduct.CANONICAL_HISTORY,
            "at_ns": 150,
        }
        for changed, reason in cases:
            with self.subTest(reason=reason):
                decision = self.policy.authorize(**{**defaults, **changed})
                self.assertFalse(decision.allowed)
                self.assertEqual(decision.reason_code, reason)
        allowed = self.policy.authorize(**defaults)
        self.assertTrue(allowed.allowed)
        self.assertEqual(allowed.license_revision, "binance-public-2026-08")


class MemoryHandoffStore:
    def __init__(self, high: int):
        self.high = high
        self.checkpoints = {}

    def high_watermark(self, stream: str, partition_key: str) -> int:
        return self.high

    def read(self, *, stream, partition_key, after=None, limit=100):
        return []

    def checkpoint(self, *, consumer_id, cursor, ttl_seconds):
        if cursor.offset > self.high:
            raise ValueError("checkpoint beyond high watermark")
        self.checkpoints[(consumer_id, cursor.stream, cursor.partition_key)] = cursor

    def get_checkpoint(self, *, consumer_id, stream, partition_key):
        return self.checkpoints.get((consumer_id, stream, partition_key))


@dataclass
class MemorySnapshot:
    snapshot_id: str
    data_key: str
    source_cursor_start: str
    source_cursor_end: str


class MemoryCatalog:
    def __init__(self, snapshot: MemorySnapshot | None):
        self.snapshot = snapshot

    def current(self, dataset_id: str):
        return self.snapshot


class AdvancingCatalog(MemoryCatalog):
    def __init__(self, snapshot: MemorySnapshot, store: MemoryHandoffStore):
        super().__init__(snapshot)
        self.store = store

    def current(self, dataset_id: str):
        self.store.high += 1
        return self.snapshot


class MutableKeyProvider:
    def __init__(self, keys: SigningKeySet):
        self.keys = keys

    def load(self) -> SigningKeySet:
        return self.keys


class HandoffReadinessTests(unittest.TestCase):
    def make_handoff(self, store, provider=None):
        provider = provider or MutableKeyProvider(SigningKeySet("old", {"old": b"o" * 32}))
        codec = SignedHandoffCursorCodec(provider, clock_ns=lambda: 100)
        return GapFreeHandoff(store, codec, clock_ns=lambda: 100), provider

    def test_non_sqlite_store_and_exact_snapshot_watermark_pass(self):
        store = MemoryHandoffStore(7)
        self.assertIsInstance(store, HandoffStore)
        handoff, _ = self.make_handoff(store)
        bundle = SnapshotHandoffCoordinator(
            MemoryCatalog(MemorySnapshot("snapshot-7", "bars/7.parquet", "1", "7")),
            handoff,
        ).prepare(
            dataset_id="bars", consumer_id="alpha-a", stream=STREAM,
            partition_key=PARTITION, ttl_seconds=60,
        )
        self.assertEqual(bundle.snapshot_watermark, Cursor(STREAM, PARTITION, 7))
        self.assertEqual(bundle.grant.watermark_offset, 7)

    def test_missing_malformed_and_racing_snapshot_fail_closed(self):
        store = MemoryHandoffStore(7)
        handoff, _ = self.make_handoff(store)
        for snapshot, message in (
            (None, "unavailable"),
            (MemorySnapshot("s", "p", "bad", "7"), "non-logical"),
            (MemorySnapshot("s", "p", "1", "6"), "does not match"),
            (MemorySnapshot("s", "p", "8", "7"), "range is invalid"),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(
                SnapshotWatermarkMismatch, message
            ):
                SnapshotHandoffCoordinator(MemoryCatalog(snapshot), handoff).prepare(
                    dataset_id="bars", consumer_id="alpha-a", stream=STREAM,
                    partition_key=PARTITION, ttl_seconds=60,
                )

    def test_live_can_advance_after_watermark_without_changing_snapshot_boundary(self):
        store = MemoryHandoffStore(7)
        handoff, _ = self.make_handoff(store)
        catalog = AdvancingCatalog(
            MemorySnapshot("snapshot-7", "bars/7.parquet", "1", "7"), store
        )
        bundle = SnapshotHandoffCoordinator(catalog, handoff).prepare(
            dataset_id="bars", consumer_id="alpha-a", stream=STREAM,
            partition_key=PARTITION, ttl_seconds=60,
        )
        self.assertEqual(store.high, 8)
        self.assertEqual(bundle.snapshot_watermark.offset, 7)

    def test_unsigned_internal_cursor_is_rejected_by_public_codec(self):
        store = MemoryHandoffStore(0)
        handoff, _ = self.make_handoff(store)
        with self.assertRaisesRegex(ValueError, "invalid signed"):
            handoff.replay(
                token=Cursor(STREAM, PARTITION, 0).to_token(),
                consumer_id="alpha-a", stream=STREAM, partition_key=PARTITION,
            )

    def test_key_rotation_verifies_overlap_and_rejects_retired_key(self):
        store = MemoryHandoffStore(0)
        handoff, provider = self.make_handoff(store)
        old = handoff.issue(
            consumer_id="alpha-a", snapshot_id="s0",
            snapshot_watermark=Cursor(STREAM, PARTITION, 0), ttl_seconds=60,
        )
        provider.keys = SigningKeySet("new", {"old": b"o" * 32, "new": b"n" * 32})
        self.assertEqual(
            handoff.replay(
                token=old.token, consumer_id="alpha-a", stream=STREAM,
                partition_key=PARTITION,
            ),
            [],
        )
        new = handoff.issue(
            consumer_id="alpha-a", snapshot_id="s0",
            snapshot_watermark=Cursor(STREAM, PARTITION, 0), ttl_seconds=60,
        )
        self.assertNotEqual(old.token, new.token)
        provider.keys = SigningKeySet("new", {"new": b"n" * 32})
        with self.assertRaisesRegex(ValueError, "invalid signed"):
            handoff.replay(
                token=old.token, consumer_id="alpha-a", stream=STREAM,
                partition_key=PARTITION,
            )


if __name__ == "__main__":
    unittest.main()
