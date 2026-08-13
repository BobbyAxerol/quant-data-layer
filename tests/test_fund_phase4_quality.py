from __future__ import annotations

import unittest
import tempfile
from pathlib import Path

from qdl.quality import (
    FeedKey,
    FeedQualityLedger,
    FeedQualityState,
    Observation,
    SourceAuthorityController,
    SourceAuthorityState,
    SourceCandidate,
    SourceRole,
    ValidationLevel,
)
from qdl.pipeline import ValidatedCanonicalPipeline
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig


KEY = FeedKey("binance-usdm", "instrument", "book")


def observation(index: int, **overrides) -> Observation:
    values = {
        "key": KEY,
        "event_id": index.to_bytes(16, "big"),
        "received_at_ns": 1_000_000_000 + index,
        "source_time_ns": 900_000_000 + index,
        "source_sequence": index,
        "freshness_limit_ns": 200_000_000,
    }
    values.update(overrides)
    return Observation(**values)


class QualityLedgerTests(unittest.TestCase):
    def test_validation_levels_do_not_treat_canonical_as_execution_eligible(self):
        ledger = FeedQualityLedger()
        invalid = ledger.observe(observation(1, canonical_valid=False))
        self.assertEqual(invalid.level, ValidationLevel.SOURCE)
        reference = ledger.observe(observation(2, source_authoritative=False))
        self.assertEqual(reference.level, ValidationLevel.CANONICAL)
        self.assertFalse(reference.executable)

    def test_duplicate_out_of_order_gap_and_verified_resync(self):
        ledger = FeedQualityLedger()
        self.assertTrue(ledger.observe(observation(1)).executable)
        duplicate = ledger.observe(observation(1))
        self.assertTrue(duplicate.duplicate)
        self.assertIn("DUPLICATE", duplicate.flags)

        gap = ledger.observe(observation(3))
        self.assertEqual(gap.state, FeedQualityState.GAPPED)
        self.assertEqual(gap.gap.expected_sequence, 2)
        self.assertFalse(gap.executable)
        ledger.begin_resync(KEY)
        ledger.complete_resync(
            KEY, snapshot_sequence=3, source_time_ns=900_000_003,
            completed_at_ns=1_000_000_100,
        )
        self.assertEqual(ledger.snapshot(KEY)["state"], "LIVE")
        self.assertTrue(ledger.observe(observation(4)).executable)

        old = ledger.observe(observation(5, source_sequence=2, source_time_ns=800_000_000))
        self.assertIn("OUT_OF_ORDER", old.flags)
        self.assertIn("CLOCK_SKEW_SUSPECTED", old.flags)
        self.assertFalse(old.executable)

    def test_stale_and_missing_are_not_coerced_to_live(self):
        ledger = FeedQualityLedger()
        stale = ledger.observe(observation(
            1, received_at_ns=2_000_000_000, source_time_ns=1_000_000_000,
            freshness_limit_ns=100,
        ))
        self.assertIn("STALE", stale.flags)
        self.assertFalse(stale.executable)
        missing = ledger.observe(observation(2, source_time_ns=None, source_sequence=None))
        self.assertIn("SOURCE_TIME_MISSING", missing.flags)
        self.assertIn("SEQUENCE_MISSING", missing.flags)


class SourceAuthorityTests(unittest.TestCase):
    def candidate(self, source: str, venue: str, role: SourceRole, healthy: bool):
        return SourceCandidate(source, venue, role, healthy, healthy)

    def test_failover_requires_hysteresis_and_explicit_secondary_policy(self):
        controller = SourceAuthorityController(
            primary_source_id="dnse", primary_venue="HOSE",
            secondary_source_id="vnstock", secondary_venue="HOSE",
            failover_delay_ns=5, recovery_delay_ns=5,
        )
        bad = self.candidate("dnse", "HOSE", SourceRole.PRIMARY, False)
        good_secondary = self.candidate("vnstock", "HOSE", SourceRole.SECONDARY, True)
        for now in (1, 2, 8):
            action = controller.evaluate(
                primary=bad, secondary=good_secondary, now_ns=now,
                allow_secondary_execution=True,
            )
        self.assertEqual(action.state, SourceAuthorityState.SECONDARY_VALIDATING)
        action = controller.evaluate(
            primary=bad, secondary=good_secondary, now_ns=9,
            allow_secondary_execution=True,
        )
        self.assertTrue(action.execution_eligible)
        self.assertEqual(action.selected_source_id, "vnstock")

    def test_cross_venue_reference_never_silently_becomes_authoritative(self):
        controller = SourceAuthorityController(
            primary_source_id="binance", primary_venue="BINANCE",
            secondary_source_id="okx", secondary_venue="OKX",
            failover_delay_ns=0,
        )
        bad = self.candidate("binance", "BINANCE", SourceRole.PRIMARY, False)
        reference = self.candidate("okx", "OKX", SourceRole.REFERENCE, True)
        for now in range(4):
            action = controller.evaluate(
                primary=bad, secondary=reference, now_ns=now,
                allow_secondary_execution=True,
            )
        self.assertFalse(action.execution_eligible)
        self.assertIsNone(action.selected_source_id)


class RawLineageAndQuarantineTests(unittest.TestCase):
    def test_unknown_instrument_keeps_raw_bytes_and_quarantines_without_coercion(self):
        with tempfile.TemporaryDirectory() as directory:
            with SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "quality.sqlite3",
                max_records=10,
                max_payload_bytes=10_000,
                max_event_bytes=1_000,
                min_free_disk_bytes=0,
            )) as spool:
                raw = DurableEvent(
                    stream="md.raw.v1.okx.swap.trade",
                    partition_key="unknown/trade/okx",
                    event_id=b"x" * 16,
                    payload=b'{"instId":"UNKNOWN"}',
                    accepted_at_ns=1,
                    content_type="application/json",
                    headers={"provenance": "REAL_PROVIDER_CAPTURE"},
                )
                result = ValidatedCanonicalPipeline(
                    spool,
                    canonicalizer=lambda _: (_ for _ in ()).throw(
                        ValueError("unknown instrument alias")
                    ),
                ).accept(raw)
                self.assertEqual(result.reason_code, "UNKNOWN_INSTRUMENT")
                self.assertIsNotNone(result.quarantine_id)
                stored = spool.find_event(stream=raw.stream, event_id=raw.event_id)
                self.assertEqual(stored.event.payload, raw.payload)
                quarantine = spool.quarantine_records()[0]
                self.assertEqual(quarantine["payload_sha256"], stored.payload_sha256)


if __name__ == "__main__":
    unittest.main()
