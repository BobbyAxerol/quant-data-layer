"""IDR-040 fix 2: the quiet-book proof is an additive, report-only quality field.

The Rust core publishes a quiet-proof BOOK_SNAPSHOT whose
``OrderBookSnapshot.proof_observed_at_ns`` is the receipt time of in-band
transport evidence (see ``rust/qdl-realtime-core``). These tests pin the Python
side of the contract:

* the generated descriptor carries the new field as int64 tag 8 and decodes
  bytes written without it (old producers) as "no proof";
* Query reports it as ``quality.proof_observed_at_ns`` only for a verified book
  snapshot and never lets it change freshness, state or eligibility;
* the public response carries the key only when a proof exists, so a strict
  (2.0.x, ``extra="forbid"``) reader keeps parsing every other response, and
  the tolerant 2.1.0 reader accepts it either way.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from qdl.marketdata.v2 import market_data_pb2
from qdl.query import ConsumerGrade, DataRequirement, FeedType
from qdl.query.results import QualityMetadata
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_source import StableSpoolQueryBackend
from qdl_sdk.models import ProblemDetails, ProblemDiagnostics, QualityView
import tests.test_qdl_sdk_stream_projection as projection_tests
from qdl_sdk import Feed, StreamEvent, market_data_view_from_stream

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
PROOF_NS = 1_800_000_000_500_000_000


class LegacyQualityView(BaseModel):
    """The QualityView shape a strict 2.0.x reader validates."""

    model_config = ConfigDict(extra="forbid")
    state: str
    freshness_ms: int
    event_recency_state: str = "LIVE"
    provider_session_state: str = "NOT_APPLICABLE"
    provider_session_liveness_ms: int | None = None
    gap_open: bool
    complete: bool
    execution_eligible: bool
    policy_id: str
    flags: list[str]


def _quality(**extra) -> QualityView:
    return QualityView(state="STALE", freshness_ms=4_000, event_recency_state="STALE",
                       gap_open=False, complete=True, execution_eligible=False,
                       policy_id="crypto_primary_v2", flags=["LAST_EVENT_STALE"], **extra)


class GeneratedContractTests(unittest.TestCase):
    def test_field_is_additive_int64_tag_8(self) -> None:
        field = market_data_pb2.OrderBookSnapshot.DESCRIPTOR.fields_by_name["proof_observed_at_ns"]
        self.assertEqual((field.number, field.type), (8, field.TYPE_INT64))
        old_fields = {f.name: f.number for f in market_data_pb2.OrderBookSnapshot.DESCRIPTOR.fields}
        self.assertEqual(
            {name: number for name, number in old_fields.items() if number < 8},
            {"native_sequence": 1, "checksum": 2, "levels": 3, "depth": 4,
             "book_generation": 5, "sequence_verified": 6, "truncated": 7},
        )

    def test_old_bytes_mean_no_proof_and_unset_field_keeps_old_bytes(self) -> None:
        book = market_data_pb2.OrderBookSnapshot(native_sequence="101", depth=1,
                                                book_generation=1, sequence_verified=True)
        encoded = book.SerializeToString()
        self.assertEqual(market_data_pb2.OrderBookSnapshot.FromString(encoded).proof_observed_at_ns, 0)
        proven = market_data_pb2.OrderBookSnapshot()
        proven.CopyFrom(book)
        proven.proof_observed_at_ns = PROOF_NS
        self.assertTrue(proven.SerializeToString().startswith(encoded))
        self.assertEqual(
            market_data_pb2.OrderBookSnapshot.FromString(proven.SerializeToString()).proof_observed_at_ns,
            PROOF_NS,
        )


class ResponseCompatibilityTests(unittest.TestCase):
    def test_no_proof_keeps_the_old_wire_shape(self) -> None:
        wire = json.loads(_quality().model_dump_json())
        self.assertNotIn("proof_observed_at_ns", wire)
        LegacyQualityView.model_validate(wire)

    def test_proof_is_emitted_and_read_back_by_the_tolerant_reader(self) -> None:
        quality = _quality(proof_observed_at_ns=PROOF_NS)
        wire = json.loads(quality.model_dump_json())
        self.assertEqual(wire["proof_observed_at_ns"], PROOF_NS)
        self.assertEqual(QualityView.model_validate(wire), quality)
        # A strict 2.0.x reader would refuse it: the reason the key is gated.
        with self.assertRaises(ValueError):
            LegacyQualityView.model_validate(wire)

    def test_non_positive_proof_is_rejected(self) -> None:
        for value in (0, -1):
            with self.assertRaises(ValueError):
                _quality(proof_observed_at_ns=value)
        with self.assertRaises(ValueError):
            QualityMetadata(state="LIVE", freshness_ms=0, gap_open=False, complete=True,
                            execution_eligible=True, policy_id="p", proof_observed_at_ns=0)

    def test_refusal_diagnostics_carry_the_key_only_with_a_proof(self) -> None:
        base = dict(evaluated_at_ns=1, state="STALE", freshness_ms=4_000,
                    event_recency_state="STALE", provider_session_state="NOT_APPLICABLE",
                    execution_eligible=False, gap_open=False, complete=True)
        plain = ProblemDiagnostics(**base).model_dump(mode="json")
        self.assertNotIn("proof_observed_at_ns", plain)
        problem = ProblemDetails(type="urn:qdl:DATA_STALE", title="DATA_STALE", status=409,
                                 code="DATA_STALE", detail="stale", request_id="test-only",
                                 retryable=True,
                                 diagnostics=ProblemDiagnostics(**base, proof_observed_at_ns=PROOF_NS))
        wire = json.loads(problem.model_dump_json())
        self.assertEqual(wire["diagnostics"]["proof_observed_at_ns"], PROOF_NS)
        self.assertEqual(ProblemDetails.model_validate(wire), problem)


class QueryQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(CATALOG_PATH)
        cls.binding = next(
            item for item in cls.catalog.bindings
            if item.feed is FeedType.BOOK_SNAPSHOT
            and item.instrument.identity.venue == "BINANCE"
        )

    def backend(self, now_ns: int) -> StableSpoolQueryBackend:
        return StableSpoolQueryBackend(None, self.catalog, schema_digest="a" * 64,
                                       clock_ns=lambda: now_ns)

    def requirement(self) -> DataRequirement:
        return DataRequirement(
            instrument_uid=self.binding.instrument.instrument_uid,
            feed=self.binding.feed,
            interval=self.binding.interval,
            consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id=self.binding.source_policy_id,
            max_freshness_ms=2_000,
        )

    def book(self, *, proof: int = 0, verified: bool = True) -> market_data_pb2.EventEnvelope:
        envelope = market_data_pb2.EventEnvelope(
            source_event_time_ns=1_800_000_000_000_000_000,
            received_at_ns=1_800_000_000_010_000_000,
        )
        envelope.book_snapshot.native_sequence = "101"
        envelope.book_snapshot.depth = 1
        envelope.book_snapshot.book_generation = 1 if verified else 0
        envelope.book_snapshot.sequence_verified = verified
        envelope.book_snapshot.proof_observed_at_ns = proof
        return envelope

    def quality(self, envelope, now_ns: int) -> QualityMetadata:
        return self.backend(now_ns)._quality(self.requirement(), self.binding, envelope,
                                             gap_open=False, watermark_offset=1)

    def test_proof_is_reported_but_never_changes_freshness_or_state(self) -> None:
        now = 1_800_000_005_000_000_000  # 5 s after the provider event
        plain = self.quality(self.book(), now)
        proven = self.quality(self.book(proof=PROOF_NS), now)
        self.assertIsNone(plain.proof_observed_at_ns)
        self.assertEqual(proven.proof_observed_at_ns, PROOF_NS)
        for field in ("state", "freshness_ms", "event_recency_state", "execution_eligible",
                      "complete", "gap_open", "flags", "provider_session_state"):
            self.assertEqual(getattr(proven, field), getattr(plain, field), field)
        self.assertEqual(proven.freshness_ms, 5_000)
        self.assertEqual(proven.state, "STALE")
        self.assertFalse(proven.execution_eligible)

    def test_unverified_book_never_reports_a_proof(self) -> None:
        quality = self.quality(self.book(proof=PROOF_NS, verified=False), 1_800_000_000_100_000_000)
        self.assertIsNone(quality.proof_observed_at_ns)


class StreamProjectionTests(unittest.TestCase):
    def test_sdk_stream_projection_reports_the_proof_and_keeps_policy(self) -> None:
        requirement = projection_tests.SdkStreamProjectionTests.requirement(None, Feed.BOOK_SNAPSHOT)
        proven = projection_tests.envelope(Feed.BOOK_SNAPSHOT)
        proven.book_snapshot.proof_observed_at_ns = PROOF_NS
        view = market_data_view_from_stream(
            StreamEvent(12, "signed", proven), template=projection_tests.template(Feed.BOOK_SNAPSHOT),
            requirement=requirement, now_ns=projection_tests.NOW + 100_000_000)
        plain = market_data_view_from_stream(
            StreamEvent(12, "signed", projection_tests.envelope(Feed.BOOK_SNAPSHOT)),
            template=projection_tests.template(Feed.BOOK_SNAPSHOT),
            requirement=requirement, now_ns=projection_tests.NOW + 100_000_000)
        self.assertEqual(view.quality.proof_observed_at_ns, PROOF_NS)
        self.assertIsNone(plain.quality.proof_observed_at_ns)
        self.assertEqual(
            view.quality.model_dump(exclude={"proof_observed_at_ns"}),
            plain.quality.model_dump(exclude={"proof_observed_at_ns"}),
        )


if __name__ == "__main__":
    unittest.main()
