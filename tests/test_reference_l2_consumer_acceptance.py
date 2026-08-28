"""Source-only checks for the bounded Reference/L2 V2 consumer receipt."""

from __future__ import annotations

import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from qdl.certification.reference_l2_acceptance import (
    _DAY_NS,
    _FUNDING_NS,
    _history_bounds,
    REFERENCE_L2_CONSUMER_ID,
    build_reference_l2_acceptance_scope,
    reference_evidence,
)
from qdl.certification.phase103_consumer_acceptance import _validate_payload
from qdl.query import FeedType
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl_sdk import Grade
from qdl_sdk.reference import ReferenceProduct, ReferenceRequirement


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
MANIFEST = ROOT / "consumers/stable/reference-l2-stable.yaml"
NOW_NS = 1_787_000_000_000_000_000


class ReferenceL2ConsumerAcceptanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG)
        self.acquisition = StableAcquisitionPlan.load(ACQUISITION, catalog=self.catalog)
        self.scope = build_reference_l2_acceptance_scope(
            MANIFEST,
            catalog=self.catalog,
            acquisition=self.acquisition,
            now_ns=NOW_NS,
        )

    def test_scope_is_exact_v2_only_reference_and_book_product_set(self):
        self.assertEqual(len(self.scope.references), 55)
        self.assertEqual(len(self.scope.books), 24)
        self.assertEqual(
            {item.consumer_id for item in self.scope.references + self.scope.books},
            {REFERENCE_L2_CONSUMER_ID},
        )
        self.assertFalse(any(
            item.venue == "OKX" and item.market == "SWAP"
            and item.requirement.feed.value == "BASIS"
            for item in self.scope.references
        ))
        self.assertTrue(all(item.delivery.value == "DURABLE" for item in self.scope.books))

    def test_reference_request_preserves_manifest_identity_and_required_selectors(self):
        for item in self.scope.references:
            request = item.sdk_requirement
            self.assertEqual(request.instrument_uid, item.instrument_uid)
            self.assertEqual(request.source_policy_id, "crypto_liquid_v2")
            self.assertEqual(request.consumer_grade.value, "RESEARCH")
            if item.requirement.feed.value in {"LONG_SHORT_RATIO", "TAKER_FLOW", "BASIS"}:
                self.assertEqual(request.interval, "1d")
                self.assertIsNotNone(request.start_time_ns)
                self.assertIsNotNone(request.end_time_ns)
            if item.requirement.feed.value == "OPEN_INTEREST":
                if item.venue == "BINANCE":
                    self.assertEqual(request.interval, "1d")
                    self.assertIsNotNone(request.start_time_ns)
                    self.assertIsNotNone(request.end_time_ns)
                else:
                    self.assertEqual(item.venue, "OKX")
                    self.assertIsNone(request.interval)
                    self.assertIsNone(request.start_time_ns)
                    self.assertIsNone(request.end_time_ns)
            if item.requirement.feed.value == "LONG_SHORT_RATIO":
                self.assertEqual(request.long_short_kind.value, "GLOBAL_ACCOUNT")
            if item.requirement.feed.value == "BASIS":
                self.assertEqual(request.basis_contract_type, "PERPETUAL")

    def test_history_windows_stop_at_the_last_settled_provider_period(self):
        daily_boundary = (NOW_NS // _DAY_NS) * _DAY_NS
        funding_boundary = (NOW_NS // _FUNDING_NS) * _FUNDING_NS
        self.assertEqual(
            _history_bounds(FeedType.FUNDING_RATE, NOW_NS),
            (funding_boundary - 2 * _FUNDING_NS, funding_boundary - _FUNDING_NS),
        )
        self.assertEqual(
            _history_bounds(FeedType.TAKER_FLOW, NOW_NS),
            (daily_boundary - 2 * _DAY_NS, daily_boundary - _DAY_NS),
        )

    def test_reference_evidence_accepts_zero_decimal_but_rejects_blank_unit(self):
        product = next(item for item in self.scope.references if item.requirement.feed.value == "FUNDING_RATE")
        field = SimpleNamespace(
            name="funding_rate",
            unit="DIMENSIONLESS_RATE",
            value=SimpleNamespace(source_text="0", coefficient="0", scale=0),
        )
        observation = SimpleNamespace(
            instrument_uid=product.instrument_uid,
            product=product.sdk_requirement.product,
            observed_at_ns=NOW_NS,
            fields=[field],
        )
        data = SimpleNamespace(
            instrument_uid=product.instrument_uid,
            product=product.sdk_requirement.product,
            received_at_ns=NOW_NS,
            coverage=SimpleNamespace(complete_left=True, complete_right=True, truncated=False, terminal_reason="TEST"),
            lineage=[SimpleNamespace(provider="TEST", provider_endpoint="TEST", capability_name="test", source_role="REFERENCE")],
            observations=[observation],
        )
        item = SimpleNamespace(
            instrument_uid=product.instrument_uid,
            product=product.sdk_requirement.product,
            status="OK",
            problem=None,
            data=data,
        )
        self.assertEqual(len(reference_evidence(product, item, observed_at_ns=NOW_NS)), 64)
        field.unit = ""
        with self.assertRaisesRegex(ValueError, "unit"):
            reference_evidence(product, item, observed_at_ns=NOW_NS)

    def test_three_day_manifest_freshness_is_a_valid_public_reference_contract(self):
        request = ReferenceRequirement(
            instrument_uid="fixture",
            product=ReferenceProduct.LONG_SHORT_RATIO,
            consumer_grade=Grade.RESEARCH,
            source_policy_id="crypto_liquid_v2",
            interval="1d",
            start_time_ns=NOW_NS - 86_400_000_000_000,
            end_time_ns=NOW_NS,
            long_short_kind="GLOBAL_ACCOUNT",
            max_freshness_ms=259_200_000,
        )
        self.assertEqual(request.max_freshness_ms, 259_200_000)

    def test_book_validator_requires_verified_generation_and_allows_zero_delta_quantity(self):
        level = SimpleNamespace(
            price=SimpleNamespace(source_text="1", coefficient="1", scale=0),
            quantity=SimpleNamespace(source_text="0", coefficient="0", scale=0),
            quantity_unit=SimpleNamespace(value="CONTRACT"),
        )
        delta = SimpleNamespace(
            native_sequence_start="1",
            native_sequence_end="2",
            snapshot_sequence="1",
            sequence_verified=True,
            book_generation=1,
            updates=[level],
        )
        product = next(item for item in self.scope.books if item.feed.value == "BOOK_DELTA")
        _validate_payload(product, SimpleNamespace(payload=delta))
        delta.sequence_verified = False
        with self.assertRaisesRegex(ValueError, "verified"):
            _validate_payload(product, SimpleNamespace(payload=delta))


if __name__ == "__main__":
    unittest.main()
