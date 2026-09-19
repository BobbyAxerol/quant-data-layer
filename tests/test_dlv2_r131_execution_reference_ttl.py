"""R1.31: an execution-grade snapshot may not be cached for as long as its own bound.

Measured on the live stack on 2026-09-19, over ten minutes of the consumer's own
`StaleExecutionReferenceError` lines: the OKX `index_price` component never came
back younger than **2,013 ms**, while `mark_price` stayed at 31-106 ms. The floor
is the signature - it is the 2,000 ms snapshot TTL plus transport, not a venue
problem. Called directly, OKX publishes the index 132-759 ms fresh.

The consumer validates the pair against `limit_ms=2000`. A cache whose TTL equals
that limit cannot satisfy it: the value is served up to one whole TTL after it was
fetched, on top of whatever age it already carried. These tests pin the
relationship rather than the number, so the arithmetic has to keep holding if
either side is ever retuned.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace

from qdl.reference.batch import ReferenceBatchPolicy
from qdl.reference.contracts import ReferenceProduct, ReferenceStatus

# What the consumer enforces, from its own error line:
# `V2 execution mark/index reference is stale: ages_ms={...} limit_ms=2000`.
CONSUMER_FRESHNESS_LIMIT_MS = 2_000
# The worst age the venue itself put on an index row, measured five times.
WORST_VENUE_INDEX_AGE_MS = 759


def _result(product: ReferenceProduct, *, history: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        status=ReferenceStatus.OK,
        request=SimpleNamespace(product=product, is_history=history),
    )


class ExecutionReferenceTtlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = ReferenceBatchPolicy()

    def test_mark_index_uses_the_execution_ttl_not_the_generic_snapshot_ttl(self) -> None:
        self.assertEqual(
            self.policy.ttl_seconds(_result(ReferenceProduct.MARK_INDEX_PRICE)),
            self.policy.execution_snapshot_ttl_seconds,
        )

    def test_other_snapshot_products_keep_the_generic_ttl(self) -> None:
        for product in (
            ReferenceProduct.FUNDING_RATE,
            ReferenceProduct.OPEN_INTEREST,
            ReferenceProduct.LONG_SHORT_RATIO,
            ReferenceProduct.TAKER_FLOW,
            ReferenceProduct.BASIS,
        ):
            with self.subTest(product=product):
                self.assertEqual(
                    self.policy.ttl_seconds(_result(product)),
                    self.policy.snapshot_ttl_seconds,
                )

    def test_contract_metadata_is_still_cached_for_minutes(self) -> None:
        self.assertEqual(
            self.policy.ttl_seconds(_result(ReferenceProduct.CONTRACT_METADATA)),
            self.policy.metadata_ttl_seconds,
        )

    def test_history_still_wins_over_the_execution_ttl(self) -> None:
        """A mark/index *history* read is not the execution snapshot path."""
        self.assertEqual(
            self.policy.ttl_seconds(_result(ReferenceProduct.MARK_INDEX_PRICE, history=True)),
            self.policy.history_ttl_seconds,
        )

    def test_the_execution_ttl_leaves_the_worst_measured_venue_age_inside_the_bound(self) -> None:
        """The relationship, not the number: TTL + venue age must fit the limit."""
        ttl_ms = self.policy.execution_snapshot_ttl_seconds * 1000
        self.assertLess(ttl_ms + WORST_VENUE_INDEX_AGE_MS, CONSUMER_FRESHNESS_LIMIT_MS)

    def test_the_generic_snapshot_ttl_alone_would_not_have_fit(self) -> None:
        """Why this change exists, stated as an assertion rather than a comment."""
        ttl_ms = ReferenceBatchPolicy().snapshot_ttl_seconds * 1000
        self.assertGreaterEqual(ttl_ms, CONSUMER_FRESHNESS_LIMIT_MS)

    def test_a_negative_execution_ttl_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            ReferenceBatchPolicy(execution_snapshot_ttl_seconds=-1.0)

    def test_the_execution_ttl_is_not_so_short_that_it_outruns_the_venue_bucket(self) -> None:
        """Five OKX index ids refreshed at this TTL against a bucket refilling at 10/s."""
        okx_index_ids = 5
        okx_market_bucket_refill_per_second = 10
        requests_per_second = okx_index_ids / self.policy.execution_snapshot_ttl_seconds
        self.assertLess(requests_per_second, okx_market_bucket_refill_per_second)


if __name__ == "__main__":
    unittest.main()
