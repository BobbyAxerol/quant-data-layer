"""A binding at one interval must not deny the pass-through at another.

Routing decides per requirement — instrument, feed and interval together — so
an instrument materialised at 1m still routes to the pass-through when a
consumer asks for 15m. Granting the pass-through only to *unbound* instruments
refused exactly that case, and refused it as unlicensed, which is both the
wrong answer and the wrong reason.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from qdl.query import AccessPurpose, DataProduct
from qdl.query.contracts import (
    ConsumerGrade,
    DataRequirement,
    FeedType,
    RecoveryPolicy,
)
from qdl.runtime.provider_history import pass_through_source_id
from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
BINANCE_ETH = "ee93fabf-68df-5b50-8924-51bf25a5a757"


def _authorize(policy, uid: str, purpose: AccessPurpose):
    return policy.authorize(
        source_id=pass_through_source_id(uid),
        purpose=purpose,
        product=DataProduct.CANONICAL_HISTORY,
        at_ns=1,
    )


class PassThroughGrantScopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.bound = {
            binding.instrument.identity.instrument_uid
            for binding in self.catalog.bindings
        }

    def test_the_chosen_instrument_really_is_bound(self) -> None:
        """Guards the test itself: a typo here would make it prove nothing."""
        self.assertIn(BINANCE_ETH, self.bound)

    def test_a_bound_instrument_still_receives_the_pass_through_grant(self) -> None:
        policy = self.catalog.entitlements(include_unbound=True)
        decision = _authorize(policy, BINANCE_ETH, AccessPurpose.INTERNAL_ALPHA)
        self.assertTrue(decision.allowed, decision.reason_code)

    def test_every_declared_instrument_receives_the_grant(self) -> None:
        policy = self.catalog.entitlements(include_unbound=True)
        for record in self.catalog.instruments:
            decision = _authorize(
                policy, record.identity.instrument_uid, AccessPurpose.INTERNAL_ALPHA
            )
            self.assertTrue(
                decision.allowed,
                f"{record.identity.instrument_id}: {decision.reason_code}",
            )

    def test_the_grant_never_carries_execution(self) -> None:
        policy = self.catalog.entitlements(include_unbound=True)
        for record in self.catalog.instruments:
            decision = _authorize(
                policy,
                record.identity.instrument_uid,
                AccessPurpose.INTERNAL_EXECUTION,
            )
            self.assertFalse(decision.allowed)
            self.assertEqual(decision.reason_code, "PURPOSE_NOT_ALLOWED")

    def test_the_product_stays_shut_while_the_flag_is_off(self) -> None:
        policy = self.catalog.entitlements()
        for record in self.catalog.instruments:
            decision = _authorize(
                policy, record.identity.instrument_uid, AccessPurpose.INTERNAL_ALPHA
            )
            self.assertFalse(decision.allowed)
            self.assertEqual(decision.reason_code, "NO_ACTIVE_ENTITLEMENT")

    def test_a_bound_interval_is_not_routed_to_the_pass_through(self) -> None:
        """The extra grant must not become a way to bypass the spool."""
        from qdl.runtime.provider_history import ProviderBarHistorySource
        from qdl.runtime.routed_query import RoutedQueryBackend

        class _Spool:
            catalog = self.catalog
            schema_digest = "d" * 64

            def history(self, requirement):
                return "FROM_SPOOL"

        routed = RoutedQueryBackend(
            _Spool(), ProviderBarHistorySource(self.catalog)
        )
        bound_requirement = DataRequirement(
            instrument_uid=BINANCE_ETH,
            feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="1m",
            warmup_limit=3,
            recovery=RecoveryPolicy.FRESH_SNAPSHOT,
        )
        self.assertFalse(routed.routes_to_pass_through(bound_requirement))
        self.assertEqual(routed.history(bound_requirement), "FROM_SPOOL")

        from dataclasses import replace

        self.assertTrue(
            routed.routes_to_pass_through(replace(bound_requirement, interval="15m"))
        )


if __name__ == "__main__":
    unittest.main()
