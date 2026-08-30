from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

import yaml

from qdl.domain.instrument import InstrumentIdentity, ProductType
from qdl.query.entitlement import AccessPurpose, DataProduct
from qdl.runtime.provider_history import (
    PASS_THROUGH_LICENSE_REVISION,
    pass_through_source_id,
)
from qdl.runtime.routed_query import RoutedQueryBackend
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_source import StableSpoolQueryBackend, build_stable_query_stack
from qdl.transport.sqlite_spool import SpoolConfig, SQLiteDurableSpool

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
SCHEMA_DIGEST = "c" * 64


class PassThroughWiringTests(unittest.TestCase):
    """Declaring metadata must not open a data product on its own."""

    def setUp(self) -> None:
        self.directory = Path(tempfile.mkdtemp())
        self.catalog, self.unbound_uid = self._catalog_with_unbound()

    def _catalog_with_unbound(self):
        payload = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
        spare = copy.deepcopy(
            next(
                item
                for item in payload["instruments"]
                if item["venue"] == "BINANCE"
                and item["market"] == "USDM"
                and item["product_type"] == "PERPETUAL"
            )
        )
        identity = InstrumentIdentity.create(
            venue="BINANCE",
            market="USDM",
            product_type=ProductType("PERPETUAL"),
            canonical_symbol="XRP-USDT",
        )
        spare["instrument_uid"] = identity.instrument_uid
        spare["instrument_id"] = identity.instrument_id
        spare["canonical_symbol"] = "XRP-USDT"
        spare["native_symbol"] = "XRPUSDT"
        spare["base_asset"] = "XRP"
        payload["instruments"].append(spare)
        path = self.directory / "catalog.yaml"
        path.write_text(yaml.safe_dump(payload), encoding="utf-8")
        return StableSourceCatalog.load(path), identity.instrument_uid

    def _stack(self, *, enabled: bool, reference_enabled: bool = False):
        spool = SQLiteDurableSpool(
            SpoolConfig(path=self.directory / f"spool-{enabled}.sqlite3")
        )
        return build_stable_query_stack(
            spool=spool,
            catalog=self.catalog,
            schema_digest=SCHEMA_DIGEST,
            handoff=None,
            cursor_ttl_seconds=3600,
            pass_through_enabled=enabled,
            reference_data_enabled=reference_enabled,
        )

    def test_disabled_by_default_leaves_the_stack_unchanged(self):
        service, backend, _ = self._stack(enabled=False)
        self.assertIsInstance(backend, StableSpoolQueryBackend)
        self.assertNotIsInstance(service.backend, RoutedQueryBackend)
        self.assertIsNone(service.reference_batch)

    def test_reference_capability_is_separate_and_execution_is_shape_gated(self):
        service, backend, _ = self._stack(enabled=False, reference_enabled=True)
        self.assertIsInstance(backend, StableSpoolQueryBackend)
        self.assertNotIsInstance(service.backend, RoutedQueryBackend)
        self.assertIsNotNone(service.reference_batch)
        # The unbound declared instrument becomes resolvable only for the
        # explicitly enabled reference product; this does not open BAR history.
        record = self.catalog.instrument_registry(include_unbound=True).get(self.unbound_uid)
        assert service._reference_source_id is not None
        source_id = service._reference_source_id(record)
        alpha = service.entitlements.authorize(
            source_id=source_id,
            purpose=AccessPurpose.INTERNAL_ALPHA,
            product=DataProduct.CANONICAL_HISTORY,
            at_ns=1,
        )
        execution = service.entitlements.authorize(
            source_id=source_id,
            purpose=AccessPurpose.INTERNAL_EXECUTION,
            product=DataProduct.CANONICAL_HISTORY,
            at_ns=1,
        )
        self.assertTrue(alpha.allowed)
        # The reference runtime can authenticate an execution caller, but the
        # typed ReferenceDataRequirement still permits only one MARK snapshot.
        self.assertTrue(execution.allowed)

    def test_an_unbound_instrument_is_unresolvable_while_disabled(self):
        registry = self.catalog.instrument_registry()
        with self.assertRaises(KeyError):
            registry.get(self.unbound_uid)

    def test_enabling_it_routes_and_resolves_the_unbound_instrument(self):
        service, _backend, _ = self._stack(enabled=True)
        self.assertIsInstance(service.backend, RoutedQueryBackend)
        registry = self.catalog.instrument_registry(include_unbound=True)
        self.assertEqual(
            registry.get(self.unbound_uid).native_symbol, "XRPUSDT"
        )

    def _authorize(self, policy, purpose: AccessPurpose):
        return policy.authorize(
            source_id=pass_through_source_id(self.unbound_uid),
            purpose=purpose,
            product=DataProduct.CANONICAL_HISTORY,
            at_ns=1_787_385_300_000_000_000,
        )

    def test_no_pass_through_grant_exists_while_disabled(self):
        decision = self._authorize(
            self.catalog.entitlements(), AccessPurpose.INTERNAL_ALPHA
        )
        self.assertFalse(decision.allowed)

    def test_a_pass_through_grant_never_authorises_execution(self):
        policy = self.catalog.entitlements(include_unbound=True)
        alpha = self._authorize(policy, AccessPurpose.INTERNAL_ALPHA)
        execution = self._authorize(policy, AccessPurpose.INTERNAL_EXECUTION)
        self.assertTrue(alpha.allowed)
        self.assertEqual(alpha.license_revision, PASS_THROUGH_LICENSE_REVISION)
        self.assertFalse(
            execution.allowed,
            "pass-through output passed no canonical core and must never be "
            "licensed for execution",
        )

    def test_bound_sources_keep_authorising_when_the_flag_is_on(self):
        bound = self.catalog.bindings[0]
        for policy in (
            self.catalog.entitlements(),
            self.catalog.entitlements(include_unbound=True),
        ):
            decision = policy.authorize(
                source_id=bound.source_id,
                purpose=AccessPurpose.INTERNAL_ALPHA,
                product=DataProduct.CANONICAL_HISTORY,
                at_ns=1_787_385_300_000_000_000,
            )
            self.assertTrue(decision.allowed)


if __name__ == "__main__":
    unittest.main()
