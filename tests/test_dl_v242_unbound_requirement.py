"""DL v2.4.2: a manifest-allowed requirement with no catalogue binding is a typed per-item refusal.

Found while retiring OKX RVN-USDT-SWAP: the deployed alpha.okx.live/sandbox realm r1 manifests still allowed the RVN
1d row while catalogue r17 had no binding. The KN backend raises `KeyError` from `StableSourceCatalog.binding_for`
(kn_query_backend.py latest/history), and V2QueryService let it escape: an untyped 500 for one item and a failed
snapshot batch for every other item in it. Now it is `INSTRUMENT_NOT_FOUND` (instrument not in the active catalogue)
or `UNSUPPORTED_FEED` (instrument present, product not bound), per item; unrelated `KeyError`s stay internal.
"""
import asyncio
import unittest

from qdl.query.contracts import BatchRequirement, CanonicalErrorCode, DataRequirement, QueryProblem
from qdl.query.entitlement import AccessPurpose, EntitlementPolicy
from qdl.query.results import InstrumentQuery, QueryBackendError
from qdl.query.service import QueryServiceError, V2QueryService
from qdl.domain.instrument import InstrumentRegistry
from tests.test_phase113_reference_v2 import record

LISTED = record(venue="OKX", market="SWAP", symbol="BTC-USDT-SWAP", base="BTC")
RETIRED_UID = "2f110c43-e43d-5784-8ceb-9970d2d16742"


def _bar(uid: str, interval: str = "1d") -> DataRequirement:
    return DataRequirement.from_mapping({
        "instrument_uid": uid, "feed": "BAR", "consumer_grade": "ALPHA", "source_policy_id": "crypto_primary_v2",
        "interval": interval, "warmup_limit": 10, "max_freshness_ms": 259200000, "require_full_coverage": True,
        "require_final_bars": True, "stale_policy": "BLOCK", "gap_policy": "BLOCK",
        "recovery": "SNAPSHOT_AND_REPLAY", "bar_revision_policy": "EMIT_REVISIONS",
    })


class _Catalog:
    """Binds only BTC 1d, like catalogue r17 after the retirement."""

    def binding_for(self, requirement):
        if requirement.instrument_uid == LISTED.instrument_uid and requirement.interval == "1d":
            return object()
        raise KeyError("requirement has no stable source binding")


class _Backend:
    catalog = _Catalog()

    def __init__(self, unrelated_key_error=False):
        self.unrelated_key_error = unrelated_key_error

    def _read(self, requirement):
        self.catalog.binding_for(requirement)  # what kn_query_backend.latest/history do first
        if self.unrelated_key_error:
            raise KeyError("some internal map")
        raise QueryBackendError(QueryProblem(CanonicalErrorCode.DATA_NOT_READY, "no rows yet", True))

    latest = history = _read


def _service(backend=None) -> V2QueryService:
    registry = InstrumentRegistry()
    registry.register(LISTED, [])
    return V2QueryService(instruments=InstrumentQuery(registry), backend=backend or _Backend(),
                          entitlements=EntitlementPolicy(()))


def _code(error):
    return error.problem.code


class UnboundRequirementTests(unittest.TestCase):
    def test_single_snapshot_of_a_retired_instrument_is_instrument_not_found(self):
        with self.assertRaises(QueryServiceError) as caught:
            _service().snapshot(_bar(RETIRED_UID), purpose=AccessPurpose.INTERNAL_ALPHA)
        self.assertEqual(_code(caught.exception), CanonicalErrorCode.INSTRUMENT_NOT_FOUND)
        self.assertFalse(caught.exception.problem.retryable)

    def test_a_listed_instrument_without_that_product_is_unsupported_feed(self):
        with self.assertRaises(QueryServiceError) as caught:
            _service().snapshot(_bar(LISTED.instrument_uid, "1h"), purpose=AccessPurpose.INTERNAL_ALPHA)
        self.assertEqual(_code(caught.exception), CanonicalErrorCode.UNSUPPORTED_FEED)

    def test_a_mixed_snapshot_batch_returns_every_item(self):
        requirements = (_bar(LISTED.instrument_uid), _bar(RETIRED_UID))
        result = asyncio.run(_service().snapshot_batch_async(
            requirements, purpose=AccessPurpose.INTERNAL_ALPHA, consumer_id="alpha.okx.live.stable"))
        codes = {item.requirement.instrument_uid: _code(item.error) for item in result.results}
        self.assertEqual(codes, {LISTED.instrument_uid: CanonicalErrorCode.DATA_NOT_READY,
                                 RETIRED_UID: CanonicalErrorCode.INSTRUMENT_NOT_FOUND})

    def test_warmup_single_and_batch_are_typed_per_item(self):
        service = _service()
        with self.assertRaises(QueryServiceError) as caught:
            service.warmup(_bar(RETIRED_UID), purpose=AccessPurpose.INTERNAL_ALPHA)
        self.assertEqual(_code(caught.exception), CanonicalErrorCode.INSTRUMENT_NOT_FOUND)
        batch = BatchRequirement("alpha.okx.live.stable", (_bar(LISTED.instrument_uid), _bar(RETIRED_UID)),
                                 require_all=False)
        result = asyncio.run(service.warmup_batch_async(batch, purpose=AccessPurpose.INTERNAL_ALPHA))
        codes = {item.instrument_uid: item.problem.code for item in result.results}
        self.assertEqual(codes[RETIRED_UID], CanonicalErrorCode.INSTRUMENT_NOT_FOUND)
        self.assertNotEqual(codes[LISTED.instrument_uid], CanonicalErrorCode.INTERNAL_ERROR)

    def test_an_unrelated_key_error_is_not_relabelled(self):
        with self.assertRaises(KeyError):
            _service(_Backend(unrelated_key_error=True)).snapshot(
                _bar(LISTED.instrument_uid), purpose=AccessPurpose.INTERNAL_ALPHA)


if __name__ == "__main__":
    unittest.main()
