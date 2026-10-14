"""IDR-099: a canonical lineage refusal on the warmup path carries a typed reason.

RCA 2026-10-14 (isolated real-capture replay): the stable catalog refused every
TRADE/QUOTE row (adapter_version differed from the binding) with a plain
``ValueError``; the warmup batch's generic branch turned it into an opaque
``INTERNAL_ERROR`` whose refusal line had ``reasons: []``. The refusal keeps its
code, status and retryability; it now names ``CANONICAL_LINEAGE_MISMATCH``, and any
other untyped exception names its class (``UNTYPED_<CLASS>``) as an additive reason.
"""

from __future__ import annotations

import json
import unittest
from types import SimpleNamespace

from qdl.query import (
    AccessPurpose,
    BatchRequirement,
    CanonicalErrorCode,
    ConsumerGrade,
    DataRequirement,
    FeedType,
    V2QueryService,
)
from qdl.query.results import CANONICAL_LINEAGE_MISMATCH, CanonicalLineageMismatch
from qdl.query.service import QueryServiceError, untyped_failure_reason
from qdl.runtime.refusal_log import RefusalLog
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.warmup.executor import WarmupExecution
from tests.test_phaseb_stable_edge import CATALOG_PATH, _stable_event

GENERIC_DETAIL = "warmup batch item failed inside the bounded executor"


def _service(outcome):
    class Backend:
        @staticmethod
        def warmup_is_local(_requirement):
            return True

        @staticmethod
        def history_many(requirements):
            return {requirement: outcome for requirement in requirements}

    class Executor:
        @staticmethod
        def stats():
            return {}

        async def execute(self, items, *, work, identity, provider, deadline_ms):
            del identity, provider, deadline_ms
            executions = []
            for item in items:
                try:  # the bounded executor keeps any exception as the item outcome
                    executions.append(WarmupExecution(item, await work(item), None, 1, False, 0.0))
                except Exception as error:  # noqa: BLE001 - mirrors BoundedWarmupExecutor
                    executions.append(WarmupExecution(item, None, error, 1, False, 0.0))
            return tuple(executions)

    class Service(V2QueryService):
        def __init__(self):
            self.backend = Backend()
            self.instruments = SimpleNamespace()
            self.warmup_executor = Executor()
            self.last_batch_evidence = {}

        def _warmup_from_history(self, requirement, history, *, purpose, request_id):
            raise AssertionError("a failed history never reaches rendering")

    return Service()


REQUIREMENT = DataRequirement(
    instrument_uid="uid-1", feed=FeedType.TRADE, consumer_grade=ConsumerGrade.EXECUTION,
    source_policy_id="crypto_primary_v2",
)


async def _batch(service):
    async def completion(result):
        return result
    return await service.warmup_batch_completed_async(
        BatchRequirement(consumer_id="ts.writer", requirements=(REQUIREMENT,)),
        purpose=AccessPurpose.INTERNAL_EXECUTION, completion=completion,
    )


class CatalogLineageTypeTests(unittest.TestCase):
    def test_adapter_mismatch_is_a_typed_value_error(self):
        catalog = StableSourceCatalog.load(CATALOG_PATH)
        event = _stable_event(catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade")
        self.assertEqual(catalog.binding_for_envelope(event).binding_id, "binance-usdm-btcusdt-trade")
        event.adapter_version = "p184-synthetic-test-only"
        with self.assertRaises(CanonicalLineageMismatch) as raised:
            catalog.binding_for_envelope(event)
        self.assertIsInstance(raised.exception, ValueError)  # every existing ``except ValueError`` unchanged
        self.assertEqual(raised.exception.reason, CANONICAL_LINEAGE_MISMATCH)


class WarmupBatchReasonTests(unittest.IsolatedAsyncioTestCase):
    async def test_lineage_mismatch_keeps_code_and_retryability_with_a_typed_reason(self):
        result = await _batch(_service(CanonicalLineageMismatch(
            "canonical event identity/lineage differs from stable binding")))
        item = result.results[0]
        self.assertEqual(item.status, "INTERNAL_ERROR")
        self.assertEqual(item.problem.code, CanonicalErrorCode.INTERNAL_ERROR)
        self.assertFalse(item.problem.retryable)
        self.assertTrue(item.problem.detail.startswith("CANONICAL_LINEAGE_MISMATCH: "))
        self.assertEqual(item.reason, CANONICAL_LINEAGE_MISMATCH)

    async def test_other_untyped_errors_keep_the_generic_detail_and_name_their_class(self):
        item = (await _batch(_service(ValueError("free text, never logged")))).results[0]
        self.assertEqual((item.status, item.problem.retryable), ("INTERNAL_ERROR", False))
        self.assertEqual(item.problem.detail, GENERIC_DETAIL)  # same detail digest as before
        self.assertEqual(item.reason, "UNTYPED_VALUEERROR")

    async def test_single_warmup_raises_the_same_problem_with_the_reason(self):
        service = _service(CanonicalLineageMismatch("canonical event identity/lineage differs from stable binding"))
        with self.assertRaises(QueryServiceError) as raised:
            await service.warmup_async(REQUIREMENT, purpose=AccessPurpose.INTERNAL_EXECUTION)
        self.assertEqual(raised.exception.problem.code, CanonicalErrorCode.INTERNAL_ERROR)
        self.assertFalse(raised.exception.problem.retryable)
        self.assertEqual(raised.exception.reason, CANONICAL_LINEAGE_MISMATCH)


class RefusalLineTests(unittest.TestCase):
    def _line(self, **kwargs):
        lines = []
        RefusalLog(emit=lines.append, replica="r").record(origin="warmup_batch_item", code="INTERNAL_ERROR",
                                                          status=500, retryable=False, **kwargs)
        return json.loads(lines[0])

    def test_lineage_detail_and_untyped_class_reach_the_refusal_line(self):
        typed = self._line(detail="CANONICAL_LINEAGE_MISMATCH: canonical event identity/lineage differs",
                           extra_reasons=(CANONICAL_LINEAGE_MISMATCH,))
        self.assertEqual(typed["reasons"], [CANONICAL_LINEAGE_MISMATCH])
        untyped = self._line(detail=GENERIC_DETAIL, extra_reasons=(untyped_failure_reason(KeyError("x")),))
        self.assertEqual(untyped["reasons"], ["UNTYPED_KEYERROR"])
        self.assertEqual(untyped["detail_sha256"],
                         "d1d2b3f607bf8700151af94d0c6d8af0590d7ed62683f65889887202585e5cef")

    def test_untyped_reason_is_a_bounded_token(self):
        class _Weird_name2(Exception):
            pass
        self.assertEqual(untyped_failure_reason(_Weird_name2()), "UNTYPED_WEIRDNAME2")
        self.assertLessEqual(len(untyped_failure_reason(type("X" * 200, (Exception,), {})())), 64)


if __name__ == "__main__":
    unittest.main()
