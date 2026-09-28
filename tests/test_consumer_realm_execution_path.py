import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from qdl_sdk import DataRequirement, Feed, Grade, StalePolicy
from scripts.accept_consumer_realms import execution_reference, read_execution_reference


class ExecutionPathTests(unittest.IsolatedAsyncioTestCase):
    def requirement(self, feed=Feed.MARK_INDEX_PRICE):
        return DataRequirement(instrument_uid="test-only", feed=feed,
            consumer_grade=Grade.EXECUTION, source_policy_id="test-policy",
            max_freshness_ms=2000, event_recency_policy=StalePolicy.OBSERVE,
            max_session_liveness_ms=2000)

    def test_ts_reference_contract_preserves_signed_limits(self):
        ref = execution_reference(self.requirement())
        self.assertEqual((ref.limit, ref.page_size, ref.max_pages), (1, 1, 1))
        self.assertTrue(ref.require_full_coverage)
        self.assertEqual(ref.max_freshness_ms, 2000)
        self.assertEqual(ref.max_session_liveness_ms, 2000)
        self.assertEqual(ref.event_recency_policy, StalePolicy.OBSERVE)
        self.assertEqual(ref.source_policy_id, "test-policy")

    def test_trade_is_not_silently_replaced(self):
        with self.assertRaises(ValueError):
            execution_reference(self.requirement(Feed.TRADE))

    async def test_item_refusal_is_preserved_not_accepted(self):
        payload = {"partial": True, "results": [{"instrument_uid": "test-only",
            "product": "MARK_INDEX_PRICE", "status": "ERROR",
            "problem": {"code": "DATA_STALE"}}]}
        response = SimpleNamespace(model_dump=lambda **kw: payload)
        client = SimpleNamespace(reference_batch=AsyncMock(return_value=response))
        evidence = await read_execution_reference(client, self.requirement())
        self.assertEqual(evidence, payload)
        self.assertIs(client.reference_batch.call_args.kwargs["require_all"], False)
        self.assertEqual(client.reference_batch.call_count, 1)


if __name__ == "__main__":
    unittest.main()
