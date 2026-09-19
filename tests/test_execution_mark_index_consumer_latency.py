from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace

from qdl_sdk.reference import ReferenceProduct
from scripts.measure_execution_mark_index_consumer_latency import (
    execution_mark_index_requirements,
    validate_live_response,
)

class ExecutionMarkIndexConsumerLatencyTests(unittest.TestCase):
    manifest = Path(__file__).resolve().parents[1] / "consumers/stable/trading-system-paper.yaml"

    def test_manifest_selects_exact_execution_mark_index_scope(self) -> None:
        requirements = execution_mark_index_requirements(self.manifest)
        self.assertEqual(len(requirements), 10)
        self.assertTrue(all(
            item.product is ReferenceProduct.MARK_INDEX_PRICE
            and item.max_freshness_ms == 2_000
            for item in requirements
        ))

    def test_live_response_rejects_non_internal_lineage(self) -> None:
        requirements = execution_mark_index_requirements(self.manifest)
        now_ns = 9_000_000_000
        results = []
        for index, requirement in enumerate(requirements):
            endpoint = "qdl://stable-stream/internal/v2/execution/mark-index/latest"
            if index == 0:
                endpoint = "https://provider.example/mark"
            observation = SimpleNamespace(
                instrument_uid=requirement.instrument_uid,
                product=ReferenceProduct.MARK_INDEX_PRICE,
                fields=(SimpleNamespace(name="mark_price"), SimpleNamespace(name="index_price")),
                labels={
                    "execution_view": "STABLE_STREAM_GATEWAY",
                    "provider_confirmation_ns": str(now_ns - 1_000_000),
                    "delivery_stage": "CANONICAL_READ_COMMITTED",
                },
            )
            data = SimpleNamespace(
                status="OK",
                instrument_uid=requirement.instrument_uid,
                product=ReferenceProduct.MARK_INDEX_PRICE,
                received_at_ns=now_ns - 1_000_000,
                observations=(observation,),
                lineage=(SimpleNamespace(provider_endpoint=endpoint),),
            )
            results.append(SimpleNamespace(
                instrument_uid=requirement.instrument_uid,
                product=ReferenceProduct.MARK_INDEX_PRICE,
                status="OK",
                problem=None,
                data=data,
            ))
        with self.assertRaisesRegex(ValueError, "internal live reader"):
            validate_live_response(
                requirements,
                SimpleNamespace(partial=False, results=tuple(results)),
                usable_at_ns=now_ns,
            )


if __name__ == "__main__":
    unittest.main()
