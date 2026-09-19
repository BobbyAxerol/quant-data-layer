from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace

from qdl_sdk.reference import ReferenceProduct
from scripts.measure_execution_mark_index_consumer_latency import (
    _acceptance_gate,
    execution_mark_index_requirements,
    validate_live_response,
)


def _response(
    requirements,
    *,
    now_ns: int,
    endpoint: str = "qdl://stable-stream/internal/v2/execution/mark-index/latest",
    **overrides,
):
    labels = {
        "execution_view": "STABLE_STREAM_GATEWAY",
        "source_event_time_ns": str(now_ns - 61_000_000_000),
        "provider_confirmation_ns": str(now_ns - 60_000_000_000),
        "delivery_stage": "CANONICAL_READ_COMMITTED",
        "event_recency_policy": "OBSERVE",
        "recency_mode": "COMPONENT_SESSION_LIVE",
        "provider_session_state": "LIVE",
        "provider_session_liveness_ms": "10",
        "provider_session_checked_at_ns": str(now_ns - 5_000_000),
        "component_mark_received_at_ns": str(now_ns - 10_000_000_000),
        "component_index_received_at_ns": str(now_ns - 60_000_000_000),
        "component_mark_quiet_after_ms": "15000",
        "component_index_quiet_after_ms": "70000",
    }
    labels.update({key: str(value) for key, value in overrides.items()})
    results = []
    for requirement in requirements:
        observation = SimpleNamespace(
            instrument_uid=requirement.instrument_uid,
            product=ReferenceProduct.MARK_INDEX_PRICE,
                fields=(
                    SimpleNamespace(name="mark_price"),
                    SimpleNamespace(name="index_price"),
                ),
            labels=dict(labels),
        )
        data = SimpleNamespace(
            status="OK",
            instrument_uid=requirement.instrument_uid,
            product=ReferenceProduct.MARK_INDEX_PRICE,
            received_at_ns=int(labels["provider_confirmation_ns"]),
            observations=(observation,),
            lineage=(SimpleNamespace(provider_endpoint=endpoint),),
        )
        results.append(
            SimpleNamespace(
                instrument_uid=requirement.instrument_uid,
                product=ReferenceProduct.MARK_INDEX_PRICE,
                status="OK",
                problem=None,
                data=data,
            )
        )
    return SimpleNamespace(partial=False, results=tuple(results))


class ExecutionMarkIndexConsumerLatencyTests(unittest.TestCase):
    manifest = Path(__file__).resolve().parents[1] / "consumers/stable/trading-system-paper.yaml"

    def test_manifest_selects_exact_execution_mark_index_scope(self) -> None:
        requirements = execution_mark_index_requirements(self.manifest)
        self.assertEqual(len(requirements), 10)
        self.assertTrue(all(
            item.product is ReferenceProduct.MARK_INDEX_PRICE
            and item.max_freshness_ms == 2_000
            and item.event_recency_policy.value == "OBSERVE"
            and item.max_session_liveness_ms == 45_000
            for item in requirements
        ))

    def test_live_response_rejects_non_internal_lineage(self) -> None:
        requirements = execution_mark_index_requirements(self.manifest)
        now_ns = 90_000_000_000
        response = _response(requirements, now_ns=now_ns)
        response.results[0].data.lineage = (
            SimpleNamespace(provider_endpoint="https://provider.example/mark"),
        )
        with self.assertRaisesRegex(ValueError, "internal live reader"):
            validate_live_response(
                requirements,
                response,
                usable_at_ns=now_ns,
            )

    def test_quiet_live_response_keeps_original_provider_time_without_failing_latency_gate(
        self,
    ) -> None:
        requirements = execution_mark_index_requirements(self.manifest)
        now_ns = 90_000_000_000
        values = validate_live_response(
            requirements,
            _response(requirements, now_ns=now_ns),
            usable_at_ns=now_ns,
        )
        self.assertEqual(len(values), 10)
        for evidence in values.values():
            self.assertEqual(evidence["recency_mode"], "COMPONENT_SESSION_LIVE")
            self.assertEqual(evidence["provider_confirmation_to_usable_ms"], 60_000)
            self.assertLess(evidence["provider_session_liveness_to_usable_ms"], 45_000)
            self.assertLess(evidence["component_mark_age_to_usable_ms"], 15_000)
            self.assertLess(evidence["component_index_age_to_usable_ms"], 70_000)

    def test_strict_event_session_response_is_validated_without_changing_lineage(self) -> None:
        requirements = execution_mark_index_requirements(self.manifest)
        now_ns = 90_000_000_000
        values = validate_live_response(
            requirements,
            _response(
                requirements,
                now_ns=now_ns,
                provider_confirmation_ns=now_ns - 1_000_000,
                source_event_time_ns=now_ns - 2_000_000,
                component_mark_received_at_ns=now_ns - 1_000_000,
                component_index_received_at_ns=now_ns - 1_000_000,
                recency_mode="STRICT_EVENT_SESSION_LIVE",
            ),
            usable_at_ns=now_ns,
        )
        self.assertEqual(
            {item["recency_mode"] for item in values.values()},
            {"STRICT_EVENT_SESSION_LIVE"},
        )

    def test_quiet_response_rejects_session_or_component_failure(self) -> None:
        requirements = execution_mark_index_requirements(self.manifest)
        now_ns = 90_000_000_000
        with self.subTest("disconnected"):
            with self.assertRaisesRegex(ValueError, "not live"):
                validate_live_response(
                    requirements,
                    _response(
                        requirements,
                        now_ns=now_ns,
                        provider_session_state="DISCONNECTED",
                    ),
                    usable_at_ns=now_ns,
                )
        with self.subTest("expired_component"):
            with self.assertRaisesRegex(ValueError, "quiet cadence"):
                validate_live_response(
                    requirements,
                    _response(
                        requirements,
                        now_ns=now_ns,
                        component_index_received_at_ns=now_ns - 70_001_000_000,
                    ),
                    usable_at_ns=now_ns,
                )
        with self.subTest("expired_session"):
            with self.assertRaisesRegex(ValueError, "session exceeded"):
                validate_live_response(
                    requirements,
                    _response(
                        requirements,
                        now_ns=now_ns,
                        provider_session_checked_at_ns=now_ns - 45_001_000_000,
                    ),
                    usable_at_ns=now_ns,
                )

        with self.subTest("missing_session"):
            response = _response(requirements, now_ns=now_ns)
            for item in response.results:
                del item.data.observations[0].labels["provider_session_checked_at_ns"]
            with self.assertRaisesRegex(ValueError, "malformed"):
                validate_live_response(
                    requirements,
                    response,
                    usable_at_ns=now_ns,
                )

    def test_gate_uses_consumer_latency_not_old_immutable_provider_lineage(self) -> None:
        requirements = execution_mark_index_requirements(self.manifest)
        per_binding = {
            requirement.instrument_uid: {
                "provider_confirmation_to_usable_ms": {"n": 149, "p99_ms": 70_000},
                "provider_session_liveness_to_usable_ms": {"n": 149},
                "component_mark_age_to_usable_ms": {"n": 149},
                "component_index_age_to_usable_ms": {"n": 149},
            }
            for requirement in requirements
        }
        minimum_samples, passed = _acceptance_gate(
            {
                "errors": [],
                "per_binding": per_binding,
                "consumer_call_to_usable_ms": {"n": 149, "p99_ms": 281},
            },
            requirement_count=10,
            duration_seconds=300,
            cadence_seconds=2,
            max_consumer_call_p99_ms=2_000,
        )
        self.assertEqual(minimum_samples, 149)
        self.assertTrue(passed)


if __name__ == "__main__":
    unittest.main()
