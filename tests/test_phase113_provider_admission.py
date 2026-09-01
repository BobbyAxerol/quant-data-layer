"""Regression tests for the bounded Phase 11.3 provider-admission harness.

The admission itself is a read-only real-provider command.  These tests keep
its batching, time-boundary and typed-unavailable semantics deterministic so a
future source edit cannot silently skip a reference chunk during certification.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

from qdl.query import AccessPurpose, ConsumerGrade, DataRequirement, FeedType
from qdl.query.reference import ReferenceDataRequirement
from qdl.reference import LongShortKind, ReferenceProduct, ReferenceStatus


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/phase113_universal_warmup_reference_admission.py"
SPEC = importlib.util.spec_from_file_location("phase113_provider_admission", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
admission = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = admission
SPEC.loader.exec_module(admission)


class _ReferenceService:
    def __init__(self) -> None:
        self.calls = []

    async def reference_data_batch_async(self, batch, *, purpose):
        self.calls.append((batch, purpose))
        results = []
        for requirement in batch.requirements:
            if requirement.product is ReferenceProduct.LONG_SHORT_RATIO:
                results.append(SimpleNamespace(
                    status="PARTIAL",
                    problem=SimpleNamespace(
                        code=SimpleNamespace(value="UNSUPPORTED_FEED"),
                        detail="fixture provider declares this product unavailable",
                    ),
                    result=SimpleNamespace(status=ReferenceStatus.UNAVAILABLE),
                ))
                continue
            request = SimpleNamespace(
                instrument=SimpleNamespace(instrument_uid=requirement.instrument_uid),
                product=requirement.product,
                is_history=False,
            )
            results.append(SimpleNamespace(
                status="OK",
                problem=None,
                result=SimpleNamespace(
                    request=request,
                    status=ReferenceStatus.OK,
                    observations=(SimpleNamespace(
                        observed_at_ns=1_800_000_000_000_000_000,
                        fields=(),
                        labels=(),
                    ),),
                    coverage=SimpleNamespace(
                        complete_left=True,
                        complete_right=True,
                        truncated=False,
                        terminal_reason="TEST_FULL",
                    ),
                ),
            ))
        return SimpleNamespace(results=tuple(results))


class _FailingWarmupService:
    async def warmup_batch_async(self, batch, *, purpose):
        del batch, purpose
        return SimpleNamespace(
            partial=True,
            results=(SimpleNamespace(
                problem=SimpleNamespace(
                    code=SimpleNamespace(value="SOURCE_UNAVAILABLE"),
                    detail="fixture provider error",
                ),
            ),),
        )


class _FailingReferenceService:
    async def reference_data_batch_async(self, batch, *, purpose):
        del purpose
        return SimpleNamespace(
            results=tuple(
                SimpleNamespace(
                    status="PARTIAL",
                    problem=SimpleNamespace(
                        code=SimpleNamespace(value="DATA_STALE"),
                        detail="fixture reference observation is stale",
                    ),
                    result=None,
                )
                for _requirement in batch.requirements
            ),
        )


class _TypedPartialReferenceService:
    async def reference_data_batch_async(self, batch, *, purpose):
        del purpose
        results = []
        for requirement in batch.requirements:
            request = SimpleNamespace(
                instrument=SimpleNamespace(instrument_uid=requirement.instrument_uid),
                product=requirement.product,
                is_history=True,
            )
            result = SimpleNamespace(
                request=request,
                status=ReferenceStatus.OK,
                observations=(SimpleNamespace(
                    observed_at_ns=1_800_000_000_000_000_000,
                    fields=(),
                    labels=(),
                ),),
                coverage=SimpleNamespace(
                    complete_left=True,
                    complete_right=True,
                    truncated=True,
                    terminal_reason="INTERNAL_GAP",
                ),
            )
            results.append(SimpleNamespace(
                status="PARTIAL_RESULT",
                problem=SimpleNamespace(
                    code=SimpleNamespace(value="PARTIAL_RESULT"),
                    detail="fixture provider interval gap",
                ),
                result=result,
            ))
        return SimpleNamespace(results=tuple(results))


class Phase113ProviderAdmissionTests(unittest.IsolatedAsyncioTestCase):
    def test_trade_bar_only_handoff_has_no_declared_reference_requirement(self):
        plan = SimpleNamespace(
            inventory=object(),
            admission=SimpleNamespace(rows=()),
        )

        self.assertEqual(
            admission._demand_reference_work(
                plan,
                now_ms=1_800_000_000_000,
                deadline_ms=60_000,
            ),
            (),
        )

    async def test_reference_admission_executes_every_bounded_chunk(self):
        available = tuple(
            admission._ReferenceWork(ReferenceDataRequirement(
                instrument_uid=f"fixture-{index}",
                product=ReferenceProduct.CONTRACT_METADATA,
                consumer_grade=ConsumerGrade.ALPHA,
                source_policy_id="fixture-policy",
            ))
            for index in range(101)
        )
        blocked = admission._ReferenceWork(ReferenceDataRequirement(
            instrument_uid="fixture-blocked",
            product=ReferenceProduct.LONG_SHORT_RATIO,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="fixture-policy",
            start_time_ns=1_799_999_000_000_000_000,
            end_time_ns=1_800_000_000_000_000_000,
            interval="1h",
            long_short_kind=LongShortKind.GLOBAL_ACCOUNT,
        ), expected_blocked=True)
        service = _ReferenceService()

        evidence = await admission._admit_references(service, available + (blocked,))

        self.assertEqual(len(service.calls), 2)
        self.assertEqual([len(batch.requirements) for batch, _purpose in service.calls], [100, 2])
        self.assertEqual(
            [batch.consumer_id for batch, _purpose in service.calls],
            ["qdl.phase113.reference.batch.1", "qdl.phase113.reference.batch.2"],
        )
        self.assertTrue(all(purpose is AccessPurpose.INTERNAL_ALPHA for _batch, purpose in service.calls))
        self.assertTrue(service.calls[0][0].require_all)
        self.assertFalse(service.calls[1][0].require_all)
        self.assertEqual(len(evidence), 102)
        self.assertEqual(sum(item["expected"] == "BLOCKED" for item in evidence), 1)
        self.assertEqual(sum(item["expected"] == "AVAILABLE" for item in evidence), 101)

    async def test_execution_mark_snapshot_uses_execution_entitlement(self):
        work = admission._ReferenceWork(ReferenceDataRequirement(
            instrument_uid="fixture-execution-mark",
            product=ReferenceProduct.MARK_INDEX_PRICE,
            consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id="crypto_liquid_v2",
            limit=1,
            page_size=1,
            max_pages=1,
            max_freshness_ms=15_000,
        ))
        service = _ReferenceService()

        evidence = await admission._admit_references(service, (work,))

        self.assertEqual(len(evidence), 1)
        self.assertEqual(len(service.calls), 1)
        self.assertIs(service.calls[0][1], AccessPurpose.INTERNAL_EXECUTION)

    def test_closed_daily_and_funding_windows_never_include_open_period(self):
        day = 86_400_000
        funding = 8 * 3_600_000
        self.assertEqual(admission._last_closed_daily_open_ms(2 * day), day)
        self.assertEqual(admission._last_closed_daily_open_ms(2 * day + 1), day)
        grace = admission._FUNDING_SETTLEMENT_GRACE_MS
        self.assertEqual(admission._last_closed_funding_ms(4 * funding), 3 * funding)
        self.assertEqual(admission._last_closed_funding_ms(4 * funding + grace - 1), 3 * funding)
        self.assertEqual(admission._last_closed_funding_ms(4 * funding + grace), 4 * funding)

    async def test_warmup_failure_names_the_exact_binding_without_provider_payload(self):
        binding = SimpleNamespace(
            binding_id="fixture-binding",
            instrument_uid="fixture-uid",
            interval="1m",
        )
        work = admission._BarWork(
            binding=binding,
            requirement=DataRequirement(
                instrument_uid="fixture-uid",
                feed=FeedType.BAR,
                interval="1m",
                consumer_grade=ConsumerGrade.ALPHA,
                source_policy_id="fixture-policy",
            ),
        )
        with self.assertRaisesRegex(admission.Phase113AdmissionError, "fixture-binding") as raised:
            await admission._admit_bars(_FailingWarmupService(), (work,))
        self.assertIn("SOURCE_UNAVAILABLE", str(raised.exception))
        self.assertIn("fixture provider error", str(raised.exception))

    async def test_reference_failure_names_identity_product_and_typed_code(self):
        work = admission._ReferenceWork(ReferenceDataRequirement(
            instrument_uid="fixture-reference",
            product=ReferenceProduct.CONTRACT_METADATA,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="fixture-policy",
        ))

        with self.assertRaisesRegex(admission.Phase113AdmissionError, "fixture-reference") as raised:
            await admission._admit_references(_FailingReferenceService(), (work,))

        self.assertIn("CONTRACT_METADATA", str(raised.exception))
        self.assertIn("DATA_STALE", str(raised.exception))
        self.assertIn("fixture reference observation is stale", str(raised.exception))

    async def test_optional_representative_metric_records_typed_partial_not_success(self):
        work = admission._ReferenceWork(
            ReferenceDataRequirement(
                instrument_uid="fixture-partial",
                product=ReferenceProduct.OPEN_INTEREST,
                consumer_grade=ConsumerGrade.ALPHA,
                source_policy_id="fixture-policy",
                start_time_ns=1_799_900_000_000_000_000,
                end_time_ns=1_800_000_000_000_000_000,
                interval="1h",
            ),
            allow_typed_partial=True,
        )

        evidence = await admission._admit_references(_TypedPartialReferenceService(), (work,))

        self.assertEqual(evidence[0]["expected"], "PARTIAL_TYPED")
        self.assertEqual(evidence[0]["error_code"], "PARTIAL_RESULT")
        self.assertEqual(evidence[0]["coverage"], "INTERNAL_GAP")


if __name__ == "__main__":
    unittest.main()
