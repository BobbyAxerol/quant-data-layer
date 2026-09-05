from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import time
from types import SimpleNamespace
import unittest

from qdl.demand import (
    CapabilityAvailability,
    DataRequirement as DemandRequirement,
    DemandFeed,
    DemandPurpose,
    DemandState,
    FeedCapability,
    ResolvedRequirement,
    UniverseSelector,
    UniverseSelectorKind,
    DemandResolver,
)
from qdl.adapters.intervals import okx_candle_channel
from qdl.query import (
    AccessPurpose,
    BarLifecycle,
    CanonicalErrorCode,
    ConsumerGrade,
    ContractMetadata,
    DataRequirement,
    FeedType,
    QualityMetadata,
    QueryProblem,
    QueryServiceError,
    RecoveryPolicy,
    SourceMetadata,
    V2QueryService,
)
from qdl.domain.calendar import trading_calendar_for_id
from qdl.query.results import MarketDataItem
from qdl.runtime.closed_bar_cache import ClosedBarWindowCache
from qdl.runtime.provider_history import (
    ProviderBarHistorySource,
    ProviderHistoryUnavailable,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.stream.grpc_service import requirement_from_proto
from qdl.warmup import (
    IntervalSourcePolicy,
    WarmupSpecification,
    WarmupTimeRange,
)
from qdl.warmup.executor import (
    BoundedWarmupExecutor,
    ProviderBudgetPolicy,
    RetryableWarmupError,
)
from qdl.warmup.handoff import ClosedBarFifo, resample_final_bars
from qdl.warmup.planner import UniversalWarmupPlanner
from qdl_sdk.handoff import ClosedBarHandoff
from qdl_sdk.client import AsyncDataLayerClient
from qdl_sdk.errors import DataLayerError
from qdl_sdk.models import (
    DataRequirement as SdkRequirement,
    Feed,
    Grade,
    MarketDataView,
    RecoveryPolicy as SdkRecoveryPolicy,
    WarmupResponse,
    WarmupSpecification as SdkWarmupSpecification,
)
from scripts.phase10_warmup_admission import (
    _bar_slices as load_warmup_bar_slices,
    _percentile as admission_percentile,
    run as run_warmup_admission,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
BINANCE_ETH = "ee93fabf-68df-5b50-8924-51bf25a5a757"
MINUTE_NS = 60_000_000_000
BASE_NS = 1_800_000_000_000_000_000


def _demand(interval: str, rows: int) -> ResolvedRequirement:
    requirement = DemandRequirement(
        consumer_id="alpha.phase10",
        purpose=DemandPurpose.ALPHA,
        universe=UniverseSelector(
            selector_id="phase10-symbol",
            kind=UniverseSelectorKind.EXPLICIT,
            venue="OKX",
            market="SWAP",
            product_type="PERPETUAL",
            native_symbols=("ETH-USDT-SWAP",),
        ),
        feed=DemandFeed.BAR,
        source_policy_id="crypto_primary_v2",
        interval=interval,
        warmup=WarmupSpecification.for_rows(rows),
        require_final_bars=True,
    )
    capability = FeedCapability(
        capability_id="OKX_DIRECT:OKX:SWAP:PERPETUAL:BAR",
        venue="OKX",
        market="SWAP",
        product_type="PERPETUAL",
        feed=DemandFeed.BAR,
        availability=CapabilityAvailability.AVAILABLE,
        rest_history=True,
        native_intervals=("1m", "3m", "5m", "15m", "30m", "1h"),
    )
    return ResolvedRequirement(
        requirement=requirement,
        instrument_uid="f" * 36,
        instrument_id="OKX:SWAP:PERPETUAL:ETH-USDT",
        native_symbol="ETH-USDT-SWAP",
        capability=capability,
        binding_id="okx-eth-bar",
        state=DemandState.REQUESTED,
        provisioned=True,
        catalog_revision=1,
        demand_revision=7,
    )


def _item(open_ns: int, *, interval: str = "1m", final: bool = True, revision: int = 0):
    step_ns = {
        "1m": MINUTE_NS,
        "3m": 3 * MINUTE_NS,
        "15m": 15 * MINUTE_NS,
    }[interval]
    lifecycle = BarLifecycle.FINAL if final else BarLifecycle.IN_PROGRESS
    value = str(open_ns // MINUTE_NS)
    return MarketDataItem(
        instrument_uid="uid",
        instrument_id="BINANCE:USDM:PERPETUAL:ETH-USDT",
        instrument_revision=1,
        feed=FeedType.BAR,
        interval=interval,
        observed_at_ns=open_ns + step_ns - 1,
        received_at_ns=open_ns + step_ns + 10,
        revision=revision,
        payload={
            "open_time_ns": open_ns,
            "close_time_ns": open_ns + step_ns - 1,
            "open": value,
            "high": str(int(value) + 2),
            "low": str(int(value) - 1),
            "close": str(int(value) + 1),
            "volume": "1.5",
            "volume_unit": "BASE_ASSET",
            "base_volume": "1.5",
            "quote_volume": "10",
            "contract_volume": None,
            "trade_count": 2,
            "origin": "VENUE_NATIVE",
            "is_final": final,
        },
        source=SourceMetadata("BINANCE", "BINANCE_DIRECT", "source", "PRIMARY", True),
        quality=QualityMetadata("LIVE", 0, False, True, True, "crypto_primary_v2"),
        contract=ContractMetadata(
            "a" * 64,
            "2.0.0",
            "normalizer",
            "adapter",
            1,
            1,
            1,
            1,
            f"bar:{open_ns}",
        ),
        watermark_offset=open_ns // MINUTE_NS,
        bar_lifecycle=lifecycle,
    )


class WarmupContractAndPlannerTests(unittest.TestCase):
    def test_rows_and_range_are_exclusive_and_range_is_bounded(self):
        with self.assertRaises(ValueError):
            WarmupSpecification()
        with self.assertRaises(ValueError):
            WarmupSpecification(
                rows=2,
                time_range=WarmupTimeRange(BASE_NS, BASE_NS + MINUTE_NS),
            )
        specification = WarmupSpecification(
            time_range=WarmupTimeRange(BASE_NS, BASE_NS + 3 * MINUTE_NS)
        )
        self.assertEqual(
            specification.resolved_window(
                interval_ns=MINUTE_NS,
                latest_closed_boundary_ns=BASE_NS + 4 * MINUTE_NS,
            ),
            (BASE_NS, BASE_NS + 3 * MINUTE_NS, 3),
        )

    def test_typed_query_proto_roundtrip_preserves_warmup(self):
        sdk = SdkRequirement(
            instrument_uid=BINANCE_ETH,
            feed=Feed.BAR,
            consumer_grade=Grade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="15m",
            recovery=SdkRecoveryPolicy.FRESH_SNAPSHOT,
            warmup=SdkWarmupSpecification(rows=700, deadline_ms=9_000),
        )
        restored = requirement_from_proto(sdk.to_proto())
        self.assertEqual(restored.warmup_specification.rows, 700)
        self.assertEqual(restored.warmup_specification.deadline_ms, 9_000)
        self.assertEqual(restored.interval, "15m")

    def test_legacy_demand_identity_does_not_gain_an_absent_warmup_field(self):
        value = _demand("1m", 3).requirement
        legacy = replace(value, warmup=None, warmup_limit=3)
        self.assertNotIn("warmup", legacy.canonical_mapping())
        self.assertEqual(
            legacy.requirement_id,
            DemandRequirement.from_mapping(legacy.canonical_mapping()).requirement_id,
        )

    def test_planner_prefers_native_and_chunks_to_provider_limit(self):
        plan = UniversalWarmupPlanner().compile(
            (_demand("1m", 601),),
            generated_at_ns=BASE_NS,
            demand_revision=7,
        )
        item = plan.items[0]
        self.assertEqual(item.source_interval, "1m")
        self.assertEqual(item.source_rows, 601)
        self.assertEqual([chunk.rows for chunk in item.chunks], [300, 300, 1])
        self.assertEqual(item.chunks[0].start_time_ns, item.start_time_ns)
        self.assertEqual(item.chunks[-1].end_time_ns, item.end_time_ns)

    def test_planner_uses_exact_largest_native_divisor(self):
        plan = UniversalWarmupPlanner().compile(
            (_demand("45m", 101),),
            generated_at_ns=BASE_NS,
            demand_revision=7,
        )
        item = plan.items[0]
        self.assertEqual(item.source_interval, "15m")
        self.assertEqual(item.resample_factor, 3)
        self.assertEqual(item.source_rows, 303)
        self.assertEqual([chunk.rows for chunk in item.chunks], [300, 3])

    def test_weekly_planner_uses_monday_closed_boundary(self):
        monday_ns = int(
            datetime(2026, 8, 24, tzinfo=timezone.utc).timestamp()
            * 1_000_000_000
        )
        weekly = _demand("1w", 2)
        weekly = replace(
            weekly,
            capability=replace(
                weekly.capability,
                native_intervals=weekly.capability.native_intervals + ("1w",),
            ),
        )
        item = UniversalWarmupPlanner().compile(
            (weekly,),
            generated_at_ns=monday_ns + 5_000_000_000,
            demand_revision=7,
        ).items[0]
        self.assertEqual(item.source_interval, "1w")
        self.assertEqual(item.end_time_ns, monday_ns)
        self.assertEqual(item.start_time_ns, monday_ns - 2 * 7 * 86_400_000_000_000)

    def test_three_day_planner_uses_each_provider_calendar_anchor(self):
        binance = _demand("3d", 1)
        binance = replace(
            binance,
            capability=replace(binance.capability, venue="BINANCE"),
        )
        binance_closed_ns = int(
            datetime(2026, 8, 24, tzinfo=timezone.utc).timestamp()
            * 1_000_000_000
        )
        okx_closed_ns = int(
            datetime(2026, 8, 26, tzinfo=timezone.utc).timestamp()
            * 1_000_000_000
        )
        self.assertEqual(
            UniversalWarmupPlanner().compile(
                (binance,), generated_at_ns=binance_closed_ns + 5_000_000_000,
                demand_revision=7,
            ).items[0].end_time_ns,
            binance_closed_ns,
        )
        self.assertEqual(
            UniversalWarmupPlanner().compile(
                (_demand("3d", 1),),
                generated_at_ns=okx_closed_ns + 5_000_000_000,
                demand_revision=7,
            ).items[0].end_time_ns,
            okx_closed_ns,
        )

    def test_native_only_rejects_a_non_native_interval(self):
        demand = _demand("45m", 2)
        strict = replace(
            demand,
            requirement=replace(
                demand.requirement,
                warmup=WarmupSpecification.for_rows(
                    2,
                    interval_source_policy=IntervalSourcePolicy.NATIVE_ONLY,
                ),
            ),
        )
        with self.assertRaisesRegex(ValueError, "not a provider-native"):
            UniversalWarmupPlanner().compile(
                (strict,), generated_at_ns=BASE_NS, demand_revision=7
            )

    def test_vn_calendar_rows_skip_lunch_and_chunk_by_session(self):
        generated_at_ns = int(
            datetime(2026, 8, 24, 8, 0, tzinfo=timezone.utc).timestamp()
            * 1_000_000_000
        )
        vn = replace(
            _demand("1m", 3),
            instrument_uid="vn-uid",
            instrument_id="HNX:VN_DERIVATIVES:FUTURE:VN30F1M",
            native_symbol="VN30F1M",
            capability=FeedCapability(
                capability_id="DNSE_DIRECT:HNX:VN_DERIVATIVES:FUTURE:BAR",
                venue="HNX",
                market="VN_DERIVATIVES",
                product_type="FUTURE",
                feed=DemandFeed.BAR,
                availability=CapabilityAvailability.AVAILABLE,
                rest_history=True,
                native_intervals=("1m",),
            ),
        )
        planner = UniversalWarmupPlanner(
            session_calendar_ids={"vn-uid": "VN_DERIVATIVES"}
        )
        plan = planner.compile(
            (vn,), generated_at_ns=generated_at_ns, demand_revision=7
        )
        self.assertEqual(plan.items[0].target_rows, 3)
        self.assertEqual([item.rows for item in plan.items[0].chunks], [2, 1])
        opens = tuple(item.start_time_ns for item in plan.items[0].chunks)
        self.assertGreater(opens[1] - opens[0], MINUTE_NS)

    def test_vn_calendar_exact_range_counts_only_trading_session_bars(self):
        calendar = trading_calendar_for_id("VN_DERIVATIVES")
        start_ns = int(
            datetime(2026, 8, 24, 2, 0, tzinfo=timezone.utc).timestamp()
            * 1_000_000_000
        )
        end_ns = int(
            datetime(2026, 8, 24, 7, 46, tzinfo=timezone.utc).timestamp()
            * 1_000_000_000
        )
        opens = calendar.bar_opens_between_ns(
            start_ns=start_ns,
            end_ns=end_ns,
            interval_ns=MINUTE_NS,
        )
        self.assertEqual(len(opens), 241)
        self.assertNotIn(
            int(
                datetime(2026, 8, 24, 5, 0, tzinfo=timezone.utc).timestamp()
                * 1_000_000_000
            ),
            opens,
        )

    def test_resolver_coalesces_compatible_consumers_to_largest_window(self):
        first = _demand("1m", 10)
        second = replace(
            _demand("1m", 50),
            requirement=replace(
                _demand("1m", 50).requirement,
                consumer_id="alpha.phase10.second",
            ),
            consumer_ids=("alpha.phase10.second",),
        )
        merged = DemandResolver._merge((first, second))
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0].requirement.warmup_specification.rows, 50)
        self.assertEqual(
            merged[0].consumer_ids,
            ("alpha.phase10", "alpha.phase10.second"),
        )

    def test_resolver_rejects_ambiguous_row_and_range_coalescing(self):
        ranged = replace(
            _demand("1m", 10),
            requirement=replace(
                _demand("1m", 10).requirement,
                consumer_id="alpha.range",
                warmup_limit=0,
                warmup=WarmupSpecification(
                    time_range=WarmupTimeRange(BASE_NS, BASE_NS + MINUTE_NS)
                ),
            ),
            consumer_ids=("alpha.range",),
        )
        with self.assertRaisesRegex(ValueError, "cannot merge row and time-range"):
            DemandResolver._merge((_demand("1m", 10), ranged))


class WarmupExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_cache_has_bounded_concurrency_without_provider_token_wait(self):
        sleeps = []
        running = 0
        peak = 0

        async def sleep(delay):
            sleeps.append(delay)

        async def work(value):
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0)
            running -= 1
            return value

        executor = BoundedWarmupExecutor(sleep=sleep)
        result = await executor.execute(
            range(100), work=work, identity=lambda value: value,
            provider=lambda _: "LOCAL_CANONICAL_CACHE", deadline_ms=lambda _: 2_000,
        )
        self.assertEqual([item.value for item in result], list(range(100)))
        self.assertTrue(all(item.ok and item.attempts == 1 for item in result))
        self.assertLessEqual(peak, 8)
        self.assertEqual(sleeps, [])
        self.assertEqual(executor.provider_policies["OKX"].requests_per_second, 5.0)
        self.assertEqual(executor.provider_policies["BINANCE"].requests_per_second, 8.0)

    async def test_identical_concurrent_work_is_singleflight(self):
        executor = BoundedWarmupExecutor[int, int]()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def work(value):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return value * 2

        arguments = dict(
            work=work,
            identity=lambda value: value,
            provider=lambda _: "BINANCE",
            deadline_ms=lambda _: 1_000,
        )
        first = asyncio.create_task(executor.execute((7,), **arguments))
        await started.wait()
        second = asyncio.create_task(executor.execute((7,), **arguments))
        await asyncio.sleep(0)
        release.set()
        one, two = await asyncio.gather(first, second)
        self.assertEqual(calls, 1)
        self.assertEqual(one[0].value, 14)
        self.assertEqual(two[0].value, 14)
        self.assertEqual(sum((one[0].shared, two[0].shared)), 1)

    async def test_retry_after_and_provider_token_budget_are_honored(self):
        now = [0.0]
        sleeps = []

        async def sleep(delay):
            sleeps.append(delay)
            now[0] += delay

        executor = BoundedWarmupExecutor[int, int](
            default_policy=ProviderBudgetPolicy(
                max_concurrency=1,
                requests_per_second=2,
                burst_requests=1,
                max_attempts=2,
            ),
            sleep=sleep,
            clock=lambda: now[0],
            random_value=lambda: 0.0,
        )
        attempts = 0

        async def work(value):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RetryableWarmupError("429", retry_after_ms=750)
            return value

        result = await executor.execute(
            (1,),
            work=work,
            identity=lambda value: value,
            provider=lambda _: "BINANCE",
            deadline_ms=lambda _: 5_000,
        )
        self.assertTrue(result[0].ok)
        self.assertEqual(result[0].attempts, 2)
        self.assertGreaterEqual(max(sleeps), 0.75)
        self.assertEqual(executor.stats()["retry_count"], 1)

    async def test_retry_after_outside_item_deadline_returns_typed_without_sleep(self):
        now = [0.0]
        sleeps = []

        async def sleep(delay):
            sleeps.append(delay)
            now[0] += delay

        executor = BoundedWarmupExecutor[int, int](
            default_policy=ProviderBudgetPolicy(
                max_concurrency=1,
                requests_per_second=100.0,
                burst_requests=1,
                max_attempts=4,
            ),
            sleep=sleep,
            clock=lambda: now[0],
            random_value=lambda: 0.0,
        )
        calls = 0

        async def deferred(_):
            nonlocal calls
            calls += 1
            raise RetryableWarmupError("provider defer", retry_after_ms=60_000)

        result = await executor.execute(
            (1,),
            work=deferred,
            identity=lambda value: value,
            provider=lambda _: "BINANCE",
            deadline_ms=lambda _: 5_000,
        )

        self.assertFalse(result[0].ok)
        self.assertIsInstance(result[0].error, RetryableWarmupError)
        self.assertEqual(result[0].error.retry_after_ms, 60_000)
        self.assertEqual(calls, 1)
        self.assertEqual(sleeps, [])
        self.assertEqual(executor.stats()["retry_count"], 0)

    async def test_circuit_opens_after_consecutive_failures(self):
        executor = BoundedWarmupExecutor[int, int](
            default_policy=ProviderBudgetPolicy(
                max_attempts=1,
                circuit_failures=2,
                circuit_cooldown_ms=10_000,
            )
        )

        async def fail(_):
            raise RetryableWarmupError("provider unavailable")

        arguments = dict(
            work=fail,
            identity=lambda value: value,
            provider=lambda _: "OKX",
            deadline_ms=lambda _: 1_000,
        )
        await executor.execute((1,), **arguments)
        await executor.execute((2,), **arguments)
        result = await executor.execute((3,), **arguments)
        self.assertFalse(result[0].ok)
        self.assertIn("circuit is open", str(result[0].error))
        self.assertEqual(executor.source_calls, 2)

    async def test_deadline_cancels_unshared_underlying_work(self):
        executor = BoundedWarmupExecutor[int, int]()
        cancelled = asyncio.Event()

        async def wait_forever(_):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        result = await executor.execute(
            (1,),
            work=wait_forever,
            identity=lambda value: value,
            provider=lambda _: "BINANCE",
            deadline_ms=lambda _: 100,
        )
        self.assertFalse(result[0].ok)
        self.assertIn("deadline exceeded", str(result[0].error))
        await asyncio.wait_for(cancelled.wait(), timeout=1)

    async def test_provider_fairness_keeps_one_stalled_venue_from_blocking_another(self):
        executor = BoundedWarmupExecutor[tuple[str, int], int]()
        release_binance = asyncio.Event()
        okx_finished = asyncio.Event()

        async def work(item):
            if item[0] == "BINANCE":
                await release_binance.wait()
            else:
                okx_finished.set()
            return item[1]

        task = asyncio.create_task(executor.execute(
            (("BINANCE", 1), ("OKX", 2)),
            work=work,
            identity=lambda item: item[1],
            provider=lambda item: item[0],
            deadline_ms=lambda _: 1_000,
        ))
        await asyncio.wait_for(okx_finished.wait(), timeout=0.5)
        release_binance.set()
        result = await task
        self.assertEqual([item.value for item in result], [1, 2])

    async def test_provider_concurrency_never_exceeds_declared_budget(self):
        active = 0
        peak = 0
        release = asyncio.Event()
        started = asyncio.Event()
        executor = BoundedWarmupExecutor[int, int](
            default_policy=ProviderBudgetPolicy(
                max_concurrency=2,
                requests_per_second=100,
                burst_requests=100,
            )
        )

        async def work(value):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if peak == 2:
                started.set()
            await release.wait()
            active -= 1
            return value

        task = asyncio.create_task(executor.execute(
            tuple(range(6)),
            work=work,
            identity=lambda value: value,
            provider=lambda _: "BINANCE",
            deadline_ms=lambda _: 1_000,
        ))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        self.assertEqual(peak, 2)
        release.set()
        result = await task
        self.assertTrue(all(item.ok for item in result))
        self.assertEqual(peak, 2)

    async def test_caller_cancellation_propagates_to_unshared_work(self):
        executor = BoundedWarmupExecutor[int, int]()
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def work(_):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        task = asyncio.create_task(executor.execute(
            (1,),
            work=work,
            identity=lambda value: value,
            provider=lambda _: "BINANCE",
            deadline_ms=lambda _: 10_000,
        ))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(cancelled.wait(), timeout=1)


class ResampleAndHandoffTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_final_resample_has_full_constituent_lineage(self):
        rows = tuple(_item(BASE_NS + index * MINUTE_NS) for index in range(6))
        result = resample_final_bars(rows, target_interval="3m")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].payload["open"], rows[0].payload["open"])
        self.assertEqual(result[0].payload["close"], rows[2].payload["close"])
        self.assertEqual(result[0].payload["volume"], "4.5")
        self.assertEqual(result[0].resample_lineage.constituent_count, 3)
        self.assertEqual(len(result[0].resample_lineage.constituent_sha256), 64)
        self.assertTrue(result[0].contract.correlation_id.startswith("resample:"))

    def test_gap_or_provisional_constituent_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "gap"):
            resample_final_bars(
                (_item(BASE_NS), _item(BASE_NS + 2 * MINUTE_NS)),
                target_interval="3m",
            )

    def test_declared_session_break_does_not_hide_an_in_session_gap(self):
        morning = tuple(_item(BASE_NS + index * MINUTE_NS) for index in range(3))
        afternoon_start = BASE_NS + 60 * MINUTE_NS
        afternoon = tuple(
            _item(afternoon_start + index * MINUTE_NS) for index in range(3)
        )
        result = resample_final_bars(
            morning + afternoon,
            target_interval="3m",
            allowed_discontinuity=lambda previous, current: (
                previous == BASE_NS + 2 * MINUTE_NS
                and current == afternoon_start
            ),
        )
        self.assertEqual(len(result), 2)
        with self.assertRaisesRegex(ValueError, "gap"):
            resample_final_bars(
                morning + (_item(BASE_NS + 4 * MINUTE_NS),),
                target_interval="3m",
                allowed_discontinuity=lambda *_: False,
            )
        with self.assertRaisesRegex(ValueError, "final"):
            resample_final_bars(
                (
                    _item(BASE_NS),
                    _item(BASE_NS + MINUTE_NS, final=False),
                    _item(BASE_NS + 2 * MINUTE_NS),
                ),
                target_interval="3m",
            )

    def test_fifo_is_bounded_and_releases_without_one_bar_delay(self):
        fifo = ClosedBarFifo(interval="1m", maxlen=3)
        fifo.seed(_item(BASE_NS + index * MINUTE_NS) for index in range(3))
        releases = []
        appended = fifo.append_closed(
            _item(BASE_NS + 3 * MINUTE_NS),
            release=lambda rows: releases.append(rows),
        )
        self.assertTrue(appended)
        self.assertEqual(len(releases), 1)
        self.assertEqual(len(fifo.bars), 3)
        self.assertEqual(
            fifo.bars[-1].payload["open_time_ns"], BASE_NS + 3 * MINUTE_NS
        )
        self.assertFalse(
            fifo.append_closed(
                _item(BASE_NS + 3 * MINUTE_NS),
                release=lambda rows: releases.append(rows),
            )
        )
        self.assertEqual(len(releases), 1)

    async def test_sdk_handoff_runs_async_callback_once(self):
        seed = [_sdk_bar(BASE_NS + index * MINUTE_NS) for index in range(2)]
        response = WarmupResponse.model_validate({
            "request_id": "request",
            "snapshot_id": "snapshot",
            "data_as_of_ns": seed[-1].payload.close_time_ns,
            "stream_cursor": "cursor",
            "watermark_offset": 2,
            "coverage": "FULL",
            "count": 2,
            "data": [item.model_dump(mode="json") for item in seed],
        })
        handoff = ClosedBarHandoff.from_warmup(
            response, interval="1m", maxlen=2
        )
        released = []

        async def callback(rows):
            released.append(rows[-1].payload.open_time_ns)

        self.assertTrue(
            await handoff.append_closed(
                _sdk_bar(BASE_NS + 2 * MINUTE_NS), release=callback
            )
        )
        self.assertFalse(
            await handoff.append_closed(
                _sdk_bar(BASE_NS + 2 * MINUTE_NS), release=callback
            )
        )
        self.assertEqual(released, [BASE_NS + 2 * MINUTE_NS])

    async def test_sdk_typed_warmup_enters_stream_handoff_not_snapshot(self):
        rows = [_sdk_bar(BASE_NS), _sdk_bar(BASE_NS + MINUTE_NS)]
        transport = _SdkQueryTransport(rows)
        client = AsyncDataLayerClient(
            query_transport=transport,
            stream_transport=_SdkStreamTransport(),
            consumer_id="alpha.phase10",
        )
        requirement = SdkRequirement(
            instrument_uid="uid",
            feed=Feed.BAR,
            consumer_grade=Grade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="1m",
            recovery=SdkRecoveryPolicy.SNAPSHOT_AND_REPLAY,
            warmup=SdkWarmupSpecification(rows=2),
        )
        async with client.warmup_then_stream(requirement) as session:
            self.assertEqual(len(session.warmup.data), 2)
        self.assertEqual(transport.warmup_calls, 1)
        self.assertEqual(transport.snapshot_calls, 0)


class CacheAgeTests(unittest.TestCase):
    def test_cache_age_and_source_semantics_are_part_of_reuse(self):
        now = [0.0]
        cache = ClosedBarWindowCache(max_entries=2, clock=lambda: now[0])
        cache.put(
            "uid", "15m", 100, ("a",),
            source_policy_id="policy-a", source_interval="5m",
        )
        self.assertIsNone(cache.get(
            "uid", "15m", 100, 1,
            source_policy_id="policy-b", source_interval="5m",
        ))
        self.assertEqual(cache.get(
            "uid", "15m", 100, 1,
            source_policy_id="policy-a", source_interval="5m", max_age_ms=10,
        ), ("a",))
        now[0] = 0.011
        self.assertIsNone(cache.get(
            "uid", "15m", 100, 1,
            source_policy_id="policy-a", source_interval="5m", max_age_ms=10,
        ))


class ProviderHistoryPhase10Tests(unittest.TestCase):
    def setUp(self):
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.clock_ns = BASE_NS
        self.calls = []

    def _fetcher(self, binding, *, limit, now_ms, **_):
        self.calls.append((binding.interval, limit, now_ms))
        step_ms = _fixed_interval_ms(binding.interval)
        last_open = ((now_ms - 1) // step_ms) * step_ms
        rows = []
        for index in range(limit):
            open_ms = last_open - (limit - index - 1) * step_ms
            frame = {
                "symbol": binding.native_symbol,
                "interval": binding.interval,
                "bar_origin": "BACKFILLED",
                "row": [
                    open_ms, "100", "102", "99", "101", "10",
                    open_ms + step_ms - 1, "1010", 5, "4", "404", "0",
                ],
            }
            rows.append(_RawEnvelope(frame))
        return tuple(rows)

    def _source(self, fetcher=None):
        return ProviderBarHistorySource(
            self.catalog,
            binance_fetcher=fetcher or self._fetcher,
            clock_ns=lambda: self.clock_ns,
        )

    def _requirement(self, interval, rows):
        return DataRequirement(
            instrument_uid=BINANCE_ETH,
            feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval=interval,
            recovery=RecoveryPolicy.FRESH_SNAPSHOT,
            warmup=WarmupSpecification.for_rows(rows),
        )

    def test_large_native_window_is_chunked_without_overlap(self):
        result = self._source().history_result(
            self._requirement("1m", 1001), schema_digest="a" * 64
        )
        self.assertEqual(len(result.items), 1001)
        self.assertEqual([call[1] for call in self.calls], [1000, 1])
        self.assertEqual(result.coverage.value, "FULL")

    def test_every_physical_page_consumes_provider_token_budget(self):
        now = [0.0]
        sleeps = []

        def sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds

        source = ProviderBarHistorySource(
            self.catalog,
            binance_fetcher=self._fetcher,
            clock_ns=lambda: self.clock_ns,
            provider_policies={
                "BINANCE": ProviderBudgetPolicy(
                    max_concurrency=1,
                    requests_per_second=2.0,
                    burst_requests=1,
                )
            },
            monotonic=lambda: now[0],
            sleep=sleep,
        )
        result = source.history_result(
            self._requirement("1m", 1001), schema_digest="a" * 64
        )
        self.assertEqual(len(result.items), 1001)
        self.assertEqual(sleeps, [0.5])
        self.assertEqual(source.stats()["provider_source_calls"], 2)
        self.assertEqual(source.stats()["provider_budget_waits"], 1)
        self.assertEqual(source.stats()["provider_budget_wait_ms"], 500)

    def test_page_budget_exhaustion_fails_closed_at_request_deadline(self):
        now = [0.0]
        sleeps = []
        source = ProviderBarHistorySource(
            self.catalog,
            binance_fetcher=self._fetcher,
            clock_ns=lambda: self.clock_ns,
            provider_policies={
                "BINANCE": ProviderBudgetPolicy(
                    max_concurrency=1,
                    requests_per_second=1.0,
                    burst_requests=1,
                )
            },
            monotonic=lambda: now[0],
            sleep=lambda seconds: sleeps.append(seconds),
        )
        requirement = replace(
            self._requirement("1m", 1001),
            warmup=WarmupSpecification.for_rows(1001, deadline_ms=100),
        )
        with self.assertRaises(ProviderHistoryUnavailable) as failure:
            source.history(requirement)
        self.assertEqual(
            failure.exception.problem.code.value,
            "RATE_LIMITED",
        )
        self.assertTrue(failure.exception.problem.retryable)
        self.assertEqual(failure.exception.problem.retry_after_ms, 1000)
        self.assertEqual(sleeps, [])
        self.assertEqual(source.stats()["provider_source_calls"], 1)

    def test_deadline_before_singleflight_lock_leaves_no_window_key(self):
        ticks = iter((0.0, 0.0, 0.0, 1.0))
        source = ProviderBarHistorySource(
            self.catalog,
            binance_fetcher=self._fetcher,
            clock_ns=lambda: self.clock_ns,
            monotonic=lambda: next(ticks, 1.0),
        )
        requirement = replace(
            self._requirement("1m", 1),
            warmup=WarmupSpecification.for_rows(1, deadline_ms=100),
        )
        with self.assertRaises(ProviderHistoryUnavailable) as failure:
            source.history(requirement)
        self.assertEqual(
            failure.exception.problem.code.value,
            "SOURCE_UNAVAILABLE",
        )
        self.assertEqual(source._inflight, {})
        self.assertEqual(self.calls, [])

    def test_non_native_interval_is_exactly_resampled(self):
        result = self._source().history_result(
            self._requirement("45m", 2), schema_digest="a" * 64
        )
        self.assertEqual(len(result.items), 2)
        self.assertEqual(self.calls[0][:2], ("15m", 6))
        self.assertTrue(all(item.interval == "45m" for item in result.items))
        self.assertTrue(all(item.resample_lineage for item in result.items))
        self.assertIn("EXACT_FINAL_RESAMPLE", result.items[-1].quality.flags)

    def test_explicit_time_range_is_exact_and_closed(self):
        start_ns = BASE_NS - 3 * MINUTE_NS
        requirement = replace(
            self._requirement("1m", 3),
            warmup=WarmupSpecification(
                time_range=WarmupTimeRange(start_ns, BASE_NS)
            ),
        )
        result = self._source().history_result(
            requirement, schema_digest="a" * 64
        )
        self.assertEqual(result.items[0].payload["open_time_ns"], start_ns)
        self.assertEqual(
            result.items[-1].payload["open_time_ns"] + MINUTE_NS,
            BASE_NS,
        )

    def test_partial_provider_page_is_never_reported_ok(self):
        def short(binding, *, limit, now_ms, **rest):
            return self._fetcher(
                binding, limit=max(0, limit - 1), now_ms=now_ms, **rest
            )

        with self.assertRaises(ProviderHistoryUnavailable):
            self._source(short).history(self._requirement("1m", 3))

    def test_provider_bar_at_or_after_observation_cutoff_is_rejected(self):
        def unfinished(binding, *, limit, now_ms, **_):
            step_ms = _fixed_interval_ms(binding.interval)
            open_ms = now_ms - step_ms
            frame = {
                "symbol": binding.native_symbol,
                "interval": binding.interval,
                "bar_origin": "BACKFILLED",
                "row": [
                    open_ms, "100", "102", "99", "101", "10",
                    now_ms + step_ms - 1, "1010", 5, "4", "404", "0",
                ],
            }
            return tuple(_RawEnvelope(frame) for _ in range(limit))

        with self.assertRaisesRegex(ProviderHistoryUnavailable, "not closed"):
            self._source(unfinished).history(self._requirement("1m", 1))

    def test_weekly_native_window_uses_monday_close_without_one_bar_delay(self):
        monday_boundary_ns = int(
            datetime(2026, 8, 24, tzinfo=timezone.utc).timestamp()
        ) * 1_000_000_000
        monday_ns = monday_boundary_ns + 1_000_000
        monday_ms = monday_ns // 1_000_000
        monday_boundary_ms = monday_boundary_ns // 1_000_000
        week_ms = 7 * 86_400_000
        observed = []

        def weekly(binding, *, limit, now_ms, **_):
            observed.append(now_ms)
            rows = []
            for index in range(limit):
                open_ms = monday_boundary_ms - (limit - index) * week_ms
                rows.append(_RawEnvelope({
                    "symbol": binding.native_symbol,
                    "interval": "1w",
                    "bar_origin": "BACKFILLED",
                    "row": [
                        open_ms, "100", "102", "99", "101", "10",
                        open_ms + week_ms - 1, "1010", 5, "4", "404", "0",
                    ],
                }))
            return tuple(rows)

        self.clock_ns = monday_ns
        source = self._source(weekly)
        first = source.history_result(
            self._requirement("1w", 2), schema_digest="a" * 64
        )
        second = source.history_result(
            self._requirement("1w", 2), schema_digest="a" * 64
        )
        self.assertEqual(observed, [monday_ms])
        self.assertEqual(
            first.items[-1].payload["open_time_ns"],
            monday_boundary_ns - week_ms * 1_000_000,
        )
        self.assertEqual(second.items, first.items)
        self.assertEqual(source.stats()["cache_hits"], 1)

    def test_large_window_warms_cache_and_smaller_overlap_uses_no_source_call(self):
        source = self._source()
        source.history_result(self._requirement("1m", 10), schema_digest="a" * 64)
        first = source.stats()
        result = source.history_result(
            self._requirement("1m", 3), schema_digest="a" * 64
        )
        second = source.stats()
        self.assertEqual(len(result.items), 3)
        self.assertEqual(second["provider_source_calls"], first["provider_source_calls"])
        self.assertGreater(second["cache_hits"], first["cache_hits"])

    def test_provider_failure_metrics_classify_429_and_5xx(self):
        class Response:
            def __init__(self, status_code):
                self.status_code = status_code

        class ProviderFailure(RuntimeError):
            def __init__(self, status_code):
                super().__init__(f"HTTP {status_code}")
                self.response = Response(status_code)

        for status, metric in ((429, "provider_429"), (503, "provider_5xx")):
            with self.subTest(status=status):
                source = self._source(lambda *_, **__: (_ for _ in ()).throw(
                    ProviderFailure(status)
                ))
                with self.assertRaises(ProviderHistoryUnavailable):
                    source.history(self._requirement("1m", 1))
                self.assertEqual(source.stats()[metric], 1)


class SdkBatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_wide_universe_is_chunked_and_preserves_cardinality(self):
        requirements = tuple(
            SdkRequirement(
                instrument_uid=f"uid-{index}",
                feed=Feed.BAR,
                consumer_grade=Grade.ALPHA,
                source_policy_id="crypto_primary_v2",
                interval="1m",
                recovery=SdkRecoveryPolicy.FRESH_SNAPSHOT,
                warmup=SdkWarmupSpecification(rows=1),
            )
            for index in range(205)
        )
        transport = _SdkBatchTransport()
        client = AsyncDataLayerClient(
            query_transport=transport,
            stream_transport=_SdkStreamTransport(),
            consumer_id="alpha.wide-universe",
        )
        response = await client.warmup_batch(requirements)
        self.assertEqual(transport.chunk_sizes, [100, 100, 5])
        self.assertEqual(len(response.results), 205)
        self.assertEqual(response.success_count, 205)

    async def test_partial_item_is_explicit_and_require_all_fails_closed(self):
        requirements = tuple(
            SdkRequirement(
                instrument_uid=f"uid-{index}",
                feed=Feed.BAR,
                consumer_grade=Grade.ALPHA,
                source_policy_id="crypto_primary_v2",
                interval="1m",
                recovery=SdkRecoveryPolicy.FRESH_SNAPSHOT,
                warmup=SdkWarmupSpecification(rows=1),
            )
            for index in range(2)
        )
        transport = _SdkBatchTransport(fail_uids={"uid-1"})
        client = AsyncDataLayerClient(
            query_transport=transport,
            stream_transport=_SdkStreamTransport(),
            consumer_id="alpha.partial",
        )
        response = await client.warmup_batch(requirements, require_all=False)
        self.assertTrue(response.partial)
        self.assertEqual(response.error_count, 1)
        with self.assertRaises(DataLayerError) as partial:
            await client.warmup_batch(requirements, require_all=True)
        self.assertEqual(partial.exception.code, "PARTIAL_RESULT")


class SingleWarmupExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_query_service_uses_local_lane_only_on_backend_opt_in(self):
        class Service(V2QueryService):
            def __init__(self, local):
                self.instruments = SimpleNamespace(get=lambda _: SimpleNamespace(
                    identity=SimpleNamespace(venue="OKX")))
                self.backend = SimpleNamespace(warmup_is_local=lambda _: local)
                self.warmup_executor = BoundedWarmupExecutor()
                self.last_batch_evidence = {}

            def warmup(self, requirement, *, purpose, request_id=None):
                return "warmup-ok"

        requirement = DataRequirement(
            instrument_uid=BINANCE_ETH, feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA, source_policy_id="crypto_primary_v2",
            interval="1m", warmup=WarmupSpecification.for_rows(1),
        )
        for local, expected in ((True, "LOCAL_CANONICAL_CACHE"), (False, "OKX")):
            service = Service(local)
            self.assertEqual(await service.warmup_async(
                requirement, purpose=AccessPurpose.INTERNAL_ALPHA), "warmup-ok")
            self.assertEqual(set(service.warmup_executor._semaphores), {expected})
            self.assertEqual(set(service.warmup_executor._tokens), set() if local else {"OKX"})

    async def test_single_warmup_is_nonblocking_and_reuses_retry_policy(self):
        class Service(V2QueryService):
            def __init__(self):
                self.calls = 0
                self.instruments = SimpleNamespace(
                    get=lambda _: SimpleNamespace(
                        identity=SimpleNamespace(venue="BINANCE")
                    )
                )
                self.backend = SimpleNamespace()
                self.warmup_executor = BoundedWarmupExecutor(
                    provider_policies={
                        "BINANCE": ProviderBudgetPolicy(
                            max_concurrency=1,
                            requests_per_second=100.0,
                            burst_requests=2,
                            max_attempts=2,
                        )
                    },
                    sleep=lambda _: asyncio.sleep(0),
                    random_value=lambda: 0.0,
                )
                self.last_batch_evidence = {}

            def warmup(self, requirement, *, purpose, request_id=None):
                del requirement, purpose, request_id
                self.calls += 1
                time.sleep(0.05)
                if self.calls == 1:
                    raise QueryServiceError(
                        QueryProblem(
                            CanonicalErrorCode.SOURCE_UNAVAILABLE,
                            "injected retryable provider failure",
                            True,
                        ),
                        request_id="single",
                    )
                return "warmup-ok"

        service = Service()
        requirement = DataRequirement(
            instrument_uid=BINANCE_ETH,
            feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="1m",
            recovery=RecoveryPolicy.FRESH_SNAPSHOT,
            warmup=WarmupSpecification.for_rows(1, deadline_ms=1_000),
        )
        pulse = asyncio.create_task(asyncio.sleep(0.01))
        pending = asyncio.create_task(service.warmup_async(
            requirement,
            purpose=AccessPurpose.INTERNAL_ALPHA,
        ))
        await asyncio.wait_for(pulse, timeout=0.04)
        self.assertFalse(pending.done())
        self.assertEqual(await pending, "warmup-ok")
        self.assertEqual(service.calls, 2)
        self.assertEqual(service.warmup_executor.retry_count, 1)


class WarmupAdmissionTests(unittest.TestCase):
    def test_small_sample_p95_uses_nearest_rank_not_a_lower_order_statistic(self):
        self.assertEqual(admission_percentile([1, 2, 3, 4, 5, 6], 0.95), 6)

    def test_read_only_admission_covers_every_demanded_bar_without_raw_payload(self):
        demand_path = ROOT / "config/v2/stable-crypto-demand.yaml"
        bar_slices = load_warmup_bar_slices(demand_path)
        self.assertGreater(len(bar_slices), 0)

        def rows(binding, limit, now_ms, *, provider):
            step_ms = _fixed_interval_ms(binding.interval)
            boundary_ms = now_ms // step_ms * step_ms
            values = []
            for index in range(limit):
                open_ms = boundary_ms - (limit - index) * step_ms
                if provider == "BINANCE_DIRECT":
                    frame = {
                        "symbol": binding.native_symbol,
                        "interval": binding.interval,
                        "bar_origin": "BACKFILLED",
                        "row": [
                            open_ms, "100", "102", "99", "101", "10",
                            open_ms + step_ms - 1, "1010", 5, "4", "404", "0",
                        ],
                    }
                else:
                    frame = {
                        "arg": {
                            "channel": okx_candle_channel(binding.interval),
                            "instId": binding.native_symbol,
                        },
                        "data": [[
                            str(open_ms), "100", "102", "99", "101", "10",
                            "4", "1010", "1",
                        ]],
                    }
                values.append(_RawEnvelope(frame, provider=provider))
            return tuple(values)

        def binance(binding, *, limit, now_ms, **_):
            return rows(
                binding, limit, now_ms, provider="BINANCE_DIRECT"
            )

        async def okx(binding, *, limit, now_ms, **_):
            return rows(binding, limit, now_ms, provider="OKX_DIRECT")

        report = run_warmup_admission(
            demand_path,
            CATALOG_PATH,
            rows=3,
            timeout_seconds=10,
            clock_ns=lambda: BASE_NS,
            binance_fetcher=binance,
            okx_fetcher=okx,
            executor=BoundedWarmupExecutor(
                default_policy=ProviderBudgetPolicy(
                    max_concurrency=64,
                    requests_per_second=10_000,
                    burst_requests=10_000,
                )
            ),
            source_provider_policies={
                venue: ProviderBudgetPolicy(
                    max_concurrency=64,
                    requests_per_second=10_000,
                    burst_requests=10_000,
                )
                for venue in ("BINANCE", "OKX")
            },
        )
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["slice_count"], len(bar_slices))
        self.assertEqual(
            report["source"]["provider_source_calls"], len(bar_slices)
        )
        self.assertEqual(report["source"]["cache_hits"], len(bar_slices))
        self.assertEqual(report["production_writes"], 0)
        self.assertFalse(report["raw_payload_persisted"])
        self.assertTrue(all("payload" not in item for item in report["slices"]))
        self.assertTrue(all(len(item["raw_lineage_sha256"]) == 64 for item in report["slices"]))


class _RawEnvelope:
    def __init__(self, frame, *, provider="BINANCE_DIRECT"):
        payload = json.dumps(frame, sort_keys=True, separators=(",", ":")).encode()
        self.raw_frame_bytes = payload
        self.provider = provider
        self.capture_id = hashlib.md5(payload, usedforsecurity=False).digest()
        self.raw_frame_sha256 = hashlib.sha256(payload).digest()


class _SdkQueryTransport:
    def __init__(self, rows):
        self.rows = rows
        self.warmup_calls = 0
        self.snapshot_calls = 0

    async def warmup(self, requirement, *, consumer_id):
        self.warmup_calls += 1
        return {
            "schema": "qdl.marketdata.warmup.v2",
            "request_id": "request",
            "snapshot_id": "snapshot",
            "data_as_of_ns": self.rows[-1].payload.close_time_ns,
            "stream_cursor": "cursor",
            "watermark_offset": len(self.rows),
            "coverage": "FULL",
            "count": len(self.rows),
            "data": [item.model_dump(mode="json") for item in self.rows],
        }

    async def snapshot(self, requirement, *, consumer_id):
        self.snapshot_calls += 1
        raise AssertionError("typed warmup must not use snapshot")


class _SdkBatchTransport:
    def __init__(self, *, fail_uids=None):
        self.fail_uids = set(fail_uids or ())
        self.chunk_sizes = []

    async def warmup_batch(
        self,
        requirements,
        *,
        consumer_id,
        require_all,
    ):
        del consumer_id, require_all
        self.chunk_sizes.append(len(requirements))
        results = []
        for requirement in requirements:
            if requirement.instrument_uid in self.fail_uids:
                results.append({
                    "instrument_uid": requirement.instrument_uid,
                    "status": "SOURCE_UNAVAILABLE",
                    "problem": {
                        "type": "about:blank",
                        "title": "Source unavailable",
                        "status": 503,
                        "code": "SOURCE_UNAVAILABLE",
                        "detail": "injected provider failure",
                        "request_id": "batch-request",
                        "retryable": True,
                    },
                })
                continue
            row = _sdk_bar(BASE_NS).model_dump(mode="json")
            row["instrument_uid"] = requirement.instrument_uid
            results.append({
                "instrument_uid": requirement.instrument_uid,
                "status": "OK",
                "data": {
                    "schema": "qdl.marketdata.warmup.v2",
                    "request_id": "batch-request",
                    "snapshot_id": f"snapshot-{requirement.instrument_uid}",
                    "data_as_of_ns": row["payload"]["close_time_ns"],
                    "stream_cursor": "cursor",
                    "watermark_offset": 1,
                    "coverage": "FULL",
                    "count": 1,
                    "data": [row],
                },
            })
        errors = sum(item["status"] != "OK" for item in results)
        return {
            "schema": "qdl.marketdata.batch.v2",
            "request_id": "batch-request",
            "partial": errors > 0,
            "success_count": len(results) - errors,
            "error_count": errors,
            "results": results,
        }


class _SdkStreamTransport:
    def subscribe(self, *args, **kwargs):
        async def empty():
            if False:
                yield None
        return empty()


def _fixed_interval_ms(interval: str) -> int:
    value = int(interval[:-1])
    return value * {
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
    }[interval[-1]]


def _sdk_bar(open_ns: int) -> MarketDataView:
    decimal = {"coefficient": "1", "scale": 0, "source_text": "1"}
    return MarketDataView.model_validate({
        "instrument_uid": "uid",
        "instrument_id": "BINANCE:USDM:PERPETUAL:ETH-USDT",
        "instrument_revision": 1,
        "feed": "BAR",
        "interval": "1m",
        "observed_at_ns": open_ns + MINUTE_NS - 1,
        "received_at_ns": open_ns + MINUTE_NS + 1,
        "revision": 0,
        "payload": {
            "feed": "BAR", "interval": "1m", "open_time_ns": open_ns,
            "close_time_ns": open_ns + MINUTE_NS - 1,
            "open": decimal, "high": decimal, "low": decimal, "close": decimal,
            "volume": decimal, "volume_unit": "BASE_ASSET", "trade_count": 1,
            "lifecycle": "FINAL", "revision": 0, "origin": "VENUE_NATIVE",
        },
        "source": {
            "venue": "BINANCE", "provider": "BINANCE_DIRECT",
            "source_id": "source", "source_role": "PRIMARY", "authoritative": True,
        },
        "quality": {
            "state": "LIVE", "freshness_ms": 0, "gap_open": False,
            "complete": True, "execution_eligible": True,
            "policy_id": "crypto_primary_v2", "flags": [],
        },
        "contract": {
            "schema_digest": "a" * 64, "contract_version": "2.0.0",
            "normalizer_version": "normalizer", "adapter_version": "adapter",
            "instrument_catalog_revision": 1, "source_policy_revision": 1,
            "authority_revision": 1, "config_revision": 1,
            "correlation_id": f"bar:{open_ns}",
        },
        "watermark_offset": open_ns // MINUTE_NS,
    })


if __name__ == "__main__":
    unittest.main()
