from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
import time
import unittest
from unittest.mock import AsyncMock, patch

from qdl.query import FeedType
from qdl_sdk.reference import BasisSeries
from scripts.phase105_consumer_v2_identity_acceptance import (
    _closing_revalidate_consumer, _reference_batch_for_c2,
)
from qdl.certification.phase103_consumer_acceptance import DeliveryClass
from scripts.phasec36_reference_l2_consumer_acceptance import _reference_batch_until_terminal


class Phase105NativeBasisLaneTests(unittest.IsolatedAsyncioTestCase):
    async def test_closing_references_share_absolute_bound_not_cache_timeout(self):
        products = (SimpleNamespace(delivery=DeliveryClass.DURABLE),
                    SimpleNamespace(delivery=DeliveryClass.ON_DEMAND))
        prefix = "scripts.phase105_consumer_v2_identity_acceptance."
        for venue in ("BINANCE", "OKX"):
            with self.subTest(venue=venue), patch(
                prefix + "_closing_batch_revalidation", new_callable=AsyncMock,
                return_value=["stream"],
            ) as stream, patch(
                prefix + "_certify_references", new_callable=AsyncMock,
                return_value=["reference"],
            ) as reference:
                deadline = time.monotonic() + 120
                result = await _closing_revalidate_consumer(
                    venue, products, identity=object(), primary_url="https://primary",
                    secondary_url="https://secondary", grpc_target="stream:8210",
                    state_dir=Path("/tmp/not-created"), timeout_seconds=15,
                    deadline_monotonic=deadline, max_batch_items=50,
                    reference_semaphore=asyncio.Semaphore(2),
                    native_basis_semaphore=asyncio.Semaphore(1), client_factory=object(),
                )
                self.assertEqual(result, ["stream", "reference"])
                self.assertEqual(reference.await_args.kwargs["deadline_monotonic"], deadline)
                self.assertEqual(reference.await_args.kwargs["timeout_seconds"], 15)
                self.assertEqual(stream.await_args.kwargs["timeout_seconds"], 15)

    async def test_native_cooldown_fits_existing_closing_bound_but_never_extends_it(self):
        product = self._product(venue="BINANCE", native=True)
        now = 100.0

        async def sleep(seconds):
            nonlocal now
            now += seconds

        response = SimpleNamespace(partial=True, success_count=0, error_count=1, results=(
            SimpleNamespace(status="SOURCE_UNAVAILABLE", data=None, problem=SimpleNamespace(
                code="SOURCE_UNAVAILABLE", retryable=True, retry_after_ms=60_000)),))
        success = SimpleNamespace(partial=False)
        client = SimpleNamespace(reference_batch=AsyncMock(side_effect=[response, success]))
        result = await _reference_batch_until_terminal(
            client, (product,), deadline_monotonic=220, clock=lambda: now, sleep=sleep,
        )
        self.assertEqual(result, (success, 2, 60_000))
        self.assertEqual(now, 160)
        for remaining in (15, 60):
            with self.subTest(remaining=remaining):
                client = SimpleNamespace(reference_batch=AsyncMock(return_value=response))
                with self.assertRaisesRegex(AssertionError, "retry_after_ms=60000"):
                    await _reference_batch_until_terminal(
                        client, (product,), deadline_monotonic=now+remaining,
                        clock=lambda: now, sleep=sleep,
                    )
                self.assertEqual(client.reference_batch.await_count, 1)

    @staticmethod
    def _product(*, venue: str, native: bool):
        return SimpleNamespace(
            venue=venue,
            requirement=SimpleNamespace(feed=FeedType.BASIS),
            sdk_requirement=SimpleNamespace(
                basis_series=BasisSeries.NATIVE if native else BasisSeries.CONTINUOUS
            ),
        )

    async def test_native_basis_batches_share_one_lane_across_replicas(self):
        product = self._product(venue="BINANCE", native=True)
        active = 0
        maximum = 0

        async def terminal(*_args, **_kwargs):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.01)
            active -= 1
            return SimpleNamespace(), 1, 0

        native_lane = asyncio.Semaphore(1)
        with patch(
            "scripts.phase105_consumer_v2_identity_acceptance._reference_batch_until_terminal",
            side_effect=terminal,
        ):
            results = await asyncio.gather(
                _reference_batch_for_c2(
                    object(), (product,), deadline_monotonic=time.monotonic() + 1,
                    semaphore=asyncio.Semaphore(2), native_basis_semaphore=native_lane,
                ),
                _reference_batch_for_c2(
                    object(), (product,), deadline_monotonic=time.monotonic() + 1,
                    semaphore=asyncio.Semaphore(2), native_basis_semaphore=native_lane,
                ),
            )

        self.assertEqual(maximum, 1)
        self.assertEqual([item[1:] for item in results], [(1, 0), (1, 0)])

    async def test_non_native_reference_does_not_wait_for_native_lane(self):
        product = self._product(venue="OKX", native=False)
        native_lane = asyncio.Semaphore(1)
        await native_lane.acquire()

        async def terminal(*_args, **_kwargs):
            return SimpleNamespace(), 1, 0

        try:
            with patch(
                "scripts.phase105_consumer_v2_identity_acceptance._reference_batch_until_terminal",
                side_effect=terminal,
            ):
                result = await asyncio.wait_for(
                    _reference_batch_for_c2(
                        object(), (product,), deadline_monotonic=time.monotonic() + 1,
                        semaphore=asyncio.Semaphore(1), native_basis_semaphore=native_lane,
                    ),
                    timeout=0.1,
                )
        finally:
            native_lane.release()

        self.assertEqual(result[1:], (1, 0))
