from __future__ import annotations

import asyncio
from types import SimpleNamespace
import time
import unittest
from unittest.mock import patch

from qdl.query import FeedType
from qdl_sdk.reference import BasisSeries
from scripts.phase105_consumer_v2_identity_acceptance import _reference_batch_for_c2


class Phase105NativeBasisLaneTests(unittest.IsolatedAsyncioTestCase):
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
