"""TEST_ONLY behavioral coverage; no provider or runtime certification."""
import asyncio
from dataclasses import replace
import unittest
from unittest.mock import AsyncMock, patch

from qdl_sdk.client import AsyncDataLayerClient
from qdl_sdk.errors import ContinuityError, DataLayerError
from qdl_sdk.models import DataRequirement, Feed, Grade, WarmupResponse
from tests.test_phase10_universal_warmup import _SdkBatchTransport, _SdkStreamTransport
from tests.test_fund_phase5_stream_sdk import FakeQueryTransport, ScriptedStreamTransport


def requirements(count, rows=1):
    return [DataRequirement(f"uid-{i}", Feed.BAR, Grade.ALPHA, "crypto_primary_v2",
                            interval="1m", warmup_limit=rows) for i in range(count)]


def client(query, stream=None):
    return AsyncDataLayerClient(query_transport=query,
                                stream_transport=stream or _SdkStreamTransport(),
                                consumer_id="test-only-kn-pre5")


class BoundedBatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_universe_350_bounds_and_early_close_no_prefetch(self):
        query = _SdkBatchTransport()
        sdk = client(query)
        chunks = sdk.iter_warmup_batches(requirements(350))
        first = await anext(chunks)
        self.assertEqual(len(first.results), 100)
        self.assertEqual(query.chunk_sizes, [100])
        await chunks.aclose()
        self.assertEqual(query.chunk_sizes, [100])
        query.chunk_sizes.clear()
        response = await sdk.warmup_batch(requirements(350))
        self.assertEqual(query.chunk_sizes, [100, 100, 100, 50])
        self.assertEqual(response.success_count, 350)

    async def test_row_budget_does_not_reduce_requested_history(self):
        for rows, sizes in ((480, [5, 5]), (2500, [1] * 10), (5000, [1] * 10), (10000, [1] * 10)):
            query = _SdkBatchTransport()
            chunks = [part async for part in client(query).iter_warmup_batches(requirements(10, rows))]
            self.assertEqual(query.chunk_sizes, sizes)
            self.assertEqual(sum(len(part.results) for part in chunks), 10)

    async def test_explicit_larger_row_budget_remains_supported(self):
        query = _SdkBatchTransport()
        chunks = [part async for part in client(query).iter_warmup_batches(
            requirements(10, 2500), max_batch_rows=10000)]
        self.assertEqual(query.chunk_sizes, [4, 4, 2])
        self.assertEqual(sum(len(part.results) for part in chunks), 10)

    async def test_invalid_duplicate_execution_partial_rejected_before_io(self):
        query = _SdkBatchTransport()
        sdk = client(query)
        for values, kwargs in (([], {}), (requirements(1) * 2, {}),
                               (requirements(1), {"batch_size": 101}),
                               (requirements(1), {"max_batch_rows": 0}),
                               ([replace(requirements(1)[0], consumer_grade=Grade.EXECUTION)],
                                {"require_all": False})):
            with self.assertRaises(ValueError):
                await anext(sdk.iter_warmup_batches(values, **kwargs))
        self.assertEqual(query.chunk_sizes, [])

    async def test_partial_data_count_and_identity_are_checked(self):
        for mutation in ("count", "identity", "interval", "data_and_error"):
            query = _SdkBatchTransport(fail_uids={"uid-1"})
            fetch = query.warmup_batch
            async def broken(*args, **kwargs):
                body = await fetch(*args, **kwargs)
                if mutation == "count":
                    body.update(success_count=2, error_count=0, partial=False)
                elif mutation == "identity":
                    body["results"][0]["data"]["data"][0]["instrument_uid"] = "other"
                elif mutation == "interval":
                    body["results"][0]["data"]["data"][0]["interval"] = "1d"
                else:
                    body["results"][0]["problem"] = body["results"][1]["problem"]
                return body
            query.warmup_batch = broken
            with self.subTest(mutation=mutation), self.assertRaises(ContinuityError):
                await client(query).warmup_batch(requirements(2), require_all=False)

    async def test_cancel_inflight_does_not_launch_next_chunk(self):
        entered, canceled = asyncio.Event(), asyncio.Event()
        query = _SdkBatchTransport()
        async def blocked(*args, **kwargs):
            query.chunk_sizes.append(len(args[0]))
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                canceled.set()
        query.warmup_batch = blocked
        task = asyncio.create_task(client(query).warmup_batch(requirements(350)))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(canceled.is_set())
        self.assertEqual(query.chunk_sizes, [100])

    async def test_typed_batch_rows_are_not_dumped_and_reparsed(self):
        with patch.object(WarmupResponse, "model_dump", side_effect=AssertionError("second serialization")):
            result = await client(_SdkBatchTransport()).warmup_batch(requirements(205))
        self.assertEqual(result.success_count, 205)


class InitialHandoffTests(unittest.IsolatedAsyncioTestCase):
    async def test_initial_window_reuses_cursor_with_no_second_query(self):
        req = requirements(1)[0]
        query = FakeQueryTransport("server-signed-cursor", watermark=0)
        initial = WarmupResponse.model_validate(await query.warmup(req, consumer_id="test"))
        query.calls = 0
        stream = ScriptedStreamTransport(((),))
        sdk = client(query, stream)
        async with sdk.warmup_then_stream(req, initial_warmup=initial) as session:
            self.assertIs(session.warmup, initial)
            with self.assertRaises(StopAsyncIteration):
                await session.__anext__()
        self.assertEqual(query.calls, 0)
        self.assertEqual(stream.tokens, ["server-signed-cursor"])

    async def test_initial_window_wrong_identity_interval_gap_or_finality_is_rejected(self):
        req = requirements(1)[0]
        query = FakeQueryTransport("server-signed-cursor")
        for mutation in ("identity", "interval", "gap", "policy", "partial", "nonfinal"):
            data = await query.warmup(req, consumer_id="test")
            row = data["data"][0]
            if mutation == "identity":
                row["instrument_uid"] = "other"
            elif mutation == "interval":
                row["interval"] = "1h"
                row["payload"]["interval"] = "1h"
            elif mutation == "gap":
                row["quality"]["gap_open"] = True
            elif mutation == "policy":
                row["quality"]["policy_id"] = "other"
            elif mutation == "partial":
                data["coverage"] = "PARTIAL"
            else:
                row["payload"]["lifecycle"] = "IN_PROGRESS"
            initial = WarmupResponse.model_validate(data)
            with self.subTest(mutation=mutation), self.assertRaises((DataLayerError, ValueError)):
                async with client(query).warmup_then_stream(req, initial_warmup=initial):
                    self.fail("unsafe handoff")


class ColdAdmissionRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_is_bounded_counted_and_resets_for_next_read(self):
        sdk = client(_SdkBatchTransport())
        calls = []
        async def fetch():
            calls.append(1)
            if len(calls) < 3:
                raise DataLayerError("RATE_LIMITED", "test-only", retryable=True)
            return "validated"
        with patch("qdl_sdk.client.asyncio.sleep", new_callable=AsyncMock) as sleep:
            self.assertEqual(await sdk._cold_read(fetch), "validated")
            self.assertEqual([c.args[0] for c in sleep.call_args_list], [0.25, 0.5])
            self.assertEqual(await sdk._cold_read(fetch), "validated")
        self.assertEqual((sdk.warmup_read_attempts, sdk.warmup_admission_retries), (4, 2))

    async def test_permanent_quality_and_excessive_retry_after_are_not_retried(self):
        for code, retryable, delay in (("DATA_STALE", True, None), ("OPEN_SEQUENCE_GAP", True, None),
                                      ("PERMISSION_DENIED", False, None), ("RATE_LIMITED", False, None),
                                      ("RATE_LIMITED", True, 30000)):
            sdk = client(_SdkBatchTransport())
            error = DataLayerError(code, "test-only", retryable=retryable, retry_after_ms=delay)
            async def fetch(): raise error
            with self.assertRaises(DataLayerError) as caught:
                await sdk._cold_read(fetch)
            self.assertIs(caught.exception, error)
            self.assertEqual(sdk.warmup_read_attempts, 1)

    async def test_all_retries_exhaust_with_original_problem(self):
        sdk = client(_SdkBatchTransport())
        error = DataLayerError("RATE_LIMITED", "test-only", retryable=True)
        async def fetch(): raise error
        with patch("qdl_sdk.client.asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaises(DataLayerError) as caught:
                await sdk._cold_read(fetch)
        self.assertIs(caught.exception, error)
        self.assertEqual((sdk.warmup_read_attempts, sdk.warmup_admission_retries), (3, 2))

    async def test_whole_batch_refusal_retries_exact_identity_but_mixed_does_not(self):
        for mixed in (False, True):
            query = _SdkBatchTransport(fail_uids={"uid-0"} if mixed else {"uid-0", "uid-1"})
            original = query.warmup_batch
            seen = []
            async def fetch(values, **kwargs):
                seen.append(tuple(values))
                body = await original(values, **kwargs)
                for item in body["results"]:
                    if item.get("problem"):
                        item["problem"].update(code="RATE_LIMITED", retryable=True)
                query.fail_uids.clear()
                return body
            query.warmup_batch = fetch
            sdk = client(query)
            with patch("qdl_sdk.client.asyncio.sleep", new_callable=AsyncMock):
                result = await sdk.warmup_batch(requirements(2, 480), require_all=False)
            self.assertEqual(len(seen), 1 if mixed else 2)
            self.assertEqual(result.partial, mixed)
            self.assertTrue(all(value == seen[0] for value in seen))

    async def test_cancel_during_backoff_never_dispatches_again(self):
        sdk = client(_SdkBatchTransport())
        waiting = asyncio.Event()
        async def fetch(): raise DataLayerError("RATE_LIMITED", "test-only", retryable=True)
        async def wait(_seconds):
            waiting.set()
            await asyncio.Event().wait()
        with patch("qdl_sdk.client.asyncio.sleep", side_effect=wait):
            task = asyncio.create_task(sdk._cold_read(fetch))
            await waiting.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(sdk.warmup_read_attempts, 1)
