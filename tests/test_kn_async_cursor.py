from __future__ import annotations

import asyncio
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from qdl_sdk.cursor import CursorCheckpoint, FileCursorStore


def checkpoint(offset: int) -> CursorCheckpoint:
    return CursorCheckpoint(f"test-only-{offset}", offset)


class AsyncCursorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "cursors.json"
        self.store = FileCursorStore(self.path, max_pending=4)

    async def asyncTearDown(self):
        await self.store.aclose()
        self.directory.cleanup()

    async def wait_for(self, predicate):
        async def poll():
            while not predicate():
                await asyncio.sleep(0.001)
        await asyncio.wait_for(poll(), 3)

    def block_writer(self):
        entered, release = threading.Event(), threading.Event()
        original = self.store._write

        def write(items):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test writer release missing")
            original(items)
        return entered, release, write

    async def test_slow_disk_keeps_loop_live_and_bounds_admitted_work(self):
        entered, release, write = self.block_writer()
        with patch.object(self.store, "_write", side_effect=write):
            tasks = [asyncio.create_task(self.store.asave(str(i), checkpoint(i))) for i in range(20)]
            try:
                await self.wait_for(entered.is_set)
                ticks = 0
                for _ in range(20):
                    await asyncio.sleep(0.001)
                    ticks += 1
                self.assertEqual(ticks, 20)
                self.assertEqual(self.store.metrics["pending"], 4)
                self.assertFalse(any(t.done() for t in tasks))
            finally:
                release.set()
            await asyncio.gather(*tasks)
        self.assertLessEqual(self.store.metrics["pending_peak"], 4)
        self.assertLess(self.store.metrics["commits"], 20)
        self.assertEqual(self.store.metrics["pending"], 0)
        for i in range(20):
            self.assertEqual(await self.store.aload(str(i)), checkpoint(i))
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    async def test_monotonic_validation_does_not_drop_other_keys(self):
        results = await asyncio.gather(
            self.store.asave("a", checkpoint(3)),
            self.store.asave("a", checkpoint(2)),
            self.store.asave("b", checkpoint(8)), return_exceptions=True,
        )
        self.assertIsNone(results[0])
        self.assertIsInstance(results[1], ValueError)
        self.assertIsNone(results[2])
        self.assertEqual(await self.store.aload("a"), checkpoint(3))
        self.assertEqual(await self.store.aload("b"), checkpoint(8))
        await self.store.areplace("a", checkpoint(0))
        await self.store.asave("a", checkpoint(1))
        self.assertEqual(await self.store.aload("a"), checkpoint(1))

    async def test_cancel_before_admission_never_writes(self):
        entered, release, write = self.block_writer()
        with patch.object(self.store, "_write", side_effect=write):
            tasks = [asyncio.create_task(self.store.asave(str(i), checkpoint(i))) for i in range(4)]
            await self.wait_for(entered.is_set)
            cancelled = asyncio.create_task(self.store.asave("unadmitted", checkpoint(9)))
            await asyncio.sleep(0)
            cancelled.cancel()
            try:
                with self.assertRaises(asyncio.CancelledError):
                    await cancelled
            finally:
                release.set()
            await asyncio.gather(*tasks)
        self.assertIsNone(await self.store.aload("unadmitted"))

    async def test_repeated_cancel_drains_admitted_write(self):
        entered, release, write = self.block_writer()
        with patch.object(self.store, "_write", side_effect=write):
            task = asyncio.create_task(self.store.asave("a", checkpoint(1)))
            await self.wait_for(entered.is_set)
            try:
                for _ in range(3):
                    task.cancel()
                    await asyncio.sleep(0.01)
                    self.assertFalse(task.done())
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(await self.store.aload("a"), checkpoint(1))
        self.assertEqual(self.store.metrics["pending"], 0)

    async def test_shutdown_drains_write_and_rejects_new_work(self):
        entered, release, write = self.block_writer()
        with patch.object(self.store, "_write", side_effect=write):
            task = asyncio.create_task(self.store.asave("a", checkpoint(1)))
            await self.wait_for(entered.is_set)
            close = asyncio.create_task(self.store.aclose())
            await asyncio.sleep(0)
            try:
                self.assertFalse(close.done())
                with self.assertRaisesRegex(RuntimeError, "closed"):
                    await self.store.asave("b", checkpoint(2))
                close.cancel()
                await asyncio.sleep(0.01)
                self.assertFalse(close.done())
            finally:
                release.set()
            await task
            with self.assertRaises(asyncio.CancelledError):
                await close
        self.assertEqual(self.store.load("a"), checkpoint(1))
        self.assertIsNone(self.store.load("b"))

    async def test_file_fsync_failure_preserves_old_checkpoint(self):
        await self.store.asave("a", checkpoint(1))
        with patch("qdl_sdk.cursor.os.fsync", side_effect=OSError("disk failed")):
            with self.assertRaisesRegex(OSError, "disk failed"):
                await self.store.asave("a", checkpoint(2))
        self.assertEqual(await self.store.aload("a"), checkpoint(1))
        await self.store.asave("a", checkpoint(2))
        self.assertEqual(await self.store.aload("a"), checkpoint(2))
        self.assertEqual(list(self.path.parent.glob(".cursors.json.*")), [])

    async def test_rename_failure_preserves_old_checkpoint(self):
        await self.store.asave("a", checkpoint(1))
        with patch("qdl_sdk.cursor.os.replace", side_effect=OSError("rename failed")):
            with self.assertRaisesRegex(OSError, "rename failed"):
                await self.store.asave("a", checkpoint(2))
        self.assertEqual(await self.store.aload("a"), checkpoint(1))
        await self.store.asave("a", checkpoint(2))

    async def test_directory_fsync_failure_requires_fsync_on_equal_retry(self):
        original = os.fsync
        calls = []

        def fsync(fd):
            calls.append(fd)
            if len(calls) == 2:
                raise OSError("directory failed")
            original(fd)
        with patch("qdl_sdk.cursor.os.fsync", side_effect=fsync):
            with self.assertRaisesRegex(OSError, "directory failed"):
                await self.store.asave("a", checkpoint(2))
            self.assertEqual(self.store.metrics["acknowledged"], 0)
            await self.store.asave("a", checkpoint(2))
        self.assertEqual(len(calls), 4)
        self.assertEqual(self.store.metrics["acknowledged"], 1)

    async def test_batch_io_failure_is_reported_to_every_caller(self):
        with patch.object(self.store, "_write", side_effect=OSError("full")):
            result = await asyncio.gather(*(self.store.asave(str(i), checkpoint(i)) for i in range(4)),
                                          return_exceptions=True)
        self.assertTrue(all(isinstance(item, OSError) for item in result))
        self.assertEqual(self.store.metrics["pending"], 0)
        self.assertEqual(self.store.metrics["acknowledged"], 0)
        await self.store.asave("recovery", checkpoint(1))

    async def test_corrupt_file_fails_closed(self):
        self.path.write_text('{"schema":"wrong","items":{}}')
        with self.assertRaisesRegex(ValueError, "schema"):
            await self.store.aload("a")
        with self.assertRaisesRegex(ValueError, "schema"):
            await self.store.asave("a", checkpoint(1))

    async def test_read_off_loop_and_close_waits_for_read(self):
        entered, release = threading.Event(), threading.Event()
        original = self.store.load

        def load(key):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test read release missing")
            return original(key)
        with patch.object(self.store, "load", side_effect=load):
            read = asyncio.create_task(self.store.aload("a"))
            await self.wait_for(entered.is_set)
            close = asyncio.create_task(self.store.aclose())
            await asyncio.sleep(0.01)
            self.assertFalse(close.done())
            release.set()
            self.assertIsNone(await read)
            await close

    async def test_cancelled_store_close_drains_admitted_read(self):
        entered, release = threading.Event(), threading.Event()
        original = self.store.load

        def load(key):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test read release missing")
            return original(key)
        with patch.object(self.store, "load", side_effect=load):
            read = asyncio.create_task(self.store.aload("a"))
            await self.wait_for(entered.is_set)
            close = asyncio.create_task(self.store.aclose())
            await asyncio.sleep(0)
            close.cancel()
            await asyncio.sleep(0.01)
            prematurely_done = close.done()
            release.set()
            await read
            with self.assertRaises(asyncio.CancelledError):
                await close
        self.assertFalse(prematurely_done)

    async def test_process_crash_leaves_atomic_old_or_new_checkpoint(self):
        for after_replace in (False, True):
            with self.subTest(after_replace=after_replace):
                self.store.replace("a", checkpoint(1))
                program = """
import os, sys
from qdl_sdk.cursor import FileCursorStore, CursorCheckpoint
original = os.replace

def crash(src, dst):
    if sys.argv[2] == 'True':
        original(src, dst)
    os._exit(17)
os.replace = crash
FileCursorStore(sys.argv[1]).save('a', CursorCheckpoint('test-only-2', 2))
"""
                result = await asyncio.to_thread(subprocess.run,
                    [sys.executable, "-B", "-c", program, str(self.path), str(after_replace)],
                    capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 17, result.stderr)
                expected = 2 if after_replace else 1
                self.assertEqual(await self.store.aload("a"), checkpoint(expected))
                # Caller never acknowledged the crashed write: durable retry is safe.
                await self.store.asave("a", checkpoint(2))
                self.assertEqual(await self.store.aload("a"), checkpoint(2))

    async def test_invalid_queue_limits(self):
        for limit in (0, -1, 1025):
            with self.assertRaises(ValueError):
                FileCursorStore(self.path, max_pending=limit)


class AsyncSessionTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = AsyncCursorTests.asyncSetUp
    asyncTearDown = AsyncCursorTests.asyncTearDown
    wait_for = AsyncCursorTests.wait_for
    block_writer = AsyncCursorTests.block_writer
    def session(self, store=None):
        from qdl_sdk.client import WarmupStreamSession
        from unittest.mock import AsyncMock, Mock
        transport = Mock()
        transport.subscribe.return_value = self.events()
        session = WarmupStreamSession(
            consumer_id="test-only", requirement=SimpleNamespace(),
            warmup=SimpleNamespace(watermark_offset=0, stream_cursor="snapshot-test"),
            events=self.events(), cursor_store=store or self.store, cursor_key="a",
            starting_offset=0, query_transport=Mock(), stream_transport=transport,
            max_buffer_events=4, max_reconnect_attempts=2, telemetry=None,
            state_restored=False,
        )
        session._fresh_snapshot = AsyncMock(return_value=SimpleNamespace(
            watermark_offset=0, stream_cursor="new-generation-test"))
        return session

    async def events(self, error=None):
        yield SimpleNamespace(logical_offset=1, resume_token="test-only-1")
        if error is not None:
            raise error

    async def test_session_cancel_finishes_generation_bookkeeping(self):
        session = self.session()
        event = await anext(session)
        entered, release, write = self.block_writer()
        with patch.object(self.store, "_write", side_effect=write):
            ack = asyncio.create_task(session.acknowledge_async(event))
            await self.wait_for(entered.is_set)
            try:
                ack.cancel()
                await asyncio.sleep(0.01)
                ack.cancel()
                await asyncio.sleep(0.01)
                self.assertFalse(ack.done())
                self.assertFalse(session._checkpoint_generation_started)
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):
                await ack
        self.assertTrue(session._checkpoint_generation_started)
        self.assertEqual(await self.store.aload("a"), checkpoint(1))
        await session.aclose()

    async def test_session_io_error_does_not_start_generation(self):
        session = self.session()
        event = await anext(session)
        with patch.object(self.store, "_write", side_effect=OSError("full")):
            with self.assertRaises(OSError):
                await session.acknowledge_async(event)
        self.assertFalse(session._checkpoint_generation_started)
        await session.acknowledge_async(event)
        self.assertTrue(session._checkpoint_generation_started)
        await session.aclose()

    async def test_generation_reset_waits_for_ack_and_rejects_old_event(self):
        from qdl_sdk.errors import CursorExpiredError
        session = self.session()
        session._events = self.events(CursorExpiredError("CURSOR_EXPIRED", "expired"))
        event = await anext(session)
        entered, release, write = self.block_writer()
        with patch.object(self.store, "_write", side_effect=write):
            ack = asyncio.create_task(session.acknowledge_async(event))
            await self.wait_for(entered.is_set)
            reset = asyncio.create_task(anext(session))
            try:
                await asyncio.sleep(0.02)
                self.assertFalse(reset.done())
            finally:
                release.set()
            await ack
            control = await reset
        self.assertEqual(control.code, "SNAPSHOT_REPLACED")
        self.assertFalse(session._checkpoint_generation_started)
        with self.assertRaisesRegex(ValueError, "superseded"):
            await session.acknowledge_async(event)
        await session.aclose()

    async def test_reconnect_reads_acknowledged_checkpoint(self):
        from qdl_sdk.errors import DataLayerError
        session = self.session()
        session._events = self.events(DataLayerError("STREAM_ENDED", "test", retryable=True))
        event = await anext(session)
        entered, release, write = self.block_writer()
        with patch.object(self.store, "_write", side_effect=write):
            ack = asyncio.create_task(session.acknowledge_async(event))
            await self.wait_for(entered.is_set)
            reconnect = asyncio.create_task(anext(session))
            try:
                await asyncio.sleep(0.15)
                self.assertFalse(reconnect.done())
            finally:
                release.set()
            await ack
            await reconnect
        self.assertEqual(session._stream_transport.subscribe.call_args.kwargs["cursor_token"],
                         "test-only-1")
        await session.aclose()

    async def test_session_close_drains_ack(self):
        session = self.session()
        event = await anext(session)
        entered, release, write = self.block_writer()
        with patch.object(self.store, "_write", side_effect=write):
            ack = asyncio.create_task(session.acknowledge_async(event))
            await self.wait_for(entered.is_set)
            close = asyncio.create_task(session.aclose())
            try:
                await asyncio.sleep(0.01)
                self.assertFalse(close.done())
            finally:
                release.set()
            await ack
            await close
        with self.assertRaisesRegex(RuntimeError, "closed"):
            await session.acknowledge_async(event)
        self.assertEqual(await self.store.aload("a"), checkpoint(1))

    async def test_memory_store_async_ack_compatibility(self):
        from qdl_sdk.cursor import MemoryCursorStore
        store = MemoryCursorStore()
        session = self.session(store)
        event = await anext(session)
        await session.acknowledge_async(event)
        self.assertEqual(store.load("a"), checkpoint(1))
        await session.aclose()


    async def test_cancelled_session_close_drains_ack_and_closes_transport(self):
        session = self.session()
        event = await anext(session)
        entered, release, write = self.block_writer()
        with patch.object(self.store, "_write", side_effect=write):
            ack = asyncio.create_task(session.acknowledge_async(event))
            await self.wait_for(entered.is_set)
            close = asyncio.create_task(session.aclose())
            await asyncio.sleep(0)
            close.cancel()
            await asyncio.sleep(0.01)
            prematurely_done = close.done()
            release.set()
            await ack
            with self.assertRaises(asyncio.CancelledError):
                await close
        self.assertFalse(prematurely_done, "cancelled close must drain accepted acknowledgement")
        self.assertIsNone(session._events)
