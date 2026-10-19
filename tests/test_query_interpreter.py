"""The Query reader's interpreter settings (v2.1.1 Phase-3 stage-5 evidence)."""

from __future__ import annotations

import asyncio
import gc
import json
import sys
import time
import unittest

from qdl.runtime.stall_telemetry import (
    STALL_LOG_PREFIX,
    GcPauseTelemetry,
    SlowRequestTelemetry,
    StallRecorder,
    install_gc_pause_telemetry,
    install_request_stall_telemetry,
    watch_loop_lag,
)
from qdl.runtime.stable import (
    QUERY_GC_THRESHOLDS,
    QUERY_GIL_SWITCH_INTERVAL_SECONDS,
    configure_query_interpreter,
    freeze_query_startup_heap,
)


class _Marker:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        await self.app(scope, receive, send)


class QueryInterpreterTests(unittest.TestCase):
    def test_query_reader_shortens_the_gil_switch_interval_and_raises_gc_thresholds(self):
        original_interval, original_thresholds = sys.getswitchinterval(), gc.get_threshold()
        try:
            configure_query_interpreter()
            self.assertAlmostEqual(sys.getswitchinterval(), QUERY_GIL_SWITCH_INTERVAL_SECONDS)
            self.assertLess(QUERY_GIL_SWITCH_INTERVAL_SECONDS, 0.005)
            self.assertEqual(gc.get_threshold(), QUERY_GC_THRESHOLDS)
            self.assertGreater(QUERY_GC_THRESHOLDS[0], 700)
        finally:
            sys.setswitchinterval(original_interval)
            gc.set_threshold(*original_thresholds)

    def test_query_reader_installs_one_gc_pause_hook(self):
        original_interval, original_thresholds = sys.getswitchinterval(), gc.get_threshold()
        original_callbacks = list(gc.callbacks)
        try:
            configure_query_interpreter()
            configure_query_interpreter()
            hooks = [item for item in gc.callbacks if isinstance(item, GcPauseTelemetry)]
            self.assertEqual(len(hooks), 1)
            self.assertIs(install_gc_pause_telemetry(), hooks[0])
        finally:
            gc.callbacks[:] = original_callbacks
            sys.setswitchinterval(original_interval)
            gc.set_threshold(*original_thresholds)

    def test_startup_heap_is_frozen_out_of_later_sweeps(self):
        try:
            freeze_query_startup_heap()
            self.assertGreater(gc.get_freeze_count(), 0)
        finally:
            gc.unfreeze()


class StallTelemetryTests(unittest.TestCase):
    def _recorder(self):
        lines = []
        return StallRecorder(write=lines.append), lines

    @staticmethod
    def _records(lines):
        out = []
        for line in lines:
            prefix, _, body = line.partition(" ")
            assert prefix == STALL_LOG_PREFIX and line.endswith("\n")
            out.append(json.loads(body))
        return out

    def test_a_long_collection_is_recorded_in_the_callback_and_written_by_the_flush(self):
        recorder, lines = self._recorder()
        ticks = iter([10.0, 10.4125])
        hook = GcPauseTelemetry(recorder, clock=lambda: next(ticks), wall_ms=lambda: 1234, rss=lambda: 512.5)
        hook("start", {"generation": 2, "collected": 0, "uncollectable": 0})
        hook("stop", {"generation": 2, "collected": 4096, "uncollectable": 0})
        self.assertEqual(lines, [])  # nothing formatted or written inside the collector
        self.assertEqual(recorder.flush(), 1)
        (record,) = self._records(lines)
        self.assertEqual((record["kind"], record["generation"]), ("gc", 2))
        self.assertAlmostEqual(record["duration_ms"], 412.5, places=3)
        self.assertEqual((record["collected"], record["uncollectable"]), (4096, 0))
        self.assertEqual(record["collections"], [0, 0, 1])
        self.assertEqual(len(record["count"]), 3)
        self.assertEqual((record["rss_mib"], record["wall_ms"]), (512.5, 1234))

    def test_short_collections_are_counted_but_not_recorded(self):
        recorder, lines = self._recorder()
        ticks = iter([1.0, 1.009, 2.0, 2.5])
        hook = GcPauseTelemetry(recorder, clock=lambda: next(ticks))
        hook("stop", {"generation": 0, "collected": 0, "uncollectable": 0})  # unpaired: ignored
        hook("start", {"generation": 0})
        hook("stop", {"generation": 0, "collected": 1, "uncollectable": 0})
        hook("start", {"generation": 1})
        hook("stop", {"generation": 1, "collected": 1, "uncollectable": 0})
        recorder.flush()
        (record,) = self._records(lines)
        self.assertEqual(record["collections"], [1, 1, 0])

    def test_a_failing_writer_never_reaches_the_caller_and_a_full_buffer_counts_drops(self):
        def broken(_line):
            raise OSError("stderr closed")

        recorder = StallRecorder(capacity=2, write=broken)
        for index in range(3):
            recorder.record({"kind": "loop_lag", "index": index})
        self.assertEqual(recorder.flush(), 2)
        lines = []
        recorder = StallRecorder(capacity=2, write=lines.append)
        for index in range(3):
            recorder.record({"kind": "loop_lag", "index": index})
        recorder.flush()
        records = self._records(lines)
        self.assertEqual([item["index"] for item in records], [1, 2])
        self.assertEqual(records[0]["dropped"], 1)

    def test_the_hook_observes_a_real_collection_without_changing_it(self):
        recorder, lines = self._recorder()
        hook = GcPauseTelemetry(recorder, threshold_seconds=0.0)
        thresholds = gc.get_threshold()
        gc.callbacks.append(hook)
        try:
            gc.collect(0)
        finally:
            gc.callbacks.remove(hook)
        self.assertEqual(gc.get_threshold(), thresholds)
        recorder.flush()
        self.assertEqual(self._records(lines)[-1]["generation"], 0)

    def test_loop_lag_is_recorded_when_the_loop_is_late(self):
        recorder, lines = self._recorder()

        async def scenario():
            task = asyncio.get_running_loop().create_task(
                watch_loop_lag(recorder, interval_seconds=0.01, threshold_seconds=0.05))
            await asyncio.sleep(0.02)
            time.sleep(0.12)  # block the loop
            await asyncio.sleep(0.05)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        asyncio.run(scenario())
        recorder.flush()
        lags = [item for item in self._records(lines) if item["kind"] == "loop_lag"]
        self.assertTrue(lags)
        self.assertGreaterEqual(lags[0]["lag_ms"], 50)

    def test_slow_requests_are_recorded_and_the_response_is_unchanged(self):
        recorder, lines = self._recorder()
        sent = []

        async def app(scope, receive, send):
            if scope.get("path") == "/slow":
                await asyncio.sleep(0.03)
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        async def send(message):
            sent.append(message)

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        wrapped = SlowRequestTelemetry(app, recorder=recorder, threshold_seconds=0.02)

        async def scenario():
            await wrapped({"type": "http", "method": "GET", "path": "/fast"}, receive, send)
            await wrapped({"type": "http", "method": "POST", "path": "/slow"}, receive, send)
            await wrapped({"type": "lifespan"}, receive, send)

        asyncio.run(scenario())
        self.assertEqual([m["type"] for m in sent], ["http.response.start", "http.response.body"] * 3)
        recorder.flush()
        (record,) = self._records(lines)
        self.assertEqual((record["kind"], record["method"], record["path"], record["status"]),
                         ("slow_request", "POST", "/slow", 200))
        self.assertGreaterEqual(record["duration_ms"], 20)
        self.assertLessEqual(record["start_wall_ms"], record["wall_ms"])

    def test_the_query_app_wraps_requests_outermost(self):
        from fastapi import FastAPI

        app = FastAPI()
        app.add_middleware(_Marker)
        install_request_stall_telemetry(app)
        self.assertIs(app.user_middleware[0].cls, SlowRequestTelemetry)

if __name__ == "__main__":
    unittest.main()
