"""KN-4 D33: the SDK accepts Kafka offset 0 as a stream record (KN-2 carry-over).

Cursor v3 makes offset 0 a valid canonical record and a valid snapshot
watermark (contract section 1, first-record bootstrap). The SDK used to
refuse ``StreamEvent(0, ...)``; continuity stays strictly increasing after
the handoff watermark, so a record at or below the watermark is still a
gap, never silently accepted.
"""
from __future__ import annotations

import unittest

from qdl.query.v2 import query_pb2
from qdl_sdk.client import WarmupStreamSession
from qdl_sdk.errors import ContinuityError
from qdl_sdk.models import StreamEvent


class _Events:
    def __init__(self, events):
        self._events = iter(events)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._events)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self):
        return None


def session(events, *, starting_offset: int) -> WarmupStreamSession:
    from qdl_sdk.cursor import MemoryCursorStore

    return WarmupStreamSession(
        consumer_id="c", requirement=None, warmup=None, events=_Events(events),
        cursor_store=MemoryCursorStore(), cursor_key="k", starting_offset=starting_offset,
        query_transport=None, stream_transport=None, max_buffer_events=10,
        max_reconnect_attempts=0, telemetry=None, state_restored=False,
    )


class OffsetZeroTests(unittest.IsolatedAsyncioTestCase):
    def test_offset_zero_is_a_record_and_negative_is_not(self):
        self.assertEqual(StreamEvent(0, "token-0", object()).logical_offset, 0)
        with self.assertRaises(ValueError):
            StreamEvent(-1, "token", object())
        with self.assertRaises(ValueError):
            StreamEvent(0, "", object())

    async def test_continuity_after_a_zero_watermark_is_strict(self):
        # A snapshot that includes record 0 hands off at watermark 0.
        stream = session([StreamEvent(1, "t1", object()), StreamEvent(2, "t2", object())], starting_offset=0)
        self.assertEqual([(await stream.__anext__()).logical_offset for _ in range(2)], [1, 2])
        again = session([StreamEvent(0, "t0", object())], starting_offset=0)
        with self.assertRaises(ContinuityError):
            await again.__anext__()

    async def test_the_grpc_transport_yields_a_record_at_offset_zero(self):
        from qdl_sdk.transport import GrpcStreamTransport

        record = query_pb2.SubscribeResponse()
        record.record.logical_offset = 0
        record.record.resume_token = "resume-0"
        record.record.event.event_id = b"\x00" * 16

        class _Call:
            def __init__(self):
                self._items = iter([record])

            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    return next(self._items)
                except StopIteration:
                    raise StopAsyncIteration from None

            def cancel(self):
                return None

        class _Credentials:
            async def get_token(self):
                return "jwt"

        transport = GrpcStreamTransport.__new__(GrpcStreamTransport)
        transport.targets = ("t",)
        transport.target = "t"
        transport._target_index = 0
        transport._subscribes = [lambda request, metadata: _Call()]
        transport._credential_provider = _Credentials()

        from qdl_sdk.models import DataRequirement, Feed, Grade

        requirement = DataRequirement(instrument_uid="u", feed=Feed.TRADE, consumer_grade=Grade.ALPHA,
                                      source_policy_id="p")
        events = [event async for event in transport.subscribe(requirement, consumer_id="c", cursor_token="x")]
        self.assertEqual([(event.logical_offset, event.resume_token) for event in events], [(0, "resume-0")])


if __name__ == "__main__":
    unittest.main()
