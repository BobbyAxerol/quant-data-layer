"""SDK 2.3.0 batched Subscribe: per-record parity against batching and older gateways.

A loopback grpc.aio server plays the gateway: it either packs up to the requested
number of records into `SubscribeResponse.records` (2.3.0 shape) or ignores the
request field and sends one `record` per response (a pre-2.3.0 gateway). The SDK
must yield the identical StreamEvent/ControlEvent sequence, with each record's own
offset and resume token, in every combination. The Rust gateway's packing rules are
pinned in `rust/qdl-stream-gateway/tests/native_stream.rs` (b01-b04).
"""

from __future__ import annotations

import asyncio
import unittest

import grpc

from qdl.marketdata.v2 import market_data_pb2
from qdl.query.v2 import query_pb2
from qdl_sdk import DataRequirement, Feed, Grade, GrpcStreamTransport, StaticBearerCredential
from qdl_sdk.models import ControlEvent, StreamEvent
from qdl_sdk.transport import DEFAULT_MAX_RECORDS_PER_RESPONSE, MAX_RECORDS_PER_RESPONSE

METHOD = "/qdl.query.v2.MarketDataStreamService/Subscribe"
RECORDS = 150


def control(code: str, high: int) -> query_pb2.StreamRecord:
    state = {
        "REPLAYING": query_pb2.STREAM_CONTROL_STATE_REPLAYING,
        "LIVE": query_pb2.STREAM_CONTROL_STATE_LIVE,
    }[code]
    return query_pb2.StreamRecord(
        resume_token=f"tok-control-{code}",
        control=query_pb2.StreamControl(
            state=state, code=code, detail=f"{code} detail", high_watermark=high
        ),
    )


def event(offset: int) -> query_pb2.StreamRecord:
    return query_pb2.StreamRecord(
        logical_offset=offset,
        resume_token=f"tok-{offset}",
        event=market_data_pb2.EventEnvelope(
            event_id=offset.to_bytes(16, "big"),
            instrument_uid="uid-test",
            partition_sequence=offset,
            trade=market_data_pb2.Trade(native_trade_id=f"t{offset}"),
        ),
    )


class FakeGateway:
    """`batching=False` is a pre-2.3.0 gateway: it never reads field 7."""

    def __init__(self, *, batching: bool) -> None:
        self.batching = batching
        self.requests: list[bytes] = []
        self.response_sizes: list[int] = []

    def responses(self, request: query_pb2.SubscribeRequest):
        yield query_pb2.SubscribeResponse(record=control("REPLAYING", 10))
        yield query_pb2.SubscribeResponse(record=event(10))  # a replayed record
        yield query_pb2.SubscribeResponse(record=control("LIVE", 10))
        limit = max(1, request.max_records_per_response) if self.batching else 1
        offsets = list(range(11, 11 + RECORDS))
        # Irregular availability: what a gateway finds queued varies.
        chunks, index, pattern = [], 0, (1, 7, 64, 3, 200, 2)
        while index < len(offsets):
            size = min(pattern[len(chunks) % len(pattern)], limit)
            chunks.append(offsets[index:index + size])
            index += size
        for chunk in chunks:
            if len(chunk) == 1:
                yield query_pb2.SubscribeResponse(record=event(chunk[0]))
            else:
                yield query_pb2.SubscribeResponse(records=[event(o) for o in chunk])

    async def subscribe(self, raw: bytes, context):
        self.requests.append(raw)
        request = query_pb2.SubscribeRequest.FromString(raw)
        for response in self.responses(request):
            self.response_sizes.append(max(1, len(response.records)))
            yield response.SerializeToString()


async def serve(gateway: FakeGateway):
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        "qdl.query.v2.MarketDataStreamService",
        {"Subscribe": grpc.unary_stream_rpc_method_handler(
            gateway.subscribe, request_deserializer=None, response_serializer=None)},
    ),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, port


def requirement() -> DataRequirement:
    return DataRequirement("uid-test", Feed.TRADE, Grade.EXECUTION, "policy-test")


def key(item) -> tuple:
    if isinstance(item, ControlEvent):
        return ("control", item.code, item.detail, dict(item.snapshot))
    assert isinstance(item, StreamEvent)
    return ("event", item.logical_offset, item.resume_token, item.event.SerializeToString())


class BatchedSubscribeTests(unittest.IsolatedAsyncioTestCase):
    async def run_stream(self, *, batching: bool, **kwargs):
        gateway = FakeGateway(batching=batching)
        server, port = await serve(gateway)
        transport = GrpcStreamTransport(
            f"127.0.0.1:{port}",
            allow_insecure_loopback=True,
            credential_provider=StaticBearerCredential("test-only"),
            **kwargs,
        )
        try:
            items = [
                item
                async for item in transport.subscribe(
                    requirement(),
                    consumer_id="consumer-test",
                    cursor_token="cursor-test",
                    max_buffer_events=1000,
                )
            ]
        finally:
            await transport.close()
            await server.stop(grace=0)
        return items, gateway

    def assert_per_record(self, items) -> None:
        events = [item for item in items if isinstance(item, StreamEvent)]
        self.assertEqual([e.logical_offset for e in events], list(range(10, 11 + RECORDS)))
        for e in events:
            self.assertEqual(e.resume_token, f"tok-{e.logical_offset}")
            self.assertEqual(e.event.partition_sequence, e.logical_offset)
        self.assertEqual(
            [item.code for item in items if isinstance(item, ControlEvent)],
            ["REPLAYING", "LIVE"],
        )
        self.assertEqual(items[2].snapshot, {"high_watermark": 10})

    async def test_default_requests_batches_and_yields_one_event_per_record(self):
        items, gateway = await self.run_stream(batching=True)
        request = query_pb2.SubscribeRequest.FromString(gateway.requests[0])
        self.assertEqual(request.max_records_per_response, DEFAULT_MAX_RECORDS_PER_RESPONSE)
        self.assertEqual(DEFAULT_MAX_RECORDS_PER_RESPONSE, 64)
        self.assertGreater(max(gateway.response_sizes), 1, "the gateway did batch")
        self.assertLessEqual(max(gateway.response_sizes), 64)
        self.assert_per_record(items)

    async def test_batched_and_unbatched_sequences_are_identical(self):
        reference, _ = await self.run_stream(batching=True, max_records_per_response=1)
        self.assert_per_record(reference)
        for limit in (2, 16, 64, MAX_RECORDS_PER_RESPONSE):
            with self.subTest(limit=limit):
                items, gateway = await self.run_stream(
                    batching=True, max_records_per_response=limit
                )
                self.assertGreater(max(gateway.response_sizes), 1)
                self.assertEqual([key(i) for i in items], [key(i) for i in reference])

    async def test_one_record_per_response_sends_the_pre_2_3_request_bytes(self):
        _, gateway = await self.run_stream(batching=True, max_records_per_response=1)
        legacy = query_pb2.SubscribeRequest(
            consumer_id="consumer-test",
            requirement=requirement().to_proto(),
            cursor_token="cursor-test",
            max_buffer_events=1000,
            # 2.4.1 always opts into replay budget status v2 (IDR-096).
            replay_budget_status_v2=True,
        ).SerializeToString()
        self.assertEqual(gateway.requests[0], legacy, "field 7 absent, as in 2.2.x")
        self.assertEqual(set(gateway.response_sizes), {1})

    async def test_every_request_opts_into_replay_budget_status_v2(self):
        # IDR-096 (B): the session bounds reconnects by progress, so the SDK
        # asks for RESOURCE_EXHAUSTED replay budgets; a gateway that does not
        # know field 8 ignores it (proto3) and keeps CURSOR_EXPIRED.
        for limit in (1, DEFAULT_MAX_RECORDS_PER_RESPONSE):
            with self.subTest(limit=limit):
                _, gateway = await self.run_stream(
                    batching=True, max_records_per_response=limit
                )
                request = query_pb2.SubscribeRequest.FromString(gateway.requests[0])
                self.assertIs(request.replay_budget_status_v2, True)
        legacy = query_pb2.SubscribeRequest(consumer_id="consumer-test")
        self.assertIs(legacy.replay_budget_status_v2, False, "absent means the old mapping")
        field = query_pb2.SubscribeRequest.DESCRIPTOR.fields_by_name["replay_budget_status_v2"]
        self.assertEqual(field.number, 8)

    async def test_an_older_gateway_is_read_one_record_per_response(self):
        reference, _ = await self.run_stream(batching=True, max_records_per_response=1)
        items, gateway = await self.run_stream(batching=False)
        self.assertEqual(
            query_pb2.SubscribeRequest.FromString(gateway.requests[0]).max_records_per_response,
            64,
        )
        self.assertEqual(set(gateway.response_sizes), {1}, "old gateway ignores the field")
        self.assertEqual([key(i) for i in items], [key(i) for i in reference])

    async def test_a_batch_can_be_abandoned_mid_way_and_resumed_by_record_token(self):
        gateway = FakeGateway(batching=True)
        server, port = await serve(gateway)
        transport = GrpcStreamTransport(
            f"127.0.0.1:{port}",
            allow_insecure_loopback=True,
            credential_provider=StaticBearerCredential("test-only"),
        )
        stream = transport.subscribe(
            requirement(), consumer_id="consumer-test", cursor_token="cursor-test"
        ).__aiter__()
        try:
            seen = []
            while len([i for i in seen if isinstance(i, StreamEvent)]) < 12:
                seen.append(await stream.__anext__())
            last = [i for i in seen if isinstance(i, StreamEvent)][-1]
            # Stopping inside a 64-record batch still leaves an exact per-record
            # resume point: the token of the last record the caller saw.
            self.assertEqual((last.logical_offset, last.resume_token), (21, "tok-21"))
        finally:
            await stream.aclose()
            await transport.close()
            await server.stop(grace=0)

    def test_the_batch_size_is_validated(self):
        for bad in (0, -1, MAX_RECORDS_PER_RESPONSE + 1, 1.5, "64", True):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                GrpcStreamTransport(
                    "127.0.0.1:1",
                    allow_insecure_loopback=True,
                    max_records_per_response=bad,
                )


if __name__ == "__main__":
    unittest.main()
