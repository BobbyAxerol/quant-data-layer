"""SDK 2.4.0 Subscribe gateway clocks: StreamEvent exposes the per-record gateway wall clocks.

A loopback grpc.aio server plays the gateway. A 2.4.0 gateway sets
`StreamRecord.gateway_dispatch_wall_ns`/`gateway_dequeue_wall_ns`/`gateway_send_wall_ns`
on event records (each record of a batch its own dispatch/dequeue values); an older
gateway sets none. The SDK must expose them on StreamEvent as ints, None when 0/absent,
leave controls untouched and keep StreamEvent equality and positional construction as in
2.3.0. The Rust gateway's stamping rules are pinned in
`rust/qdl-stream-gateway/tests/native_stream.rs` (b05).
"""

from __future__ import annotations

import unittest

import grpc

from qdl.marketdata.v2 import market_data_pb2
from qdl.query.v2 import query_pb2
from qdl_sdk import DataRequirement, Feed, Grade, GrpcStreamTransport, StaticBearerCredential
from qdl_sdk.models import ControlEvent, StreamEvent

BASE = 1_760_000_000_000_000_000


def control(code: str) -> query_pb2.StreamRecord:
    state = {"REPLAYING": query_pb2.STREAM_CONTROL_STATE_REPLAYING,
             "LIVE": query_pb2.STREAM_CONTROL_STATE_LIVE}[code]
    return query_pb2.StreamRecord(
        resume_token=f"tok-control-{code}",
        control=query_pb2.StreamControl(state=state, code=code, detail=f"{code} detail"),
    )


def event(offset: int, *, clocks: bool) -> query_pb2.StreamRecord:
    record = query_pb2.StreamRecord(
        logical_offset=offset,
        resume_token=f"tok-{offset}",
        event=market_data_pb2.EventEnvelope(
            event_id=offset.to_bytes(16, "big"),
            instrument_uid="uid-test",
            partition_sequence=offset,
            trade=market_data_pb2.Trade(native_trade_id=f"t{offset}"),
        ),
    )
    if clocks:
        # Offset 10 is a replayed record: no hub dispatch.
        if offset != 10:
            record.gateway_dispatch_wall_ns = BASE + offset * 1000
        record.gateway_dequeue_wall_ns = BASE + offset * 1000 + 100
        record.gateway_send_wall_ns = BASE + 900_000
    return record


class FakeGateway:
    def __init__(self, *, clocks: bool) -> None:
        self.clocks = clocks

    async def subscribe(self, raw: bytes, context):
        yield query_pb2.SubscribeResponse(record=control("REPLAYING")).SerializeToString()
        yield query_pb2.SubscribeResponse(record=event(10, clocks=self.clocks)).SerializeToString()
        yield query_pb2.SubscribeResponse(record=control("LIVE")).SerializeToString()
        yield query_pb2.SubscribeResponse(record=event(11, clocks=self.clocks)).SerializeToString()
        yield query_pb2.SubscribeResponse(
            records=[event(offset, clocks=self.clocks) for offset in range(12, 20)]
        ).SerializeToString()


async def stream_items(*, clocks: bool) -> list:
    gateway = FakeGateway(clocks=clocks)
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        "qdl.query.v2.MarketDataStreamService",
        {"Subscribe": grpc.unary_stream_rpc_method_handler(
            gateway.subscribe, request_deserializer=None, response_serializer=None)},
    ),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    transport = GrpcStreamTransport(
        f"127.0.0.1:{port}",
        allow_insecure_loopback=True,
        credential_provider=StaticBearerCredential("test-only"),
    )
    try:
        return [
            item
            async for item in transport.subscribe(
                DataRequirement("uid-test", Feed.TRADE, Grade.EXECUTION, "policy-test"),
                consumer_id="consumer-test",
                cursor_token="cursor-test",
            )
        ]
    finally:
        await transport.close()
        await server.stop(grace=0)


class GatewayClockTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_2_4_gateway_clocks_reach_each_stream_event(self):
        items = await stream_items(clocks=True)
        events = [item for item in items if isinstance(item, StreamEvent)]
        self.assertEqual([e.logical_offset for e in events], list(range(10, 20)))
        replayed, *live = events
        self.assertIsNone(replayed.gateway_dispatch_wall_ns, "0 on the wire is None")
        self.assertEqual(replayed.gateway_dequeue_wall_ns, BASE + 10_100)
        self.assertEqual(replayed.gateway_send_wall_ns, BASE + 900_000)
        for e in live:  # single and batched records alike, each with its own clocks
            offset = e.logical_offset
            self.assertEqual(
                (e.gateway_dispatch_wall_ns, e.gateway_dequeue_wall_ns, e.gateway_send_wall_ns),
                (BASE + offset * 1000, BASE + offset * 1000 + 100, BASE + 900_000),
            )
            self.assertIs(type(e.gateway_send_wall_ns), int)
        self.assertEqual(
            [item.code for item in items if isinstance(item, ControlEvent)], ["REPLAYING", "LIVE"]
        )

    async def test_an_older_gateway_leaves_every_clock_none_and_events_equal(self):
        stamped = await stream_items(clocks=True)
        plain = await stream_items(clocks=False)
        for e in plain:
            if isinstance(e, StreamEvent):
                self.assertEqual(
                    (e.gateway_dispatch_wall_ns, e.gateway_dequeue_wall_ns, e.gateway_send_wall_ns),
                    (None, None, None),
                )
        # Clocks are diagnostics, not identity: the same records compare equal.
        self.assertEqual(
            [i for i in stamped if isinstance(i, StreamEvent)],
            [i for i in plain if isinstance(i, StreamEvent)],
        )

    def test_positional_construction_is_unchanged(self):
        envelope = market_data_pb2.EventEnvelope(instrument_uid="uid-test")
        legacy = StreamEvent(7, "tok-7", envelope)
        self.assertIsNone(legacy.gateway_send_wall_ns)
        stamped = StreamEvent(7, "tok-7", envelope, gateway_send_wall_ns=BASE)
        self.assertEqual(legacy, stamped)
        self.assertEqual(stamped.gateway_send_wall_ns, BASE)

    def test_the_wire_fields_are_additive(self):
        fields = {f.name: f.number for f in query_pb2.StreamRecord.DESCRIPTOR.fields}
        self.assertEqual(fields["gateway_dispatch_wall_ns"], 3)
        self.assertEqual(fields["gateway_dequeue_wall_ns"], 4)
        self.assertEqual(fields["gateway_send_wall_ns"], 5)
        self.assertEqual(
            (fields["logical_offset"], fields["resume_token"], fields["event"], fields["control"]),
            (1, 2, 10, 11),
        )
        # Unset clocks (0) are not encoded: a record without them is the 2.3.0 bytes.
        stamped = event(11, clocks=True)
        for name in ("gateway_dispatch_wall_ns", "gateway_dequeue_wall_ns", "gateway_send_wall_ns"):
            stamped.ClearField(name)
        self.assertEqual(stamped.SerializeToString(), event(11, clocks=False).SerializeToString())


if __name__ == "__main__":
    unittest.main()
