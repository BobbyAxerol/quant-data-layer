"""SDK 2.7.0 session-alive frames (Data Layer v2.3.2, ``qdl.stream.session-alive.v1``).

A loopback grpc.aio server plays the gateway and sends ``StreamRecord.session_alive``
(field 12) records whatever the request says, so both sides of the compatibility rule
are pinned: a client that never set ``SubscribeRequest.session_alive_interval_ms``
(field 9) sends the pre-2.7.0 request bytes and ignores field 12; a client that set it
yields ``SessionAliveEvent`` in stream order. The gateway rules (route, clamp, fail
closed, bounded handoff) are pinned in ``rust/qdl-stream-gateway``.
"""

from __future__ import annotations

import unittest

import grpc

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.query.v2 import query_pb2
from qdl_sdk import (
    DataRequirement,
    Feed,
    Grade,
    GrpcStreamTransport,
    SessionAliveEvent,
    StaticBearerCredential,
)
from qdl_sdk.errors import DataLayerError
from qdl_sdk.models import ControlEvent, StreamEvent

SERVICE = "qdl.query.v2.MarketDataStreamService"


def control(code: str) -> query_pb2.StreamRecord:
    state = {
        "REPLAYING": query_pb2.STREAM_CONTROL_STATE_REPLAYING,
        "LIVE": query_pb2.STREAM_CONTROL_STATE_LIVE,
    }[code]
    return query_pb2.StreamRecord(
        resume_token=f"tok-{code}",
        control=query_pb2.StreamControl(state=state, code=code, detail=f"{code} detail"),
    )


def event(offset: int) -> query_pb2.StreamRecord:
    return query_pb2.StreamRecord(
        logical_offset=offset,
        resume_token=f"tok-{offset}",
        event=market_data_pb2.EventEnvelope(
            instrument_uid="uid-test",
            source_session_id="okx-public-7",
            connection_generation=3,
            config_revision=9,
            quote=market_data_pb2.Quote(level=1),
            quality_flags=[common_pb2.QUALITY_FLAG_DELIVERY_ON_CHANGE],
        ),
    )


def frame(token: str, liveness_ms: int) -> query_pb2.StreamRecord:
    return query_pb2.StreamRecord(
        logical_offset=0,
        resume_token=token,
        session_alive=query_pb2.SessionAlive(
            source_session_id="okx-public-7",
            connection_generation=3,
            config_revision=9,
            provider_last_transport_at_ns=1_800_000_000_000_000_000,
            provider_session_liveness_ms=liveness_ms,
            gateway_epoch=42,
            high_watermark=11,
        ),
    )


class FakeGateway:
    """Sends frames to every client, as a broken server would; or refuses."""

    def __init__(self, *, refuse: str | None = None) -> None:
        self.refuse = refuse
        self.requests: list[bytes] = []

    async def subscribe(self, raw: bytes, context):
        self.requests.append(raw)
        if self.refuse is not None:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, self.refuse)
        records = (
            control("REPLAYING"),
            control("LIVE"),
            frame("tok-LIVE", 120),
            event(11),
            frame("tok-11", 260),
            frame("tok-11", 510),
        )
        for record in records:
            yield query_pb2.SubscribeResponse(record=record).SerializeToString()


async def serve(gateway: FakeGateway):
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        SERVICE,
        {"Subscribe": grpc.unary_stream_rpc_method_handler(
            gateway.subscribe, request_deserializer=None, response_serializer=None)},
    ),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, port


def requirement() -> DataRequirement:
    return DataRequirement("uid-test", Feed.QUOTE, Grade.EXECUTION, "policy-test")


class SessionAliveTransportTests(unittest.IsolatedAsyncioTestCase):
    async def run_stream(self, gateway: FakeGateway, **kwargs):
        server, port = await serve(gateway)
        transport = GrpcStreamTransport(
            f"127.0.0.1:{port}",
            allow_insecure_loopback=True,
            credential_provider=StaticBearerCredential("test-only"),
            max_records_per_response=1,
            **kwargs,
        )
        try:
            return [
                item
                async for item in transport.subscribe(
                    requirement(),
                    consumer_id="consumer-test",
                    cursor_token="cursor-test",
                    max_buffer_events=8,
                )
            ]
        finally:
            await transport.close()
            await server.stop(grace=0)

    async def test_default_client_sends_pre_2_7_bytes_and_ignores_field_12(self):
        gateway = FakeGateway()
        items = await self.run_stream(gateway)
        legacy = query_pb2.SubscribeRequest(
            consumer_id="consumer-test",
            requirement=requirement().to_proto(),
            cursor_token="cursor-test",
            max_buffer_events=8,
            replay_budget_status_v2=True,
        ).SerializeToString()
        self.assertEqual(gateway.requests[0], legacy, "field 9 absent, as in 2.6.0")
        self.assertEqual(
            [type(item).__name__ for item in items],
            ["ControlEvent", "ControlEvent", "StreamEvent"],
        )

    async def test_requested_frames_are_yielded_in_stream_order(self):
        gateway = FakeGateway()
        items = await self.run_stream(gateway, session_alive_interval_ms=500)
        request = query_pb2.SubscribeRequest.FromString(gateway.requests[0])
        self.assertEqual(request.session_alive_interval_ms, 500)
        kinds = [type(item).__name__ for item in items]
        self.assertEqual(
            kinds,
            ["ControlEvent", "ControlEvent", "SessionAliveEvent", "StreamEvent",
             "SessionAliveEvent", "SessionAliveEvent"],
        )
        first = items[2]
        self.assertIsInstance(first, SessionAliveEvent)
        self.assertEqual(
            first,
            SessionAliveEvent(
                resume_token="tok-LIVE",
                source_session_id="okx-public-7",
                connection_generation=3,
                config_revision=9,
                provider_last_transport_at_ns=1_800_000_000_000_000_000,
                provider_session_liveness_ms=120,
                gateway_epoch=42,
                high_watermark=11,
            ),
        )
        # A frame carries the current token unchanged and is not a StreamEvent.
        self.assertEqual(items[4].resume_token, items[3].resume_token)
        self.assertFalse(any(isinstance(item, StreamEvent) for item in items[4:]))
        self.assertTrue(all(isinstance(item, ControlEvent) for item in items[:2]))

    async def test_delivery_on_change_opt_in_sends_field_10_only_when_true(self):
        gateway = FakeGateway()
        items = await self.run_stream(gateway, delivery_on_change_flag=True)
        request = query_pb2.SubscribeRequest.FromString(gateway.requests[0])
        self.assertIs(request.delivery_on_change_flag, True)
        (event_item,) = [item for item in items if isinstance(item, StreamEvent)]
        self.assertIn(common_pb2.QUALITY_FLAG_DELIVERY_ON_CHANGE, event_item.event.quality_flags)
        gateway = FakeGateway()
        await self.run_stream(gateway)
        request = query_pb2.SubscribeRequest.FromString(gateway.requests[0])
        self.assertIs(request.delivery_on_change_flag, False)
        field = query_pb2.SubscribeRequest.DESCRIPTOR.fields_by_name["delivery_on_change_flag"]
        self.assertEqual((field.number, field.type), (10, field.TYPE_BOOL))
        for bad in (1, "true", None):
            with self.subTest(value=bad), self.assertRaises(ValueError):
                GrpcStreamTransport(
                    "127.0.0.1:1", allow_insecure_loopback=True, delivery_on_change_flag=bad
                )

    async def test_refused_request_keeps_the_session_alive_code(self):
        gateway = FakeGateway(
            refuse="SESSION_ALIVE_INVALID:session_alive_interval_ms requires an event-recency "
            "OBSERVE QUOTE or TRADE requirement"
        )
        with self.assertRaises(DataLayerError) as caught:
            await self.run_stream(gateway, session_alive_interval_ms=250)
        self.assertEqual(caught.exception.code, "SESSION_ALIVE_INVALID")
        self.assertFalse(caught.exception.retryable)

    async def test_interval_is_validated_on_the_client(self):
        for bad in (1, 249, 501, True, "500", 500.0, -1):
            with self.subTest(value=bad), self.assertRaises(ValueError):
                GrpcStreamTransport(
                    "127.0.0.1:1",
                    allow_insecure_loopback=True,
                    session_alive_interval_ms=bad,
                )
        for good in (0, 250, 500):
            transport = GrpcStreamTransport(
                "127.0.0.1:1", allow_insecure_loopback=True, session_alive_interval_ms=good
            )
            self.assertEqual(transport.session_alive_interval_ms, good)
            await transport.close()


class SessionAliveContractTests(unittest.TestCase):
    def test_wire_numbers_are_the_published_contract(self):
        request = query_pb2.SubscribeRequest.DESCRIPTOR.fields_by_name
        self.assertEqual(request["session_alive_interval_ms"].number, 9)
        self.assertEqual(
            request["session_alive_interval_ms"].type,
            request["session_alive_interval_ms"].TYPE_UINT32,
        )
        record = query_pb2.StreamRecord.DESCRIPTOR
        self.assertEqual(record.fields_by_name["session_alive"].number, 12)
        self.assertEqual(
            [field.name for field in record.oneofs_by_name["payload"].fields],
            ["event", "control", "session_alive"],
        )
        alive = query_pb2.SessionAlive.DESCRIPTOR
        self.assertEqual(
            [(field.name, field.number, field.type) for field in alive.fields],
            [
                ("source_session_id", 1, alive.fields[0].TYPE_STRING),
                ("connection_generation", 2, alive.fields[0].TYPE_UINT64),
                ("config_revision", 3, alive.fields[0].TYPE_UINT32),
                ("provider_last_transport_at_ns", 4, alive.fields[0].TYPE_UINT64),
                ("provider_session_liveness_ms", 5, alive.fields[0].TYPE_UINT64),
                ("gateway_epoch", 6, alive.fields[0].TYPE_UINT64),
                ("high_watermark", 7, alive.fields[0].TYPE_UINT64),
            ],
        )

    def test_a_pre_2_7_reader_sees_no_payload_it_knows(self):
        # What an older generated reader does with field 12 is what proto3 does
        # with any unknown field: keeps the record, sets neither event nor
        # control. Model that reader with a record that drops field 12.
        wire = frame("tok-1", 100).SerializeToString()
        old = query_pb2.StreamRecord.FromString(wire)
        old.ClearField("session_alive")
        self.assertIsNone(old.WhichOneof("payload"))
        self.assertEqual(old.resume_token, "tok-1")


if __name__ == "__main__":
    unittest.main()
