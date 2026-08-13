from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import grpc

from qdl.marketdata.v2 import market_data_pb2
from qdl.query import AccessPurpose, DataRequirement, QueryServiceError, V2QueryService
from qdl.query.v2 import query_pb2
from qdl.replay import ReplayGapError
from qdl.stream.gateway import DurableStreamGateway, SlowConsumer, StreamCapacityExceeded
from qdl.transport import CursorExpired, StoredEvent


SERVICE_NAME = "qdl.query.v2.MarketDataStreamService"


@dataclass(frozen=True)
class GrpcSnapshot:
    request_id: str
    snapshot_id: str
    stream_cursor: str
    data_as_of_ns: int
    watermark_offset: int
    events: tuple[market_data_pb2.EventEnvelope, ...]


class SnapshotLoader(Protocol):
    def load(self, requirement: DataRequirement, *, consumer_id: str) -> GrpcSnapshot: ...


def requirement_from_proto(value: query_pb2.DataRequirement) -> DataRequirement:
    return DataRequirement.from_mapping({
        "instrument_uid": value.instrument_uid,
        "feed": value.feed,
        "interval": value.interval or None,
        "consumer_grade": value.consumer_grade,
        "source_policy_id": value.source_policy_id,
        "warmup_limit": value.warmup_limit,
        "max_freshness_ms": value.max_freshness_ms or None,
        "require_full_coverage": value.require_full_coverage,
        "require_final_bars": value.require_final_bars,
        "stale_policy": value.stale_policy,
        "gap_policy": value.gap_policy,
        "recovery": value.recovery,
        "bar_revision_policy": value.bar_revision_policy,
    })


class GrpcMarketDataService:
    def __init__(
        self,
        *,
        gateway: DurableStreamGateway,
        query_service: V2QueryService,
        snapshot_loader: SnapshotLoader,
    ) -> None:
        self.gateway = gateway
        self.query_service = query_service
        self.snapshot_loader = snapshot_loader

    @staticmethod
    def _event(stored: StoredEvent, token: str) -> query_pb2.StreamRecord:
        envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
        return query_pb2.StreamRecord(
            logical_offset=stored.cursor.offset,
            resume_token=token,
            event=envelope,
        )

    async def subscribe(self, request: query_pb2.SubscribeRequest, context):
        subscription = None
        try:
            requirement_from_proto(request.requirement)
            subscription = await self.gateway.open(
                consumer_id=request.consumer_id,
                stream=request.stream,
                partition_key=request.partition_key,
                token=request.cursor_token,
                max_buffer_events=request.max_buffer_events or None,
            )
            high = self.gateway.handoff.capture_watermark(
                stream=request.stream, partition_key=request.partition_key
            ).offset
            yield query_pb2.SubscribeResponse(record=query_pb2.StreamRecord(
                resume_token=request.cursor_token,
                control=query_pb2.StreamControl(
                    state=query_pb2.STREAM_CONTROL_STATE_REPLAYING,
                    code="REPLAYING",
                    detail="replaying durable records after the supplied cursor",
                    high_watermark=high,
                ),
            ))
            for stored in subscription.initial:
                record = subscription.record(stored)
                yield query_pb2.SubscribeResponse(
                    record=self._event(record.stored, record.resume_token)
                )
            yield query_pb2.SubscribeResponse(record=query_pb2.StreamRecord(
                resume_token=subscription.token,
                control=query_pb2.StreamControl(
                    state=query_pb2.STREAM_CONTROL_STATE_LIVE,
                    code="LIVE",
                    detail="durable replay is complete; live delivery is active",
                    high_watermark=high,
                ),
            ))
            while True:
                record = await subscription.next_live()
                yield query_pb2.SubscribeResponse(
                    record=self._event(record.stored, record.resume_token)
                )
        except SlowConsumer as error:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, f"RATE_LIMITED:{error}")
        except StreamCapacityExceeded as error:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, f"RATE_LIMITED:{error}")
        except CursorExpired as error:
            await context.abort(grpc.StatusCode.OUT_OF_RANGE, f"CURSOR_EXPIRED:{error}")
        except (ValueError, ReplayGapError) as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"CURSOR_INVALID:{error}")
        finally:
            if subscription is not None:
                await subscription.close()

    async def replay(self, request: query_pb2.ReplayRequest, context):
        token = request.cursor_token
        try:
            records = self.gateway.handoff.replay(
                token=token,
                consumer_id=request.consumer_id,
                stream=request.stream,
                partition_key=request.partition_key,
                limit=request.limit or 1000,
            )
            for stored in records:
                grant = self.gateway.handoff.advance_token(
                    token=token,
                    consumer_id=request.consumer_id,
                    cursor=stored.cursor,
                    ttl_seconds=self.gateway.cursor_ttl_seconds,
                )
                token = grant.token
                yield query_pb2.ReplayResponse(record=self._event(stored, token))
        except CursorExpired as error:
            await context.abort(grpc.StatusCode.OUT_OF_RANGE, f"CURSOR_EXPIRED:{error}")
        except (ValueError, ReplayGapError) as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"CURSOR_INVALID:{error}")

    async def get_snapshot(self, request: query_pb2.GetSnapshotRequest, context):
        try:
            requirement = requirement_from_proto(request.requirement)
            snapshot = self.snapshot_loader.load(requirement, consumer_id=request.consumer_id)
            return query_pb2.GetSnapshotResponse(
                request_id=snapshot.request_id,
                snapshot_id=snapshot.snapshot_id,
                stream_cursor=snapshot.stream_cursor,
                data_as_of_ns=snapshot.data_as_of_ns,
                watermark_offset=snapshot.watermark_offset,
                events=snapshot.events,
            )
        except QueryServiceError as error:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"{error.problem.code.value}:{error.problem.detail}",
            )
        except ValueError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"INVALID_ARGUMENT:{error}")

    async def get_feed_status(self, request: query_pb2.GetFeedStatusRequest, context):
        try:
            status = self.query_service.status(requirement_from_proto(request.requirement))
            return query_pb2.GetFeedStatusResponse(
                state=status.state,
                freshness_ms=status.freshness_ms,
                gap_open=status.gap_open,
                complete=status.complete,
                execution_eligible=status.execution_eligible,
                policy_id=status.policy_id,
                flags=status.flags,
            )
        except QueryServiceError as error:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"{error.problem.code.value}:{error.problem.detail}",
            )
        except ValueError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"INVALID_ARGUMENT:{error}")


def add_market_data_service(server: grpc.aio.Server, service: GrpcMarketDataService) -> None:
    handlers = {
        "Subscribe": grpc.unary_stream_rpc_method_handler(
            service.subscribe,
            request_deserializer=query_pb2.SubscribeRequest.FromString,
            response_serializer=query_pb2.SubscribeResponse.SerializeToString,
        ),
        "Replay": grpc.unary_stream_rpc_method_handler(
            service.replay,
            request_deserializer=query_pb2.ReplayRequest.FromString,
            response_serializer=query_pb2.ReplayResponse.SerializeToString,
        ),
        "GetSnapshot": grpc.unary_unary_rpc_method_handler(
            service.get_snapshot,
            request_deserializer=query_pb2.GetSnapshotRequest.FromString,
            response_serializer=query_pb2.GetSnapshotResponse.SerializeToString,
        ),
        "GetFeedStatus": grpc.unary_unary_rpc_method_handler(
            service.get_feed_status,
            request_deserializer=query_pb2.GetFeedStatusRequest.FromString,
            response_serializer=query_pb2.GetFeedStatusResponse.SerializeToString,
        ),
    }
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(SERVICE_NAME, handlers),))


def create_grpc_server(
    service: GrpcMarketDataService,
    *,
    maximum_concurrent_rpcs: int = 10_000,
) -> grpc.aio.Server:
    if maximum_concurrent_rpcs <= 0:
        raise ValueError("maximum_concurrent_rpcs must be positive")
    server = grpc.aio.server(maximum_concurrent_rpcs=maximum_concurrent_rpcs)
    add_market_data_service(server, service)
    return server
