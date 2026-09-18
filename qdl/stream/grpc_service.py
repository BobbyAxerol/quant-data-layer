from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

import grpc

from qdl.marketdata.v2 import market_data_pb2
from qdl.query import (
    AccessPurpose,
    DataRequirement,
    FeedType,
    QueryServiceError,
    StalePolicy,
    V2QueryService,
)
from qdl.query.v2 import query_pb2
from qdl.replay import ReplayGapError
from qdl.runtime.stable_catalog import canonical_payload_interval
from qdl.security import (
    DataPlaneAccessError,
    DataPlaneIdentityService,
    DataPlanePermission,
    GrpcDataPlaneInterceptor,
    current_grpc_data_access,
)
from qdl.runtime import GatewayFenced
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


class CursorScopeValidator(Protocol):
    def validate(
        self,
        requirement: DataRequirement,
        *,
        stream: str,
        partition_key: str,
    ) -> None: ...


class FeedScopedCursorScopeValidator:
    """Validate the original feed-specific canonical stream convention."""

    def validate(
        self,
        requirement: DataRequirement,
        *,
        stream: str,
        partition_key: str,
    ) -> None:
        parts = partition_key.split("/")
        if (
            len(parts) != 3
            or not parts[2]
            or parts[:2]
            != [requirement.instrument_uid, requirement.feed.value.lower()]
        ):
            raise ValueError("cursor scope does not match the data requirement")
        if stream != f"md.canonical.v2.{requirement.feed.value.lower()}":
            raise ValueError("cursor stream does not match the data requirement")


def requirement_from_proto(value: query_pb2.DataRequirement) -> DataRequirement:
    def enum_value(number: int, enum_wrapper, prefix: str) -> str:
        name = enum_wrapper.Name(number)
        if name.endswith("_UNSPECIFIED"):
            raise ValueError(f"{prefix.lower()} cannot be UNSPECIFIED")
        return name.removeprefix(prefix)

    mapping = {
        "instrument_uid": value.instrument_uid,
        "feed": enum_value(value.feed_type, query_pb2.FeedType, "FEED_TYPE_"),
        "interval": value.interval or None,
        "consumer_grade": enum_value(
            value.grade, query_pb2.ConsumerGrade, "CONSUMER_GRADE_"
        ),
        "source_policy_id": value.source_policy_id,
        "warmup_limit": value.warmup_limit,
        "max_freshness_ms": value.max_freshness_ms or None,
        "event_recency_policy": (
            None
            if value.event_recency_policy == query_pb2.STALE_POLICY_UNSPECIFIED
            else enum_value(
                value.event_recency_policy,
                query_pb2.StalePolicy,
                "STALE_POLICY_",
            )
        ),
        "max_session_liveness_ms": value.max_session_liveness_ms or None,
        "require_full_coverage": value.require_full_coverage,
        "require_final_bars": value.require_final_bars,
        "stale_policy": enum_value(
            value.stale_policy_type, query_pb2.StalePolicy, "STALE_POLICY_"
        ),
        "gap_policy": enum_value(
            value.gap_policy_type, query_pb2.GapPolicy, "GAP_POLICY_"
        ),
        "recovery": enum_value(
            value.recovery_policy, query_pb2.RecoveryPolicy, "RECOVERY_POLICY_"
        ),
        "bar_revision_policy": enum_value(
            value.revision_policy,
            query_pb2.BarRevisionPolicy,
            "BAR_REVISION_POLICY_",
        ),
    }
    if value.HasField("warmup"):
        horizon = value.warmup.WhichOneof("horizon")
        if horizon is None:
            raise ValueError("warmup horizon is required")
        if (
            value.warmup.interval_source_policy
            == query_pb2.INTERVAL_SOURCE_POLICY_UNSPECIFIED
        ):
            raise ValueError("warmup interval source policy is required")
        warmup = {
            "rows": value.warmup.rows if horizon == "rows" else None,
            "time_range": (
                {
                    "start_time_ns": value.warmup.time_range.start_time_ns,
                    "end_time_ns": value.warmup.time_range.end_time_ns,
                }
                if horizon == "time_range"
                else None
            ),
            "interval_source_policy": query_pb2.IntervalSourcePolicy.Name(
                value.warmup.interval_source_policy
            ).removeprefix("INTERVAL_SOURCE_POLICY_"),
            "max_cache_age_ms": value.warmup.max_cache_age_ms,
            "deadline_ms": value.warmup.deadline_ms,
        }
        mapping["warmup"] = warmup
    return DataRequirement.from_mapping(mapping)


# DL-V2 R1.8. Which demanded feeds are latest-state, so a full bounded buffer
# keeps the newest record rather than the oldest. This mirrors
# `qdl/ingestion/contracts.py:delivery_policy`, which already declares every
# non-lossless feed LATEST_STATE; it is written out explicitly here because the
# stream layer carries the query FeedType, not the ingestion one, and a wrong
# entry would silently drop records from a feed that must not lose any.
#
# Never add TRADE, BAR or BOOK_DELTA: a trade is an economic event, a final bar
# is a settled fact, and a book delta is only meaningful in sequence.
LATEST_STATE_FEEDS = frozenset({
    FeedType.QUOTE,
    FeedType.BOOK_SNAPSHOT,
    FeedType.MARK_INDEX_PRICE,
    FeedType.TICKER,
    FeedType.FUNDING_RATE,
    FeedType.OPEN_INTEREST,
    FeedType.BASIS,
    FeedType.LONG_SHORT_RATIO,
    FeedType.TAKER_FLOW,
    FeedType.CONTRACT_METADATA,
})


logger = logging.getLogger(__name__)

# One line per this many freshness checks on a feed/interval. Quote runs at
# roughly 15 records a second per instrument, so this is a line every few
# minutes per feed rather than per record.
_FRESHNESS_REPORT_EVERY = 2_000


class GrpcMarketDataService:
    def __init__(
        self,
        *,
        gateway: DurableStreamGateway,
        query_service: V2QueryService,
        snapshot_loader: SnapshotLoader,
        cursor_scope_validator: CursorScopeValidator | None = None,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.gateway = gateway
        self.query_service = query_service
        self.snapshot_loader = snapshot_loader
        self._clock_ns = clock_ns
        # R1.29. Freshness outcomes per feed/interval/bound, reset each time
        # they are reported, so a line describes its own interval.
        self._freshness: dict[tuple[str, str, int], dict[str, int]] = {}
        self.cursor_scope_validator = (
            cursor_scope_validator or FeedScopedCursorScopeValidator()
        )

    @staticmethod
    def _event(stored: StoredEvent, token: str) -> query_pb2.StreamRecord:
        envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
        return query_pb2.StreamRecord(
            logical_offset=stored.cursor.offset,
            resume_token=token,
            event=envelope,
        )

    def _matches_requirement(
        self, stored: StoredEvent, requirement: DataRequirement,
    ) -> bool:
        """Keep product identity and strict freshness exact before delivery."""

        envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
        if (
            envelope.WhichOneof("payload") != requirement.feed.value.lower()
            or canonical_payload_interval(envelope) != requirement.interval
        ):
            return False
        if (
            requirement.max_freshness_ms is None
            or requirement.effective_event_recency_policy
            not in {StalePolicy.BLOCK, StalePolicy.PAUSE}
        ):
            return True
        observed_at_ns = (
            int(envelope.bar.close_time_ns)
            if requirement.feed.value == "BAR"
            else int(envelope.source_event_time_ns)
        )
        age_ns = self._clock_ns() - observed_at_ns
        bound_ns = requirement.max_freshness_ms * 1_000_000
        # DL-V2 R1.29. Knowing a record was refused says nothing about what to
        # change. The distance to the bound does: a feed missing by 40 ms is a
        # pipeline to tune, a feed missing by seconds is a different fault, and
        # a feed that never misses proves this predicate is not the reason a
        # slice looks stale. Summarised per feed and interval so one busy
        # instrument cannot drown the rest.
        self._observe_freshness(requirement, age_ns, bound_ns)
        return age_ns <= bound_ns

    def _observe_freshness(
        self, requirement: DataRequirement, age_ns: int, bound_ns: int
    ) -> None:
        key = (
            requirement.feed.value,
            requirement.interval or "-",
            requirement.max_freshness_ms,
        )
        summary = self._freshness.get(key)
        if summary is None:
            summary = {"checked": 0, "refused": 0, "age_sum_ns": 0, "worst_ns": 0}
            self._freshness[key] = summary
        summary["checked"] += 1
        summary["age_sum_ns"] += age_ns
        summary["worst_ns"] = max(summary["worst_ns"], age_ns)
        if age_ns > bound_ns:
            summary["refused"] += 1
        if summary["checked"] % _FRESHNESS_REPORT_EVERY:
            return
        logger.info(
            "qdl_stream_freshness %s",
            json.dumps(
                {
                    "event": "qdl_stream_freshness",
                    "feed": key[0],
                    "interval": key[1],
                    "max_freshness_ms": key[2],
                    "checked": summary["checked"],
                    "refused": summary["refused"],
                    "mean_age_ms": round(
                        summary["age_sum_ns"] / summary["checked"] / 1e6, 1
                    ),
                    "worst_age_ms": round(summary["worst_ns"] / 1e6, 1),
                },
                sort_keys=True,
            ),
        )
        self._freshness[key] = {
            "checked": 0, "refused": 0, "age_sum_ns": 0, "worst_ns": 0,
        }

    async def subscribe(self, request: query_pb2.SubscribeRequest, context):
        subscription = None
        stream = ""
        partition_key = ""
        try:
            requirement = requirement_from_proto(request.requirement)
            request_access = current_grpc_data_access()
            request_access.access.require_consumer(request.consumer_id)
            request_access.access.require_permission(DataPlanePermission.STREAM_READ)
            request_access.access.require_requirement(requirement)
            buffer_events = (
                request.max_buffer_events
                or request_access.access.manifest.quotas.max_buffer_events
            )
            request_access.access.require_stream_buffer(buffer_events)
            scope = self.gateway.handoff.resolve_scope(
                token=request.cursor_token, consumer_id=request.consumer_id
            )
            stream, partition_key = scope.stream, scope.partition_key
            self.cursor_scope_validator.validate(
                requirement, stream=stream, partition_key=partition_key
            )
            subscription = await self.gateway.open(
                consumer_id=request.consumer_id,
                stream=stream,
                partition_key=partition_key,
                token=request.cursor_token,
                max_buffer_events=buffer_events,
                max_consumer_streams=request_access.access.manifest.quotas.max_streams,
                replay_limit=self.gateway.max_replay_events,
                accepts=lambda stored: self._matches_requirement(stored, requirement),
                coalesce=requirement.feed in LATEST_STATE_FEEDS,
            )
            high = (await self.gateway.capture_watermark(
                stream=stream, partition_key=partition_key
            )).offset
            yield query_pb2.SubscribeResponse(record=query_pb2.StreamRecord(
                resume_token=request.cursor_token,
                control=query_pb2.StreamControl(
                    state=query_pb2.STREAM_CONTROL_STATE_REPLAYING,
                    code="REPLAYING",
                    detail="replaying durable records after the supplied cursor",
                    high_watermark=high,
                ),
            ))
            # Replay advances the resume token past every physical record it
            # sees, matched or not, so a resumed consumer never re-reads a run
            # it already skipped. Signing a cursor per record costs a durable
            # read, so an unmatched run is collapsed: the token is advanced once
            # at the end of the run instead of once per record. The delivered
            # records keep their own per-record tokens, so nothing a consumer
            # resumes from changes.
            pending_skip = None
            for stored in subscription.initial:
                if not subscription.accepts(stored):
                    pending_skip = stored
                    continue
                if pending_skip is not None:
                    await subscription.record(pending_skip)
                    pending_skip = None
                record = await subscription.record(stored)
                yield query_pb2.SubscribeResponse(
                    record=self._event(record.stored, record.resume_token)
                )
            if pending_skip is not None:
                await subscription.record(pending_skip)
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
                subscription.mark_delivered()
        except SlowConsumer as error:
            yield query_pb2.SubscribeResponse(record=query_pb2.StreamRecord(
                resume_token=subscription.token if subscription else request.cursor_token,
                control=query_pb2.StreamControl(
                    state=query_pb2.STREAM_CONTROL_STATE_BACKPRESSURE,
                    code="RATE_LIMITED",
                    detail=str(error),
                    high_watermark=(await self.gateway.capture_watermark(
                        stream=stream, partition_key=partition_key
                    )).offset,
                ),
            ))
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, f"RATE_LIMITED:{error}")
        except StreamCapacityExceeded as error:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, f"RATE_LIMITED:{error}")
        except GatewayFenced as error:
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"GATEWAY_FENCED:{error}")
        except CursorExpired as error:
            await context.abort(grpc.StatusCode.OUT_OF_RANGE, f"CURSOR_EXPIRED:{error}")
        except (ValueError, ReplayGapError) as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"CURSOR_INVALID:{error}")
        except DataPlaneAccessError as error:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, error.detail)
        finally:
            if subscription is not None:
                await subscription.close()

    async def replay(self, request: query_pb2.ReplayRequest, context):
        token = request.cursor_token
        try:
            request_access = current_grpc_data_access()
            request_access.access.require_consumer(request.consumer_id)
            request_access.access.require_permission(DataPlanePermission.STREAM_READ)
            scope = self.gateway.handoff.resolve_scope(
                token=token, consumer_id=request.consumer_id
            )
            partition_parts = scope.partition_key.split("/", 2)
            if len(partition_parts) != 3:
                raise ValueError("cursor partition scope is invalid")
            request_access.access.require_feed_scope(
                instrument_uid=partition_parts[0],
                feed=partition_parts[1].upper(),
            )
            replay_limit = request.limit or 1000
            request_access.access.require_stream_buffer(replay_limit)
            records = await self.gateway.replay(
                token=token,
                consumer_id=request.consumer_id,
                stream=scope.stream,
                partition_key=scope.partition_key,
                limit=replay_limit,
            )
            for stored in records:
                grant = await self.gateway.advance_token(
                    token=token,
                    consumer_id=request.consumer_id,
                    cursor=stored.cursor,
                )
                token = grant.token
                yield query_pb2.ReplayResponse(record=self._event(stored, token))
        except CursorExpired as error:
            await context.abort(grpc.StatusCode.OUT_OF_RANGE, f"CURSOR_EXPIRED:{error}")
        except (ValueError, ReplayGapError) as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"CURSOR_INVALID:{error}")
        except DataPlaneAccessError as error:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, error.detail)
        except GatewayFenced as error:
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"GATEWAY_FENCED:{error}")

    async def get_snapshot(self, request: query_pb2.GetSnapshotRequest, context):
        try:
            requirement = requirement_from_proto(request.requirement)
            request_access = current_grpc_data_access()
            request_access.access.require_consumer(request.consumer_id)
            request_access.access.require_permission(
                DataPlanePermission.HISTORY_READ
                if requirement.warmup_specification is not None
                else DataPlanePermission.SNAPSHOT_READ
            )
            request_access.access.require_requirement(requirement)
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
        except DataPlaneAccessError as error:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, error.detail)

    async def get_feed_status(self, request: query_pb2.GetFeedStatusRequest, context):
        try:
            requirement = requirement_from_proto(request.requirement)
            request_access = current_grpc_data_access()
            request_access.access.require_consumer(request.consumer_id)
            request_access.access.require_permission(DataPlanePermission.STATUS_READ)
            request_access.access.require_requirement(requirement)
            status = self.query_service.status(requirement)
            return query_pb2.GetFeedStatusResponse(
                state=status.state,
                freshness_ms=status.freshness_ms,
                gap_open=status.gap_open,
                complete=status.complete,
                execution_eligible=status.execution_eligible,
                policy_id=status.policy_id,
                flags=status.flags,
                event_recency_state=status.event_recency_state,
                provider_session_state=status.provider_session_state,
                provider_session_liveness_ms=(
                    status.provider_session_liveness_ms or 0
                ),
            )
        except QueryServiceError as error:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"{error.problem.code.value}:{error.problem.detail}",
            )
        except ValueError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"INVALID_ARGUMENT:{error}")
        except DataPlaneAccessError as error:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, error.detail)


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
    identity_service: DataPlaneIdentityService,
    maximum_concurrent_rpcs: int = 10_000,
    max_receive_message_bytes: int = 1_048_576,
    max_send_message_bytes: int = 4_194_304,
) -> grpc.aio.Server:
    if min(
        maximum_concurrent_rpcs,
        max_receive_message_bytes,
        max_send_message_bytes,
    ) <= 0:
        raise ValueError("gRPC concurrency and message bounds must be positive")
    server = grpc.aio.server(
        interceptors=(GrpcDataPlaneInterceptor(identity_service),),
        maximum_concurrent_rpcs=maximum_concurrent_rpcs,
        options=(
            ("grpc.max_receive_message_length", max_receive_message_bytes),
            ("grpc.max_send_message_length", max_send_message_bytes),
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 5_000),
            ("grpc.http2.max_pings_without_data", 2),
        ),
    )
    add_market_data_service(server, service)
    return server
