from __future__ import annotations

import ipaddress
from collections.abc import AsyncIterator
from urllib.parse import urlsplit

import grpc
import httpx

from qdl.query.v2 import query_pb2
from qdl_sdk.errors import CursorExpiredError, DataLayerError, SlowConsumerError
from qdl_sdk.models import DataRequirement, StreamEvent


class RestQueryTransport:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("query timeout must be positive")
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    async def warmup(self, requirement: DataRequirement, *, consumer_id: str) -> dict:
        del consumer_id
        response = await self._client.get(
            f"/v2/market-data/{requirement.instrument_uid}/warmup",
            params=requirement.query_params(),
            headers={"X-QDL-Purpose": self._purpose(requirement)},
        )
        return self._decode(response)

    async def snapshot(self, requirement: DataRequirement, *, consumer_id: str) -> dict:
        del consumer_id
        response = await self._client.get(
            f"/v2/market-data/{requirement.instrument_uid}/snapshot",
            params=requirement.query_params(),
            headers={"X-QDL-Purpose": self._purpose(requirement)},
        )
        return self._decode(response)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    @staticmethod
    def _purpose(requirement: DataRequirement) -> str:
        return {
            "EXECUTION": "INTERNAL_EXECUTION",
            "ALPHA": "INTERNAL_ALPHA",
            "RESEARCH": "INTERNAL_RESEARCH",
        }[requirement.consumer_grade]

    @staticmethod
    def _decode(response: httpx.Response) -> dict:
        payload = response.json()
        if response.is_success:
            return payload
        raise DataLayerError(
            str(payload.get("code", "INTERNAL_ERROR")),
            str(payload.get("detail", response.text)),
            retryable=bool(payload.get("retryable", False)),
            retry_after_ms=payload.get("retry_after_ms"),
        )


class GrpcStreamTransport:
    def __init__(
        self,
        target: str,
        *,
        credentials: grpc.ChannelCredentials | None = None,
        allow_insecure_loopback: bool = False,
    ) -> None:
        if not target.strip():
            raise ValueError("gRPC stream target is required")
        self.target = target
        if credentials is None:
            if not allow_insecure_loopback or not self._is_loopback(target):
                raise ValueError("insecure gRPC is allowed only for explicit loopback tests")
            self._channel = grpc.aio.insecure_channel(target)
        else:
            self._channel = grpc.aio.secure_channel(target, credentials)
        self._subscribe = self._channel.unary_stream(
            "/qdl.query.v2.MarketDataStreamService/Subscribe",
            request_serializer=query_pb2.SubscribeRequest.SerializeToString,
            response_deserializer=query_pb2.SubscribeResponse.FromString,
        )

    async def subscribe(
        self,
        requirement: DataRequirement,
        *,
        consumer_id: str,
        stream: str,
        partition_key: str,
        cursor_token: str,
        max_buffer_events: int = 1000,
    ) -> AsyncIterator[StreamEvent]:
        request = query_pb2.SubscribeRequest(
            consumer_id=consumer_id,
            requirement=requirement.to_proto(),
            stream=stream,
            partition_key=partition_key,
            cursor_token=cursor_token,
            max_buffer_events=max_buffer_events,
        )
        try:
            async for response in self._subscribe(request):
                record = response.record
                if record.WhichOneof("payload") != "event":
                    continue
                yield StreamEvent(record.logical_offset, record.resume_token, record.event)
        except grpc.aio.AioRpcError as error:
            detail = error.details() or "gRPC stream failed"
            if error.code() is grpc.StatusCode.OUT_OF_RANGE:
                raise CursorExpiredError("CURSOR_EXPIRED", detail, retryable=False) from error
            if error.code() is grpc.StatusCode.RESOURCE_EXHAUSTED:
                raise SlowConsumerError("RATE_LIMITED", detail, retryable=True) from error
            raise DataLayerError("DEPENDENCY_UNAVAILABLE", detail, retryable=True) from error

    async def close(self) -> None:
        await self._channel.close()

    @staticmethod
    def _is_loopback(target: str) -> bool:
        host = urlsplit(f"//{target}").hostname
        if host == "localhost":
            return True
        try:
            return bool(host and ipaddress.ip_address(host).is_loopback)
        except ValueError:
            return False
