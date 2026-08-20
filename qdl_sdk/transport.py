from __future__ import annotations

import ipaddress
from collections.abc import AsyncIterator, Sequence
from urllib.parse import urlsplit

import grpc
import httpx

from qdl.query.v2 import query_pb2
from qdl_sdk.credentials import CredentialProvider
from qdl_sdk.errors import CursorExpiredError, DataLayerError, SlowConsumerError
from qdl_sdk.models import ControlEvent, DataRequirement, Grade, StreamEvent
from qdl_sdk.tls import WorkloadTlsConfig


class RestQueryTransport:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
        credential_provider: CredentialProvider | None = None,
        tls: WorkloadTlsConfig | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("query timeout must be positive")
        self.base_url = base_url.rstrip("/")
        if client is not None and tls is not None:
            raise ValueError("provide either REST client or workload TLS, not both")
        self._owns_client = client is None
        self._credential_provider = credential_provider
        self._client = client or httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            verify=tls.ssl_context() if tls is not None else True,
        )

    async def warmup(self, requirement: DataRequirement, *, consumer_id: str) -> dict:
        headers = await self._headers(requirement, consumer_id)
        response = await self._client.get(
            f"/v2/market-data/{requirement.instrument_uid}/warmup",
            params=requirement.query_params(),
            headers=headers,
        )
        return self._decode(response)

    async def snapshot(self, requirement: DataRequirement, *, consumer_id: str) -> dict:
        headers = await self._headers(requirement, consumer_id)
        params = requirement.query_params()
        params.pop("limit", None)
        response = await self._client.get(
            f"/v2/market-data/{requirement.instrument_uid}/snapshot",
            params=params,
            headers=headers,
        )
        return self._decode(response)

    async def instruments(
        self,
        *,
        consumer_id: str,
        consumer_grade: Grade,
        cursor: str | None = None,
        limit: int = 500,
    ) -> dict:
        headers = await self._identity_headers(consumer_grade, consumer_id)
        params: dict[str, str | int] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        response = await self._client.get(
            "/v2/instruments", params=params, headers=headers
        )
        return self._decode(response)

    async def instrument(
        self,
        identity: str,
        *,
        consumer_id: str,
        consumer_grade: Grade,
    ) -> dict:
        if not identity.strip():
            raise ValueError("instrument identity is required")
        headers = await self._identity_headers(consumer_grade, consumer_id)
        response = await self._client.get(
            f"/v2/instruments/{identity}", headers=headers
        )
        return self._decode(response)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _headers(self, requirement: DataRequirement, consumer_id: str) -> dict[str, str]:
        return await self._identity_headers(requirement.consumer_grade, consumer_id)

    async def _identity_headers(
        self, consumer_grade: Grade, consumer_id: str
    ) -> dict[str, str]:
        if self._credential_provider is None:
            raise DataLayerError(
                "UNAUTHENTICATED",
                "V2 REST transport requires a workload credential provider",
                retryable=False,
            )
        if not isinstance(consumer_grade, Grade):
            raise TypeError("consumer_grade must use the typed SDK enum")
        token = await self._credential_provider.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "X-QDL-Consumer-ID": consumer_id,
            "X-QDL-Purpose": self._purpose(consumer_grade),
        }

    @staticmethod
    def _purpose(consumer_grade: Grade | DataRequirement) -> str:
        grade = (
            consumer_grade.consumer_grade
            if isinstance(consumer_grade, DataRequirement)
            else consumer_grade
        )
        return {
            Grade.EXECUTION: "INTERNAL_EXECUTION",
            Grade.ALPHA: "INTERNAL_ALPHA",
            Grade.RESEARCH: "INTERNAL_RESEARCH",
        }[grade]

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
        target: str | Sequence[str],
        *,
        credentials: grpc.ChannelCredentials | None = None,
        tls: WorkloadTlsConfig | None = None,
        allow_insecure_loopback: bool = False,
        credential_provider: CredentialProvider | None = None,
    ) -> None:
        raw_targets = (target,) if isinstance(target, str) else tuple(target)
        targets = tuple(
            value.strip()
            for item in raw_targets
            for value in item.split(",")
            if value.strip()
        )
        if not targets or len(set(targets)) != len(targets):
            raise ValueError("gRPC stream targets must be non-empty and unique")
        self.targets = targets
        self.target = targets[0]
        self._target_index = 0
        self._credential_provider = credential_provider
        if credentials is not None and tls is not None:
            raise ValueError("provide either credentials or workload TLS, not both")
        credentials = credentials or (tls.grpc_credentials() if tls is not None else None)
        if credentials is None:
            if not allow_insecure_loopback or not all(
                self._is_loopback(value) for value in targets
            ):
                raise ValueError("insecure gRPC is allowed only for explicit loopback tests")
            self._channels = tuple(grpc.aio.insecure_channel(value) for value in targets)
        else:
            self._channels = tuple(
                grpc.aio.secure_channel(value, credentials) for value in targets
            )
        self._subscribes = tuple(
            channel.unary_stream(
                "/qdl.query.v2.MarketDataStreamService/Subscribe",
                request_serializer=query_pb2.SubscribeRequest.SerializeToString,
                response_deserializer=query_pb2.SubscribeResponse.FromString,
            )
            for channel in self._channels
        )

    async def subscribe(
        self,
        requirement: DataRequirement,
        *,
        consumer_id: str,
        cursor_token: str,
        max_buffer_events: int = 1000,
    ) -> AsyncIterator[StreamEvent | ControlEvent]:
        request = query_pb2.SubscribeRequest(
            consumer_id=consumer_id,
            requirement=requirement.to_proto(),
            cursor_token=cursor_token,
            max_buffer_events=max_buffer_events,
        )
        if self._credential_provider is None:
            raise DataLayerError(
                "UNAUTHENTICATED",
                "V2 gRPC transport requires a workload credential provider",
                retryable=False,
            )
        token = await self._credential_provider.get_token()
        metadata = (
            ("authorization", f"Bearer {token}"),
            ("x-qdl-consumer-id", consumer_id),
            ("x-qdl-purpose", RestQueryTransport._purpose(requirement)),
        )
        try:
            subscribe = self._subscribes[self._target_index]
            async for response in subscribe(request, metadata=metadata):
                record = response.record
                payload = record.WhichOneof("payload")
                if payload == "control":
                    yield ControlEvent(
                        record.control.code,
                        record.control.detail,
                        {"high_watermark": record.control.high_watermark},
                    )
                    continue
                if payload == "event":
                    yield StreamEvent(record.logical_offset, record.resume_token, record.event)
        except grpc.aio.AioRpcError as error:
            detail = error.details() or "gRPC stream failed"
            if error.code() is grpc.StatusCode.UNAVAILABLE and len(self.targets) > 1:
                self._target_index = (self._target_index + 1) % len(self.targets)
                self.target = self.targets[self._target_index]
            if error.code() is grpc.StatusCode.OUT_OF_RANGE:
                raise CursorExpiredError("CURSOR_EXPIRED", detail, retryable=False) from error
            if error.code() is grpc.StatusCode.RESOURCE_EXHAUSTED:
                raise SlowConsumerError("RATE_LIMITED", detail, retryable=True) from error
            if error.code() is grpc.StatusCode.INVALID_ARGUMENT:
                raise DataLayerError("CURSOR_INVALID", detail, retryable=False) from error
            if error.code() is grpc.StatusCode.PERMISSION_DENIED:
                raise DataLayerError("SOURCE_NOT_ALLOWED", detail, retryable=False) from error
            if error.code() is grpc.StatusCode.FAILED_PRECONDITION:
                code = detail.partition(":")[0]
                raise DataLayerError(code or "DATA_NOT_READY", detail, retryable=False) from error
            raise DataLayerError("DEPENDENCY_UNAVAILABLE", detail, retryable=True) from error

    async def close(self) -> None:
        for channel in self._channels:
            await channel.close()

    @staticmethod
    def _is_loopback(target: str) -> bool:
        host = urlsplit(f"//{target}").hostname
        if host == "localhost":
            return True
        try:
            return bool(host and ipaddress.ip_address(host).is_loopback)
        except ValueError:
            return False
