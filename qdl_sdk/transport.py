from __future__ import annotations

import asyncio
import ipaddress
import random
import time
from dataclasses import dataclass
from collections.abc import AsyncIterator, Sequence
from urllib.parse import urlsplit

import grpc
import httpx

from qdl.query.v2 import query_pb2
from qdl_sdk.credentials import CredentialProvider
from qdl_sdk.errors import CursorExpiredError, DataLayerError, SlowConsumerError
from qdl_sdk.models import ControlEvent, DataRequirement, Grade, StreamEvent
from qdl_sdk.reference import ReferenceRequirement
from qdl_sdk.tls import WorkloadTlsConfig


REPLICA_REDIRECT_HEADER = "X-QDL-Replica-Redirect"
REFUSAL_REASON_HEADER = "X-QDL-Refusal-Reason"
REPLICA_LANE_BUSY = "REPLICA_LANE_BUSY"


def _redirect_headers(headers: dict[str, str], replica_redirect: bool | None) -> dict[str, str]:
    if replica_redirect is not None:
        headers[REPLICA_REDIRECT_HEADER] = "1" if replica_redirect else "0"
    return headers


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
        self._timeout_seconds = timeout_seconds
        self._credential_provider = credential_provider
        self._client = client or httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            verify=tls.ssl_context() if tls is not None else True,
        )

    async def _read_request(self, method: str, path: str, **kwargs) -> httpx.Response:
        # Query POSTs are read-only too. A peer can close an idle keep-alive
        # socket between pool reuse and receipt of headers; retry it once.
        deadline = time.monotonic() + self._timeout_seconds
        request = getattr(self._client, method)
        try:
            return await request(path, **kwargs)
        except httpx.RemoteProtocolError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            return await request(path, timeout=remaining, **kwargs)

    async def warmup(
        self,
        requirement: DataRequirement,
        *,
        consumer_id: str,
        replica_redirect: bool | None = None,
    ) -> dict:
        """``replica_redirect``: None sends no header (queue, as before 2.2.0);
        True asks a busy large-warmup lane to refuse with REPLICA_LANE_BUSY;
        False explicitly queues (the redirected retry)."""
        headers = _redirect_headers(
            await self._headers(requirement, consumer_id), replica_redirect
        )
        response = await self._read_request("get",
            f"/v2/market-data/{requirement.instrument_uid}/warmup",
            params=requirement.query_params(),
            headers=headers,
        )
        return self._decode(response)

    async def warmup_batch(
        self,
        requirements: Sequence[DataRequirement],
        *,
        consumer_id: str,
        require_all: bool,
        replica_redirect: bool | None = None,
    ) -> dict:
        values = tuple(requirements)
        if not values:
            raise ValueError("warmup batch cannot be empty")
        grades = {item.consumer_grade for item in values}
        if len(grades) != 1:
            raise ValueError("one warmup batch cannot mix consumer grades")
        headers = _redirect_headers(
            await self._identity_headers(next(iter(grades)), consumer_id),
            replica_redirect,
        )
        response = await self._read_request("post",
            "/v2/market-data/warmup:batch",
            json={
                "consumer_id": consumer_id,
                "require_all": require_all,
                "requirements": [item.to_mapping() for item in values],
            },
            headers=headers,
        )
        return self._decode(response)

    async def reference_batch(
        self,
        requirements: Sequence[ReferenceRequirement],
        *,
        consumer_id: str,
        require_all: bool,
    ) -> dict:
        values = tuple(requirements)
        if not values:
            raise ValueError("reference batch cannot be empty")
        grades = {item.consumer_grade for item in values}
        if len(grades) != 1:
            raise ValueError("one reference batch cannot mix consumer grades")
        headers = await self._identity_headers(next(iter(grades)), consumer_id)
        response = await self._read_request("post",
            "/v2/market-data/reference:batch",
            json={
                "consumer_id": consumer_id,
                "require_all": require_all,
                "requirements": [item.model_dump(mode="json") for item in values],
            },
            headers=headers,
        )
        return self._decode(response)

    async def snapshot(self, requirement: DataRequirement, *, consumer_id: str) -> dict:
        headers = await self._headers(requirement, consumer_id)
        params = requirement.query_params()
        params.pop("limit", None)
        response = await self._read_request("get",
            f"/v2/market-data/{requirement.instrument_uid}/snapshot",
            params=params,
            headers=headers,
        )
        return self._decode(response)

    async def feed_status(
        self, requirement: DataRequirement, *, consumer_id: str
    ) -> dict:
        """Read governed quality without treating stale data as a snapshot."""
        headers = await self._headers(requirement, consumer_id)
        params = requirement.query_params()
        for field in (
            "limit",
            "interval_source_policy",
            "max_cache_age_ms",
            "deadline_ms",
            "start_time_ns",
            "end_time_ns",
        ):
            params.pop(field, None)
        response = await self._read_request("get",
            f"/v2/feeds/{requirement.instrument_uid}/status",
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
        response = await self._read_request("get",
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
        response = await self._read_request("get",
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
            diagnostics=(
                payload.get("diagnostics")
                if isinstance(payload.get("diagnostics"), dict)
                else None
            ),
            reason=response.headers.get(REFUSAL_REASON_HEADER) or None,
        )


@dataclass(slots=True)
class _ReplicaReadHealth:
    failures: int = 0
    successes: int = 0
    unavailable_until: float = 0.0


class ReplicatedRestQueryTransport:
    """Bounded, health-aware distribution over existing V2 Query replicas.

    It retries only a transport failure which occurred before a typed V2
    response exists.  A data-quality, entitlement or schema error remains the
    selected replica's authoritative result; trying another reader to find a
    green answer would weaken the public contract.

    One replica-local admission signal is the exception (2.2.0, P18.4 G5).
    ``warmup``/``warmup_batch`` ask the first replica to refuse instead of
    queuing behind its single-active large-warmup lane
    (``X-QDL-Replica-Redirect: 1``). On exactly that refusal - RATE_LIMITED
    with reason ``REPLICA_LANE_BUSY``, which says nothing about the data - the
    same request goes once to the next replica with the header ``0``, which
    queues there, so a redirect never ping-pongs. The header is sent only
    when another candidate remains. Each attempt is a separate HTTP request
    and consumes one unit of the identity's per-minute request quota.
    """

    def __init__(
        self,
        replicas: Sequence[RestQueryTransport],
        *,
        max_attempts: int = 2,
        cooldown_seconds: float = 1.0,
        start_index: int | None = None,
    ) -> None:
        values = tuple(replicas)
        if len(values) < 2:
            raise ValueError("replicated Query transport requires at least two replicas")
        urls = tuple(getattr(replica, "base_url", "") for replica in values)
        if not all(urls) or len(set(urls)) != len(urls):
            raise ValueError("replicated Query transport replicas must have unique base URLs")
        if not 1 <= max_attempts <= len(values):
            raise ValueError("replicated Query max_attempts is outside replica bounds")
        if cooldown_seconds <= 0:
            raise ValueError("replicated Query cooldown must be positive")
        if start_index is not None and not 0 <= start_index < len(values):
            raise ValueError("replicated Query start_index is outside replica bounds")
        self.replicas = values
        self._health = [_ReplicaReadHealth() for _ in values]
        self._max_attempts = max_attempts
        self._cooldown_seconds = cooldown_seconds
        # Each instance starts its round-robin on a random replica. A fixed
        # start sent the first read of every new client (an alpha session's
        # startup warmup) to replica 0, where large warmups serialize behind
        # the one-active lane (P18.4 D3 stage 50: one cold read waited a whole
        # warmup while the other replica was idle).
        self._next = random.randrange(len(values)) if start_index is None else start_index

    async def warmup(self, requirement: DataRequirement, *, consumer_id: str) -> dict:
        return await self._read(
            "warmup", requirement, consumer_id=consumer_id, _redirectable=True
        )

    async def warmup_batch(
        self,
        requirements: Sequence[DataRequirement],
        *,
        consumer_id: str,
        require_all: bool,
    ) -> dict:
        return await self._read(
            "warmup_batch",
            requirements,
            consumer_id=consumer_id,
            require_all=require_all,
            _redirectable=True,
        )

    async def reference_batch(
        self,
        requirements: Sequence[ReferenceRequirement],
        *,
        consumer_id: str,
        require_all: bool,
    ) -> dict:
        return await self._read(
            "reference_batch",
            requirements,
            consumer_id=consumer_id,
            require_all=require_all,
        )

    async def snapshot(self, requirement: DataRequirement, *, consumer_id: str) -> dict:
        return await self._read("snapshot", requirement, consumer_id=consumer_id)

    async def feed_status(
        self, requirement: DataRequirement, *, consumer_id: str
    ) -> dict:
        return await self._read("feed_status", requirement, consumer_id=consumer_id)

    async def instruments(
        self,
        *,
        consumer_id: str,
        consumer_grade: Grade,
        cursor: str | None = None,
        limit: int = 500,
    ) -> dict:
        return await self._read(
            "instruments",
            consumer_id=consumer_id,
            consumer_grade=consumer_grade,
            cursor=cursor,
            limit=limit,
        )

    async def instrument(
        self,
        identity: str,
        *,
        consumer_id: str,
        consumer_grade: Grade,
    ) -> dict:
        return await self._read(
            "instrument",
            identity,
            consumer_id=consumer_id,
            consumer_grade=consumer_grade,
        )

    async def close(self) -> None:
        await asyncio.gather(*(replica.close() for replica in self.replicas))

    def stats(self) -> tuple[dict[str, int | str], ...]:
        now = time.monotonic()
        return tuple(
            {
                "base_url": replica.base_url,
                "successes": health.successes,
                "failures": health.failures,
                "cooling": int(health.unavailable_until > now),
            }
            for replica, health in zip(self.replicas, self._health, strict=True)
        )

    async def _read(
        self, method: str, *args, _redirectable: bool = False, **kwargs
    ) -> dict:
        selected = self._candidate_indexes()
        last_error: httpx.TransportError | None = None
        redirected = False
        for position, index in enumerate(selected):
            replica = self.replicas[index]
            call_kwargs = kwargs
            if _redirectable and (redirected or position + 1 < len(selected)):
                # First attempt with a fallback left: may be refused as busy.
                # After a busy refusal: explicitly queue (no ping-pong).
                call_kwargs = {**kwargs, "replica_redirect": not redirected}
            try:
                result = await getattr(replica, method)(*args, **call_kwargs)
            except DataLayerError as error:
                if (
                    call_kwargs.get("replica_redirect") is True
                    and error.code == "RATE_LIMITED"
                    and error.reason == REPLICA_LANE_BUSY
                ):
                    # Replica-local admission, not a data outcome: the replica
                    # is healthy, so its health record is left unchanged.
                    redirected = True
                    continue
                raise
            except httpx.TransportError as error:
                last_error = error
                health = self._health[index]
                health.failures += 1
                health.unavailable_until = time.monotonic() + self._cooldown_seconds
                continue
            self._health[index].successes += 1
            self._health[index].unavailable_until = 0.0
            return result
        assert last_error is not None
        raise last_error

    def _candidate_indexes(self) -> tuple[int, ...]:
        count = len(self.replicas)
        ordered = tuple((self._next + offset) % count for offset in range(count))
        self._next = (self._next + 1) % count
        now = time.monotonic()
        available = tuple(
            index
            for index in ordered
            if self._health[index].unavailable_until <= now
        )
        # A total reader outage still performs one bounded transport attempt so
        # the caller gets the current network error rather than a synthetic
        # local cooldown error. It never fans out beyond max_attempts.
        selected = available or ordered
        return selected[: self._max_attempts]


# Records the SDK accepts in one SubscribeResponse (SDK 2.3.0). A gateway that
# knows `SubscribeRequest.max_records_per_response` packs up to this many
# already-available lossless records into `SubscribeResponse.records`; an older
# gateway ignores the field and keeps sending one `record` per response.
DEFAULT_MAX_RECORDS_PER_RESPONSE = 64
MAX_RECORDS_PER_RESPONSE = 256


class GrpcStreamTransport:
    # Class default for instances built without __init__ (test doubles).
    max_records_per_response: int = DEFAULT_MAX_RECORDS_PER_RESPONSE

    def __init__(
        self,
        target: str | Sequence[str],
        *,
        credentials: grpc.ChannelCredentials | None = None,
        tls: WorkloadTlsConfig | None = None,
        allow_insecure_loopback: bool = False,
        credential_provider: CredentialProvider | None = None,
        start_index: int | None = None,
        max_records_per_response: int = DEFAULT_MAX_RECORDS_PER_RESPONSE,
    ) -> None:
        if (
            type(max_records_per_response) is not int
            or not 1 <= max_records_per_response <= MAX_RECORDS_PER_RESPONSE
        ):
            raise ValueError(
                "max_records_per_response must be an int between 1 and "
                f"{MAX_RECORDS_PER_RESPONSE}"
            )
        # 1 sends the pre-2.3.0 request (field absent): one record per response.
        self.max_records_per_response = max_records_per_response
        raw_targets = (target,) if isinstance(target, str) else tuple(target)
        targets = tuple(
            value.strip()
            for item in raw_targets
            for value in item.split(",")
            if value.strip()
        )
        if not targets or len(set(targets)) != len(targets):
            raise ValueError("gRPC stream targets must be non-empty and unique")
        if start_index is not None and not 0 <= start_index < len(targets):
            raise ValueError("gRPC stream start_index is outside target bounds")
        self.targets = targets
        # Each instance starts on a random replica, like the replicated Query
        # transport. A fixed start put every client's streams on target 0
        # (P18.4 rc8 campaign: stream_kn_1 served 244 streams, stream_kn_2
        # none), so one replica's live byte budget carried the whole load.
        self._target_index = (
            random.randrange(len(targets)) if start_index is None else start_index
        )
        self.target = targets[self._target_index]
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
        if self.max_records_per_response > 1:
            request.max_records_per_response = self.max_records_per_response
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
        start_index = self._target_index
        for attempt in range(len(self.targets)):
            target_index = (start_index + attempt) % len(self.targets)
            self._target_index = target_index
            self.target = self.targets[target_index]
            responses_seen = False
            call = None
            try:
                subscribe = self._subscribes[target_index]
                call = subscribe(request, metadata=metadata)
                async for response in call:
                    responses_seen = True
                    # A batched response carries `records` (2.3.0 gateway);
                    # every other response, and any older gateway, `record`.
                    # Callers see the same per-record events either way.
                    records = response.records or (response.record,)
                    for record in records:
                        payload = record.WhichOneof("payload")
                        if payload == "control":
                            yield ControlEvent(
                                record.control.code,
                                record.control.detail,
                                {"high_watermark": record.control.high_watermark},
                            )
                        elif payload == "event":
                            # Gateway clocks (2.4.0): 0 means not set.
                            yield StreamEvent(
                                record.logical_offset,
                                record.resume_token,
                                record.event,
                                record.gateway_dispatch_wall_ns or None,
                                record.gateway_dequeue_wall_ns or None,
                                record.gateway_send_wall_ns or None,
                            )
                return
            except grpc.aio.AioRpcError as error:
                detail = error.details() or "gRPC stream failed"
                if (
                    error.code() is grpc.StatusCode.UNAVAILABLE
                    and len(self.targets) > 1
                ):
                    next_index = (target_index + 1) % len(self.targets)
                    self._target_index = next_index
                    self.target = self.targets[next_index]
                    if not responses_seen and attempt + 1 < len(self.targets):
                        continue
                if error.code() is grpc.StatusCode.OUT_OF_RANGE:
                    raise CursorExpiredError(
                        "CURSOR_EXPIRED", detail, retryable=False
                    ) from error
                if error.code() is grpc.StatusCode.RESOURCE_EXHAUSTED:
                    raise SlowConsumerError(
                        "RATE_LIMITED", detail, retryable=True
                    ) from error
                if error.code() is grpc.StatusCode.INVALID_ARGUMENT:
                    raise DataLayerError(
                        "CURSOR_INVALID", detail, retryable=False
                    ) from error
                if error.code() is grpc.StatusCode.PERMISSION_DENIED:
                    raise DataLayerError(
                        "SOURCE_NOT_ALLOWED", detail, retryable=False
                    ) from error
                if error.code() is grpc.StatusCode.FAILED_PRECONDITION:
                    code = detail.partition(":")[0]
                    raise DataLayerError(
                        code or "DATA_NOT_READY", detail, retryable=False
                    ) from error
                raise DataLayerError(
                    "DEPENDENCY_UNAVAILABLE", detail, retryable=True
                ) from error
            finally:
                if call is not None:
                    call.cancel()

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
