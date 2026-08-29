from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import re
import ssl
import uuid
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from google.protobuf.message import DecodeError

from qdl.marketdata.v2 import market_data_pb2
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.envelope import validate_raw_envelope
from qdl.runtime.lease import GatewayFenced
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.stream import DurableStreamGateway
from qdl.transport import DurableEvent, SQLiteDurableSpool, StoredEvent


_INGEST_SCHEMA = "qdl.v2.stable-canonical-ingest.v1"
_RESULT_SCHEMA = "qdl.v2.stable-canonical-ingest-result.v1"
_MAX_REJECTION_DETAIL_CHARS = 192
_UNSAFE_REJECTION_TOKEN = re.compile(r"[A-Za-z0-9+/=_-]{33,}")


def _signature(secret: bytes, body: bytes) -> str:
    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def _internal_url(value: str) -> bool:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False
    try:
        return ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        return parsed.hostname in {
            "localhost",
            "stream_v2",
            "stream_v2_active",
            "stream_v2_passive",
            "qdl-stable-stream",
        } or parsed.hostname.endswith(".internal")


def _bounded_rejection_detail(response: httpx.Response) -> str:
    """Return a bounded, payload-safe stable-ingest validation reason."""

    try:
        body = response.json()
    except (ValueError, json.JSONDecodeError):
        return "unavailable"
    detail = body.get("detail") if isinstance(body, dict) else None
    if not isinstance(detail, str):
        return "unavailable"
    normalized = " ".join(detail.split())
    if not normalized:
        return "unavailable"
    return _UNSAFE_REJECTION_TOKEN.sub(
        "<redacted>", normalized
    )[:_MAX_REJECTION_DETAIL_CHARS]


def install_stable_canonical_ingest(
    app: FastAPI,
    *,
    gateway: DurableStreamGateway,
    catalog: StableSourceCatalog,
    spool: SQLiteDurableSpool,
    secret: bytes,
) -> None:
    if len(secret) < 32:
        raise ValueError("stable internal ingest secret must contain at least 256 bits")

    @app.post("/internal/v2/canonical/events", include_in_schema=False)
    async def ingest(
        request: Request,
        signature: str | None = Header(None, alias="X-QDL-Stable-Signature"),
    ):
        body = await request.body()
        if not signature or not hmac.compare_digest(signature, _signature(secret, body)):
            raise HTTPException(status_code=401, detail="invalid stable ingest signature")
        try:
            payload = json.loads(body)
            if set(payload) != {"schema", "batch_id", "events"}:
                raise ValueError("stable ingest fields are incomplete or unknown")
            if payload["schema"] != _INGEST_SCHEMA:
                raise ValueError("stable ingest schema is invalid")
            uuid.UUID(str(payload["batch_id"]))
            values = payload["events"]
            if not isinstance(values, list) or not 1 <= len(values) <= 1000:
                raise ValueError("stable ingest batch must contain 1..1000 events")
        except (ValueError, TypeError, json.JSONDecodeError) as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        try:
            lease_epoch = gateway.assert_active()
        except GatewayFenced as error:
            raise HTTPException(status_code=409, detail="stable gateway is not active") from error

        references = []
        for value in values:
            try:
                required_fields = {"canonical", "raw_stream", "raw_event_id"}
                allowed_fields = required_fields | {"raw_provider_envelope"}
                if (
                    not isinstance(value, dict)
                    or not required_fields.issubset(value)
                    or set(value) - allowed_fields
                ):
                    raise ValueError("stable event reference fields are incomplete or unknown")
                canonical = base64.b64decode(value["canonical"], validate=True)
                raw_event_id = bytes.fromhex(str(value["raw_event_id"]))
                raw_stream = str(value["raw_stream"])
                inline_raw = (
                    base64.b64decode(value["raw_provider_envelope"], validate=True)
                    if "raw_provider_envelope" in value
                    else None
                )
                envelope = market_data_pb2.EventEnvelope.FromString(canonical)
                binding = catalog.binding_for_envelope(envelope)
                if raw_event_id != bytes(envelope.raw_capture_id):
                    raise ValueError("stable canonical raw reference is unavailable")
                if inline_raw is not None:
                    raw = raw_provider_pb2.RawProviderEnvelope.FromString(inline_raw)
                    validate_raw_envelope(raw)
                    if (
                        bytes(raw.capture_id) != raw_event_id
                        or bytes(raw.raw_frame_sha256) != bytes(envelope.raw_payload_hash)
                        or raw.provider != envelope.provider
                        or raw.venue != envelope.venue
                        or raw.market != envelope.market
                        or raw.native_symbol != envelope.native_symbol
                        or raw.source_session_id != envelope.source_session_id
                        or raw.connection_generation != envelope.connection_generation
                        or raw.authority_revision != envelope.authority_revision
                    ):
                        raise ValueError("private Kafka raw lineage validation failed")
                references.append((
                    binding, envelope, canonical, raw_stream, raw_event_id, inline_raw
                ))
            except (ValueError, TypeError, DecodeError) as error:
                raise HTTPException(status_code=422, detail=str(error)) from error

        raw_by_reference = {}
        for raw_stream in sorted({
            item[3] for item in references if item[5] is None
        }):
            event_ids = [
                item[4]
                for item in references
                if item[3] == raw_stream and item[5] is None
            ]
            found = await asyncio.to_thread(
                spool.find_events, stream=raw_stream, event_ids=event_ids
            )
            raw_by_reference.update(
                {(raw_stream, event_id): stored for event_id, stored in found.items()}
            )
        prepared = []
        for (
            binding, envelope, canonical, raw_stream, raw_event_id, inline_raw
        ) in references:
            if (
                inline_raw is None
                and (raw_stream, raw_event_id) not in raw_by_reference
            ):
                raise HTTPException(
                    status_code=422,
                    detail="stable canonical raw reference is unavailable",
                )
            prepared.append((binding, envelope, DurableEvent(
                stream=catalog.canonical_stream,
                partition_key=binding.partition_key,
                event_id=bytes(envelope.event_id),
                payload=canonical,
                accepted_at_ns=max(envelope.received_at_ns, 1),
                headers={
                    "raw_stream": raw_stream,
                    "raw_event_id": raw_event_id.hex(),
                },
            )))

        try:
            stored_values = await gateway.publish_many(
                [event for _binding, _envelope, event in prepared]
            )
        except GatewayFenced as error:
            raise HTTPException(status_code=409, detail="stable gateway was fenced") from error
        duplicate_ids = [
            event.event_id
            for (_binding, _envelope, event), stored in zip(
                prepared, stored_values, strict=True
            )
            if stored is None
        ]
        duplicates = await asyncio.to_thread(
            spool.find_events,
            stream=catalog.canonical_stream,
            event_ids=duplicate_ids,
        )
        results = []
        for (binding, envelope, event), stored in zip(
            prepared, stored_values, strict=True
        ):
            duplicate = stored is None
            stored = stored or duplicates.get(event.event_id)
            if stored is None:
                raise HTTPException(status_code=503, detail="stable cache ACK is unavailable")
            results.append({
                "event_id": envelope.event_id.hex(),
                "partition_key": binding.partition_key,
                "offset": stored.cursor.offset,
                "duplicate": duplicate,
            })
        return {
            "schema": _RESULT_SCHEMA,
            "lease_epoch": lease_epoch,
            "results": results,
        }


@dataclass(slots=True)
class StableHttpCanonicalSink:
    urls: tuple[str, ...]
    secret: bytes
    spool: SQLiteDurableSpool
    timeout_seconds: float = 10.0
    max_request_bytes: int = 1_048_576
    client: httpx.AsyncClient | None = None
    ssl_context: ssl.SSLContext | None = None
    _owns_client: bool = field(init=False)

    def __post_init__(self) -> None:
        if (
            not self.urls
            or any(not _internal_url(value) for value in self.urls)
            or len(self.secret) < 32
            or self.timeout_seconds <= 0
            or self.max_request_bytes <= 0
        ):
            raise ValueError("stable HTTP sink configuration is invalid")
        if any(urlsplit(value).scheme == "https" for value in self.urls) and self.ssl_context is None and self.client is None:
            raise ValueError("stable HTTPS sink requires a workload TLS context")
        if self.client is not None and self.ssl_context is not None:
            raise ValueError("stable HTTP sink client and TLS context are mutually exclusive")
        self._owns_client = self.client is None
        if self.client is None:
            self.client = httpx.AsyncClient(
                follow_redirects=False,
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                timeout=self.timeout_seconds,
                verify=self.ssl_context or True,
            )

    async def publish(self, event: DurableEvent) -> StoredEvent:
        return (await self.publish_many((event,)))[0]

    async def publish_many(
        self, events: tuple[DurableEvent, ...] | list[DurableEvent]
    ) -> tuple[StoredEvent, ...]:
        values = tuple(events)
        if not 1 <= len(values) <= 1000:
            raise ValueError("stable HTTP sink batch must contain 1..1000 events")
        encoded = tuple(self._encode_event(event) for event in values)
        stored_values = []
        for chunk_values, chunk_encoded in self._request_chunks(values, encoded):
            stored_values.extend(
                await self._publish_chunk(chunk_values, chunk_encoded)
            )
        return tuple(stored_values)

    @staticmethod
    def _encode_event(event: DurableEvent) -> dict[str, str]:
        raw_stream = event.headers.get("raw_stream")
        raw_event_id = event.headers.get("raw_event_id")
        if not raw_stream or not raw_event_id:
            raise ValueError("stable HTTP sink requires a durable raw reference")
        item = {
            "canonical": base64.b64encode(event.payload).decode(),
            "raw_stream": raw_stream,
            "raw_event_id": raw_event_id,
        }
        inline_raw = event.headers.get("raw_provider_envelope")
        if inline_raw:
            item["raw_provider_envelope"] = inline_raw
        return item

    @staticmethod
    def _body(encoded: tuple[dict[str, str], ...]) -> bytes:
        return json.dumps({
            "schema": _INGEST_SCHEMA,
            "batch_id": str(uuid.uuid4()),
            "events": encoded,
        }, sort_keys=True, separators=(",", ":")).encode()

    def _request_chunks(
        self,
        values: tuple[DurableEvent, ...],
        encoded: tuple[dict[str, str], ...],
    ) -> tuple[tuple[tuple[DurableEvent, ...], tuple[dict[str, str], ...]], ...]:
        chunks = []
        current_values: list[DurableEvent] = []
        current_encoded: list[dict[str, str]] = []
        for event, item in zip(values, encoded, strict=True):
            candidate = (*current_encoded, item)
            if len(self._body(candidate)) > self.max_request_bytes:
                if not current_values:
                    raise ValueError(
                        "stable canonical event exceeds request byte bound"
                    )
                chunks.append((tuple(current_values), tuple(current_encoded)))
                current_values = [event]
                current_encoded = [item]
                if len(self._body(tuple(current_encoded))) > self.max_request_bytes:
                    raise ValueError(
                        "stable canonical event exceeds request byte bound"
                    )
                continue
            current_values.append(event)
            current_encoded.append(item)
        if current_values:
            chunks.append((tuple(current_values), tuple(current_encoded)))
        return tuple(chunks)

    async def _publish_chunk(
        self,
        values: tuple[DurableEvent, ...],
        encoded: tuple[dict[str, str], ...],
    ) -> tuple[StoredEvent, ...]:
        body = self._body(encoded)
        last_error: BaseException | None = None
        assert self.client is not None
        for url in self.urls:
            try:
                response = await self.client.post(
                    f"{url.rstrip('/')}/internal/v2/canonical/events",
                    content=body,
                    headers={
                        "Content-Type": "application/json",
                        "X-QDL-Stable-Signature": _signature(self.secret, body),
                    },
                )
                if response.status_code in {409, 503}:
                    continue
                response.raise_for_status()
                result = response.json()
                acknowledgements = result.get("results", ())
                if (
                    result.get("schema") != _RESULT_SCHEMA
                    or len(acknowledgements) != len(values)
                    or [item.get("event_id") for item in acknowledgements]
                    != [event.event_id.hex() for event in values]
                ):
                    raise ValueError("stable ingest ACK contract is invalid")
                stored_by_id = await asyncio.to_thread(
                    self.spool.find_events,
                    stream=values[0].stream,
                    event_ids=[event.event_id for event in values],
                )
                stored_values = []
                for event, acknowledgement in zip(
                    values, acknowledgements, strict=True
                ):
                    stored = stored_by_id.get(event.event_id)
                    if (
                        stored is None
                        or stored.cursor.offset != int(acknowledgement["offset"])
                    ):
                        raise ValueError("stable ingest ACK differs from shared cache")
                    stored_values.append(stored)
                return tuple(stored_values)
            except (httpx.HTTPError, ValueError, TypeError) as error:
                last_error = error
        if isinstance(last_error, httpx.HTTPStatusError):
            raise RuntimeError(
                "stable canonical ingest rejected "
                f"http_status={last_error.response.status_code} "
                f"detail={_bounded_rejection_detail(last_error.response)}"
            ) from last_error
        raise RuntimeError(
            "no active stable stream gateway accepted canonical data"
        ) from last_error

    async def close(self) -> None:
        if self._owns_client and self.client is not None:
            await self.client.aclose()
