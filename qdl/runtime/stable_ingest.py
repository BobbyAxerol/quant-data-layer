from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
import json
import uuid
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Header, HTTPException, Request

from qdl.marketdata.v2 import market_data_pb2
from qdl.runtime.lease import GatewayFenced
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.stream import DurableStreamGateway
from qdl.transport import DurableEvent, SQLiteDurableSpool, StoredEvent


_INGEST_SCHEMA = "qdl.v2.stable-canonical-ingest.v1"
_RESULT_SCHEMA = "qdl.v2.stable-canonical-ingest-result.v1"


def _signature(secret: bytes, body: bytes) -> str:
    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def _internal_url(value: str) -> bool:
    parsed = urlsplit(value)
    if parsed.scheme != "http" or not parsed.hostname:
        return False
    try:
        return ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        return parsed.hostname in {"localhost", "stream_v2", "qdl-stable-stream"} or (
            parsed.hostname.endswith(".internal")
        )


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

        prepared = []
        for value in values:
            try:
                if not isinstance(value, dict) or set(value) != {
                    "canonical", "raw_stream", "raw_event_id"
                }:
                    raise ValueError("stable event reference fields are incomplete or unknown")
                canonical = base64.b64decode(value["canonical"], validate=True)
                raw_event_id = bytes.fromhex(str(value["raw_event_id"]))
                raw_stream = str(value["raw_stream"])
                envelope = market_data_pb2.EventEnvelope.FromString(canonical)
                binding = catalog.binding_for_envelope(envelope)
                raw = spool.find_event(stream=raw_stream, event_id=raw_event_id)
                if raw is None or raw_event_id != bytes(envelope.raw_capture_id):
                    raise ValueError("stable canonical raw reference is unavailable")
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
            except (ValueError, TypeError) as error:
                raise HTTPException(status_code=422, detail=str(error)) from error

        results = []
        for binding, envelope, event in prepared:
            try:
                stored = await gateway.publish(event)
            except GatewayFenced as error:
                raise HTTPException(status_code=409, detail="stable gateway was fenced") from error
            duplicate = stored is None
            if stored is None:
                stored = spool.find_event(stream=event.stream, event_id=event.event_id)
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
    client: httpx.AsyncClient | None = None
    _owns_client: bool = field(init=False)

    def __post_init__(self) -> None:
        if (
            not self.urls
            or any(not _internal_url(value) for value in self.urls)
            or len(self.secret) < 32
            or self.timeout_seconds <= 0
        ):
            raise ValueError("stable HTTP sink configuration is invalid")
        self._owns_client = self.client is None
        if self.client is None:
            self.client = httpx.AsyncClient(
                follow_redirects=False,
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                timeout=self.timeout_seconds,
            )

    async def publish(self, event: DurableEvent) -> StoredEvent:
        raw_stream = event.headers.get("raw_stream")
        raw_event_id = event.headers.get("raw_event_id")
        if not raw_stream or not raw_event_id:
            raise ValueError("stable HTTP sink requires a durable raw reference")
        body = json.dumps({
            "schema": _INGEST_SCHEMA,
            "batch_id": str(uuid.uuid4()),
            "events": [{
                "canonical": base64.b64encode(event.payload).decode(),
                "raw_stream": raw_stream,
                "raw_event_id": raw_event_id,
            }],
        }, sort_keys=True, separators=(",", ":")).encode()
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
                if (
                    result.get("schema") != _RESULT_SCHEMA
                    or len(result.get("results", ())) != 1
                    or result["results"][0].get("event_id") != event.event_id.hex()
                ):
                    raise ValueError("stable ingest ACK contract is invalid")
                stored = self.spool.find_event(stream=event.stream, event_id=event.event_id)
                if stored is None or stored.cursor.offset != int(result["results"][0]["offset"]):
                    raise ValueError("stable ingest ACK differs from shared cache")
                return stored
            except (httpx.HTTPError, ValueError, TypeError) as error:
                last_error = error
        raise RuntimeError("no active stable stream gateway accepted canonical data") from last_error

    async def close(self) -> None:
        if self._owns_client and self.client is not None:
            await self.client.aclose()
