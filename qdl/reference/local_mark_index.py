"""Alpha MARK/INDEX reads served inside the Query process from its own spool.

Alpha reads of the current MARK/INDEX first moved from venue REST to the stream
gateway's live view (OKX REST exceeded the venue bucket at stage 50). At stage
35 (2026-09-23) that added HTTP work to the single stream process that also
writes and fans out, and a market burst then left the writer 46 s behind. This
reader keeps the same verified view and the same private endpoint code, but
runs them in-process on the Query replica: before each read the instrument's
latest canonical record is offered from the local spool (exactly as
``hydrate_from_spool`` does on a lease change), then the unchanged endpoint and
``HttpExecutionMarkIndexReader`` conversion apply every identity, freshness and
gap gate. No network hop, no stream-process load, no venue call.

Execution (TS) reads keep the stream gateway's live view: it is fed before the
spool append and carries the quiet-session evidence execution requires.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import os
from urllib.parse import unquote, urlsplit

import httpx
from fastapi import FastAPI
from google.protobuf.message import DecodeError

from qdl.marketdata.v2 import market_data_pb2
from qdl.reference.execution_live import HttpExecutionMarkIndexReader
from qdl.runtime.execution_mark_index import (
    ExecutionMarkIndexRead,
    ExecutionMarkIndexLiveView,
    MarkIndexReadRefused,
    install_execution_mark_index_read,
    parse_execution_mark_index_read,
    serve_execution_mark_index_read,
)

_LOCAL_EPOCH = 1
# Upper bound of the request freshness the private endpoint accepts.
_ALPHA_STALE_AFTER_MS = 300_000
_LOCAL_URL = "http://localhost"


class _LocalGateway:
    """The Query replica is never a writer; its local view has one epoch."""

    @staticmethod
    def assert_active(epoch: int | None = None) -> int:
        return _LOCAL_EPOCH


class SpoolRefreshingMarkIndexView(ExecutionMarkIndexLiveView):
    """The execution view, offered this replica's latest spooled record per read."""

    def attach(self, *, spool, canonical_stream: str) -> "SpoolRefreshingMarkIndexView":
        self._spool = spool
        self._canonical_stream = canonical_stream
        # Alpha reads are judged by the alpha's own declared freshness (the
        # view bounds a read by min(request freshness, binding stale_after)).
        # The binding's 2 s bound is the execution horizon; applied to alpha it
        # pushed about 30 % of Binance reads (1 s mark cadence plus spool
        # latency) back to venue REST. Execution reads never use this view.
        self._bindings = {
            uid: replace(binding, stale_after_ms=max(binding.stale_after_ms, _ALPHA_STALE_AFTER_MS))
            for uid, binding in self._bindings.items()
        }
        return self

    async def read(self, *, instrument_uid: str, **kwargs):
        binding = self._bindings.get(instrument_uid)
        if binding is not None:
            rows = await asyncio.to_thread(
                self._spool.read_tail,
                stream=self._canonical_stream,
                partition_key=binding.partition_key,
                limit=1,
            )
            if rows:
                stored = rows[-1]
                try:
                    envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
                except DecodeError:
                    envelope = None
                if envelope is not None and envelope.WhichOneof("payload") == "mark_index_price":
                    await self.remember(
                        binding=binding, envelope=envelope, stored=stored,
                        gateway_epoch=_LOCAL_EPOCH,
                    )
        return await super().read(instrument_uid=instrument_uid, **kwargs)


class CacheRefreshingMarkIndexView(SpoolRefreshingMarkIndexView):
    """The same view offered the market cache's latest record (KN-4 D31).

    The Kafka-native Query backend has no spool: before each read it offers
    the MARK_INDEX product's latest entry from the market cache (one READY
    generation, read by ``KnMarketCacheQueryBackend``), then the unchanged
    endpoint and conversion apply every identity, freshness and gap gate.
    A product that is not ready offers nothing (the view answers unavailable).

    Alpha reads relax the binding horizon exactly like the spool reader.
    Execution reads keep the binding's own ``stale_after_ms`` and the quiet
    evidence of the stream's view (the event's paired MARK/INDEX receipts,
    the provider session files and the acquisition quiet policies): the Rust
    Stream serves no private endpoint, and the cache holds the same
    read-committed canonical record the Python stream view held.
    """

    def attach_cache(self, *, backend, relax_to_alpha: bool = True) -> "CacheRefreshingMarkIndexView":
        self._backend = backend
        if relax_to_alpha:
            self._bindings = {
                uid: replace(binding, stale_after_ms=max(binding.stale_after_ms, _ALPHA_STALE_AFTER_MS))
                for uid, binding in self._bindings.items()
            }
        return self

    async def read(self, *, instrument_uid: str, **kwargs):
        result = await self._read_cache(instrument_uid=instrument_uid, **kwargs)
        if (result.record is None and getattr(self._backend, "hot_client", None) is not None
                and result.reason in {"STALE", "COMPONENT_STALE", "NOT_READY",
                    "MARKET_CACHE_NOT_READY", "MARKET_CACHE_UNAVAILABLE", "MARKET_CACHE_BACKUP_UNAVAILABLE"}):
            # Exactly one bounded attempt; identity/fence/gap/session refusals
            # are not healed by remembering an older view from another replica.
            return await self._read_cache(instrument_uid=instrument_uid, prefer_hot=True, **kwargs)
        return result

    async def _read_cache(self, *, instrument_uid: str, prefer_hot: bool = False, **kwargs):
        binding = self._bindings.get(instrument_uid)
        if binding is not None:
            if prefer_hot:
                stored, state = await asyncio.to_thread(self._backend.latest_stored_event, binding, prefer_hot=True)
            else:
                stored, state = await asyncio.to_thread(self._backend.latest_stored_event, binding)
            reason = None if state == "OK" else f"MARKET_CACHE_{state}"
            if stored is not None:
                try:
                    envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
                except DecodeError:
                    envelope = None
                if envelope is None or envelope.WhichOneof("payload") != "mark_index_price":
                    reason = "LINEAGE_INVALID"
                else:
                    try:
                        await self.remember(
                            binding=binding, envelope=envelope, stored=stored,
                            gateway_epoch=_LOCAL_EPOCH,
                        )
                    except ValueError:
                        reason = "LINEAGE_INVALID"
            if reason is not None:
                # D36: a cache, fencing or integrity failure (or a product
                # without state) never lets a remembered price answer.
                async with self._lock:
                    self._records.pop(instrument_uid, None)
                return ExecutionMarkIndexRead(None, reason)
        return await ExecutionMarkIndexLiveView.read(self, instrument_uid=instrument_uid, **kwargs)


def build_cache_alpha_mark_index_reader(*, catalog, backend) -> HttpExecutionMarkIndexReader:
    view = CacheRefreshingMarkIndexView.from_catalog(catalog).attach_cache(backend=backend)
    return reader_for_view(view)


def build_cache_execution_mark_index_reader(
    *, catalog, backend, acquisition, session_liveness_reader,
) -> HttpExecutionMarkIndexReader:
    """Execution MARK/INDEX from the market cache with execution bounds (D31)."""

    view = CacheRefreshingMarkIndexView.from_catalog(
        catalog, acquisition=acquisition, session_liveness_reader=session_liveness_reader,
    ).attach_cache(backend=backend, relax_to_alpha=False)
    return reader_for_view(view)


def build_local_alpha_mark_index_reader(*, catalog, spool) -> HttpExecutionMarkIndexReader:
    view = SpoolRefreshingMarkIndexView.from_catalog(catalog).attach(
        spool=spool, canonical_stream=catalog.canonical_stream,
    )
    return reader_for_view(view)


class InProcessAsgiResponse:
    """The parts of an ``httpx.Response`` the MARK/INDEX reader reads."""

    __slots__ = ("status_code", "headers", "content")

    def __init__(self, status_code: int, headers: httpx.Headers, content: bytes) -> None:
        self.status_code = status_code
        self.headers = headers
        self.content = content

    def json(self):
        return json.loads(self.content)


class _AsgiExchange:
    """One request/response through the app (no closures, no back references)."""

    __slots__ = ("body", "sent_body", "status_code", "headers", "chunks", "complete")

    def __init__(self, body: bytes) -> None:
        self.body = body
        self.sent_body = False
        self.status_code: int | None = None
        self.headers: list[tuple[bytes, bytes]] = []
        self.chunks: list[bytes] = []
        self.complete = asyncio.Event()

    async def receive(self) -> dict:
        if not self.sent_body:
            self.sent_body = True
            return {"type": "http.request", "body": self.body, "more_body": False}
        await self.complete.wait()
        return {"type": "http.disconnect"}

    async def send(self, message: dict) -> None:
        if message["type"] == "http.response.start":
            self.status_code = message["status"]
            self.headers = list(message.get("headers", ()))
        elif message["type"] == "http.response.body" and not self.complete.is_set():
            self.chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                self.complete.set()


class InProcessAsgiClient:
    """POST into an in-process ASGI app, as ``httpx.AsyncClient(transport=httpx.ASGITransport(app))`` did.

    IDR-114: every httpx request/response pair is a reference cycle (Response <-> BoundAsyncStream,
    25 cyclic objects per request, measured with httpx 0.28.1 with or without ``aclose``). At the
    stage-100 MARK/INDEX rate that was all of the cyclic garbage of a Query replica: one 0.2-0.5 s
    GIL-holding young collection every 76-110 s. This client drives the same app with the same ASGI
    messages (scope, one ``http.request``, ``http.disconnect`` once the response is complete) and
    returns a plain response object, so the exchange is freed by reference counting. App exceptions
    propagate, as with ``ASGITransport(raise_app_exceptions=True)``; timeouts stay the caller's
    ``asyncio.wait_for``."""

    def __init__(self, app, *, base_url: str = _LOCAL_URL) -> None:
        self.app = app
        self.base_url = base_url

    async def post(self, url: str, *, content: bytes, headers: dict[str, str], timeout=None) -> InProcessAsgiResponse:
        del timeout  # in-process: bounded by the caller's asyncio.wait_for, as with ASGITransport
        target = urlsplit(url if "://" in url else self.base_url.rstrip("/") + url)
        host = target.hostname or "localhost"
        port = target.port or (443 if target.scheme == "https" else 80)
        raw_headers = [(b"host", (target.netloc or host).encode("latin-1")),
                       (b"content-length", str(len(content)).encode("latin-1"))]
        raw_headers += [(name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in headers.items()]
        scope = {
            "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "POST",
            "scheme": target.scheme or "http", "path": unquote(target.path or "/"),
            "raw_path": (target.path or "/").encode("latin-1"), "query_string": target.query.encode("latin-1"),
            "root_path": "", "headers": raw_headers, "server": (host, port), "client": ("127.0.0.1", 123),
            "extensions": {},
        }
        exchange = _AsgiExchange(content)
        await self.app(scope, exchange.receive, exchange.send)
        assert exchange.complete.is_set() and exchange.status_code is not None, "ASGI app sent no complete response"
        return InProcessAsgiResponse(exchange.status_code, httpx.Headers(exchange.headers), b"".join(exchange.chunks))

    async def aclose(self) -> None:
        return None


class InProcessViewClient:
    """The private MARK/INDEX endpoint called directly, without an ASGI hop (DL v2.4.0 S4).

    ``reference:batch`` reached the in-process view through a FastAPI app:
    building an ASGI scope, routing, header injection and JSON response
    rendering were about 55 % of 0.59 ms per MARK/INDEX item (2026-11-18
    review). This client keeps every check of that endpoint and calls the
    same code (``parse_execution_mark_index_read`` verifies the HMAC
    signature, schema and fields; ``serve_execution_mark_index_read`` applies
    the gateway fence, the view read and the refusal record) and returns the
    same status, headers and body the route would, so
    ``HttpExecutionMarkIndexReader`` converts and validates it unchanged.
    """

    __slots__ = ("gateway", "view", "secret", "calls")

    def __init__(self, *, gateway, view, secret: bytes) -> None:
        if len(secret) < 32:
            raise ValueError("stable internal read secret must contain at least 256 bits")
        self.gateway = gateway
        self.view = view
        self.secret = secret
        self.calls = 0

    async def post(self, url: str, *, content: bytes, headers: dict[str, str], timeout=None) -> InProcessAsgiResponse:
        del url, timeout  # in-process: bounded by the caller's asyncio.wait_for
        self.calls += 1
        signature = next(
            (value for name, value in headers.items() if name.lower() == "x-qdl-stable-signature"), None,
        )
        try:
            fields = parse_execution_mark_index_read(secret=self.secret, body=content, signature=signature)
            response_headers, payload = await serve_execution_mark_index_read(
                gateway=self.gateway, view=self.view, fields=fields,
            )
        except MarkIndexReadRefused as refused:
            body = json.dumps({"detail": refused.detail}, separators=(",", ":")).encode()
            return InProcessAsgiResponse(
                refused.status_code, httpx.Headers({"content-type": "application/json"}), body,
            )
        body = json.dumps(payload, separators=(",", ":")).encode()
        response_headers = {"content-type": "application/json", **response_headers}
        return InProcessAsgiResponse(200, httpx.Headers(response_headers), body)

    async def aclose(self) -> None:
        return None


def reader_for_view(view: SpoolRefreshingMarkIndexView, *, direct: bool = True) -> HttpExecutionMarkIndexReader:
    """The in-process MARK/INDEX reader; ``direct=False`` keeps the pre-v2.4.0 ASGI hop (tests, A/B)."""

    secret = os.urandom(32)
    if direct:
        client = InProcessViewClient(gateway=_LocalGateway(), view=view, secret=secret)
    else:
        app = FastAPI()
        install_execution_mark_index_read(app, gateway=_LocalGateway(), view=view, secret=secret)
        client = InProcessAsgiClient(app, base_url=_LOCAL_URL)
    return HttpExecutionMarkIndexReader(urls=(_LOCAL_URL,), secret=secret, client=client)
