from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RequestBounds:
    max_request_bytes: int = 1_048_576
    max_query_string_bytes: int = 16_384
    request_deadline_seconds: float = 10.0
    admission_timeout_seconds: float = 0.05
    max_concurrent_requests: int = 200

    def __post_init__(self) -> None:
        if min(
            self.max_request_bytes,
            self.max_query_string_bytes,
            self.max_concurrent_requests,
        ) <= 0:
            raise ValueError("request bounds must be positive")
        if min(self.request_deadline_seconds, self.admission_timeout_seconds) <= 0:
            raise ValueError("request time bounds must be positive")


class BoundedRequestMiddleware:
    """ASGI byte/deadline/concurrency boundary independent of proxy behavior."""

    def __init__(self, app, *, bounds: RequestBounds) -> None:
        self.app = app
        self.bounds = bounds
        self._capacity = asyncio.Semaphore(bounds.max_concurrent_requests)

    @staticmethod
    async def _problem(send, status: int, code: str, detail: str) -> None:
        payload = json.dumps(
            {
                "type": f"urn:qdl:error:{code.lower().replace('_', '-')}",
                "title": code.replace("_", " ").title(),
                "status": status,
                "code": code,
                "detail": detail,
                "request_id": "gateway-boundary",
                "retryable": status in {429, 503, 504},
            },
            separators=(",", ":"),
        ).encode()
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/problem+json"),
                (b"content-length", str(len(payload)).encode()),
            ],
        })
        await send({"type": "http.response.body", "body": payload})

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {key.lower(): value for key, value in scope.get("headers", ())}
        if len(scope.get("query_string", b"")) > self.bounds.max_query_string_bytes:
            await self._problem(send, 414, "INVALID_ARGUMENT", "query string exceeds bound")
            return
        encoding = headers.get(b"content-encoding", b"identity").decode().lower()
        if encoding not in {"", "identity"}:
            await self._problem(
                send, 415, "INVALID_ARGUMENT", "compressed request bodies are not accepted"
            )
            return
        try:
            declared = int(headers.get(b"content-length", b"0"))
        except ValueError:
            await self._problem(send, 400, "INVALID_ARGUMENT", "invalid content-length")
            return
        if declared < 0 or declared > self.bounds.max_request_bytes:
            await self._problem(send, 413, "PAYLOAD_TOO_LARGE", "request exceeds byte bound")
            return

        consumed = 0
        response_started = False

        async def bounded_receive():
            nonlocal consumed
            message = await receive()
            if message["type"] == "http.request":
                consumed += len(message.get("body", b""))
                if consumed > self.bounds.max_request_bytes:
                    raise ValueError("request exceeds byte bound")
            return message

        async def bounded_send(message):
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await asyncio.wait_for(
                self._capacity.acquire(), timeout=self.bounds.admission_timeout_seconds
            )
        except TimeoutError:
            await self._problem(send, 429, "RATE_LIMITED", "request concurrency exhausted")
            return
        try:
            try:
                await asyncio.wait_for(
                    self.app(scope, bounded_receive, bounded_send),
                    timeout=self.bounds.request_deadline_seconds,
                )
            except TimeoutError:
                if not response_started:
                    await self._problem(
                        send, 504, "DEADLINE_EXCEEDED", "request deadline exceeded"
                    )
            except ValueError as error:
                if str(error) != "request exceeds byte bound":
                    raise
                if not response_started:
                    await self._problem(send, 413, "PAYLOAD_TOO_LARGE", str(error))
        finally:
            self._capacity.release()
