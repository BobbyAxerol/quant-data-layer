from __future__ import annotations

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass

import grpc

from qdl.query import AccessPurpose
from qdl.security.data_plane import (
    DataPlaneAccess,
    DataPlaneAccessError,
    DataPlaneIdentityService,
)


@dataclass(frozen=True, slots=True)
class GrpcDataAccess:
    access: DataPlaneAccess
    purpose: AccessPurpose


_CURRENT_ACCESS: ContextVar[GrpcDataAccess | None] = ContextVar(
    "qdl_grpc_data_access", default=None
)


def current_grpc_data_access() -> GrpcDataAccess:
    value = _CURRENT_ACCESS.get()
    if value is None:
        raise DataPlaneAccessError(
            "UNAUTHENTICATED", "gRPC data-plane identity is unavailable", status_code=401
        )
    return value


class GrpcDataPlaneInterceptor(grpc.aio.ServerInterceptor):
    def __init__(self, identity: DataPlaneIdentityService) -> None:
        self._identity = identity

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None:
            return None
        metadata = dict(handler_call_details.invocation_metadata or ())

        async def authorize(context) -> GrpcDataAccess:
            authorization = metadata.get("authorization", "")
            consumer_id = metadata.get("x-qdl-consumer-id", "")
            purpose_value = metadata.get("x-qdl-purpose", "")
            try:
                if not authorization.startswith("Bearer "):
                    raise DataPlaneAccessError(
                        "UNAUTHENTICATED",
                        "workload bearer token is required",
                        status_code=401,
                    )
                access = await asyncio.to_thread(
                    self._identity.authenticate,
                    authorization.removeprefix("Bearer ").strip(),
                    consumer_id=consumer_id,
                )
                purpose = AccessPurpose(purpose_value.upper())
                if purpose is AccessPurpose.UNSPECIFIED:
                    raise ValueError("purpose cannot be UNSPECIFIED")
                access.require_purpose(purpose)
                return GrpcDataAccess(access, purpose)
            except (DataPlaneAccessError, ValueError) as error:
                detail = getattr(error, "detail", str(error))
                if getattr(error, "status_code", None) == 401:
                    status = grpc.StatusCode.UNAUTHENTICATED
                elif getattr(error, "status_code", None) == 429:
                    status = grpc.StatusCode.RESOURCE_EXHAUSTED
                elif getattr(error, "status_code", None) == 503:
                    status = grpc.StatusCode.UNAVAILABLE
                else:
                    status = grpc.StatusCode.PERMISSION_DENIED
                await context.abort(status, detail)
                raise AssertionError("context.abort must terminate the RPC")

        if handler.unary_unary:
            behavior = handler.unary_unary

            async def unary_unary(request, context):
                request_access = await authorize(context)
                token = _CURRENT_ACCESS.set(request_access)
                try:
                    return await behavior(request, context)
                finally:
                    _CURRENT_ACCESS.reset(token)

            return grpc.unary_unary_rpc_method_handler(
                unary_unary,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        if handler.unary_stream:
            behavior = handler.unary_stream

            async def unary_stream(request, context):
                request_access = await authorize(context)
                token = _CURRENT_ACCESS.set(request_access)
                try:
                    async for response in behavior(request, context):
                        yield response
                finally:
                    _CURRENT_ACCESS.reset(token)

            return grpc.unary_stream_rpc_method_handler(
                unary_stream,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        return handler
