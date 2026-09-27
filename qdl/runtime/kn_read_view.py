"""KN-4 D29: the private read view behind the native Stream's GetSnapshot/GetFeedStatus.

Purpose: the Rust Stream gateway keeps one semantic owner for the two unary
read RPCs. After its own access sequence (authenticated consumer, permission,
manifest requirement - ``qdl-stream-gateway`` ``service.rs``) it asks the
paired Query replica here, and this endpoint runs the unchanged Python
oracle of ``qdl/stream/grpc_service.py`` over the market-cache backend:

* ``SNAPSHOT`` - ``requirement_from_proto``, the service's own warmup
  enforcement (``_warmup_from_history``: readiness, content, quality, policy)
  on ONE product view, the cursor v3 issuer, and the canonical envelopes of
  that same view (the spool oracle read the window twice);
* ``STATUS`` - ``V2QueryService.status_async`` on the hot lane.

Protocol: ``POST /internal/v2/kn/read-view`` (not in the public schema),
JSON ``{"schema": "qdl.v2.kn-read-view.v1", "kind": "SNAPSHOT"|"STATUS",
"consumer_id": ..., "requirement": base64(query_pb2.DataRequirement)}``,
signed ``X-QDL-Stable-Signature: sha256=<hex HMAC(secret, body)>`` like the
existing private edges; mutual TLS is the Query listener's. Replies: 200
``application/x-protobuf`` (the gRPC response message), 409 JSON
``{"code", "detail"}`` for a typed ``QueryServiceError`` (the gateway maps it
to ``FAILED_PRECONDITION "{code}:{detail}"`` exactly like the Python
handler), 400 JSON ``{"detail"}`` for an invalid request (INVALID_ARGUMENT),
401 for a bad signature.

Boundary: read-only; bounded by the existing hot lane (status) and by one
in-flight history read per replica for snapshots with history. A product
outside the catalog is typed ``UNSUPPORTED_FEED`` (the spool oracle failed
it as an internal error).
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import json

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from qdl.query import AccessPurpose, ConsumerGrade
from qdl.query.cold_work import await_in_thread
from qdl.query.contracts import CanonicalErrorCode, QueryProblem
from qdl.query.service import QueryServiceError
from qdl.runtime.internal_auth import stable_hmac_signature
from qdl.stream.grpc_service import requirement_from_proto

READ_VIEW_PATH = "/internal/v2/kn/read-view"
READ_VIEW_SCHEMA = "qdl.v2.kn-read-view.v1"
READ_VIEW_SECRET_FILE_ENV = "QDL_KN_READ_VIEW_SECRET_FILE"
_PURPOSE = {
    ConsumerGrade.ALPHA: AccessPurpose.INTERNAL_ALPHA,
    ConsumerGrade.RESEARCH: AccessPurpose.INTERNAL_RESEARCH,
    ConsumerGrade.EXECUTION: AccessPurpose.INTERNAL_EXECUTION,
}


def _problem(error: QueryServiceError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"code": error.problem.code.value, "detail": error.problem.detail},
    )


def install_kn_read_view(app: FastAPI, *, service, backend, issuer, secret: bytes) -> None:
    """Install the private read view on a Query app (kn3 backend only)."""

    if len(secret) < 32:
        raise ValueError("KN read-view secret must contain at least 256 bits")
    from qdl.query.v2 import query_pb2

    history_reads = asyncio.Semaphore(1)

    def snapshot_work(requirement, consumer_id: str):
        try:
            backend.catalog.binding_for(requirement)
        except KeyError as error:
            raise QueryServiceError(
                QueryProblem(
                    CanonicalErrorCode.UNSUPPORTED_FEED,
                    "the native read view serves catalog-bound products only",
                    False,
                ),
                request_id=service.request_id(),
                instrument_uid=requirement.instrument_uid,
            ) from error
        request_id = service.request_id()
        try:
            history, envelopes = backend.history_with_envelopes(requirement)
        except Exception as error:
            problem = getattr(error, "problem", None)
            if problem is None:
                raise
            raise QueryServiceError(
                problem, request_id=request_id, instrument_uid=requirement.instrument_uid,
            ) from error
        result = service._warmup_from_history(
            requirement, history, purpose=_PURPOSE[requirement.consumer_grade], request_id=request_id,
        )
        bound = issuer.bind_history(requirement, result.history, consumer_id=consumer_id)
        return query_pb2.GetSnapshotResponse(
            request_id=result.request_id,
            snapshot_id=bound.snapshot_id,
            stream_cursor=bound.stream_cursor,
            data_as_of_ns=bound.data_as_of_ns,
            watermark_offset=bound.watermark_offset,
            events=envelopes,
        )

    @app.post(READ_VIEW_PATH, include_in_schema=False)
    async def read_view(
        request: Request,
        signature: str | None = Header(None, alias="X-QDL-Stable-Signature"),
    ):
        body = await request.body()
        if not signature or not hmac.compare_digest(signature, stable_hmac_signature(secret, body)):
            raise HTTPException(status_code=401, detail="invalid stable read signature")
        try:
            payload = json.loads(body)
            if (
                not isinstance(payload, dict)
                or set(payload) != {"schema", "kind", "consumer_id", "requirement"}
                or payload["schema"] != READ_VIEW_SCHEMA
                or payload["kind"] not in {"SNAPSHOT", "STATUS"}
                or not isinstance(payload["consumer_id"], str)
                or not payload["consumer_id"].strip()
                or not isinstance(payload["requirement"], str)
            ):
                raise ValueError("KN read-view request schema is invalid")
            proto = query_pb2.DataRequirement.FromString(
                base64.b64decode(payload["requirement"], validate=True)
            )
            requirement = requirement_from_proto(proto)
        except (TypeError, ValueError, binascii.Error, json.JSONDecodeError) as error:
            return JSONResponse(status_code=400, content={"detail": f"INVALID_ARGUMENT:{error}"})
        except Exception as error:  # protobuf DecodeError
            return JSONResponse(status_code=400, content={"detail": f"INVALID_ARGUMENT:{type(error).__name__}"})
        consumer_id = payload["consumer_id"]
        try:
            if payload["kind"] == "STATUS":
                status = await service.status_async(requirement, consumer_id=consumer_id)
                message = query_pb2.GetFeedStatusResponse(
                    state=status.state,
                    freshness_ms=status.freshness_ms,
                    gap_open=status.gap_open,
                    complete=status.complete,
                    execution_eligible=status.execution_eligible,
                    policy_id=status.policy_id,
                    flags=status.flags,
                    event_recency_state=status.event_recency_state,
                    provider_session_state=status.provider_session_state,
                    provider_session_liveness_ms=status.provider_session_liveness_ms or 0,
                )
            elif requirement.warmup_specification is None:
                message = await await_in_thread(None, snapshot_work, requirement, consumer_id)
            else:
                async with history_reads:
                    message = await await_in_thread(
                        None, snapshot_work, requirement, consumer_id, cold=True,
                    )
        except QueryServiceError as error:
            return _problem(error)
        except ValueError as error:
            return JSONResponse(status_code=400, content={"detail": f"INVALID_ARGUMENT:{error}"})
        return Response(content=message.SerializeToString(), media_type="application/x-protobuf")
