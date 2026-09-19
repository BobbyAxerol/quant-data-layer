"""Fenced latest execution view for canonical MARK/INDEX events.

This is intentionally a tiny in-process view owned by the existing active
stream gateway.  It is not a second cache, replay source, provider adapter, or
public API: a query replica can only read the newest already-validated canonical
event from the current lease holder.
"""

from __future__ import annotations

import asyncio
import base64
import hmac
import json
import time
from dataclasses import dataclass

from fastapi import FastAPI, Header, HTTPException, Request, Response

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.query.contracts import FeedType
from qdl.runtime.internal_auth import stable_hmac_signature
from qdl.runtime.lease import GatewayFenced
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog
from qdl.stream import DurableStreamGateway
from qdl.transport import StoredEvent


_REQUEST_SCHEMA = "qdl.v2.execution-mark-index-read.v1"
_RESPONSE_SCHEMA = "qdl.v2.execution-mark-index-view.v2"
_DELIVERY_CANONICAL_READ_COMMITTED = "CANONICAL_READ_COMMITTED"
_DELIVERY_SPOOL_CONFIRMED = "SPOOL_CONFIRMED"
_FRESHNESS_BASIS_HEADER = "X-QDL-Execution-Freshness-Basis"
_GAP_FLAGS = frozenset({
    common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE,
    common_pb2.QUALITY_FLAG_OUT_OF_ORDER,
    common_pb2.QUALITY_FLAG_RESYNC_REQUIRED,
})


@dataclass(frozen=True, slots=True)
class ExecutionMarkIndexRecord:
    """One already-admitted canonical MARK/INDEX event and its stream fence."""

    canonical: bytes
    event_id: bytes
    instrument_uid: str
    instrument_revision: int
    source_policy_id: str
    freshness_basis: str
    stale_after_ms: int
    source_event_time_ns: int
    received_at_ns: int
    connection_generation: int
    partition_sequence: int
    spool_watermark_offset: int | None
    delivery_stage: str
    gateway_epoch: int


@dataclass(frozen=True, slots=True)
class ExecutionMarkIndexRead:
    """A bounded read result; non-available values are never serialized."""

    record: ExecutionMarkIndexRecord | None
    reason: str | None = None


class ExecutionMarkIndexLiveView:
    """Bounded latest-state view keyed by exact canonical instrument identity."""

    def __init__(self, allowed_instrument_uids: frozenset[str]) -> None:
        if not allowed_instrument_uids:
            raise ValueError("execution MARK/INDEX view requires allowed bindings")
        self._allowed_instrument_uids = allowed_instrument_uids
        self._records: dict[str, ExecutionMarkIndexRecord] = {}
        self._invalid: dict[str, tuple[int, int, str]] = {}
        self._lock = asyncio.Lock()

    @classmethod
    def from_catalog(cls, catalog: StableSourceCatalog) -> "ExecutionMarkIndexLiveView":
        allowed = frozenset(
            binding.instrument.instrument_uid
            for binding in catalog.bindings
            if (
                binding.feed is FeedType.MARK_INDEX_PRICE
                and binding.authoritative
                and binding.source_role == "PRIMARY"
            )
        )
        return cls(allowed)

    async def remember(
        self,
        *,
        binding: StableSourceBinding,
        envelope: market_data_pb2.EventEnvelope,
        stored: StoredEvent | None,
        gateway_epoch: int,
    ) -> None:
        """Offer one verified canonical event in the current gateway generation.

        The stable projector only posts records it read from Kafka
        ``read_committed``.  A pre-spool view may therefore be served as a
        short-lived execution latest-state read; the secondary SQLite spool
        remains the replay source and promotes the same record once confirmed.
        """

        if binding.feed is not FeedType.MARK_INDEX_PRICE:
            return
        if not binding.authoritative or binding.source_role != "PRIMARY":
            raise ValueError("execution MARK/INDEX binding is not authoritative primary")
        if envelope.WhichOneof("payload") != "mark_index_price":
            raise ValueError("execution MARK/INDEX view received a different payload")
        uid = envelope.instrument_uid
        if uid not in self._allowed_instrument_uids:
            raise ValueError("execution MARK/INDEX event is outside declared bindings")
        if (
            envelope.instrument_id != binding.instrument.instrument_id
            or int(envelope.instrument_revision) != binding.instrument.metadata_revision
            or envelope.source_id != binding.source_id
            or envelope.source_event_time_ns <= 0
            or envelope.received_at_ns <= 0
            or gateway_epoch < 1
        ):
            raise ValueError("execution MARK/INDEX event identity/provenance is invalid")
        canonical = envelope.SerializeToString(deterministic=True)
        if stored is not None and (
            stored.event.event_id != bytes(envelope.event_id)
            or stored.event.payload != canonical
        ):
            raise ValueError("execution MARK/INDEX spool confirmation differs from canonical")
        record = ExecutionMarkIndexRecord(
            canonical=canonical,
            event_id=bytes(envelope.event_id),
            instrument_uid=uid,
            instrument_revision=int(envelope.instrument_revision),
            source_policy_id=binding.source_policy_id,
            freshness_basis=binding.freshness_basis,
            stale_after_ms=binding.stale_after_ms,
            source_event_time_ns=int(envelope.source_event_time_ns),
            received_at_ns=int(envelope.received_at_ns),
            connection_generation=int(envelope.connection_generation),
            partition_sequence=int(envelope.partition_sequence),
            spool_watermark_offset=(stored.cursor.offset if stored is not None else None),
            delivery_stage=(
                _DELIVERY_SPOOL_CONFIRMED
                if stored is not None
                else _DELIVERY_CANONICAL_READ_COMMITTED
            ),
            gateway_epoch=gateway_epoch,
        )
        has_gap = bool(_GAP_FLAGS.intersection(envelope.quality_flags))
        async with self._lock:
            current = self._records.get(uid)
            if has_gap:
                self._records.pop(uid, None)
                self._invalid[uid] = (
                    gateway_epoch,
                    record.connection_generation,
                    "GAP_OR_RESYNC",
                )
                return
            invalid = self._invalid.get(uid)
            if invalid is not None:
                invalid_epoch, invalid_generation, _reason = invalid
                if gateway_epoch < invalid_epoch:
                    return
                # A gap remains blocking until the provider has established a
                # newer connection generation.  A later frame from the same
                # uncertain session is not proof that the missing range was
                # recovered.
                if (
                    gateway_epoch == invalid_epoch
                    and record.connection_generation <= invalid_generation
                ):
                    return
                self._invalid.pop(uid, None)
            if current is not None:
                if current.gateway_epoch > gateway_epoch:
                    return
                if current.gateway_epoch == gateway_epoch:
                    # Never let a delayed frame from an older provider session
                    # overwrite the confirmed current generation.
                    if record.connection_generation < current.connection_generation:
                        return
                    if record.connection_generation == current.connection_generation:
                        current_order = (
                            current.received_at_ns,
                            current.partition_sequence,
                        )
                        incoming_order = (
                            record.received_at_ns,
                            record.partition_sequence,
                        )
                        if current_order > incoming_order:
                            return
                        if current_order == incoming_order:
                            # A spool confirmation may only promote the exact
                            # event already visible from the read-committed
                            # canonical plane.  It may not replace an equally
                            # ordered, different event.
                            if current.event_id != record.event_id:
                                return
                            if (
                                current.spool_watermark_offset is not None
                                or record.spool_watermark_offset is None
                            ):
                                return
            self._records[uid] = record

    async def withdraw(
        self,
        *,
        instrument_uid: str,
        event_id: bytes,
        gateway_epoch: int,
    ) -> None:
        """Withdraw only an unconfirmed event when its spool append fails."""

        if not instrument_uid or not event_id or gateway_epoch < 1:
            raise ValueError("execution MARK/INDEX withdrawal identity is invalid")
        async with self._lock:
            current = self._records.get(instrument_uid)
            if (
                current is not None
                and current.gateway_epoch == gateway_epoch
                and current.event_id == event_id
                and current.spool_watermark_offset is None
            ):
                self._records.pop(instrument_uid, None)

    async def read(
        self,
        *,
        instrument_uid: str,
        instrument_revision: int,
        source_policy_id: str,
        max_freshness_ms: int,
        gateway_epoch: int,
        now_ns: int | None = None,
    ) -> ExecutionMarkIndexRead:
        """Return only a view valid for this exact execution request."""

        if not instrument_uid or instrument_revision < 1 or max_freshness_ms <= 0:
            raise ValueError("execution MARK/INDEX read identity/freshness is invalid")
        now_ns = time.time_ns() if now_ns is None else now_ns
        async with self._lock:
            invalid = self._invalid.get(instrument_uid)
            if invalid is not None and invalid[0] == gateway_epoch:
                return ExecutionMarkIndexRead(None, invalid[2])
            record = self._records.get(instrument_uid)
        if record is None:
            return ExecutionMarkIndexRead(None, "NOT_READY")
        if record.gateway_epoch != gateway_epoch:
            return ExecutionMarkIndexRead(None, "FENCED")
        if record.instrument_revision != instrument_revision:
            return ExecutionMarkIndexRead(None, "IDENTITY_MISMATCH")
        if record.source_policy_id != source_policy_id:
            return ExecutionMarkIndexRead(None, "SOURCE_POLICY_MISMATCH")
        freshness_anchor_ns = (
            record.received_at_ns
            if record.freshness_basis == "PROVIDER_CONFIRMATION"
            else record.source_event_time_ns
        )
        bound_ms = min(max_freshness_ms, record.stale_after_ms)
        if max(0, (now_ns - freshness_anchor_ns) // 1_000_000) > bound_ms:
            return ExecutionMarkIndexRead(None, "STALE")
        return ExecutionMarkIndexRead(record)

    async def fence_all(self) -> None:
        """A passive/reacquired gateway may never retain old writer state."""

        async with self._lock:
            self._records.clear()
            self._invalid.clear()

    async def size(self) -> int:
        async with self._lock:
            return len(self._records)


def install_execution_mark_index_read(
    app: FastAPI,
    *,
    gateway: DurableStreamGateway,
    view: ExecutionMarkIndexLiveView,
    secret: bytes,
) -> None:
    """Install the private read edge used only by stable query replicas."""

    if len(secret) < 32:
        raise ValueError("stable internal read secret must contain at least 256 bits")

    @app.post("/internal/v2/execution/mark-index/latest", include_in_schema=False)
    async def latest_mark_index(
        request: Request,
        response: Response,
        signature: str | None = Header(None, alias="X-QDL-Stable-Signature"),
    ):
        body = await request.body()
        if not signature or not hmac.compare_digest(
            signature, stable_hmac_signature(secret, body)
        ):
            raise HTTPException(status_code=401, detail="invalid stable read signature")
        try:
            payload = json.loads(body)
            if set(payload) != {
                "schema", "instrument_uid", "instrument_revision",
                "source_policy_id", "max_freshness_ms",
            } or payload["schema"] != _REQUEST_SCHEMA:
                raise ValueError("execution MARK/INDEX read schema is invalid")
            instrument_uid = str(payload["instrument_uid"])
            instrument_revision = int(payload["instrument_revision"])
            source_policy_id = str(payload["source_policy_id"])
            max_freshness_ms = int(payload["max_freshness_ms"])
            if not instrument_uid or not source_policy_id or not 1 <= max_freshness_ms <= 300_000:
                raise ValueError("execution MARK/INDEX read fields are invalid")
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        try:
            epoch = gateway.assert_active()
            assert epoch is not None
            result = await view.read(
                instrument_uid=instrument_uid,
                instrument_revision=instrument_revision,
                source_policy_id=source_policy_id,
                max_freshness_ms=max_freshness_ms,
                gateway_epoch=epoch,
            )
            gateway.assert_active(epoch)
        except GatewayFenced as error:
            raise HTTPException(status_code=409, detail="execution MARK/INDEX gateway fenced") from error
        if result.record is None:
            raise HTTPException(
                status_code=409,
                detail=f"execution MARK/INDEX live view unavailable:{result.reason}",
            )
        record = result.record
        # A header keeps the private JSON response additive-compatible with a
        # rolling reader deployment while carrying the stream-authoritative
        # basis that governed this exact record's live admission.
        response.headers[_FRESHNESS_BASIS_HEADER] = record.freshness_basis
        return {
            "schema": _RESPONSE_SCHEMA,
            "lease_epoch": record.gateway_epoch,
            "spool_watermark_offset": record.spool_watermark_offset,
            "delivery_stage": record.delivery_stage,
            "canonical": base64.b64encode(record.canonical).decode("ascii"),
        }
