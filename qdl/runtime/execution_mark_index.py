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
from typing import Mapping

from fastapi import FastAPI, Header, HTTPException, Request, Response

from qdl.common.v1 import common_pb2
from qdl.data_quality.binding_decision import (
    BindingQualityInput,
    ComponentEvidence,
    evaluate_binding_quality,
)
from qdl.marketdata.v2 import market_data_pb2
from qdl.query.contracts import FeedType, StalePolicy
from qdl.runtime.internal_auth import stable_hmac_signature
from qdl.runtime.lease import GatewayFenced
from qdl.runtime.mark_index_lineage import paired_mark_index_lineage
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl.runtime.session_liveness import (
    ProviderSessionStatus,
    StableSessionLivenessReader,
)
from qdl.stream import DurableStreamGateway
from qdl.transport import SQLiteDurableSpool, StoredEvent


_LEGACY_REQUEST_SCHEMA = "qdl.v2.execution-mark-index-read.v1"
_REQUEST_SCHEMA = "qdl.v2.execution-mark-index-read.v2"
_RESPONSE_SCHEMA = "qdl.v2.execution-mark-index-view.v2"
_DELIVERY_CANONICAL_READ_COMMITTED = "CANONICAL_READ_COMMITTED"
_DELIVERY_SPOOL_CONFIRMED = "SPOOL_CONFIRMED"
_FRESHNESS_BASIS_HEADER = "X-QDL-Execution-Freshness-Basis"
_RECENCY_MODE_HEADER = "X-QDL-Execution-Recency-Mode"
_SESSION_STATE_HEADER = "X-QDL-Execution-Session-State"
_SESSION_LIVENESS_HEADER = "X-QDL-Execution-Session-Liveness-Ms"
_SESSION_CHECKED_AT_HEADER = "X-QDL-Execution-Session-Checked-At-Ns"
_COMPONENT_RECEIPTS_HEADER = "X-QDL-Execution-Component-Receipts-Ns"
_COMPONENT_CADENCE_HEADER = "X-QDL-Execution-Component-Quiet-After-Ms"
_GAP_FLAGS = frozenset({
    common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE,
    common_pb2.QUALITY_FLAG_OUT_OF_ORDER,
    common_pb2.QUALITY_FLAG_RESYNC_REQUIRED,
})


@dataclass(frozen=True, slots=True)
class ExecutionMarkIndexQuietPolicy:
    """Signed component cadence for one logical MARK/INDEX product."""

    component_quiet_after_ms: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        values = dict(self.component_quiet_after_ms)
        if (
            set(values) != {"MARK", "INDEX"}
            or any(not 250 <= value <= 120_000 for value in values.values())
        ):
            raise ValueError("execution MARK/INDEX quiet policy is incomplete")

    @classmethod
    def from_acquisition(cls, acquisition) -> "ExecutionMarkIndexQuietPolicy | None":
        mark_index = acquisition.mark_index
        if mark_index is None or not mark_index.component_quiet_after_ms:
            return None
        values = dict(mark_index.component_quiet_after_ms)
        if "BOTH" in values:
            values = {"MARK": values["BOTH"], "INDEX": values["BOTH"]}
        return cls(tuple(sorted((str(name), int(value)) for name, value in values.items())))


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
    venue: str
    market: str
    source_session_id: str
    config_revision: int
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
    recency_mode: str | None = None
    session: ProviderSessionStatus | None = None
    session_checked_at_ns: int | None = None
    component_receipts_ns: tuple[tuple[str, int], ...] = ()
    component_quiet_after_ms: tuple[tuple[str, int], ...] = ()


class ExecutionMarkIndexLiveView:
    """Bounded latest-state view keyed by exact canonical instrument identity."""

    def __init__(
        self,
        allowed_instrument_uids: frozenset[str],
        *,
        quiet_policies: Mapping[str, ExecutionMarkIndexQuietPolicy] | None = None,
        session_liveness_reader: StableSessionLivenessReader | None = None,
        bindings: Mapping[str, StableSourceBinding] | None = None,
    ) -> None:
        if not allowed_instrument_uids:
            raise ValueError("execution MARK/INDEX view requires allowed bindings")
        policies = dict(quiet_policies or {})
        if not set(policies).issubset(allowed_instrument_uids):
            raise ValueError("execution MARK/INDEX quiet policy is outside allowed bindings")
        declared_bindings = dict(bindings or {})
        if declared_bindings and set(declared_bindings) != set(allowed_instrument_uids):
            raise ValueError("execution MARK/INDEX hydration bindings are incomplete")
        if any(
            binding.instrument.instrument_uid != instrument_uid
            or binding.feed is not FeedType.MARK_INDEX_PRICE
            or not binding.authoritative
            or binding.source_role != "PRIMARY"
            for instrument_uid, binding in declared_bindings.items()
        ):
            raise ValueError("execution MARK/INDEX hydration binding is invalid")
        self._allowed_instrument_uids = allowed_instrument_uids
        self._quiet_policies = policies
        self._session_liveness_reader = session_liveness_reader
        self._bindings = declared_bindings
        self._records: dict[str, ExecutionMarkIndexRecord] = {}
        self._invalid: dict[str, tuple[int, int, str]] = {}
        self._lock = asyncio.Lock()

    @classmethod
    def from_catalog(
        cls,
        catalog: StableSourceCatalog,
        *,
        acquisition: StableAcquisitionPlan | None = None,
        session_liveness_reader: StableSessionLivenessReader | None = None,
    ) -> "ExecutionMarkIndexLiveView":
        bindings = tuple(
            binding
            for binding in catalog.bindings
            if (
                binding.feed is FeedType.MARK_INDEX_PRICE
                and binding.authoritative
                and binding.source_role == "PRIMARY"
            )
        )
        bindings_by_uid: dict[str, StableSourceBinding] = {}
        for binding in bindings:
            existing = bindings_by_uid.setdefault(binding.instrument.instrument_uid, binding)
            if existing != binding:
                raise ValueError("execution MARK/INDEX binding is ambiguous")
        allowed = frozenset(bindings_by_uid)
        quiet_policies: dict[str, ExecutionMarkIndexQuietPolicy] = {}
        if acquisition is not None:
            acquisitions = {item.binding_id: item for item in acquisition.bindings}
            for binding in bindings:
                try:
                    candidate = acquisitions[binding.binding_id]
                except KeyError as error:
                    raise ValueError(
                        "execution MARK/INDEX acquisition binding is unavailable"
                    ) from error
                policy = ExecutionMarkIndexQuietPolicy.from_acquisition(candidate)
                if policy is not None:
                    existing = quiet_policies.setdefault(
                        binding.instrument.instrument_uid, policy
                    )
                    if existing != policy:
                        raise ValueError(
                            "execution MARK/INDEX quiet policy differs for one instrument"
                        )
        return cls(
            allowed,
            quiet_policies=quiet_policies,
            session_liveness_reader=session_liveness_reader,
            bindings=bindings_by_uid,
        )

    async def hydrate_from_spool(
        self,
        *,
        spool: SQLiteDurableSpool,
        canonical_stream: str,
        gateway_epoch: int,
    ) -> int:
        """Restore one bounded durable latest record per declared binding.

        A lease acquisition must not wait for a new update-on-change provider
        frame when the exact canonical event is already durably committed. The
        restored record still travels through ``remember`` and therefore keeps
        all normal identity, gap, generation and later session/freshness gates.
        This method never publishes, replays, rewrites timestamps or contacts a
        provider.
        """

        if not canonical_stream.strip() or gateway_epoch < 1:
            raise ValueError("execution MARK/INDEX hydration scope is invalid")
        restored = 0
        for instrument_uid, binding in sorted(self._bindings.items()):
            records = await asyncio.to_thread(
                spool.read_tail,
                stream=canonical_stream,
                partition_key=binding.partition_key,
                limit=1,
            )
            if not records:
                continue
            stored = records[-1]
            if (
                stored.event.stream != canonical_stream
                or stored.cursor.stream != canonical_stream
                or stored.event.partition_key != binding.partition_key
                or stored.cursor.partition_key != binding.partition_key
            ):
                raise ValueError("execution MARK/INDEX hydration spool identity differs")
            try:
                envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
            except DecodeError as error:
                raise ValueError("execution MARK/INDEX hydration canonical payload is invalid") from error
            await self.remember(
                binding=binding,
                envelope=envelope,
                stored=stored,
                gateway_epoch=gateway_epoch,
            )
            restored += 1
        return restored

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
            or not envelope.source_session_id
            or envelope.connection_generation < 1
            or envelope.config_revision < 1
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
            venue=str(envelope.venue),
            market=str(envelope.market),
            source_session_id=str(envelope.source_session_id),
            config_revision=int(envelope.config_revision),
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
        event_recency_policy: StalePolicy = StalePolicy.BLOCK,
        max_session_liveness_ms: int | None = None,
        now_ns: int | None = None,
    ) -> ExecutionMarkIndexRead:
        """Return only a view valid for this exact execution request."""

        if (
            not instrument_uid
            or instrument_revision < 1
            or max_freshness_ms <= 0
            or not isinstance(event_recency_policy, StalePolicy)
        ):
            raise ValueError("execution MARK/INDEX read identity/freshness is invalid")
        if (
            event_recency_policy is StalePolicy.UNSPECIFIED
            or (
                event_recency_policy is StalePolicy.OBSERVE
                and (max_session_liveness_ms is None or max_session_liveness_ms <= 0)
            )
        ):
            raise ValueError("execution MARK/INDEX quiet policy is invalid")
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
        event_age_ms = max(0, (now_ns - freshness_anchor_ns) // 1_000_000)
        if event_recency_policy is not StalePolicy.OBSERVE:
            decision = evaluate_binding_quality(BindingQualityInput(
                binding_id=f"execution-mark-index:{instrument_uid}",
                instrument_uid=instrument_uid,
                feed=FeedType.MARK_INDEX_PRICE.value,
                source_role="PRIMARY",
                authoritative=True,
                acquisition_enabled=True,
                acquisition_mode="RUST_NATIVE",
                market_open=True,
                event_present=True,
                event_age_ms=event_age_ms,
                event_limit_ms=bound_ms,
                event_recency_policy=event_recency_policy.value,
                session_state="NOT_APPLICABLE",
                session_liveness_ms=None,
                session_limit_ms=None,
                watermark_offset=record.spool_watermark_offset or 0,
            ))
            if decision.state != "LIVE":
                return ExecutionMarkIndexRead(None, "STALE")
            return ExecutionMarkIndexRead(record, recency_mode="STRICT_EVENT")

        policy = self._quiet_policies.get(instrument_uid)
        if policy is None or self._session_liveness_reader is None:
            return ExecutionMarkIndexRead(None, "QUIET_POLICY_UNAVAILABLE")
        try:
            envelope = market_data_pb2.EventEnvelope.FromString(record.canonical)
            lineage = paired_mark_index_lineage(envelope)
        except (TypeError, ValueError):
            return ExecutionMarkIndexRead(None, "LINEAGE_INVALID")
        component_receipts = (
            ("MARK", lineage.mark_received_at_ns),
            ("INDEX", lineage.index_received_at_ns),
        )
        component_cadence = policy.component_quiet_after_ms
        cadence_by_component = dict(component_cadence)
        session = self._session_liveness_reader.status(
            venue=record.venue,
            market=record.market,
            source_session_id=record.source_session_id,
            connection_generation=record.connection_generation,
            config_revision=record.config_revision,
            now_ns=now_ns,
        )
        assert max_session_liveness_ms is not None
        decision = evaluate_binding_quality(BindingQualityInput(
            binding_id=f"execution-mark-index:{instrument_uid}",
            instrument_uid=instrument_uid,
            feed=FeedType.MARK_INDEX_PRICE.value,
            source_role="PRIMARY",
            authoritative=True,
            acquisition_enabled=True,
            acquisition_mode="RUST_NATIVE",
            market_open=True,
            event_present=True,
            event_age_ms=event_age_ms,
            event_limit_ms=bound_ms,
            event_recency_policy=event_recency_policy.value,
            session_state=session.state,
            session_liveness_ms=session.liveness_ms,
            session_limit_ms=max_session_liveness_ms,
            components=tuple(
                ComponentEvidence(
                    component,
                    max(0, (now_ns - receipt_ns) // 1_000_000),
                    cadence_by_component[component],
                )
                for component, receipt_ns in component_receipts
            ),
            generation_matches="SOURCE_SESSION_AMBIGUOUS" not in session.flags,
            config_matches="SOURCE_SESSION_CONFIG_MISMATCH" not in session.flags,
            watermark_offset=record.spool_watermark_offset or 0,
            allow_quiet_execution=True,
            flags=session.flags,
        ))
        if decision.state != "LIVE":
            if any(value.startswith("COMPONENT_") for value in decision.reason_codes):
                return ExecutionMarkIndexRead(None, "COMPONENT_STALE")
            if session.state != "LIVE":
                return ExecutionMarkIndexRead(None, "SESSION_STATE")
            if (
                session.liveness_ms is None
                or session.liveness_ms > max_session_liveness_ms
            ):
                return ExecutionMarkIndexRead(None, "SESSION_LIVENESS")
            return ExecutionMarkIndexRead(None, "STALE")
        return ExecutionMarkIndexRead(
            record,
            recency_mode=(
                "STRICT_EVENT_SESSION_LIVE"
                if decision.event_recency_state == "LIVE"
                else "COMPONENT_SESSION_LIVE"
            ),
            session=session,
            session_checked_at_ns=now_ns,
            component_receipts_ns=component_receipts,
            component_quiet_after_ms=component_cadence,
        )

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
            legacy_fields = {
                "schema", "instrument_uid", "instrument_revision",
                "source_policy_id", "max_freshness_ms",
            }
            quiet_fields = legacy_fields | {
                "event_recency_policy", "max_session_liveness_ms",
            }
            if (
                not isinstance(payload, dict)
                or payload.get("schema") not in {
                    _LEGACY_REQUEST_SCHEMA,
                    _REQUEST_SCHEMA,
                }
                or (
                    payload["schema"] == _LEGACY_REQUEST_SCHEMA
                    and set(payload) != legacy_fields
                )
                or (
                    payload["schema"] == _REQUEST_SCHEMA
                    and set(payload) != quiet_fields
                )
            ):
                raise ValueError("execution MARK/INDEX read schema is invalid")
            instrument_uid = str(payload["instrument_uid"])
            instrument_revision = int(payload["instrument_revision"])
            source_policy_id = str(payload["source_policy_id"])
            max_freshness_ms = int(payload["max_freshness_ms"])
            if payload["schema"] == _REQUEST_SCHEMA:
                event_recency_policy = StalePolicy(
                    str(payload["event_recency_policy"]).upper()
                )
                max_session_liveness_ms = int(payload["max_session_liveness_ms"])
                if event_recency_policy is not StalePolicy.OBSERVE:
                    raise ValueError("quiet execution MARK/INDEX policy is invalid")
            else:
                event_recency_policy = StalePolicy.BLOCK
                max_session_liveness_ms = None
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
                event_recency_policy=event_recency_policy,
                max_session_liveness_ms=max_session_liveness_ms,
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
        if result.recency_mode is not None:
            response.headers[_RECENCY_MODE_HEADER] = result.recency_mode
        if result.session is not None and result.session_checked_at_ns is not None:
            response.headers[_SESSION_STATE_HEADER] = result.session.state
            response.headers[_SESSION_LIVENESS_HEADER] = str(
                result.session.liveness_ms
            )
            response.headers[_SESSION_CHECKED_AT_HEADER] = str(
                result.session_checked_at_ns
            )
            response.headers[_COMPONENT_RECEIPTS_HEADER] = _component_header(
                result.component_receipts_ns
            )
            response.headers[_COMPONENT_CADENCE_HEADER] = _component_header(
                result.component_quiet_after_ms
            )
        return {
            "schema": _RESPONSE_SCHEMA,
            "lease_epoch": record.gateway_epoch,
            "spool_watermark_offset": record.spool_watermark_offset,
            "delivery_stage": record.delivery_stage,
            "canonical": base64.b64encode(record.canonical).decode("ascii"),
        }


def _component_header(values: tuple[tuple[str, int], ...]) -> str:
    """Encode fixed component evidence into one additive private header."""

    if set(name for name, _value in values) != {"MARK", "INDEX"}:
        raise ValueError("execution MARK/INDEX component header is incomplete")
    return ",".join(f"{name}={value}" for name, value in sorted(values))
