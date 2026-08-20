from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Protocol


_CONTROL_SCHEMA = "qdl.authority-control-event.v1"
_OUTBOX_SCHEMA = "qdl.authority-outbox-event.v1"
_PHASE92_STATES = {
    "RUST_CANARY", "RUST_PRIMARY", "BLOCKED",
    "ROLLBACK_PENDING", "PYTHON_PRIMARY",
}


def _timestamp_ns(value: Any) -> int:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("authority timestamp is missing or invalid")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1_000_000_000)


def _uuid(value: Any, field: str) -> str:
    text = str(value or "")
    parts = text.split("-")
    if [len(part) for part in parts] != [8, 4, 4, 4, 12] or any(
        not all(character in "0123456789abcdefABCDEF" for character in part)
        for part in parts
    ):
        raise ValueError(f"{field} is not a UUID")
    return text.lower()


def _digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _checkpoint(raw: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    result = {
        "schema": "qdl.terminal-owner-checkpoint.v1",
        "checkpoint_id": _uuid(raw.get("checkpoint_id"), "checkpoint_id"),
        "slice_id": str(raw.get("slice_id") or ""),
        "owner_id": str(raw.get("owner_id") or ""),
        "authority_revision": int(raw.get("authority_revision") or 0),
        "lease_epoch": int(raw.get("lease_epoch") or 0),
        "partition_plan_epoch": int(raw.get("partition_plan_epoch") or 0),
        "source_session_id": str(raw.get("source_session_id") or ""),
        "connection_generation": int(raw.get("connection_generation") or 0),
        "terminal_watermark": int(raw.get("terminal_watermark") or 0),
        "terminal_event_id": str(raw.get("terminal_event_id") or ""),
        "terminal_payload_sha256": str(raw.get("terminal_payload_sha256") or ""),
        "candidate_digest": str(raw.get("candidate_digest") or ""),
        "committed_at_ns": _timestamp_ns(raw.get("committed_at")),
    }
    if any(
        not result[key]
        for key in ("slice_id", "owner_id", "source_session_id", "terminal_event_id")
    ) or any(
        result[key] <= 0
        for key in (
            "authority_revision", "lease_epoch", "partition_plan_epoch",
            "connection_generation", "committed_at_ns",
        )
    ) or any(
        len(result[key]) != 64
        for key in ("terminal_payload_sha256", "candidate_digest")
    ):
        raise ValueError("terminal checkpoint is incomplete")
    return result


def _handoff(
    raw: Mapping[str, Any] | None,
    checkpoint: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if raw is None:
        return None
    if checkpoint is None:
        raise ValueError("authority handoff has no terminal checkpoint")
    result = {
        "schema": "qdl.accepted-authority-handoff.v1",
        "handoff_id": _uuid(raw.get("handoff_id"), "handoff_id"),
        "direction": str(raw.get("direction") or ""),
        "checkpoint_digest": _digest(checkpoint),
        "slice_id": str(raw.get("slice_id") or ""),
        "old_owner_id": str(raw.get("old_owner_id") or ""),
        "new_owner_id": str(raw.get("new_owner_id") or ""),
        "expected_state": str(raw.get("expected_state") or ""),
        "new_state": str(raw.get("new_state") or ""),
        "expected_authority_revision": int(raw.get("expected_authority_revision") or 0),
        "new_authority_revision": int(raw.get("new_authority_revision") or 0),
        "expected_lease_epoch": int(raw.get("expected_lease_epoch") or 0),
        "new_lease_epoch": int(raw.get("new_lease_epoch") or 0),
        "partition_plan_epoch": int(raw.get("partition_plan_epoch") or 0),
        "terminal_watermark": int(raw.get("terminal_watermark") or 0),
        "first_new_watermark": int(raw.get("first_new_watermark") or 0),
        "overlap_start_watermark": int(raw.get("overlap_start_watermark") or 0),
        "overlap_end_watermark": int(raw.get("overlap_end_watermark") or 0),
        "old_event_count": int(raw.get("old_event_count") or 0),
        "new_event_count": int(raw.get("new_event_count") or 0),
        "semantic_mismatches": int(raw.get("semantic_mismatches") or 0),
        "open_gaps": int(raw.get("open_gaps") or 0),
        "candidate_digest": str(raw.get("candidate_digest") or ""),
        "prerequisite_bundle_id": _uuid(
            raw.get("prerequisite_bundle_id"), "prerequisite_bundle_id"
        ),
        "approved_by": str(raw.get("approved_by") or ""),
        "approved_at_ns": _timestamp_ns(raw.get("approved_at")),
        "expires_at_ns": _timestamp_ns(raw.get("expires_at")),
    }
    expected_digest = str(raw.get("handoff_sha256") or "")
    actual_digest = _digest(result)
    if len(expected_digest) != 64 or actual_digest != expected_digest:
        raise ValueError("accepted handoff digest differs from immutable DB evidence")
    if (
        result["new_authority_revision"] != result["expected_authority_revision"] + 1
        or result["new_lease_epoch"] <= result["expected_lease_epoch"]
        or result["first_new_watermark"] != result["terminal_watermark"] + 1
        or result["semantic_mismatches"] != 0
        or result["open_gaps"] != 0
        or result["old_event_count"] != result["new_event_count"]
    ):
        raise ValueError("accepted handoff boundary is invalid")
    return result


def _authority(
    raw: Mapping[str, Any],
    transition: Mapping[str, Any],
    handoff: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    state = str(raw.get("state") or "")
    if state not in _PHASE92_STATES:
        return None
    terminal = raw.get("terminal_watermark")
    start_watermark = int(terminal or 0)
    primary = state in {"RUST_PRIMARY", "PYTHON_PRIMARY"}
    if primary and handoff is None:
        raise ValueError("primary authority requires accepted handoff evidence")
    approved_by = raw.get("approved_by")
    approved_at = raw.get("approved_at")
    hold_until = raw.get("hold_until")
    if state == "PYTHON_PRIMARY" and handoff is not None:
        approved_by = handoff["approved_by"]
        approved_at = handoff["approved_at_ns"]
        hold_until = handoff["expires_at_ns"]
    approved_at_ns = (
        int(approved_at)
        if isinstance(approved_at, int)
        else _timestamp_ns(approved_at)
        if approved_at is not None
        else None
    )
    hold_until_ns = (
        int(hold_until)
        if isinstance(hold_until, int)
        else _timestamp_ns(hold_until)
        if hold_until is not None
        else None
    )
    active = state in {"RUST_CANARY", "RUST_PRIMARY", "PYTHON_PRIMARY"}
    result = {
        "schema": "qdl.authority-record.v3",
        "slice_id": str(raw.get("slice_id") or ""),
        "state": state,
        "owner_id": str(raw.get("owner_id") or ""),
        "authority_revision": int(raw.get("authority_revision") or 0),
        "lease_epoch": int(raw.get("lease_epoch") or 0),
        "partition_plan_epoch": int(raw.get("partition_plan_epoch") or 0),
        "candidate_digest": str(raw.get("candidate_digest") or ""),
        "prerequisite_bundle_id": (
            str(raw["prerequisite_bundle_id"])
            if raw.get("prerequisite_bundle_id") is not None
            else None
        ),
        "start_watermark": start_watermark,
        "terminal_watermark": start_watermark if primary else None,
        "previous_owner_id": handoff["old_owner_id"] if primary else None,
        "handoff_digest": _digest(handoff) if primary else None,
        "approved_by": str(approved_by) if active and approved_by is not None else None,
        "approved_at_ns": approved_at_ns if active else None,
        "hold_until_ns": hold_until_ns if active else None,
        "public_write_allowed": primary,
        "legacy_write_allowed": primary,
    }
    if result["authority_revision"] != int(transition.get("new_revision") or 0):
        raise ValueError("authority row and transition revision differ")
    if active and (
        not result["approved_by"]
        or not result["approved_at_ns"]
        or not result["hold_until_ns"]
        or result["hold_until_ns"] <= result["approved_at_ns"]
    ):
        raise ValueError("active authority approval window is invalid")
    if state in {"RUST_CANARY", "RUST_PRIMARY"} and not result["prerequisite_bundle_id"]:
        raise ValueError("Rust canary/primary requires prerequisite bundle")
    return result


def build_authority_control_event(payload: Mapping[str, Any]) -> dict[str, Any]:
    if payload.get("schema") != _OUTBOX_SCHEMA:
        raise ValueError("unsupported authority outbox payload schema")
    event_id = _uuid(payload.get("event_id"), "event_id")
    transition = payload.get("transition")
    authority_row = payload.get("authority")
    if not isinstance(transition, Mapping) or not isinstance(authority_row, Mapping):
        raise ValueError("authority outbox snapshot is incomplete")
    checkpoint = _checkpoint(
        payload.get("checkpoint") if isinstance(payload.get("checkpoint"), Mapping) else None
    )
    handoff = _handoff(
        payload.get("handoff") if isinstance(payload.get("handoff"), Mapping) else None,
        checkpoint,
    )
    authority = _authority(authority_row, transition, handoff)
    slice_id = str(authority_row.get("slice_id") or "")
    revision = int(authority_row.get("authority_revision") or 0)
    if (
        not slice_id
        or revision <= 0
        or str(transition.get("transition_id") or "").lower() != event_id
        or str(transition.get("slice_id") or "") != slice_id
    ):
        raise ValueError("authority outbox event identity is inconsistent")
    return {
        "schema": _CONTROL_SCHEMA,
        "event_id": event_id,
        "slice_id": slice_id,
        "authority_revision": revision,
        "database_state": str(authority_row.get("state") or ""),
        "authority": authority,
        "checkpoint": checkpoint,
        "handoff": handoff,
    }


@dataclass(frozen=True, slots=True)
class ClaimedAuthorityEvent:
    event_id: str
    payload: Mapping[str, Any]
    attempts: int = 1


@dataclass(frozen=True, slots=True)
class BrokerAck:
    topic: str
    partition: int
    offset: int


class AuthorityOutboxRepository(Protocol):
    async def claim(self, lock_owner: str, limit: int) -> list[ClaimedAuthorityEvent]: ...
    async def complete(self, event_id: str, lock_owner: str, ack: BrokerAck) -> None: ...
    async def retry(self, event_id: str, lock_owner: str, error: str, delay_seconds: float) -> None: ...


class AuthorityPublisher(Protocol):
    async def publish(self, *, key: str, event_id: str, payload: bytes) -> BrokerAck: ...


class AuthorityOutboxDispatcher:
    def __init__(
        self,
        *,
        repository: AuthorityOutboxRepository,
        publisher: AuthorityPublisher,
        lock_owner: str,
        batch_size: int = 20,
    ) -> None:
        if not lock_owner.strip() or not 1 <= batch_size <= 100:
            raise ValueError("authority dispatcher identity/batch bounds are invalid")
        self.repository = repository
        self.publisher = publisher
        self.lock_owner = lock_owner
        self.batch_size = batch_size

    async def dispatch_once(self) -> int:
        claimed = await self.repository.claim(self.lock_owner, self.batch_size)
        published = 0
        for item in claimed:
            try:
                event = build_authority_control_event(item.payload)
                encoded = json.dumps(
                    event, sort_keys=True, separators=(",", ":"), ensure_ascii=True
                ).encode()
                ack = await self.publisher.publish(
                    key=event["slice_id"], event_id=item.event_id, payload=encoded
                )
                await self.repository.complete(item.event_id, self.lock_owner, ack)
                published += 1
            except Exception as error:
                delay = min(60.0, 0.5 * 2 ** min(7, max(0, item.attempts - 1)))
                await self.repository.retry(
                    item.event_id, self.lock_owner, str(error), delay
                )
        return published


class AsyncpgAuthorityOutboxRepository:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @classmethod
    async def connect(cls, dsn: str, *, min_size: int = 1, max_size: int = 4):
        try:
            import asyncpg
        except ImportError as error:
            raise RuntimeError("authority dispatcher requires asyncpg") from error
        pool = await asyncpg.create_pool(
            dsn=dsn, min_size=min_size, max_size=max_size,
            command_timeout=15, server_settings={"application_name": "qdl-authority-outbox"},
        )
        return cls(pool)

    async def claim(self, lock_owner: str, limit: int) -> list[ClaimedAuthorityEvent]:
        rows = await self._pool.fetch(
            "SELECT event_id, payload FROM qdl_claim_authority_outbox($1, $2)",
            lock_owner, limit,
        )
        return [
            ClaimedAuthorityEvent(
                str(row["event_id"]), row["payload"], int(row["attempts"])
            )
            for row in rows
        ]

    async def complete(self, event_id: str, lock_owner: str, ack: BrokerAck) -> None:
        await self._pool.execute(
            "SELECT qdl_complete_authority_outbox($1::uuid, $2, $3, $4, $5)",
            event_id, lock_owner, ack.topic, ack.partition, ack.offset,
        )

    async def retry(self, event_id: str, lock_owner: str, error: str, delay_seconds: float) -> None:
        await self._pool.execute(
            "SELECT qdl_retry_authority_outbox($1::uuid, $2, $3, make_interval(secs => $4))",
            event_id, lock_owner, error, delay_seconds,
        )

    async def close(self) -> None:
        await self._pool.close()


class KafkaAuthorityPublisher:
    def __init__(self, config: Mapping[str, Any], *, topic: str, timeout_seconds: float = 15.0):
        if not topic.strip() or timeout_seconds <= 0:
            raise ValueError("authority Kafka topic/timeout is invalid")
        from confluent_kafka import Producer
        values = dict(config)
        values.update({
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "max.in.flight.requests.per.connection": 5,
        })
        self._producer = Producer(values)
        self._topic = topic
        self._timeout = timeout_seconds

    async def publish(self, *, key: str, event_id: str, payload: bytes) -> BrokerAck:
        loop = asyncio.get_running_loop()
        completed: asyncio.Future[BrokerAck] = loop.create_future()

        def delivery(error, message) -> None:
            if error is not None:
                loop.call_soon_threadsafe(completed.set_exception, RuntimeError(str(error)))
            else:
                loop.call_soon_threadsafe(
                    completed.set_result,
                    BrokerAck(message.topic(), message.partition(), message.offset()),
                )

        self._producer.produce(
            self._topic,
            key=key.encode(),
            value=payload,
            headers={"qdl-event-id": event_id.encode()},
            on_delivery=delivery,
        )
        deadline = loop.time() + self._timeout
        while not completed.done():
            self._producer.poll(0)
            if loop.time() >= deadline:
                raise TimeoutError("authority Kafka durable ACK timed out")
            await asyncio.sleep(0.01)
        return await completed
