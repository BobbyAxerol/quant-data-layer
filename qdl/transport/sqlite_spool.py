from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from qdl.transport.contracts import (
    AppendResult,
    BackpressureRequired,
    CheckpointRegression,
    Cursor,
    CursorExpired,
    DurableEvent,
    EventIdCollision,
    PayloadCorruption,
    StoredEvent,
)


@dataclass(frozen=True)
class SpoolConfig:
    path: Path
    max_records: int = 100_000
    max_payload_bytes: int = 256 * 1024 * 1024
    max_event_bytes: int = 2 * 1024 * 1024
    max_batch_events: int = 1000
    max_storage_bytes: int = 384 * 1024 * 1024
    max_partitions: int = 1024
    max_consumer_checkpoints: int = 4096
    max_quarantine_records: int = 10_000
    min_free_disk_bytes: int = 512 * 1024 * 1024
    consumer_ttl_seconds: int = 3600
    replay_retention_seconds: int = 24 * 3600
    maintenance_interval_seconds: int = 30

    def __post_init__(self) -> None:
        if self.max_records <= 0 or self.max_payload_bytes <= 0:
            raise ValueError("spool bounds must be positive")
        if self.max_event_bytes <= 0 or self.max_event_bytes > self.max_payload_bytes:
            raise ValueError("max_event_bytes must fit inside max_payload_bytes")
        if self.max_batch_events <= 0:
            raise ValueError("max_batch_events must be positive")
        if self.max_storage_bytes <= self.max_event_bytes:
            raise ValueError("max_storage_bytes must exceed max_event_bytes")
        if min(
            self.max_partitions,
            self.max_consumer_checkpoints,
            self.max_quarantine_records,
        ) <= 0:
            raise ValueError("metadata bounds must be positive")
        if self.min_free_disk_bytes < 0:
            raise ValueError("min_free_disk_bytes must be non-negative")
        if min(
            self.consumer_ttl_seconds,
            self.replay_retention_seconds,
            self.maintenance_interval_seconds,
        ) <= 0:
            raise ValueError("retention, maintenance and consumer TTL must be positive")


@dataclass(frozen=True)
class SpoolStats:
    records: int
    payload_bytes: int
    storage_bytes: int
    max_records: int
    max_payload_bytes: int
    oldest_accepted_at_ns: int | None
    newest_accepted_at_ns: int | None

    @property
    def utilization(self) -> float:
        return max(
            self.records / self.max_records,
            self.payload_bytes / self.max_payload_bytes,
        )


class SQLiteDurableSpool:
    """Bounded, fsync-backed migration bridge with portable logical cursors.

    The bridge is deliberately local and shadow-only. SQLite row IDs never
    leave this class; callers receive a per-stream/partition logical cursor.
    """

    def __init__(self, config: SpoolConfig, *, clock_ns=time.time_ns):
        self.config = config
        self._clock_ns = clock_ns
        self._lock = threading.RLock()
        config.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            str(config.path), timeout=10.0, isolation_level=None, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        self._configure()
        self._migrate()
        self._validate_integrity()

    def _configure(self) -> None:
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA busy_timeout=10000")
        self._connection.execute("PRAGMA wal_autocheckpoint=100")
        self._connection.execute("PRAGMA journal_size_limit=16777216")

    def _migrate(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS partitions (
                stream TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                next_offset INTEGER NOT NULL,
                PRIMARY KEY (stream, partition_key)
            );

            CREATE TABLE IF NOT EXISTS events (
                stream TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                logical_offset INTEGER NOT NULL,
                event_id BLOB NOT NULL,
                payload BLOB NOT NULL,
                payload_sha256 TEXT NOT NULL,
                accepted_at_ns INTEGER NOT NULL,
                committed_at_ns INTEGER NOT NULL,
                content_type TEXT NOT NULL,
                headers_json TEXT NOT NULL,
                PRIMARY KEY (stream, partition_key, logical_offset),
                UNIQUE (stream, event_id)
            );
            DROP INDEX IF EXISTS idx_qdl_spool_events_retention;
            CREATE INDEX IF NOT EXISTS idx_qdl_spool_events_retention
                ON events (committed_at_ns);

            CREATE TABLE IF NOT EXISTS consumer_checkpoints (
                consumer_id TEXT NOT NULL,
                stream TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                logical_offset INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL,
                expires_at_ns INTEGER NOT NULL,
                PRIMARY KEY (consumer_id, stream, partition_key)
            );
            CREATE INDEX IF NOT EXISTS idx_qdl_spool_consumers_expiry
                ON consumer_checkpoints (expires_at_ns);

            CREATE TABLE IF NOT EXISTS quarantine (
                quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                event_id BLOB NOT NULL,
                payload_sha256 TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                reason_message TEXT NOT NULL,
                retry_count INTEGER NOT NULL,
                quarantined_at_ns INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS spool_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                event_records INTEGER NOT NULL,
                payload_bytes INTEGER NOT NULL,
                last_maintenance_ns INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO spool_state(
                singleton, event_records, payload_bytes, last_maintenance_ns
            )
            SELECT 1, COUNT(*), COALESCE(SUM(LENGTH(payload)), 0), 0 FROM events;
            """
        )

    def append(self, event: DurableEvent) -> AppendResult:
        return self.append_many([event])[0]

    def append_many(self, events: list[DurableEvent]) -> list[AppendResult]:
        if not events:
            return []
        if len(events) > self.config.max_batch_events:
            raise BackpressureRequired("event batch exceeds configured bridge bound")
        if any(len(event.payload) > self.config.max_event_bytes for event in events):
            raise BackpressureRequired("event exceeds configured per-event bridge bound")
        total_input_bytes = sum(len(event.payload) for event in events)

        with self._lock:
            self._preflight_disk(total_input_bytes)
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self._maybe_maintain_locked(self._clock_ns())
                records, payload_bytes = self._logical_usage_locked()
                partition_count = int(
                    self._connection.execute("SELECT COUNT(*) FROM partitions").fetchone()[0]
                )
                added_records = 0
                added_payload_bytes = 0
                results = []
                for event in events:
                    digest = hashlib.sha256(event.payload).hexdigest()
                    headers_json = json.dumps(
                        dict(event.headers), sort_keys=True, separators=(",", ":")
                    )
                    existing = self._connection.execute(
                        """
                        SELECT partition_key, logical_offset, payload_sha256, committed_at_ns
                        FROM events WHERE stream = ? AND event_id = ?
                        """,
                        (event.stream, event.event_id),
                    ).fetchone()
                    if existing is not None:
                        if (
                            existing["partition_key"] != event.partition_key
                            or existing["payload_sha256"] != digest
                        ):
                            raise EventIdCollision(
                                "event ID maps to different immutable content"
                            )
                        results.append(
                            AppendResult(
                                cursor=Cursor(
                                    event.stream,
                                    existing["partition_key"],
                                    int(existing["logical_offset"]),
                                ),
                                committed_at_ns=int(existing["committed_at_ns"]),
                                duplicate=True,
                                payload_sha256=digest,
                            )
                        )
                        continue

                    if records + added_records + 1 > self.config.max_records:
                        raise BackpressureRequired("bridge max_records exhausted")
                    if (
                        payload_bytes + added_payload_bytes + len(event.payload)
                        > self.config.max_payload_bytes
                    ):
                        raise BackpressureRequired("bridge max_payload_bytes exhausted")

                    row = self._connection.execute(
                        """
                        SELECT next_offset FROM partitions
                        WHERE stream = ? AND partition_key = ?
                        """,
                        (event.stream, event.partition_key),
                    ).fetchone()
                    offset = int(row["next_offset"]) if row else 1
                    if row is None:
                        if partition_count >= self.config.max_partitions:
                            raise BackpressureRequired("bridge max_partitions exhausted")
                        self._connection.execute(
                            """
                            INSERT INTO partitions(stream, partition_key, next_offset)
                            VALUES (?, ?, ?)
                            """,
                            (event.stream, event.partition_key, 2),
                        )
                        partition_count += 1
                    else:
                        self._connection.execute(
                            """
                            UPDATE partitions SET next_offset = ?
                            WHERE stream = ? AND partition_key = ?
                            """,
                            (offset + 1, event.stream, event.partition_key),
                        )
                    committed_at_ns = self._clock_ns()
                    self._connection.execute(
                        """
                        INSERT INTO events(
                            stream, partition_key, logical_offset, event_id, payload,
                            payload_sha256, accepted_at_ns, committed_at_ns,
                            content_type, headers_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            event.stream,
                            event.partition_key,
                            offset,
                            event.event_id,
                            event.payload,
                            digest,
                            event.accepted_at_ns,
                            committed_at_ns,
                            event.content_type,
                            headers_json,
                        ),
                    )
                    added_records += 1
                    added_payload_bytes += len(event.payload)
                    results.append(
                        AppendResult(
                            cursor=Cursor(event.stream, event.partition_key, offset),
                            committed_at_ns=committed_at_ns,
                            duplicate=False,
                            payload_sha256=digest,
                        )
                    )
                self._connection.execute(
                    """
                    UPDATE spool_state
                    SET event_records = event_records + ?,
                        payload_bytes = payload_bytes + ?
                    WHERE singleton = 1
                    """,
                    (added_records, added_payload_bytes),
                )
                self._connection.execute("COMMIT")
                return results
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def read(
        self,
        *,
        stream: str,
        partition_key: str,
        after: Cursor | None = None,
        limit: int = 100,
    ) -> list[StoredEvent]:
        if limit <= 0 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        if after and (after.stream != stream or after.partition_key != partition_key):
            raise ValueError("cursor does not belong to requested stream/partition")
        offset = after.offset if after else 0
        with self._lock:
            oldest = self._connection.execute(
                "SELECT MIN(logical_offset) FROM events WHERE stream = ? AND partition_key = ?",
                (stream, partition_key),
            ).fetchone()[0]
            if oldest is not None and offset < int(oldest) - 1:
                raise CursorExpired(
                    f"cursor {offset} predates oldest retained offset {int(oldest)}"
                )
            rows = self._connection.execute(
                """
                SELECT * FROM events
                WHERE stream = ? AND partition_key = ? AND logical_offset > ?
                ORDER BY logical_offset ASC LIMIT ?
                """,
                (stream, partition_key, offset, limit),
            ).fetchall()
        return [self._stored_event(row) for row in rows]

    def find_event(self, *, stream: str, event_id: bytes) -> StoredEvent | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM events WHERE stream = ? AND event_id = ?",
                (stream, event_id),
            ).fetchone()
        return self._stored_event(row) if row is not None else None

    def register_consumer(
        self,
        *,
        consumer_id: str,
        stream: str,
        partition_key: str,
        after_offset: int = 0,
        ttl_seconds: int | None = None,
    ) -> Cursor:
        if not consumer_id.strip():
            raise ValueError("consumer_id is required")
        cursor = Cursor(stream, partition_key, after_offset)
        self.checkpoint(
            consumer_id=consumer_id,
            cursor=cursor,
            ttl_seconds=ttl_seconds or self.config.consumer_ttl_seconds,
        )
        return cursor

    def checkpoint(
        self,
        *,
        consumer_id: str,
        cursor: Cursor,
        ttl_seconds: int,
    ) -> None:
        if not consumer_id.strip():
            raise ValueError("consumer_id is required")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        now_ns = self._clock_ns()
        with self._lock:
            high = self.high_watermark(cursor.stream, cursor.partition_key)
            if cursor.offset > high:
                raise ValueError("checkpoint is beyond the partition high watermark")
            current = self._connection.execute(
                """
                SELECT logical_offset FROM consumer_checkpoints
                WHERE consumer_id = ? AND stream = ? AND partition_key = ?
                """,
                (consumer_id, cursor.stream, cursor.partition_key),
            ).fetchone()
            if current is not None and cursor.offset < int(current["logical_offset"]):
                raise CheckpointRegression("consumer checkpoint cannot move backwards")
            if current is None:
                checkpoint_count = self._connection.execute(
                    "SELECT COUNT(*) FROM consumer_checkpoints"
                ).fetchone()[0]
                if int(checkpoint_count) >= self.config.max_consumer_checkpoints:
                    raise BackpressureRequired("bridge consumer checkpoint bound exhausted")
            self._connection.execute(
                """
                INSERT INTO consumer_checkpoints(
                    consumer_id, stream, partition_key, logical_offset,
                    updated_at_ns, expires_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(consumer_id, stream, partition_key) DO UPDATE SET
                    logical_offset = excluded.logical_offset,
                    updated_at_ns = excluded.updated_at_ns,
                    expires_at_ns = excluded.expires_at_ns
                """,
                (
                    consumer_id,
                    cursor.stream,
                    cursor.partition_key,
                    cursor.offset,
                    now_ns,
                    now_ns + ttl_seconds * 1_000_000_000,
                ),
            )

    def get_checkpoint(
        self, *, consumer_id: str, stream: str, partition_key: str
    ) -> Cursor | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT logical_offset FROM consumer_checkpoints
                WHERE consumer_id = ? AND stream = ? AND partition_key = ?
                  AND expires_at_ns > ?
                """,
                (consumer_id, stream, partition_key, self._clock_ns()),
            ).fetchone()
        if row is None:
            return None
        return Cursor(stream, partition_key, int(row["logical_offset"]))

    def trim_consumed(self, *, now_ns: int | None = None) -> int:
        """Delete only records acknowledged by every active consumer."""

        effective_now = now_ns or self._clock_ns()
        deleted = 0
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self._expire_consumers_locked(effective_now)
                partitions = self._connection.execute(
                    "SELECT DISTINCT stream, partition_key FROM events"
                ).fetchall()
                for row in partitions:
                    checkpoint = self._connection.execute(
                        """
                        SELECT MIN(logical_offset) FROM consumer_checkpoints
                        WHERE stream = ? AND partition_key = ? AND expires_at_ns > ?
                        """,
                        (row["stream"], row["partition_key"], effective_now),
                    ).fetchone()[0]
                    if checkpoint is None:
                        continue
                    removed = self._connection.execute(
                        """
                        SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0)
                        FROM events
                        WHERE stream = ? AND partition_key = ? AND logical_offset <= ?
                        """,
                        (row["stream"], row["partition_key"], int(checkpoint)),
                    ).fetchone()
                    result = self._connection.execute(
                        """
                        DELETE FROM events
                        WHERE stream = ? AND partition_key = ? AND logical_offset <= ?
                        """,
                        (row["stream"], row["partition_key"], int(checkpoint)),
                    )
                    deleted += result.rowcount
                    self._decrement_usage_locked(int(removed[0]), int(removed[1]))
                self._connection.execute("COMMIT")
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise
        return deleted

    def quarantine(
        self,
        *,
        event: DurableEvent,
        reason_code: str,
        reason_message: str,
        retry_count: int,
    ) -> int:
        if not reason_code.strip() or retry_count < 0:
            raise ValueError("valid quarantine reason and retry_count are required")
        with self._lock:
            count = self._connection.execute(
                "SELECT COUNT(*) FROM quarantine"
            ).fetchone()[0]
            if int(count) >= self.config.max_quarantine_records:
                raise BackpressureRequired("bridge quarantine bound exhausted")
            result = self._connection.execute(
                """
                INSERT INTO quarantine(
                    stream, partition_key, event_id, payload_sha256, reason_code,
                    reason_message, retry_count, quarantined_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.stream,
                    event.partition_key,
                    event.event_id,
                    hashlib.sha256(event.payload).hexdigest(),
                    reason_code,
                    reason_message,
                    retry_count,
                    self._clock_ns(),
                ),
            )
        return int(result.lastrowid)

    def quarantine_records(self, *, limit: int = 100) -> list[dict[str, int | str]]:
        if limit <= 0 or limit > 1000:
            raise ValueError("quarantine limit must be between 1 and 1000")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT quarantine_id, stream, partition_key, hex(event_id) AS event_id_hex,
                       payload_sha256, reason_code, reason_message, retry_count,
                       quarantined_at_ns
                FROM quarantine ORDER BY quarantine_id ASC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def high_watermark(self, stream: str, partition_key: str) -> int:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT next_offset FROM partitions
                WHERE stream = ? AND partition_key = ?
                """,
                (stream, partition_key),
            ).fetchone()
        return int(row["next_offset"]) - 1 if row else 0

    def stats(self) -> SpoolStats:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT COUNT(*) AS records,
                       COALESCE(SUM(LENGTH(payload)), 0) AS payload_bytes,
                       MIN(accepted_at_ns) AS oldest,
                       MAX(accepted_at_ns) AS newest
                FROM events
                """
            ).fetchone()
        return SpoolStats(
            records=int(row["records"]),
            payload_bytes=int(row["payload_bytes"]),
            storage_bytes=self.storage_bytes(),
            max_records=self.config.max_records,
            max_payload_bytes=self.config.max_payload_bytes,
            oldest_accepted_at_ns=int(row["oldest"]) if row["oldest"] is not None else None,
            newest_accepted_at_ns=int(row["newest"]) if row["newest"] is not None else None,
        )

    def storage_bytes(self) -> int:
        return sum(
            path.stat().st_size
            for path in (
                self.config.path,
                Path(f"{self.config.path}-wal"),
                Path(f"{self.config.path}-shm"),
            )
            if path.exists()
        )

    def close(self) -> None:
        with self._lock:
            if self._connection is None:
                return
            self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self._connection.close()
            self._connection = None

    def integrity_check(self) -> bool:
        with self._lock:
            return self._connection.execute("PRAGMA quick_check(1)").fetchone()[0] == "ok"

    def _validate_integrity(self) -> None:
        if not self.integrity_check():
            raise PayloadCorruption("SQLite durable spool integrity check failed")

    def _preflight_disk(self, event_bytes: int) -> None:
        free = shutil.disk_usage(self.config.path.parent).free
        if free - event_bytes < self.config.min_free_disk_bytes:
            raise BackpressureRequired("bridge minimum free-disk reserve would be violated")
        conservative_growth = max(1 * 1024 * 1024, event_bytes * 4)
        if self.storage_bytes() + conservative_growth > self.config.max_storage_bytes:
            raise BackpressureRequired("bridge physical storage bound would be violated")

    def _logical_usage_locked(self) -> tuple[int, int]:
        row = self._connection.execute(
            "SELECT event_records, payload_bytes FROM spool_state WHERE singleton = 1"
        ).fetchone()
        return int(row[0]), int(row[1])

    def _maybe_maintain_locked(self, now_ns: int) -> None:
        last = self._connection.execute(
            "SELECT last_maintenance_ns FROM spool_state WHERE singleton = 1"
        ).fetchone()[0]
        interval_ns = self.config.maintenance_interval_seconds * 1_000_000_000
        if now_ns - int(last) < interval_ns:
            return
        self._expire_consumers_locked(now_ns)
        self._trim_aged_unowned_locked(now_ns)
        self._connection.execute(
            "UPDATE spool_state SET last_maintenance_ns = ? WHERE singleton = 1",
            (now_ns,),
        )

    def _decrement_usage_locked(self, records: int, payload_bytes: int) -> None:
        if records <= 0:
            return
        self._connection.execute(
            """
            UPDATE spool_state
            SET event_records = MAX(0, event_records - ?),
                payload_bytes = MAX(0, payload_bytes - ?)
            WHERE singleton = 1
            """,
            (records, payload_bytes),
        )

    def _expire_consumers_locked(self, now_ns: int) -> None:
        self._connection.execute(
            "DELETE FROM consumer_checkpoints WHERE expires_at_ns <= ?", (now_ns,)
        )

    def _trim_aged_unowned_locked(self, now_ns: int) -> None:
        cutoff = now_ns - self.config.replay_retention_seconds * 1_000_000_000
        removed = self._connection.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0)
            FROM events
            WHERE committed_at_ns < ?
              AND NOT EXISTS (
                  SELECT 1 FROM consumer_checkpoints c
                  WHERE c.stream = events.stream
                    AND c.partition_key = events.partition_key
                    AND c.expires_at_ns > ?
              )
            """,
            (cutoff, now_ns),
        ).fetchone()
        self._connection.execute(
            """
            DELETE FROM events
            WHERE committed_at_ns < ?
              AND NOT EXISTS (
                  SELECT 1 FROM consumer_checkpoints c
                  WHERE c.stream = events.stream
                    AND c.partition_key = events.partition_key
                    AND c.expires_at_ns > ?
              )
            """,
            (cutoff, now_ns),
        )
        self._decrement_usage_locked(int(removed[0]), int(removed[1]))

    @staticmethod
    def _stored_event(row: sqlite3.Row) -> StoredEvent:
        payload = bytes(row["payload"])
        actual_digest = hashlib.sha256(payload).hexdigest()
        if actual_digest != row["payload_sha256"]:
            raise PayloadCorruption("committed payload checksum mismatch")
        event = DurableEvent(
            stream=row["stream"],
            partition_key=row["partition_key"],
            event_id=bytes(row["event_id"]),
            payload=payload,
            accepted_at_ns=int(row["accepted_at_ns"]),
            content_type=row["content_type"],
            headers=json.loads(row["headers_json"]),
        )
        return StoredEvent(
            event=event,
            cursor=Cursor(row["stream"], row["partition_key"], int(row["logical_offset"])),
            committed_at_ns=int(row["committed_at_ns"]),
            payload_sha256=row["payload_sha256"],
        )

    def __enter__(self) -> "SQLiteDurableSpool":
        return self

    def __exit__(self, *_args) -> None:
        self.close()
