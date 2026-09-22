from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from qdl.transport.contracts import (
    AppendResult,
    BackpressureRequired,
    CheckpointRegression,
    Cursor,
    CursorExpired,
    DurableEvent,
    EventIdCollision,
    FINAL_BAR_CLOSE_TIME_NS_HEADER,
    PayloadCorruption,
    StoredEvent,
)


JOURNAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024
FINAL_BAR_LOOKUP_INDEX_NAME = "idx_qdl_spool_events_final_bar_close"
# Keep this expression byte-for-byte aligned with the exact final-BAR lookup
# below. SQLite only uses an expression index when the query expression has
# the same shape; a semantically similar JSON predicate can silently regress
# to a partition scan.
FINAL_BAR_CLOSE_TIME_EXPRESSION = (
    "CAST(json_extract(headers_json, "
    "'$.\"qdl.final_bar_close_time_ns\"') AS INTEGER)"
)
FINAL_BAR_LOOKUP_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS {FINAL_BAR_LOOKUP_INDEX_NAME}
ON events (stream, partition_key, {FINAL_BAR_CLOSE_TIME_EXPRESSION}, logical_offset)
"""


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
    max_partition_records: int = 0
    retain_partition_windows: bool = False
    verify_integrity_on_open: bool = True

    def __post_init__(self) -> None:
        if self.max_records <= 0 or self.max_payload_bytes <= 0:
            raise ValueError("spool bounds must be positive")
        if self.max_event_bytes <= 0 or self.max_event_bytes > self.max_payload_bytes:
            raise ValueError("max_event_bytes must fit inside max_payload_bytes")
        if self.max_batch_events <= 0:
            raise ValueError("max_batch_events must be positive")
        if self.max_partition_records < 0:
            raise ValueError("max_partition_records cannot be negative")
        if not isinstance(self.retain_partition_windows, bool) or (
            self.retain_partition_windows and self.max_partition_records <= 0
        ):
            raise ValueError("retained partition windows require a positive record bound")
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


@dataclass(frozen=True)
class SpoolReadiness:
    """Bounded, read-only cache health summary.

    `spool_state` is updated atomically alongside append and retention work.
    Health checks need that durable state, not a full aggregate over `events`.
    """

    records: int
    payload_bytes: int


@dataclass(frozen=True)
class FinalBarTailWindow:
    """One exact, bounded final-BAR lookup inside a spool read snapshot.

    This is deliberately transport-private.  Callers must validate the
    returned canonical rows and request the normal retained tail when the
    exact window is incomplete or ambiguous.
    """

    interval_ns: int
    rows: int

    def __post_init__(self) -> None:
        if self.interval_ns <= 0:
            raise ValueError("final BAR interval must be positive")
        if self.rows not in {1, 2}:
            raise ValueError("final BAR exact lookup supports one or two rows")


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
            str(config.path), timeout=30.0, isolation_level=None, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        self._initialize_schema()
        if config.verify_integrity_on_open:
            self._validate_integrity()

    def _initialize_schema(self) -> None:
        for attempt in range(4):
            try:
                self._configure()
                self._migrate()
                return
            except sqlite3.OperationalError as error:
                if "locked" not in str(error).lower() or attempt == 3:
                    raise
                if self._connection.in_transaction:
                    self._connection.rollback()
                time.sleep(0.25 * (attempt + 1))

    def _configure(self) -> None:
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA busy_timeout=30000")
        self._connection.execute("PRAGMA wal_autocheckpoint=1000")
        self._connection.execute(f"PRAGMA journal_size_limit={JOURNAL_SIZE_LIMIT_BYTES}")

    def _migrate(self) -> None:
        # A live cache can hold millions of canonical events. Creating an
        # expression index over it is an intentional operational migration,
        # never an incidental consequence of upgrading a reader. New spools
        # get the index before any rows exist; old spools use the explicit
        # migration method below.
        events_preexisted = self._table_exists_locked("events")
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

            CREATE TABLE IF NOT EXISTS cache_identity (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                cache_id TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS final_bar_watermarks (
                stream TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                close_time_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL,
                PRIMARY KEY (stream, partition_key)
            );
            """
        )
        if not events_preexisted:
            self._create_final_bar_lookup_index_locked()
        self._ensure_usage_state()
        self._connection.execute(
            """
            INSERT OR IGNORE INTO cache_identity(singleton, cache_id, created_at_ns)
            VALUES (1, ?, ?)
            """,
            (uuid.uuid4().hex, self._clock_ns()),
        )

    def _table_exists_locked(self, table_name: str) -> bool:
        row = self._connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """,
            (table_name,),
        ).fetchone()
        return row is not None

    def _final_bar_lookup_index_present_locked(self) -> bool:
        row = self._connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'index' AND name = ?
            """,
            (FINAL_BAR_LOOKUP_INDEX_NAME,),
        ).fetchone()
        return row is not None

    def final_bar_lookup_index_present(self) -> bool:
        """Return whether this spool has the optional exact-BAR lookup index."""
        with self._lock:
            return self._final_bar_lookup_index_present_locked()

    def _create_final_bar_lookup_index_locked(self) -> bool:
        try:
            self._connection.execute(FINAL_BAR_LOOKUP_INDEX_SQL)
        except sqlite3.OperationalError as error:
            # Exact-window materialization is already explicitly optional on
            # SQLite builds without JSON support. Keep that portable fallback
            # rather than making an otherwise valid spool fail on open.
            if "json" in str(error).lower():
                return False
            raise
        return self._final_bar_lookup_index_present_locked()

    def _final_bar_lookup_events_source_locked(self) -> str:
        # SQLite may prefer the primary-key tail scan until a full ANALYZE has
        # run, even when the exact expression index exists. The latter would
        # scan a live multi-thousand-row partition for each requested close.
        # Force the known-compatible index only when it is actually present;
        # legacy caches retain the existing unhinted query and tail fallback.
        if self._final_bar_lookup_index_present_locked():
            return f"events INDEXED BY {FINAL_BAR_LOOKUP_INDEX_NAME}"
        return "events"

    def create_final_bar_lookup_index(self) -> bool:
        """Run the explicit, idempotent final-BAR lookup-index migration.

        Callers must invoke this only from an approved operational packet for
        an existing cache: SQLite builds the index by scanning current rows.
        It is intentionally not called during normal reopen of a live spool.
        ``False`` means the SQLite build lacks JSON-expression support and the
        retained-tail implementation remains authoritative.
        """
        with self._lock:
            return self._create_final_bar_lookup_index_locked()

    def _ensure_usage_state(self) -> None:
        """Initialize legacy spool usage once without rescanning live caches."""
        with self._lock:
            existing = self._connection.execute(
                "SELECT 1 FROM spool_state WHERE singleton = 1"
            ).fetchone()
            if existing is not None:
                return
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                existing = self._connection.execute(
                    "SELECT 1 FROM spool_state WHERE singleton = 1"
                ).fetchone()
                if existing is None:
                    records, payload_bytes = self._aggregate_event_usage_locked()
                    self._connection.execute(
                        """
                        INSERT INTO spool_state(
                            singleton, event_records, payload_bytes, last_maintenance_ns
                        ) VALUES (1, ?, ?, 0)
                        """,
                        (records, payload_bytes),
                    )
                self._connection.execute("COMMIT")
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def _aggregate_event_usage_locked(self) -> tuple[int, int]:
        row = self._connection.execute(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0) FROM events"
        ).fetchone()
        return int(row[0]), int(row[1])

    @property
    def cache_id(self) -> str:
        with self._lock:
            row = self._connection.execute(
                "SELECT cache_id FROM cache_identity WHERE singleton = 1"
            ).fetchone()
        value = str(row["cache_id"]) if row is not None else ""
        if len(value) != 32 or any(char not in "0123456789abcdef" for char in value):
            raise PayloadCorruption("spool cache identity is invalid")
        return value

    def append(self, event: DurableEvent) -> AppendResult:
        return self.append_many([event])[0]

    @staticmethod
    def _final_bar_close_time_ns(event: DurableEvent) -> int | None:
        value = event.headers.get(FINAL_BAR_CLOSE_TIME_NS_HEADER)
        if value is None:
            return None
        if (
            not isinstance(value, str)
            or not value.isascii()
            or not value.isdecimal()
        ):
            raise ValueError("final BAR watermark header is invalid")
        close_time_ns = int(value)
        if not 0 < close_time_ns < 2**63:
            raise ValueError("final BAR watermark header is invalid")
        return close_time_ns

    def _upsert_final_bar_watermark_locked(
        self,
        *,
        stream: str,
        partition_key: str,
        close_time_ns: int,
        updated_at_ns: int,
    ) -> int:
        self._connection.execute(
            """
            INSERT INTO final_bar_watermarks(
                stream, partition_key, close_time_ns, updated_at_ns
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(stream, partition_key) DO UPDATE SET
                close_time_ns = MAX(
                    final_bar_watermarks.close_time_ns,
                    excluded.close_time_ns
                ),
                updated_at_ns = CASE
                    WHEN excluded.close_time_ns >= final_bar_watermarks.close_time_ns
                    THEN excluded.updated_at_ns
                    ELSE final_bar_watermarks.updated_at_ns
                END
            """,
            (stream, partition_key, close_time_ns, updated_at_ns),
        )
        row = self._connection.execute(
            """
            SELECT close_time_ns FROM final_bar_watermarks
            WHERE stream = ? AND partition_key = ?
            """,
            (stream, partition_key),
        ).fetchone()
        if row is None or int(row["close_time_ns"]) <= 0:
            raise PayloadCorruption("final BAR watermark is unavailable")
        return int(row["close_time_ns"])

    def final_bar_watermark(
        self, *, stream: str, partition_key: str
    ) -> int | None:
        if not stream.strip() or not partition_key.strip():
            raise ValueError("final BAR watermark identity is incomplete")
        with self._lock:
            row = self._connection.execute(
                """
                SELECT close_time_ns FROM final_bar_watermarks
                WHERE stream = ? AND partition_key = ?
                """,
                (stream, partition_key),
            ).fetchone()
        if row is None:
            return None
        value = int(row["close_time_ns"])
        if value <= 0:
            raise PayloadCorruption("final BAR watermark is invalid")
        return value

    def seed_final_bar_watermark(
        self, *, stream: str, partition_key: str, close_time_ns: int
    ) -> int:
        if not stream.strip() or not partition_key.strip() or not 0 < close_time_ns < 2**63:
            raise ValueError("final BAR watermark is invalid")
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                value = self._upsert_final_bar_watermark_locked(
                    stream=stream,
                    partition_key=partition_key,
                    close_time_ns=close_time_ns,
                    updated_at_ns=self._clock_ns(),
                )
                self._connection.execute("COMMIT")
                return value
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def hydrate_final_bar_watermark(
        self,
        *,
        stream: str,
        partition_key: str,
        legacy_lookup: Callable[[], int | None],
    ) -> int | None:
        """Seed one legacy BAR partition exactly once across spool processes.

        Read legacy history without reserving the shared writer. A concurrent
        append may publish a newer close during that read; the short write
        transaction rechecks and keeps the monotonic maximum.
        """

        if not stream.strip() or not partition_key.strip():
            raise ValueError("final BAR watermark identity is incomplete")
        with self._lock:
            current = self.final_bar_watermark(stream=stream, partition_key=partition_key)
            if current is not None:
                return current
            close_time_ns = legacy_lookup()
            if close_time_ns is None:
                return self.final_bar_watermark(stream=stream, partition_key=partition_key)
            if not 0 < close_time_ns < 2**63:
                raise PayloadCorruption("legacy final BAR watermark is invalid")
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                value = self._upsert_final_bar_watermark_locked(
                    stream=stream,
                    partition_key=partition_key,
                    close_time_ns=close_time_ns,
                    updated_at_ns=self._clock_ns(),
                )
                self._connection.execute("COMMIT")
                return value
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

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
                maintenance_ran = self._maybe_maintain_locked(self._clock_ns())
                records, payload_bytes = self._logical_usage_locked()
                partition_count = int(
                    self._connection.execute("SELECT COUNT(*) FROM partitions").fetchone()[0]
                )
                added_records = 0
                added_payload_bytes = 0
                results = []
                for event in events:
                    final_bar_close_time_ns = self._final_bar_close_time_ns(event)
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
                        if final_bar_close_time_ns is not None:
                            self._upsert_final_bar_watermark_locked(
                                stream=event.stream,
                                partition_key=event.partition_key,
                                close_time_ns=final_bar_close_time_ns,
                                updated_at_ns=self._clock_ns(),
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
                    if final_bar_close_time_ns is not None:
                        self._upsert_final_bar_watermark_locked(
                            stream=event.stream,
                            partition_key=event.partition_key,
                            close_time_ns=final_bar_close_time_ns,
                            updated_at_ns=committed_at_ns,
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
                if self.config.max_partition_records:
                    self._trim_partition_windows_locked({
                        (event.stream, event.partition_key) for event in events
                    })
                self._connection.execute("COMMIT")
                if maintenance_ran:
                    # Routine retention must never turn a hot append into a
                    # blocking TRUNCATE checkpoint. Under a shared read-heavy
                    # cache, TRUNCATE can wait for another reader's snapshot
                    # until SQLite's busy timeout and make otherwise fresh
                    # market events stale. PASSIVE is nonblocking; the
                    # physical-capacity path below retains the bounded
                    # TRUNCATE reclaim/fail-closed policy when disk headroom
                    # actually requires it.
                    self._checkpoint_wal_passive_locked()
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

    def read_tail(
        self,
        *,
        stream: str,
        partition_key: str,
        limit: int = 100,
    ) -> list[StoredEvent]:
        """Return a bounded retained partition window in logical order.

        The public replay/query contract remains capped independently at
        10,000 rows.  A stable runtime may retain a small, explicitly bounded
        per-partition headroom for authentic late backfills; its internal
        reader must be able to inspect that configured physical window before
        selecting the public market-time tail.
        """

        max_tail_rows = max(10_000, self.config.max_partition_records)
        if limit <= 0 or limit > max_tail_rows:
            raise ValueError(f"limit must be between 1 and {max_tail_rows}")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM events
                WHERE stream = ? AND partition_key = ?
                ORDER BY logical_offset DESC LIMIT ?
                """,
                (stream, partition_key, limit),
            ).fetchall()
        return [self._stored_event(row) for row in reversed(rows)]

    def read_tails(
        self,
        *,
        requests: tuple[tuple[str, str, int], ...] | list[tuple[str, str, int]],
    ) -> dict[tuple[str, str], tuple[StoredEvent, ...]]:
        """Read bounded tails for up to one public query batch in one snapshot.

        This is an internal local-cache primitive, not a replay API.  A single
        SQL statement gives every selected partition one SQLite-consistent
        view, avoids one lock acquisition per item, and preserves the logical
        ordering returned by :meth:`read_tail`.  Multiple callers may request
        the same physical partition with different limits; the largest bounded
        tail is read once and callers select their own logical windows above
        this transport boundary.
        """

        grouped: dict[tuple[str, str], list[StoredEvent]] = {}

        def collect(key: tuple[str, str], rows: tuple[StoredEvent, ...]) -> None:
            grouped[key] = list(rows)

        self.visit_tails(requests=requests, visit=collect)
        return {key: tuple(value) for key, value in grouped.items()}

    def visit_tails(
        self,
        *,
        requests: tuple[tuple[str, str, int], ...] | list[tuple[str, str, int]],
        visit: Callable[[tuple[str, str], tuple[StoredEvent, ...]], None],
    ) -> None:
        """Visit each deduplicated indexed tail under one read snapshot.

        The callback runs before the next physical partition is read.  This is
        intentionally separate from :meth:`read_tails`: large callers can
        materialize a result and release a physical tail before reading the
        next one, while preserving the same SQLite snapshot for every route in
        the public batch.
        """

        normalized = self._normalized_tail_requests(requests)
        if not normalized:
            return
        with self._lock:
            # A deferred read transaction keeps one consistent generation
            # without blocking WAL writers.  Do not use a window-function CTE
            # here: on a large spool SQLite may sort the whole events table
            # into temp storage before applying each tail cap.
            self._connection.execute("BEGIN")
            try:
                for (stream, partition_key), limit in normalized.items():
                    rows = self._connection.execute(
                        """
                        SELECT * FROM events
                        WHERE stream = ? AND partition_key = ?
                        ORDER BY logical_offset DESC LIMIT ?
                        """,
                        (stream, partition_key, limit),
                    ).fetchall()
                    visit(
                        (stream, partition_key),
                        tuple(self._stored_event(row) for row in reversed(rows)),
                    )
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

    def visit_final_bar_windows(
        self,
        *,
        requests: tuple[tuple[str, str, int], ...] | list[tuple[str, str, int]],
        windows: dict[tuple[str, str], FinalBarTailWindow],
        visit: Callable[[tuple[str, str], tuple[StoredEvent, ...], tuple[int, ...]], bool],
    ) -> None:
        """Visit exact final-BAR windows with retained-tail fallback in one snapshot.

        A final watermark identifies the current one/two closed BARs without
        decoding the full retained partition.  The visitor is the semantic
        authority: it returns ``True`` only after it accepts the exact rows.
        Missing, duplicate, revised or otherwise ambiguous rows therefore use
        the existing physical-tail path under the same SQLite read transaction.
        """

        normalized = self._normalized_tail_requests(requests)
        if not normalized:
            return
        unknown = set(windows) - set(normalized)
        if unknown:
            raise ValueError("final BAR window is not part of the tail request")
        with self._lock:
            self._connection.execute("BEGIN")
            try:
                for key, limit in normalized.items():
                    window = windows.get(key)
                    if window is not None:
                        exact = self._final_bar_window_rows_locked(
                            stream=key[0],
                            partition_key=key[1],
                            window=window,
                        )
                        if exact is not None:
                            rows, expected_closes = exact
                            if visit(key, rows, expected_closes):
                                continue
                    rows = self._read_tail_rows_locked(
                        stream=key[0], partition_key=key[1], limit=limit
                    )
                    visit(key, rows, ())
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

    def _final_bar_window_rows_locked(
        self,
        *,
        stream: str,
        partition_key: str,
        window: FinalBarTailWindow,
    ) -> tuple[tuple[StoredEvent, ...], tuple[int, ...]] | None:
        watermark = self._connection.execute(
            """
            SELECT close_time_ns FROM final_bar_watermarks
            WHERE stream = ? AND partition_key = ?
            """,
            (stream, partition_key),
        ).fetchone()
        if watermark is None:
            return None
        latest_close_ns = int(watermark["close_time_ns"])
        if latest_close_ns <= 0:
            raise PayloadCorruption("final BAR watermark is invalid")
        expected_closes = tuple(
            latest_close_ns - window.interval_ns * offset
            for offset in range(window.rows - 1, -1, -1)
        )
        if any(value <= 0 for value in expected_closes):
            raise PayloadCorruption("final BAR watermark window is invalid")
        placeholders = ",".join("?" for _ in expected_closes)
        try:
            events_source = self._final_bar_lookup_events_source_locked()
            rows = self._connection.execute(
                f"""
                SELECT * FROM {events_source}
                WHERE stream = ? AND partition_key = ?
                  AND {FINAL_BAR_CLOSE_TIME_EXPRESSION} IN ({placeholders})
                ORDER BY logical_offset ASC
                """,
                (stream, partition_key, *expected_closes),
            ).fetchall()
        except sqlite3.OperationalError as error:
            # JSON extraction is an optimization only.  Older SQLite builds or
            # malformed legacy metadata must retain the authoritative tail path.
            if "json" in str(error).lower():
                return None
            raise
        return (
            tuple(self._stored_event(row) for row in rows),
            expected_closes,
        )

    def _read_tail_rows_locked(
        self,
        *,
        stream: str,
        partition_key: str,
        limit: int,
    ) -> tuple[StoredEvent, ...]:
        rows = self._connection.execute(
            """
            SELECT * FROM events
            WHERE stream = ? AND partition_key = ?
            ORDER BY logical_offset DESC LIMIT ?
            """,
            (stream, partition_key, limit),
        ).fetchall()
        return tuple(self._stored_event(row) for row in reversed(rows))

    def _normalized_tail_requests(
        self,
        requests: tuple[tuple[str, str, int], ...] | list[tuple[str, str, int]],
    ) -> dict[tuple[str, str], int]:
        max_tail_rows = max(10_000, self.config.max_partition_records)
        normalized: dict[tuple[str, str], int] = {}
        for stream, partition_key, limit in requests:
            if not stream.strip() or not partition_key.strip():
                raise ValueError("tail stream and partition_key are required")
            if limit <= 0 or limit > max_tail_rows:
                raise ValueError(f"limit must be between 1 and {max_tail_rows}")
            key = (stream, partition_key)
            normalized[key] = max(normalized.get(key, 0), int(limit))
        if len(normalized) > 100:
            raise ValueError("batch tail read exceeds the public request bound")
        return normalized

    def find_event(self, *, stream: str, event_id: bytes) -> StoredEvent | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM events WHERE stream = ? AND event_id = ?",
                (stream, event_id),
            ).fetchone()
        return self._stored_event(row) if row is not None else None

    def find_events(
        self, *, stream: str, event_ids: list[bytes] | tuple[bytes, ...]
    ) -> dict[bytes, StoredEvent]:
        """Resolve one bounded immutable-event batch without per-event queries."""

        values = tuple(dict.fromkeys(event_ids))
        if not values:
            return {}
        if len(values) > 10_000:
            raise ValueError("event lookup batch exceeds the bounded query window")
        rows = []
        with self._lock:
            for start in range(0, len(values), 500):
                chunk = values[start:start + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows.extend(self._connection.execute(
                    f"SELECT * FROM events WHERE stream = ? AND event_id IN ({placeholders})",
                    (stream, *chunk),
                ).fetchall())
        return {
            bytes(row["event_id"]): self._stored_event(row)
            for row in rows
        }

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

        if self.config.retain_partition_windows:
            return 0
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

    def quarantine_once(
        self,
        *,
        event: DurableEvent,
        reason_code: str,
        reason_message: str,
        retry_count: int,
    ) -> int:
        """Persist one bounded forensic record for one immutable poison event.

        This differs from :meth:`quarantine`: callers use it when replaying the
        exact same durable record is expected and must not consume a new
        quarantine slot.  The immediate transaction also makes the decision
        stable across projector replicas sharing one SQLite cache.
        """

        if not reason_code.strip() or retry_count < 0:
            raise ValueError("valid quarantine reason and retry_count are required")
        digest = hashlib.sha256(event.payload).hexdigest()
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                existing = self._connection.execute(
                    """
                    SELECT quarantine_id FROM quarantine
                    WHERE stream = ? AND partition_key = ? AND event_id = ?
                      AND payload_sha256 = ? AND reason_code = ?
                    """,
                    (
                        event.stream,
                        event.partition_key,
                        event.event_id,
                        digest,
                        reason_code,
                    ),
                ).fetchone()
                if existing is not None:
                    self._connection.execute("COMMIT")
                    return int(existing["quarantine_id"])
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
                        digest,
                        reason_code,
                        reason_message,
                        retry_count,
                        self._clock_ns(),
                    ),
                )
                self._connection.execute("COMMIT")
                return int(result.lastrowid)
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

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

    def readiness_summary(self) -> SpoolReadiness:
        """Read the bounded, transaction-maintained usage state for health checks.

        A missing or invalid singleton is a durable-state invariant violation,
        so callers deliberately fail closed rather than approximating it from
        the much larger event table.
        """
        with self._lock:
            row = self._connection.execute(
                "SELECT event_records, payload_bytes FROM spool_state WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise PayloadCorruption("spool usage state is missing")
        records = int(row["event_records"])
        payload_bytes = int(row["payload_bytes"])
        if records < 0 or payload_bytes < 0:
            raise PayloadCorruption("spool usage state is invalid")
        return SpoolReadiness(records=records, payload_bytes=payload_bytes)

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
            # Other roles may still own read snapshots. Closing this handle is
            # not a storage-reclamation operation and must not block their work.
            self._checkpoint_wal_passive_locked()
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

        def over_bound() -> bool:
            return (
                self.storage_bytes() + conservative_growth
                > self.config.max_storage_bytes
            )

        if not over_bound():
            return
        # A complete checkpoint is safe to attempt outside a transaction and can
        # reclaim a stale WAL without changing logical retention. If a reader
        # pins the WAL, PASSIVE returns promptly.
        self._checkpoint_wal_passive_locked()
        if not over_bound():
            return
        # PASSIVE recycles a WAL but never shrinks the file, and on 2026-09-17
        # that file was the whole overrun: 922 MB of already-checkpointed frames
        # against 7,720 bytes of headroom. TRUNCATE reclaims it, and a bounded
        # pause while readers drain is strictly better than failing every writer
        # closed until an operator intervenes.
        self._checkpoint_wal_truncate_locked()
        if over_bound():
            raise BackpressureRequired("bridge physical storage bound would be violated")

    def _checkpoint_wal_passive_locked(self) -> bool:
        """Best-effort nonblocking WAL checkpoint for a shared durable spool.

        The caller holds this process' spool lock and must be outside a write
        transaction. SQLite's PASSIVE mode does not wait for or invalidate
        readers, so a pinned reader leaves the normal physical bound fail-closed.
        """

        try:
            row = self._connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        except sqlite3.DatabaseError:
            return False
        if row is None or len(row) != 3:
            return False
        busy, log_frames, checkpointed_frames = (int(value) for value in row)
        return busy == 0 and log_frames == checkpointed_frames

    def _checkpoint_wal_truncate_locked(self) -> bool:
        """Reclaim the WAL file itself, bounded by this connection's busy timeout.

        TRUNCATE waits for readers, so it can return busy under load; the caller
        must treat failure as "no space reclaimed" and keep its own bound
        fail-closed. It never discards a committed event.
        """

        try:
            row = self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        except sqlite3.DatabaseError:
            return False
        if row is None or len(row) != 3:
            return False
        busy, log_frames, checkpointed_frames = (int(value) for value in row)
        return busy == 0 and log_frames == checkpointed_frames

    def _wal_bytes(self) -> int:
        path = Path(f"{self.config.path}-wal")
        return path.stat().st_size if path.exists() else 0

    def _logical_usage_locked(self) -> tuple[int, int]:
        row = self._connection.execute(
            "SELECT event_records, payload_bytes FROM spool_state WHERE singleton = 1"
        ).fetchone()
        return int(row[0]), int(row[1])

    def _maybe_maintain_locked(self, now_ns: int) -> bool:
        last = self._connection.execute(
            "SELECT last_maintenance_ns FROM spool_state WHERE singleton = 1"
        ).fetchone()[0]
        interval_ns = self.config.maintenance_interval_seconds * 1_000_000_000
        if now_ns - int(last) < interval_ns:
            return False
        self._expire_consumers_locked(now_ns)
        self._trim_aged_unowned_locked(now_ns)
        self._connection.execute(
            "UPDATE spool_state SET last_maintenance_ns = ? WHERE singleton = 1",
            (now_ns,),
        )
        return True

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
        # Canonical warmup windows are count-bounded: age eviction would punch
        # holes into sparse bars even while their advertised window still fits.
        if self.config.retain_partition_windows:
            return
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

    def _trim_partition_windows_locked(
        self, partitions: set[tuple[str, str]]
    ) -> None:
        """Keep the newest replay window; older cursors fail with CursorExpired."""

        limit = self.config.max_partition_records
        if limit <= 0:
            return
        for stream, partition_key in partitions:
            threshold = self._connection.execute(
                """
                SELECT logical_offset FROM events
                WHERE stream = ? AND partition_key = ?
                ORDER BY logical_offset DESC LIMIT 1 OFFSET ?
                """,
                (stream, partition_key, limit - 1),
            ).fetchone()
            if threshold is None:
                continue
            removed = self._connection.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0)
                FROM events
                WHERE stream = ? AND partition_key = ? AND logical_offset < ?
                """,
                (stream, partition_key, int(threshold[0])),
            ).fetchone()
            if int(removed[0]) == 0:
                continue
            self._connection.execute(
                """
                DELETE FROM events
                WHERE stream = ? AND partition_key = ? AND logical_offset < ?
                """,
                (stream, partition_key, int(threshold[0])),
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
