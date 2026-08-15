from __future__ import annotations

from collections.abc import Callable

from qdl.transport.contracts import AppendResult, Cursor, DurableEvent
from qdl.transport.sqlite_spool import SQLiteDurableSpool


class ShadowCanonicalPipeline:
    """Raw durability followed by restartable canonicalization and checkpointing."""

    def __init__(
        self,
        spool: SQLiteDurableSpool,
        *,
        consumer_id: str,
        canonicalizer: Callable[[DurableEvent], DurableEvent],
        checkpoint_ttl_seconds: int = 3600,
    ):
        if not consumer_id.strip():
            raise ValueError("consumer_id is required")
        self._spool = spool
        self._consumer_id = consumer_id
        self._canonicalizer = canonicalizer
        self._checkpoint_ttl_seconds = checkpoint_ttl_seconds

    def accept(self, raw_event: DurableEvent) -> tuple[AppendResult, AppendResult]:
        raw_result = self._spool.append(raw_event)
        canonical_result = self._canonicalize_and_checkpoint(raw_event, raw_result.cursor)
        return raw_result, canonical_result

    def drain(self, *, stream: str, partition_key: str, limit: int = 100) -> int:
        cursor = self._spool.get_checkpoint(
            consumer_id=self._consumer_id,
            stream=stream,
            partition_key=partition_key,
        )
        rows = self._spool.read(
            stream=stream,
            partition_key=partition_key,
            after=cursor,
            limit=limit,
        )
        for row in rows:
            self._canonicalize_and_checkpoint(row.event, row.cursor)
        return len(rows)

    def _canonicalize_and_checkpoint(
        self, raw_event: DurableEvent, raw_cursor: Cursor
    ) -> AppendResult:
        canonical_event = self._canonicalizer(raw_event)
        result = self._spool.append(canonical_event)
        self._spool.checkpoint(
            consumer_id=self._consumer_id,
            cursor=raw_cursor,
            ttl_seconds=self._checkpoint_ttl_seconds,
        )
        return result
