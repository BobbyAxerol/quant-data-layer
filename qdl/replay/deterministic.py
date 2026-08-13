from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Callable, Iterable

from qdl.transport.contracts import DurableEvent, StoredEvent


@dataclass(frozen=True)
class ReplayReport:
    stream: str
    partition_key: str
    event_count: int
    first_offset: int | None
    last_offset: int | None
    raw_checksum: str
    canonical_checksum: str
    lineage_checksum: str
    normalizer_version: str
    config_revision: int
    source_revision: str
    run_checksum: str


class DeterministicReplayEngine:
    """Pure raw-to-canonical replay with versioned, reproducible checksums."""

    def replay(
        self,
        records: Iterable[StoredEvent],
        *,
        canonicalizer: Callable[[DurableEvent], DurableEvent],
        normalizer_version: str,
        config_revision: int,
        source_revision: str,
    ) -> ReplayReport:
        if not normalizer_version.strip() or not source_revision.strip():
            raise ValueError("normalizer_version and source_revision are required")
        if config_revision < 1:
            raise ValueError("config_revision must be positive")

        raw_digest = hashlib.sha256()
        canonical_digest = hashlib.sha256()
        lineage_digest = hashlib.sha256()
        stream = ""
        partition_key = ""
        first_offset = None
        previous_offset = None
        count = 0
        for stored in records:
            if not stream:
                stream = stored.cursor.stream
                partition_key = stored.cursor.partition_key
                first_offset = stored.cursor.offset
            if (stored.cursor.stream, stored.cursor.partition_key) != (stream, partition_key):
                raise ValueError("one replay report may cover only one ordered partition")
            if previous_offset is not None and stored.cursor.offset != previous_offset + 1:
                raise ValueError("raw replay contains a logical offset gap")
            canonical = canonicalizer(stored.event)
            if canonical.headers.get("raw_event_id") != stored.event.event_id.hex():
                raise ValueError("canonical replay output lost raw-event lineage")
            raw_digest.update(stored.event.event_id)
            raw_digest.update(stored.event.payload)
            canonical_digest.update(canonical.event_id)
            canonical_digest.update(canonical.payload)
            lineage_digest.update(stored.event.event_id)
            lineage_digest.update(canonical.event_id)
            previous_offset = stored.cursor.offset
            count += 1

        report_identity = {
            "canonical_checksum": canonical_digest.hexdigest(),
            "config_revision": config_revision,
            "event_count": count,
            "first_offset": first_offset,
            "last_offset": previous_offset,
            "lineage_checksum": lineage_digest.hexdigest(),
            "normalizer_version": normalizer_version,
            "partition_key": partition_key,
            "raw_checksum": raw_digest.hexdigest(),
            "source_revision": source_revision,
            "stream": stream,
        }
        run_checksum = hashlib.sha256(
            json.dumps(report_identity, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return ReplayReport(**report_identity, run_checksum=run_checksum)
