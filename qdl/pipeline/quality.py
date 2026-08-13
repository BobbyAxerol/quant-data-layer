from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from qdl.transport.contracts import AppendResult, DurableEvent
from qdl.transport.sqlite_spool import SQLiteDurableSpool


@dataclass(frozen=True)
class QualityPipelineResult:
    raw_result: AppendResult
    canonical_result: AppendResult | None
    quarantine_id: int | None
    reason_code: str | None


class ValidatedCanonicalPipeline:
    """Raw-first canonicalization; invalid events retain bytes and enter quarantine."""

    def __init__(
        self,
        spool: SQLiteDurableSpool,
        *,
        canonicalizer: Callable[[DurableEvent], DurableEvent],
    ) -> None:
        self._spool = spool
        self._canonicalizer = canonicalizer

    def accept(self, raw_event: DurableEvent) -> QualityPipelineResult:
        raw_result = self._spool.append(raw_event)
        try:
            canonical = self._canonicalizer(raw_event)
            raw_reference = canonical.headers.get("raw_event_id")
            if raw_reference != raw_event.event_id.hex():
                raise ValueError("canonical event does not retain its durable raw-event reference")
            canonical_result = self._spool.append(canonical)
            return QualityPipelineResult(raw_result, canonical_result, None, None)
        except (KeyError, TypeError, ValueError) as error:
            reason_code = (
                "UNKNOWN_INSTRUMENT"
                if "instrument" in str(error).lower()
                else "CANONICAL_VALIDATION_FAILED"
            )
            quarantine_id = self._spool.quarantine(
                event=raw_event,
                reason_code=reason_code,
                reason_message=str(error)[:500],
                retry_count=0,
            )
            return QualityPipelineResult(
                raw_result, None, quarantine_id, reason_code
            )
