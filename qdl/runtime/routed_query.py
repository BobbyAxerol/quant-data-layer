"""Route each requirement to the source that is allowed to answer it.

Two sources can answer a BAR request. The spool holds what the Rust canonical
core produced for a materialised binding, and is the authoritative answer
whenever a binding exists. The provider pass-through re-fetches a window from
the venue and is the only answer for an instrument or interval no binding
covers.

Order matters and is deliberate. A binding always wins, so declaring
`recovery: FRESH_SNAPSHOT` never downgrades a consumer that is already covered
by the authoritative path. The declaration means "replay continuity is not
required", not "give me the pass-through", and the server still picks the best
source available for the request.
"""

from __future__ import annotations

from qdl.query.contracts import DataRequirement
from qdl.query.results import (
    GapRecord,
    HistoryResult,
    MarketDataItem,
    QualityMetadata,
)
from qdl.runtime.provider_history import (
    ProviderBarHistorySource,
    ProviderHistoryUnavailable,
)
from qdl.runtime.stable_source import StableSpoolQueryBackend

_LATEST_WINDOW = 1


class RoutedQueryBackend:
    def __init__(
        self,
        spool: StableSpoolQueryBackend,
        pass_through: ProviderBarHistorySource | None = None,
    ) -> None:
        self.spool = spool
        self.pass_through = pass_through

    def _binding_exists(self, requirement: DataRequirement) -> bool:
        try:
            self.spool.catalog.binding_for(requirement)
        except (KeyError, ValueError):
            return False
        return True

    def routes_to_pass_through(self, requirement: DataRequirement) -> bool:
        """Whether the pass-through is the only source that can answer."""
        if self.pass_through is None:
            return False
        if self._binding_exists(requirement):
            return False
        return self.pass_through.serves(requirement)

    def history(self, requirement: DataRequirement) -> HistoryResult | None:
        if not self.routes_to_pass_through(requirement):
            return self.spool.history(requirement)
        try:
            return self.pass_through.history_result(
                requirement, schema_digest=self.spool.schema_digest
            )
        except ProviderHistoryUnavailable:
            # Refusal is not an empty result: the caller must see "not ready"
            # rather than a silently short window.
            return None

    def latest(self, requirement: DataRequirement) -> MarketDataItem | None:
        if not self.routes_to_pass_through(requirement):
            return self.spool.latest(requirement)
        result = self.history(
            _with_window(requirement, requirement.warmup_limit or _LATEST_WINDOW)
        )
        return result.items[-1] if result and result.items else None

    def feed_status(self, requirement: DataRequirement) -> QualityMetadata | None:
        item = self.latest(requirement)
        return item.quality if item else None

    def open_gaps(self) -> tuple[GapRecord, ...]:
        # Only materialised bindings can hold an open gap. A pass-through window
        # is validated at fetch time and never becomes a tracked gap.
        return self.spool.open_gaps()


def _with_window(requirement: DataRequirement, limit: int) -> DataRequirement:
    from dataclasses import replace

    return replace(requirement, warmup_limit=max(1, int(limit)))
