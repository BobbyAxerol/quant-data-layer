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

from qdl.query.contracts import (
    CanonicalErrorCode,
    DataRequirement,
    RecoveryPolicy,
)
from qdl.query.results import (
    GapRecord,
    HistoryResult,
    MarketDataItem,
    QualityMetadata,
    QueryBackendError,
)
from qdl.runtime.provider_history import (
    ProviderBarHistorySource,
    ProviderHistoryUnavailable,
)
from qdl.runtime.stable_source import StableSpoolQueryBackend

_LATEST_WINDOW = 2


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

    def warmup_is_local(self, requirement: DataRequirement) -> bool:
        # A fresh-snapshot recovery may call a provider after a cache miss.
        # It must keep provider admission even when the current cache is full.
        return self._binding_exists(requirement) and not (
            requirement.recovery is RecoveryPolicy.FRESH_SNAPSHOT
            and self.pass_through is not None
            and self.pass_through.serves(requirement)
        )

    def history(self, requirement: DataRequirement) -> HistoryResult | None:
        if self._binding_exists(requirement):
            result = self.spool.history(requirement)
            specification = requirement.warmup_specification
            explicit_range = bool(
                specification is not None and specification.time_range is not None
            )
            result_ready = result is not None and (
                not isinstance(result, HistoryResult)
                or (
                    result.coverage.value == "FULL"
                    and (
                        explicit_range
                        or result.items[-1].quality.state
                        not in {"STALE", "OFFLINE", "UNAVAILABLE"}
                    )
                )
            )
            if result_ready:
                return result
            if (
                requirement.recovery is not RecoveryPolicy.FRESH_SNAPSHOT
                or self.pass_through is None
                or not self.pass_through.serves(requirement)
            ):
                return result
        elif not self.routes_to_pass_through(requirement):
            return self.spool.history(requirement)
        try:
            return self.pass_through.history_result(
                requirement, schema_digest=self.spool.schema_digest
            )
        except ProviderHistoryUnavailable as error:
            # Preserve the established backend contract for a static refusal:
            # QueryService turns ``None`` into DATA_NOT_READY. Provider/runtime
            # failures keep their typed code and retry metadata instead.
            if (
                error.problem.code is CanonicalErrorCode.DATA_NOT_READY
                and not error.problem.retryable
            ):
                return None
            raise QueryBackendError(error.problem) from error

    def latest(self, requirement: DataRequirement) -> MarketDataItem | None:
        if not self.routes_to_pass_through(requirement):
            item = self.spool.latest(requirement)
            may_recover = (
                isinstance(item, MarketDataItem)
                and item.quality.state in {"STALE", "OFFLINE", "UNAVAILABLE"}
            ) or item is None
            if not (
                may_recover
                and requirement.recovery is RecoveryPolicy.FRESH_SNAPSHOT
                and self.pass_through is not None
                and self.pass_through.serves(requirement)
            ):
                return item
        result = self.history(
            _with_window(requirement, _LATEST_WINDOW)
        )
        return result.items[-1] if result and result.items else None

    def feed_status(self, requirement: DataRequirement) -> QualityMetadata | None:
        item = self.latest(requirement)
        return item.quality if item else None

    def open_gaps(self) -> tuple[GapRecord, ...]:
        # Only materialised bindings can hold an open gap. A pass-through window
        # is validated at fetch time and never becomes a tracked gap.
        return self.spool.open_gaps()

    def warmup_stats(self) -> dict[str, int]:
        if self.pass_through is None:
            return {}
        return self.pass_through.stats()


def _with_window(requirement: DataRequirement, limit: int) -> DataRequirement:
    from dataclasses import replace

    return replace(
        requirement,
        warmup_limit=max(1, int(limit)),
        warmup=None,
    )
