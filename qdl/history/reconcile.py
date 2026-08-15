from __future__ import annotations

from dataclasses import dataclass

from qdl.history.bars import BarRecord, select_revisions


@dataclass(frozen=True)
class BarReconciliationReport:
    row_count: int
    overlap_count: int
    revised_count: int
    expected_gap_open_times_ns: tuple[int, ...]


def reconcile_history_live(
    historical: list[BarRecord],
    live: list[BarRecord],
    *,
    expected_open_times_ns: tuple[int, ...],
) -> BarReconciliationReport:
    """Reconcile only calendar-provided expected times; sparse feeds stay sparse."""

    def key(record: BarRecord) -> tuple[str, str, int]:
        return record.instrument_uid, record.interval, record.open_time_ns

    historical_selected = {key(row): row for row in select_revisions(historical)}
    live_selected = {key(row): row for row in select_revisions(live)}
    overlap_count = 0
    revised_count = 0
    merged = dict(historical_selected)
    for identity, live_row in live_selected.items():
        previous = historical_selected.get(identity)
        if previous is not None:
            overlap_count += 1
            if live_row.revision < previous.revision:
                continue
            if live_row.revision == previous.revision and live_row != previous:
                raise ValueError("history/live overlap conflicts at the same revision")
            if live_row.revision > previous.revision:
                revised_count += 1
        merged[identity] = live_row
    available = {identity[2] for identity in merged}
    gaps = tuple(value for value in expected_open_times_ns if value not in available)
    return BarReconciliationReport(len(merged), overlap_count, revised_count, gaps)
