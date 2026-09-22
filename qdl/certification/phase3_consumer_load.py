"""Bounded, manifest-derived logical-consumer workload planning.

This module deliberately owns no socket, provider, cursor, order, or runtime
lifecycle.  It freezes a truthful load shape before the external Phase-3
driver is allowed to contact the V2 Query/Stream plane.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Mapping, Sequence

from qdl.certification.phase103_consumer_acceptance import AcceptanceProduct
from qdl.consumer.manifest import ConsumerManifest


_HOT_FEED_ORDER = {
    "TRADE": 0,
    "QUOTE": 1,
    "MARK_INDEX_PRICE": 2,
    "BOOK_SNAPSHOT": 3,
    "BOOK_DELTA": 4,
    "BAR": 5,
}


@dataclass(frozen=True, slots=True)
class LogicalConsumerSession:
    """One independent workload session over an existing authenticated identity."""

    ordinal: int
    consumer_id: str
    products: tuple[AcceptanceProduct, ...]

    def __post_init__(self) -> None:
        if self.ordinal < 1 or not self.consumer_id or not 2 <= len(self.products) <= 5:
            raise ValueError("logical session fields are invalid")
        if any(item.consumer_id != self.consumer_id for item in self.products):
            raise ValueError("logical session mixes consumer identities")
        identities = [item.identity for item in self.products]
        if len(identities) != len(set(identities)):
            raise ValueError("logical session contains duplicate products")


@dataclass(frozen=True, slots=True)
class IdentityLoadBudget:
    """Quota-derived admission budget for one already registered identity."""

    consumer_id: str
    logical_sessions: int
    requests_per_minute: int
    test_requests_per_minute: int
    seconds_per_request: float
    max_streams: int
    planned_streams: int

    def __post_init__(self) -> None:
        if (
            not self.consumer_id
            or self.logical_sessions < 1
            or self.requests_per_minute < 1
            or not 1 <= self.test_requests_per_minute <= self.requests_per_minute
            or self.seconds_per_request <= 0
            or self.max_streams < 1
            or not 0 <= self.planned_streams <= self.max_streams
        ):
            raise ValueError("identity load budget is invalid")


@dataclass(frozen=True, slots=True)
class ConsumerLoadPlan:
    """A deterministic no-order workload with explicit session/identity counts."""

    logical_sessions: tuple[LogicalConsumerSession, ...]
    identity_budgets: tuple[IdentityLoadBudget, ...]
    covered_instruments: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if not self.logical_sessions or not self.identity_budgets:
            raise ValueError("consumer load plan cannot be empty")
        budget_ids = {item.consumer_id for item in self.identity_budgets}
        session_ids = {item.consumer_id for item in self.logical_sessions}
        if budget_ids != session_ids:
            raise ValueError("consumer load plan budget/session identities differ")
        if tuple(sorted(set(self.covered_instruments))) != self.covered_instruments:
            raise ValueError("consumer load coverage must be sorted and unique")

    @property
    def identity_count(self) -> int:
        return len(self.identity_budgets)

    @property
    def stream_count(self) -> int:
        return sum(item.planned_streams for item in self.identity_budgets)


def _product_sort_key(product: AcceptanceProduct) -> tuple[object, ...]:
    return (
        product.venue,
        product.native_symbol,
        _HOT_FEED_ORDER.get(product.feed.value, 99),
        product.feed.value,
        product.interval or "",
        product.source_policy_id,
    )


def _interleave_by_instrument(
    products: Sequence[AcceptanceProduct],
) -> tuple[AcceptanceProduct, ...]:
    """Avoid a sorted BAR-heavy prefix hiding an instrument during a load run."""

    groups: dict[tuple[str, str], list[AcceptanceProduct]] = defaultdict(list)
    for product in products:
        groups[(product.venue, product.native_symbol)].append(product)
    if len(groups) < 1:
        raise ValueError("load identity has no eligible products")
    ordered_groups = [
        sorted(groups[key], key=_product_sort_key)
        for key in sorted(groups)
    ]
    result: list[AcceptanceProduct] = []
    index = 0
    while True:
        emitted = False
        for group in ordered_groups:
            if index < len(group):
                result.append(group[index])
                emitted = True
        if not emitted:
            break
        index += 1
    return tuple(result)


def _session_products(
    values: Sequence[AcceptanceProduct], *, start: int, count: int
) -> tuple[AcceptanceProduct, ...]:
    selected: list[AcceptanceProduct] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for offset in range(len(values)):
        candidate = values[(start + offset) % len(values)]
        if candidate.identity in seen:
            continue
        selected.append(candidate)
        seen.add(candidate.identity)
        if len(selected) == count:
            return tuple(selected)
    raise ValueError("load identity cannot provide a unique session product mix")


def _ensure_streamable_product(
    selected: tuple[AcceptanceProduct, ...],
    values: Sequence[AcceptanceProduct],
    *,
    start: int,
) -> tuple[AcceptanceProduct, ...]:
    """Keep one high-activity consumer feed inside each logical session.

    The driver measures a stream over one of the session's declared feeds. A
    deterministic trade/quote replacement avoids accidentally treating a
    sparse reference or a long BAR as evidence that a stream connection is
    healthy.
    """

    streamable = {"TRADE", "QUOTE", "BOOK_DELTA", "BOOK_SNAPSHOT"}

    def is_streamable(item: AcceptanceProduct) -> bool:
        # Unit-test stand-ins predate DeliveryClass and intentionally omit it;
        # real acceptance products must be durable before a session can count
        # them as a live stream subscription.
        delivery = getattr(getattr(item, "delivery", None), "value", "DURABLE")
        return item.feed.value in streamable and delivery == "DURABLE"

    if any(is_streamable(item) for item in selected):
        return selected
    seen = {item.identity for item in selected[:-1]}
    for offset in range(len(values)):
        candidate = values[(start + offset) % len(values)]
        if is_streamable(candidate) and candidate.identity not in seen:
            return (*selected[:-1], candidate)
    raise ValueError("load identity has no streamable product")


def _instrument_pair(product: AcceptanceProduct) -> tuple[str, str]:
    return (product.venue, product.native_symbol)


def _coverage_counts(
    sessions: Sequence[LogicalConsumerSession],
) -> Counter[tuple[str, str]]:
    return Counter(
        _instrument_pair(product)
        for session in sessions
        for product in session.products
    )


def _ensure_required_instrument_coverage(
    sessions: Sequence[LogicalConsumerSession],
    *,
    eligible: Mapping[str, Sequence[AcceptanceProduct]],
    required_instruments: Sequence[tuple[str, str]],
) -> tuple[LogicalConsumerSession, ...]:
    """Make every bounded stage cover its declared venue-symbol scope.

    The logical-session count is deliberately independent of the demanded
    universe.  For small stages, deterministically append a missing declared
    product to a same-identity session with spare capacity; only then replace a
    redundant pair.  This keeps each session at `2..5` products, retains the
    sealed identity/quota allocation, and fails before traffic when coverage is
    impossible rather than silently shrinking the denominator.
    """

    required = tuple(sorted(set(required_instruments)))
    if not required:
        return tuple(sessions)
    if any(not venue or not symbol for venue, symbol in required):
        raise ValueError("required instrument coverage contains an empty identity")

    result = list(sessions)
    for pair in required:
        coverage = _coverage_counts(result)
        if coverage[pair]:
            continue
        candidate_by_consumer = {
            consumer_id: tuple(sorted(
                (product for product in values if _instrument_pair(product) == pair),
                key=_product_sort_key,
            ))
            for consumer_id, values in eligible.items()
        }
        candidate_by_consumer = {
            consumer_id: values
            for consumer_id, values in candidate_by_consumer.items()
            if values
        }
        applied = False
        for session_index in sorted(
            range(len(result)),
            key=lambda index: (len(result[index].products), result[index].ordinal),
        ):
            session = result[session_index]
            if len(session.products) >= 5:
                continue
            existing = {product.identity for product in session.products}
            candidate = next(
                (product for product in candidate_by_consumer.get(session.consumer_id, ())
                 if product.identity not in existing),
                None,
            )
            if candidate is None:
                continue
            result[session_index] = LogicalConsumerSession(
                ordinal=session.ordinal,
                consumer_id=session.consumer_id,
                products=(*session.products, candidate),
            )
            applied = True
            break
        if applied:
            continue

        coverage = _coverage_counts(result)
        for session_index in sorted(
            range(len(result)), key=lambda index: result[index].ordinal
        ):
            session = result[session_index]
            existing = {product.identity for product in session.products}
            candidate = next(
                (product for product in candidate_by_consumer.get(session.consumer_id, ())
                 if product.identity not in existing),
                None,
            )
            if candidate is None:
                continue
            replace_index = next(
                (
                    index for index, product in enumerate(session.products)
                    if coverage[_instrument_pair(product)] > 1
                ),
                None,
            )
            if replace_index is None:
                continue
            products = list(session.products)
            products[replace_index] = candidate
            result[session_index] = LogicalConsumerSession(
                ordinal=session.ordinal,
                consumer_id=session.consumer_id,
                products=tuple(products),
            )
            applied = True
            break
        if not applied:
            raise ValueError(
                "load plan cannot cover required instrument " + repr(pair)
            )

    missing = set(required) - set(_coverage_counts(result))
    if missing:
        raise ValueError(
            "load plan misses required instruments: " + repr(sorted(missing))
        )
    return tuple(result)


def build_consumer_load_plan(
    *,
    manifests: Mapping[str, ConsumerManifest],
    products_by_consumer: Mapping[str, Sequence[AcceptanceProduct]],
    logical_session_count: int,
    test_quota_fraction: float = 0.10,
    extra_streams_per_identity: int = 1,
    required_instruments: Sequence[tuple[str, str]] = (),
) -> ConsumerLoadPlan:
    """Render `2..5`-feed sessions without inventing identities or quota.

    Every planned session opens at most one stream.  A caller must therefore
    fail before traffic starts if its requested logical-session count cannot fit
    the sealed stream quota of any selected authenticated identity.
    """

    if not 1 <= logical_session_count <= 500:
        raise ValueError("logical session count must be within 1..500")
    if not 0.0 < test_quota_fraction <= 0.10:
        raise ValueError("test quota fraction must be positive and at most ten percent")
    if not 0 <= extra_streams_per_identity <= 4:
        raise ValueError("extra stream probes must be within 0..4 per identity")
    consumer_ids = tuple(sorted(products_by_consumer))
    if not consumer_ids or set(consumer_ids) != set(manifests):
        raise ValueError("load manifests and products must use the same identities")

    eligible: dict[str, tuple[AcceptanceProduct, ...]] = {}
    for consumer_id in consumer_ids:
        values = tuple(products_by_consumer[consumer_id])
        if len(values) < 2:
            raise ValueError("each load identity requires at least two eligible products")
        if any(item.consumer_id != consumer_id for item in values):
            raise ValueError("load products are assigned to the wrong identity")
        eligible[consumer_id] = _interleave_by_instrument(values)

    sessions_by_consumer: dict[str, int] = defaultdict(int)
    cursors: dict[str, int] = defaultdict(int)
    sessions: list[LogicalConsumerSession] = []
    for ordinal in range(1, logical_session_count + 1):
        consumer_id = consumer_ids[(ordinal - 1) % len(consumer_ids)]
        values = eligible[consumer_id]
        feed_count = 2 + ((ordinal - 1) % 4)
        count = min(feed_count, len(values), 5)
        selected = _ensure_streamable_product(
            _session_products(values, start=cursors[consumer_id], count=count),
            values,
            start=cursors[consumer_id],
        )
        session = LogicalConsumerSession(
            ordinal=ordinal,
            consumer_id=consumer_id,
            products=selected,
        )
        cursors[consumer_id] = (cursors[consumer_id] + count) % len(values)
        sessions_by_consumer[consumer_id] += 1
        sessions.append(session)

    sessions = list(_ensure_required_instrument_coverage(
        sessions,
        eligible=eligible,
        required_instruments=required_instruments,
    ))

    budgets: list[IdentityLoadBudget] = []
    for consumer_id in consumer_ids:
        manifest = manifests[consumer_id]
        session_count = sessions_by_consumer[consumer_id]
        safe_rpm = max(1, int(manifest.quotas.requests_per_minute * test_quota_fraction))
        planned_streams = session_count + extra_streams_per_identity
        if planned_streams > manifest.quotas.max_streams:
            raise ValueError(
                "logical stream sessions exceed sealed max_streams for " + consumer_id
            )
        budgets.append(IdentityLoadBudget(
            consumer_id=consumer_id,
            logical_sessions=session_count,
            requests_per_minute=manifest.quotas.requests_per_minute,
            test_requests_per_minute=safe_rpm,
            seconds_per_request=60.0 / safe_rpm,
            max_streams=manifest.quotas.max_streams,
            planned_streams=planned_streams,
        ))

    coverage = tuple(sorted({
        (product.venue, product.native_symbol)
        for session in sessions
        for product in session.products
    }))
    return ConsumerLoadPlan(
        logical_sessions=tuple(sessions),
        identity_budgets=tuple(budgets),
        covered_instruments=coverage,
    )


def assert_required_instrument_coverage(
    plan: ConsumerLoadPlan,
    required: Sequence[tuple[str, str]],
) -> None:
    """Require a declared final stage to include every requested venue/symbol."""

    expected = frozenset(required)
    if not expected:
        raise ValueError("required coverage cannot be empty")
    missing = expected - frozenset(plan.covered_instruments)
    if missing:
        raise ValueError("load plan misses required instruments: " + repr(sorted(missing)))
