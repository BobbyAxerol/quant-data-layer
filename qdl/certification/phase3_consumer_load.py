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


# ---------------------------------------------------------------------------
# v2.1.1 frozen target workload
#
# The preflight planner above derives its request rate from ten percent of each
# identity's sealed quota and opens at most one stream per session. That is a
# safe mapping check, not the owner's target. The target below is the frozen
# four-class profile of the v2.1.1 closure contract: the offered load is an
# INPUT (declared streams and fixed request periods per class), and the plan
# refuses to exist when a sealed identity quota cannot carry it, instead of
# quietly lowering the rate to fit.
# ---------------------------------------------------------------------------

TARGET_STAGE_MIX: dict[int, tuple[int, int, int, int]] = {
    # candle, realtime, grid, multi
    5: (2, 1, 1, 1),
    20: (8, 6, 4, 2),
    35: (14, 10, 7, 4),
    50: (20, 15, 10, 5),
}
TARGET_STAGE_SECONDS: dict[int, int] = {5: 90, 20: 120, 35: 180, 50: 300}
TARGET_CLASSES = ("CANDLE", "REALTIME", "GRID", "MULTI")
TARGET_HOT_PERIOD_SECONDS = 1.0
TARGET_REFERENCE_PERIOD_SECONDS = 60.0
# Sealed quota is a fixed-minute window shared by both Query replicas through
# Redis, with no burst smoothing, so start-up warmups, reconnects and the final
# 25% burst land in the same minute as steady traffic.
TARGET_QUOTA_HEADROOM = 1.5
TARGET_STREAM_HEADROOM = 1.2


@dataclass(frozen=True, slots=True)
class TargetPoll:
    """One fixed-period read a logical alpha issues for the whole observation."""

    products: tuple[AcceptanceProduct, ...]
    period_seconds: float
    operation: str

    def __post_init__(self) -> None:
        if not self.products or self.period_seconds <= 0:
            raise ValueError("target poll is invalid")
        if self.operation not in {"SNAPSHOT", "REFERENCE_BATCH"}:
            raise ValueError("target poll operation is unknown")
        if self.operation == "SNAPSHOT" and len(self.products) != 1:
            raise ValueError("a snapshot poll reads exactly one product")


@dataclass(frozen=True, slots=True)
class TargetAlphaSession:
    ordinal: int
    consumer_id: str
    alpha_class: str
    streams: tuple[AcceptanceProduct, ...]
    polls: tuple[TargetPoll, ...]
    startup_snapshots: tuple[AcceptanceProduct, ...] = ()

    def __post_init__(self) -> None:
        if self.alpha_class not in TARGET_CLASSES or self.ordinal < 1:
            raise ValueError("target session class or ordinal is invalid")
        if not self.streams or not self.polls:
            raise ValueError("target session needs streams and polls")
        products = (*self.streams, *(p for poll in self.polls for p in poll.products),
                    *self.startup_snapshots)
        if any(item.consumer_id != self.consumer_id for item in products):
            raise ValueError("target session mixes consumer identities")
        declared = {item.identity for item in products}
        if not 2 <= len(declared) <= 6:
            raise ValueError("target session must declare 2..5 products plus a startup snapshot")

    @property
    def requests_per_minute(self) -> float:
        return sum(60.0 / poll.period_seconds for poll in self.polls)


@dataclass(frozen=True, slots=True)
class TargetIdentityDemand:
    consumer_id: str
    sessions: int
    required_requests_per_minute: int
    required_streams: int
    sealed_requests_per_minute: int
    sealed_max_streams: int

    @property
    def quota_needed(self) -> int:
        import math
        return math.ceil(self.required_requests_per_minute * TARGET_QUOTA_HEADROOM)

    @property
    def streams_needed(self) -> int:
        import math
        return math.ceil(self.required_streams * TARGET_STREAM_HEADROOM)

    @property
    def fits(self) -> bool:
        return (self.sealed_requests_per_minute >= self.quota_needed
                and self.sealed_max_streams >= self.streams_needed)


@dataclass(frozen=True, slots=True)
class TargetWorkloadPlan:
    stage: int
    sessions: tuple[TargetAlphaSession, ...]
    demands: tuple[TargetIdentityDemand, ...]

    @property
    def stream_count(self) -> int:
        return sum(len(item.streams) for item in self.sessions)

    @property
    def hot_requests_per_second(self) -> float:
        return sum(1.0 / poll.period_seconds for s in self.sessions for poll in s.polls
                   if poll.period_seconds <= TARGET_HOT_PERIOD_SECONDS)

    @property
    def class_counts(self) -> tuple[int, ...]:
        counts = Counter(item.alpha_class for item in self.sessions)
        return tuple(counts[name] for name in TARGET_CLASSES)

    @property
    def covered_instruments(self) -> tuple[tuple[str, str], ...]:
        return tuple(sorted({(p.venue, p.native_symbol) for s in self.sessions for p in s.streams}))


def _one(products: Sequence[AcceptanceProduct], venue: str, symbol: str,
         feed: str, interval: str | None = None) -> AcceptanceProduct:
    found = [item for item in products
             if item.venue == venue and item.native_symbol == symbol
             and item.feed.value == feed and (interval is None or item.interval == interval)]
    if len(found) != 1:
        raise ValueError(f"target workload needs exactly one {feed}{'/' + interval if interval else ''} "
                         f"for {venue} {symbol}; manifest declares {len(found)}")
    return found[0]


def build_target_workload_plan(
    *,
    stage: int,
    manifests: Mapping[str, ConsumerManifest],
    products_by_consumer: Mapping[str, Sequence[AcceptanceProduct]],
    venue_identity: Mapping[str, str],
    instruments: Mapping[str, Sequence[str]],
    enforce_quota: bool = True,
) -> TargetWorkloadPlan:
    """Materialize the frozen four-class workload for one stage.

    Sessions are assigned to venue/symbol pairs round-robin, so every stage of
    at least ten single-instrument sessions covers all ten pairs. A MULTI
    session takes two symbols of one venue because one alpha identity serves
    one venue. Raises before any traffic when a class product is missing from
    the identity's manifest or when a sealed quota cannot carry the demand with
    the declared headroom.
    """

    if stage not in TARGET_STAGE_MIX:
        raise ValueError(f"target stage must be one of {sorted(TARGET_STAGE_MIX)}")
    venues = sorted(venue_identity)
    # Interleave venues symbol by symbol so every stage, including five
    # sessions, spreads over both venues and both identities rather than
    # filling one venue first.
    columns = [sorted(instruments[venue]) for venue in venues]
    pairs = [(venue, column[row]) for row in range(max(map(len, columns)))
             for venue, column in zip(venues, columns) if row < len(column)]
    if not pairs:
        raise ValueError("target workload has no instruments")
    classes = [name for name, count in zip(TARGET_CLASSES, TARGET_STAGE_MIX[stage]) for _ in range(count)]
    sessions: list[TargetAlphaSession] = []
    for ordinal, alpha_class in enumerate(classes, start=1):
        venue, symbol = pairs[(ordinal - 1) % len(pairs)]
        consumer_id = venue_identity[venue]
        values = products_by_consumer[consumer_id]
        hot = lambda *items: TargetPoll(tuple(items), TARGET_HOT_PERIOD_SECONDS,
                                        "REFERENCE_BATCH" if items[0].feed.value == "MARK_INDEX_PRICE" else "SNAPSHOT")
        if alpha_class == "CANDLE":
            streams = (_one(values, venue, symbol, "BAR", "1m"),)
            polls = (hot(_one(values, venue, symbol, "QUOTE")),)
            startup = ()
        elif alpha_class == "REALTIME":
            streams = (_one(values, venue, symbol, "TRADE"), _one(values, venue, symbol, "QUOTE"))
            polls = (hot(_one(values, venue, symbol, "MARK_INDEX_PRICE")),)
            startup = ()
        elif alpha_class == "GRID":
            streams = (_one(values, venue, symbol, "BAR", "1m"), _one(values, venue, symbol, "QUOTE"),
                       _one(values, venue, symbol, "BOOK_DELTA"))
            polls = (hot(_one(values, venue, symbol, "MARK_INDEX_PRICE")),)
            startup = (_one(values, venue, symbol, "BOOK_SNAPSHOT"),)
        else:
            symbols = sorted(instruments[venue])
            second = symbols[(symbols.index(symbol) + 1) % len(symbols)]
            streams = (_one(values, venue, symbol, "QUOTE"), _one(values, venue, second, "QUOTE"))
            polls = (
                TargetPoll((_one(values, venue, symbol, "MARK_INDEX_PRICE"),
                            _one(values, venue, second, "MARK_INDEX_PRICE")),
                           TARGET_HOT_PERIOD_SECONDS, "REFERENCE_BATCH"),
                TargetPoll((_one(values, venue, symbol, "FUNDING_RATE"),),
                           TARGET_REFERENCE_PERIOD_SECONDS, "REFERENCE_BATCH"),
            )
            startup = ()
        sessions.append(TargetAlphaSession(ordinal, consumer_id, alpha_class, streams, polls, startup))

    demands = []
    for consumer_id in sorted({item.consumer_id for item in sessions}):
        mine = [item for item in sessions if item.consumer_id == consumer_id]
        quotas = manifests[consumer_id].quotas
        demand = TargetIdentityDemand(
            consumer_id=consumer_id,
            sessions=len(mine),
            required_requests_per_minute=round(sum(item.requests_per_minute for item in mine)),
            required_streams=sum(len(item.streams) for item in mine),
            sealed_requests_per_minute=quotas.requests_per_minute,
            sealed_max_streams=quotas.max_streams,
        )
        if enforce_quota and not demand.fits:
            raise ValueError(
                f"target stage {stage} does not fit sealed quota of {consumer_id}: needs "
                f"{demand.quota_needed} rpm / {demand.streams_needed} streams with headroom, sealed "
                f"{demand.sealed_requests_per_minute} rpm / {demand.sealed_max_streams} streams")
        demands.append(demand)
    return TargetWorkloadPlan(stage=stage, sessions=tuple(sessions), demands=tuple(demands))
