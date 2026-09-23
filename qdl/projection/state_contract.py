"""Kafka-native state contract (KN-1 K1.3), shared with the Rust projector.

Frozen here, before any projector exists, so KN-3 implements rules rather than
inventing them:

* ``LogicalProductKey`` - product identity for latest/BAR state. It is not the
  physical Kafka key: BOOK snapshot and delta share one physical partition to
  keep order, but are distinct logical products (guide KD04).
* ``SourceCoordinate`` vs ``ChangelogCoordinate`` - the canonical record a
  state was derived from, versus where the derived state record itself sits.
  Public cursors and watermarks only ever carry the source coordinate (KD08).
* ``latest_apply_decision`` - stage-B idempotent apply for latest state:
  offsets compare only inside one topic identity and physical partition.
* ``bar_revision_decision`` - append-only BAR revision rules: a final bar is
  never replaced by an in-progress update (invariant 27); an equal revision
  with different content is a conflict, never last-write-wins (KD05).
* ``cache_read_state`` - a cached product is served only from the published
  ready generation of the market cache (guide 18.4.4).

Every rule has golden vectors in ``contracts/golden/kn_v220/`` that the Rust
implementation must reproduce.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re

LPK_VERSION = "lpk1"
_LPK_FIELD = re.compile(r"^[A-Za-z0-9._:-]{1,96}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
MAX_OFFSET = 2**63 - 1


@dataclass(frozen=True)
class LogicalProductKey:
    environment: str
    venue: str
    market: str
    instrument_uid: str
    feed: str
    qualifier: str = "-"

    def __post_init__(self) -> None:
        for name in ("environment", "venue", "market", "instrument_uid", "feed", "qualifier"):
            if not _LPK_FIELD.match(getattr(self, name)):
                raise ValueError(f"logical product key field is invalid: {name}")
        if self.venue != self.venue.upper() or self.market != self.market.upper() or self.feed != self.feed.upper():
            raise ValueError("venue, market and feed are upper-case enum names")

    @classmethod
    def for_product(
        cls, *, environment: str, venue: str, market: str, instrument_uid: str,
        feed: str, interval: str | None,
    ) -> "LogicalProductKey":
        return cls(environment, venue, market, instrument_uid, feed, interval or "-")

    def encode(self) -> str:
        return "|".join((LPK_VERSION, self.environment, self.venue, self.market,
                         self.instrument_uid, self.feed, self.qualifier))

    @classmethod
    def parse(cls, value: str) -> "LogicalProductKey":
        parts = value.split("|")
        if len(parts) != 7 or parts[0] != LPK_VERSION:
            raise ValueError("unsupported logical product key")
        key = cls(*parts[1:])
        if key.encode() != value:
            raise ValueError("logical product key is not canonical")
        return key


@dataclass(frozen=True)
class SourceCoordinate:
    """The committed canonical record a state was derived from."""

    topic_id: str
    partition: int
    offset: int

    def __post_init__(self) -> None:
        if not self.topic_id or not 0 <= self.partition < 1 << 31 or not 0 <= self.offset <= MAX_OFFSET:
            raise ValueError("source coordinate is out of range")


@dataclass(frozen=True)
class ChangelogCoordinate:
    """Where a derived state record sits; delivery metadata, never a cursor."""

    topic: str
    partition: int
    offset: int
    materializer_epoch: int

    def __post_init__(self) -> None:
        if (not self.topic or not 0 <= self.partition < 1 << 31
                or not 0 <= self.offset <= MAX_OFFSET or self.materializer_epoch < 1):
            raise ValueError("changelog coordinate is out of range")


class ApplyDecision(StrEnum):
    APPLY = "APPLY"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    NOT_COMPARABLE = "NOT_COMPARABLE"
    STALE_IN_PROGRESS = "STALE_IN_PROGRESS"
    STALE_REVISION = "STALE_REVISION"
    CONFLICT = "CONFLICT"


def latest_apply_decision(current: SourceCoordinate | None, incoming: SourceCoordinate) -> ApplyDecision:
    """Stage-B compare for a latest-state product.

    Offsets are comparable only within one topic identity and partition. A
    different topic identity (topic recreated) or partition (new plan) is a
    generation change that the sink fence must resolve; it is never ordered by
    comparing raw numbers.
    """

    if current is None:
        return ApplyDecision.APPLY
    if (current.topic_id, current.partition) != (incoming.topic_id, incoming.partition):
        return ApplyDecision.NOT_COMPARABLE
    if incoming.offset > current.offset:
        return ApplyDecision.APPLY
    if incoming.offset == current.offset:
        return ApplyDecision.DUPLICATE
    return ApplyDecision.STALE


@dataclass(frozen=True)
class BarState:
    is_final: bool
    revision: int
    content_sha256: str
    source: SourceCoordinate

    def __post_init__(self) -> None:
        if not 0 <= self.revision < 1 << 32:
            raise ValueError("bar revision is a uint32")
        if not _HEX64.match(self.content_sha256):
            raise ValueError("bar content hash must be lowercase SHA-256")


def bar_revision_decision(current: BarState | None, incoming: BarState) -> ApplyDecision:
    """Decide one incoming bar against the current row at the same open time."""

    if current is None:
        return ApplyDecision.APPLY
    if current.is_final and not incoming.is_final:
        return ApplyDecision.STALE_IN_PROGRESS
    if not current.is_final and incoming.is_final:
        return ApplyDecision.APPLY
    if not current.is_final:
        # Two in-progress updates: canonical order decides, within one partition.
        return latest_apply_decision(current.source, incoming.source)
    if incoming.revision > current.revision:
        return ApplyDecision.APPLY
    if incoming.revision < current.revision:
        return ApplyDecision.STALE_REVISION
    if incoming.content_sha256 == current.content_sha256:
        return ApplyDecision.DUPLICATE
    return ApplyDecision.CONFLICT


class CacheReadState(StrEnum):
    READY = "READY"
    NOT_READY_NO_GENERATION = "NOT_READY_NO_GENERATION"
    NOT_READY_MISSING = "NOT_READY_MISSING"
    NOT_READY_OTHER_GENERATION = "NOT_READY_OTHER_GENERATION"


def cache_read_state(ready_generation: int | None, entry_generation: int | None) -> CacheReadState:
    """A product is served only from the published ready cache generation.

    A staging rebuild writes a new generation beside the old one and publishes
    it atomically; an entry from any other generation is not served, and a
    missing entry is typed NOT_READY rather than a default value.
    """

    if ready_generation is None:
        return CacheReadState.NOT_READY_NO_GENERATION
    if entry_generation is None:
        return CacheReadState.NOT_READY_MISSING
    if entry_generation != ready_generation:
        return CacheReadState.NOT_READY_OTHER_GENERATION
    return CacheReadState.READY
