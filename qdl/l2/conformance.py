"""Deterministic Python reference for Phase 10.4-C Rust L2 conformance.

This module has no provider I/O, endpoint, Kafka/Redis, or runtime wiring. It
is deliberately a small independent oracle for the shared synthetic protocol
fixture. Production adapters must use the Rust core after a later, separately
approved runtime slice.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum


_MAX_DECIMAL_DIGITS = 256
_MAX_ABS_DECIMAL_SCALE = 1_024


class SequencePolicy(str, Enum):
    PREVIOUS_SEQUENCE = "PREVIOUS_SEQUENCE"
    RANGE_BRIDGE_THEN_PREVIOUS = "RANGE_BRIDGE_THEN_PREVIOUS"
    SNAPSHOT_ONLY = "SNAPSHOT_ONLY"


class ChecksumPolicy(str, Enum):
    IGNORE = "IGNORE"
    VERIFY_IF_PRESENT = "VERIFY_IF_PRESENT"
    REQUIRE_VERIFIED = "REQUIRE_VERIFIED"


class ChecksumEvidence(str, Enum):
    NOT_PROVIDED = "NOT_PROVIDED"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"


class SnapshotOrigin(str, Enum):
    WEBSOCKET = "WEBSOCKET"
    REST = "REST"


class BookStatus(str, Enum):
    AWAITING_SNAPSHOT = "AWAITING_SNAPSHOT"
    READY = "READY"
    GAPPED = "GAPPED"
    RESYNCING = "RESYNCING"
    DISCONNECTED = "DISCONNECTED"


@dataclass(frozen=True)
class BookIdentity:
    provider_profile: str
    instrument_uid: str
    channel: str

    def __post_init__(self) -> None:
        for value in (self.provider_profile, self.instrument_uid, self.channel):
            if not value.strip():
                raise ValueError("book identity fields must be nonblank")


@dataclass(frozen=True)
class BookConfig:
    identity: BookIdentity
    sequence_policy: SequencePolicy
    checksum_policy: ChecksumPolicy
    view_depth_per_side: int

    def __post_init__(self) -> None:
        if self.view_depth_per_side <= 0:
            raise ValueError("view_depth_per_side must be positive")


@dataclass(frozen=True)
class BookLevelInput:
    side: str
    price: str
    quantity: str
    order_count: int | None = None


@dataclass(frozen=True)
class _BookLevel:
    side: str
    price: Decimal
    quantity: Decimal
    order_count: int | None


@dataclass(frozen=True)
class BookView:
    generation: int
    last_sequence: int
    bids: tuple[_BookLevel, ...]
    asks: tuple[_BookLevel, ...]
    truncated: bool


def _parse_exact_decimal(source: str) -> Decimal:
    text = source.strip()
    if not text:
        raise ValueError("book decimal is required")
    _validate_decimal_lexeme(text)
    try:
        value = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("book decimal is invalid") from exc
    if not value.is_finite():
        raise ValueError("book decimal is invalid")
    _, digits, exponent = value.as_tuple()
    if len(digits) > _MAX_DECIMAL_DIGITS or abs(-exponent) > _MAX_ABS_DECIMAL_SCALE:
        raise ValueError("book decimal exceeds bounded core limits")
    rendered = "".join(str(digit) for digit in digits).lstrip("0")
    normalized_scale = -exponent
    while rendered.endswith("0"):
        rendered = rendered[:-1]
        normalized_scale -= 1
    if abs(normalized_scale) > _MAX_ABS_DECIMAL_SCALE:
        raise ValueError("book decimal exceeds bounded core limits")
    return value


def _validate_decimal_lexeme(text: str) -> None:
    """Match the Rust core's deliberately narrow ASCII decimal grammar."""
    unsigned = text[1:] if text[:1] in {"+", "-"} else text
    if not unsigned:
        raise ValueError("book decimal is invalid")
    exponent_positions = [
        index for index, character in enumerate(unsigned) if character in {"e", "E"}
    ]
    if len(exponent_positions) > 1:
        raise ValueError("book decimal is invalid")
    if exponent_positions:
        position = exponent_positions[0]
        base, exponent = unsigned[:position], unsigned[position + 1 :]
        if exponent[:1] in {"+", "-"}:
            exponent = exponent[1:]
        if not exponent or not _ascii_digits(exponent):
            raise ValueError("book decimal is invalid")
    else:
        base = unsigned
    if base.count(".") > 1:
        raise ValueError("book decimal is invalid")
    whole, _, fraction = base.partition(".")
    if not whole and not fraction:
        raise ValueError("book decimal is invalid")
    if not _ascii_digits(whole) or not _ascii_digits(fraction):
        raise ValueError("book decimal is invalid")


def _ascii_digits(value: str) -> bool:
    return all("0" <= character <= "9" for character in value)


def canonical_decimal_text(value: Decimal) -> str:
    sign, digits, exponent = value.as_tuple()
    rendered = "".join(str(digit) for digit in digits).lstrip("0")
    if not rendered:
        return "0"
    scale = -exponent
    while rendered.endswith("0"):
        rendered = rendered[:-1]
        scale -= 1
    prefix = "-" if sign else ""
    if scale <= 0:
        return f"{prefix}{rendered}{'0' * (-scale)}"
    if len(rendered) <= scale:
        return f"{prefix}0.{'0' * (scale - len(rendered))}{rendered}"
    split = len(rendered) - scale
    return f"{prefix}{rendered[:split]}.{rendered[split:]}"


class L2BookReference:
    """Independent, exact-decimal state oracle for the shared fixture only."""

    def __init__(self, config: BookConfig):
        self.config = config
        self.generation = 0
        self.status = BookStatus.AWAITING_SNAPSHOT
        self.last_sequence: int | None = None
        self._range_bridge_complete = False
        self._bids: dict[Decimal, _BookLevel] = {}
        self._asks: dict[Decimal, _BookLevel] = {}
        self.last_error: str | None = None

    def apply_snapshot(
        self,
        *,
        identity: BookIdentity,
        generation: int,
        sequence_end: int,
        checksum: ChecksumEvidence,
        origin: SnapshotOrigin,
        levels: list[BookLevelInput],
    ) -> str:
        if identity != self.config.identity:
            return "IDENTITY_MISMATCH"
        if origin is not SnapshotOrigin.WEBSOCKET:
            return "SNAPSHOT_SOURCE_REJECTED"
        if not self._accept_generation(generation):
            return "IGNORED_STALE_GENERATION"
        if not self._checksum_accepted(checksum):
            self._invalidate(BookStatus.GAPPED, "checksum rejected")
            return "CHECKSUM_REJECTED"
        try:
            bids, asks = self._parse_snapshot(levels)
        except ValueError as exc:
            self._invalidate(BookStatus.AWAITING_SNAPSHOT, str(exc))
            return "INVALID_FRAME"
        self._bids = bids
        self._asks = asks
        self.last_sequence = sequence_end
        self._range_bridge_complete = False
        self.status = BookStatus.READY
        self.last_error = None
        return "SNAPSHOT_APPLIED"

    def apply_delta(
        self,
        *,
        identity: BookIdentity,
        generation: int,
        sequence_start: int | None,
        previous_sequence: int | None,
        sequence_end: int,
        checksum: ChecksumEvidence,
        levels: list[BookLevelInput],
    ) -> str:
        if identity != self.config.identity:
            return "IDENTITY_MISMATCH"
        if not self._accept_generation(generation):
            return "IGNORED_STALE_GENERATION"
        if self.config.sequence_policy is SequencePolicy.SNAPSHOT_ONLY:
            return "DELTA_UNSUPPORTED"
        if self.status is not BookStatus.READY or self.last_sequence is None:
            return "REJECTED_AWAITING_SNAPSHOT"

        continuity = self._continuity(
            sequence_start=sequence_start,
            previous_sequence=previous_sequence,
            sequence_end=sequence_end,
            empty=not levels,
        )
        if continuity in {"KEEPALIVE", "DUPLICATE"}:
            return continuity
        if continuity in {"OUT_OF_ORDER", "SEQUENCE_GAP"}:
            self._invalidate(BookStatus.GAPPED, continuity)
            return continuity
        if not self._checksum_accepted(checksum):
            self._invalidate(BookStatus.GAPPED, "checksum rejected")
            return "CHECKSUM_REJECTED"
        try:
            updates = self._parse_delta(levels)
        except ValueError as exc:
            self._invalidate(BookStatus.GAPPED, str(exc))
            return "INVALID_FRAME"

        next_bids = dict(self._bids)
        next_asks = dict(self._asks)
        for level in updates:
            target = next_bids if level.side == "BID" else next_asks
            if level.quantity.is_zero():
                target.pop(level.price, None)
            else:
                target[level.price] = level
        self._bids = next_bids
        self._asks = next_asks
        self.last_sequence = sequence_end
        self._range_bridge_complete = (
            self._range_bridge_complete
            or self.config.sequence_policy is SequencePolicy.RANGE_BRIDGE_THEN_PREVIOUS
        )
        self.status = BookStatus.READY
        self.last_error = None
        return "DELTA_APPLIED"

    def request_resync(self, generation: int) -> str:
        if generation < self.generation:
            return "IGNORED_STALE_GENERATION"
        self.generation = generation
        self._clear_book()
        self.status = BookStatus.RESYNCING
        self.last_error = None
        return "RESYNC_REQUESTED"

    def disconnect(self) -> str:
        self._clear_book()
        self.status = BookStatus.DISCONNECTED
        self.last_error = None
        return "DISCONNECTED"

    def view(self) -> BookView | None:
        if self.status is not BookStatus.READY or self.last_sequence is None:
            return None
        bids = tuple(
            level
            for _, level in sorted(self._bids.items(), key=lambda item: item[0], reverse=True)[
                : self.config.view_depth_per_side
            ]
        )
        asks = tuple(
            level
            for _, level in sorted(self._asks.items(), key=lambda item: item[0])[
                : self.config.view_depth_per_side
            ]
        )
        return BookView(
            generation=self.generation,
            last_sequence=self.last_sequence,
            bids=bids,
            asks=asks,
            truncated=(
                len(self._bids) > self.config.view_depth_per_side
                or len(self._asks) > self.config.view_depth_per_side
            ),
        )

    def _accept_generation(self, incoming: int) -> bool:
        if incoming < self.generation:
            return False
        if incoming > self.generation:
            self.generation = incoming
            self._clear_book()
            self.status = BookStatus.AWAITING_SNAPSHOT
            self.last_error = None
        return True

    def _checksum_accepted(self, evidence: ChecksumEvidence) -> bool:
        if self.config.checksum_policy is ChecksumPolicy.IGNORE:
            return True
        if self.config.checksum_policy is ChecksumPolicy.VERIFY_IF_PRESENT:
            return evidence is not ChecksumEvidence.FAILED
        return evidence is ChecksumEvidence.VERIFIED

    def _continuity(
        self,
        *,
        sequence_start: int | None,
        previous_sequence: int | None,
        sequence_end: int,
        empty: bool,
    ) -> str:
        assert self.last_sequence is not None
        last = self.last_sequence
        if self.config.sequence_policy is SequencePolicy.PREVIOUS_SEQUENCE:
            if previous_sequence is None:
                return "SEQUENCE_GAP"
            if previous_sequence == last:
                if sequence_end == last:
                    return "KEEPALIVE" if empty else "DUPLICATE"
                return "APPLY"
            if sequence_end <= last:
                return "DUPLICATE"
            return "OUT_OF_ORDER" if previous_sequence < last else "SEQUENCE_GAP"

        if sequence_start is None or sequence_start > sequence_end:
            return "SEQUENCE_GAP"
        if sequence_end <= last:
            return "DUPLICATE"
        if not self._range_bridge_complete:
            expected = last + 1
            if sequence_start <= expected <= sequence_end:
                return "APPLY"
            return "SEQUENCE_GAP" if sequence_start > expected else "OUT_OF_ORDER"
        if previous_sequence == last:
            return "APPLY"
        if previous_sequence is not None and previous_sequence < last:
            return "OUT_OF_ORDER"
        return "SEQUENCE_GAP"

    def _parse_snapshot(
        self, levels: list[BookLevelInput]
    ) -> tuple[dict[Decimal, _BookLevel], dict[Decimal, _BookLevel]]:
        parsed = self._parse_levels(levels)
        bids: dict[Decimal, _BookLevel] = {}
        asks: dict[Decimal, _BookLevel] = {}
        seen: set[tuple[str, Decimal]] = set()
        for level in parsed:
            key = (level.side, level.price)
            if key in seen:
                raise ValueError("duplicate side/price snapshot")
            seen.add(key)
            if level.quantity.is_zero():
                continue
            target = bids if level.side == "BID" else asks
            target[level.price] = level
        return bids, asks

    def _parse_delta(self, levels: list[BookLevelInput]) -> list[_BookLevel]:
        parsed = self._parse_levels(levels)
        if len({(level.side, level.price) for level in parsed}) != len(parsed):
            raise ValueError("duplicate side/price delta")
        return parsed

    @staticmethod
    def _parse_levels(levels: list[BookLevelInput]) -> list[_BookLevel]:
        parsed: list[_BookLevel] = []
        for item in levels:
            if item.side not in {"BID", "ASK"}:
                raise ValueError("book side is invalid")
            price = _parse_exact_decimal(item.price)
            quantity = _parse_exact_decimal(item.quantity)
            if price <= 0:
                raise ValueError("book price must be positive")
            if quantity < 0:
                raise ValueError("book quantity must be nonnegative")
            parsed.append(
                _BookLevel(
                    side=item.side,
                    price=price,
                    quantity=quantity,
                    order_count=item.order_count,
                )
            )
        return parsed

    def _clear_book(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self.last_sequence = None
        self._range_bridge_complete = False

    def _invalidate(self, status: BookStatus, reason: str) -> None:
        self._clear_book()
        self.status = status
        self.last_error = reason
