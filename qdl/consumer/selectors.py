"""Universe selectors: read entitlements granted by a declared universe, not row by row.

DL v2.3.0. ``alpha-okx-paper.yaml`` lists one requirement per instrument and
interval (375 rows in r15); 150 OKX instruments x 14 BAR intervals is 2,100.
A selector grants the same rows from one line:

    spec:
      selectors:
      - universe: okx-swap-tier-e        # <universe_dir>/okx-swap-tier-e.json
        venue: OKX
        market: SWAP
        intervals: [1m, 5m, 1H, 4H]      # optional; else the template interval
        template:                        # a DataRequirement without instrument_uid
          feed: BAR
          consumer_grade: ALPHA
          source_policy_id: crypto_primary_v2
          warmup_limit: 10000
          ...

Rules (fail closed):

* only ALPHA and RESEARCH grades: execution-grade rows stay explicit, because a
  Trading System execution entitlement is reviewed per instrument;
* expansion is deterministic: members in sorted order, intervals in declared
  order, each member resolved to its catalogue ``instrument_uid`` through the
  mounted catalogue (venue, market, native symbol); a member the catalogue does
  not know refuses the manifest;
* every expansion is audited: universe id, revision, file sha256, member count,
  row count and the sha256 of the expanded rows, carried on the manifest
  (``selector_expansions``), logged by the runtime and written into the gateway
  bundle, so Python and Rust enforce the same, inspectable rows;
* adding a member to the universe file grants the new rows on the next ordered
  restart with no manifest revision; removing one is a removal and the
  declaration gate requires a migration record for it.

Boundary: pure expansion. It never reads the network or runtime state.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from qdl.query import ConsumerGrade, DataRequirement

SELECTOR_FIELDS = frozenset({"universe", "venue", "market", "intervals", "template"})
SELECTOR_GRADES = frozenset({ConsumerGrade.ALPHA, ConsumerGrade.RESEARCH})
MAX_SELECTORS = 64
MAX_EXPANDED_REQUIREMENTS = 16_384


def _requirement_row(requirement: DataRequirement) -> dict[str, Any]:
    row = asdict(requirement)
    return {key: getattr(value, "value", value) for key, value in row.items()}


def requirements_sha256(requirements: tuple[DataRequirement, ...]) -> str:
    """Order-independent digest of a requirement set (one canonical row per line)."""

    rows = sorted(
        json.dumps(_requirement_row(item), sort_keys=True, separators=(",", ":"), default=str)
        for item in requirements
    )
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class SelectorExpansion:
    universe_id: str
    universe_revision: int
    universe_sha256: str
    venue: str
    market: str
    members: int
    requirements: int
    requirements_sha256: str

    def as_record(self) -> dict[str, Any]:
        return asdict(self)


class SelectorContext:
    """Resolves selectors against mounted universe files and the mounted catalogue.

    ``universe_loader(universe_id, venue, market)`` returns a
    ``qdl.runtime.declaration.DeclaredUniverse``; ``uid_for(venue, market,
    native_symbol)`` returns the catalogue instrument uid or None.
    """

    def __init__(
        self,
        *,
        universe_loader: Callable[[str, str, str], Any],
        uid_for: Callable[[str, str, str], str | None],
    ) -> None:
        self._universe_loader = universe_loader
        self._uid_for = uid_for

    @classmethod
    def from_catalog(cls, catalog: Any, *, universe_dir: str | Path) -> "SelectorContext":
        from qdl.runtime.declaration import load_universe

        root = Path(universe_dir)
        by_symbol: dict[tuple[str, str, str], str] = {}
        for record in catalog.instruments:
            identity = record.identity
            by_symbol[(identity.venue, identity.market, record.native_symbol)] = (
                identity.instrument_uid
            )

        def universe_loader(universe_id: str, venue: str, market: str):
            if "/" in universe_id or universe_id.startswith("."):
                raise ValueError("selector universe id must be a plain name")
            return load_universe(root / f"{universe_id}.json", venue=venue, market=market)

        return cls(
            universe_loader=universe_loader,
            uid_for=lambda venue, market, symbol: by_symbol.get((venue, market, symbol)),
        )

    def expand(
        self, selector: Mapping[str, Any]
    ) -> tuple[tuple[DataRequirement, ...], SelectorExpansion]:
        if not isinstance(selector, Mapping) or set(selector) - SELECTOR_FIELDS or not {
            "universe", "venue", "market", "template",
        } <= set(selector):
            raise ValueError("manifest selector fields are incomplete or unknown")
        template = selector["template"]
        if not isinstance(template, Mapping) or "instrument_uid" in template:
            raise ValueError("manifest selector template must omit instrument_uid")
        intervals_raw = selector.get("intervals")
        if intervals_raw is None:
            intervals: tuple[str | None, ...] = (template.get("interval"),)
        else:
            if (
                not isinstance(intervals_raw, list)
                or not intervals_raw
                or len(set(intervals_raw)) != len(intervals_raw)
                or "interval" in template
            ):
                raise ValueError("manifest selector intervals must be unique and replace the template interval")
            intervals = tuple(str(item) for item in intervals_raw)
        venue = str(selector["venue"]).upper()
        market = str(selector["market"]).upper()
        universe = self._universe_loader(str(selector["universe"]), venue, market)
        rows: list[DataRequirement] = []
        missing: list[str] = []
        for symbol in sorted(universe.members):
            uid = self._uid_for(venue, market, symbol)
            if uid is None:
                missing.append(symbol)
                continue
            for interval in intervals:
                payload = dict(template)
                payload["instrument_uid"] = uid
                if interval is not None:
                    payload["interval"] = interval
                requirement = DataRequirement.from_mapping(payload)
                if requirement.consumer_grade not in SELECTOR_GRADES:
                    raise ValueError(
                        "manifest selectors grant ALPHA/RESEARCH reads only; "
                        "execution rows stay explicit"
                    )
                rows.append(requirement)
        if missing:
            raise ValueError(
                f"universe {universe.universe_id} r{universe.revision} has members the "
                f"catalogue does not declare for {venue}/{market}: {','.join(missing[:10])}"
            )
        expanded = tuple(rows)
        return expanded, SelectorExpansion(
            universe_id=universe.universe_id,
            universe_revision=universe.revision,
            universe_sha256=universe.sha256,
            venue=venue,
            market=market,
            members=len(universe.members),
            requirements=len(expanded),
            requirements_sha256=requirements_sha256(expanded),
        )


class LazySelectorContext:
    """A ``SelectorContext`` built on the first selector, never for a plain manifest.

    A runtime that loads only explicit manifests must not pay for, or depend on,
    the catalogue/universe read a selector needs.
    """

    def __init__(self, factory: Callable[[], SelectorContext]) -> None:
        self._factory = factory
        self._context: SelectorContext | None = None

    def expand(self, selector: Mapping[str, Any]):
        if self._context is None:
            self._context = self._factory()
        return self._context.expand(selector)
