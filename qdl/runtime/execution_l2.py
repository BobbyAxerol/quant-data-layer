"""Manifest-derived execution-grade L2 materialization policy.

The acquisition file owns provider protocol and bootstrap semantics.  The
production demand document owns which physical books are actually execution
products.  This module joins the two contracts and prevents a hand-maintained
symbol allow-list from silently drifting after demand expands.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from qdl.query import FeedType
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


EXECUTION_L2_MATERIALIZATION_INTERVAL_MS = 1_000
_BOOK_FEEDS = frozenset({FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA})


@dataclass(frozen=True, slots=True)
class ExecutionL2MaterializationPlan:
    """One explicit execution-book set compiled from production demand."""

    demand_path: Path
    demand_sha256: str
    source_ids: tuple[str, ...]
    binding_ids: tuple[str, ...]
    materialized_snapshot_interval_ms: int

    def evidence(self) -> dict[str, object]:
        return {
            "demand_path": str(self.demand_path),
            "demand_sha256": self.demand_sha256,
            "source_ids": list(self.source_ids),
            "binding_ids": list(self.binding_ids),
            "materialized_snapshot_interval_ms": self.materialized_snapshot_interval_ms,
        }


def _required_text(value: object, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"execution L2 demand {field} is required")
    return result


def _load_execution_book_rows(path: Path) -> tuple[dict[str, str], ...]:
    try:
        encoded = path.read_bytes()
        payload = yaml.safe_load(encoded)
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as error:
        raise ValueError(f"execution L2 demand is unreadable: {path}") from error
    if not isinstance(payload, Mapping) or payload.get("schema") != "qdl.v2.production-demand.v1":
        raise ValueError("execution L2 demand schema is invalid")
    consumers = payload.get("consumers")
    if not isinstance(consumers, list) or not consumers:
        raise ValueError("execution L2 demand has no consumers")
    rows: list[dict[str, str]] = []
    for consumer in consumers:
        if not isinstance(consumer, Mapping):
            raise ValueError("execution L2 demand consumer is invalid")
        if str(consumer.get("consumer_grade") or "").strip().upper() != "EXECUTION":
            continue
        requirements = consumer.get("requirements")
        if not isinstance(requirements, list):
            raise ValueError("execution L2 demand requirements are invalid")
        for requirement in requirements:
            if not isinstance(requirement, Mapping):
                raise ValueError("execution L2 demand requirement is invalid")
            try:
                feed = FeedType(_required_text(requirement.get("feed"), "feed").upper())
            except ValueError as error:
                raise ValueError("execution L2 demand feed is invalid") from error
            if feed not in _BOOK_FEEDS:
                continue
            depth = requirement.get("depth_per_side")
            freshness = requirement.get("max_freshness_ms")
            if (
                isinstance(depth, bool)
                or not isinstance(depth, int)
                or depth < 1
                or isinstance(freshness, bool)
                or not isinstance(freshness, int)
                or freshness < 1
                or requirement.get("require_live") is not True
            ):
                raise ValueError("execution L2 demand acquisition fields are invalid")
            rows.append({
                "venue": _required_text(requirement.get("venue"), "venue").upper(),
                "market": _required_text(requirement.get("market"), "market").upper(),
                "product_type": _required_text(requirement.get("product_type"), "product_type").upper(),
                "native_symbol": _required_text(requirement.get("native_symbol"), "native_symbol").upper(),
                "source_policy_id": _required_text(
                    requirement.get("source_policy_id"), "source_policy_id"
                ),
                "feed": feed.value,
            })
    if not rows:
        raise ValueError("execution L2 demand has no execution-grade book pairs")
    return tuple(rows)


def execution_l2_materialization_plan(
    *,
    demand_path: str | Path,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
    materialized_snapshot_interval_ms: int = EXECUTION_L2_MATERIALIZATION_INTERVAL_MS,
) -> ExecutionL2MaterializationPlan:
    """Resolve the only L2 sources eligible for hot execution materialization."""

    if not 100 <= materialized_snapshot_interval_ms <= 60_000:
        raise ValueError("execution L2 materialization cadence is outside bounds")
    path = Path(demand_path).resolve()
    encoded = path.read_bytes()
    rows = _load_execution_book_rows(path)
    catalog_by_identity = {
        (
            binding.instrument.identity.venue,
            binding.instrument.identity.market,
            binding.instrument.identity.product_type.value,
            binding.instrument.native_symbol.upper(),
            binding.source_policy_id,
            binding.feed.value,
        ): binding
        for binding in catalog.bindings
    }
    acquisition_by_id = {binding.binding_id: binding for binding in acquisition.bindings}
    source_rows: dict[str, dict[str, object]] = {}
    for row in rows:
        key = (
            row["venue"],
            row["market"],
            row["product_type"],
            row["native_symbol"],
            row["source_policy_id"],
            row["feed"],
        )
        source = catalog_by_identity.get(key)
        if source is None:
            raise ValueError(
                "execution L2 demand has no catalog binding: " + "/".join(key)
            )
        acquired = acquisition_by_id.get(source.binding_id)
        if acquired is None or acquired.l2 is None:
            raise ValueError(
                f"execution L2 demand is not backed by an acquired L2 source: {source.binding_id}"
            )
        record = source_rows.setdefault(
            source.source_id,
            {
                "feeds": set(),
                "binding_ids": set(),
                "l2": acquired.l2,
            },
        )
        if record["l2"] != acquired.l2:
            raise ValueError(f"execution L2 aliases disagree: {source.source_id}")
        feeds = record["feeds"]
        bindings = record["binding_ids"]
        assert isinstance(feeds, set) and isinstance(bindings, set)
        feeds.add(source.feed)
        bindings.add(source.binding_id)

    source_ids: list[str] = []
    binding_ids: list[str] = []
    for source_id, record in sorted(source_rows.items()):
        feeds = record["feeds"]
        bindings = record["binding_ids"]
        l2 = record["l2"]
        assert isinstance(feeds, set) and isinstance(bindings, set)
        if feeds != _BOOK_FEEDS or len(bindings) != 2:
            raise ValueError(f"execution L2 source lacks a complete snapshot/delta pair: {source_id}")
        if l2.snapshot_refresh_seconds != 30:
            raise ValueError(f"execution L2 provider refresh must remain 30 seconds: {source_id}")
        if l2.materialized_snapshot_interval_ms != materialized_snapshot_interval_ms:
            raise ValueError(
                "execution L2 materialization differs from the common policy: "
                f"{source_id}"
            )
        source_ids.append(source_id)
        binding_ids.extend(sorted(bindings))
    return ExecutionL2MaterializationPlan(
        demand_path=path,
        demand_sha256=hashlib.sha256(encoded).hexdigest(),
        source_ids=tuple(source_ids),
        binding_ids=tuple(sorted(binding_ids)),
        materialized_snapshot_interval_ms=materialized_snapshot_interval_ms,
    )
