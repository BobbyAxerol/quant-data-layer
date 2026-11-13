"""Bar-edge checkpoint across a catalogue/acquisition revision: keep what is unchanged.

DL v2.3.0. ``StableBinanceBarEdge._restore_state`` refused any checkpoint whose
``catalog_revision``, ``acquisition_revision`` or ``binding_ids`` differed from
the runtime (``stable BAR checkpoint ... differs from runtime authority``). So
any declaration that touched the catalogue (even one that only added QUOTE rows)
stopped the edge from starting, for every BAR binding on both venues: the
C.19/C.32 checkpoint strand (CLAUDE.md section 4.3). This module decides, per
binding, which watermarks survive a revision crossing:

* a binding present before and after with the same **series identity** keeps its
  watermark (``retained``);
* a binding only in the new plan is bootstrapped through the normal bounded,
  real-provider history path (``added``);
* a binding removed, or one whose series identity changed (instrument, symbol,
  interval, source, acquisition mode/kind/channel: e.g. native -> derived), is
  refused unless an explicit migration record names exactly that set; with the
  record its watermark is dropped and it re-bootstraps (``dropped``);
* revisions never move backwards: a rollback is a forward declaration.

The series identity is ``binding_series_digest``; a v5 checkpoint stores one per
binding. A legacy (v2-v4) checkpoint has none, so it may cross a revision only
with a migration record that carries ``previous_binding_digests``, computed by
``scripts/dl_declaration.py checkpoint-migration`` from the exact previous
catalogue/acquisition files. Same-revision restarts are unchanged.

Boundary: pure decision over already-parsed documents; the edge applies it.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

MIGRATION_SCHEMA = "qdl.stable-bar-edge-checkpoint-migration.v1"
MIGRATION_ENV = "QDL_STABLE_BAR_CHECKPOINT_MIGRATION"
_MIGRATION_FIELDS = frozenset({
    "schema", "from", "to", "removed_binding_ids", "changed_binding_ids",
    "previous_binding_digests", "reason", "approved_by",
})


class CheckpointCrossingRefused(RuntimeError):
    """The checkpoint cannot cross to the runtime revision without an explicit record."""


def binding_series_digest(source: Any, acquisition: Any) -> str:
    """What a BAR watermark means: which series, acquired how.

    Fields that do not change the meaning of "the last closed bar open" are
    left out on purpose (stale bounds, adapter/normaliser versions, URLs,
    revisions), so a metadata-only refresh never drops a watermark.
    """

    identity = source.instrument.identity
    body = {
        "binding_id": source.binding_id,
        "instrument_uid": identity.instrument_uid,
        "venue": identity.venue,
        "market": identity.market,
        "native_symbol": source.instrument.native_symbol,
        "provider": source.provider,
        "source_id": source.source_id,
        "source_policy_id": source.source_policy_id,
        "feed": getattr(source.feed, "value", source.feed),
        "interval": source.interval,
        "require_final_bar": source.require_final_bar,
        "continuous_calendar": source.continuous_calendar,
        "runtime": acquisition.runtime,
        "mode": acquisition.mode,
        "provider_kind": acquisition.provider_kind,
        "native_channel": acquisition.native_channel,
    }
    # DL v2.3.0 DERIVED_FINAL bindings name their base interval
    # (``StableDerivedBarAcquisition.base_interval``); included only when present
    # so every existing binding keeps its digest. A plain string is accepted for
    # a parsed mapping; anything else is refused rather than silently hashed.
    derived_from = getattr(acquisition, "derived_from", None)
    if derived_from is not None:
        base_interval = (
            derived_from if isinstance(derived_from, str)
            else getattr(derived_from, "base_interval", None)
        )
        if not isinstance(base_interval, str) or not base_interval.strip():
            raise ValueError(
                f"derived acquisition of {source.binding_id} has no base_interval to fingerprint"
            )
        body["derived_from"] = base_interval
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def load_migration_record(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise CheckpointCrossingRefused("bar-edge checkpoint migration record is unreadable") from error
    validate_migration_record(payload)
    return payload


def validate_migration_record(payload: Any) -> None:
    if not isinstance(payload, dict) or payload.get("schema") != MIGRATION_SCHEMA:
        raise CheckpointCrossingRefused("bar-edge checkpoint migration record schema is unsupported")
    if set(payload) - _MIGRATION_FIELDS or not {
        "schema", "from", "to", "removed_binding_ids", "changed_binding_ids", "reason",
        "approved_by",
    } <= set(payload):
        raise CheckpointCrossingRefused("bar-edge checkpoint migration record fields are invalid")
    for side in ("from", "to"):
        value = payload[side]
        if not isinstance(value, dict) or set(value) != {"catalog_revision", "acquisition_revision"} or any(
            isinstance(item, bool) or not isinstance(item, int) or item < 1 for item in value.values()
        ):
            raise CheckpointCrossingRefused(f"bar-edge checkpoint migration record {side} is invalid")
    for name in ("removed_binding_ids", "changed_binding_ids"):
        value = payload[name]
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value) or len(
            set(value)
        ) != len(value):
            raise CheckpointCrossingRefused(f"bar-edge checkpoint migration record {name} is invalid")
    digests = payload.get("previous_binding_digests")
    if digests is not None and (
        not isinstance(digests, dict)
        or any(not isinstance(value, str) or len(value) != 64 for value in digests.values())
    ):
        raise CheckpointCrossingRefused("bar-edge checkpoint migration record digests are invalid")
    for name in ("reason", "approved_by"):
        if not isinstance(payload[name], str) or not payload[name].strip():
            raise CheckpointCrossingRefused(f"bar-edge checkpoint migration record needs {name}")


@dataclass(frozen=True, slots=True)
class CheckpointCrossing:
    crossed: bool
    retained: frozenset[str]
    added: frozenset[str]
    dropped: frozenset[str]
    migration_used: bool

    def summary(self) -> str:
        return (
            f"crossed={self.crossed} retained={len(self.retained)} added={len(self.added)} "
            f"dropped={len(self.dropped)} migration_record={self.migration_used}"
        )


def _revisions(document: Mapping[str, Any]) -> dict[str, int]:
    return {
        "catalog_revision": int(document["catalog_revision"]),
        "acquisition_revision": int(document["acquisition_revision"]),
    }


def plan_crossing(
    *,
    checkpoint: Mapping[str, Any],
    runtime: Mapping[str, Any],
    current_digests: Mapping[str, str],
    migration: Mapping[str, Any] | None = None,
) -> CheckpointCrossing:
    """Decide which watermarks of ``checkpoint`` survive into ``runtime``.

    ``checkpoint``/``runtime`` carry ``catalog_revision``, ``acquisition_revision``
    and ``binding_ids``; ``checkpoint`` may carry ``binding_digests`` (v5).
    ``current_digests`` maps every runtime binding id to its series digest.
    """

    old_ids = frozenset(checkpoint["binding_ids"])
    new_ids = frozenset(runtime["binding_ids"])
    if set(current_digests) != set(new_ids):
        raise CheckpointCrossingRefused("bar-edge runtime binding digests do not cover its bindings")
    before = _revisions(checkpoint)
    after = _revisions(runtime)
    stored = checkpoint.get("binding_digests")
    if before == after:
        if old_ids != new_ids:
            raise CheckpointCrossingRefused(
                "stable BAR checkpoint binding_ids differ from runtime authority at the same revision"
            )
        if stored is not None and dict(stored) != dict(current_digests):
            raise CheckpointCrossingRefused(
                "stable BAR checkpoint binding identity changed without a revision"
            )
        return CheckpointCrossing(False, new_ids, frozenset(), frozenset(), False)
    for name, value in after.items():
        if value < before[name]:
            raise CheckpointCrossingRefused(
                f"stable BAR checkpoint {name} {before[name]} is newer than runtime {value}; "
                "a rollback is a forward declaration"
            )
    record = None
    if migration is not None:
        validate_migration_record(migration)
        if migration["from"] == before and migration["to"] == after:
            record = migration
    if stored is None:
        stored = None if record is None else record.get("previous_binding_digests")
        if stored is None:
            raise CheckpointCrossingRefused(
                "stable BAR legacy checkpoint cannot prove which bindings are unchanged across "
                f"catalog {before['catalog_revision']}->{after['catalog_revision']} / acquisition "
                f"{before['acquisition_revision']}->{after['acquisition_revision']}; render a "
                "migration record with previous_binding_digests (scripts/dl_declaration.py "
                "checkpoint-migration)"
            )
    if set(stored) != set(old_ids):
        raise CheckpointCrossingRefused("stable BAR checkpoint binding digests do not cover its bindings")
    removed = old_ids - new_ids
    added = new_ids - old_ids
    changed = frozenset(
        binding for binding in old_ids & new_ids if stored[binding] != current_digests[binding]
    )
    if removed or changed:
        if record is None:
            raise CheckpointCrossingRefused(
                "stable BAR checkpoint crossing is not additive and has no matching migration "
                f"record: removed={sorted(removed)[:5]} ({len(removed)}) "
                f"changed={sorted(changed)[:5]} ({len(changed)})"
            )
        if frozenset(record["removed_binding_ids"]) != removed or frozenset(
            record["changed_binding_ids"]
        ) != changed:
            raise CheckpointCrossingRefused(
                "stable BAR checkpoint migration record does not name exactly the removed and "
                "changed bindings"
            )
    return CheckpointCrossing(
        True,
        (old_ids & new_ids) - changed,
        added,
        removed | changed,
        record is not None,
    )
