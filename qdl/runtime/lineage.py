"""Per-binding contract lineage across declarations: pin it, and gate that retained bindings keep it.

DL v2.3.0 slice 2d. A consumer (the TS writer) keys execution ordering authority and resume markers on a record's
contract lineage: schema_digest, contract_version, authority_revision, config_revision, source_policy_revision and
instrument_catalog_revision. Until v2.3.0 the last two were global (the ingestor config revision, the catalogue
revision), so the additive stage A declaration (catalog 12 -> 13, acquisition 20 -> 21, 40 new bindings, none
changed) moved the lineage of every untouched binding and the writer refused all BOOK slices
(EXECUTION_ORDERING_AUTHORITY_MISMATCH) and native resume (RESUME_GENERATION_MISMATCH).

Now each binding declares the revision at which it last changed:

* catalogue binding ``lineage_revision`` -> records' ``instrument_catalog_revision`` (and cursor v3 catalog claim);
* acquisition binding ``lineage_config_revision`` -> records' ``config_revision`` (ingestor/bar edge envelopes).

``pin_lineage`` is the renderer rule: a binding whose catalogue and acquisition rows are unchanged keeps its previous
lineage; an added or changed binding gets the new revision. ``retained_lineage_problems`` is the gate: the lineage
of every binding retained unchanged must be unchanged, unless an explicit migration record lists it.
schema_digest (contract line, scripts/schema_line.py), contract_version, authority_revision and
source_policy_revision are not per binding and do not move in a declaration; the gate refuses if they do.

Boundary: pure functions over loaded or raw declaration documents; no I/O.
"""

from __future__ import annotations

import copy
from typing import Any, Iterable, Mapping

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan

_CATALOG_KEY = "lineage_revision"
_PLAN_KEY = "lineage_config_revision"


def binding_lineage(
    catalog: StableSourceCatalog, plan: StableAcquisitionPlan, binding_id: str,
) -> dict[str, int]:
    """The declaration-controlled lineage a binding's records carry."""

    source = next(item for item in catalog.bindings if item.binding_id == binding_id)
    return {
        "instrument_catalog_revision": catalog.lineage_revision_for(source),
        "config_revision": plan.lineage_config_revision_for(binding_id),
        "source_policy_revision": catalog.source_policy_revision,
        "authority_revision": catalog.authority_revision,
    }


def _rows(document: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(item["binding_id"]): item for item in document["bindings"]}


def _without(row: Mapping[str, Any], key: str) -> dict[str, Any]:
    return {name: value for name, value in row.items() if name != key}


def retained_unchanged_ids(
    old_catalog_doc: Mapping[str, Any], old_plan_doc: Mapping[str, Any],
    new_catalog_doc: Mapping[str, Any], new_plan_doc: Mapping[str, Any],
) -> frozenset[str]:
    """Bindings present before and after whose catalogue and acquisition rows are equal (lineage keys aside)."""

    old_c, old_p = _rows(old_catalog_doc), _rows(old_plan_doc)
    new_c, new_p = _rows(new_catalog_doc), _rows(new_plan_doc)
    return frozenset(
        binding for binding in set(old_c) & set(new_c) & set(old_p) & set(new_p)
        if _without(old_c[binding], _CATALOG_KEY) == _without(new_c[binding], _CATALOG_KEY)
        and _without(old_p[binding], _PLAN_KEY) == _without(new_p[binding], _PLAN_KEY)
    )


def live_config_lineage(
    catalog: StableSourceCatalog, ingestor_configs: Iterable[Mapping[str, Any]],
) -> dict[str, int]:
    """binding_id -> the config_revision its LIVE ingestor envelopes carry.

    Each rendered ingestor config stamps a binding's raw envelopes with the binding row's own
    ``config_revision`` or, absent, the file's ``config_revision`` (``qdl-native-raw-ingestor`` RawBinding).
    The plan document's revision is NOT that value (Binance ran 19 while the plan said 21; review H2), so
    config lineage must be seeded from here. Rows map to bindings through ``subscription_id`` = source_id.
    """

    by_source = {item.source_id: item.binding_id for item in catalog.bindings}
    result: dict[str, int] = {}
    for config in ingestor_configs:
        default = int(config["config_revision"])
        for row in config["bindings"]:
            binding = by_source.get(str(row["subscription_id"]))
            if binding is None:
                continue
            value = int(row.get("config_revision", default))
            if result.setdefault(binding, value) != value:
                raise ValueError(f"binding {binding} carries two live config revisions")
    return result


def live_config_lineage_problems(
    plan: StableAcquisitionPlan, live: Mapping[str, int],
) -> list[str]:
    """Gate: every binding a live ingestor stamps keeps that exact config lineage in the plan."""

    moved = sorted(
        binding for binding, value in live.items()
        if any(item.binding_id == binding for item in plan.bindings)
        and plan.lineage_config_revision_for(binding) != value
    )
    if not moved:
        return []
    return [f"{len(moved)} bindings' plan config lineage differs from the live ingestor stamp"] + [
        f"{binding}: plan {plan.lineage_config_revision_for(binding)} live {live[binding]}" for binding in moved[:5]
    ]


def pin_lineage(
    old_catalog_doc: Mapping[str, Any], old_plan_doc: Mapping[str, Any],
    new_catalog_doc: Mapping[str, Any], new_plan_doc: Mapping[str, Any],
    *,
    live_config: Mapping[str, int] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Return the new documents with every binding's lineage pinned, and a report.

    Retained-unchanged bindings keep the lineage they had (their pinned value, or the old document's revision when
    the old document did not pin); every other binding is pinned at the new revision.
    """

    retained = retained_unchanged_ids(old_catalog_doc, old_plan_doc, new_catalog_doc, new_plan_doc)
    old_c, old_p = _rows(old_catalog_doc), _rows(old_plan_doc)
    catalog = copy.deepcopy(dict(new_catalog_doc))
    plan = copy.deepcopy(dict(new_plan_doc))
    new_catalog_revision = int(catalog["catalog_revision"])
    new_plan_revision = int(plan["revision"])
    kept = moved = 0
    for row in catalog["bindings"]:
        binding = str(row["binding_id"])
        if binding in retained:
            row[_CATALOG_KEY] = int(old_c[binding].get(_CATALOG_KEY) or old_catalog_doc["catalog_revision"])
            kept += 1
        else:
            row[_CATALOG_KEY] = new_catalog_revision
            moved += 1
    for row in plan["bindings"]:
        binding = str(row["binding_id"])
        if binding in retained:
            # Live stamp first (review H2): the value the binding's records actually carry.
            live_value = None if live_config is None else live_config.get(binding)
            row[_PLAN_KEY] = int(live_value if live_value is not None
                                 else old_p[binding].get(_PLAN_KEY) or old_plan_doc["revision"])
        else:
            row[_PLAN_KEY] = new_plan_revision
    return catalog, plan, {"retained": kept, "new_or_changed": moved}


def retained_lineage_problems(
    old: tuple[StableSourceCatalog, StableAcquisitionPlan],
    new: tuple[StableSourceCatalog, StableAcquisitionPlan],
    retained: Iterable[str],
    *,
    migration_binding_ids: Iterable[str] = (),
) -> list[str]:
    """Gate: every retained-unchanged binding keeps its lineage (except those a migration record names)."""

    allowed = frozenset(migration_binding_ids)
    problems: list[str] = []
    moved: list[str] = []
    for binding in sorted(set(retained) - allowed):
        before = binding_lineage(*old, binding)
        after = binding_lineage(*new, binding)
        if before != after:
            moved.append(binding)
            if len(problems) < 5:
                changed = {key: (before[key], after[key]) for key in before if before[key] != after[key]}
                problems.append(f"{binding} lineage moved {changed}")
    if moved:
        problems.insert(0, f"{len(moved)} retained bindings would change their contract lineage")
    return problems
