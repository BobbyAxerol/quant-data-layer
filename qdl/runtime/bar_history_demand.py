"""Demanded BAR history depth per binding, from the KN gateway bundle (KN-4 D47-3).

Purpose: the BAR edge fills venue history only as deep as a consumer asks,
with the same rule the projector uses for its retained caps
(``rust/qdl-projector/src/products.rs`` ``retained_caps``, D15 as amended):
per BAR product, the largest ``max_warmup_rows`` of a manifest that requires
it (instrument, BAR, interval); 0 without demand. The retained cap adds the
2,064-row headroom for retention only - the headroom is never fetched.

Boundary: pure function over the bundle document (the single product-identity
owner, ``scripts/kn_gateway_bundle.py``); no I/O besides reading that file.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def demanded_history_rows(bundle: Mapping[str, Any]) -> dict[str, int]:
    """BAR binding id -> demanded rows (0 when no manifest requires it)."""

    requirements = [
        (requirement["instrument_uid"], requirement.get("interval"), int(manifest["quotas"]["max_warmup_rows"]))
        for manifest in bundle["manifests"]
        for requirement in manifest["requirements"]
        if requirement["feed"] == "BAR"
    ]
    demand: dict[str, int] = {}
    for binding in bundle["catalog"]["bindings"]:
        if binding["feed"] != "BAR":
            continue
        demand[binding["binding_id"]] = max(
            (rows for uid, interval, rows in requirements
             if uid == binding["instrument_uid"] and interval == binding.get("interval")),
            default=0,
        )
    return demand


def load_demanded_history_rows(path: str | Path) -> dict[str, int]:
    return demanded_history_rows(json.loads(Path(path).read_text(encoding="utf-8")))
