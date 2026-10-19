"""Pre-extension inputs for the IDR-075 / sandbox inverse (BTC-USD-SWAP) preparation tests.

The committed catalog (revision 12), acquisition plan (revision 20) and Trading
System Paper manifest (revision 11) already carry the four BTC-USD-SWAP feeds
(IDR-127 brought the repository up to the deployed runtime). The preparation
scripts extend the scope *before* those feeds existed, so their tests need the
pre-extension documents. They are recovered from the committed ones by removing
exactly the inverse instrument/rows and restoring the prior revision/quotas, and
each recovered document is pinned by the canonical-JSON SHA-256 of the
historical repository file it reproduces (r11 catalog, r19 acquisition, r10
manifest), so a drift in either the committed file or this recovery fails.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
INVERSE_UID = "8482b54b-6d9d-5aad-abe8-e22447d183a1"  # OKX.SWAP.PERPETUAL.BTC-USD

# Canonical-JSON SHA-256 of the historical repository documents (git 3afd932):
# config/v2/stable-source-bindings.yaml catalog_revision 11 (also the
# base_catalog_sha256 of the 2026-09-30 inverse-r2 preparation),
# config/v2/stable-acquisition-bindings.yaml revision 19 (port-less OKX URLs),
# consumers/stable/trading-system-paper.yaml revision 10 (the manifest_sha256
# routed by stable-v2-release-routing revision 28).
CATALOG_R11_SHA256 = "8981cad849f0af9ba87eac884c6ac3df5a9c276f010e19ee73857251857b09df"
ACQUISITION_R19_SHA256 = "d66d72378138a07096efa5f15834aaacff9fb5aee81eef5b064c7f2e75cab50a"
PAPER_MANIFEST_R10_SHA256 = "5ca9aff1960883f59827e3a34ed709f4c30cf5db96c4e1253dd6f1116fb20cdf"
PAPER_R10_QUOTAS = {"max_streams": 50, "requests_per_minute": 1500}


def canonical_sha256(value) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()).hexdigest()


def _load(relative: str):
    return yaml.safe_load((ROOT / relative).read_text(encoding="utf-8"))


def _pinned(value, expected: str, name: str):
    actual = canonical_sha256(value)
    if actual != expected:
        raise AssertionError(f"recovered pre-inverse {name} differs from the historical file: {actual}")
    return value


def inverse_binding_ids() -> frozenset[str]:
    return frozenset(
        row["binding_id"] for row in _load("config/v2/stable-source-bindings.yaml")["bindings"]
        if row["instrument_uid"] == INVERSE_UID
    )


def pre_inverse_catalog() -> dict:
    catalog = _load("config/v2/stable-source-bindings.yaml")
    catalog["catalog_revision"] -= 1
    catalog["instruments"] = [r for r in catalog["instruments"] if r["instrument_uid"] != INVERSE_UID]
    catalog["bindings"] = [r for r in catalog["bindings"] if r["instrument_uid"] != INVERSE_UID]
    return _pinned(catalog, CATALOG_R11_SHA256, "catalog")


def pre_inverse_acquisition() -> dict:
    removed = inverse_binding_ids()
    acquisition = _load("config/v2/stable-acquisition-bindings.yaml")
    acquisition["revision"] -= 1
    acquisition["bindings"] = [r for r in acquisition["bindings"] if r["binding_id"] not in removed]
    return _pinned(acquisition, ACQUISITION_R19_SHA256, "acquisition")


def pre_inverse_paper_manifest() -> dict:
    manifest = _load("consumers/stable/trading-system-paper.yaml")
    manifest["metadata"]["revision"] -= 1
    manifest["spec"]["requirements"] = [
        r for r in manifest["spec"]["requirements"] if r["instrument_uid"] != INVERSE_UID
    ]
    manifest["spec"]["quotas"].update(PAPER_R10_QUOTAS)
    return _pinned(manifest, PAPER_MANIFEST_R10_SHA256, "Paper manifest")


def without_inverse_products(binding: dict, *, manifest_revision: int, reseal) -> dict:
    """Drop the inverse products/policies from a rendered binding and reseal it."""
    result = deepcopy(binding)
    kept = [p for p in result["products"] if p["instrument_uid"] != INVERSE_UID]
    ids = {p["requirement_id"] for p in kept}
    result["products"] = kept
    if "request_policies" in result:
        result["request_policies"] = [p for p in result["request_policies"] if p["requirement_id"] in ids]
    result["consumer_manifest_revision"] = manifest_revision
    result.pop("binding_sha256")
    result["binding_sha256"] = reseal(result)
    return result
