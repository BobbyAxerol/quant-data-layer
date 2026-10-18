"""Shared helper for the P18.4 stage-100 receipt-parity tests (A3, G4-A1, G4-A2, A4).

Purpose: the stored campaign receipts under ~/.local/state/qdl-v2 record absolute config paths inside the run
worktrees that produced them (data-layer-p184-final-run-<sha>). Those worktrees were removed after release
packaging, so a parity test that re-scores a receipt in place fails on a missing file, not on a scorer change.
`mirror()` builds a temporary receipt copy whose recorded config paths point at this repository's file of the same
name, only when that file's sha256 equals the one the receipt recorded; every other entry is a symlink to the
stored receipt. `as_recorded()` maps those substituted paths back in a serialized result, so a byte-identity assertion
against the stored scorecard stays byte-identity. The stored receipt is never modified and no assertion is relaxed.

Boundary: stdlib only, host-runnable, writes only below the caller's temporary directory.
"""
from __future__ import annotations

import json
from pathlib import Path

from scripts import p184_gate_campaign as gc

# Recorded config path -> recorded sha256 key in campaign.json.
PATH_KEYS = {"budget_path": "budget_sha256", "g4_addendum_path": "g4_addendum_sha256",
             "g4_amendment_path": "g4_amendment_sha256", "g4_amendment_2_path": "g4_amendment_2_sha256",
             "a3_path": "a3_sha256"}
# Mirrored receipt directory -> {substituted local path: path the receipt recorded}.
SUBSTITUTED: dict[Path, dict[str, str]] = {}


def mirror(d: Path, root: Path) -> Path:
    """Receipt copy whose recorded config paths point at this repository's files when the sha256 is the recorded one
    (the run worktrees of older receipts were removed after release packaging). Everything else is a symlink."""
    out = root / d.name
    out.mkdir()
    for item in d.iterdir():
        if item.name != "campaign.json":
            (out / item.name).symlink_to(item)
    campaign = json.loads((d / "campaign.json").read_text())
    substituted = SUBSTITUTED.setdefault(out, {})
    for key, sha_key in PATH_KEYS.items():
        path = campaign.get(key)
        if path and not Path(path).exists():
            local = gc.ROOT / "config/v2" / Path(path).name
            if gc.sha256_file(local) == campaign.get(sha_key):
                substituted[str(local)] = path
                campaign[key] = str(local)
    (out / "campaign.json").write_text(json.dumps(campaign))
    return out


def as_recorded(text: str, mirrored: Path) -> str:
    """Serialized scorer output of a mirrored receipt with every substituted config path put back to the recorded
    one (no other byte changes)."""
    for local, recorded in SUBSTITUTED.get(mirrored, {}).items():
        text = text.replace(local, recorded)
    return text
