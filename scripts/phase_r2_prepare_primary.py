#!/usr/bin/env python3
"""Prepare a dynamic R2 terminal-handoff and PRIMARY C3 packet.

This command is read-only. It consumes real canary evidence, binds each terminal
watermark to the current authority row, and writes two review artifacts. The
existing append-only evidence apply and C3 CAS remain separate explicit steps.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Mapping
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.control.authority_rollover import CandidateRolloverPacket
from qdl.control.cutover_packet import AuthorityCutoverPacket
from scripts.phasec40_prepare_cutover import (
    TERMINAL_SCHEMA,
    _canonical_digest,
    _checkpoint,
    _digest as _authority_digest,
    _handoff_public,
    validate_terminal,
)


_CURRENT_SQL = """
SELECT slice_id,state,authority_revision,owner_id,lease_epoch,partition_plan_epoch,
       candidate_digest,artifact_image_digest,contract_digest,partition_plan_digest,
       rollback_manifest_digest,partition_id
FROM qdl_authority_slices
WHERE slice_id = ANY($1::text[])
ORDER BY slice_id
"""


def _required_dsn() -> str:
    value = os.environ.get("QDL_CONTROL_ADMIN_DSN", "").strip()
    if not value:
        raise RuntimeError("QDL_CONTROL_ADMIN_DSN is required")
    return value


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    return value


def _iso_from_ns(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, timezone.utc).isoformat()


async def _rows(dsn: str, slice_ids: list[str]) -> list[dict[str, Any]]:
    try:
        import asyncpg
    except ImportError as error:
        raise RuntimeError("R2 primary packet preparation requires asyncpg") from error
    connection = await asyncpg.connect(
        dsn=dsn,
        command_timeout=15,
        server_settings={"application_name": "qdl-r2-primary-prepare"},
    )
    try:
        return [dict(row) for row in await connection.fetch(_CURRENT_SQL, slice_ids)]
    finally:
        await connection.close()


def prepare_primary_packets(
    *,
    rollover: CandidateRolloverPacket,
    live: Mapping[str, Any],
    current_rows: list[Mapping[str, Any]],
    actor: str,
    change_ticket: str,
    primary_owner_id: str = "qdl-v2-rust-primary",
    issued_at_ns: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    now = time.time_ns() if issued_at_ns is None else int(issued_at_ns)
    if not actor.strip() or not change_ticket.strip() or not primary_owner_id.strip():
        raise ValueError("R2 primary actor/ticket/owner is invalid")
    if (
        live.get("schema") != "qdl.c40.live-handoff-evidence.v1"
        or live.get("provider_provenance") != "REAL"
        or int(live.get("production_mutations", -1)) != 0
        or live.get("scope_digest") != rollover.candidate["promotion_scope_digest"]
        or not isinstance(live.get("slices"), list)
        or len(live["slices"]) != 12
    ):
        raise ValueError("R2 live canary evidence is incomplete or not real")
    rows = {str(item["slice_id"]): dict(item) for item in current_rows}
    expected = {item.slice_id: item for item in rollover.rollovers}
    evidence = {str(item.get("slice_id", "")): item for item in live["slices"]}
    if set(rows) != set(expected) or set(rows) != set(evidence) or len(rows) != 12:
        raise ValueError("R2 current authority, rollover and live evidence scopes differ")
    rollback_digests = {item.new_provenance["rollback_manifest_digest"] for item in rollover.rollovers}
    if len(rollback_digests) != 1:
        raise ValueError("R2 rollover rollback manifests are not uniform")
    bundle = rollover.prerequisite_bundle
    bundle_expires_ns = int(datetime.fromisoformat(str(bundle["expires_at"])).timestamp() * 1_000_000_000)
    handoff_expires_ns = min(now + 1_800_000_000_000, int(rollover.raw["expires_at_ns"]), bundle_expires_ns)
    if handoff_expires_ns <= now:
        raise ValueError("R2 prerequisite/handoff approval window is inactive")
    terminal_rows = []
    handoff_rows = []
    c3_slices = []
    for slice_id in sorted(rows):
        current = rows[slice_id]
        item = expected[slice_id]
        value = evidence[slice_id]
        checkpoint = value.get("checkpoint")
        if not isinstance(checkpoint, Mapping):
            raise ValueError(f"R2 canary checkpoint is absent: {slice_id}")
        if (
            current.get("state") != "RUST_CANARY"
            or current.get("candidate_digest") != rollover.candidate_digest
            or current.get("artifact_image_digest") != rollover.candidate["rust_image_digest"]
            or current.get("contract_digest") != rollover.candidate["contract_digest"]
            or current.get("partition_plan_digest") != rollover.candidate["partition_plan_digest"]
            or current.get("rollback_manifest_digest") != item.new_provenance["rollback_manifest_digest"]
            or int(current.get("partition_plan_epoch", 0)) != item.expected_partition_plan_epoch
            or current.get("partition_id") != item.expected_partition_id
            or checkpoint.get("owner_id") != current.get("owner_id")
            or int(checkpoint.get("authority_revision", 0)) != int(current.get("authority_revision", 0))
            or int(checkpoint.get("lease_epoch", 0)) != int(current.get("lease_epoch", 0))
            or checkpoint.get("shard_id") != item.expected_partition_id
            or value.get("binding_id") != item.expected_partition_id
            or int(value.get("semantic_mismatches", -1)) != 0
            or int(value.get("open_gaps", -1)) != 0
            or int(value.get("old_event_count", 0)) <= 0
            or int(value.get("old_event_count", 0)) != int(value.get("new_event_count", -1))
        ):
            raise ValueError(f"R2 canary authority/parity differs: {slice_id}")
        watermark = int(checkpoint.get("source_watermark", -1))
        if watermark < 0 or int(value.get("overlap_end_watermark", -1)) != watermark:
            raise ValueError(f"R2 terminal watermark is invalid: {slice_id}")
        committed_at_ns = int(checkpoint.get("committed_at_ns", 0))
        if committed_at_ns <= 0:
            raise ValueError(f"R2 canary checkpoint time is invalid: {slice_id}")
        checkpoint_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"qdl-r2-terminal:{rollover.candidate_digest}:{slice_id}:{current['authority_revision']}:{watermark}",
        ))
        terminal = {
            "checkpoint_id": checkpoint_id,
            "slice_id": slice_id,
            "owner_id": str(current["owner_id"]),
            "authority_revision": int(current["authority_revision"]),
            "lease_epoch": int(current["lease_epoch"]),
            "partition_plan_epoch": int(current["partition_plan_epoch"]),
            "source_session_id": str(value["source_session_id"]),
            "connection_generation": int(value["connection_generation"]),
            "terminal_watermark": watermark,
            "terminal_event_id": str(checkpoint["source_event_id"]),
            "terminal_payload_sha256": str(checkpoint["output_payload_sha256"]),
            "candidate_digest": rollover.candidate_digest,
            "committed_at": _iso_from_ns(committed_at_ns),
        }
        checkpoint_public = _checkpoint({
            **terminal,
            "committed_at": datetime.fromisoformat(terminal["committed_at"]),
        })
        if checkpoint_public is None:
            raise AssertionError("R2 terminal checkpoint serialization failed")
        handoff_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"qdl-r2-primary:{rollover.candidate_digest}:{slice_id}:{current['authority_revision']}:{watermark}",
        ))
        handoff = {
            "handoff_id": handoff_id,
            # Existing Phase 9.2 DB enum names this approved publication handoff
            # PYTHON_TO_RUST even though the bounded canary already runs Rust.
            "direction": "PYTHON_TO_RUST",
            "checkpoint_id": checkpoint_id,
            "slice_id": slice_id,
            "old_owner_id": str(current["owner_id"]),
            "new_owner_id": primary_owner_id,
            "expected_state": "RUST_CANARY",
            "new_state": "RUST_PRIMARY",
            "expected_authority_revision": int(current["authority_revision"]),
            "new_authority_revision": int(current["authority_revision"]) + 1,
            "expected_lease_epoch": int(current["lease_epoch"]),
            "new_lease_epoch": int(current["lease_epoch"]) + 1,
            "partition_plan_epoch": int(current["partition_plan_epoch"]),
            "terminal_watermark": watermark,
            "first_new_watermark": watermark + 1,
            "overlap_start_watermark": int(value["overlap_start_watermark"]),
            "overlap_end_watermark": watermark,
            "old_event_count": int(value["old_event_count"]),
            "new_event_count": int(value["new_event_count"]),
            "semantic_mismatches": 0,
            "open_gaps": 0,
            "candidate_digest": rollover.candidate_digest,
            "prerequisite_bundle_id": str(bundle["bundle_id"]),
            "approved_by": actor,
            "approved_at": _iso_from_ns(now),
            "expires_at": _iso_from_ns(handoff_expires_ns),
            "approved_at_ns": now,
            "expires_at_ns": handoff_expires_ns,
        }
        handoff_public = _handoff_public(handoff, checkpoint_public)
        handoff["handoff_sha256"] = _authority_digest(handoff_public)
        terminal_rows.append(terminal)
        handoff_rows.append(handoff)
        c3_slices.append({
            "transition_id": str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"qdl-r2-primary-transition:{rollover.candidate_digest}:{slice_id}:{watermark}",
            )),
            "handoff_id": handoff_id,
            "slice_id": slice_id,
            "expected_state": "RUST_CANARY",
            "expected_revision": int(current["authority_revision"]),
            "expected_owner_id": str(current["owner_id"]),
            "expected_lease_epoch": int(current["lease_epoch"]),
            "partition_plan_epoch": int(current["partition_plan_epoch"]),
            "new_state": "RUST_PRIMARY",
            "new_owner_id": primary_owner_id,
            "new_lease_epoch": int(current["lease_epoch"]) + 1,
            "terminal_watermark": watermark,
            "prerequisite_bundle_id": str(bundle["bundle_id"]),
            "hold_until": _iso_from_ns(handoff_expires_ns),
            "reason": f"R2 exact primary handoff {change_ticket}",
        })
    terminal_packet = {
        "schema": TERMINAL_SCHEMA,
        "packet_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"qdl-r2-terminal-packet:{rollover.candidate_digest}:{now}",
        )),
        "issued_at_ns": now,
        "expires_at_ns": handoff_expires_ns,
        "candidate_digest": rollover.candidate_digest,
        "live_evidence_sha256": _canonical_digest(dict(live)),
        "checkpoints": terminal_rows,
        "handoffs": handoff_rows,
    }
    validate_terminal(terminal_packet, now_ns=now)
    c3_packet = {
        "schema": "qdl.c3.authority-cutover-packet.v1",
        "packet_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"qdl-r2-primary-c3:{rollover.candidate_digest}:{','.join(item['transition_id'] for item in c3_slices)}",
        )),
        "stage": "PRIMARY",
        "issued_at_ns": now,
        "expires_at_ns": handoff_expires_ns,
        "actor": actor,
        "change_ticket": change_ticket,
        "candidate_digest": rollover.candidate_digest,
        "artifact_image_digest": rollover.candidate["rust_image_digest"],
        "contract_digest": rollover.candidate["contract_digest"],
        "partition_plan_digest": rollover.candidate["partition_plan_digest"],
        "route_manifest_digest": next(iter({item.new_provenance["rollback_manifest_digest"] for item in rollover.rollovers})),
        "consumer_route": {
            "consumer_id": "trading-system",
            "expected_route": "V1",
            "new_route": "V2_PRIMARY",
            "rollback_route": "V1",
            "rollback_command": ["scripts/qdl_route.py", "--route", "V1"],
        },
        "evidence": {
            "provider_provenance": "REAL",
            "semantic_mismatches": 0,
            "open_gaps": 0,
            "duplicate_external_effects": 0,
            "consumer_errors": 0,
        },
        "slices": c3_slices,
    }
    AuthorityCutoverPacket.parse(c3_packet, now_ns=now)
    return terminal_packet, c3_packet


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rollover-packet", type=Path, required=True)
    parser.add_argument("--live-evidence", type=Path, required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--change-ticket", required=True)
    parser.add_argument("--primary-owner-id", default="qdl-v2-rust-primary")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    rollover = CandidateRolloverPacket.parse(_load_json(args.rollover_packet))
    rows = asyncio.run(_rows(_required_dsn(), sorted(item.slice_id for item in rollover.rollovers)))
    terminal, primary = prepare_primary_packets(
        rollover=rollover,
        live=_load_json(args.live_evidence),
        current_rows=rows,
        actor=args.actor,
        change_ticket=args.change_ticket,
        primary_owner_id=args.primary_owner_id,
    )
    args.output_dir.mkdir(parents=True, exist_ok=False)
    terminal_path = args.output_dir / "terminal-handoff.json"
    primary_path = args.output_dir / "primary-cutover.json"
    terminal_path.write_text(json.dumps(terminal, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    primary_path.write_text(json.dumps(primary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": "PASS",
        "terminal_packet": str(terminal_path.resolve()),
        "primary_packet": str(primary_path.resolve()),
        "terminal_confirmation_token": f"APPLY_C40_HANDOFF_{_canonical_digest(terminal)[:16]}",
        "primary_confirmation_token": AuthorityCutoverPacket.parse(primary).confirmation_token,
        "production_mutations": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
