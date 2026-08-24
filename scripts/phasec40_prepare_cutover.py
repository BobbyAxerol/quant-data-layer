#!/usr/bin/env python3
"""Prepare C40 CAS packets and immutable terminal handoff evidence."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any, Mapping
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.control.authority_outbox import _checkpoint, _digest, _timestamp_ns
from qdl.control.cutover_packet import AuthorityCutoverPacket
from qdl.control.operator_env import require_control_admin_dsn


BOOTSTRAP_SCHEMA = "qdl.c40.authority-bootstrap-packet.v1"
LIVE_SCHEMA = "qdl.c40.live-handoff-evidence.v1"
TERMINAL_SCHEMA = "qdl.c40.terminal-handoff-packet.v1"
SHADOW_OWNER = "qdl-v2-rust-shadow"
CANARY_OWNER = "qdl-v2-rust-canary"
PRIMARY_OWNER = "qdl-v2-rust-primary"


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()


def _canonical_digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    return value


def _uuid(name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def _iso_from_ns(value: int) -> str:
    seconds, nanoseconds = divmod(int(value), 1_000_000_000)
    result = datetime.fromtimestamp(seconds, timezone.utc).replace(
        microsecond=nanoseconds // 1_000
    )
    return result.isoformat()


def _cutover_base(
    bootstrap: Mapping[str, Any], *, stage: str, actor: str, issued_at_ns: int
) -> dict[str, Any]:
    expires_at_ns = issued_at_ns + 1_800_000_000_000
    rollback_digests = {
        item["rollback_manifest_digest"] for item in bootstrap["slices"]
    }
    if len(rollback_digests) != 1:
        raise ValueError("bootstrap rollback manifest digests differ")
    return {
        "schema": "qdl.c3.authority-cutover-packet.v1",
        "packet_id": _uuid(
            f"qdl-c40-cutover:{bootstrap['candidate_digest']}:{stage}:{issued_at_ns}"
        ),
        "stage": stage,
        "issued_at_ns": issued_at_ns,
        "expires_at_ns": expires_at_ns,
        "actor": actor,
        "change_ticket": "QDL-C40-GLOBAL-RUST-AUTHORITY",
        "candidate_digest": bootstrap["candidate_digest"],
        "artifact_image_digest": bootstrap["candidate"]["rust_image_digest"],
        "contract_digest": bootstrap["candidate"]["contract_digest"],
        "partition_plan_digest": bootstrap["candidate"]["partition_plan_digest"],
        "route_manifest_digest": next(iter(rollback_digests)),
        "consumer_route": {
            "consumer_id": "trading-system",
            "expected_route": "V2_PRIMARY",
            "new_route": "V2_PRIMARY",
            "rollback_route": "V1",
            "rollback_command": [
                "scripts/qdl_route.py", "--route", "V1",
            ],
        },
        "evidence": {
            "provider_provenance": "REAL",
            "semantic_mismatches": 0,
            "open_gaps": 0,
            "duplicate_external_effects": 0,
            "consumer_errors": 0,
        },
        "slices": [],
    }


def prepare_stage(
    bootstrap: Mapping[str, Any], *, stage: str, actor: str, issued_at_ns: int
) -> dict[str, Any]:
    if bootstrap.get("schema") != BOOTSTRAP_SCHEMA or len(bootstrap.get("slices", [])) != 12:
        raise ValueError("C40 bootstrap packet is invalid")
    if stage not in {"SHADOW_VALIDATE", "CANARY"} or not actor.strip():
        raise ValueError("C40 preparatory stage/actor is invalid")
    packet = _cutover_base(
        bootstrap, stage=stage, actor=actor, issued_at_ns=issued_at_ns
    )
    bundle = bootstrap["prerequisite_bundle"]
    for item in sorted(bootstrap["slices"], key=lambda value: value["slice_id"]):
        if stage == "SHADOW_VALIDATE":
            expected_state, expected_revision = "RUST_SHADOW", 1
            new_state, new_owner, new_lease = "VALIDATING", SHADOW_OWNER, 1
            terminal, prerequisite, hold = None, None, None
        else:
            expected_state, expected_revision = "VALIDATING", 2
            new_state, new_owner, new_lease = "RUST_CANARY", CANARY_OWNER, 2
            terminal = 0
            prerequisite = bundle["bundle_id"]
            hold = bundle["expires_at"]
        packet["slices"].append({
            "transition_id": _uuid(
                f"qdl-c40-transition:{bootstrap['candidate_digest']}:"
                f"{stage}:{item['slice_id']}"
            ),
            "handoff_id": None,
            "slice_id": item["slice_id"],
            "expected_state": expected_state,
            "expected_revision": expected_revision,
            "expected_owner_id": SHADOW_OWNER,
            "expected_lease_epoch": 1,
            "partition_plan_epoch": item["partition_plan_epoch"],
            "new_state": new_state,
            "new_owner_id": new_owner,
            "new_lease_epoch": new_lease,
            "terminal_watermark": terminal,
            "prerequisite_bundle_id": prerequisite,
            "hold_until": hold,
            "reason": f"C40 exact crypto authority {stage.lower()}",
        })
    AuthorityCutoverPacket.parse(packet, now_ns=issued_at_ns)
    return packet


def _handoff_public(raw: Mapping[str, Any], checkpoint: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema": "qdl.accepted-authority-handoff.v1",
        "handoff_id": raw["handoff_id"],
        "direction": raw["direction"],
        "checkpoint_digest": _digest(checkpoint),
        "slice_id": raw["slice_id"],
        "old_owner_id": raw["old_owner_id"],
        "new_owner_id": raw["new_owner_id"],
        "expected_state": raw["expected_state"],
        "new_state": raw["new_state"],
        "expected_authority_revision": raw["expected_authority_revision"],
        "new_authority_revision": raw["new_authority_revision"],
        "expected_lease_epoch": raw["expected_lease_epoch"],
        "new_lease_epoch": raw["new_lease_epoch"],
        "partition_plan_epoch": raw["partition_plan_epoch"],
        "terminal_watermark": raw["terminal_watermark"],
        "first_new_watermark": raw["first_new_watermark"],
        "overlap_start_watermark": raw["overlap_start_watermark"],
        "overlap_end_watermark": raw["overlap_end_watermark"],
        "old_event_count": raw["old_event_count"],
        "new_event_count": raw["new_event_count"],
        "semantic_mismatches": raw["semantic_mismatches"],
        "open_gaps": raw["open_gaps"],
        "candidate_digest": raw["candidate_digest"],
        "prerequisite_bundle_id": raw["prerequisite_bundle_id"],
        "approved_by": raw["approved_by"],
        "approved_at_ns": raw["approved_at_ns"],
        "expires_at_ns": raw["expires_at_ns"],
    }


def prepare_primary(
    bootstrap: Mapping[str, Any],
    live: Mapping[str, Any],
    *,
    actor: str,
    issued_at_ns: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if (
        bootstrap.get("schema") != BOOTSTRAP_SCHEMA
        or live.get("schema") != LIVE_SCHEMA
        or live.get("provider_provenance") != "REAL"
        or int(live.get("production_mutations", -1)) != 0
        or len(live.get("slices", [])) != 12
        or live.get("scope_digest")
        != bootstrap["candidate"]["promotion_scope_digest"]
    ):
        raise ValueError("C40 live handoff evidence differs from bootstrap")
    packet = _cutover_base(
        bootstrap, stage="PRIMARY", actor=actor, issued_at_ns=issued_at_ns
    )
    bundle = bootstrap["prerequisite_bundle"]
    bundle_expires_ns = int(
        datetime.fromisoformat(bundle["expires_at"]).timestamp() * 1_000_000_000
    )
    handoff_expires_ns = min(
        issued_at_ns + 1_800_000_000_000, bundle_expires_ns
    )
    if handoff_expires_ns <= issued_at_ns:
        raise ValueError("C40 prerequisite/handoff window has expired")
    bootstrap_by_slice = {item["slice_id"]: item for item in bootstrap["slices"]}
    terminal_rows = []
    handoff_rows = []
    for value in sorted(live["slices"], key=lambda item: item["slice_id"]):
        slice_id = value["slice_id"]
        bootstrap_slice = bootstrap_by_slice.get(slice_id)
        checkpoint = value["checkpoint"]
        if (
            bootstrap_slice is None
            or value.get("semantic_mismatches") != 0
            or value.get("open_gaps") != 0
            or value.get("old_event_count") != value.get("new_event_count")
            or int(value.get("old_event_count", 0)) <= 0
            or checkpoint.get("owner_id") != CANARY_OWNER
            or int(checkpoint.get("authority_revision", 0)) != 3
            or int(checkpoint.get("lease_epoch", 0)) != 2
            or checkpoint.get("candidate_digest") != bootstrap["candidate_digest"]
        ):
            raise ValueError(f"C40 live slice evidence is invalid: {slice_id}")
        watermark = int(checkpoint["source_watermark"])
        committed_at = _iso_from_ns(int(checkpoint["committed_at_ns"]))
        checkpoint_id = _uuid(
            f"qdl-c40-terminal:{bootstrap['candidate_digest']}:{slice_id}:{watermark}"
        )
        checkpoint_row = {
            "checkpoint_id": checkpoint_id,
            "slice_id": slice_id,
            "owner_id": CANARY_OWNER,
            "authority_revision": 3,
            "lease_epoch": 2,
            "partition_plan_epoch": bootstrap_slice["partition_plan_epoch"],
            "source_session_id": value["source_session_id"],
            "connection_generation": value["connection_generation"],
            "terminal_watermark": watermark,
            "terminal_event_id": checkpoint["source_event_id"],
            "terminal_payload_sha256": checkpoint["output_payload_sha256"],
            "candidate_digest": bootstrap["candidate_digest"],
            "committed_at": committed_at,
        }
        checkpoint_public = _checkpoint({
            **checkpoint_row,
            "committed_at": datetime.fromisoformat(committed_at),
        })
        if checkpoint_public is None:
            raise AssertionError("terminal checkpoint serialization failed")
        handoff_id = _uuid(
            f"qdl-c40-handoff:{bootstrap['candidate_digest']}:{slice_id}:{watermark}"
        )
        handoff_row = {
            "handoff_id": handoff_id,
            "checkpoint_id": checkpoint_id,
            "direction": "PYTHON_TO_RUST",
            "slice_id": slice_id,
            "old_owner_id": CANARY_OWNER,
            "new_owner_id": PRIMARY_OWNER,
            "expected_state": "RUST_CANARY",
            "new_state": "RUST_PRIMARY",
            "expected_authority_revision": 3,
            "new_authority_revision": 4,
            "expected_lease_epoch": 2,
            "new_lease_epoch": 3,
            "partition_plan_epoch": bootstrap_slice["partition_plan_epoch"],
            "terminal_watermark": watermark,
            "first_new_watermark": watermark + 1,
            "overlap_start_watermark": value["overlap_start_watermark"],
            "overlap_end_watermark": watermark,
            "old_event_count": value["old_event_count"],
            "new_event_count": value["new_event_count"],
            "semantic_mismatches": 0,
            "open_gaps": 0,
            "candidate_digest": bootstrap["candidate_digest"],
            "prerequisite_bundle_id": bundle["bundle_id"],
            "approved_by": actor,
            "approved_at": _iso_from_ns(issued_at_ns),
            "expires_at": _iso_from_ns(handoff_expires_ns),
            "approved_at_ns": _timestamp_ns(_iso_from_ns(issued_at_ns)),
            "expires_at_ns": _timestamp_ns(_iso_from_ns(handoff_expires_ns)),
        }
        handoff_public = _handoff_public(handoff_row, checkpoint_public)
        handoff_row["handoff_sha256"] = _digest(handoff_public)
        terminal_rows.append(checkpoint_row)
        handoff_rows.append(handoff_row)
        packet["slices"].append({
            "transition_id": _uuid(
                f"qdl-c40-transition:{bootstrap['candidate_digest']}:PRIMARY:{slice_id}:{watermark}"
            ),
            "handoff_id": handoff_id,
            "slice_id": slice_id,
            "expected_state": "RUST_CANARY",
            "expected_revision": 3,
            "expected_owner_id": CANARY_OWNER,
            "expected_lease_epoch": 2,
            "partition_plan_epoch": bootstrap_slice["partition_plan_epoch"],
            "new_state": "RUST_PRIMARY",
            "new_owner_id": PRIMARY_OWNER,
            "new_lease_epoch": 3,
            "terminal_watermark": watermark,
            "prerequisite_bundle_id": bundle["bundle_id"],
            "hold_until": _iso_from_ns(handoff_expires_ns),
            "reason": "C40 exact crypto authority primary handoff",
        })
    AuthorityCutoverPacket.parse(packet, now_ns=issued_at_ns)
    terminal = {
        "schema": TERMINAL_SCHEMA,
        "packet_id": _uuid(
            f"qdl-c40-terminal-packet:{bootstrap['candidate_digest']}:{issued_at_ns}"
        ),
        "issued_at_ns": issued_at_ns,
        "expires_at_ns": handoff_expires_ns,
        "candidate_digest": bootstrap["candidate_digest"],
        "live_evidence_sha256": _canonical_digest(live),
        "checkpoints": terminal_rows,
        "handoffs": handoff_rows,
    }
    return terminal, packet


def validate_terminal(packet: Mapping[str, Any], *, now_ns: int | None = None) -> None:
    if set(packet) != {
        "schema", "packet_id", "issued_at_ns", "expires_at_ns",
        "candidate_digest", "live_evidence_sha256", "checkpoints", "handoffs",
    } or packet.get("schema") != TERMINAL_SCHEMA:
        raise ValueError("terminal handoff packet schema/fields are invalid")
    current = time.time_ns() if now_ns is None else now_ns
    if not int(packet["issued_at_ns"]) <= current < int(packet["expires_at_ns"]):
        raise ValueError("terminal handoff packet approval window is inactive")
    if len(packet["checkpoints"]) != 12 or len(packet["handoffs"]) != 12:
        raise ValueError("terminal handoff packet requires twelve exact rows")
    checkpoints = {item["checkpoint_id"]: item for item in packet["checkpoints"]}
    if len(checkpoints) != 12:
        raise ValueError("terminal checkpoint identities are not unique")
    for handoff in packet["handoffs"]:
        raw_checkpoint = checkpoints.get(handoff["checkpoint_id"])
        if raw_checkpoint is None:
            raise ValueError("handoff checkpoint is absent")
        checkpoint = _checkpoint({
            **raw_checkpoint,
            "committed_at": datetime.fromisoformat(raw_checkpoint["committed_at"]),
        })
        if checkpoint is None:
            raise ValueError("handoff checkpoint cannot be serialized")
        public = _handoff_public(handoff, checkpoint)
        if handoff["handoff_sha256"] != _digest(public):
            raise ValueError("handoff digest differs from authority serializer")


async def apply_terminal(packet: Mapping[str, Any], dsn: str) -> dict[str, int]:
    try:
        import asyncpg
    except ImportError as error:
        raise RuntimeError("terminal handoff apply requires asyncpg") from error
    connection = await asyncpg.connect(
        dsn=dsn, command_timeout=15,
        server_settings={"application_name": "qdl-c40-terminal-handoff"},
    )
    try:
        async with connection.transaction():
            mutations = 0
            for item in packet["checkpoints"]:
                checkpoint_status = await connection.execute(
                    "INSERT INTO qdl_terminal_owner_checkpoints ("
                    "checkpoint_id,slice_id,owner_id,authority_revision,lease_epoch,"
                    "partition_plan_epoch,source_session_id,connection_generation,"
                    "terminal_watermark,terminal_event_id,terminal_payload_sha256,"
                    "candidate_digest,committed_at) VALUES ("
                    "$1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::timestamptz) "
                    "ON CONFLICT (checkpoint_id) DO NOTHING",
                    item["checkpoint_id"], item["slice_id"], item["owner_id"],
                    item["authority_revision"], item["lease_epoch"],
                    item["partition_plan_epoch"], item["source_session_id"],
                    item["connection_generation"], item["terminal_watermark"],
                    item["terminal_event_id"], item["terminal_payload_sha256"],
                    item["candidate_digest"],
                    datetime.fromisoformat(item["committed_at"]),
                )
                mutations += int(checkpoint_status.rsplit(" ", 1)[-1])
            for item in packet["handoffs"]:
                handoff_status = await connection.execute(
                    "INSERT INTO qdl_authority_handoffs (handoff_id,checkpoint_id,"
                    "direction,slice_id,old_owner_id,new_owner_id,expected_state,new_state,"
                    "expected_authority_revision,new_authority_revision,expected_lease_epoch,"
                    "new_lease_epoch,partition_plan_epoch,terminal_watermark,first_new_watermark,"
                    "overlap_start_watermark,overlap_end_watermark,old_event_count,new_event_count,"
                    "semantic_mismatches,open_gaps,candidate_digest,prerequisite_bundle_id,"
                    "handoff_sha256,approved_by,approved_at,expires_at) VALUES ("
                    "$1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,"
                    "$17,$18,$19,$20,$21,$22,$23::uuid,$24,$25,$26::timestamptz,$27::timestamptz) "
                    "ON CONFLICT (handoff_id) DO NOTHING",
                    item["handoff_id"], item["checkpoint_id"], item["direction"],
                    item["slice_id"], item["old_owner_id"], item["new_owner_id"],
                    item["expected_state"], item["new_state"],
                    item["expected_authority_revision"], item["new_authority_revision"],
                    item["expected_lease_epoch"], item["new_lease_epoch"],
                    item["partition_plan_epoch"], item["terminal_watermark"],
                    item["first_new_watermark"], item["overlap_start_watermark"],
                    item["overlap_end_watermark"], item["old_event_count"],
                    item["new_event_count"], item["semantic_mismatches"], item["open_gaps"],
                    item["candidate_digest"], item["prerequisite_bundle_id"],
                    item["handoff_sha256"], item["approved_by"],
                    datetime.fromisoformat(item["approved_at"]),
                    datetime.fromisoformat(item["expires_at"]),
                )
                mutations += int(handoff_status.rsplit(" ", 1)[-1])
            checkpoint_fields = (
                "checkpoint_id", "slice_id", "owner_id", "authority_revision",
                "lease_epoch", "partition_plan_epoch", "source_session_id",
                "connection_generation", "terminal_watermark", "terminal_event_id",
                "terminal_payload_sha256", "candidate_digest", "committed_at",
            )
            checkpoint_rows = await connection.fetch(
                "SELECT checkpoint_id::text,slice_id,owner_id,authority_revision,lease_epoch,"
                "partition_plan_epoch,source_session_id,connection_generation,"
                "terminal_watermark,terminal_event_id,terminal_payload_sha256,"
                "candidate_digest,committed_at FROM qdl_terminal_owner_checkpoints "
                "WHERE checkpoint_id=ANY($1::uuid[]) ORDER BY checkpoint_id",
                [item["checkpoint_id"] for item in packet["checkpoints"]],
            )
            expected_checkpoints = []
            for item in sorted(packet["checkpoints"], key=lambda value: value["checkpoint_id"]):
                expected = {field: item[field] for field in checkpoint_fields}
                expected["committed_at"] = datetime.fromisoformat(item["committed_at"])
                expected_checkpoints.append(expected)
            actual_checkpoints = [dict(row) for row in checkpoint_rows]

            handoff_fields = (
                "handoff_id", "checkpoint_id", "direction", "slice_id",
                "old_owner_id", "new_owner_id", "expected_state", "new_state",
                "expected_authority_revision", "new_authority_revision",
                "expected_lease_epoch", "new_lease_epoch", "partition_plan_epoch",
                "terminal_watermark", "first_new_watermark", "overlap_start_watermark",
                "overlap_end_watermark", "old_event_count", "new_event_count",
                "semantic_mismatches", "open_gaps", "candidate_digest",
                "prerequisite_bundle_id", "handoff_sha256", "approved_by",
                "approved_at", "expires_at",
            )
            handoff_rows = await connection.fetch(
                "SELECT handoff_id::text,checkpoint_id::text,direction,slice_id,old_owner_id,"
                "new_owner_id,expected_state,new_state,expected_authority_revision,"
                "new_authority_revision,expected_lease_epoch,new_lease_epoch,"
                "partition_plan_epoch,terminal_watermark,first_new_watermark,"
                "overlap_start_watermark,overlap_end_watermark,old_event_count,new_event_count,"
                "semantic_mismatches,open_gaps,candidate_digest,prerequisite_bundle_id::text,"
                "handoff_sha256,approved_by,approved_at,expires_at FROM qdl_authority_handoffs "
                "WHERE handoff_id=ANY($1::uuid[]) ORDER BY handoff_id",
                [item["handoff_id"] for item in packet["handoffs"]],
            )
            expected_handoffs = []
            for item in sorted(packet["handoffs"], key=lambda value: value["handoff_id"]):
                expected = {field: item[field] for field in handoff_fields}
                expected["approved_at"] = datetime.fromisoformat(item["approved_at"])
                expected["expires_at"] = datetime.fromisoformat(item["expires_at"])
                expected_handoffs.append(expected)
            actual_handoffs = [dict(row) for row in handoff_rows]
            if actual_checkpoints != expected_checkpoints or actual_handoffs != expected_handoffs:
                raise RuntimeError("terminal checkpoint/handoff DB rows conflict with packet")
    finally:
        await connection.close()
    return {"checkpoints": 12, "handoffs": 12, "production_mutations": mutations}


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    stage = sub.add_parser("stage")
    stage.add_argument("--bootstrap-packet", type=Path, required=True)
    stage.add_argument("--stage", choices=("SHADOW_VALIDATE", "CANARY"), required=True)
    stage.add_argument("--actor", required=True)
    stage.add_argument("--output", type=Path, required=True)
    primary = sub.add_parser("primary")
    primary.add_argument("--bootstrap-packet", type=Path, required=True)
    primary.add_argument("--live-evidence", type=Path, required=True)
    primary.add_argument("--actor", required=True)
    primary.add_argument("--output-dir", type=Path, required=True)
    apply = sub.add_parser("apply-evidence")
    apply.add_argument("--packet", type=Path, required=True)
    apply.add_argument("--apply", action="store_true")
    apply.add_argument("--confirm")
    args = parser.parse_args()
    now_ns = time.time_ns()
    if args.command == "stage":
        packet = prepare_stage(
            _load(args.bootstrap_packet), stage=args.stage,
            actor=args.actor, issued_at_ns=now_ns,
        )
        args.output.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n")
        parsed = AuthorityCutoverPacket.parse(packet, now_ns=now_ns)
        print(json.dumps(parsed.plan(), sort_keys=True))
        return 0
    if args.command == "primary":
        terminal, packet = prepare_primary(
            _load(args.bootstrap_packet), _load(args.live_evidence),
            actor=args.actor, issued_at_ns=now_ns,
        )
        args.output_dir.mkdir(parents=True, exist_ok=False)
        terminal_path = args.output_dir / "terminal-handoff.json"
        cutover_path = args.output_dir / "primary-cutover.json"
        terminal_path.write_text(json.dumps(terminal, indent=2, sort_keys=True) + "\n")
        cutover_path.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n")
        print(json.dumps({
            "status": "PASS", "terminal_packet": str(terminal_path),
            "cutover_packet": str(cutover_path),
            "terminal_confirmation_token":
                f"APPLY_C40_HANDOFF_{_canonical_digest(terminal)[:16]}",
            "cutover_confirmation_token":
                AuthorityCutoverPacket.parse(packet, now_ns=now_ns).confirmation_token,
            "production_mutations": 0,
        }, sort_keys=True))
        return 0
    packet = _load(args.packet)
    validate_terminal(packet)
    digest = _canonical_digest(packet)
    token = f"APPLY_C40_HANDOFF_{digest[:16]}"
    if not args.apply:
        print(json.dumps({
            "status": "PASS", "confirmation_token": token,
            "checkpoints": 12, "handoffs": 12, "production_mutations": 0,
        }, sort_keys=True))
        return 0
    if args.confirm != token:
        raise RuntimeError("terminal handoff confirmation token differs from packet")
    result = asyncio.run(apply_terminal(packet, require_control_admin_dsn()))
    print(json.dumps({
        "status": "PASS", "confirmation_token": token, **result,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
