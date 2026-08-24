#!/usr/bin/env python3
"""Prepare/apply one exact blocked-only R1 authority candidate rollover."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.control.authority_rollover import CandidateRolloverPacket, prepare_rollover_packet


_CURRENT_SQL = """
SELECT slice_id,state,authority_revision,owner_id,lease_epoch,partition_plan_epoch,
       candidate_digest,artifact_image_digest,environment,venue,market,product_type,
       feed,partition_id,schema_major,signature_identity,contract_digest,
       normalizer_version,adapter_version,config_revision,instrument_catalog_revision,
       source_policy_revision,partition_plan_digest
FROM qdl_authority_slices
WHERE slice_id = ANY($1::text[])
ORDER BY slice_id
"""
_BUNDLE_INSERT = """
INSERT INTO qdl_production_prerequisite_bundles (
    bundle_id,candidate_digest,policy_revision,decision,evidence,evidence_sha256,
    issued_by,issued_at,expires_at
) VALUES ($1::uuid,$2,$3,$4,$5::jsonb,$6,$7,$8::timestamptz,$9::timestamptz)
ON CONFLICT (bundle_id) DO NOTHING
"""
_ROLLOVER_SQL = """
SELECT * FROM qdl_rollover_authority_candidate(
    $1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::uuid,$13,$14
)
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


async def _connection(dsn: str):
    try:
        import asyncpg
    except ImportError as error:
        raise RuntimeError("R1 candidate rollover requires asyncpg") from error
    return await asyncpg.connect(
        dsn=dsn,
        command_timeout=20,
        server_settings={"application_name": "qdl-r1-candidate-rollover"},
    )


async def read_current_rows(dsn: str, slice_ids: list[str]) -> list[dict[str, Any]]:
    connection = await _connection(dsn)
    try:
        return [dict(row) for row in await connection.fetch(_CURRENT_SQL, slice_ids)]
    finally:
        await connection.close()


def _bundle_equal(row: Mapping[str, Any], bundle: Mapping[str, Any], candidate: str) -> bool:
    evidence = row["evidence"]
    if isinstance(evidence, str):
        evidence = json.loads(evidence)
    return {
        "candidate_digest": row["candidate_digest"],
        "policy_revision": int(row["policy_revision"]),
        "decision": row["decision"],
        "evidence": evidence,
        "evidence_sha256": row["evidence_sha256"],
        "issued_by": row["issued_by"],
        "issued_at": row["issued_at"],
        "expires_at": row["expires_at"],
    } == {
        "candidate_digest": candidate,
        "policy_revision": int(bundle["policy_revision"]),
        "decision": bundle["decision"],
        "evidence": dict(bundle["evidence"]),
        "evidence_sha256": bundle["evidence_sha256"],
        "issued_by": bundle["issued_by"],
        "issued_at": datetime.fromisoformat(str(bundle["issued_at"])),
        "expires_at": datetime.fromisoformat(str(bundle["expires_at"])),
    }


async def apply_packet(packet: CandidateRolloverPacket, dsn: str) -> dict[str, Any]:
    connection = await _connection(dsn)
    try:
        async with connection.transaction():
            bundle = packet.prerequisite_bundle
            existing = await connection.fetchrow(
                "SELECT candidate_digest,policy_revision,decision,evidence,evidence_sha256,"
                "issued_by,issued_at,expires_at FROM qdl_production_prerequisite_bundles "
                "WHERE bundle_id=$1::uuid",
                bundle["bundle_id"],
            )
            bundle_new = existing is None
            if bundle_new:
                await connection.execute(
                    _BUNDLE_INSERT,
                    bundle["bundle_id"], packet.candidate_digest,
                    int(bundle["policy_revision"]), bundle["decision"],
                    json.dumps(bundle["evidence"], sort_keys=True),
                    bundle["evidence_sha256"], bundle["issued_by"],
                    datetime.fromisoformat(str(bundle["issued_at"])),
                    datetime.fromisoformat(str(bundle["expires_at"])),
                )
                existing = await connection.fetchrow(
                    "SELECT candidate_digest,policy_revision,decision,evidence,evidence_sha256,"
                    "issued_by,issued_at,expires_at FROM qdl_production_prerequisite_bundles "
                    "WHERE bundle_id=$1::uuid",
                    bundle["bundle_id"],
                )
            if existing is None or not _bundle_equal(dict(existing), bundle, packet.candidate_digest):
                raise RuntimeError("existing prerequisite bundle conflicts with rollover packet")
            existing_ids = {
                str(row["rollover_id"])
                for row in await connection.fetch(
                    "SELECT rollover_id FROM qdl_authority_candidate_rollovers "
                    "WHERE rollover_id = ANY($1::uuid[])",
                    [item.rollover_id for item in packet.rollovers],
                )
            }
            rows = []
            for item in packet.rollovers:
                row = await connection.fetchrow(
                    _ROLLOVER_SQL,
                    item.rollover_id, item.slice_id, item.expected_revision,
                    item.expected_owner_id, item.expected_lease_epoch,
                    item.expected_partition_plan_epoch, item.expected_candidate_digest,
                    item.expected_artifact_image_digest, item.new_owner_id,
                    item.new_lease_epoch, json.dumps(item.new_provenance, sort_keys=True),
                    bundle["bundle_id"], packet.raw["actor"],
                    f"R1 candidate rollover {packet.raw['change_ticket']}",
                )
                rows.append(dict(row))
            return {
                "schema": "qdl.r1.authority-candidate-rollover-apply.v1",
                "packet_digest": packet.digest,
                "slice_count": len(rows),
                "new_rollovers": len(rows) - len(existing_ids),
                "idempotent_rollovers": len(existing_ids),
                "bundle_new": bundle_new,
                "kafka_offset_mutations": 0,
                "production_mutations": int(bundle_new) + len(rows) - len(existing_ids),
                "states": sorted({str(row["state"]) for row in rows}),
                "revisions": sorted({int(row["authority_revision"]) for row in rows}),
            }
    finally:
        await connection.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--bootstrap", type=Path, required=True)
    prepare.add_argument("--actor", required=True)
    prepare.add_argument("--change-ticket", required=True)
    prepare.add_argument("--ttl-seconds", type=int, default=1_800)
    prepare.add_argument("--output", type=Path, required=True)
    apply = sub.add_parser("apply")
    apply.add_argument("--packet", type=Path, required=True)
    apply.add_argument("--apply", action="store_true")
    apply.add_argument("--confirm")
    args = parser.parse_args()

    if args.command == "prepare":
        bootstrap = _load_json(args.bootstrap)
        slice_ids = [str(item["slice_id"]) for item in bootstrap.get("slices", [])]
        rows = asyncio.run(read_current_rows(_required_dsn(), slice_ids))
        packet = prepare_rollover_packet(
            bootstrap, rows, actor=args.actor, change_ticket=args.change_ticket,
            ttl_seconds=args.ttl_seconds,
        )
        parsed = CandidateRolloverPacket.parse(packet)
        args.output.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps({**parsed.plan(), "output": str(args.output.resolve())}, sort_keys=True))
        return 0

    parsed = CandidateRolloverPacket.parse(_load_json(args.packet))
    plan = parsed.plan()
    if not args.apply:
        print(json.dumps(plan, sort_keys=True))
        return 0
    if args.confirm != parsed.confirmation_token:
        raise RuntimeError("candidate rollover confirmation token differs from packet")
    result = asyncio.run(apply_packet(parsed, _required_dsn()))
    print(json.dumps({**plan, **result, "apply_requested": True}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
