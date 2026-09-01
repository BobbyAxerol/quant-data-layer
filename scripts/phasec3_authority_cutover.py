#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from qdl.control.cutover_packet import AuthorityCutoverPacket, CutoverSlice
from qdl.control.operator_env import require_control_admin_dsn


_CURRENT_SQL = """
SELECT state, authority_revision, owner_id, lease_epoch, partition_plan_epoch,
       candidate_digest, artifact_image_digest, contract_digest,
       partition_plan_digest
FROM qdl_authority_slices
WHERE slice_id = $1
FOR UPDATE
"""
_STANDARD_SQL = """
SELECT *
FROM qdl_transition_authority(
    $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12::uuid, $13::timestamptz, $14, $15
)
"""
_HANDOFF_SQL = """
SELECT *
FROM qdl_transition_authority_v2(
    $1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13::uuid, $14::timestamptz, $15, $16
)
"""



def _assert_current(
    row: Any, item: CutoverSlice, packet: AuthorityCutoverPacket
) -> None:
    if row is None:
        raise RuntimeError(f"authority slice is absent: {item.slice_id}")
    expected = {
        "state": item.expected_state,
        "authority_revision": item.expected_revision,
        "owner_id": item.expected_owner_id,
        "lease_epoch": item.expected_lease_epoch,
        "partition_plan_epoch": item.partition_plan_epoch,
        "candidate_digest": packet.candidate_digest,
        "artifact_image_digest": packet.artifact_image_digest,
        "contract_digest": packet.contract_digest,
        "partition_plan_digest": packet.partition_plan_digest,
    }
    actual = {name: row[name] for name in expected}
    if actual != expected:
        raise RuntimeError(
            f"authority CAS precondition differs for {item.slice_id}: "
            f"expected={expected!r} actual={actual!r}"
        )


async def _apply_one(
    connection: Any, packet: AuthorityCutoverPacket, item: CutoverSlice
) -> dict[str, Any]:
    async with connection.transaction():
        current = await connection.fetchrow(_CURRENT_SQL, item.slice_id)
        _assert_current(current, item, packet)
        if packet.stage in {"PRIMARY", "PYTHON_RESTORE"}:
            updated = await connection.fetchrow(
                _HANDOFF_SQL,
                item.handoff_id,
                item.transition_id,
                item.slice_id,
                item.expected_state,
                item.expected_revision,
                item.expected_owner_id,
                item.expected_lease_epoch,
                item.partition_plan_epoch,
                item.new_state,
                item.new_owner_id,
                item.new_lease_epoch,
                item.terminal_watermark,
                item.prerequisite_bundle_id,
                datetime.fromisoformat(item.hold_until) if item.hold_until else None,
                packet.actor,
                item.reason,
            )
        else:
            updated = await connection.fetchrow(
                _STANDARD_SQL,
                item.transition_id,
                item.slice_id,
                item.expected_state,
                item.expected_revision,
                item.expected_owner_id,
                item.expected_lease_epoch,
                item.partition_plan_epoch,
                item.new_state,
                item.new_owner_id,
                item.new_lease_epoch,
                item.terminal_watermark,
                item.prerequisite_bundle_id,
                datetime.fromisoformat(item.hold_until) if item.hold_until else None,
                packet.actor,
                item.reason,
            )
        return {
            "slice_id": item.slice_id,
            "state": updated["state"],
            "authority_revision": updated["authority_revision"],
            "owner_id": updated["owner_id"],
            "lease_epoch": updated["lease_epoch"],
        }


async def apply_packet(packet: AuthorityCutoverPacket, dsn: str) -> list[dict[str, Any]]:
    try:
        import asyncpg
    except ImportError as error:
        raise RuntimeError("authority cutover apply requires asyncpg") from error
    connection = await asyncpg.connect(
        dsn=dsn,
        command_timeout=15,
        server_settings={"application_name": "qdl-c3-authority-cutover"},
    )
    results = []
    try:
        for item in packet.slices:
            results.append(await _apply_one(connection, packet, item))
    finally:
        await connection.close()
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    raw = json.loads(args.packet.read_text(encoding="utf-8"))
    packet = AuthorityCutoverPacket.parse(raw)
    plan = packet.plan()
    if not args.apply:
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0
    if args.confirm != packet.confirmation_token:
        raise RuntimeError(
            "authority cutover confirmation token differs from immutable packet"
        )
    results = asyncio.run(
        apply_packet(packet, require_control_admin_dsn())
    )
    print(json.dumps({
        **plan,
        "apply_requested": True,
        "production_mutations": len(results),
        "results": results,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
