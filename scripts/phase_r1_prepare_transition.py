#!/usr/bin/env python3
"""Prepare exact C3 terminalize/revalidate/canary packets for R1.

The command is read-only: it selects current authority rows and writes an
operator-review packet.  `phasec3_authority_cutover.py --apply` remains the
sole state-transition executor.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
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


_CURRENT_SQL = """
SELECT slice_id,state,authority_revision,owner_id,lease_epoch,partition_plan_epoch,
       candidate_digest,artifact_image_digest,contract_digest,partition_plan_digest,
       rollback_manifest_digest
FROM qdl_authority_slices
WHERE slice_id = ANY($1::text[])
ORDER BY slice_id
"""

_STAGE_EXPECTED = {
    "BLOCK_CANARY": ("RUST_CANARY", "BLOCKED"),
    "REVALIDATE": ("BLOCKED", "VALIDATING"),
    "CANARY": ("VALIDATING", "RUST_CANARY"),
}


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()



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
        raise RuntimeError("R1 C3 packet preparation requires asyncpg") from error
    connection = await asyncpg.connect(
        dsn=dsn,
        command_timeout=15,
        server_settings={"application_name": "qdl-r1-transition-prepare"},
    )
    try:
        return [dict(row) for row in await connection.fetch(_CURRENT_SQL, slice_ids)]
    finally:
        await connection.close()


def _scope_from_bootstrap(raw: Mapping[str, Any]) -> list[str]:
    values = raw.get("slices")
    if not isinstance(values, list) or len(values) != 12:
        raise ValueError("C40 bootstrap scope must contain exactly twelve slices")
    result = [str(item.get("slice_id", "")) for item in values if isinstance(item, Mapping)]
    if len(result) != 12 or any(not value for value in result) or len(set(result)) != 12:
        raise ValueError("C40 bootstrap slice identities are invalid")
    return sorted(result)


def prepare_transition_packet(
    *,
    stage: str,
    current_rows: list[Mapping[str, Any]],
    actor: str,
    change_ticket: str,
    rollover: CandidateRolloverPacket | None = None,
    issued_at_ns: int | None = None,
    ttl_seconds: int = 900,
) -> dict[str, Any]:
    if stage not in _STAGE_EXPECTED:
        raise ValueError("R1 transition stage is unsupported")
    if not actor.strip() or not change_ticket.strip() or not 60 <= ttl_seconds <= 3_600:
        raise ValueError("R1 transition actor/ticket/TTL is invalid")
    if len(current_rows) != 12:
        raise ValueError("R1 transition requires exactly twelve current rows")
    expected_state, new_state = _STAGE_EXPECTED[stage]
    rows = sorted((dict(item) for item in current_rows), key=lambda item: str(item["slice_id"]))
    if len({str(item["slice_id"]) for item in rows}) != 12:
        raise ValueError("R1 transition authority slice identities are not unique")
    for row in rows:
        if str(row.get("state")) != expected_state:
            raise ValueError(
                f"R1 transition state differs slice={row.get('slice_id')} expected={expected_state}"
            )
    common = {
        name: {str(row[name]) for row in rows}
        for name in (
            "candidate_digest", "artifact_image_digest", "contract_digest",
            "partition_plan_digest", "rollback_manifest_digest",
        )
    }
    if any(len(values) != 1 for values in common.values()):
        raise ValueError("R1 transition authority provenance is not uniform")
    candidate_digest = next(iter(common["candidate_digest"]))
    image_digest = next(iter(common["artifact_image_digest"]))
    contract_digest = next(iter(common["contract_digest"]))
    partition_plan_digest = next(iter(common["partition_plan_digest"]))
    now = time.time_ns() if issued_at_ns is None else int(issued_at_ns)
    bundle_id: str | None = None
    hold_until: str | None = None
    terminal: int | None = None
    if stage == "CANARY":
        if rollover is None:
            raise ValueError("R1 CANARY preparation requires the accepted rollover packet")
        if (
            rollover.candidate_digest != candidate_digest
            or rollover.candidate["rust_image_digest"] != image_digest
            or rollover.candidate["contract_digest"] != contract_digest
            or rollover.candidate["partition_plan_digest"] != partition_plan_digest
        ):
            raise ValueError("R1 CANARY authority differs from the accepted rollover packet")
        expected_rollovers = {item.slice_id: item for item in rollover.rollovers}
        if set(expected_rollovers) != {str(row["slice_id"]) for row in rows}:
            raise ValueError("R1 CANARY scope differs from rollover packet")
        bundle_id = str(rollover.prerequisite_bundle["bundle_id"])
        hold_until = _iso_from_ns(min(
            int(rollover.raw["expires_at_ns"]),
            int(datetime.fromisoformat(
                str(rollover.prerequisite_bundle["expires_at"])
            ).timestamp() * 1_000_000_000),
        ))
        if datetime.fromisoformat(hold_until).timestamp() * 1_000_000_000 <= now:
            raise ValueError("R1 CANARY hold window is no longer active")
        terminal = 0
    route_manifest_digest = next(iter(common["rollback_manifest_digest"]))
    if len(route_manifest_digest) != 64 or any(
        char not in "0123456789abcdef" for char in route_manifest_digest
    ):
        raise ValueError("R1 transition route manifest digest is invalid")
    if stage == "BLOCK_CANARY":
        evidence = {
            "provider_provenance": "UNKNOWN",
            "semantic_mismatches": 0,
            "open_gaps": 0,
            "duplicate_external_effects": 0,
            "consumer_errors": 0,
        }
        reason = "R1 terminalizes C40 canary: unsafe historical offset bootstrap is fenced"
    else:
        evidence = {
            "provider_provenance": "REAL",
            "semantic_mismatches": 0,
            "open_gaps": 0,
            "duplicate_external_effects": 0,
            "consumer_errors": 0,
        }
        reason = f"R1 exact twelve-slice {stage.lower()} transition"
    slices = []
    for row in rows:
        transition_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"qdl-r1-c3:{stage}:{candidate_digest}:{row['slice_id']}:{row['authority_revision']}",
        ))
        slices.append({
            "transition_id": transition_id,
            "handoff_id": None,
            "slice_id": str(row["slice_id"]),
            "expected_state": expected_state,
            "expected_revision": int(row["authority_revision"]),
            "expected_owner_id": str(row["owner_id"]),
            "expected_lease_epoch": int(row["lease_epoch"]),
            "partition_plan_epoch": int(row["partition_plan_epoch"]),
            "new_state": new_state,
            "new_owner_id": str(row["owner_id"]),
            "new_lease_epoch": int(row["lease_epoch"]),
            "terminal_watermark": terminal,
            "prerequisite_bundle_id": bundle_id,
            "hold_until": hold_until,
            "reason": reason,
        })
    packet = {
        "schema": "qdl.c3.authority-cutover-packet.v1",
        "packet_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"qdl-r1-c3-packet:{stage}:{candidate_digest}:{','.join(item['transition_id'] for item in slices)}",
        )),
        "stage": stage,
        "issued_at_ns": now,
        "expires_at_ns": now + ttl_seconds * 1_000_000_000,
        "actor": actor,
        "change_ticket": change_ticket,
        "candidate_digest": candidate_digest,
        "artifact_image_digest": image_digest,
        "contract_digest": contract_digest,
        "partition_plan_digest": partition_plan_digest,
        "route_manifest_digest": route_manifest_digest,
        "consumer_route": {
            "consumer_id": "trading-system",
            "expected_route": "V1",
            "new_route": "V2_PRIMARY",
            "rollback_route": "V1",
            "rollback_command": ["scripts/qdl_route.py", "--route", "V1"],
        },
        "evidence": evidence,
        "slices": slices,
    }
    AuthorityCutoverPacket.parse(packet, now_ns=now)
    return packet


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=sorted(_STAGE_EXPECTED), required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--change-ticket", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--bootstrap", type=Path)
    parser.add_argument("--rollover-packet", type=Path)
    parser.add_argument("--ttl-seconds", type=int, default=900)
    args = parser.parse_args()

    rollover = None
    if args.stage == "BLOCK_CANARY":
        if args.bootstrap is None:
            raise ValueError("BLOCK_CANARY requires --bootstrap")
        slice_ids = _scope_from_bootstrap(_load_json(args.bootstrap))
    else:
        if args.rollover_packet is None:
            raise ValueError("REVALIDATE/CANARY requires --rollover-packet")
        rollover = CandidateRolloverPacket.parse(_load_json(args.rollover_packet))
        slice_ids = sorted(item.slice_id for item in rollover.rollovers)
    rows = asyncio.run(_rows(_required_dsn(), slice_ids))
    packet = prepare_transition_packet(
        stage=args.stage,
        current_rows=rows,
        actor=args.actor,
        change_ticket=args.change_ticket,
        rollover=rollover,
        ttl_seconds=args.ttl_seconds,
    )
    parsed = AuthorityCutoverPacket.parse(packet)
    args.output.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({**parsed.plan(), "output": str(args.output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
