from __future__ import annotations

import copy
import unittest

from qdl.control.cutover_packet import AuthorityCutoverPacket
from scripts.phasec3_authority_cutover import _assert_current


NOW = 1_800_000_000_000_000_000
DIGEST = "a" * 64


def packet(stage: str = "CANARY") -> dict:
    pairs = {
        "SHADOW_VALIDATE": ("RUST_SHADOW", "VALIDATING"),
        "CANARY": ("VALIDATING", "RUST_CANARY"),
        "PRIMARY": ("RUST_CANARY", "RUST_PRIMARY"),
    }
    expected, new = pairs[stage]
    active = stage in {"CANARY", "PRIMARY"}
    return {
        "schema": "qdl.c3.authority-cutover-packet.v1",
        "packet_id": "10000000-0000-4000-8000-000000000001",
        "stage": stage,
        "issued_at_ns": NOW - 1,
        "expires_at_ns": NOW + 1_000_000_000,
        "actor": "operator@example",
        "change_ticket": "QDL-C3-001",
        "candidate_digest": DIGEST,
        "artifact_image_digest": "sha256:" + "b" * 64,
        "contract_digest": "c" * 64,
        "partition_plan_digest": "d" * 64,
        "route_manifest_digest": "e" * 64,
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
        "slices": [{
            "transition_id": "20000000-0000-4000-8000-000000000002",
            "handoff_id": (
                "30000000-0000-4000-8000-000000000003"
                if stage == "PRIMARY" else None
            ),
            "slice_id": "production/binance/usdm/perpetual/trade/plan-1/btcusdt",
            "expected_state": expected,
            "expected_revision": 2,
            "expected_owner_id": "rust-shadow",
            "expected_lease_epoch": 1,
            "partition_plan_epoch": 1,
            "new_state": new,
            "new_owner_id": "rust-canary",
            "new_lease_epoch": 2,
            "terminal_watermark": 500 if active else None,
            "prerequisite_bundle_id": (
                "40000000-0000-4000-8000-000000000004" if active else None
            ),
            "hold_until": "2026-08-20T12:00:00Z" if active else None,
            "reason": "bounded C.3 transition",
        }],
    }


class AuthorityCutoverPacketTests(unittest.TestCase):
    def test_plan_is_immutable_and_has_no_mutation(self):
        parsed = AuthorityCutoverPacket.parse(packet(), now_ns=NOW)
        plan = parsed.plan()
        self.assertEqual(plan["stage"], "CANARY")
        self.assertEqual(plan["slice_count"], 1)
        self.assertEqual(plan["production_mutations"], 0)
        self.assertTrue(plan["confirmation_token"].startswith("APPLY_C3_"))

        changed = packet()
        changed["change_ticket"] = "QDL-C3-002"
        changed_packet = AuthorityCutoverPacket.parse(changed, now_ns=NOW)
        self.assertNotEqual(
            parsed.confirmation_token, changed_packet.confirmation_token
        )

    def test_unknown_dirty_or_invalid_transition_fails_closed(self):
        cases = []
        unknown = packet()
        unknown["unexpected"] = True
        cases.append(unknown)
        dirty = packet()
        dirty["evidence"]["semantic_mismatches"] = 1
        cases.append(dirty)
        wrong_pair = packet()
        wrong_pair["slices"][0]["new_state"] = "RUST_PRIMARY"
        cases.append(wrong_pair)
        missing_rollback = packet()
        missing_rollback["consumer_route"]["rollback_command"] = []
        cases.append(missing_rollback)
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    AuthorityCutoverPacket.parse(value, now_ns=NOW)

    def test_primary_requires_handoff_and_exact_database_preconditions(self):
        parsed = AuthorityCutoverPacket.parse(packet("PRIMARY"), now_ns=NOW)
        item = parsed.slices[0]
        row = {
            "state": item.expected_state,
            "authority_revision": item.expected_revision,
            "owner_id": item.expected_owner_id,
            "lease_epoch": item.expected_lease_epoch,
            "partition_plan_epoch": item.partition_plan_epoch,
            "candidate_digest": parsed.candidate_digest,
            "artifact_image_digest": parsed.artifact_image_digest,
            "contract_digest": parsed.contract_digest,
            "partition_plan_digest": parsed.partition_plan_digest,
        }
        _assert_current(row, item, parsed)
        stale = copy.deepcopy(row)
        stale["authority_revision"] += 1
        with self.assertRaises(RuntimeError):
            _assert_current(stale, item, parsed)

        no_handoff = packet("PRIMARY")
        no_handoff["slices"][0]["handoff_id"] = None
        with self.assertRaises(ValueError):
            AuthorityCutoverPacket.parse(no_handoff, now_ns=NOW)


if __name__ == "__main__":
    unittest.main()
