from __future__ import annotations

from datetime import datetime, timezone
import unittest

from qdl.control.authority_outbox import (
    AsyncpgAuthorityOutboxRepository,
    AuthorityOutboxDispatcher,
    BrokerAck,
    ClaimedAuthorityEvent,
    _checkpoint,
    _digest,
    build_authority_control_event,
)


EVENT_ID = "10000000-0000-4000-8000-000000000001"
CHECKPOINT_ID = "20000000-0000-4000-8000-000000000002"
HANDOFF_ID = "30000000-0000-4000-8000-000000000003"
BUNDLE_ID = "40000000-0000-4000-8000-000000000004"
NOW = datetime(2026, 8, 20, 9, 0, tzinfo=timezone.utc)
LATER = datetime(2026, 8, 20, 10, 0, tzinfo=timezone.utc)
DIGEST = "a" * 64


def checkpoint_raw():
    return {
        "checkpoint_id": CHECKPOINT_ID,
        "slice_id": "BINANCE:USDM:TRADE:ETHUSDT",
        "owner_id": "python-v1-owner",
        "authority_revision": 7,
        "lease_epoch": 10,
        "partition_plan_epoch": 3,
        "source_session_id": "python-session-7",
        "connection_generation": 2,
        "terminal_watermark": 500,
        "terminal_event_id": "event-500",
        "terminal_payload_sha256": "b" * 64,
        "candidate_digest": DIGEST,
        "committed_at": NOW.isoformat(),
    }


def handoff_raw():
    checkpoint = _checkpoint(checkpoint_raw())
    assert checkpoint is not None
    expected = {
        "schema": "qdl.accepted-authority-handoff.v1",
        "handoff_id": HANDOFF_ID,
        "direction": "PYTHON_TO_RUST",
        "checkpoint_digest": _digest(checkpoint),
        "slice_id": "BINANCE:USDM:TRADE:ETHUSDT",
        "old_owner_id": "python-v1-owner",
        "new_owner_id": "rust-primary-owner",
        "expected_state": "RUST_CANARY",
        "new_state": "RUST_PRIMARY",
        "expected_authority_revision": 7,
        "new_authority_revision": 8,
        "expected_lease_epoch": 10,
        "new_lease_epoch": 11,
        "partition_plan_epoch": 3,
        "terminal_watermark": 500,
        "first_new_watermark": 501,
        "overlap_start_watermark": 490,
        "overlap_end_watermark": 500,
        "old_event_count": 11,
        "new_event_count": 11,
        "semantic_mismatches": 0,
        "open_gaps": 0,
        "candidate_digest": DIGEST,
        "prerequisite_bundle_id": BUNDLE_ID,
        "approved_by": "operator@example",
        "approved_at_ns": int(NOW.timestamp() * 1_000_000_000),
        "expires_at_ns": int(LATER.timestamp() * 1_000_000_000),
    }
    return {
        "handoff_id": HANDOFF_ID,
        "checkpoint_id": CHECKPOINT_ID,
        "direction": "PYTHON_TO_RUST",
        "slice_id": expected["slice_id"],
        "old_owner_id": expected["old_owner_id"],
        "new_owner_id": expected["new_owner_id"],
        "expected_state": expected["expected_state"],
        "new_state": expected["new_state"],
        "expected_authority_revision": 7,
        "new_authority_revision": 8,
        "expected_lease_epoch": 10,
        "new_lease_epoch": 11,
        "partition_plan_epoch": 3,
        "terminal_watermark": 500,
        "first_new_watermark": 501,
        "overlap_start_watermark": 490,
        "overlap_end_watermark": 500,
        "old_event_count": 11,
        "new_event_count": 11,
        "semantic_mismatches": 0,
        "open_gaps": 0,
        "candidate_digest": DIGEST,
        "prerequisite_bundle_id": BUNDLE_ID,
        "handoff_sha256": _digest(expected),
        "approved_by": "operator@example",
        "approved_at": NOW.isoformat(),
        "expires_at": LATER.isoformat(),
    }


def outbox_payload(state="RUST_PRIMARY", *, handoff=True):
    authority = {
        "slice_id": "BINANCE:USDM:TRADE:ETHUSDT",
        "state": state,
        "owner_id": "rust-primary-owner",
        "authority_revision": 8,
        "lease_epoch": 11,
        "partition_plan_epoch": 3,
        "candidate_digest": DIGEST,
        "prerequisite_bundle_id": BUNDLE_ID,
        "terminal_watermark": 500,
        "approved_by": "operator@example",
        "approved_at": NOW.isoformat(),
        "hold_until": LATER.isoformat(),
    }
    return {
        "schema": "qdl.authority-outbox-event.v1",
        "event_id": EVENT_ID,
        "transition": {
            "transition_id": EVENT_ID,
            "slice_id": authority["slice_id"],
            "previous_state": "RUST_CANARY",
            "new_state": state,
            "previous_revision": 7,
            "new_revision": 8,
        },
        "authority": authority,
        "checkpoint": checkpoint_raw() if handoff else None,
        "handoff": handoff_raw() if handoff else None,
    }


class AuthorityControlEventTests(unittest.TestCase):
    def test_primary_event_binds_exact_handoff_and_w_boundary(self):
        event = build_authority_control_event(outbox_payload())
        authority = event["authority"]
        self.assertEqual(authority["state"], "RUST_PRIMARY")
        self.assertEqual(authority["start_watermark"], 500)
        self.assertEqual(authority["terminal_watermark"], 500)
        self.assertEqual(authority["previous_owner_id"], "python-v1-owner")
        self.assertEqual(authority["handoff_digest"], _digest(event["handoff"]))
        self.assertEqual(event["handoff"]["first_new_watermark"], 501)

    def test_shadow_transition_is_durable_but_not_phase92_writable(self):
        payload = outbox_payload("RUST_SHADOW", handoff=False)
        payload["authority"].update({
            "owner_id": "rust-shadow-owner",
            "authority_revision": 2,
            "lease_epoch": 2,
            "terminal_watermark": None,
            "prerequisite_bundle_id": None,
            "approved_by": None,
            "approved_at": None,
            "hold_until": None,
        })
        payload["transition"].update({"previous_revision": 1, "new_revision": 2})
        event = build_authority_control_event(payload)
        self.assertIsNone(event["authority"])
        self.assertEqual(event["database_state"], "RUST_SHADOW")

    def test_tampered_handoff_and_primary_without_handoff_fail_closed(self):
        tampered = outbox_payload()
        tampered["handoff"]["handoff_sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "digest differs"):
            build_authority_control_event(tampered)
        with self.assertRaisesRegex(ValueError, "requires accepted handoff"):
            build_authority_control_event(outbox_payload(handoff=False))


class _Repository:
    def __init__(self, payload):
        self.claimed = [ClaimedAuthorityEvent(EVENT_ID, payload)]
        self.completed = []
        self.retried = []

    async def claim(self, lock_owner, limit):
        del lock_owner, limit
        values, self.claimed = self.claimed, []
        return values

    async def complete(self, event_id, lock_owner, ack):
        self.completed.append((event_id, lock_owner, ack))

    async def retry(self, event_id, lock_owner, error, delay_seconds):
        self.retried.append((event_id, lock_owner, error, delay_seconds))


class _Publisher:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []

    async def publish(self, *, key, event_id, payload):
        self.calls.append((key, event_id, payload))
        if self.fail:
            raise RuntimeError("broker unavailable")
        return BrokerAck("qdl.authority.v1", 0, 12)


class _AsyncpgPool:
    def __init__(self):
        self.query = ""
        self.arguments = ()

    async def fetch(self, query, *arguments):
        self.query = query
        self.arguments = arguments
        return [{"event_id": EVENT_ID, "payload": outbox_payload(), "attempts": 3}]


class AuthorityOutboxDispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_asyncpg_claim_projects_and_maps_attempts(self):
        pool = _AsyncpgPool()
        repository = AsyncpgAuthorityOutboxRepository(pool)
        claimed = await repository.claim("dispatcher-1", 7)
        self.assertIn("event_id, payload, attempts", pool.query)
        self.assertEqual(pool.arguments, ("dispatcher-1", 7))
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0].event_id, EVENT_ID)
        self.assertEqual(claimed[0].attempts, 3)

    async def test_ack_completes_and_failure_is_retried_without_false_publish(self):
        repository = _Repository(outbox_payload())
        publisher = _Publisher()
        dispatcher = AuthorityOutboxDispatcher(
            repository=repository, publisher=publisher,
            lock_owner="dispatcher-1", batch_size=1,
        )
        self.assertEqual(await dispatcher.dispatch_once(), 1)
        self.assertEqual(len(repository.completed), 1)
        self.assertEqual(repository.retried, [])
        self.assertEqual(publisher.calls[0][0], "BINANCE:USDM:TRADE:ETHUSDT")

        failed_repository = _Repository(outbox_payload())
        failed = AuthorityOutboxDispatcher(
            repository=failed_repository, publisher=_Publisher(fail=True),
            lock_owner="dispatcher-2", batch_size=1,
        )
        self.assertEqual(await failed.dispatch_once(), 0)
        self.assertEqual(failed_repository.completed, [])
        self.assertEqual(len(failed_repository.retried), 1)


if __name__ == "__main__":
    unittest.main()
