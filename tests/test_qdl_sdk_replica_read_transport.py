from __future__ import annotations

import unittest

import httpx

from qdl_sdk.errors import DataLayerError
from qdl_sdk.transport import ReplicatedRestQueryTransport


class _Replica:
    def __init__(self, base_url: str, outcomes) -> None:
        self.base_url = base_url
        self.outcomes = list(outcomes)
        self.calls = 0
        self.closed = False

    async def snapshot(self, requirement, *, consumer_id: str):
        del requirement, consumer_id
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def close(self) -> None:
        self.closed = True


def _connect_error() -> httpx.ConnectError:
    return httpx.ConnectError("reader unavailable", request=httpx.Request("GET", "https://qdl"))


class ReplicaReadTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_round_robin_uses_both_healthy_replicas(self):
        first = _Replica("https://query-a", ({"replica": "a"},))
        second = _Replica("https://query-b", ({"replica": "b"},))
        transport = ReplicatedRestQueryTransport((first, second))

        self.assertEqual(await transport.snapshot(object(), consumer_id="alpha"), {"replica": "a"})
        self.assertEqual(await transport.snapshot(object(), consumer_id="alpha"), {"replica": "b"})
        self.assertEqual((first.calls, second.calls), (1, 1))

    async def test_transport_failure_uses_one_alternate_and_cools_failed_replica(self):
        first = _Replica("https://query-a", (_connect_error(),))
        second = _Replica("https://query-b", ({"replica": "b1"}, {"replica": "b2"}))
        transport = ReplicatedRestQueryTransport((first, second), cooldown_seconds=60)

        self.assertEqual(await transport.snapshot(object(), consumer_id="alpha"), {"replica": "b1"})
        self.assertEqual(await transport.snapshot(object(), consumer_id="alpha"), {"replica": "b2"})
        self.assertEqual((first.calls, second.calls), (1, 2))
        stats = {item["base_url"]: item for item in transport.stats()}
        self.assertEqual(stats["https://query-a"]["failures"], 1)
        self.assertEqual(stats["https://query-a"]["cooling"], 1)

    async def test_typed_v2_error_never_falls_back_to_another_replica(self):
        first = _Replica(
            "https://query-a",
            (DataLayerError("DATA_STALE", "authoritative stale", retryable=True),),
        )
        second = _Replica("https://query-b", ({"replica": "b"},))
        transport = ReplicatedRestQueryTransport((first, second))

        with self.assertRaisesRegex(DataLayerError, "authoritative stale"):
            await transport.snapshot(object(), consumer_id="alpha")
        self.assertEqual((first.calls, second.calls), (1, 0))

    async def test_close_closes_every_owned_replica(self):
        first = _Replica("https://query-a", ())
        second = _Replica("https://query-b", ())
        transport = ReplicatedRestQueryTransport((first, second))
        await transport.close()
        self.assertTrue(first.closed)
        self.assertTrue(second.closed)
