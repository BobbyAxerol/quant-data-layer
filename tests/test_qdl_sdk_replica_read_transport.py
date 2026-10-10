from __future__ import annotations

from types import SimpleNamespace
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
        transport = ReplicatedRestQueryTransport((first, second), start_index=0)

        self.assertEqual(await transport.snapshot(object(), consumer_id="alpha"), {"replica": "a"})
        self.assertEqual(await transport.snapshot(object(), consumer_id="alpha"), {"replica": "b"})
        self.assertEqual((first.calls, second.calls), (1, 1))

    async def test_default_start_spreads_first_reads_over_replicas(self):
        firsts = set()
        for _ in range(64):
            first = _Replica("https://query-a", ({"replica": "a"},))
            second = _Replica("https://query-b", ({"replica": "b"},))
            transport = ReplicatedRestQueryTransport((first, second))
            firsts.add((await transport.snapshot(object(), consumer_id="alpha"))["replica"])
        self.assertEqual(firsts, {"a", "b"})

    def test_start_index_must_name_a_replica(self):
        replicas = (_Replica("https://query-a", ()), _Replica("https://query-b", ()))
        for bad in (-1, 2):
            with self.assertRaises(ValueError):
                ReplicatedRestQueryTransport(replicas, start_index=bad)

    async def test_transport_failure_uses_one_alternate_and_cools_failed_replica(self):
        first = _Replica("https://query-a", (_connect_error(),))
        second = _Replica("https://query-b", ({"replica": "b1"}, {"replica": "b2"}))
        transport = ReplicatedRestQueryTransport((first, second), cooldown_seconds=60, start_index=0)

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
        transport = ReplicatedRestQueryTransport((first, second), start_index=0)

        with self.assertRaisesRegex(DataLayerError, "authoritative stale"):
            await transport.snapshot(object(), consumer_id="alpha")
        self.assertEqual((first.calls, second.calls), (1, 0))

    async def test_close_closes_every_owned_replica(self):
        first = _Replica("https://query-a", ())
        second = _Replica("https://query-b", ())
        transport = ReplicatedRestQueryTransport((first, second), start_index=0)
        await transport.close()
        self.assertTrue(first.closed)
        self.assertTrue(second.closed)


def _lane_busy() -> DataLayerError:
    return DataLayerError(
        "RATE_LIMITED", "REPLICA_LANE_BUSY: lane busy", retryable=True,
        retry_after_ms=0, reason="REPLICA_LANE_BUSY",
    )


class _WarmReplica(_Replica):
    """Records the redirect opt-in each warmup attempt carried."""

    def __init__(self, base_url: str, outcomes) -> None:
        super().__init__(base_url, outcomes)
        self.redirects: list[object] = []
        self.args: list[tuple] = []

    async def _attempt(self, args, replica_redirect):
        self.calls += 1
        self.redirects.append(replica_redirect)
        self.args.append(args)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def warmup(self, requirement, *, consumer_id: str, replica_redirect=None):
        return await self._attempt((requirement, consumer_id), replica_redirect)

    async def warmup_batch(self, requirements, *, consumer_id: str, require_all: bool,
                           replica_redirect=None):
        return await self._attempt((requirements, consumer_id, require_all), replica_redirect)


class ReplicaLaneBusyRedirectTests(unittest.IsolatedAsyncioTestCase):
    """2.2.0 (P18.4 G5): one retry on the next replica, only for REPLICA_LANE_BUSY."""

    async def test_lane_busy_retries_once_on_the_next_replica_with_redirect_off(self):
        for method, args, kwargs in (
            ("warmup", ("req",), {"consumer_id": "alpha"}),
            ("warmup_batch", (("r1", "r2"),), {"consumer_id": "alpha", "require_all": True}),
        ):
            first = _WarmReplica("https://query-a", (_lane_busy(),))
            second = _WarmReplica("https://query-b", ({"replica": "b"},))
            transport = ReplicatedRestQueryTransport((first, second), start_index=0)
            result = await getattr(transport, method)(*args, **kwargs)
            self.assertEqual(result, {"replica": "b"})
            self.assertEqual((first.redirects, second.redirects), ([True], [False]))
            # The same request (requirements, identity) goes to the second replica.
            self.assertEqual(first.args, second.args)
            # A busy lane is not a replica failure.
            self.assertEqual([item["failures"] for item in transport.stats()], [0, 0])

    async def test_both_replicas_busy_queues_on_the_second_no_ping_pong(self):
        first = _WarmReplica("https://query-a", (_lane_busy(), {"replica": "a"}))
        second = _WarmReplica("https://query-b", ({"replica": "b-queued"},))
        transport = ReplicatedRestQueryTransport((first, second), start_index=0)
        # The second replica is busy too, but redirect=0 makes it queue there.
        self.assertEqual(await transport.warmup("req", consumer_id="alpha"), {"replica": "b-queued"})
        self.assertEqual((first.calls, second.calls), (1, 1))
        self.assertEqual(second.redirects, [False])

    async def test_a_second_busy_refusal_is_not_redirected_again(self):
        # A server must queue on redirect=0; if a refusal still comes back it
        # is returned to the caller, never sent around again.
        first = _WarmReplica("https://query-a", (_lane_busy(),))
        second = _WarmReplica("https://query-b", (_lane_busy(),))
        transport = ReplicatedRestQueryTransport((first, second), start_index=0)
        with self.assertRaises(DataLayerError) as raised:
            await transport.warmup("req", consumer_id="alpha")
        self.assertEqual(raised.exception.reason, "REPLICA_LANE_BUSY")
        self.assertEqual((first.calls, second.calls), (1, 1))

    async def test_other_typed_errors_stay_authoritative(self):
        for error in (
            DataLayerError("RATE_LIMITED", "consumer request quota is exhausted", retryable=True),
            DataLayerError("RATE_LIMITED", "read lane is full", retryable=True, reason="OTHER"),
            DataLayerError("DATA_STALE", "REPLICA_LANE_BUSY", retryable=True, reason="REPLICA_LANE_BUSY"),
            DataLayerError("SOURCE_NOT_ALLOWED", "entitlement", retryable=False),
        ):
            first = _WarmReplica("https://query-a", (error,))
            second = _WarmReplica("https://query-b", ({"replica": "b"},))
            transport = ReplicatedRestQueryTransport((first, second), start_index=0)
            with self.assertRaises(DataLayerError):
                await transport.warmup("req", consumer_id="alpha")
            self.assertEqual((first.calls, second.calls), (1, 0), error.detail)

    async def test_no_opt_in_without_a_fallback_candidate(self):
        # Transport failure consumed the first candidate: the last attempt
        # must queue (no header), never be refused as busy with nowhere to go.
        first = _WarmReplica("https://query-a", (_connect_error(),))
        second = _WarmReplica("https://query-b", ({"replica": "b"},))
        transport = ReplicatedRestQueryTransport((first, second), start_index=0)
        self.assertEqual(await transport.warmup("req", consumer_id="alpha"), {"replica": "b"})
        self.assertEqual((first.redirects, second.redirects), ([True], [None]))
        single = _WarmReplica("https://query-c", ({"replica": "c"},))
        other = _WarmReplica("https://query-d", ())
        limited = ReplicatedRestQueryTransport((single, other), max_attempts=1, start_index=0)
        self.assertEqual(await limited.warmup("req", consumer_id="alpha"), {"replica": "c"})
        self.assertEqual(single.redirects, [None])

    async def test_snapshot_never_carries_the_opt_in(self):
        first = _Replica("https://query-a", ({"replica": "a"},))
        second = _Replica("https://query-b", ())
        transport = ReplicatedRestQueryTransport((first, second), start_index=0)
        # _Replica.snapshot takes no replica_redirect keyword; a TypeError would fail here.
        self.assertEqual(await transport.snapshot("req", consumer_id="alpha"), {"replica": "a"})


class RestRedirectHeaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_header_values_and_refusal_reason_decode(self):
        from qdl_sdk.models import Grade
        from qdl_sdk.transport import RestQueryTransport

        seen: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.headers.get("X-QDL-Replica-Redirect"))
            return httpx.Response(
                429,
                json={"code": "RATE_LIMITED", "detail": "REPLICA_LANE_BUSY: busy",
                      "retryable": True, "retry_after_ms": 0},
                headers={"X-QDL-Refusal-Reason": "REPLICA_LANE_BUSY", "Retry-After": "0"},
            )

        class Credential:
            async def get_token(self):
                return "token"

        client = httpx.AsyncClient(base_url="https://qdl", transport=httpx.MockTransport(handler))
        rest = RestQueryTransport("https://qdl", client=client, credential_provider=Credential())
        requirement = SimpleNamespace(
            instrument_uid="inst", consumer_grade=Grade.ALPHA,
            query_params=lambda: {"feed": "BAR"}, to_mapping=lambda: {"instrument_uid": "inst"},
        )
        for value in (None, True, False):
            with self.assertRaises(DataLayerError) as raised:
                await rest.warmup(requirement, consumer_id="alpha", replica_redirect=value)
            self.assertEqual(raised.exception.reason, "REPLICA_LANE_BUSY")
            self.assertEqual(raised.exception.retry_after_ms, 0)
        with self.assertRaises(DataLayerError):
            await rest.warmup_batch((requirement,), consumer_id="alpha", require_all=True,
                                    replica_redirect=True)
        self.assertEqual(seen, [None, "1", "0", "1"])
        await client.aclose()

    def test_reason_is_none_without_the_header(self):
        from qdl_sdk.transport import RestQueryTransport

        with self.assertRaises(DataLayerError) as raised:
            RestQueryTransport._decode(httpx.Response(
                429, json={"code": "RATE_LIMITED", "detail": "quota", "retryable": True}))
        self.assertIsNone(raised.exception.reason)


class RefusalDiagnosticsDecodeTests(unittest.TestCase):
    def test_a_refusal_keeps_the_servers_quality_diagnostics(self) -> None:
        from qdl_sdk.transport import RestQueryTransport

        body = {"code": "DATA_STALE", "detail": "required data exceeds its freshness policy (SESSION_STATE)",
                "retryable": True, "diagnostics": {"state": "STALE", "provider_session_state": "UNKNOWN",
                                                   "reason_codes": ["SOURCE_SESSION_CLOCK_SKEW"]}}
        with self.assertRaises(DataLayerError) as raised:
            RestQueryTransport._decode(httpx.Response(503, json=body))
        self.assertEqual(raised.exception.code, "DATA_STALE")
        self.assertEqual(raised.exception.diagnostics["reason_codes"], ["SOURCE_SESSION_CLOCK_SKEW"])
        # An older server sends none; a malformed value is dropped, not trusted.
        for extra in ({}, {"diagnostics": "x"}):
            with self.assertRaises(DataLayerError) as raised:
                RestQueryTransport._decode(httpx.Response(503, json={**body, "diagnostics": None, **extra}))
            self.assertIsNone(raised.exception.diagnostics)
