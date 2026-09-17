"""R1.24: one retry on a dead pooled connection before giving up on a gateway.

The defect, recorded three times during the R1 rollouts and once more on
2026-09-17: recreating a stream container leaves every projector unable to
publish. The projector's pooled connection to the process that restarted fails
on first use, the loop moves to the other URL, and that one answers 409 because
it is not the active writer. Both URLs are then spent and the batch fails with
`statuses=409:stable gateway is not active`. Every stream rollout in R1 needed a
manual projector restart afterwards.

A transport failure on a pooled connection is not evidence that the gateway is
gone; it is evidence that this particular socket is. One retry against the same
URL opens a fresh one. A rejection, a bad status and a contract violation are
all different and must still move on or fail immediately.
"""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

import httpx

from qdl.runtime.stable_ingest import StableHttpCanonicalSink


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.headers: dict[str, str] = {}
        self.text = ""

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "error", request=mock.Mock(), response=self  # type: ignore[arg-type]
            )


def build_sink(urls: tuple[str, ...]) -> StableHttpCanonicalSink:
    sink = StableHttpCanonicalSink.__new__(StableHttpCanonicalSink)
    sink.urls = urls
    sink.secret = b"x" * 32
    sink.client = mock.AsyncMock()
    sink.spool = mock.Mock()
    return sink


def run(coro):
    return asyncio.run(coro)


class ConnectionRetryTests(unittest.TestCase):
    ACTIVE = ("https://stream_v2_active:8200", "https://stream_v2_passive:8200")

    def _publish(self, sink):
        event = mock.Mock()
        event.event_id.hex.return_value = "aa"
        event.stream = "md.canonical.v2"
        return run(sink._publish_chunk((event,), ({"canonical": "x"},)))

    def test_a_dead_pooled_connection_is_retried_on_the_same_url(self) -> None:
        # The behaviour under test is the second attempt against the *same* URL.
        # What happens after the socket is replaced belongs to the ACK contract
        # and is covered by the existing ingest tests.
        sink = build_sink(self.ACTIVE)
        sink.client.post = mock.AsyncMock(side_effect=[
            httpx.ConnectError("closed"),
            FakeResponse(200, {"schema": "wrong", "results": []}),
            FakeResponse(409),
        ])
        with self.assertRaises(RuntimeError):
            self._publish(sink)
        urls = [call.args[0] for call in sink.client.post.await_args_list]
        self.assertGreaterEqual(len(urls), 2)
        self.assertEqual(urls[0], urls[1])
        self.assertIn("stream_v2_active", urls[0])

    def test_two_transport_failures_move_to_the_second_url(self) -> None:
        sink = build_sink(self.ACTIVE)
        sink.client.post = mock.AsyncMock(side_effect=httpx.ConnectError("closed"))
        with self.assertRaises(RuntimeError):
            self._publish(sink)
        # Two attempts per URL, two URLs.
        self.assertEqual(sink.client.post.await_count, 4)
        urls = [call.args[0] for call in sink.client.post.await_args_list]
        self.assertIn("stream_v2_active", urls[0])
        self.assertIn("stream_v2_active", urls[1])
        self.assertIn("stream_v2_passive", urls[2])
        self.assertIn("stream_v2_passive", urls[3])

    def test_the_recreate_scenario_reaches_the_active_gateway_again(self) -> None:
        # Active restarted, so its pooled socket is dead. Before the retry the
        # loop moved straight to passive, got 409 because passive is not the
        # writer, and the projector stalled. Now the second attempt lands on the
        # active gateway and gets a 200 out of it.
        sink = build_sink(self.ACTIVE)
        sink.client.post = mock.AsyncMock(side_effect=[
            httpx.ConnectError("closed"),
            FakeResponse(200, {"schema": "wrong", "results": []}),
            FakeResponse(409),
        ])
        with self.assertRaises(RuntimeError):
            self._publish(sink)
        calls = sink.client.post.await_args_list
        self.assertIn("stream_v2_active", calls[1].args[0])
        self.assertEqual(calls[1].kwargs["content"], calls[0].kwargs["content"])

    def test_a_409_is_not_retried_and_moves_on(self) -> None:
        # 409 means "I am not the active writer", which a fresh socket does not
        # change. Retrying it would double the cost of every failover.
        sink = build_sink(self.ACTIVE)
        sink.client.post = mock.AsyncMock(return_value=FakeResponse(409))
        with self.assertRaises(RuntimeError) as caught:
            self._publish(sink)
        self.assertEqual(sink.client.post.await_count, 2)
        self.assertIn("unavailable", str(caught.exception))

    def test_a_503_is_not_retried_and_moves_on(self) -> None:
        sink = build_sink(self.ACTIVE)
        sink.client.post = mock.AsyncMock(return_value=FakeResponse(503))
        with self.assertRaises(RuntimeError):
            self._publish(sink)
        self.assertEqual(sink.client.post.await_count, 2)

    def test_a_contract_violation_is_not_retried(self) -> None:
        # A malformed ACK is a contract break, not a broken socket. Retrying it
        # would hide a real protocol divergence behind a duplicate request.
        sink = build_sink(("https://one:8200",))
        sink.client.post = mock.AsyncMock(
            return_value=FakeResponse(200, {"schema": "wrong", "results": []})
        )
        with self.assertRaises(RuntimeError):
            self._publish(sink)
        self.assertEqual(sink.client.post.await_count, 1)

    def test_a_500_is_not_retried(self) -> None:
        sink = build_sink(("https://one:8200",))
        sink.client.post = mock.AsyncMock(return_value=FakeResponse(500))
        with self.assertRaises(RuntimeError) as caught:
            self._publish(sink)
        self.assertEqual(sink.client.post.await_count, 1)
        self.assertIn("rejected", str(caught.exception))

    def test_a_read_timeout_is_a_transport_error_and_is_retried(self) -> None:
        sink = build_sink(("https://one:8200",))
        sink.client.post = mock.AsyncMock(side_effect=httpx.ReadTimeout("slow"))
        with self.assertRaises(RuntimeError):
            self._publish(sink)
        self.assertEqual(sink.client.post.await_count, 2)

    def test_the_retry_is_bounded_to_one_per_url(self) -> None:
        # Never more than two attempts per URL, so a gateway that is genuinely
        # gone costs a bounded amount of time.
        sink = build_sink(("https://one:8200",))
        sink.client.post = mock.AsyncMock(side_effect=httpx.ConnectError("closed"))
        with self.assertRaises(RuntimeError):
            self._publish(sink)
        self.assertEqual(sink.client.post.await_count, 2)


if __name__ == "__main__":
    unittest.main()
