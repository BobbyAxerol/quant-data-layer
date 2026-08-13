from __future__ import annotations

import asyncio
import unittest
from unittest.mock import Mock, patch

from qdl.adapters.okx.client import (
    AsyncTokenBucket,
    BookState,
    OkxOrderBook,
    OkxRestClient,
    OkxSubscription,
    OkxWebSocketSupervisor,
    _receive_until_stop,
)
from qdl.canonical.book import canonicalize_okx_book
from qdl.canonical.trade import TradeContext


def snapshot(book: OkxOrderBook, *, sequence: int = 10) -> dict:
    row = {"seqId": sequence, "prevSeqId": -1,
           "bids": [["100", "2", "0", "1"]],
           "asks": [["101", "3", "0", "1"]], "checksum": 0}
    return {"arg": {"instId": book.inst_id}, "action": "snapshot", "data": [row]}


class OkxBookTests(unittest.TestCase):
    def test_snapshot_update_then_true_gap_invalidates_all_executable_state(self):
        book = OkxOrderBook("BTC-USDT-SWAP")
        book.reconnect(1)
        self.assertTrue(book.apply_ws(snapshot(book), generation=1))
        self.assertEqual(book.state, BookState.LIVE)
        update = {"arg": {"instId": book.inst_id}, "action": "update", "data": [{
            "prevSeqId": 10, "seqId": 11, "bids": [["100", "0", "0", "1"]],
            "asks": [["102", "1", "0", "1"]],
        }]}
        self.assertTrue(book.apply_ws(update, generation=1))
        self.assertNotIn("100", book.bids)
        gap = {"arg": {"instId": book.inst_id}, "action": "update", "data": [{
            "prevSeqId": 99, "seqId": 100, "bids": [], "asks": [],
        }]}
        self.assertFalse(book.apply_ws(gap, generation=1))
        self.assertEqual(book.state, BookState.GAPPED)
        self.assertEqual(book.bids, {})
        self.assertEqual(book.asks, {})

    def test_stale_connection_generation_and_rest_bridge_are_rejected(self):
        book = OkxOrderBook("BTC-USDT-SWAP")
        book.reconnect(2)
        self.assertFalse(book.apply_ws(snapshot(book), generation=1))
        with self.assertRaisesRegex(RuntimeError, "cannot establish"):
            book.apply_rest_snapshot({"bids": [], "asks": []})

    def test_deprecated_fixed_checksum_is_not_used_as_crc(self):
        book = OkxOrderBook("BTC-USDT-SWAP")
        book.reconnect(1)
        frame = snapshot(book)
        self.assertTrue(book.apply_ws(frame, generation=1))
        self.assertEqual(book.state, BookState.LIVE)

    def test_validated_ws_snapshot_maps_to_exact_canonical_book(self):
        book = OkxOrderBook("BTC-USDT-SWAP")
        book.reconnect(1)
        frame = snapshot(book)
        frame["data"][0]["ts"] = "1786352400125"
        self.assertTrue(book.apply_ws(frame, generation=1))
        context = TradeContext(
            instrument_uid="uid", instrument_id="OKX.SWAP.PERPETUAL.BTC-USDT",
            instrument_revision=1, venue="OKX", market="SWAP",
            product_type="PERPETUAL", native_symbol="BTC-USDT-SWAP",
            provider="OKX_DIRECT", source_id="okx-books-shadow-1", lease_epoch=2,
            received_at_ns=10, normalized_at_ns=11, published_at_ns=12,
            partition_sequence=3, normalizer_version="qdl/2",
            adapter_version="okx-v5/1", config_revision=4,
        )
        event = canonicalize_okx_book(frame, context)
        self.assertEqual(event.source_sequence, "10")
        self.assertEqual(event.source_event_time_ns, 1_786_352_400_125_000_000)
        self.assertEqual(event.book_snapshot.levels[0].price.source_text, "100")


class OkxRateAndRestTests(unittest.IsolatedAsyncioTestCase):
    async def test_endpoint_bucket_waits_instead_of_exceeding_budget(self):
        clock = [0.0]
        bucket = AsyncTokenBucket(capacity=1, refill_per_second=10, clock=lambda: clock[0])
        await bucket.acquire()
        waiting = asyncio.create_task(bucket.acquire())
        await asyncio.sleep(0)
        self.assertFalse(waiting.done())
        clock[0] = 1.0
        await asyncio.sleep(0.11)
        await waiting

    @patch("qdl.adapters.okx.client.requests.get")
    async def test_v5_envelope_validation_and_provider_bytes_only(self, get: Mock):
        response = get.return_value
        response.raise_for_status.return_value = None
        response.json.return_value = {"code": "0", "msg": "", "data": [{"tradeId": "7"}]}
        rows = await OkxRestClient().trades("BTC-USDT-SWAP", limit=1)
        self.assertEqual(rows, [{"tradeId": "7"}])
        self.assertEqual(get.call_args.kwargs["params"]["instId"], "BTC-USDT-SWAP")


class OkxSupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_storm_is_bounded_and_stop_is_honored(self):
        stop = asyncio.Event()
        attempts = 0

        class FailingConnection:
            async def __aenter__(self):
                nonlocal attempts
                attempts += 1
                if attempts == 3:
                    stop.set()
                raise OSError("isolated reconnect test")

            async def __aexit__(self, *_):
                return False

        supervisor = OkxWebSocketSupervisor(
            on_frame=lambda *_: asyncio.sleep(0), max_backoff_seconds=0
        )
        with patch("websockets.asyncio.client.connect", return_value=FailingConnection()):
            self.assertEqual(
                await supervisor.run(
                    (OkxSubscription("trades", "BTC-USDT-SWAP"),), stop=stop
                ),
                0,
            )
        self.assertEqual(attempts, 3)

    async def test_stop_interrupts_blocked_receive(self):
        stop = asyncio.Event()

        class BlockingSocket:
            async def recv(self):
                await asyncio.Event().wait()

        task = asyncio.create_task(_receive_until_stop(BlockingSocket(), stop, 60))
        await asyncio.sleep(0)
        stop.set()
        self.assertIsNone(await asyncio.wait_for(task, timeout=0.2))


if __name__ == "__main__":
    unittest.main()
