from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import json
import pathlib
import unittest
from unittest.mock import patch

from qdl.adapters.binance_usdm import BinanceUsdmSupervisor
from qdl.adapters.okx.client import OkxSubscription, OkxWebSocketSupervisor
from qdl.canonical.book import canonicalize_deribit_option_book_fixture
from qdl.canonical.market import canonicalize_dnse_bar
from qdl.canonical.trade import TradeContext
from qdl.common.v1 import common_pb2
from qdl.domain.instrument import InstrumentIdentity, ProductType
from qdl.ingestion.contracts import ConnectionShard, FeedType, Subscription
from qdl.raw.capture import bind_capture_context, capture_exact_frame, derive_capture_id


ROOT = pathlib.Path(__file__).resolve().parents[1]


def base_context(*, venue: str, market: str, product: str, symbol: str) -> TradeContext:
    identity = InstrumentIdentity.create(
        venue=venue, market=market, product_type=product, canonical_symbol=symbol
    )
    return TradeContext(
        instrument_uid=identity.instrument_uid,
        instrument_id=identity.instrument_id,
        instrument_revision=1,
        venue=venue,
        market=market,
        product_type=product,
        native_symbol=symbol,
        provider=f"{venue}_DIRECT",
        source_id="phase82-test",
        lease_epoch=1,
        received_at_ns=1,
        normalized_at_ns=2,
        published_at_ns=3,
        partition_sequence=1,
        normalizer_version="phase82/1",
        adapter_version="phase82/1",
        config_revision=1,
    )


class Phase82CaptureContractTests(unittest.TestCase):
    def test_capture_id_is_deterministic_and_receive_time_scoped(self):
        first = derive_capture_id(
            source_session_id="session", connection_generation=1,
            received_at_ns=10, raw_frame_bytes=b"frame",
        )
        self.assertEqual(first, derive_capture_id(
            source_session_id="session", connection_generation=1,
            received_at_ns=10, raw_frame_bytes=b"frame",
        ))
        self.assertNotEqual(first, derive_capture_id(
            source_session_id="session", connection_generation=1,
            received_at_ns=11, raw_frame_bytes=b"frame",
        ))
        self.assertEqual(len(first), 16)

    def test_dnse_missing_trade_count_is_explicit_not_plausible_default(self):
        raw = {
            "symbol": "VN30F1M", "interval": "1m",
            "open_time_ms": 1_000, "close_time_ms": 60_999,
            "o": "1800.1", "h": "1801.2", "l": "1799.8",
            "c": "1800.9", "v": "12", "is_final": True,
            "trade_count_available": False, "revision": 0,
        }
        captured = capture_exact_frame(
            provider="DNSE_DIRECT", venue="DNSE", market="VN_DERIVATIVES",
            product_type="FUTURE", native_symbol="VN30F1M", native_channel="ohlcv/1m",
            subscription_id="dnse-test", source_session_id="dnse-session",
            connection_generation=1, lease_epoch=1, authority_revision=1,
            partition_plan_epoch=1, received_at_ns=100,
            raw_frame_bytes=json.dumps(raw, sort_keys=True).encode(),
            adapter_version="dnse/2", config_revision=1,
            instrument_catalog_revision=1, correlation_id="phase82", test_provenance=True,
            transport_protocol=3, capture_boundary=3,
        )
        context = bind_capture_context(
            base_context(
                venue="DNSE", market="VN_DERIVATIVES", product="FUTURE",
                symbol="VN30F1M",
            ),
            captured,
        )
        event = canonicalize_dnse_bar(raw, context)
        self.assertEqual(event.bar.trade_count, 0)
        self.assertIn(common_pb2.QUALITY_FLAG_FIELD_MISSING, event.quality_flags)
        self.assertEqual(event.raw_capture_id, captured.capture_id)

    def test_deribit_extension_fixture_cannot_claim_live_provenance(self):
        raw = json.loads(
            (ROOT / "tests/fixtures/phase3/deribit_option_book.json").read_text()
        )
        context = base_context(
            venue="DERIBIT", market="OPTIONS", product="OPTION",
            symbol="BTC-30JUN26-60000-C",
        )
        raw["provenance"] = "REAL_PROVIDER"
        with self.assertRaisesRegex(ValueError, "cannot accept live provenance"):
            canonicalize_deribit_option_book_fixture(raw, context)


class Phase82AtomicTeeTests(unittest.IsolatedAsyncioTestCase):
    async def test_binance_exact_callback_receives_same_raw_and_parsed_frame(self):
        raw = b'{"stream":"btcusdt@trade","data":{"e":"trade","s":"BTCUSDT","t":7,"p":"1","q":"2","T":3,"m":false}}'

        class Socket:
            async def recv(self):
                return raw

        class Connection:
            async def __aenter__(self):
                return Socket()

            async def __aexit__(self, *_):
                return False

        seen = []

        async def exact(frame_bytes, stream, frame, received_at_ns):
            seen.append((frame_bytes, stream, frame, received_at_ns))

        supervisor = BinanceUsdmSupervisor(
            on_frame=lambda *_: asyncio.sleep(0), on_exact_frame=exact,
        )
        subscription = Subscription("BINANCE", "USDM", FeedType.TRADE, "BTCUSDT")
        with patch("websockets.asyncio.client.connect", return_value=Connection()):
            count = await supervisor.run(
                ConnectionShard("s", "BINANCE", "USDM", FeedType.TRADE, (subscription,), 1),
                active_symbols={"BTCUSDT"}, stop=asyncio.Event(), max_events=1,
            )
        self.assertEqual(count, 1)
        self.assertEqual(seen[0][0], raw)
        self.assertEqual(seen[0][1], "btcusdt@trade")
        self.assertEqual(seen[0][2]["t"], 7)

    async def test_okx_exact_callback_covers_ack_and_data_without_reparse_race(self):
        messages = iter((
            '{"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT-SWAP"}}',
            '{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"7","px":"1","sz":"2","side":"buy","ts":"3"}]}',
        ))

        class Socket:
            async def send(self, _):
                return None

            async def recv(self):
                return next(messages)

        class Connection:
            async def __aenter__(self):
                return Socket()

            async def __aexit__(self, *_):
                return False

        seen = []

        async def exact(raw_bytes, payload, generation, received_at_ns):
            seen.append((raw_bytes, payload, generation, received_at_ns))

        supervisor = OkxWebSocketSupervisor(
            on_frame=lambda *_: asyncio.sleep(0), on_exact_frame=exact,
        )
        with patch("websockets.asyncio.client.connect", return_value=Connection()):
            count = await supervisor.run(
                (OkxSubscription("trades", "BTC-USDT-SWAP"),),
                stop=asyncio.Event(), max_events=1,
            )
        self.assertEqual(count, 1)
        self.assertEqual(len(seen), 2)
        self.assertEqual(seen[0][1]["event"], "subscribe")
        self.assertEqual(seen[1][1]["data"][0]["tradeId"], "7")
        self.assertEqual(seen[0][2], seen[1][2])


class Phase82EvidenceTests(unittest.TestCase):
    def test_real_capture_bundle_is_bounded_checksummed_and_not_synthetic(self):
        evidence = json.loads(
            (ROOT / "upgrade/evidence/phase8-real-provider-shadow.json").read_text()
        )
        path = ROOT / evidence["capture_bundle"]
        compressed = path.read_bytes()
        self.assertEqual(hashlib.sha256(compressed).hexdigest(), evidence["capture_bundle_sha256"])
        payload = json.loads(gzip.decompress(compressed))
        self.assertEqual(payload["provenance"], "REAL_PROVIDER_READ_ONLY")
        self.assertEqual(payload["production_writes"], 0)
        self.assertEqual(len(payload["captures"]), 497)
        for item in payload["captures"]:
            self.assertFalse(item["test_provenance"])
            raw = base64.b64decode(item["raw_frame_base64"], validate=True)
            self.assertEqual(hashlib.sha256(raw).hexdigest(), item["raw_frame_sha256"])
        self.assertTrue(payload["fixture_only_deribit"]["test_provenance"])

    def test_cross_language_capacity_and_authority_gates_pass(self):
        parity = json.loads(
            (ROOT / "upgrade/evidence/phase8-python-rust-parity.json").read_text()
        )
        capacity = json.loads(
            (ROOT / "upgrade/evidence/phase8-capacity.json").read_text()
        )
        conformance = json.loads(
            (ROOT / "upgrade/evidence/phase8-cross-venue-conformance.json").read_text()
        )
        self.assertEqual(parity["events"], 99_600)
        self.assertEqual(parity["record_mismatches"], 0)
        self.assertEqual(parity["process_restart_mismatches"], 0)
        self.assertTrue(capacity["thresholds_pass"])
        self.assertEqual(conformance["authority"], "RUST_SHADOW")
        self.assertEqual(conformance["public_or_legacy_writes"], 0)
        self.assertFalse(conformance["deribit_live_certified"])


if __name__ == "__main__":
    unittest.main()
