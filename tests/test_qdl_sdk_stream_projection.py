from __future__ import annotations

import unittest

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl_sdk import (
    DataRequirement,
    Feed,
    Grade,
    StreamEvent,
    market_data_view_from_stream,
)
from qdl_sdk.errors import ContinuityError
from qdl_sdk.models import MarketDataView

NOW = 1_800_000_000_000_000_000
DIGEST = "a" * 64


def dec(value: int, scale: int = 2):
    return common_pb2.DecimalValue(mantissa=value, scale=scale, source_text=str(value))


def payload_fixture(feed: Feed) -> dict:
    common = {"feed": feed.value}
    return {
        Feed.TRADE: {
            **common,
            "native_trade_id": "1",
            "price": dv(),
            "quantity": dv(),
            "quantity_unit": "BASE_ASSET",
            "aggressor_side": "BUY",
            "identity_kind": "NATIVE",
        },
        Feed.QUOTE: {
            **common,
            "bid_price": dv(),
            "bid_quantity": dv(),
            "ask_price": dv(101),
            "ask_quantity": dv(),
            "quantity_unit": "BASE_ASSET",
            "level": 1,
        },
        Feed.BAR: {
            **common,
            "interval": "1m",
            "open_time_ns": NOW - 60_000_000_000,
            "close_time_ns": NOW - 1,
            "open": dv(),
            "high": dv(102),
            "low": dv(99),
            "close": dv(101),
            "volume": dv(),
            "volume_unit": "BASE_ASSET",
            "trade_count": 1,
            "lifecycle": "FINAL",
            "revision": 0,
            "origin": "VENUE_NATIVE",
        },
        Feed.BOOK_SNAPSHOT: {
            **common,
            "native_sequence": "1",
            "levels": [level()],
            "depth": 1,
        },
        Feed.BOOK_DELTA: {
            **common,
            "native_sequence_start": "1",
            "native_sequence_end": "2",
            "snapshot_sequence": "1",
            "updates": [level()],
            "reset": False,
        },
        Feed.FUNDING_RATE: {**common, "rate": dv(), "funding_time_ns": NOW},
        Feed.OPEN_INTEREST: {**common, "quantity": dv(), "quantity_unit": "CONTRACT"},
        Feed.MARK_INDEX_PRICE: {**common, "mark_price": dv(), "index_price": dv()},
        Feed.TICKER: {**common, "last_price": dv()},
    }[feed]


def dv(value: int = 100) -> dict:
    return {"coefficient": str(value), "scale": 2, "source_text": str(value)}


def level() -> dict:
    return {
        "side": "BID",
        "price": dv(),
        "quantity": dv(),
        "quantity_unit": "BASE_ASSET",
        "order_count": 1,
    }


def template(feed: Feed) -> MarketDataView:
    return MarketDataView.model_validate(
        {
            "instrument_uid": "uid-1",
            "instrument_id": "BINANCE.USDM.PERPETUAL.BTC-USDT",
            "instrument_revision": 7,
            "feed": feed.value,
            "interval": "1m" if feed is Feed.BAR else None,
            "observed_at_ns": NOW,
            "revision": 0,
            "payload": payload_fixture(feed),
            "source": {
                "venue": "BINANCE",
                "provider": "BINANCE_DIRECT",
                "source_id": "source-1",
                "source_role": "PRIMARY",
                "authoritative": True,
            },
            "quality": {
                "state": "LIVE",
                "freshness_ms": 1,
                "gap_open": False,
                "complete": True,
                "execution_eligible": True,
                "policy_id": "crypto_primary_v2",
                "flags": [],
            },
            "contract": {
                "schema_digest": DIGEST,
                "contract_version": "2.0.0",
                "normalizer_version": "old",
                "adapter_version": "old",
                "instrument_catalog_revision": 7,
                "source_policy_revision": 2,
                "authority_revision": 3,
                "config_revision": 1,
                "correlation_id": "snapshot",
            },
            "watermark_offset": 10,
        }
    )


def envelope(feed: Feed) -> market_data_pb2.EventEnvelope:
    result = market_data_pb2.EventEnvelope(
        schema_name="qdl.marketdata.v2.EventEnvelope",
        schema_major=2,
        schema_minor=0,
        event_id=b"event-1",
        instrument_uid="uid-1",
        instrument_id="BINANCE.USDM.PERPETUAL.BTC-USDT",
        instrument_revision=7,
        venue="BINANCE",
        market="USDM",
        product_type="PERPETUAL",
        native_symbol="BTCUSDT",
        provider="BINANCE_DIRECT",
        source_id="source-1",
        source_role=common_pb2.SOURCE_ROLE_PRIMARY,
        source_event_time_ns=NOW,
        normalizer_version="rust-v2",
        adapter_version="binance-v2",
        correlation_id="stream",
        config_revision=2,
        authority_revision=3,
    )
    if feed is Feed.TRADE:
        result.trade.CopyFrom(
            market_data_pb2.Trade(
                native_trade_id="1",
                price=dec(100),
                quantity=dec(2),
                aggressor_side=common_pb2.AGGRESSOR_SIDE_BUY,
                quantity_unit=common_pb2.QUANTITY_UNIT_BASE_ASSET,
                identity_kind=market_data_pb2.TRADE_IDENTITY_KIND_NATIVE,
            )
        )
    elif feed is Feed.QUOTE:
        result.quote.CopyFrom(
            market_data_pb2.Quote(
                bid_price=dec(100),
                bid_quantity=dec(2),
                ask_price=dec(101),
                ask_quantity=dec(3),
                level=1,
                quantity_unit=common_pb2.QUANTITY_UNIT_BASE_ASSET,
            )
        )
    elif feed is Feed.BAR:
        result.bar.CopyFrom(
            market_data_pb2.Bar(
                interval="1m",
                open_time_ns=NOW - 60_000_000_000,
                close_time_ns=NOW - 1,
                open=dec(100),
                high=dec(102),
                low=dec(99),
                close=dec(101),
                volume=dec(4),
                trade_count=2,
                is_final=True,
                revision=0,
                origin=common_pb2.BAR_ORIGIN_VENUE_NATIVE,
                lifecycle=market_data_pb2.BAR_LIFECYCLE_FINAL,
                volume_unit=common_pb2.QUANTITY_UNIT_BASE_ASSET,
            )
        )
    elif feed is Feed.BOOK_SNAPSHOT:
        result.book_snapshot.CopyFrom(
            market_data_pb2.OrderBookSnapshot(
                native_sequence="1",
                levels=[
                    market_data_pb2.BookLevel(
                        side=common_pb2.BOOK_SIDE_BID,
                        price=dec(100),
                        quantity=dec(2),
                        order_count=1,
                        quantity_unit=common_pb2.QUANTITY_UNIT_BASE_ASSET,
                    )
                ],
                depth=1,
            )
        )
    elif feed is Feed.BOOK_DELTA:
        result.book_delta.CopyFrom(
            market_data_pb2.OrderBookDelta(
                native_sequence_start="1",
                native_sequence_end="2",
                snapshot_sequence="1",
                updates=[
                    market_data_pb2.BookLevel(
                        side=common_pb2.BOOK_SIDE_ASK,
                        price=dec(101),
                        quantity=dec(2),
                        order_count=1,
                        quantity_unit=common_pb2.QUANTITY_UNIT_BASE_ASSET,
                    )
                ],
            )
        )
    elif feed is Feed.FUNDING_RATE:
        result.funding_rate.CopyFrom(
            market_data_pb2.FundingRate(rate=dec(1, 4), funding_time_ns=NOW)
        )
    elif feed is Feed.OPEN_INTEREST:
        result.open_interest.CopyFrom(
            market_data_pb2.OpenInterest(
                quantity=dec(10), quantity_unit=common_pb2.QUANTITY_UNIT_CONTRACT
            )
        )
    elif feed is Feed.MARK_INDEX_PRICE:
        result.mark_index_price.CopyFrom(
            market_data_pb2.MarkIndexPrice(mark_price=dec(100), index_price=dec(99))
        )
    elif feed is Feed.TICKER:
        result.ticker.CopyFrom(market_data_pb2.Ticker(last_price=dec(100)))
    return result


class SdkStreamProjectionTests(unittest.TestCase):
    def requirement(self, feed: Feed) -> DataRequirement:
        return DataRequirement(
            instrument_uid="uid-1",
            feed=feed,
            consumer_grade=Grade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            interval="1m" if feed is Feed.BAR else None,
            max_freshness_ms=1000,
        )

    def test_all_public_market_payloads_project_to_typed_view(self):
        for feed in Feed:
            with self.subTest(feed=feed):
                result = market_data_view_from_stream(
                    StreamEvent(11, "signed", envelope(feed)),
                    template=template(feed),
                    requirement=self.requirement(feed),
                    now_ns=NOW + 100_000_000,
                )
                self.assertIs(result.feed, feed)
                self.assertEqual(result.watermark_offset, 11)
                self.assertEqual(result.cursor, "signed")
                self.assertTrue(result.quality.execution_eligible)

    def test_gap_and_stale_execution_events_fail_closed(self):
        gapped = envelope(Feed.TRADE)
        gapped.quality_flags.append(common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE)
        with self.assertRaises(ContinuityError) as gap:
            market_data_view_from_stream(
                StreamEvent(11, "signed", gapped),
                template=template(Feed.TRADE),
                requirement=self.requirement(Feed.TRADE),
                now_ns=NOW + 1,
            )
        self.assertEqual(gap.exception.code, "OPEN_SEQUENCE_GAP")
        with self.assertRaises(ContinuityError) as stale:
            market_data_view_from_stream(
                StreamEvent(11, "signed", envelope(Feed.TRADE)),
                template=template(Feed.TRADE),
                requirement=self.requirement(Feed.TRADE),
                now_ns=NOW + 2_000_000_000,
            )
        self.assertEqual(stale.exception.code, "DATA_STALE")

    def test_source_transition_and_revision_regression_require_snapshot(self):
        changed = envelope(Feed.TRADE)
        changed.source_id = "source-2"
        with self.assertRaises(ContinuityError) as source:
            market_data_view_from_stream(
                StreamEvent(11, "signed", changed),
                template=template(Feed.TRADE),
                requirement=self.requirement(Feed.TRADE),
                now_ns=NOW,
            )
        self.assertEqual(source.exception.code, "SOURCE_NON_AUTHORITATIVE")
        changed = envelope(Feed.TRADE)
        changed.authority_revision = 2
        with self.assertRaises(ContinuityError) as authority:
            market_data_view_from_stream(
                StreamEvent(11, "signed", changed),
                template=template(Feed.TRADE),
                requirement=self.requirement(Feed.TRADE),
                now_ns=NOW,
            )
        self.assertEqual(authority.exception.code, "SOURCE_NON_AUTHORITATIVE")


if __name__ == "__main__":
    unittest.main()
