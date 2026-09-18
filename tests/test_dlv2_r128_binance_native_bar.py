"""R1.28: Binance USD-M 1m bars move to the native `/market` kline lane.

Binance USD-M BAR had been pinned to `PYTHON_REST` by a comment in
`production_catalog.py` saying the platform had never proven "final kline
delivery after a valid WS ACK". It could not have proven it: the ingestor was
dialling the base Binance decommissioned on 2026-04-23, so a `@kline_*`
subscription was ACKed and then pushed nothing. R1.27 routed the lanes and
`scripts/certify_binance_native_bar_admission.py` measured 15 of 15 final
klines across the five symbols, 0.043-1.144 s after their own close, carrying
values REST only converges onto by +6 s.

These tests pin the three decisions that follow, each against the generator or
the rule itself rather than against a hand-written copy of it:

1. only USD-M 1m moves, and it moves onto the routed `/market` pair;
2. every other Binance BAR interval stays on the REST edge;
3. a `RUST_NATIVE` binding is not polled by the bar edge after bootstrap, for
   either venue - a rule the operator docs described as OKX-only for weeks.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import yaml

from qdl.adapters.binance_usdm import parse_exchange_info
from qdl.runtime.production_catalog import (
    ProductionCatalogBuilder,
    ProductionDemandManifest,
)
from qdl.runtime.stable_bar_edge import recurring_rest_bar_bindings
from qdl.runtime.stable_deployment import binance_route_for_channel

PUBLIC_BASE = "wss://fstream.binance.com/public/ws"
MARKET_BASE = "wss://fstream.binance.com/market/ws"

EXCHANGE_INFO = {
    "serverTime": 1000,
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "contractType": "PERPETUAL",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "deliveryDate": 0,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001"},
            ],
        },
    ],
}

# Every interval the stack carries, so "1m only" is pinned against the whole
# set rather than against the two or three an author happened to think of.
ALL_INTERVALS = (
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h",
    "6h", "8h", "12h", "1d", "3d", "1w",
)


def _bar_bindings(intervals: tuple[str, ...]) -> list[dict]:
    # A BAR requirement carries exactly the required fields; the generator
    # refuses freshness/liveness keys on anything that is not a book or a
    # mark/index demand.
    feeds = [{"feed": "BAR", "interval": interval} for interval in intervals]
    payload = {
        "schema": "qdl.v2.production-demand.v1",
        "revision": 5,
        "consumers": [
            {
                "consumer_id": "trading-system.execution.v2",
                "consumer_grade": "EXECUTION",
                "requirements": [
                    {
                        "venue": "BINANCE",
                        "market": "USDM",
                        "product_type": "PERPETUAL",
                        "native_symbol": "BTCUSDT",
                        "source_policy_id": "crypto_primary_v2",
                        **feed,
                    }
                    for feed in feeds
                ],
            }
        ],
    }
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "demand.yaml"
        path.write_text(yaml.safe_dump(payload, sort_keys=False))
        bundle = ProductionCatalogBuilder(
            catalog_revision=8, source_policy_revision=3, authority_revision=11
        ).build(
            demand=ProductionDemandManifest.load_many([path]),
            binance_usdm=parse_exchange_info(EXCHANGE_INFO, valid_from_ns=99),
            okx_rows=[],
            metadata_provenance={"capture": "a" * 64},
        )
    return [
        item
        for item in bundle.acquisition_plan["bindings"]
        if item["runtime"] == "BINANCE" and "-bar-" in item["binding_id"]
    ]


class BinanceNativeBarScopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bindings = _bar_bindings(ALL_INTERVALS)
        self.by_interval = {
            item["binding_id"].rsplit("-", 1)[-1]: item for item in self.bindings
        }

    def test_every_declared_interval_produced_a_binding(self):
        self.assertEqual(set(self.by_interval), set(ALL_INTERVALS))

    def test_only_1m_is_native(self):
        native = sorted(
            interval
            for interval, item in self.by_interval.items()
            if item["mode"] == "RUST_NATIVE"
        )
        self.assertEqual(native, ["1m"], "R1.28 admits one interval, not a batch")

    def test_the_native_1m_binding_is_a_routed_market_kline(self):
        item = self.by_interval["1m"]
        self.assertEqual(item["mode"], "RUST_NATIVE")
        self.assertEqual(item["provider_kind"], "binance_usdm_bar")
        self.assertEqual(item["native_channel"], "btcusdt@kline_1m")
        self.assertEqual(item["sequence_policy"], "NONE")
        self.assertEqual(item["websocket_url"], PUBLIC_BASE)
        self.assertEqual(item["market_websocket_url"], MARKET_BASE)
        # A kline is a `/market` stream. If this ever answers "public" the lane
        # is ACKed and silent, which is the failure this whole program exists
        # to have ended.
        self.assertEqual(binance_route_for_channel(item["native_channel"]), "market")

    def test_every_other_interval_stays_on_the_rest_edge(self):
        for interval, item in sorted(self.by_interval.items()):
            if interval == "1m":
                continue
            self.assertEqual(item["mode"], "PYTHON_REST", interval)
            self.assertEqual(item["provider_kind"], "binance_usdm_rest_bar", interval)
            self.assertEqual(item["native_channel"], f"rest-klines/{interval}", interval)
            self.assertIsNone(item["websocket_url"], interval)
            self.assertIsNone(item["market_websocket_url"], interval)

    def test_the_canonicaliser_named_by_the_binding_exists_for_this_kind(self):
        """`binance_usdm_bar` is what the Rust dispatch matches on.

        A binding naming a kind the core does not know quarantines every frame
        it produces, and the lane looks alive from the ingestor's side.
        """
        dispatch = (
            Path(__file__).resolve().parents[1] / "rust/qdl-core/src/canonical.rs"
        ).read_text(encoding="utf-8")
        self.assertIn('"binance_usdm_bar" | "binance_spot_bar"', dispatch)


class BarEdgeOwnershipTests(unittest.TestCase):
    """The bar edge must not poll a binding its Rust lane owns."""

    @staticmethod
    def _pair(binding_id: str, mode: str, runtime: str = "BINANCE") -> tuple:
        return (
            SimpleNamespace(binding_id=binding_id),
            SimpleNamespace(binding_id=binding_id, mode=mode, runtime=runtime),
        )

    def test_a_native_binance_binding_is_not_polled(self):
        pairs = (
            self._pair("binance-usdm-btcusdt-bar-1m", "RUST_NATIVE"),
            self._pair("binance-usdm-btcusdt-bar-5m", "PYTHON_REST"),
        )
        polled = [pair[1].binding_id for pair in recurring_rest_bar_bindings(pairs)]
        self.assertEqual(polled, ["binance-usdm-btcusdt-bar-5m"])

    def test_the_rule_is_the_same_for_okx(self):
        """Pinned because the operator docs said it applied to OKX only.

        `scripts/verify_runtime_generations.py` carried that claim; the code has
        filtered both venues since 2026-08-25. A test is what keeps the two from
        drifting again.
        """
        pairs = (
            self._pair("okx-swap-btcusdt-bar-1m", "RUST_NATIVE", runtime="OKX"),
            self._pair("okx-swap-btcusdt-bar-5m", "PYTHON_REST", runtime="OKX"),
        )
        polled = [pair[1].binding_id for pair in recurring_rest_bar_bindings(pairs)]
        self.assertEqual(polled, ["okx-swap-btcusdt-bar-5m"])

    def test_the_deployed_plan_moves_exactly_five_bindings_out_of_the_poll(self):
        """Against the shipped config, not a fixture.

        Five symbols, 1m, USD-M. If a regeneration ever moves more than that,
        the bar edge stops polling bars nothing else publishes yet.
        """
        root = Path(__file__).resolve().parents[1]
        plan = yaml.safe_load(
            (root / "config/v2/stable-acquisition-bindings.yaml").read_text(
                encoding="utf-8"
            )
        )
        bars = [
            item
            for item in plan["bindings"]
            if "-bar-" in item["binding_id"] and item.get("enabled", True)
        ]
        native = sorted(
            item["binding_id"] for item in bars if item["mode"] == "RUST_NATIVE"
        )
        binance_native = [item for item in native if item.startswith("binance-usdm-")]
        self.assertEqual(
            binance_native,
            [
                "binance-usdm-bnbusdt-bar-1m",
                "binance-usdm-btcusdt-bar-1m",
                "binance-usdm-dogeusdt-bar-1m",
                "binance-usdm-ethusdt-bar-1m",
                "binance-usdm-solusdt-bar-1m",
            ],
        )


class CaptureChannelTests(unittest.TestCase):
    """A REST row for a native binding must be captured on the native channel.

    The core routes a raw envelope to a binding by its `native_channel`, and one
    binding has one channel. The bar edge still bootstraps and repairs warmup
    history over REST for a `RUST_NATIVE` binding, so those rows have to arrive
    on the channel the core registered or every one of them is quarantined as
    FencingRejected - which is exactly what happened on 2026-09-18 between the
    core roll and this fix: 1,447 to 3,486 quarantines per core, and the 1m
    warmup repair silently publishing into nothing.
    """

    @staticmethod
    def _binding(channel: str | None):
        from qdl.adapters.binance.bar_edge import BinanceBarRawBinding

        return BinanceBarRawBinding(
            market="USDM",
            product_type="PERPETUAL",
            native_symbol="BTCUSDT",
            interval="1m",
            subscription_id="binance-usdm-btcusdt-bar-stable-001",
            source_session_id="qdl-v2-stable-binance-rest-r1-g1",
            connection_generation=1,
            lease_epoch=1,
            authority_revision=1,
            partition_plan_epoch=1,
            adapter_version="binance-usdm/2.0.0",
            config_revision=17,
            instrument_catalog_revision=8,
            native_channel=channel,
        )

    def test_a_rest_binding_keeps_the_rest_channel(self):
        self.assertEqual(self._binding(None).capture_channel, "rest-klines/1m")

    def test_a_native_binding_captures_on_its_websocket_channel(self):
        self.assertEqual(
            self._binding("btcusdt@kline_1m").capture_channel, "btcusdt@kline_1m"
        )

    def test_the_capture_envelope_carries_that_channel(self):
        from qdl.adapters.binance import bar_edge

        envelope = bar_edge._capture_row(
            self._binding("btcusdt@kline_1m"),
            [1786352340000, "61200.00", "61240.00", "61190.00", "61234.10", "12.500",
             1786352399999, "765200.00", 11, "0", "0", "0"],
            origin="BACKFILLED",
            received_at_ns=1_786_352_400_123_456_000,
            test_provenance=False,
        )
        self.assertEqual(envelope.native_channel, "btcusdt@kline_1m")


if __name__ == "__main__":
    unittest.main()
