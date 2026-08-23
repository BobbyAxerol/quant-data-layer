from __future__ import annotations

import asyncio
import copy
import io
import json
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import AsyncMock, patch
from pathlib import Path

import yaml

from qdl.adapters.vn import build_dnse_bar_raw_envelope
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge
from qdl.runtime.stable_vn_edge import StableDnseVendorEdge
from qdl.runtime.stable_catalog import StableSourceCatalog
from scripts.build_production_core_bundle import main as build_production_core_bundle_main
from scripts.phaseb_prepare_stable_candidate import prepare_candidate
from scripts.phasec1_isolated_consumer_acceptance import (
    CASES as C39_ACCEPTANCE_CASES,
    token_claims as c39_token_claims,
)

from qdl.runtime.stable_deployment import (
    STABLE_CORE_WORKER_COUNT,
    AuthorityPromotionScope,
    StableAcquisitionPlan,
    stable_authority_record,
    write_stable_runtime_bundle,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
PROMOTION_SCOPE_PATH = ROOT / "config/v2/stable-authority-promotion-scope.yaml"


class StableDeploymentContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.acquisition = StableAcquisitionPlan.load(
            ACQUISITION_PATH, catalog=self.catalog
        )
        self.promotion_scope = AuthorityPromotionScope.load(
            PROMOTION_SCOPE_PATH, catalog=self.catalog
        )
        self.authority = stable_authority_record(
            rust_image_digest="a" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION_PATH.read_bytes(),
            effective_at_ns=time.time_ns(),
        )

    def test_c39_acceptance_matrix_covers_btc_and_eth_on_both_venues(self):
        self.assertEqual(
            {(item.venue, item.symbol) for item in C39_ACCEPTANCE_CASES},
            {
                ("BINANCE", "BTCUSDT"),
                ("BINANCE", "ETHUSDT"),
                ("OKX", "BTC-USDT-SWAP"),
                ("OKX", "ETH-USDT-SWAP"),
            },
        )
        self.assertEqual(
            len({item.instrument_uid for item in C39_ACCEPTANCE_CASES}),
            4,
        )

    def test_c39_acceptance_token_binds_current_manifest_revision(self):
        claims = c39_token_claims(
            "spiffe://qdl/paper/trading-system-stable",
            issuer="https://identity.qdl.stable.internal",
            audience="qdl-v2-stable",
            manifest_revision=2,
            now=1_800_000_000,
        )
        self.assertEqual(claims["consumer_manifest_revision"], 2)
        self.assertEqual(claims["iat"], 1_800_000_000)
        self.assertEqual(claims["exp"], 1_800_000_300)
        self.assertEqual(claims["environment"], "paper")

    def test_tls_generator_covers_all_published_ingress_aliases(self):
        script = (ROOT / "scripts/phase80_generate_tls.sh").read_text(
            encoding="utf-8"
        )
        for alias in (
            "qdl-v2-query",
            "qdl-v2-stream-a",
            "qdl-v2-stream-b",
        ):
            with self.subTest(alias=alias):
                self.assertIn(f"DNS:{alias}", script)

    def test_tls_generator_names_validity_and_issues_alpha_identity(self):
        script = (ROOT / "scripts/phase80_generate_tls.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn('CERT_DAYS="${QDL_PHASE8_CERT_DAYS:-90}"', script)
        self.assertEqual(script.count('-days "${CERT_DAYS}"'), 2)
        self.assertNotIn("-days 2", script)
        for artifact in (
            "stable-alpha-binance",
            "stable-alpha-binance-jwt",
            "stable-alpha-binance-jwt.public.pem",
        ):
            with self.subTest(artifact=artifact):
                self.assertIn(artifact, script)

    def test_initial_authority_scope_matches_active_crypto_derivatives_exactly(self):
        expected = {
            "binance-usdm-btcusdt-bar-1m",
            "binance-usdm-btcusdt-quote",
            "binance-usdm-btcusdt-trade",
            "binance-usdm-ethusdt-bar-1m",
            "binance-usdm-ethusdt-quote",
            "binance-usdm-ethusdt-trade",
            "okx-swap-btcusdt-bar-1m",
            "okx-swap-btcusdt-quote",
            "okx-swap-btcusdt-trade",
            "okx-swap-eth-usdt-swap-bar-1m",
            "okx-swap-eth-usdt-swap-quote",
            "okx-swap-eth-usdt-swap-trade",
        }
        self.assertEqual(self.promotion_scope.revision, 2)
        self.assertEqual(set(self.promotion_scope.binding_ids), expected)
        self.assertEqual(
            expected,
            {
                item.binding_id
                for item in self.acquisition.bindings
                if item.enabled and item.runtime in {"BINANCE", "OKX"}
            },
        )
        runtime = self.acquisition.production_core_config(
            catalog=self.catalog,
            raw_authority=self.authority,
            promotion_scope=self.promotion_scope,
            worker_index=1,
        )
        self.assertEqual(len(runtime["slices"]), 12)
        self.assertEqual(
            {item["subscription_id"] for item in runtime["slices"]},
            {
                item.source_id
                for item in self.catalog.bindings
                if item.binding_id in expected
            },
        )
        core_bindings = runtime["core"]["bindings"]
        self.assertEqual(
            {
                (item["venue"], item["market"], item["native_symbol"])
                for item in core_bindings
            },
            {
                ("BINANCE", "USDM", "BTCUSDT"),
                ("BINANCE", "USDM", "ETHUSDT"),
                ("OKX", "SWAP", "BTC-USDT-SWAP"),
                ("OKX", "SWAP", "ETH-USDT-SWAP"),
            },
        )
        self.assertFalse(any(
            item["market"] == "SPOT" or item["venue"] in {"DNSE", "HNX", "HOSE"}
            for item in core_bindings
        ))

    def test_authority_scope_rejects_unknown_and_duplicate_bindings(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scope.yaml"
            path.write_text(
                "schema: qdl.v2.authority-promotion-scope.v1\n"
                "revision: 1\nbinding_ids: [missing-binding]\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "unknown bindings"):
                AuthorityPromotionScope.load(path, catalog=self.catalog)
            path.write_text(
                "schema: qdl.v2.authority-promotion-scope.v1\n"
                "revision: 1\nbinding_ids: [binance-usdm-btcusdt-trade, "
                "binance-usdm-btcusdt-trade]\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "scope is invalid"):
                AuthorityPromotionScope.load(path, catalog=self.catalog)

    def test_all_catalog_bindings_have_one_capability_truthful_acquisition(self):
        self.assertEqual(len(self.catalog.bindings), 22)
        self.assertEqual(len(self.acquisition.bindings), 22)
        self.assertEqual(self.acquisition.revision, 5)
        modes = {item.mode for item in self.acquisition.bindings}
        self.assertEqual(modes, {"RUST_NATIVE", "PYTHON_REST", "PYTHON_VENDOR_SDK"})
        native = self.acquisition.native_ingestor_configs(
            catalog=self.catalog, authority=self.authority
        )
        # Spot is disabled by configuration, so no role is generated for it
        # while its capability stays declared in the catalog.
        self.assertEqual(set(native), {"binance-usdm", "okx-swap"})
        self.assertEqual(sum(len(item["bindings"]) for item in native.values()), 8)
        self.assertTrue(all(item["authority"]["mode"] == "RUST_SHADOW" for item in native.values()))
        self.assertEqual(
            {
                item.binding_id
                for item in self.acquisition.bindings
                if item.mode == "PYTHON_REST"
            },
            {
                "binance-usdm-btcusdt-bar-1m",
                "binance-spot-btcusdt-bar-1m",
                "binance-usdm-ethusdt-bar-1m",
                "okx-swap-btcusdt-bar-1m",
                "okx-swap-eth-usdt-swap-bar-1m",
                "okx-spot-btcusdt-bar-1m",
            },
        )
        self.assertEqual(
            {item["max_inflight_publishes"] for item in native.values()}, {512}
        )
        self.assertEqual(
            {key: item["max_subscriptions_per_connection"] for key, item in native.items()},
            {
                "binance-usdm": 200,
                "okx-swap": 100,
            },
        )
        generation_paths = {
            item["generation_state_path"] for item in native.values()
        }
        self.assertEqual(len(generation_paths), 2)
        self.assertTrue(all(
            value.startswith("/var/lib/qdl-stable/runtime/generations/")
            for value in generation_paths
        ))
        self.assertEqual(
            {item["latest_state_flush_ms"] for item in native.values()}, {50}
        )
        delivery_by_feed = {
            binding["feed"]: binding["delivery_class"]
            for item in native.values()
            for binding in item["bindings"]
        }
        self.assertEqual(
            delivery_by_feed,
            {"TRADE": "LOSSLESS", "QUOTE": "LATEST_STATE"},
        )
        okx_bbo = {
            item.binding_id: item.sequence_policy
            for item in self.acquisition.bindings
            if item.provider_kind == "okx_bbo"
        }
        self.assertEqual(
            okx_bbo,
            {
                "okx-spot-btcusdt-quote": "NONE",
                "okx-swap-btcusdt-quote": "NONE",
                "okx-swap-eth-usdt-swap-quote": "NONE",
            },
        )

    def test_core_bundle_uses_stable_identity_lineage_and_never_enables_public_writes(self):
        core = self.acquisition.core_config(
            catalog=self.catalog, authority=self.authority
        )
        bindings = core["core"]["bindings"]
        # The core is configured from acquired bindings; a disabled binding
        # keeps its catalog capability but is not consumed by any role.
        acquired = {
            item.binding_id for item in self.acquisition.bindings if item.enabled
        }
        expected = {
            item.instrument.instrument_uid for item in self.catalog.bindings
            if item.binding_id in acquired
        }
        self.assertEqual({item["instrument_uid"] for item in bindings}, expected)
        self.assertEqual(
            {item["source_id"] for item in bindings},
            {
                item.source_id for item in self.catalog.bindings
                if item.binding_id in acquired
            },
        )
        finality_by_source = {
            item.source_id: item.require_final_bar
            for item in self.catalog.bindings
            if item.binding_id in acquired
        }
        self.assertEqual(
            {item["source_id"]: item["require_final_bar"] for item in bindings},
            finality_by_source,
        )
        self.assertEqual(sum(finality_by_source.values()), 6)
        self.assertFalse(core["authority"]["public_write_allowed"])
        self.assertFalse(core["authority"]["legacy_write_allowed"])
        self.assertFalse(core["core"]["allow_test_provenance"])
        self.assertEqual(core["raw_topics"], ["md.raw.stable.v1"])
        workers = [
            self.acquisition.core_config(
                catalog=self.catalog,
                authority=self.authority,
                worker_index=index,
            )
            for index in range(1, STABLE_CORE_WORKER_COUNT + 1)
        ]
        self.assertEqual(
            {item["transactional_id"] for item in workers},
            {"qdl-v2-stable-core-001", "qdl-v2-stable-core-002", "qdl-v2-stable-core-003"},
        )
        self.assertEqual(
            {item["shard_id"] for item in workers},
            {item["transactional_id"] for item in workers},
        )
        self.assertEqual({item["raw_topics"][0] for item in workers}, {"md.raw.stable.v1"})
        self.assertEqual({json.dumps(item["authority"], sort_keys=True) for item in workers}, {
            json.dumps(self.authority, sort_keys=True)
        })
        for invalid_index in (0, STABLE_CORE_WORKER_COUNT + 1):
            with self.assertRaisesRegex(ValueError, "worker index"):
                self.acquisition.core_config(
                    catalog=self.catalog,
                    authority=self.authority,
                    worker_index=invalid_index,
                )

        with tempfile.TemporaryDirectory(prefix="qdl-phaseb-bundle-") as directory:
            first = write_stable_runtime_bundle(
                Path(directory),
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
            )
            second = write_stable_runtime_bundle(
                Path(directory),
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
            )
            self.assertEqual(first, second)
            self.assertEqual(
                set(first),
                {
                    "authority.json",
                    "core.json",
                    "core-002.json",
                    "core-003.json",
                    "ingestor-binance-usdm.json",
                    "ingestor-okx-swap.json",
                },
            )
            persisted = json.loads((Path(directory) / "core.json").read_text())
            self.assertEqual(persisted, core)
            persisted_workers = [
                json.loads((Path(directory) / name).read_text())
                for name in ("core.json", "core-002.json", "core-003.json")
            ]
            self.assertEqual(
                len({item["transactional_id"] for item in persisted_workers}),
                STABLE_CORE_WORKER_COUNT,
            )

    def test_binance_bar_edge_publishes_each_closed_bar_once(self):
        class Publisher:
            def __init__(self):
                self.batches = []

            def publish_many(self, values):
                batch = tuple(values)
                self.batches.append(batch)
                return tuple(range(len(batch)))

        class Envelope:
            def __init__(self, venue, open_time_ms):
                payload = (
                    {"row": [open_time_ms, "1", "1", "1", "1", "1", open_time_ms + 59999]}
                    if venue == "BINANCE"
                    else {"data": [[str(open_time_ms), "1", "1", "1", "1", "1", "1", "1", "1"]]}
                )
                self.raw_frame_bytes = json.dumps(payload).encode()

        publisher = Publisher()
        edge = StableBinanceBarEdge(
            catalog=self.catalog,
            acquisition=self.acquisition,
            authority=self.authority,
            publisher=publisher,
            clock=lambda: 120.0,
        )
        binance_count = len(edge.bindings)
        okx_count = len(edge.okx_bindings)
        total_count = binance_count + okx_count
        with patch(
            "qdl.runtime.stable_bar_edge.fetch_latest_closed_bar_raw_envelope",
            side_effect=[Envelope("BINANCE", 60_000)] * (binance_count * 2),
        ) as binance_latest, patch(
            "qdl.runtime.stable_bar_edge.fetch_okx_latest",
            side_effect=[Envelope("OKX", 60_000)] * (okx_count * 2),
        ) as okx_latest:
            self.assertEqual(edge.run_cycle(), total_count)
            self.assertEqual(edge.run_cycle(), 0)
        self.assertEqual([len(batch) for batch in publisher.batches], [total_count])
        self.assertEqual(
            {call.kwargs["now_ms"] for call in binance_latest.call_args_list},
            {110_000},
        )
        self.assertEqual(
            {call.kwargs["now_ms"] for call in okx_latest.call_args_list},
            {110_000},
        )

    def test_bar_edge_retries_complete_catchup_after_kafka_ack_failure(self):
        class Publisher:
            def __init__(self):
                self.calls = 0
                self.batches = []

            def publish_many(self, values):
                batch = tuple(values)
                self.calls += 1
                self.batches.append(batch)
                if self.calls == 1:
                    raise RuntimeError("injected Kafka ACK failure")
                return tuple(range(len(batch)))

        class Envelope:
            def __init__(self, venue, open_time_ms):
                payload = (
                    {"row": [
                        open_time_ms, "1", "1", "1", "1", "1",
                        open_time_ms + 59_999,
                    ]}
                    if venue == "BINANCE"
                    else {"data": [[
                        str(open_time_ms), "1", "1", "1", "1",
                        "1", "1", "1", "1",
                    ]]}
                )
                self.raw_frame_bytes = json.dumps(payload).encode()

        publisher = Publisher()
        edge = StableBinanceBarEdge(
            catalog=self.catalog,
            acquisition=self.acquisition,
            authority=self.authority,
            publisher=publisher,
            clock=lambda: 240.0,
        )
        binding_ids = [
            source.binding_id
            for source, _acquisition in edge.bindings + edge.okx_bindings
        ]
        edge._last_open_ms.update({binding_id: 60_000 for binding_id in binding_ids})
        binance_count = len(edge.bindings)
        okx_count = len(edge.okx_bindings)
        expected_catchup = 2 * (binance_count + okx_count)
        binance_history = (
            Envelope("BINANCE", 120_000),
            Envelope("BINANCE", 180_000),
        )
        okx_history = (
            Envelope("OKX", 120_000),
            Envelope("OKX", 180_000),
        )
        with patch(
            "qdl.runtime.stable_bar_edge.fetch_latest_closed_bar_raw_envelope",
            side_effect=[Envelope("BINANCE", 180_000)] * (binance_count * 2),
        ), patch(
            "qdl.runtime.stable_bar_edge.fetch_okx_latest",
            new_callable=AsyncMock,
            side_effect=[Envelope("OKX", 180_000)] * (okx_count * 2),
        ), patch(
            "qdl.runtime.stable_bar_edge.fetch_binance_history",
            side_effect=[binance_history] * (binance_count * 2),
        ), patch(
            "qdl.runtime.stable_bar_edge.fetch_okx_history",
            new_callable=AsyncMock,
            side_effect=[okx_history] * (okx_count * 2),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected Kafka ACK failure"):
                edge.run_cycle()
            self.assertEqual(
                edge._last_open_ms,
                {binding_id: 60_000 for binding_id in binding_ids},
            )
            self.assertEqual(edge.run_cycle(), expected_catchup)

        self.assertEqual(
            [len(batch) for batch in publisher.batches],
            [expected_catchup, expected_catchup],
        )
        self.assertEqual(
            edge._last_open_ms,
            {binding_id: 180_000 for binding_id in binding_ids},
        )

    def test_bar_edge_rejects_incomplete_catchup_without_advancing_watermark(self):
        class Publisher:
            def __init__(self):
                self.calls = 0

            def publish_many(self, _values):
                self.calls += 1
                return ()

        class Envelope:
            def __init__(self, venue, open_time_ms):
                payload = (
                    {"row": [
                        open_time_ms, "1", "1", "1", "1", "1",
                        open_time_ms + 59_999,
                    ]}
                    if venue == "BINANCE"
                    else {"data": [[
                        str(open_time_ms), "1", "1", "1", "1",
                        "1", "1", "1", "1",
                    ]]}
                )
                self.raw_frame_bytes = json.dumps(payload).encode()

        publisher = Publisher()
        edge = StableBinanceBarEdge(
            catalog=self.catalog,
            acquisition=self.acquisition,
            authority=self.authority,
            publisher=publisher,
            clock=lambda: 240.0,
        )
        binding_ids = [
            source.binding_id
            for source, _acquisition in edge.bindings + edge.okx_bindings
        ]
        edge._last_open_ms.update({binding_id: 60_000 for binding_id in binding_ids})
        with patch(
            "qdl.runtime.stable_bar_edge.fetch_latest_closed_bar_raw_envelope",
            side_effect=[Envelope("BINANCE", 180_000)] * len(edge.bindings),
        ), patch(
            "qdl.runtime.stable_bar_edge.fetch_okx_latest",
            new_callable=AsyncMock,
            side_effect=[Envelope("OKX", 180_000)] * len(edge.okx_bindings),
        ), patch(
            "qdl.runtime.stable_bar_edge.fetch_binance_history",
            return_value=(Envelope("BINANCE", 180_000),),
        ):
            with self.assertRaisesRegex(RuntimeError, "not contiguous"):
                edge.run_cycle()

        self.assertEqual(publisher.calls, 0)
        self.assertEqual(
            edge._last_open_ms,
            {binding_id: 60_000 for binding_id in binding_ids},
        )

    def test_dnse_edge_fences_on_queue_pressure_and_bar_keeps_exact_units(self):
        class Publisher:
            pass

        edge = StableDnseVendorEdge(
            catalog=self.catalog,
            acquisition=self.acquisition,
            authority=self.authority,
            publisher=Publisher(),
            queue_capacity=1,
        )
        trade = type("Trade", (), {
            "symbol": "VN30F1M",
            "price": "1820.7",
            "quantity": "1",
            "marketId": "VN30",
            "boardId": "G3",
            "tradingSessionId": "CONTINUOUS",
            "totalVolumeTraded": "12",
        })()
        edge.on_trade(trade)
        edge.on_trade(trade)
        self.assertTrue(edge._fatal.is_set())
        self.assertEqual(edge._queue.qsize(), 1)

        source = edge.bar_sources["VN30F1M"]
        row = {"t": 1_786_352_340, "o": "1820.7", "h": "1821.2",
               "l": "1820.2", "c": "1820.7", "v": "0"}
        envelope = build_dnse_bar_raw_envelope(
            row,
            edge._binding(source),
            received_at_ns=1_786_352_400_000_000_000,
        )
        websocket_envelope = build_dnse_bar_raw_envelope(
            row,
            edge._binding(source),
            received_at_ns=1_786_352_400_000_000_001,
            acquisition_origin="WEBSOCKET_CLOSED",
        )
        payload = json.loads(envelope.raw_frame_bytes)
        self.assertEqual(payload["interval"], "1m")
        self.assertEqual(payload["v"], "0")
        self.assertTrue(payload["is_final"])
        self.assertEqual(envelope.native_channel, websocket_envelope.native_channel)
        self.assertNotEqual(
            envelope.transport_protocol, websocket_envelope.transport_protocol
        )
        self.assertNotEqual(
            envelope.capture_boundary, websocket_envelope.capture_boundary
        )
        self.assertFalse(envelope.test_provenance)

    def test_dnse_history_bootstrap_retries_checkpoints_and_restores(self):
        class Publisher:
            def __init__(self):
                self.batches = []
                self.fail = False

            def publish_many(self, values):
                batch = tuple(values)
                self.batches.append(batch)
                return () if self.fail else tuple(range(len(batch)))

        rows = [
            {"t": value, "o": "100", "h": "101", "l": "99", "c": "100.5", "v": "10"}
            for value in (120, 180, 240)
        ]
        calls = []
        sleeps = []

        def fetcher(symbol, resolution, start, end):
            calls.append((symbol, resolution, start, end))
            if len(calls) == 1:
                raise TimeoutError("injected transient DNSE timeout")
            return rows

        with tempfile.TemporaryDirectory(prefix="qdl-dnse-state-") as directory:
            state_path = Path(directory) / "dnse.json"
            publisher = Publisher()
            edge = StableDnseVendorEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=publisher,
                warmup_rows=2,
                history_lookback_days=1,
                history_attempts=2,
                history_fetcher=fetcher,
                state_path=state_path,
                clock=lambda: 400.0,
                sleep=sleeps.append,
            )
            self.assertEqual(edge.bootstrap_history(), 4)
            self.assertEqual(edge.bootstrap_history(), 0)
            self.assertEqual([len(batch) for batch in publisher.batches], [4])
            self.assertEqual(sleeps, [1])
            self.assertEqual(set(edge._last_bar), set(edge._bar_binding_ids))
            self.assertEqual(state_path.stat().st_mode & 0o777, 0o600)
            self.assertTrue(all(
                not item.test_provenance
                for batch in publisher.batches
                for item in batch
            ))

            restored = StableDnseVendorEdge(
                catalog=self.catalog,
                acquisition=self.acquisition,
                authority=self.authority,
                publisher=publisher,
                warmup_rows=2,
                history_fetcher=lambda *_args: (_ for _ in ()).throw(
                    AssertionError("matching checkpoint must avoid REST bootstrap")
                ),
                state_path=state_path,
                clock=lambda: 400.0,
            )
            self.assertEqual(restored.bootstrap_history(), 0)
            self.assertEqual(restored._last_bar, edge._last_bar)

    def test_dnse_closed_bar_uses_websocket_and_ack_advances_checkpoint(self):
        class Publisher:
            def __init__(self):
                self.batches = []

            def publish_many(self, values):
                batch = tuple(values)
                self.batches.append(batch)
                return tuple(range(len(batch)))

        rows = [
            {"t": value, "o": "100", "h": "101", "l": "99", "c": "100.5", "v": "10"}
            for value in (120, 180, 240)
        ]
        with tempfile.TemporaryDirectory(prefix="qdl-dnse-state-") as directory:
            state_path = Path(directory) / "dnse.json"
            publisher = Publisher()
            edge = StableDnseVendorEdge(
                catalog=self.catalog, acquisition=self.acquisition,
                authority=self.authority, publisher=publisher, warmup_rows=2,
                history_attempts=1, history_fetcher=lambda *_args: rows,
                state_path=state_path, clock=lambda: 400.0,
            )
            edge.bootstrap_history()
            edge.history_fetcher = lambda *_args: (_ for _ in ()).throw(
                AssertionError("live closed BAR must not poll REST")
            )
            bar = type("Ohlc", (), {
                "symbol": "VN30F1M", "resolution": "1", "time": 300,
                "open": "100", "high": "102", "low": "99", "close": "101",
                "volume": "12",
            })()
            edge.on_ohlc_closed(bar)
            edge._stopped.set()
            edge._publish_worker()
            self.assertFalse(edge._fatal.is_set())
            self.assertEqual([len(batch) for batch in publisher.batches], [4, 1])
            live = publisher.batches[-1][0]
            self.assertEqual(live.native_channel, "ohlcv/1m")
            self.assertNotEqual(
                live.transport_protocol, publisher.batches[0][0].transport_protocol
            )
            binding_id = edge.bar_sources["VN30F1M"].binding_id
            self.assertEqual(edge._last_bar[binding_id][0], 300_000)
            restored = StableDnseVendorEdge(
                catalog=self.catalog, acquisition=self.acquisition,
                authority=self.authority, publisher=publisher,
                state_path=state_path, clock=lambda: 400.0,
            )
            self.assertEqual(restored._last_bar[binding_id][0], 300_000)
            restored.on_ohlc_closed(bar)
            self.assertTrue(restored._queue.empty())

    def test_dnse_run_subscribes_native_closed_bar_without_live_rest_polling(self):
        calls = []

        class Publisher:
            def publish_many(self, values):
                batch = tuple(values)
                return tuple(range(len(batch)))

            def close(self):
                calls.append(("close",))

        class Client:
            is_healthy = True

            def __init__(self, **kwargs):
                calls.append((
                    "init", kwargs["base_url"], kwargs["dispatch_queue_capacity"]
                ))

            async def connect(self):
                calls.append(("connect",))

            async def subscribe_trades(self, **kwargs):
                calls.append(("trade", kwargs["board_id"], tuple(kwargs["symbols"])))

            async def subscribe_ohlc_closed(self, **kwargs):
                calls.append(("bar", kwargs["resolution"], tuple(kwargs["symbols"])))
                edge.stop()

            async def disconnect(self):
                calls.append(("disconnect",))

        edge = StableDnseVendorEdge(
            catalog=self.catalog, acquisition=self.acquisition,
            authority=self.authority, publisher=Publisher(),
            history_fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("restored runtime must not call REST")
            ),
        )
        edge._history_bootstrapped = True
        with patch("qdl.adapters.vn.stable_edge.DNSE_API_KEY", "key"), patch(
            "qdl.adapters.vn.stable_edge.DNSE_API_SECRET_KEY", "secret"
        ), patch("qdl.adapters.vn.stable_edge.TradingClient", Client):
            asyncio.run(edge.run())
        self.assertIn(
            ("init", "wss://ws-openapi.dnse.com.vn", 833), calls
        )
        self.assertIn(("trade", "G1", ("FPT", "VN30F1M")), calls)
        self.assertIn(("trade", "G3", ("FPT", "VN30F1M")), calls)
        self.assertIn(("bar", "1", ("FPT", "VN30F1M")), calls)
        self.assertEqual(calls[-2:], [("disconnect",), ("close",)])

    def test_dnse_history_conflict_partial_checkpoint_and_ack_failure_fail_closed(self):
        base = {"t": 120, "o": "100", "h": "101", "l": "99", "c": "100", "v": "1"}
        conflict = {**base, "c": "101"}
        edge = StableDnseVendorEdge(
            catalog=self.catalog, acquisition=self.acquisition, authority=self.authority,
            publisher=object(), warmup_rows=2, history_attempts=1,
            history_fetcher=lambda *_args: [base, conflict],
            clock=lambda: 400.0, sleep=lambda _delay: None,
        )
        with self.assertRaisesRegex(RuntimeError, "bootstrap exhausted"):
            edge._closed_history("VN30F1M")

        partial = StableDnseVendorEdge(
            catalog=self.catalog, acquisition=self.acquisition, authority=self.authority,
            publisher=object(), warmup_rows=2, history_attempts=1,
            history_fetcher=lambda *_args: [base],
            clock=lambda: 400.0, sleep=lambda _delay: None,
        )
        with self.assertRaisesRegex(RuntimeError, "bootstrap exhausted"):
            partial._closed_history("VN30F1M")

        with tempfile.TemporaryDirectory(prefix="qdl-dnse-state-") as directory:
            state_path = Path(directory) / "dnse.json"
            state_path.write_text(json.dumps({
                "schema": "qdl.stable-dnse-edge-state.v1",
                "slice_id": self.authority["slice_id"],
                "authority_revision": self.authority["revision"],
                "catalog_revision": self.catalog.catalog_revision,
                "acquisition_revision": self.acquisition.revision,
                "binding_ids": list(edge._bar_binding_ids),
                "last_bar": {},
            }))
            with self.assertRaisesRegex(RuntimeError, "partial"):
                StableDnseVendorEdge(
                    catalog=self.catalog, acquisition=self.acquisition,
                    authority=self.authority, publisher=object(), state_path=state_path,
                )

        class MissingAckPublisher:
            def publish_many(self, values):
                tuple(values)
                return ()

        no_ack = StableDnseVendorEdge(
            catalog=self.catalog, acquisition=self.acquisition,
            authority=self.authority, publisher=MissingAckPublisher(),
            warmup_rows=1, history_attempts=1,
            history_fetcher=lambda *_args: [base], clock=lambda: 400.0,
        )
        with self.assertRaisesRegex(RuntimeError, "missed a Kafka ACK"):
            no_ack.bootstrap_history()
        self.assertEqual(no_ack._last_bar, {})

    def test_missing_binding_wrong_provider_kind_and_primary_authority_fail_closed(self):
        payload = yaml.safe_load(ACQUISITION_PATH.read_text(encoding="utf-8"))
        missing = copy.deepcopy(payload)
        missing["bindings"].pop()
        with tempfile.TemporaryDirectory(prefix="qdl-phaseb-acquisition-") as directory:
            path = Path(directory) / "missing.yaml"
            path.write_text(yaml.safe_dump(missing, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "binding sets differ"):
                StableAcquisitionPlan.load(path, catalog=self.catalog)

            wrong = copy.deepcopy(payload)
            wrong["bindings"][0]["provider_kind"] = "okx_trade"
            path.write_text(yaml.safe_dump(wrong, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "provider kind differs"):
                StableAcquisitionPlan.load(path, catalog=self.catalog)

            contiguous_bbo = copy.deepcopy(payload)
            for item in contiguous_bbo["bindings"]:
                if item["provider_kind"] == "okx_bbo":
                    item["sequence_policy"] = "CONTIGUOUS"
                    break
            path.write_text(
                yaml.safe_dump(contiguous_bbo, sort_keys=False), encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "replace-only"):
                StableAcquisitionPlan.load(path, catalog=self.catalog)

        primary = copy.deepcopy(self.authority)
        primary["mode"] = "RUST_PRIMARY"
        primary["public_write_allowed"] = True
        with self.assertRaisesRegex(ValueError, "not an isolated Rust shadow"):
            self.acquisition.core_config(catalog=self.catalog, authority=primary)


class StableComposeAndBundleTests(unittest.TestCase):
    def test_python_release_base_is_digest_pinned_in_both_stages(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        pinned = (
            "python:3.12-slim@sha256:"
            "2c941e860699f878900b0edc2403613c234d4b32"
            "eda3cc9fa7036991a2a63c4a"
        )
        self.assertEqual(dockerfile.count(pinned), 2)
        self.assertNotIn("FROM python:3.12-slim AS", dockerfile)

    def test_compose_is_isolated_bounded_nonroot_and_has_no_v1_route(self):
        raw = (ROOT / "docker-compose.v2-stable.yml").read_text(encoding="utf-8")
        compose = yaml.safe_load(raw)
        services = compose["services"]
        self.assertNotIn("8100", raw)
        self.assertNotIn("redis_marketdata", raw)
        self.assertTrue(compose["networks"]["stable_internal"]["internal"])
        self.assertFalse(compose["networks"]["stable_ingress"].get("internal", False))
        for name in ("query_v2_1", "query_v2_2", "stream_v2_active", "stream_v2_passive"):
            self.assertEqual(
                set(services[name]["networks"]),
                {"stable_internal", "stable_ingress", "stable_consumer"},
            )
            self.assertTrue(
                all(str(port).startswith("127.0.0.1:") for port in services[name]["ports"])
            )
        projector_names = ("projector_v2", "projector_v2_2", "projector_v2_3")
        for name in projector_names:
            self.assertNotIn("ports", services[name])
            self.assertEqual(services[name]["networks"], ["stable_internal"])
        self.assertEqual(
            {
                services[name]["environment"]["QDL_STABLE_CONSUMER_GROUP"]
                for name in projector_names
            },
            {"stable-projector-v1"},
        )
        self.assertEqual(
            {
                services[name]["environment"]["QDL_STABLE_KAFKA_CLIENT_ID"]
                for name in projector_names
            },
            {"stable-projector-1", "stable-projector-2", "stable-projector-3"},
        )
        self.assertEqual(
            len({
                services[name]["environment"]["QDL_STABLE_AUDIT_PATH"]
                for name in projector_names
            }),
            len(projector_names),
        )
        self.assertEqual(
            compose["x-kafka-env"]["KAFKA_MIN_INSYNC_REPLICAS"], 2
        )
        self.assertEqual(
            compose["x-kafka-env"]["KAFKA_DEFAULT_REPLICATION_FACTOR"], 3
        )
        self.assertEqual(compose["x-kafka"]["mem_limit"], "768m")
        self.assertEqual(
            compose["x-kafka-env"]["KAFKA_HEAP_OPTS"], "-Xms256m -Xmx256m"
        )
        kafka_tmpfs = compose["x-kafka"]["tmpfs"]
        self.assertEqual(kafka_tmpfs, ["/tmp:rw,nosuid,nodev,exec,size=32m"])
        self.assertNotIn("noexec", kafka_tmpfs[0])
        for name in projector_names:
            self.assertIn("stable_tls:/stable-certs:ro", services[name]["volumes"])
            self.assertNotIn("/certs:ro", " ".join(services[name]["volumes"]))
        bar_edge = services["binance_bar_edge"]
        self.assertEqual(
            bar_edge["environment"]["QDL_STABLE_BAR_SETTLEMENT_DELAY_SECONDS"],
            "10",
        )
        self.assertEqual(
            bar_edge["environment"]["QDL_STABLE_BAR_STATE_PATH"],
            "/var/lib/qdl-stable/runtime/stable-crypto-bar-edge.json",
        )
        self.assertIn("stable_state:/var/lib/qdl-stable", bar_edge["volumes"])
        self.assertEqual(
            bar_edge["depends_on"]["stable_state_init"],
            {"condition": "service_completed_successfully"},
        )
        for name in (
            "query_v2_1", "query_v2_2", "stream_v2_active",
            "stream_v2_passive", *projector_names,
        ):
            with self.subTest(service=name):
                self.assertEqual(services[name]["user"], "10001:10001")
                self.assertTrue(services[name]["read_only"])
                self.assertIn("ALL", services[name]["cap_drop"])
                self.assertEqual(services[name]["restart"], "no")
        ingress_aliases = {
            "query_v2_1": "qdl-v2-query",
            "query_v2_2": "qdl-v2-query",
            "stream_v2_active": "qdl-v2-stream-a",
            "stream_v2_passive": "qdl-v2-stream-b",
        }
        for name, alias in ingress_aliases.items():
            self.assertEqual(
                services[name]["networks"]["stable_consumer"]["aliases"],
                [alias],
            )
        self.assertEqual(
            compose["networks"]["stable_consumer"],
            {
                "external": True,
                "name": "${QDL_STABLE_CONSUMER_NETWORK:"
                "?set QDL_STABLE_CONSUMER_NETWORK}",
            },
        )
        for name in (
            "kafka1", "kafka2", "kafka3", "stable_redis",
            *projector_names, "rust_core", "rust_core_2", "rust_core_3",
        ):
            self.assertNotIn("stable_consumer", services[name]["networks"])
        self.assertEqual(
            set(services["ingestor_okx_swap"]["networks"]),
            {"stable_internal", "stable_egress"},
        )
        for name in (
            "ingestor_binance_usdm", "ingestor_binance_spot",
            "ingestor_okx_swap", "ingestor_okx_spot",
        ):
            with self.subTest(native_ingestor=name):
                self.assertTrue(services[name]["read_only"])
                self.assertIn(
                    "stable_state:/var/lib/qdl-stable", services[name]["volumes"]
                )
                self.assertEqual(
                    services[name]["depends_on"],
                    {
                        "stable_tls_init": {
                            "condition": "service_completed_successfully"
                        },
                        "stable_state_init": {
                            "condition": "service_completed_successfully"
                        },
                        "kafka1": {"condition": "service_healthy"},
                        "kafka2": {"condition": "service_healthy"},
                        "kafka3": {"condition": "service_healthy"},
                    },
                )
        core_names = ("rust_core", "rust_core_2", "rust_core_3")
        self.assertLessEqual(
            len(core_names), compose["x-kafka-env"]["KAFKA_NUM_PARTITIONS"]
        )
        self.assertEqual(
            {
                services[name]["environment"]["QDL_KAFKA_CLIENT_ID"]
                for name in core_names
            },
            {
                "qdl-v2-stable-core-001",
                "qdl-v2-stable-core-002",
                "qdl-v2-stable-core-003",
            },
        )
        self.assertEqual(
            {
                services[name]["environment"]["QDL_KAFKA_GROUP_ID"]
                for name in core_names
            },
            {"qdl-v2-stable-core-v1"},
        )
        for name in core_names:
            with self.subTest(service=name):
                self.assertEqual(
                    services[name]["entrypoint"],
                    ["/usr/local/bin/qdl-realtime-core"],
                )
                self.assertIn("stable_tls:/stable-certs:ro", services[name]["volumes"])

        authority_db = services["stable_authority_db"]
        self.assertEqual(authority_db["profiles"], ["stable-authority"])
        self.assertNotIn("ports", authority_db)
        self.assertIn(
            "./migrations/postgres:/docker-entrypoint-initdb.d:ro",
            authority_db["volumes"],
        )
        self.assertIn(
            "stable_authority_db:/var/lib/postgresql/data",
            authority_db["volumes"],
        )
        dispatcher = services["authority_outbox_v2"]
        self.assertEqual(dispatcher["profiles"], ["stable-authority"])
        self.assertEqual(dispatcher["networks"], ["stable_internal"])
        self.assertNotIn("ports", dispatcher)
        self.assertEqual(
            dispatcher["environment"]["QDL_AUTHORITY_TOPIC"], "qdl.authority.v1"
        )
        self.assertIn(
            "/stable-certs/authority-dispatcher/client.crt",
            dispatcher["environment"]["QDL_KAFKA_CERT_LOCATION"],
        )
        production_names = (
            "production_core_1", "production_core_2", "production_core_3"
        )
        self.assertEqual(
            {
                services[name]["environment"]["QDL_KAFKA_CLIENT_ID"]
                for name in production_names
            },
            {
                "qdl-v2-production-core-001",
                "qdl-v2-production-core-002",
                "qdl-v2-production-core-003",
            },
        )
        for name in production_names:
            with self.subTest(production_service=name):
                self.assertEqual(
                    services[name]["profiles"], ["stable-authority-primary"]
                )
                self.assertEqual(
                    services[name]["entrypoint"],
                    ["/usr/local/bin/qdl-production-core"],
                )
                self.assertEqual(services[name]["user"], "10001:10001")
                self.assertTrue(services[name]["read_only"])
                self.assertNotIn("ports", services[name])

    def test_candidate_bundle_uses_image_ids_and_never_records_secret_values(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phaseb-cert-") as cert_directory:
            certs = Path(cert_directory)
            (certs / "ca.crt").write_text("ca", encoding="ascii")
            for principal in (
                "phase8-producer",
                "phase8-core",
                "phase8-consumer",
                "stable-authority-dispatcher",
                "stable-trading-system",
                "stable-alpha-binance",
                "stable-query",
                "stable-stream",
            ):
                (certs / f"{principal}.crt").write_text("crt", encoding="ascii")
                (certs / f"{principal}.key").write_text("key", encoding="ascii")
            (certs / "stable-trading-system-jwt.key").write_text(
                "private", encoding="ascii"
            )
            (certs / "stable-trading-system-jwt.public.pem").write_text(
                "public", encoding="ascii"
            )
            (certs / "stable-alpha-binance-jwt.key").write_text(
                "alpha-private", encoding="ascii"
            )
            (certs / "stable-alpha-binance-jwt.public.pem").write_text(
                "alpha-public", encoding="ascii"
            )
            with tempfile.TemporaryDirectory(prefix="qdl-phaseb-output-") as parent:
                output = Path(parent) / "candidate"
                with patch(
                    "scripts.phaseb_prepare_stable_candidate.image_id",
                    side_effect=("sha256:" + "a" * 64, "sha256:" + "b" * 64),
                ):
                    manifest = prepare_candidate(
                        rust_image="qdl-rust:test",
                        python_image="qdl-python:test",
                        cert_dir=certs,
                        output_dir=output,
                        consumer_network="executor_network",
                        host_cert_dir=Path("/host/qdl/certs"),
                        host_output_dir=Path("/host/qdl/candidate"),
                    )
                self.assertFalse(manifest["cutover_authorized"])
                self.assertFalse(manifest["secret_values_recorded"])
                self.assertEqual(manifest["consumer_count"], 6)
                self.assertEqual(manifest["workload_identity_count"], 5)
                self.assertEqual(manifest["authority_promotion_binding_count"], 12)
                self.assertEqual(manifest["consumer_network"], "executor_network")
                self.assertEqual(len(manifest["authority_promotion_scope_digest"]), 64)
                production = json.loads(
                    (output / "runtime/production-core-001.json").read_text()
                )
                self.assertEqual(len(production["slices"]), 12)
                self.assertEqual(
                    {item["venue"] for item in production["core"]["bindings"]},
                    {"BINANCE", "OKX"},
                )
                self.assertEqual((output / "stable.env").stat().st_mode & 0o777, 0o600)
                env_text = (output / "stable.env").read_text()
                self.assertIn("QDL_STABLE_CERT_DIR=/host/qdl/certs", env_text)
                self.assertIn("QDL_STABLE_CONSUMER_NETWORK=executor_network", env_text)
                self.assertIn(
                    "QDL_STABLE_RUNTIME_DIR=/host/qdl/candidate/runtime", env_text
                )
                self.assertIn(
                    "QDL_STABLE_AUTHORITY_CERT_DIR="
                    "/host/qdl/candidate/identities/authority-dispatcher",
                    env_text,
                )
                self.assertIn(
                    "postgresql://qdl_authority_dispatcher:", env_text
                )
                self.assertIn(
                    "stable-alpha-binance-rs256-v1", env_text
                )
                self.assertIn(
                    "QDL_STABLE_ALPHA_BINANCE_CERT_DIR="
                    "/host/qdl/candidate/identities/alpha-binance",
                    env_text,
                )
                self.assertIn(
                    "QDL_STABLE_ALPHA_BINANCE_JWT_PRIVATE_KEY="
                    "/host/qdl/candidate/identities/alpha-binance-jwt/private.key",
                    env_text,
                )
                public_manifest = (output / "candidate-manifest.json").read_text()
                self.assertNotIn("QDL_STABLE_INTERNAL_INGEST_SECRET", public_manifest)
                for name in ("core.json", "core-002.json", "core-003.json"):
                    self.assertTrue((output / f"runtime/{name}").is_file())
                for index in range(1, 4):
                    self.assertTrue(
                        (output / f"runtime/production-core-{index:03d}.json").is_file()
                    )
                self.assertTrue(
                    (output / "runtime/production-core-manifest.json").is_file()
                )
                self.assertTrue((output / "identities/projector/client.key").is_file())
                self.assertTrue(
                    (
                        output
                        / "identities/authority-dispatcher/client.key"
                    ).is_file()
                )
                alpha_key = output / "identities/alpha-binance/client.key"
                alpha_jwt_key = (
                    output / "identities/alpha-binance-jwt/private.key"
                )
                self.assertTrue(alpha_key.is_file())
                self.assertTrue(alpha_jwt_key.is_file())
                self.assertEqual(alpha_key.stat().st_mode & 0o777, 0o440)
                self.assertEqual(alpha_jwt_key.stat().st_mode & 0o777, 0o440)


if __name__ == "__main__":
    unittest.main()


class ProductionCoreBundleCliTests(unittest.TestCase):
    """The CLI must stay callable against the library signature it wraps.

    A missing keyword only surfaced at rollout time because no test invoked the
    entry point itself.
    """

    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.promotion_scope = AuthorityPromotionScope.load(
            PROMOTION_SCOPE_PATH, catalog=self.catalog
        )
        self.authority = stable_authority_record(
            rust_image_digest="b" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION_PATH.read_bytes(),
            effective_at_ns=time.time_ns(),
        )

    def _run(self, directory: Path, *extra: str) -> dict:
        authority_path = directory / "authority.json"
        authority_path.write_text(json.dumps(self.authority), encoding="utf-8")
        output_dir = directory / "runtime"
        argv = [
            "build_production_core_bundle.py",
            "--source-catalog", str(CATALOG_PATH),
            "--acquisition-plan", str(ACQUISITION_PATH),
            "--raw-authority", str(authority_path),
            "--output-dir", str(output_dir),
            *extra,
        ]
        stdout = io.StringIO()
        with patch("sys.argv", argv), redirect_stdout(stdout):
            self.assertEqual(build_production_core_bundle_main(), 0)
        return json.loads(stdout.getvalue())

    def test_cli_builds_bundle_scoped_to_the_promotion_scope(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            result = self._run(
                directory, "--promotion-scope", str(PROMOTION_SCOPE_PATH)
            )
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(
                result["promotion_scope_revision"], self.promotion_scope.revision
            )
            self.assertEqual(
                result["promotion_scope_digest"], self.promotion_scope.digest()
            )
            self.assertEqual(
                result["promotion_binding_count"],
                len(self.promotion_scope.binding_ids),
            )
            manifest = json.loads(
                (directory / "runtime/production-core-manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(
                manifest["promotion_binding_count"],
                len(self.promotion_scope.binding_ids),
            )
            for worker_index in range(1, STABLE_CORE_WORKER_COUNT + 1):
                payload = json.loads(
                    (
                        directory / f"runtime/production-core-{worker_index:03d}.json"
                    ).read_text(encoding="utf-8")
                )
                self.assertEqual(
                    len(payload["slices"]), len(self.promotion_scope.binding_ids)
                )

    def test_cli_requires_an_explicit_promotion_scope(self):
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaises(SystemExit) as caught:
                self._run(Path(raw))
            self.assertEqual(caught.exception.code, 2)
