from __future__ import annotations

import asyncio
import copy
import json
import tempfile
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from pathlib import Path

import yaml

from qdl.adapters.vn import build_dnse_bar_raw_envelope
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge
from qdl.runtime.stable_vn_edge import StableDnseVendorEdge
from qdl.runtime.stable_catalog import StableSourceCatalog
from scripts.phaseb_prepare_stable_candidate import prepare_candidate

from qdl.runtime.stable_deployment import (
    STABLE_CORE_WORKER_COUNT,
    StableAcquisitionPlan,
    stable_authority_record,
    write_stable_runtime_bundle,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"


class StableDeploymentContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.acquisition = StableAcquisitionPlan.load(
            ACQUISITION_PATH, catalog=self.catalog
        )
        self.authority = stable_authority_record(
            rust_image_digest="a" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION_PATH.read_bytes(),
            effective_at_ns=time.time_ns(),
        )

    def test_all_catalog_bindings_have_one_capability_truthful_acquisition(self):
        self.assertEqual(len(self.catalog.bindings), 16)
        self.assertEqual(len(self.acquisition.bindings), 16)
        modes = {item.mode for item in self.acquisition.bindings}
        self.assertEqual(modes, {"RUST_NATIVE", "PYTHON_REST", "PYTHON_VENDOR_SDK"})
        native = self.acquisition.native_ingestor_configs(
            catalog=self.catalog, authority=self.authority
        )
        self.assertEqual(
            set(native),
            {"binance-usdm", "binance-spot", "okx-swap", "okx-spot"},
        )
        self.assertEqual(sum(len(item["bindings"]) for item in native.values()), 10)
        self.assertTrue(all(item["authority"]["mode"] == "RUST_SHADOW" for item in native.values()))
        self.assertEqual(
            {item["max_inflight_publishes"] for item in native.values()}, {512}
        )
        generation_paths = {
            item["generation_state_path"] for item in native.values()
        }
        self.assertEqual(len(generation_paths), 4)
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
            {"TRADE": "LOSSLESS", "QUOTE": "LATEST_STATE", "BAR": "LOSSLESS"},
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
            },
        )

    def test_core_bundle_uses_stable_identity_lineage_and_never_enables_public_writes(self):
        core = self.acquisition.core_config(
            catalog=self.catalog, authority=self.authority
        )
        bindings = core["core"]["bindings"]
        expected = {item.instrument.instrument_uid for item in self.catalog.bindings}
        self.assertEqual({item["instrument_uid"] for item in bindings}, expected)
        self.assertEqual(
            {item["source_id"] for item in bindings},
            {item.source_id for item in self.catalog.bindings},
        )
        finality_by_source = {
            item.source_id: item.require_final_bar for item in self.catalog.bindings
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
                    "ingestor-binance-spot.json",
                    "ingestor-binance-usdm.json",
                    "ingestor-okx-spot.json",
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
        with patch(
            "qdl.runtime.stable_bar_edge.fetch_latest_closed_bar_raw_envelope",
            side_effect=(
                Envelope("BINANCE", 60_000), Envelope("BINANCE", 60_000),
                Envelope("BINANCE", 60_000), Envelope("BINANCE", 60_000),
            ),
        ), patch(
            "qdl.runtime.stable_bar_edge.fetch_okx_latest",
            side_effect=(
                Envelope("OKX", 60_000), Envelope("OKX", 60_000),
                Envelope("OKX", 60_000), Envelope("OKX", 60_000),
            ),
        ):
            self.assertEqual(edge.run_cycle(), 4)
            self.assertEqual(edge.run_cycle(), 0)
        self.assertEqual([len(batch) for batch in publisher.batches], [4])

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
        envelope = build_dnse_bar_raw_envelope(
            {"t": 1_786_352_340, "o": "1820.7", "h": "1821.2",
             "l": "1820.2", "c": "1820.7", "v": "0"},
            edge._binding(source),
            received_at_ns=1_786_352_400_000_000_000,
        )
        payload = json.loads(envelope.raw_frame_bytes)
        self.assertEqual(payload["interval"], "1m")
        self.assertEqual(payload["v"], "0")
        self.assertTrue(payload["is_final"])
        self.assertFalse(envelope.test_provenance)

    def test_dnse_history_bootstrap_retries_validates_and_publishes_once(self):
        class Publisher:
            def __init__(self):
                self.batches = []

            def publish_many(self, values):
                batch = tuple(values)
                self.batches.append(batch)
                return tuple(range(len(batch)))

        rows = [
            {"t": value, "o": "100", "h": "101", "l": "99", "c": "100.5", "v": "10"}
            for value in (100, 160, 220)
        ]
        calls = []
        sleeps = []

        def fetcher(symbol, resolution, start, end):
            calls.append((symbol, resolution, start, end))
            if len(calls) == 1:
                raise TimeoutError("injected transient DNSE timeout")
            return rows

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
            clock=lambda: 400.0,
            sleep=sleeps.append,
        )
        self.assertEqual(edge.bootstrap_history(), 4)
        self.assertEqual(edge.bootstrap_history(), 0)
        self.assertEqual([len(batch) for batch in publisher.batches], [2, 2])
        self.assertEqual(sleeps, [1])
        self.assertEqual(set(edge._last_bar_open_ms), {"FPT", "VN30F1M"})
        self.assertTrue(all(
            not item.test_provenance
            for batch in publisher.batches
            for item in batch
        ))

    def test_dnse_history_conflict_partial_and_closed_session_fail_safe(self):
        base = {"t": 100, "o": "100", "h": "101", "l": "99", "c": "100", "v": "1"}
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

        closed_source = edge.bar_sources["VN30F1M"]
        # 2026-08-22 is Saturday in Asia/Ho_Chi_Minh.
        closed = datetime(2026, 8, 22, 3, tzinfo=timezone.utc).timestamp()
        edge.clock = lambda: closed
        self.assertFalse(edge._market_open(closed_source))
        edge.history_fetcher = lambda *_args: (_ for _ in ()).throw(
            AssertionError("closed session must not call DNSE REST")
        )
        self.assertEqual(asyncio.run(edge.poll_bars_once()), 0)

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
                set(services[name]["networks"]), {"stable_internal", "stable_ingress"}
            )
            self.assertTrue(
                all(str(port).startswith("127.0.0.1:") for port in services[name]["ports"])
            )
        self.assertNotIn("ports", services["projector_v2"])
        self.assertEqual(services["projector_v2"]["networks"], ["stable_internal"])
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
        self.assertIn("stable_tls:/stable-certs:ro", services["projector_v2"]["volumes"])
        self.assertNotIn("/certs:ro", " ".join(services["projector_v2"]["volumes"]))
        for name in (
            "query_v2_1", "query_v2_2", "stream_v2_active",
            "stream_v2_passive", "projector_v2",
        ):
            with self.subTest(service=name):
                self.assertEqual(services[name]["user"], "10001:10001")
                self.assertTrue(services[name]["read_only"])
                self.assertIn("ALL", services[name]["cap_drop"])
                self.assertEqual(services[name]["restart"], "no")
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

    def test_candidate_bundle_uses_image_ids_and_never_records_secret_values(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phaseb-cert-") as cert_directory:
            certs = Path(cert_directory)
            (certs / "ca.crt").write_text("ca", encoding="ascii")
            for principal in ("phase8-producer", "phase8-core", "phase8-consumer"):
                (certs / f"{principal}.crt").write_text("crt", encoding="ascii")
                (certs / f"{principal}.key").write_text("key", encoding="ascii")
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
                        host_cert_dir=Path("/host/qdl/certs"),
                        host_output_dir=Path("/host/qdl/candidate"),
                    )
                self.assertFalse(manifest["cutover_authorized"])
                self.assertFalse(manifest["secret_values_recorded"])
                self.assertEqual((output / "stable.env").stat().st_mode & 0o777, 0o600)
                env_text = (output / "stable.env").read_text()
                self.assertIn("QDL_STABLE_CERT_DIR=/host/qdl/certs", env_text)
                self.assertIn(
                    "QDL_STABLE_RUNTIME_DIR=/host/qdl/candidate/runtime", env_text
                )
                public_manifest = (output / "candidate-manifest.json").read_text()
                self.assertNotIn("QDL_STABLE_INTERNAL_INGEST_SECRET", public_manifest)
                for name in ("core.json", "core-002.json", "core-003.json"):
                    self.assertTrue((output / f"runtime/{name}").is_file())
                self.assertTrue((output / "identities/projector/client.key").is_file())


if __name__ == "__main__":
    unittest.main()
