from __future__ import annotations

import copy
import json
import tempfile
import time
import unittest
from unittest.mock import patch
from pathlib import Path

import yaml

from qdl.adapters.vn import build_dnse_bar_raw_envelope
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge
from qdl.runtime.stable_vn_edge import StableDnseVendorEdge
from qdl.runtime.stable_catalog import StableSourceCatalog
from scripts.phaseb_prepare_stable_candidate import prepare_candidate

from qdl.runtime.stable_deployment import (
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
                    "ingestor-binance-spot.json",
                    "ingestor-binance-usdm.json",
                    "ingestor-okx-spot.json",
                    "ingestor-okx-swap.json",
                },
            )
            persisted = json.loads((Path(directory) / "core.json").read_text())
            self.assertEqual(persisted, core)

    def test_binance_bar_edge_publishes_each_closed_bar_once(self):
        class Publisher:
            def __init__(self):
                self.batches = []

            def publish_many(self, values):
                batch = tuple(values)
                self.batches.append(batch)
                return tuple(range(len(batch)))

        class Envelope:
            def __init__(self, open_time_ms):
                self.raw_frame_bytes = json.dumps(
                    {"row": [open_time_ms, "1", "1", "1", "1", "1", open_time_ms + 59999]}
                ).encode()

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
            side_effect=(Envelope(60_000), Envelope(60_000), Envelope(60_000), Envelope(60_000)),
        ):
            self.assertEqual(edge.run_cycle(), 2)
            self.assertEqual(edge.run_cycle(), 0)
        self.assertEqual([len(batch) for batch in publisher.batches], [2])

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
        self.assertEqual(
            compose["x-kafka-env"]["KAFKA_MIN_INSYNC_REPLICAS"], 2
        )
        self.assertEqual(
            compose["x-kafka-env"]["KAFKA_DEFAULT_REPLICATION_FACTOR"], 3
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
        self.assertEqual(
            services["rust_core"]["entrypoint"],
            ["/usr/local/bin/qdl-realtime-core"],
        )

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
                self.assertTrue((output / "runtime/core.json").is_file())
                self.assertTrue((output / "identities/projector/client.key").is_file())


if __name__ == "__main__":
    unittest.main()
