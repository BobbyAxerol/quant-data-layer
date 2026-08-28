from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
import uuid
from pathlib import Path

import yaml

from qdl.consumer.stable import (
    StablePrimaryConsumerRoutePlan,
    primary_fallback_return_drill,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    stable_authority_record,
)
from scripts.phase103_prepare_shared_primary_packet import (
    _ALLOWED_SERVICE_ORDER,
    prepare_shared_primary_packet,
    validate_prepared_shared_primary_bundle,
    validate_shared_primary_packet,
)
from scripts.phase103_packet_contract import (
    SHARED_PRIMARY_CRYPTO_BINDING_COUNT,
    authority_scoped_bar_state_path,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
ROUTE_PATH = ROOT / "config/v2/stable-primary-consumer-routing.yaml"


class SharedPrimaryConsumerRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.acquisition = StableAcquisitionPlan.load(
            ACQUISITION_PATH,
            catalog=self.catalog,
        )

    def load(self) -> StablePrimaryConsumerRoutePlan:
        return StablePrimaryConsumerRoutePlan.load(
            ROUTE_PATH,
            manifest_root=ROOT,
            catalog=self.catalog,
        )

    def authority(self, *, mode: str = "RUST_PRIMARY") -> dict[str, object]:
        return stable_authority_record(
            rust_image_digest="a" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION_PATH.read_bytes(),
            effective_at_ns=1_800_000_000_000_000_000,
            mode=mode,
            revision=7,
            slice_id="qdl-test-shared-primary",
            approved_by="phase103-test",
        )

    def test_all_registered_consumers_have_v2_primary_and_v1_rollback_contracts(self):
        plan = self.load()
        self.assertEqual(plan.target_route, "V2_PRIMARY")
        self.assertEqual(plan.rollback_route, "V1")
        self.assertEqual(
            {item.consumer_id for item in plan.consumers},
            {
                "monitoring.multivenue.stable",
                "alpha.binance.paper.stable",
                "alpha.okx.paper.stable",
                "alpha.vn.paper.stable",
                "trading-system.paper.stable",
            },
        )
        sealed = plan.seal(self.authority())
        self.assertEqual(sealed["authority_mode"], "RUST_PRIMARY")
        self.assertEqual(sealed["target_route"], "V2_PRIMARY")
        self.assertEqual(sealed["rollback_route"], "V1")
        self.assertEqual(len(sealed["consumers"]), 5)
        self.assertEqual(len(sealed["route_plan_sha256"]), 64)

    def test_every_requirement_makes_the_exact_v2_v1_v2_route_transition(self):
        plan = self.load()
        drill = primary_fallback_return_drill(plan)
        expected_count = sum(len(item.manifest.requirements) for item in plan.consumers)
        self.assertEqual(drill["consumer_count"], 5)
        self.assertEqual(drill["requirement_count"], expected_count)
        self.assertTrue(drill["test_provenance"])
        self.assertTrue(all(
            item["before"] == "V2_PRIMARY"
            and item["fallback"] == "V1_FALLBACK"
            and item["returned"] == "V2_PRIMARY"
            for item in drill["transitions"]
        ))

    def test_route_plan_and_authority_fail_closed_when_unsafe(self):
        payload = yaml.safe_load(ROUTE_PATH.read_text(encoding="utf-8"))
        wrong_route = copy.deepcopy(payload)
        wrong_route["target_route"] = "V1"
        with self.assertRaisesRegex(ValueError, "V2 primary and V1 rollback"):
            StablePrimaryConsumerRoutePlan.from_mapping(
                wrong_route,
                manifest_root=ROOT,
                catalog=self.catalog,
            )

        duplicate = copy.deepcopy(payload)
        duplicate["consumers"].append(copy.deepcopy(duplicate["consumers"][0]))
        with self.assertRaisesRegex(ValueError, "IDs must be unique"):
            StablePrimaryConsumerRoutePlan.from_mapping(
                duplicate,
                manifest_root=ROOT,
                catalog=self.catalog,
            )

        with self.assertRaisesRegex(ValueError, "requires RUST_PRIMARY"):
            self.load().seal(self.authority(mode="RUST_SHADOW"))


class SharedPrimaryPacketTests(unittest.TestCase):
    def packet(
        self,
        output_dir: Path,
        *,
        host_runtime_dir: Path | None = None,
    ) -> dict[str, object]:
        return prepare_shared_primary_packet(
            output_dir=output_dir,
            rust_image_digest="sha256:" + "b" * 64,
            python_image_digest="sha256:" + "c" * 64,
            source_commit="0123456789abcdef",
            actor="BobbyAxerol",
            change_ticket="QDL-PHASE103-TEST",
            observation_seconds=300,
            issued_at_ns=1_800_000_000_000_000_000,
            host_runtime_dir=host_runtime_dir,
        )

    @staticmethod
    def reseal(packet: dict[str, object]) -> None:
        body = {
            key: value
            for key, value in packet.items()
            if key not in {"packet_id", "packet_sha256", "confirmation_token"}
        }
        digest = hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        packet["packet_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, digest))
        packet["packet_sha256"] = digest
        packet["confirmation_token"] = f"APPLY_QDL_PHASE103_{digest[:16]}"

    def test_packet_uses_one_shared_topology_and_has_review_only_rollback(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-packet-") as directory:
            output = Path(directory) / "packet"
            packet = self.packet(
                output,
                host_runtime_dir=Path("/host/qdl/phase103/runtime"),
            )
            validate_shared_primary_packet(packet)
            self.assertFalse(packet["apply_requested"])
            self.assertEqual(packet["production_mutations"], 0)
            self.assertEqual(packet["authority"]["mode"], "RUST_PRIMARY")
            self.assertEqual(
                packet["runtime_bundle"]["rust_image_digest"],
                "sha256:" + "b" * 64,
            )
            self.assertEqual(
                packet["runtime_bundle"]["python_image_digest"],
                "sha256:" + "c" * 64,
            )
            environment = packet["compose_environment"]
            self.assertEqual(
                environment["QDL_STABLE_RUNTIME_DIR"],
                "/host/qdl/phase103/runtime",
            )
            self.assertEqual(
                environment["QDL_STABLE_PYTHON_IMAGE"],
                "sha256:" + "c" * 64,
            )
            self.assertEqual(
                environment["QDL_STABLE_RUST_IMAGE"],
                "sha256:" + "b" * 64,
            )
            self.assertEqual(environment["QDL_STABLE_AUTHORITY_MODE"], "RUST_PRIMARY")
            self.assertEqual(environment["QDL_STABLE_AUTHORITY_REVISION"], "1")
            self.assertEqual(environment["QDL_CONFIG_REVISION"], "phase103-shared-primary-r1")
            self.assertEqual(
                environment["QDL_STABLE_BAR_STATE_PATH"],
                authority_scoped_bar_state_path(
                    packet["authority"],
                    python_image_digest="sha256:" + "c" * 64,
                ),
            )
            self.assertEqual(
                packet["deployment"]["services"],
                list(_ALLOWED_SERVICE_ORDER),
            )
            self.assertFalse(any(
                "production_core" in service
                for service in packet["deployment"]["services"]
            ))
            self.assertEqual(packet["rollback"]["consumer_route"], "V1")
            self.assertEqual(
                packet["rollback"]["stop_only_services"],
                list(_ALLOWED_SERVICE_ORDER),
            )
            self.assertEqual(
                packet["acceptance"]["crypto_binding_count"],
                SHARED_PRIMARY_CRYPTO_BINDING_COUNT,
            )
            handoff = packet["trading_system_handoff"]
            lock = handoff["route_lock"]
            self.assertEqual(lock["consumer_id"], "trading-system.paper.stable")
            self.assertEqual(lock["service"], "market_data")
            self.assertEqual(lock["route_manifest"]["revision"], 2)
            self.assertEqual(len(lock["route_manifest"]["identities"]), 8)
            self.assertEqual(
                lock["compose_override"]["okx_symbols"],
                ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
            )
            self.assertEqual(len(handoff["route_lock_sha256"]), 64)
            self.assertTrue((output / "shared-primary-handoff-packet.json").is_file())
            runtime = output / "runtime"
            self.assertEqual(
                {item.name for item in runtime.iterdir()},
                {
                    "authority.json",
                    "consumer-route-primary.json",
                    "core.json",
                    "core-002.json",
                    "core-003.json",
                    "ingestor-binance-usdm.json",
                    "ingestor-okx-swap.json",
                    "shared-primary-runtime-manifest.json",
                },
            )
            core = json.loads((runtime / "core.json").read_text(encoding="utf-8"))
            self.assertEqual(core["raw_topics"], ["md.raw.realtime.v2"])
            self.assertTrue(core["strict_subscription_scope"])
            enabled_bindings = sum(
                item.enabled
                for item in StableAcquisitionPlan.load(
                    ACQUISITION_PATH,
                    catalog=StableSourceCatalog.load(CATALOG_PATH),
                ).bindings
            )
            self.assertEqual(len(core["core"]["bindings"]), enabled_bindings)

    def test_checkpoint_path_fences_python_artifact_and_schema(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-packet-") as directory:
            first = self.packet(Path(directory) / "first")
            second = prepare_shared_primary_packet(
                output_dir=Path(directory) / "second",
                rust_image_digest="sha256:" + "b" * 64,
                python_image_digest="sha256:" + "d" * 64,
                source_commit="0123456789abcdef",
                actor="BobbyAxerol",
                change_ticket="QDL-PHASE103-TEST",
                observation_seconds=300,
                issued_at_ns=1_800_000_000_000_000_000,
            )
            self.assertEqual(first["authority"], second["authority"])
            self.assertNotEqual(
                first["compose_environment"]["QDL_STABLE_BAR_STATE_PATH"],
                second["compose_environment"]["QDL_STABLE_BAR_STATE_PATH"],
            )

    def test_packet_rejects_obsolete_topology_and_nonempty_output(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-packet-") as directory:
            root = Path(directory)
            packet = self.packet(root / "packet")
            obsolete = copy.deepcopy(packet)
            obsolete["deployment"]["services"].append("production_core_1")
            with self.assertRaisesRegex(ValueError, "packet integrity"):
                validate_shared_primary_packet(obsolete)
            self.reseal(obsolete)
            with self.assertRaisesRegex(ValueError, "service topology"):
                validate_shared_primary_packet(obsolete)

            bad_route = copy.deepcopy(packet)
            bad_route["consumer_route"]["sealed_route"]["authority_mode"] = "RUST_SHADOW"
            with self.assertRaisesRegex(ValueError, "packet integrity"):
                validate_shared_primary_packet(bad_route)
            self.reseal(bad_route)
            with self.assertRaisesRegex(ValueError, "route is not bound"):
                validate_shared_primary_packet(bad_route)

            bad_environment = copy.deepcopy(packet)
            bad_environment["compose_environment"]["QDL_STABLE_AUTHORITY_MODE"] = "RUST_SHADOW"
            self.reseal(bad_environment)
            with self.assertRaisesRegex(ValueError, "Compose environment differs"):
                validate_shared_primary_packet(bad_environment)

            bad_checkpoint = copy.deepcopy(packet)
            bad_checkpoint["compose_environment"]["QDL_STABLE_BAR_STATE_PATH"] = (
                "/var/lib/qdl-stable/runtime/stable-crypto-bar-edge-unsealed.json"
            )
            self.reseal(bad_checkpoint)
            with self.assertRaisesRegex(ValueError, "Compose environment differs"):
                validate_shared_primary_packet(bad_checkpoint)

            bad_topic = copy.deepcopy(packet)
            bad_topic["deployment"]["topic"]["partitions"] = 7
            self.reseal(bad_topic)
            with self.assertRaisesRegex(ValueError, "topic scope"):
                validate_shared_primary_packet(bad_topic)

            bad_acl = copy.deepcopy(packet)
            bad_acl["deployment"]["acl_intent"]["core"] = "phase8-consumer"
            self.reseal(bad_acl)
            with self.assertRaisesRegex(ValueError, "ACL scope"):
                validate_shared_primary_packet(bad_acl)

            bad_scope = copy.deepcopy(packet)
            bad_scope["acceptance"]["crypto_binding_count"] -= 1
            self.reseal(bad_scope)
            with self.assertRaisesRegex(ValueError, "acceptance scope"):
                validate_shared_primary_packet(bad_scope)

            bad_handoff = copy.deepcopy(packet)
            bad_handoff["trading_system_handoff"]["route_lock"]["route_manifest"][
                "identities"
            ][0]["native_symbol"] = "SOLUSDT"
            self.reseal(bad_handoff)
            with self.assertRaisesRegex(ValueError, "route scope"):
                validate_shared_primary_packet(bad_handoff)

            with self.assertRaisesRegex(FileExistsError, "must be empty"):
                self.packet(root / "packet")

    def test_packet_accepts_a_precreated_empty_output_directory(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-packet-") as directory:
            output = Path(directory) / "empty"
            output.mkdir()
            packet = self.packet(output, host_runtime_dir=output / "runtime")
            self.assertTrue((output / "shared-primary-handoff-packet.json").is_file())
            validate_prepared_shared_primary_bundle(
                packet,
                runtime_dir=output / "runtime",
                now_ns=1_800_000_000_000_000_001,
            )

    def test_packet_requires_bounded_observation_window_and_immutable_image(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-packet-") as directory:
            with self.assertRaisesRegex(ValueError, "immutable sha256"):
                prepare_shared_primary_packet(
                    output_dir=Path(directory) / "bad-image",
                    rust_image_digest="qdl-v2-rust:latest",
                    python_image_digest="sha256:" + "c" * 64,
                    source_commit="0123456789abcdef",
                    actor="BobbyAxerol",
                    change_ticket="QDL-PHASE103-TEST",
                    observation_seconds=300,
                    issued_at_ns=1_800_000_000_000_000_000,
                )
            with self.assertRaisesRegex(ValueError, "between 60 and 1800"):
                prepare_shared_primary_packet(
                    output_dir=Path(directory) / "bad-window",
                    rust_image_digest="sha256:" + "b" * 64,
                    python_image_digest="sha256:" + "c" * 64,
                    source_commit="0123456789abcdef",
                    actor="BobbyAxerol",
                    change_ticket="QDL-PHASE103-TEST",
                    observation_seconds=59,
                    issued_at_ns=1_800_000_000_000_000_000,
                )
            with self.assertRaisesRegex(ValueError, "python_image_digest"):
                prepare_shared_primary_packet(
                    output_dir=Path(directory) / "bad-python-image",
                    rust_image_digest="sha256:" + "b" * 64,
                    python_image_digest="qdl-v2-python:latest",
                    source_commit="0123456789abcdef",
                    actor="BobbyAxerol",
                    change_ticket="QDL-PHASE103-TEST",
                    observation_seconds=300,
                    issued_at_ns=1_800_000_000_000_000_000,
                )
            with self.assertRaisesRegex(ValueError, "rust_image_digest"):
                prepare_shared_primary_packet(
                    output_dir=Path(directory) / "bad-rust-hex",
                    rust_image_digest="sha256:" + "z" * 64,
                    python_image_digest="sha256:" + "c" * 64,
                    source_commit="0123456789abcdef",
                    actor="BobbyAxerol",
                    change_ticket="QDL-PHASE103-TEST",
                    observation_seconds=300,
                    issued_at_ns=1_800_000_000_000_000_000,
                )
            with self.assertRaisesRegex(ValueError, "host_runtime_dir"):
                prepare_shared_primary_packet(
                    output_dir=Path(directory) / "bad-host-runtime-dir",
                    rust_image_digest="sha256:" + "b" * 64,
                    python_image_digest="sha256:" + "c" * 64,
                    source_commit="0123456789abcdef",
                    actor="BobbyAxerol",
                    change_ticket="QDL-PHASE103-TEST",
                    observation_seconds=300,
                    issued_at_ns=1_800_000_000_000_000_000,
                    host_runtime_dir=Path("runtime"),
                )

    def test_prepared_bundle_rejects_tamper_extra_files_and_expiry(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-packet-") as directory:
            output = Path(directory) / "packet"
            packet = self.packet(output, host_runtime_dir=output / "runtime")
            runtime = output / "runtime"
            bundle = validate_prepared_shared_primary_bundle(
                packet,
                runtime_dir=runtime,
                now_ns=1_800_000_000_000_000_001,
            )
            self.assertEqual(bundle["status"], "PASS")
            self.assertEqual(bundle["runtime_file_count"], 8)
            with self.assertRaisesRegex(ValueError, "approved time window"):
                validate_prepared_shared_primary_bundle(
                    packet,
                    runtime_dir=runtime,
                    now_ns=packet["expires_at_ns"],
                )
            (runtime / "unexpected.json").write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "file set"):
                validate_prepared_shared_primary_bundle(
                    packet,
                    runtime_dir=runtime,
                    now_ns=1_800_000_000_000_000_001,
                )
            (runtime / "unexpected.json").unlink()
            (runtime / "authority.json").write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "file digest mismatch"):
                validate_prepared_shared_primary_bundle(
                    packet,
                    runtime_dir=runtime,
                    now_ns=1_800_000_000_000_000_001,
                )


if __name__ == "__main__":
    unittest.main()
