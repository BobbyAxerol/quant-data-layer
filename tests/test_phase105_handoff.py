from __future__ import annotations

import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from qdl.certification.phase105_handoff import (
    ALL_KEY_SUBJECTS,
    V1_FALLBACK_COMMIT,
    V1_FALLBACK_VERSION,
    active_query_environment_commitment,
    active_runtime_binding,
    handoff_packet,
    load_dotenv,
    prepare_handoff_environment,
    public_handoff_overlay,
    render_dotenv,
    sha256_file,
    validate_active_query_environment_commitment,
    v1_image_attestation,
)
from scripts.phase105_prepare_handoff_bundle import main as prepare_handoff_main


class Phase105HandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base = {
            "QDL_STABLE_RUNTIME_DIR": "/runtime",
            "QDL_STABLE_RUST_IMAGE": "sha256:" + "a" * 64,
            "QDL_STABLE_JWT_KEYS_JSON": json.dumps({
                "stable-trading-system-rs256-v1": "-----BEGIN PUBLIC KEY-----\\ntrading\\n-----END PUBLIC KEY-----",
                "stable-alpha-binance-rs256-v1": "-----BEGIN PUBLIC KEY-----\\nalpha\\n-----END PUBLIC KEY-----",
            }),
        }

    def _extension(self, root: Path) -> Path:
        for relative in (
            "monitoring-jwt/public.pem",
            "alpha-okx-jwt/public.pem",
            "reference-l2-jwt/public.pem",
        ):
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                "-----BEGIN PUBLIC KEY-----\\n" + relative + "\\n-----END PUBLIC KEY-----\\n",
                encoding="utf-8",
            )
        (root / "client-ca-bundle.crt").write_text("server-ca\\nexternal-ca\\n", encoding="utf-8")
        return root

    def _final_bar_packet(self) -> dict[str, object]:
        runtime_dir = "/home/bobby/.local/state/qdl-v2/phase105c-test/runtime"
        rust_image = "sha256:" + "d" * 64
        python_image = "sha256:" + "e" * 64
        authority_sha256 = "f" * 64
        services = {}
        for service in (
            "ingestor_okx_swap",
            "binance_bar_edge",
            "rust_core",
            "query_v2_1",
            "query_v2_2",
            "stream_v2_active",
            "stream_v2_passive",
        ):
            services[service] = {
                "image_digest": "sha256:" + ("1" if service == "ingestor_okx_swap" else "2") * 64,
                "runtime_dir": "/home/bobby/.local/state/qdl-v2/phase105c-r10/runtime",
                "checkpoint_path": (
                    "/var/lib/qdl-stable/runtime/stable-crypto-bar-edge-r10.json"
                    if service == "binance_bar_edge" else None
                ),
            }
        return {
            "schema": "qdl.phase105c.final-bar-repair.v1",
            "compose_environment": {
                "QDL_CONFIG_REVISION": "phase105c-final-bar-r10",
                "QDL_STABLE_PYTHON_IMAGE": python_image,
                "QDL_STABLE_RUNTIME_DIR": runtime_dir,
                "QDL_STABLE_RUST_IMAGE": rust_image,
            },
            "runtime": {
                "authority_bytes_preserved": True,
                "authority_mode": "RUST_PRIMARY",
                "authority_revision": 1,
                "authority_sha256": authority_sha256,
                "host_runtime_dir": runtime_dir,
                "python_image_digest": python_image,
                "rust_image_digest": rust_image,
                "runtime_files": {"authority.json": authority_sha256},
            },
            "final_bar": {
                "previous_checkpoint_path": "/var/lib/qdl-stable/runtime/stable-crypto-bar-edge-r10.json",
            },
            "rollback": {
                "schema": "qdl.phase105c.final-bar-repair.rollback.v1",
                "services": services,
                "durable_data_deletion": False,
            },
        }

    def test_load_dotenv_unquotes_standard_private_env_values(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "stable.env"
            path.write_text(
                "QDL_STABLE_JWT_KEYS_JSON='{\"key\":\"value\"}'\n"
                "QDL_STABLE_RUNTIME_DIR=/runtime\n",
                encoding="utf-8",
            )
            values = load_dotenv(path)
        self.assertEqual(values["QDL_STABLE_JWT_KEYS_JSON"], '{"key":"value"}')
        self.assertEqual(values["QDL_STABLE_RUNTIME_DIR"], "/runtime")

    def test_environment_has_exact_five_key_subject_bindings(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            values = prepare_handoff_environment(
                self.base,
                extension_dir=self._extension(Path(raw)),
                python_image="sha256:" + "b" * 64,
            )
        self.assertEqual(values["QDL_STABLE_PYTHON_IMAGE"], "sha256:" + "b" * 64)
        self.assertEqual(set(json.loads(values["QDL_STABLE_JWT_KEYS_JSON"])), set(ALL_KEY_SUBJECTS))
        self.assertEqual(json.loads(values["QDL_STABLE_JWT_KEY_SUBJECTS_JSON"]), ALL_KEY_SUBJECTS)
        self.assertNotIn("PRIVATE KEY", values["QDL_STABLE_JWT_KEYS_JSON"])

    def test_active_runtime_packet_only_overlays_allowlisted_selectors(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = {
                **self.base,
                "QDL_STABLE_SCHEMA_DIGEST": "c" * 64,
                "QDL_CONFIG_REVISION": "older",
                "QDL_STABLE_AUTHORITY_MODE": "RUST_SHADOW",
                "QDL_STABLE_AUTHORITY_REVISION": "0",
            }
            active = {
                "schema": "qdl.v2.shared-primary-handoff-packet.v2",
                "authority": {
                    "mode": "RUST_PRIMARY",
                    "revision": 1,
                    "public_write_allowed": False,
                    "legacy_write_allowed": False,
                    "contract_digest": "c" * 64,
                },
                "compose_environment": {
                    "QDL_CONFIG_REVISION": "phase103-shared-primary-r1",
                    "QDL_STABLE_AUTHORITY_MODE": "RUST_PRIMARY",
                    "QDL_STABLE_AUTHORITY_REVISION": "1",
                    "QDL_STABLE_RUNTIME_DIR": "/home/bobby/.local/state/qdl-v2/packet/runtime",
                    "QDL_STABLE_RUST_IMAGE": "sha256:" + "d" * 64,
                },
                "runtime_bundle": {"rust_image_digest": "sha256:" + "d" * 64},
            }
            binding = active_runtime_binding(base, active)
            values = prepare_handoff_environment(
                base,
                extension_dir=self._extension(Path(raw)),
                python_image="sha256:" + "e" * 64,
                runtime_binding=binding,
            )
        self.assertEqual(values["QDL_STABLE_RUNTIME_DIR"], active["compose_environment"]["QDL_STABLE_RUNTIME_DIR"])
        self.assertEqual(values["QDL_STABLE_AUTHORITY_MODE"], "RUST_PRIMARY")
        self.assertEqual(values["QDL_STABLE_RUST_IMAGE"], "sha256:" + "d" * 64)
        self.assertEqual(values["QDL_STABLE_SCHEMA_DIGEST"], "c" * 64)
        active["authority"]["contract_digest"] = "x" * 64
        with self.assertRaisesRegex(ValueError, "contract digest"):
            active_runtime_binding(base, active)

    def test_final_bar_runtime_packet_requires_preserved_authority_and_exact_images(self) -> None:
        packet = self._final_bar_packet()
        binding = active_runtime_binding(self.base, packet)
        self.assertEqual(binding["packet_schema"], "qdl.phase105c.final-bar-repair.v1")
        self.assertEqual(binding["selectors"]["QDL_CONFIG_REVISION"], "phase105c-final-bar-r10")
        self.assertEqual(binding["python_image_digest"], "sha256:" + "e" * 64)
        runtime = packet["runtime"]
        assert isinstance(runtime, dict)
        runtime["authority_bytes_preserved"] = False
        with self.assertRaisesRegex(ValueError, "authority evidence"):
            active_runtime_binding(self.base, packet)

    def test_final_bar_runtime_packet_rejects_stale_path_rust_or_authority_hash(self) -> None:
        for field, value in (
            ("host_runtime_dir", "/tmp/qdl-v2/runtime"),
            ("rust_image_digest", "sha256:" + "9" * 64),
        ):
            with self.subTest(field=field):
                packet = self._final_bar_packet()
                runtime = packet["runtime"]
                assert isinstance(runtime, dict)
                runtime[field] = value
                with self.assertRaisesRegex(ValueError, "runtime image or path|runtime directory"):
                    active_runtime_binding(self.base, packet)
        packet = self._final_bar_packet()
        runtime = packet["runtime"]
        assert isinstance(runtime, dict)
        runtime["runtime_files"] = {"authority.json": "0" * 64}
        with self.assertRaisesRegex(ValueError, "authority evidence"):
            active_runtime_binding(self.base, packet)

    def test_final_bar_runtime_packet_binds_the_python_image(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            binding = active_runtime_binding(self.base, self._final_bar_packet())
            with self.assertRaisesRegex(ValueError, "Python image differs"):
                prepare_handoff_environment(
                    self.base,
                    extension_dir=self._extension(Path(raw)),
                    python_image="sha256:" + "1" * 64,
                    runtime_binding=binding,
                )

    def test_final_bar_runtime_packet_rejects_incomplete_or_ambiguous_rollback(self) -> None:
        packet = self._final_bar_packet()
        rollback = packet["rollback"]
        assert isinstance(rollback, dict)
        services = rollback["services"]
        assert isinstance(services, dict)
        services.pop("rust_core")
        with self.assertRaisesRegex(ValueError, "rollback services"):
            active_runtime_binding(self.base, packet)
        packet = self._final_bar_packet()
        rollback = packet["rollback"]
        assert isinstance(rollback, dict)
        services = rollback["services"]
        assert isinstance(services, dict)
        services["binance_bar_edge"]["checkpoint_path"] = "/var/lib/qdl-stable/runtime/other.json"
        with self.assertRaisesRegex(ValueError, "rollback checkpoint"):
            active_runtime_binding(self.base, packet)

    def test_prepare_cli_writes_only_a_public_overlay(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            base = {
                **self.base,
                "QDL_STABLE_SCHEMA_DIGEST": "c" * 64,
                "QDL_STABLE_INTERNAL_INGEST_SECRET": "test-secret-not-output",
                "QDL_STABLE_CURSOR_KEYS_JSON": json.dumps({"stable-k2": "cursor"}),
            }
            packet = self._final_bar_packet()
            runtime = active_runtime_binding(base, packet)
            current = {
                "QDL_STABLE_INTERNAL_INGEST_SECRET": base["QDL_STABLE_INTERNAL_INGEST_SECRET"],
                "QDL_STABLE_CURSOR_KEYS_JSON": base["QDL_STABLE_CURSOR_KEYS_JSON"],
                "QDL_DATA_JWT_KEYS_JSON": base["QDL_STABLE_JWT_KEYS_JSON"],
                "QDL_STABLE_SCHEMA_DIGEST": base["QDL_STABLE_SCHEMA_DIGEST"],
                "QDL_STABLE_AUTHORITY_MODE": "RUST_PRIMARY",
                "QDL_STABLE_AUTHORITY_REVISION": "1",
                "QDL_CONFIG_REVISION": "phase105c-final-bar-r10",
            }
            expected = prepare_handoff_environment(
                base,
                extension_dir=self._extension(root / "expected-extension"),
                python_image="sha256:" + "e" * 64,
                runtime_binding=runtime,
            )
            current["QDL_DATA_JWT_KEYS_JSON"] = expected["QDL_STABLE_JWT_KEYS_JSON"]
            commitment = active_query_environment_commitment(
                base, current, runtime, json.loads(expected["QDL_STABLE_JWT_KEYS_JSON"])
            )
            binding = {
                "schema": "qdl.phase105.active-query-env-commitment.v1",
                "status": "PASS",
                "service": "query_v2_1",
                "container_image_id": "sha256:" + "e" * 64,
                "container_id_sha256": "f" * 64,
                **commitment,
            }
            base_path = root / "stable.env"
            packet_path = root / "runtime.json"
            commitment_path = root / "query-proof.json"
            provenance_path = root / "v1.json"
            output = root / "output"
            base_path.write_text(render_dotenv(base), encoding="utf-8")
            packet_path.write_text(json.dumps(packet), encoding="utf-8")
            commitment_path.write_text(json.dumps(binding), encoding="utf-8")
            provenance_path.write_text(json.dumps({"status": "PASS"}), encoding="utf-8")
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                status = prepare_handoff_main([
                    "--base-env", str(base_path),
                    "--active-runtime-packet", str(packet_path),
                    "--active-query-commitment", str(commitment_path),
                    "--extension-dir", str(self._extension(root / "extension")),
                    "--python-image", "sha256:" + "e" * 64,
                    "--v1-provenance", str(provenance_path),
                    "--output-dir", str(output),
                ])
            self.assertEqual(status, 0)
            self.assertFalse(output.exists())
            self.assertNotIn("test-secret-not-output", stdout.getvalue())
            preview = json.loads(stdout.getvalue())
            self.assertEqual(preview["recreated_services"], [
                "query_v2_1", "query_v2_2", "stream_v2_active", "stream_v2_passive",
            ])

    def test_active_query_commitment_keeps_secret_values_out_of_handoff_output(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = {
                **self.base,
                "QDL_STABLE_SCHEMA_DIGEST": "c" * 64,
                "QDL_STABLE_INTERNAL_INGEST_SECRET": "unchanged-secret",
                "QDL_STABLE_CURSOR_KEYS_JSON": json.dumps({"stable-k2": "rotated"}),
            }
            active = {
                "schema": "qdl.v2.shared-primary-handoff-packet.v2",
                "authority": {
                    "mode": "RUST_PRIMARY", "revision": 1,
                    "public_write_allowed": False, "legacy_write_allowed": False,
                    "contract_digest": "c" * 64,
                },
                "compose_environment": {
                    "QDL_CONFIG_REVISION": "phase103-shared-primary-r1",
                    "QDL_STABLE_AUTHORITY_MODE": "RUST_PRIMARY",
                    "QDL_STABLE_AUTHORITY_REVISION": "1",
                    "QDL_STABLE_RUNTIME_DIR": "/home/bobby/.local/state/qdl-v2/packet/runtime",
                    "QDL_STABLE_RUST_IMAGE": "sha256:" + "d" * 64,
                },
                "runtime_bundle": {"rust_image_digest": "sha256:" + "d" * 64},
            }
            runtime = active_runtime_binding(base, active)
            current = {
                "QDL_STABLE_INTERNAL_INGEST_SECRET": "unchanged-secret",
                "QDL_STABLE_CURSOR_KEYS_JSON": json.dumps({"stable-k2": "rotated"}),
                "QDL_DATA_JWT_KEYS_JSON": base["QDL_STABLE_JWT_KEYS_JSON"],
                "QDL_STABLE_SCHEMA_DIGEST": "c" * 64,
                "QDL_CONFIG_REVISION": "phase103-shared-primary-r1",
                "QDL_STABLE_AUTHORITY_MODE": "RUST_PRIMARY",
                "QDL_STABLE_AUTHORITY_REVISION": "1",
            }
            expected = prepare_handoff_environment(
                base,
                extension_dir=self._extension(Path(raw) / "expected-extension"),
                python_image="sha256:" + "e" * 64,
                runtime_binding=runtime,
            )
            current["QDL_DATA_JWT_KEYS_JSON"] = expected["QDL_STABLE_JWT_KEYS_JSON"]
            commitment = active_query_environment_commitment(
                base, current, runtime, json.loads(expected["QDL_STABLE_JWT_KEYS_JSON"])
            )
            raw_commitment = {
                "schema": "qdl.phase105.active-query-env-commitment.v1",
                "status": "PASS",
                "service": "query_v2_1",
                "container_image_id": "sha256:" + "e" * 64,
                "container_id_sha256": "f" * 64,
                **commitment,
            }
            verified = validate_active_query_environment_commitment(
                base, runtime, raw_commitment, json.loads(expected["QDL_STABLE_JWT_KEYS_JSON"])
            )
            values = prepare_handoff_environment(
                base,
                extension_dir=self._extension(Path(raw)),
                python_image="sha256:" + "e" * 64,
                runtime_binding=runtime,
            )
            overlay = public_handoff_overlay(values)
        self.assertEqual(values["QDL_STABLE_CURSOR_KEYS_JSON"], base["QDL_STABLE_CURSOR_KEYS_JSON"])
        self.assertEqual(
            set(json.loads(overlay["QDL_STABLE_JWT_KEYS_JSON"])),
            set(ALL_KEY_SUBJECTS),
        )
        self.assertEqual(set(overlay), {
            "QDL_STABLE_JWT_KEYS_JSON", "QDL_STABLE_JWT_KEY_SUBJECTS_JSON",
        })
        self.assertNotIn("unchanged-secret", json.dumps(overlay, sort_keys=True))
        self.assertEqual(set(verified["verified_keys"]), set(current))
        current["QDL_DATA_JWT_KEYS_JSON"] = base["QDL_STABLE_JWT_KEYS_JSON"]
        with self.assertRaisesRegex(ValueError, "JWT keyring mismatches public overlay"):
            active_query_environment_commitment(
                base, current, runtime, json.loads(expected["QDL_STABLE_JWT_KEYS_JSON"])
            )
        current["QDL_DATA_JWT_KEYS_JSON"] = expected["QDL_STABLE_JWT_KEYS_JSON"]
        current["QDL_STABLE_INTERNAL_INGEST_SECRET"] = "mismatch"
        with self.assertRaisesRegex(ValueError, "mismatches controlled reference"):
            active_query_environment_commitment(
                base, current, runtime, json.loads(expected["QDL_STABLE_JWT_KEYS_JSON"])
            )

    def test_environment_rejects_missing_extra_private_mismatched_and_wrong_subject_keys(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = self._extension(Path(raw))
            cases = []

            missing = Path(raw) / "missing"
            self._extension(missing)
            (missing / "reference-l2-jwt/public.pem").unlink()
            cases.append((dict(self.base), missing, "public key is missing"))

            extra = dict(self.base)
            extra["QDL_STABLE_JWT_KEYS_JSON"] = json.dumps({
                **json.loads(extra["QDL_STABLE_JWT_KEYS_JSON"]),
                "unexpected": "-----BEGIN PUBLIC KEY-----\\nx\\n-----END PUBLIC KEY-----",
            })
            cases.append((extra, root, "exactly match"))

            private = Path(raw) / "private"
            self._extension(private)
            (private / "reference-l2-jwt/public.pem").write_text(
                "-----BEGIN PRIVATE KEY-----\\nprivate\\n-----END PRIVATE KEY-----\\n",
                encoding="utf-8",
            )
            cases.append((dict(self.base), private, "public key is invalid"))

            mismatched = dict(self.base)
            mismatched["QDL_STABLE_JWT_KEYS_JSON"] = json.dumps({
                **json.loads(mismatched["QDL_STABLE_JWT_KEYS_JSON"]),
                "stable-reference-l2-rs256-v1": "-----BEGIN PUBLIC KEY-----\\nwrong\\n-----END PUBLIC KEY-----",
            })
            cases.append((mismatched, root, "conflicts"))

            wrong_subject = dict(self.base)
            wrong_subject["QDL_STABLE_JWT_KEY_SUBJECTS_JSON"] = json.dumps({
                "stable-alpha-binance-rs256-v1": "spiffe://qdl/paper/wrong",
            })
            cases.append((wrong_subject, root, "conflict with approved"))

            for base, extension, error in cases:
                with self.subTest(error=error):
                    with self.assertRaisesRegex(ValueError, error):
                        prepare_handoff_environment(
                            base, extension_dir=extension, python_image="image"
                        )

    def test_v1_attestation_requires_all_frozen_labels(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            dockerfile = Path(raw) / "Dockerfile"
            dockerfile.write_text("FROM scratch\\n", encoding="utf-8")
            tree = "c" * 40
            image = {
                "Id": "sha256:" + "d" * 64,
                "Config": {"Labels": {
                    "org.opencontainers.image.revision": V1_FALLBACK_COMMIT,
                    "org.opencontainers.image.version": V1_FALLBACK_VERSION,
                    "io.qdl.source-tree": tree,
                    "io.qdl.dockerfile-sha256": sha256_file(dockerfile),
                }},
            }
            evidence = v1_image_attestation(
                image,
                source_commit=V1_FALLBACK_COMMIT,
                source_tree=tree,
                dockerfile_sha256=sha256_file(dockerfile),
            )
        self.assertEqual(evidence["status"], "PASS")
        image["Config"]["Labels"].pop("io.qdl.source-tree")
        with self.assertRaisesRegex(ValueError, "labels"):
            v1_image_attestation(
                image,
                source_commit=V1_FALLBACK_COMMIT,
                source_tree=tree,
                dockerfile_sha256="a" * 64,
            )

    def test_packet_contains_hashes_but_no_private_material(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = self._extension(Path(raw))
            env = prepare_handoff_environment(self.base, extension_dir=root, python_image="image")
            v1 = {
                "schema": "qdl.phase105.v1-fallback-provenance.v1",
                "status": "PASS",
            }
            packet = handoff_packet(environment=env, extension_dir=root, v1_attestation=v1)
        encoded = json.dumps(packet, sort_keys=True)
        self.assertEqual(packet["recreated_services"], [
            "query_v2_1", "query_v2_2", "stream_v2_active", "stream_v2_passive",
        ])
        self.assertNotIn("rust_core", packet["recreated_services"])
        self.assertNotIn("data_layer_service", packet["recreated_services"])
        self.assertNotIn("PRIVATE KEY", encoded)
        self.assertEqual(packet["jwt_subjects"], ALL_KEY_SUBJECTS)

    def test_compose_overrides_remain_bounded(self) -> None:
        root = Path(__file__).resolve().parents[1]
        v2 = (root / "docker-compose.phase105c.override.yml").read_text(encoding="utf-8")
        v1 = (root / "docker-compose.phase105c-v1-fallback.yml").read_text(encoding="utf-8")
        for name in ("rust_core", "rust_core_2", "rust_core_3"):
            self.assertIn(f"{name}:", v2)
        self.assertEqual(v2.count("QDL_PHASE105C_RUST_CORE_MEMORY_LIMIT"), 3)
        self.assertIn("QDL_STABLE_TLS_CLIENT_CA_FILE", v2)
        c2 = (root / "docker-compose.phase105c-c2.override.yml").read_text(encoding="utf-8")
        self.assertIn("query_v2_1:", c2)
        self.assertIn("query_v2_2:", c2)
        self.assertIn("stream_v2_active:", c2)
        self.assertIn("stream_v2_passive:", c2)
        self.assertNotRegex(c2, r"(?m)^  rust_core:\\s*$")
        # C3.6 keeps the existing admission endpoint available to the query
        # adapters. C2 can reference that endpoint but must never configure or
        # recreate rust_core itself.
        self.assertIn("QDL_STABLE_PROVIDER_ADMISSION_URL: http://rust_core:8300", c2)
        self.assertNotIn("entrypoint:", c2)
        self.assertNotIn("command:", c2)
        self.assertNotIn("volumes:", c2)
        self.assertNotIn("data_layer_service:", c2)
        self.assertIn("build: !reset null", v1)
        self.assertIn("volumes: !override", v1)
        self.assertNotIn(":/app\n", v1)


if __name__ == "__main__":
    unittest.main()
