from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qdl.certification.phase105_handoff import (
    ALL_KEY_SUBJECTS,
    V1_FALLBACK_COMMIT,
    V1_FALLBACK_VERSION,
    handoff_packet,
    prepare_handoff_environment,
    sha256_file,
    v1_image_attestation,
)


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
        for relative in ("monitoring-jwt/public.pem", "alpha-okx-jwt/public.pem"):
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                "-----BEGIN PUBLIC KEY-----\\n" + relative + "\\n-----END PUBLIC KEY-----\\n",
                encoding="utf-8",
            )
        (root / "client-ca-bundle.crt").write_text("server-ca\\nexternal-ca\\n", encoding="utf-8")
        return root

    def test_environment_has_exact_four_key_subject_bindings(self) -> None:
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

    def test_environment_rejects_unapproved_existing_key(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = dict(self.base)
            base["QDL_STABLE_JWT_KEYS_JSON"] = json.dumps({
                **json.loads(base["QDL_STABLE_JWT_KEYS_JSON"]),
                "unexpected": "-----BEGIN PUBLIC KEY-----\\nx\\n-----END PUBLIC KEY-----",
            })
            with self.assertRaisesRegex(ValueError, "exactly match"):
                prepare_handoff_environment(
                    base, extension_dir=self._extension(Path(raw)), python_image="image"
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
        self.assertEqual(packet["rust_core_memory_limit_bytes"], 512 * 1024 * 1024)
        self.assertNotIn("PRIVATE KEY", encoded)
        self.assertEqual(packet["jwt_subjects"], ALL_KEY_SUBJECTS)

    def test_compose_overrides_remain_bounded(self) -> None:
        root = Path(__file__).resolve().parents[1]
        v2 = (root / "docker-compose.phase105c.override.yml").read_text(encoding="utf-8")
        v1 = (root / "docker-compose.phase105c-v1-fallback.yml").read_text(encoding="utf-8")
        self.assertIn("rust_core:", v2)
        self.assertNotIn("rust_core_2:", v2)
        self.assertNotIn("rust_core_3:", v2)
        self.assertIn("QDL_STABLE_TLS_CLIENT_CA_FILE", v2)
        self.assertIn("build: !reset null", v1)
        self.assertIn("volumes: !override", v1)
        self.assertNotIn(":/app\n", v1)


if __name__ == "__main__":
    unittest.main()
