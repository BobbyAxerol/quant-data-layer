from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from qdl.certification import build_spdx, verify_release_bundle, write_release_bundle


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "ghcr.io/bobbyaxerol/quant-data-layer@sha256:" + "a" * 64


class ReleaseBundleTests(unittest.TestCase):
    def test_runtime_image_is_non_root_and_trivy_waiver_is_narrow(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("USER qdl:qdl", dockerfile)
        self.assertIn("python -m venv /opt/venv", dockerfile)
        self.assertIn(
            "COPY --from=builder --chown=qdl:qdl /opt/venv /opt/venv",
            dockerfile,
        )
        self.assertNotIn("COPY --from=builder --chown=qdl:qdl /app/.venv", dockerfile)
        preparation = (ROOT / "scripts/prepare_nonroot_runtime.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn('QDL_RUNTIME_UID:-10001', preparation)
        self.assertIn('QDL_RUNTIME_GID:-10001', preparation)
        self.assertIn('for relative in data logs', preparation)
        docker_ignored = {
            line.strip()
            for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        }
        self.assertTrue({"data", "logs"}.issubset(docker_ignored))
        workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        self.assertIn("if [[ -d ../base-contracts/contracts ]]", workflow)
        self.assertIn("frozen Phase 1/7 baselines remain authoritative", workflow)
        self.assertIn(
            "clang cmake libclang-dev libcurl4-openssl-dev libssl-dev "
            "libzstd-dev make pkg-config zlib1g-dev",
            workflow,
        )
        self.assertIn("pip-audit --cache-dir /tmp/qdl-pip-audit-cache", workflow)
        self.assertIn('python-version: "3.12"', workflow)
        self.assertIn("python -m scripts.phase6_release_bundle", workflow)
        self.assertIn(
            'test "$(head -n 1 /opt/venv/bin/uvicorn)" = '
            '"#!/opt/venv/bin/python"',
            workflow,
        )
        ci_compose = (ROOT / "docker-compose.ci.yml").read_text(encoding="utf-8")
        self.assertGreaterEqual(ci_compose.count("container_name: !reset null"), 5)
        self.assertIn("ports: !reset []", ci_compose)
        self.assertGreaterEqual(ci_compose.count("volumes: !reset []"), 4)
        ignored = {
            line.strip()
            for line in (ROOT / ".trivyignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        }
        self.assertEqual(
            ignored,
            {"GHSA-6v7p-g79w-8964", "CVE-2025-47273"},
        )

    def test_sbom_contains_locked_python_and_rust_components(self):
        packages = build_spdx(ROOT, release="phase6-test")["packages"]
        purls = {item["externalRefs"][0]["referenceLocator"] for item in packages}
        self.assertTrue(any(item.startswith("pkg:pypi/fastapi@") for item in purls))
        self.assertTrue(any(item.startswith("pkg:cargo/prost@") for item in purls))

    def test_bundle_requires_immutable_image_and_detects_artifact_tamper(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            with self.assertRaisesRegex(ValueError, "immutable"):
                write_release_bundle(
                    ROOT, output, release="phase6", git_sha="abcdef1",
                    image_ref="ghcr.io/bobbyaxerol/quant-data-layer:latest",
                )
            manifest = write_release_bundle(
                ROOT, output, release="phase6", git_sha="abcdef1", image_ref=IMAGE,
            )
            self.assertEqual(manifest["authority"], "SHADOW")
            self.assertEqual(verify_release_bundle(ROOT, output)["git_sha"], "abcdef1")
            sbom_path = output / "sbom.spdx.json"
            original = sbom_path.read_bytes()
            sbom_path.write_bytes(original + b"tampered")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                verify_release_bundle(ROOT, output)


if __name__ == "__main__":
    unittest.main()
