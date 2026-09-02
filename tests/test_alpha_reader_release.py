from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import stat
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
COMPILER_PATH = ROOT / "scripts/compile_alpha_deployment_bindings.py"
PREPARER_PATH = ROOT / "scripts/prepare_alpha_reader_release.py"
IMAGE_ID = "sha256:" + "a" * 64
QUERY_ROLLBACK_IMAGE_ID = "sha256:" + "b" * 64
STREAM_ROLLBACK_IMAGE_ID = "sha256:" + "c" * 64
SOURCE_REVISION = "c" * 40
ROLLBACK_IMAGES = {
    "query_v2_1": {
        "image_reference": "qdl-v2-python:2.0.0-rollback-query-bbbbbbb",
        "image_id": QUERY_ROLLBACK_IMAGE_ID,
    },
    "query_v2_2": {
        "image_reference": "qdl-v2-python:2.0.0-rollback-query-bbbbbbb",
        "image_id": QUERY_ROLLBACK_IMAGE_ID,
    },
    "stream_v2_active": {
        "image_reference": "qdl-v2-python:2.0.0-rollback-stream-ccccccc",
        "image_id": STREAM_ROLLBACK_IMAGE_ID,
    },
    "stream_v2_passive": {
        "image_reference": "qdl-v2-python:2.0.0-rollback-stream-ccccccc",
        "image_id": STREAM_ROLLBACK_IMAGE_ID,
    },
}


def _module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AlphaReaderReleaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.compiler = _module("alpha_deployment_bindings", COMPILER_PATH)
        cls.preparer = _module("alpha_reader_release", PREPARER_PATH)
        cls.catalog_path = ROOT / "config/v2/stable-source-bindings.yaml"
        cls.reference_path = ROOT / "consumers/stable/reference-l2-stable.yaml"
        cls.routing_path = ROOT / "config/v2/stable-v2-release-routing.yaml"
        cls.policy_path = ROOT / "config/v2/universal-release-policy.yaml"
        cls.catalog = yaml.safe_load(cls.catalog_path.read_text(encoding="utf-8"))
        cls.reference = yaml.safe_load(cls.reference_path.read_text(encoding="utf-8"))
        cls.routing = yaml.safe_load(cls.routing_path.read_text(encoding="utf-8"))
        cls.policy = yaml.safe_load(cls.policy_path.read_text(encoding="utf-8"))

    def _compiled_input(self, root: Path) -> tuple[Path, Path, Path]:
        root.mkdir(parents=True, exist_ok=True)
        route = {
            "venue": "BINANCE",
            "market": "USDM",
            "product_type": "PERPETUAL",
            "native_symbol": "ETHUSDT",
            "feed": "BAR",
            "interval": "15m",
            "source_policy_id": "crypto_primary_v2",
            "provider_plane": "REALTIME",
            "max_freshness_ms": 180000,
            "require_final_bars": True,
            "require_live": False,
            "gap_policy": "BLOCK",
            "fallback": "BLOCKED",
            "fallback_rule_id": None,
            "blocked_reason": "V1_FINAL_BAR_EQUIVALENCE_UNPROVEN",
            "required": True,
        }
        deployment = {
            "deployment_id": "adaptive:binance:paper",
            "alpha_id": "adaptive_hma_cpp_00115m",
            "alpha_directory": "adaptive_hma_cpp",
            "profile": "directional_bar",
            "mode": "paper",
            "status": "DECLARED",
            "blocked_reason": None,
            "compose": {"path": "alphas/example/docker-compose.yml", "service": "adaptive", "sha256": "a" * 64},
            "sources": {"strategy_config_path": "alphas/example/config.yaml", "strategy_config_sha256": "b" * 64},
            "history": {"maxlen": 2500, "min_bars": 400},
            "routes": [route],
        }
        inventory: dict[str, object] = {
            "schema": "execution-alpha.data-requirements.v1",
            "revision": 1,
            "registry_path": "runtime/config/v2_alpha_deployment_requirements.yaml",
            "registry_sha256": "c" * 64,
            "deployments": [deployment],
        }
        inventory["inventory_sha256"] = self.compiler._digest(inventory)
        report = self.compiler.compile_inventory(
            inventory=inventory,
            catalog=self.catalog,
            reference_manifest=self.reference,
            release_routing=self.routing,
            release_routing_sha256=self.compiler._sha256_file(self.routing_path),
            policy=self.policy,
            policy_sha256=self.compiler._sha256_file(self.policy_path),
            catalog_sha256=self.compiler._sha256_file(self.catalog_path),
            reference_manifest_sha256=self.compiler._sha256_file(self.reference_path),
        )
        inventory_path = root / "inventory.json"
        inventory_path.write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bindings = root / "bindings"
        self.compiler.write_compilation(bindings, report)
        return inventory_path, bindings, bindings / "compilation-report.json"

    def _prepare(self, root: Path, *, output: str = "release", apply: bool = False):
        inventory, bindings, report = self._compiled_input(root)
        return self.preparer.prepare_alpha_reader_release(
            inventory_path=inventory,
            bindings_dir=bindings,
            report_path=report,
            output_dir=root / output,
            source_revision=SOURCE_REVISION,
            image_reference="qdl-v2-python:2.0.0-dev-ccccccc",
            image_id=IMAGE_ID,
            rollback_images=ROLLBACK_IMAGES,
            apply=apply,
        )

    def test_dry_run_is_secret_free_and_does_not_write(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            result = self._prepare(root, apply=False)
            self.assertEqual(result["status"], "DRY_RUN")
            self.assertFalse((root / "release").exists())
            self.assertFalse(result["secret_values_recorded"])
            self.assertNotIn("password", json.dumps(result, sort_keys=True).lower())

    def test_apply_is_atomic_deterministic_and_has_only_reader_override(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            first = self._prepare(root, output="release-one", apply=True)
            second = self._prepare(root, output="release-two", apply=True)
            self.assertEqual(first["status"], "APPLIED")
            first_dir = root / "release-one"
            second_dir = root / "release-two"
            self.assertEqual(
                sorted(path.relative_to(first_dir).as_posix() for path in first_dir.rglob("*") if path.is_file()),
                sorted(path.relative_to(second_dir).as_posix() for path in second_dir.rglob("*") if path.is_file()),
            )
            for relative in ("inventory.json", "compilation-report.json", "reader-image.override.yml", "reader-rollback.override.yml", "release-manifest.json"):
                self.assertEqual((first_dir / relative).read_bytes(), (second_dir / relative).read_bytes())
            manifest = json.loads((first_dir / "release-manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema"], "qdl.v2.alpha-reader-release.v2")
            self.assertEqual(manifest["image_id"], IMAGE_ID)
            self.assertEqual(manifest["image_selector"], "qdl-v2-python:2.0.0-dev-ccccccc@" + IMAGE_ID)
            self.assertEqual(manifest["rollback_images"], ROLLBACK_IMAGES)
            self.assertEqual(manifest["services"], list(self.preparer.READER_SERVICES))
            self.assertFalse(manifest["secret_values_recorded"])
            override = (first_dir / "reader-image.override.yml").read_text(encoding="utf-8")
            self.assertEqual(override.count("image: qdl-v2-python:2.0.0-dev-ccccccc@" + IMAGE_ID), 4)
            self.assertEqual(
                (first_dir / "reader-rollback.override.yml").read_text(encoding="utf-8").count(
                    "image: qdl-v2-python:2.0.0-rollback-query-bbbbbbb@" + QUERY_ROLLBACK_IMAGE_ID
                ), 2,
            )
            self.assertEqual(
                (first_dir / "reader-rollback.override.yml").read_text(encoding="utf-8").count(
                    "image: qdl-v2-python:2.0.0-rollback-stream-ccccccc@" + STREAM_ROLLBACK_IMAGE_ID
                ), 2,
            )
            self.assertNotIn("runtime", override.lower())
            self.assertNotIn("password", (first_dir / "release-manifest.json").read_text(encoding="utf-8").lower())
            binding_files = sorted((first_dir / "bindings").glob("*.binding.json"))
            self.assertTrue(binding_files)
            self.assertTrue(
                all(stat.S_IMODE(path.stat().st_mode) == 0o444 for path in binding_files)
            )
            self.assertEqual(
                stat.S_IMODE((first_dir / "release-manifest.json").stat().st_mode),
                0o640,
            )

    def test_rejects_tampered_binding_and_existing_output(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            inventory, bindings, report = self._compiled_input(root)
            binding = next(bindings.glob("*.binding.json"))
            payload = json.loads(binding.read_text(encoding="utf-8"))
            payload["consumer_id"] = "tampered.consumer"
            binding.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(self.preparer.ReleasePreparationError, "invalid|canonical|differ"):
                self.preparer.prepare_alpha_reader_release(
                    inventory_path=inventory,
                    bindings_dir=bindings,
                    report_path=report,
                    output_dir=root / "release",
                    source_revision=SOURCE_REVISION,
                    image_reference="qdl-v2-python:2.0.0-dev-ccccccc",
                    image_id=IMAGE_ID,
                    rollback_images=ROLLBACK_IMAGES,
                    apply=False,
                )
            self._prepare(root / "fresh", apply=True)
            with self.assertRaises(FileExistsError):
                self._prepare(root / "fresh", apply=True)

    def test_rejects_invalid_release_coordinates(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            inventory, bindings, report = self._compiled_input(root)
            with self.assertRaisesRegex(self.preparer.ReleasePreparationError, "canonical reader tag"):
                self.preparer.prepare_alpha_reader_release(
                    inventory_path=inventory,
                    bindings_dir=bindings,
                    report_path=report,
                    output_dir=root / "release",
                    source_revision=SOURCE_REVISION,
                    image_reference="not-an-image",
                    image_id=IMAGE_ID,
                    rollback_images=ROLLBACK_IMAGES,
                    apply=False,
                )
            same_as_candidate = deepcopy(ROLLBACK_IMAGES)
            same_as_candidate["query_v2_1"]["image_id"] = IMAGE_ID
            with self.assertRaisesRegex(self.preparer.ReleasePreparationError, "must differ"):
                self.preparer.prepare_alpha_reader_release(
                    inventory_path=inventory,
                    bindings_dir=bindings,
                    report_path=report,
                    output_dir=root / "release-two",
                    source_revision=SOURCE_REVISION,
                    image_reference="qdl-v2-python:2.0.0-dev-ccccccc",
                    image_id=IMAGE_ID,
                    rollback_images=same_as_candidate,
                    apply=False,
                )
            incomplete = deepcopy(ROLLBACK_IMAGES)
            incomplete.pop("stream_v2_passive")
            with self.assertRaisesRegex(self.preparer.ReleasePreparationError, "cover exactly"):
                self.preparer.prepare_alpha_reader_release(
                    inventory_path=inventory,
                    bindings_dir=bindings,
                    report_path=report,
                    output_dir=root / "release-three",
                    source_revision=SOURCE_REVISION,
                    image_reference="qdl-v2-python:2.0.0-dev-ccccccc",
                    image_id=IMAGE_ID,
                    rollback_images=incomplete,
                    apply=False,
                )
            malformed = deepcopy(ROLLBACK_IMAGES)
            malformed["stream_v2_active"]["image_reference"] = "not-an-image"
            with self.assertRaisesRegex(self.preparer.ReleasePreparationError, "rollback image reference"):
                self.preparer.prepare_alpha_reader_release(
                    inventory_path=inventory,
                    bindings_dir=bindings,
                    report_path=report,
                    output_dir=root / "release-four",
                    source_revision=SOURCE_REVISION,
                    image_reference="qdl-v2-python:2.0.0-dev-ccccccc",
                    image_id=IMAGE_ID,
                    rollback_images=malformed,
                    apply=False,
                )


if __name__ == "__main__":
    unittest.main()
