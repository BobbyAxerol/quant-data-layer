from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.refresh_stable_runtime_bundle import PRESERVED, refresh

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
PROMOTION_SCOPE_PATH = ROOT / "config/v2/stable-authority-promotion-scope.yaml"
RUST_IMAGE_ID = "sha256:" + "c" * 64

ENV_BYTES = b"QDL_STABLE_CURSOR_KEYS_JSON={'stable-k1':'do-not-rotate'}\n"


class StableRuntimeRefreshTests(unittest.TestCase):
    """A live bundle must be refreshable without minting new secrets.

    phaseb_prepare_stable_candidate refuses a non-empty directory and rotates
    the cursor key, ingest secret and both database passwords, so it can never
    be used against a bundle that is already serving consumers.
    """

    def _bundle(self, directory: Path) -> Path:
        bundle = directory / "bundle"
        (bundle / "runtime").mkdir(parents=True)
        (bundle / "identities/trading-system").mkdir(parents=True)
        (bundle / "identities/trading-system/client.crt").write_bytes(b"cert")
        (bundle / "stable.env").write_bytes(ENV_BYTES)
        (bundle / "runtime/authority.json").write_bytes(b"{}\n")
        (bundle / "runtime/stale-core.json").write_bytes(b"{}\n")
        return bundle

    def _refresh(self, bundle: Path, *, apply: bool) -> dict:
        return refresh(
            bundle_dir=bundle,
            rust_image_id=RUST_IMAGE_ID,
            source_catalog=CATALOG_PATH,
            acquisition_plan=ACQUISITION_PATH,
            promotion_scope_path=PROMOTION_SCOPE_PATH,
            partition_plan_epoch=1,
            apply=apply,
        )

    def test_refuses_a_directory_that_is_not_an_existing_bundle(self):
        with tempfile.TemporaryDirectory() as raw:
            empty = Path(raw) / "empty"
            (empty / "runtime").mkdir(parents=True)
            with self.assertRaises(FileNotFoundError):
                self._refresh(empty, apply=True)

    def test_requires_an_immutable_rust_image_id(self):
        with tempfile.TemporaryDirectory() as raw:
            bundle = self._bundle(Path(raw))
            with self.assertRaises(ValueError):
                refresh(
                    bundle_dir=bundle,
                    rust_image_id="qdl-v2-rust:latest",
                    source_catalog=CATALOG_PATH,
                    acquisition_plan=ACQUISITION_PATH,
                    promotion_scope_path=PROMOTION_SCOPE_PATH,
                    partition_plan_epoch=1,
                    apply=False,
                )

    def test_dry_run_reports_the_diff_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as raw:
            bundle = self._bundle(Path(raw))
            before = sorted(path.name for path in (bundle / "runtime").iterdir())
            result = self._refresh(bundle, apply=False)
            self.assertEqual(result["status"], "DRY_RUN")
            self.assertIsNone(result["backup_dir"])
            self.assertIn("core.json", result["files"]["added"])
            self.assertIn("stale-core.json", result["files"]["removed"])
            self.assertEqual(
                sorted(path.name for path in (bundle / "runtime").iterdir()), before
            )
            # A dry run against a live bundle must not create anything inside it,
            # not even a staging directory it later removes.
            self.assertEqual(
                sorted(item.name for item in bundle.iterdir()),
                ["identities", "runtime", "stable.env"],
            )

    def test_apply_regenerates_configs_and_preserves_every_secret(self):
        with tempfile.TemporaryDirectory() as raw:
            bundle = self._bundle(Path(raw))
            result = self._refresh(bundle, apply=True)
            self.assertEqual(result["status"], "APPLIED")

            self.assertEqual((bundle / "stable.env").read_bytes(), ENV_BYTES)
            self.assertEqual(
                (bundle / "identities/trading-system/client.crt").read_bytes(), b"cert"
            )
            for name in PRESERVED:
                self.assertTrue((bundle / name).exists())

            runtime = bundle / "runtime"
            names = {path.name for path in runtime.iterdir()}
            self.assertIn("core.json", names)
            self.assertIn("production-core-manifest.json", names)
            self.assertNotIn("stale-core.json", names)

            authority = json.loads((runtime / "authority.json").read_text())
            self.assertEqual(authority["candidate_image_digest"], RUST_IMAGE_ID)
            self.assertEqual(authority["mode"], "RUST_SHADOW")
            self.assertFalse(authority["public_write_allowed"])

            backup = Path(str(result["backup_dir"]))
            self.assertTrue((backup / "stale-core.json").is_file())

    def test_core_carries_the_whole_catalog_while_production_core_honours_the_scope(self):
        with tempfile.TemporaryDirectory() as raw:
            bundle = self._bundle(Path(raw))
            result = self._refresh(bundle, apply=True)
            production = json.loads(
                (bundle / "runtime/production-core-001.json").read_text()
            )
            self.assertEqual(
                len(production["slices"]), result["promotion_binding_count"]
            )
            manifest = json.loads(
                (bundle / "runtime/production-core-manifest.json").read_text()
            )
            self.assertEqual(
                manifest["promotion_scope_revision"],
                result["promotion_scope_revision"],
            )


if __name__ == "__main__":
    unittest.main()
