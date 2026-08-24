from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import AuthorityPromotionScope, StableAcquisitionPlan
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

    def _refresh(self, bundle: Path, *, apply: bool, state_dir: Path | None = None) -> dict:
        return refresh(
            bundle_dir=bundle,
            rust_image_id=RUST_IMAGE_ID,
            source_catalog=CATALOG_PATH,
            acquisition_plan=ACQUISITION_PATH,
            promotion_scope_path=PROMOTION_SCOPE_PATH,
            partition_plan_epoch=1,
            apply=apply,
            state_dir=state_dir,
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

    def test_generic_core_excludes_the_production_scope_and_bundle_binds_bootstrap(self):
        with tempfile.TemporaryDirectory() as raw:
            bundle = self._bundle(Path(raw))
            result = self._refresh(bundle, apply=True)
            generic = json.loads((bundle / "runtime/core.json").read_text())
            production = json.loads(
                (bundle / "runtime/production-core-001.json").read_text()
            )
            generic_subscriptions = {
                item["source_id"] for item in generic["core"]["bindings"]
            }
            production_subscriptions = {
                item["subscription_id"] for item in production["slices"]
            }
            scope = AuthorityPromotionScope.load(
                PROMOTION_SCOPE_PATH,
                catalog=StableSourceCatalog.load(CATALOG_PATH),
            )
            self.assertEqual(
                len(production["slices"]), result["promotion_binding_count"]
            )
            self.assertTrue(generic_subscriptions.isdisjoint(production_subscriptions))
            self.assertEqual(len(production_subscriptions), len(scope.binding_ids))
            self.assertEqual(
                production["promotion_scope_digest"], scope.digest()
            )
            self.assertEqual(production["partition_plan_epoch"], 1)
            self.assertEqual(
                production["bootstrap_cursor_path"], "/runtime/production-bootstrap.json"
            )
            manifest = json.loads(
                (bundle / "runtime/production-core-manifest.json").read_text()
            )
            self.assertEqual(
                manifest["promotion_scope_revision"],
                result["promotion_scope_revision"],
            )


class StrandedCheckpointTests(unittest.TestCase):
    """A revision bump strands every edge checkpoint that pins the old value.

    The crypto BAR edge refuses to restore state when catalog_revision or
    acquisition_revision moves, so a refresh that bumps either one takes the
    role down at its next start. That has to be visible in the dry run, not
    discovered when the container exits.
    """

    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.acquisition = StableAcquisitionPlan.load(
            ACQUISITION_PATH, catalog=self.catalog
        )

    def _bundle(self, directory: Path) -> Path:
        bundle = directory / "bundle"
        (bundle / "runtime").mkdir(parents=True)
        (bundle / "identities").mkdir(parents=True)
        (bundle / "stable.env").write_bytes(ENV_BYTES)
        return bundle

    def _checkpoint(self, state_dir: Path, name: str, **overrides) -> Path:
        payload = {
            "schema": "qdl.stable-bar-edge-state.v1",
            "slice_id": "qdl-v2-stable-multivenue-shadow",
            "authority_revision": 1,
            "catalog_revision": self.catalog.catalog_revision,
            "acquisition_revision": self.acquisition.revision,
            "binding_ids": ["a", "b"],
            "last_open_ms": {},
        }
        payload.update(overrides)
        path = state_dir / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _run(self, directory: Path, state_dir: Path | None):
        return refresh(
            bundle_dir=self._bundle(directory),
            rust_image_id=RUST_IMAGE_ID,
            source_catalog=CATALOG_PATH,
            acquisition_plan=ACQUISITION_PATH,
            promotion_scope_path=PROMOTION_SCOPE_PATH,
            partition_plan_epoch=1,
            apply=False,
            state_dir=state_dir,
        )

    def test_a_matching_checkpoint_is_reported_compatible(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            state = directory / "state"
            state.mkdir()
            self._checkpoint(state, "stable-crypto-bar-edge.json")
            result = self._run(directory, state)
            self.assertEqual(result["stranded_checkpoints"], 0)
            self.assertTrue(result["checkpoints"][0]["compatible"])

    def test_a_stale_catalog_revision_is_reported_before_apply(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            state = directory / "state"
            state.mkdir()
            self._checkpoint(
                state,
                "stable-crypto-bar-edge.json",
                catalog_revision=self.catalog.catalog_revision - 1,
            )
            result = self._run(directory, state)
            self.assertEqual(result["stranded_checkpoints"], 1)
            entry = result["checkpoints"][0]
            self.assertFalse(entry["compatible"])
            self.assertIn("catalog_revision", entry["drift"])
            self.assertEqual(
                entry["drift"]["catalog_revision"]["refreshed"],
                self.catalog.catalog_revision,
            )

    def test_a_stale_acquisition_revision_is_reported(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            state = directory / "state"
            state.mkdir()
            self._checkpoint(
                state,
                "stable-crypto-bar-edge.json",
                acquisition_revision=self.acquisition.revision - 1,
            )
            result = self._run(directory, state)
            self.assertEqual(result["stranded_checkpoints"], 1)
            self.assertIn("acquisition_revision", result["checkpoints"][0]["drift"])

    def test_an_unreadable_checkpoint_is_reported_not_raised(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            state = directory / "state"
            state.mkdir()
            (state / "stable-crypto-bar-edge.json").write_text("{not json", encoding="utf-8")
            result = self._run(directory, state)
            self.assertEqual(result["stranded_checkpoints"], 1)
            self.assertIn("unreadable", str(result["checkpoints"][0]["reason"]))

    def test_every_edge_checkpoint_is_inspected(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            state = directory / "state"
            state.mkdir()
            self._checkpoint(state, "stable-crypto-bar-edge.json")
            self._checkpoint(
                state, "stable-dnse-edge.json", catalog_revision=1
            )
            result = self._run(directory, state)
            self.assertEqual(len(result["checkpoints"]), 2)
            self.assertEqual(result["stranded_checkpoints"], 1)

    def test_an_unreadable_state_directory_is_never_silently_clean(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            missing = directory / "not-mounted"
            result = self._run(directory, missing)
            self.assertEqual(result["stranded_checkpoints"], 1)
            self.assertIn("does not exist", str(result["checkpoints"][0]["reason"]))

    def test_an_empty_state_directory_says_so_explicitly(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            state = directory / "state"
            state.mkdir()
            result = self._run(directory, state)
            self.assertEqual(len(result["checkpoints"]), 1)
            self.assertIn("no edge checkpoint", str(result["checkpoints"][0]["reason"]))
            self.assertEqual(result["stranded_checkpoints"], 0)

    def test_omitting_the_state_dir_reports_no_checkpoints(self):
        with tempfile.TemporaryDirectory() as raw:
            result = self._run(Path(raw), None)
            self.assertEqual(result["checkpoints"], [])
            self.assertEqual(result["stranded_checkpoints"], 0)


if __name__ == "__main__":
    unittest.main()
