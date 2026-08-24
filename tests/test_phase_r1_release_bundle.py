from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import AuthorityPromotionScope
from scripts.phase_r1_prepare_release_bundle import prepare_release_bundle


ROOT = Path(__file__).resolve().parents[1]
RUST_IMAGE = "sha256:" + "c" * 64


class R1ReleaseBundleTests(unittest.TestCase):
    def _source_bundle(self, root: Path) -> Path:
        bundle = root / "source"
        (bundle / "runtime").mkdir(parents=True)
        for role in ("core", "projector", "trading-system-jwt"):
            (bundle / "identities" / role).mkdir(parents=True)
        (bundle / "identities-rotate-test/core").mkdir(parents=True)
        (bundle / "cert-material-rotate-test").mkdir(parents=True)
        (bundle / "identities/core/client.crt").write_text("core-cert", encoding="utf-8")
        (bundle / "identities-rotate-test/core/client.crt").write_text("rotated-core-cert", encoding="utf-8")
        (bundle / "cert-material-rotate-test/ca.crt").write_text("rotated-ca", encoding="utf-8")
        (bundle / "identities/trading-system-jwt/private.key").write_text("jwt-key", encoding="utf-8")
        base_env = "\n".join((
            "QDL_STABLE_RUST_IMAGE=sha256:" + "a" * 64,
            f"QDL_STABLE_RUNTIME_DIR={bundle / 'runtime'}",
            'QDL_STABLE_CURSOR_KEYS_JSON=\'{"stable-k1":"legacy-cursor-secret"}\'',
            "QDL_STABLE_INTERNAL_INGEST_SECRET=legacy-ingest-secret",
            "QDL_STABLE_CONTROL_DB_DSN=postgresql://control:legacy-password@db/qdl",
            "QDL_STABLE_CONTROL_ADMIN_DSN=postgresql://admin:legacy-password@db/qdl",
            f"QDL_STABLE_CORE_CERT_DIR={bundle / 'identities/core'}",
            f"QDL_STABLE_PROJECTOR_CERT_DIR={bundle / 'identities/projector'}",
            f"QDL_STABLE_TRADING_SYSTEM_JWT_PRIVATE_KEY={bundle / 'identities/trading-system-jwt/private.key'}",
            "QDL_TEST_PRESERVED=legacy-secret-value",
            "",
        ))
        active_env = base_env.replace(
            "QDL_STABLE_RUST_IMAGE=sha256:" + "a" * 64,
            "QDL_STABLE_RUST_IMAGE=sha256:" + "b" * 64,
        ).replace(
            f"QDL_STABLE_CORE_CERT_DIR={bundle / 'identities/core'}",
            f"QDL_STABLE_CORE_CERT_DIR={bundle / 'identities-rotate-test/core'}",
        ).replace(
            "QDL_TEST_PRESERVED=legacy-secret-value",
            f"QDL_STABLE_CERT_DIR={bundle / 'cert-material-rotate-test'}\n"
            "QDL_STABLE_COMPOSE_OVERRIDE=/historical/c39.override.yml\n"
            "QDL_TEST_PRESERVED=legacy-secret-value",
        )
        (bundle / "stable.env").write_text(base_env, encoding="utf-8")
        (bundle / "stable.env.active").write_text(active_env, encoding="utf-8")
        return bundle

    def test_dry_run_is_secret_free_and_leaves_source_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = self._source_bundle(root)
            before = (source / "stable.env").read_bytes()
            result = prepare_release_bundle(
                source_bundle=source,
                output_bundle=root / "release",
                rust_image_id=RUST_IMAGE,
                apply=False,
                source_env=source / "stable.env.active",
                key_factory=lambda _: "f" * 64,
                clock=lambda: 123,
            )
            self.assertEqual(result["status"], "DRY_RUN")
            self.assertEqual(result["source_env"], "stable.env.active")
            self.assertFalse((root / "release").exists())
            self.assertEqual((source / "stable.env").read_bytes(), before)
            serialized = json.dumps(result, sort_keys=True)
            self.assertNotIn("legacy-secret", serialized)
            self.assertNotIn("legacy-password", serialized)
            self.assertNotIn("f" * 64, serialized)

    def test_apply_preserves_live_credentials_and_excludes_promoted_generic_scope(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = self._source_bundle(root)
            output = root / "release"
            result = prepare_release_bundle(
                source_bundle=source,
                output_bundle=output,
                rust_image_id=RUST_IMAGE,
                apply=True,
                source_env=source / "stable.env.active",
                key_factory=lambda _: "f" * 64,
                clock=lambda: 123,
            )
            self.assertEqual(result["status"], "APPLIED")
            values = {}
            for line in (output / "stable.env").read_text(encoding="utf-8").splitlines():
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key] = value.strip("'")
            self.assertEqual(values["QDL_STABLE_INTERNAL_INGEST_SECRET"], "legacy-ingest-secret")
            self.assertEqual(values["QDL_STABLE_CONTROL_DB_DSN"], "postgresql://control:legacy-password@db/qdl")
            self.assertEqual(values["QDL_STABLE_RUNTIME_DIR"], str(output / "runtime"))
            self.assertEqual(values["QDL_STABLE_CORE_CERT_DIR"], str(output / "identities-rotate-test/core"))
            self.assertEqual(values["QDL_STABLE_CERT_DIR"], str(output / "cert-material-rotate-test"))
            self.assertNotIn("QDL_STABLE_COMPOSE_OVERRIDE", values)
            self.assertEqual(values["QDL_PHASE92_BOOTSTRAP_GROUP_ID"], "qdl-v2-production-core-r1-cccccccccccc")
            self.assertTrue((output / "identities/core/client.crt").is_file())
            self.assertTrue((output / "identities-rotate-test/core/client.crt").is_file())
            self.assertTrue((output / "cert-material-rotate-test/ca.crt").is_file())
            self.assertEqual(oct((output / "stable.env").stat().st_mode & 0o777), "0o600")
            manifest = json.loads((output / "release-manifest.json").read_text(encoding="utf-8"))
            self.assertFalse(manifest["secret_values_recorded"])
            self.assertNotIn("legacy-password", json.dumps(manifest, sort_keys=True))
            generic = json.loads((output / "runtime/core.json").read_text(encoding="utf-8"))
            production = json.loads((output / "runtime/production-core-001.json").read_text(encoding="utf-8"))
            generic_ids = {item["source_id"] for item in generic["core"]["bindings"]}
            production_ids = {item["subscription_id"] for item in production["slices"]}
            scope = AuthorityPromotionScope.load(
                ROOT / "config/v2/stable-authority-promotion-scope.yaml",
                catalog=StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml"),
            )
            self.assertEqual(len(production_ids), len(scope.binding_ids))
            self.assertTrue(generic_ids.isdisjoint(production_ids))

    def test_source_env_is_required_and_must_remain_in_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = self._source_bundle(root)
            with self.assertRaisesRegex(ValueError, "explicitly provided"):
                prepare_release_bundle(
                    source_bundle=source,
                    output_bundle=root / "release",
                    rust_image_id=RUST_IMAGE,
                    apply=False,
                    source_env=None,
                )
            with self.assertRaisesRegex(ValueError, "inside the source bundle"):
                prepare_release_bundle(
                    source_bundle=source,
                    output_bundle=root / "release",
                    rust_image_id=RUST_IMAGE,
                    apply=False,
                    source_env=root / "outside.env",
                )
            with self.assertRaisesRegex(ValueError, "not sealed release material"):
                invalid = source / "stable.env.invalid"
                invalid.write_text(
                    (source / "stable.env.active").read_text(encoding="utf-8")
                    + f"QDL_STABLE_QUERY_CERT_DIR={source / 'runtime/not-material'}\n",
                    encoding="utf-8",
                )
                prepare_release_bundle(
                    source_bundle=source,
                    output_bundle=root / "release",
                    rust_image_id=RUST_IMAGE,
                    apply=False,
                    source_env=invalid,
                )


if __name__ == "__main__":
    unittest.main()
