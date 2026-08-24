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
        (bundle / "identities/core/client.crt").write_text("core-cert", encoding="utf-8")
        (bundle / "identities/trading-system-jwt/private.key").write_text("jwt-key", encoding="utf-8")
        env = "\n".join((
            "QDL_STABLE_RUST_IMAGE=sha256:" + "a" * 64,
            f"QDL_STABLE_RUNTIME_DIR={bundle / 'runtime'}",
            "QDL_STABLE_CURSOR_KEYS_JSON='{\"stable-k1\":\"legacy-cursor-secret\"}'",
            "QDL_STABLE_INTERNAL_INGEST_SECRET=legacy-ingest-secret",
            "QDL_STABLE_CONTROL_DB_DSN=postgresql://control:legacy-password@db/qdl",
            "QDL_STABLE_CONTROL_ADMIN_DSN=postgresql://admin:legacy-password@db/qdl",
            f"QDL_STABLE_CORE_CERT_DIR={bundle / 'identities/core'}",
            f"QDL_STABLE_PROJECTOR_CERT_DIR={bundle / 'identities/projector'}",
            f"QDL_STABLE_TRADING_SYSTEM_JWT_PRIVATE_KEY={bundle / 'identities/trading-system-jwt/private.key'}",
            "QDL_TEST_PRESERVED=legacy-secret-value",
            "",
        ))
        (bundle / "stable.env").write_text(env, encoding="utf-8")
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
                key_factory=lambda _: "f" * 64,
                clock=lambda: 123,
            )
            self.assertEqual(result["status"], "DRY_RUN")
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
            self.assertEqual(values["QDL_STABLE_CORE_CERT_DIR"], str(output / "identities/core"))
            self.assertEqual(values["QDL_PHASE92_BOOTSTRAP_GROUP_ID"], "qdl-v2-production-core-r1-cccccccccccc")
            self.assertTrue((output / "identities/core/client.crt").is_file())
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


if __name__ == "__main__":
    unittest.main()
