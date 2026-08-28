from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from qdl.certification import reference_l2_rollout
from qdl.certification.reference_l2_rollout import (
    LEGACY_CONSUMER_MANIFESTS,
    PYTHON_ROLLBACK_SERVICES,
    ROLLING_SERVICES,
    dry_run_reference_l2_rollout,
    prepare_reference_l2_rollout,
)
from qdl.runtime.stable_deployment import stable_authority_record


ROOT = Path(__file__).resolve().parents[1]
PEM_ONE = b"-----BEGIN CERTIFICATE-----\nONE\n-----END CERTIFICATE-----\n"
PEM_TWO = b"-----BEGIN CERTIFICATE-----\nTWO\n-----END CERTIFICATE-----\n"


class ReferenceL2RolloutTests(unittest.TestCase):
    def _key(self, name: str) -> str:
        return f"-----BEGIN PUBLIC KEY-----\n{name}\n-----END PUBLIC KEY-----\n"

    def _write_env(self, path: Path, values: dict[str, str]) -> None:
        path.write_text("".join(f"{key}={value}\n" for key, value in values.items()))

    def _identity(self, root: Path, role: str, *, key: str | None = None) -> None:
        directory = root / role
        directory.mkdir(parents=True)
        if role.endswith("-jwt"):
            (directory / "private.key").write_text("private material\n")
            (directory / "public.pem").write_text(key or self._key(role))
        else:
            (directory / "client.crt").write_bytes(PEM_ONE)
            (directory / "client.key").write_text("private material\n")
            (directory / "ca.crt").write_bytes(PEM_ONE)

    def _authority(self, runtime: Path) -> bytes:
        authority = stable_authority_record(
            rust_image_digest="sha256:" + "a" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=(ROOT / "config/v2/stable-acquisition-bindings.yaml").read_bytes(),
            effective_at_ns=time.time_ns(),
            mode="RUST_PRIMARY",
            revision=5,
            slice_id="qdl-v2-shared-realtime-primary",
            approved_by="BobbyAxerol",
        )
        runtime.mkdir()
        encoded = (json.dumps(authority, indent=4, sort_keys=False) + "\n").encode()
        (runtime / "authority.json").write_bytes(encoded)
        return encoded

    def _rollback(self, checkpoint: str, *, state_root: Path) -> dict[str, object]:
        return {
            service: {
                "image_digest": "sha256:" + ("1" if service.startswith("ingestor") else "2") * 64,
                "runtime_dir": str(state_root / "previous/runtime"),
                "checkpoint_path": checkpoint if service == "binance_bar_edge" else None,
            }
            for service in ROLLING_SERVICES
        }

    def _inputs(self, root: Path) -> dict[str, object]:
        active_runtime = root / "active-runtime"
        self._authority(active_runtime)
        prior = root / "prior-external"
        for role in ("monitoring", "monitoring-jwt", "alpha-okx", "alpha-okx-jwt"):
            self._identity(prior, role, key=self._key(role))
        extension = root / "reference-extension"
        self._identity(extension, "reference-l2")
        self._identity(extension, "reference-l2-jwt", key=self._key("reference-l2-jwt"))
        (extension / "external-client-ca.crt").write_bytes(PEM_TWO)
        current_query = root / "query-ca.crt"
        current_stream = root / "stream-ca.crt"
        current_query.write_bytes(PEM_ONE)
        current_stream.write_bytes(PEM_ONE)

        old_keys = {
            "stable-trading-system-rs256-v1": self._key("trading"),
            "stable-alpha-binance-rs256-v1": self._key("binance"),
            "stable-monitoring-rs256-v1": self._key("monitoring-jwt"),
            "stable-alpha-okx-rs256-v1": self._key("alpha-okx-jwt"),
        }
        subjects = {
            "stable-trading-system-rs256-v1": "spiffe://qdl/paper/trading-system-stable",
            "stable-alpha-binance-rs256-v1": "spiffe://qdl/paper/alpha-binance-stable",
            "stable-monitoring-rs256-v1": "spiffe://qdl/paper/monitoring-multivenue-stable",
            "stable-alpha-okx-rs256-v1": "spiffe://qdl/paper/alpha-okx-stable",
        }
        base_env = root / "base.env"
        self._write_env(base_env, {
            "QDL_STABLE_CONTROL_DB_PASSWORD": "database-secret",
            "QDL_STABLE_JWT_KEYS_JSON": json.dumps(old_keys, separators=(",", ":")),
            "QDL_STABLE_JWT_KEY_SUBJECTS_JSON": json.dumps(subjects, separators=(",", ":")),
        })
        query_env = root / "query.env"
        self._write_env(query_env, {
            "QDL_STABLE_INTERNAL_INGEST_SECRET": "ingest-secret",
            "QDL_STABLE_CURSOR_KEYS_JSON": '{"stable-k1":"cursor-secret"}',
            "QDL_DATA_JWT_KEYS_JSON": json.dumps(old_keys, separators=(",", ":")),
            "QDL_DATA_JWT_KEY_SUBJECTS_JSON": json.dumps(subjects, separators=(",", ":")),
            "QDL_STABLE_SCHEMA_DIGEST": "a" * 64,
            "QDL_STABLE_AUTHORITY_MODE": "RUST_PRIMARY",
            "QDL_STABLE_AUTHORITY_REVISION": "5",
        })
        checkpoint = "/var/lib/qdl-stable/runtime/current.json"
        bar_env = root / "bar.env"
        self._write_env(bar_env, {"QDL_STABLE_BAR_STATE_PATH": checkpoint})
        return {
            "base_compose_env": base_env,
            "active_query_env": query_env,
            "active_bar_env": bar_env,
            "active_runtime_dir": active_runtime,
            "prior_external_dir": prior,
            "reference_extension_dir": extension,
            "current_query_client_ca": current_query,
            "current_stream_client_ca": current_stream,
            "output_dir": root / "successor",
            "host_runtime_dir": Path("/home/bobby/.local/state/qdl-v2/successor/runtime"),
            "python_image_digest": "sha256:" + "b" * 64,
            "rust_image_digest": "sha256:" + "c" * 64,
            "source_commit": "a370ea1",
            "rollback_provenance": self._rollback(checkpoint, state_root=root),
        }

    def test_prepares_exact_scope_and_preserves_existing_identity_trust(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            with patch.object(reference_l2_rollout, "STATE_ROOT", root):
                inputs = self._inputs(root)
                inputs["host_runtime_dir"] = root / "successor/runtime"
                packet = prepare_reference_l2_rollout(**inputs)
                output = Path(inputs["output_dir"])
                self.assertEqual(packet["recreated_services"], list(ROLLING_SERVICES))
                self.assertTrue(packet["runtime"]["authority_bytes_preserved"])
                self.assertEqual(packet["environment"]["jwt_key_ids"], [
                    "stable-alpha-binance-rs256-v1",
                    "stable-alpha-okx-rs256-v1",
                    "stable-monitoring-rs256-v1",
                    "stable-reference-l2-rs256-v1",
                    "stable-trading-system-rs256-v1",
                ])
                self.assertEqual((output / "runtime/authority.json").read_bytes(), (Path(inputs["active_runtime_dir"]) / "authority.json").read_bytes())
                self.assertEqual((output / "trust/query-client-ca-bundle.crt").read_bytes(), PEM_ONE + PEM_TWO)
                self.assertEqual((output / "trust/stream-client-ca-bundle.crt").read_bytes(), PEM_ONE + PEM_TWO)
                self.assertTrue((output / "external-identities/monitoring-jwt/public.pem").is_file())
                self.assertTrue((output / "external-identities/reference-l2-jwt/private.key").is_file())
                rollback_override = (output / "rollback-legacy-manifests.override.yml").read_text()
                self.assertNotIn("reference-l2-stable.yaml", rollback_override)
                self.assertIn(LEGACY_CONSUMER_MANIFESTS, rollback_override)
                for service in PYTHON_ROLLBACK_SERVICES:
                    self.assertIn(f"  {service}:\n", rollback_override)
                self.assertEqual(
                    packet["rollback"]["compose_override"]["python_services"],
                    list(PYTHON_ROLLBACK_SERVICES),
                )
                rendered = (output / "rollout-packet.json").read_text()
                self.assertNotIn("PRIVATE KEY", rendered)
                self.assertNotIn("database-secret", rendered)
                self.assertNotIn("cursor-secret", rendered)

    def test_fails_closed_on_existing_public_key_conflict_and_cleans_partial_output(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            with patch.object(reference_l2_rollout, "STATE_ROOT", root):
                inputs = self._inputs(root)
                inputs["host_runtime_dir"] = root / "successor/runtime"
                prior_key = Path(inputs["prior_external_dir"]) / "monitoring-jwt/public.pem"
                prior_key.write_text(self._key("wrong-monitoring"))
                with self.assertRaisesRegex(ValueError, "conflicts"):
                    prepare_reference_l2_rollout(**inputs)
                self.assertFalse(Path(inputs["output_dir"]).exists())

    def test_dry_run_leaves_no_state_directory(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            with patch.object(reference_l2_rollout, "STATE_ROOT", root):
                inputs = self._inputs(root)
                inputs["host_runtime_dir"] = root / "successor/runtime"
                packet = dry_run_reference_l2_rollout(**inputs)
                self.assertEqual(packet["status"], "PREPARED")
                self.assertFalse(Path(inputs["output_dir"]).exists())


if __name__ == "__main__":
    unittest.main()
