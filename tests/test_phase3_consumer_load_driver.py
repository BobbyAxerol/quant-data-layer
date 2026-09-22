from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from argparse import Namespace
import json


_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "phase3_consumer_load_acceptance.py"
_SPEC = importlib.util.spec_from_file_location("phase3_consumer_load_driver", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


class Phase3ConsumerLoadDriverTests(unittest.TestCase):
    def _profile(self, root: Path) -> dict[str, object]:
        runtime = root / "runtime"
        runtime.mkdir()
        identity = root / "identity"
        identity.mkdir()
        values = {}
        for name in ("ca.crt", "client.crt", "client.key", "private.key"):
            path = identity / name
            path.write_text(name, encoding="utf-8")
            values[name] = str(path)
        return {
            "image": "qdl-v2-python:2.1.1-test",
            "network": "qdl_v2_stable_candidate_default",
            "runtime_dir": str(runtime),
            "queries": ["https://query_v2_1:8200", "https://query_v2_2:8200"],
            "stream_targets": ["stream_v2_active:8210", "stream_v2_passive:8210"],
            "query_containers": ["qdl_v2_stable_candidate-query_v2_1-1", "qdl_v2_stable_candidate-query_v2_2-1"],
            "identities": [{
                "id": "alpha.binance.paper.stable",
                "tls": {
                    "ca_file": values["ca.crt"],
                    "cert_file": values["client.crt"],
                    "key_file": values["client.key"],
                },
                "jwt": {"private_key_file": values["private.key"], "key_id": "test-key"},
            }],
        }

    def test_disposable_command_is_bounded_read_only_and_secret_paths_are_not_exposed_inside(self):
        with tempfile.TemporaryDirectory() as temporary:
            profile = _MODULE.validate_profile(self._profile(Path(temporary)))
            command, inner = _MODULE.docker_command(
                profile,
                name="qdl-phase3-load-unit",
                image_id="sha256:" + "a" * 64,
                mode="load",
                sessions=5,
                duration_seconds=60,
            )
        self.assertIn("--read-only", command)
        self.assertIn("--security-opt", command)
        self.assertIn("no-new-privileges", command)
        self.assertIn("--memory", command)
        self.assertIn("512m", command)
        self.assertIn("--cpus", command)
        self.assertIn("1.0", command)
        self.assertIn("--pids-limit", command)
        self.assertIn("PYTHONPATH=/app:/driver", command)
        self.assertNotIn("--privileged", command)
        self.assertFalse(any("docker.sock" in value for value in command))
        self.assertTrue(any(
            "/app/qdl/certification/phase3_consumer_load.py" in value
            for value in command
        ))
        self.assertFalse(any("/driver/qdl" in value for value in command))
        identity = inner["identities"][0]
        self.assertTrue(identity["tls"]["ca_file"].startswith("/tmp/identity/"))
        self.assertTrue(identity["jwt"]["private_key_file"].startswith("/tmp/identity/"))

    def test_profile_rejects_unknown_field_before_docker_is_called(self):
        with tempfile.TemporaryDirectory() as temporary:
            profile = self._profile(Path(temporary))
            profile["unexpected"] = "not-permitted"
            with self.assertRaisesRegex(ValueError, "incomplete or unknown"):
                _MODULE.validate_profile(profile)

    def test_profile_rejects_identity_path_outside_managed_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            profile = self._profile(Path(temporary))
            profile["identities"][0]["tls"]["ca_file"] = "/etc/passwd"
            with self.assertRaisesRegex(ValueError, "identity file is unavailable"):
                _MODULE.validate_profile(profile)

    def test_percentiles_do_not_invent_p99_from_small_sample(self):
        result = _MODULE._percentiles([1.0, 2.0, 3.0])
        self.assertEqual(result["n"], 3)
        self.assertEqual(result["p50_ms"], 2.0)
        self.assertIsNone(result["p99_ms"])

    def test_host_refuses_partial_identity_scope_before_docker_is_called(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            profile = root / "profile.json"
            profile.write_text(json.dumps(self._profile(root)), encoding="utf-8")
            args = Namespace(
                profile=profile,
                output=root / "output",
                mode="matrix",
                sessions=5,
                duration_seconds=0,
            )
            with self.assertRaisesRegex(ValueError, "exactly the four approved"):
                _MODULE.run_host(args)

    def test_bootstrap_transfers_tmpfs_identity_ownership_before_privilege_drop(self):
        script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "phase3_consumer_load_bootstrap.sh"
        ).read_text(encoding="utf-8")
        self.assertIn("chown -R 10001:10001 /tmp/identity", script)
        self.assertIn("chmod -R u=rwX,go= /tmp/identity", script)
        self.assertLess(
            script.index("chown -R 10001:10001 /tmp/identity"),
            script.index("exec setpriv"),
        )


if __name__ == "__main__":
    unittest.main()
