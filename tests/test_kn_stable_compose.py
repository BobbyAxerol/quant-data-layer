"""Contract of the versioned KN compose (deploy/compose/qdl-v2-stable-kn.yml, DL v2.2.5).

Static checks only (no Docker): the 17 production roles, images by variable with every release env pinning a
digest, the v2.2.4 env equal to the certified runtime images and caps, healthcheck/logging/restart on every role,
loopback-only ports, the consumer aliases, external networks/volumes, TLS sources only from the generation
variables, and no secret literal in any committed file. Live parity is ``scripts/kn_versioned_compose.py parity``;
its field comparison (cpus tolerance) is proven here against a faked ``docker inspect``.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import unittest
from unittest import mock

import yaml

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "deploy/compose/qdl-v2-stable-kn.yml"
ENV_V224 = ROOT / "deploy/compose/qdl-v2-stable-kn.v2.2.4.env"
ENV_V225 = ROOT / "deploy/compose/qdl-v2-stable-kn.v2.2.5.env"
CERTIFICATE_V224 = ROOT / "upgrade/evidence/releases/v2.2.4/certificate.json"
ROLES = {"kafka1", "kafka2", "kafka3", "rust_core", "rust_core_2", "rust_core_3", "ingestor_binance_usdm",
         "ingestor_okx_swap", "market_projector_1", "market_projector_2", "market_cache", "stable_redis",
         "query_kn_1", "query_kn_2", "stream_kn_1", "stream_kn_2", "binance_bar_edge"}
SECRET_KEYS = ("QDL_PROVIDER_ADMISSION_SECRET", "QDL_STABLE_INTERNAL_INGEST_SECRET", "QDL_STABLE_CURSOR_KEYS_JSON")
DIGEST = re.compile(r"^[a-z0-9./_-]+@sha256:[0-9a-f]{64}$")


def env_file(path: Path) -> dict:
    out = {}
    for line in path.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            key, value = line.split("=", 1)
            out[key] = value
    return out


class KnStableComposeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = COMPOSE.read_text()
        cls.doc = yaml.safe_load(cls.text)
        cls.services = cls.doc["services"]

    def test_exactly_the_seventeen_production_roles_in_the_production_project(self):
        self.assertEqual(self.doc["name"], "qdl_v2_stable_candidate")
        self.assertEqual(set(self.services), ROLES)

    def test_images_are_variables_and_every_release_env_pins_digests(self):
        variables = set()
        for role, svc in self.services.items():
            match = re.fullmatch(r"\$\{(QDL_IMAGE_[A-Z_]+):\?[^}]*\}", svc["image"])
            self.assertIsNotNone(match, role)
            variables.add(match.group(1))
            self.assertEqual(svc.get("pull_policy"), "never", role)
        for path in (ENV_V224, ENV_V225):
            if not path.exists():
                continue
            env = env_file(path)
            for var in variables:
                with self.subTest(env=path.name, var=var):
                    self.assertRegex(env[var], DIGEST)

    def test_v224_env_and_caps_equal_the_certified_runtime(self):
        cert = json.loads(CERTIFICATE_V224.read_text())
        rows = {row["service"]: row for row in cert["runtime"]}
        env = env_file(ENV_V224)
        for role, svc in self.services.items():
            row = rows.get(f"qdl_v2_stable_candidate-{role}-1")
            if row is None:
                continue
            var = svc["image"][2:].split(":", 1)[0]
            with self.subTest(role=role):
                self.assertEqual(env[var].split("@", 1)[1], row["image"])
                self.assertAlmostEqual(float(svc["cpus"]), float(row["cpus"]))
                self.assertEqual(int(svc["mem_limit"]), row["memory_mib"] * 2**20)
                self.assertEqual(int(svc["memswap_limit"]), row["memory_swap_mib"] * 2**20)

    def test_every_role_restarts_logs_bounded_and_has_a_healthcheck(self):
        for role, svc in self.services.items():
            with self.subTest(role=role):
                self.assertEqual(svc["restart"], "unless-stopped")
                self.assertTrue(svc["healthcheck"]["test"])
                self.assertEqual(svc["logging"]["driver"], "json-file")
                self.assertIn("max-size", svc["logging"]["options"])
                self.assertIn("max-file", svc["logging"]["options"])
                self.assertIn("no-new-privileges:true", svc.get("security_opt", []))

    def test_new_liveness_healthchecks_for_the_roles_that_had_none(self):
        expected = {"stream_kn_1": "/dev/tcp/127.0.0.1/8210", "stream_kn_2": "/dev/tcp/127.0.0.1/8210",
                    "rust_core": "/dev/tcp/127.0.0.1/8300", "rust_core_2": "/dev/tcp/127.0.0.1/8300",
                    "rust_core_3": "/dev/tcp/127.0.0.1/8300", "market_projector_1": "/status/status.json",
                    "market_projector_2": "/status/status.json"}
        for role, needle in expected.items():
            with self.subTest(role=role):
                self.assertIn(needle, " ".join(self.services[role]["healthcheck"]["test"]))

    def test_ports_are_loopback_only_and_consumer_aliases_exist(self):
        for role, svc in self.services.items():
            for port in svc.get("ports", []):
                with self.subTest(role=role, port=port):
                    self.assertTrue(str(port).startswith("127.0.0.1:"))
        aliases = {role: {a for net in (svc.get("networks") or {}).values() for a in ((net or {}).get("aliases") or [])}
                   for role, svc in self.services.items()}
        self.assertTrue({"qdl-v2-query", "query_v2_1"} <= aliases["query_kn_1"])
        self.assertTrue({"qdl-v2-query", "query_v2_2"} <= aliases["query_kn_2"])
        self.assertIn("qdl-v2-stream-a", aliases["stream_kn_1"])
        self.assertIn("qdl-v2-stream-b", aliases["stream_kn_2"])

    def test_networks_and_volumes_are_the_existing_external_ones(self):
        for key, net in self.doc["networks"].items():
            self.assertTrue(net["external"], key)
        self.assertEqual({n["name"] for n in self.doc["networks"].values()},
                         {"qdl_v2_stable_candidate_stable_internal", "qdl_v2_stable_candidate_stable_egress",
                          "qdl_v2_native_consumer", "executor_network"})
        for key, vol in self.doc["volumes"].items():
            self.assertTrue(vol["external"], key)

    def test_tls_material_only_from_the_generation_variables(self):
        for role, svc in self.services.items():
            for vol in svc.get("volumes", []):
                target, source = vol["target"], str(vol["source"])
                if target in ("/stable-certs", "/kn/tls", "/etc/kafka/secrets") or target.endswith("client-ca-bundle.crt"):
                    with self.subTest(role=role, target=target):
                        self.assertTrue(source.startswith("${QDL_TLS_"), source)
                self.assertNotIn("stable_tls", source)

    def test_no_secret_literal_in_committed_files(self):
        files = [COMPOSE] + [p for p in (ENV_V224, ENV_V225) if p.exists()]
        for path in files:
            text = path.read_text()
            with self.subTest(file=path.name):
                self.assertNotIn("PRIVATE KEY", text)
                for key in SECRET_KEYS:
                    for match in re.finditer(rf"{key}[:=]\s*(.*)", text):
                        self.assertTrue(match.group(1).strip().strip("'\"").startswith("${"), key)
        for role, svc in self.services.items():
            for key, value in (svc.get("environment") or {}).items():
                if re.search(r"SECRET(?!_FILE)|PASSWORD|CURSOR_KEYS_JSON", key) and not key.endswith("_CREDENTIALS"):
                    with self.subTest(role=role, key=key):
                        self.assertTrue(str(value).startswith("${"), key)


SPEC = importlib.util.spec_from_file_location("kn_versioned_compose", ROOT / "scripts/kn_versioned_compose.py")
compose_tool = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compose_tool)


class KnComposeParityCpusTest(unittest.TestCase):
    """DL v2.2.5 window R2: compose ``cpus: 2.25`` reaches the engine as NanoCpus 2249999872 (float32)."""

    def run_parity(self, live_nano_cpus: int, compose_cpus) -> dict:
        live = {
            "image_id": "sha256:kafka", "image_env": {}, "entrypoint": None, "image_entrypoint": None,
            "cmd": None, "image_cmd": None, "user": None, "image_user": None, "stop_timeout": 60,
            "healthcheck": {"Test": ["CMD", "true"]}, "mounts": [], "networks": {},
            "host": {"NanoCpus": live_nano_cpus, "Memory": 0, "MemorySwap": 0, "ReadonlyRootfs": False,
                     "Init": False, "PidsLimit": None, "CapDrop": None, "SecurityOpt": None, "Tmpfs": None,
                     "RestartPolicy": {"Name": "unless-stopped"}, "PortBindings": None,
                     "LogConfig": {"Type": "json-file", "Config": {"max-size": "50m"}}},
        }
        svc = {"image": "kafka@sha256:x", "cpus": compose_cpus, "restart": "unless-stopped",
               "stop_grace_period": "60s", "healthcheck": {"test": ["CMD", "true"]},
               "logging": {"driver": "json-file", "options": {"max-size": "50m"}}}
        inspect = json.dumps([{"Config": {"Env": []}}])
        with mock.patch.object(compose_tool, "ROLES", ("kafka1",)), \
                mock.patch.object(compose_tool, "compose_config", return_value={"services": {"kafka1": svc}}), \
                mock.patch.object(compose_tool, "inspect_role", return_value=live), \
                mock.patch.object(compose_tool, "image_id", return_value="sha256:kafka"), \
                mock.patch.object(compose_tool, "sh", return_value=inspect):
            return compose_tool.parity(Path("compose.yml"), [])

    def test_float32_rounding_of_the_compose_cap_is_not_drift(self):
        result = self.run_parity(2_249_999_872, 2.25)
        self.assertEqual(result["verdict"], "PASS", result["problems"])
        self.assertEqual(result["problems"], [])

    def test_a_real_cap_difference_still_fails(self):
        for live in (2_240_000_000, 2_260_000_000, 2_000_000_000):
            with self.subTest(live=live):
                result = self.run_parity(live, 2.25)
                self.assertEqual(result["verdict"], "FAIL")
                self.assertEqual(len(result["problems"]), 1)
                self.assertTrue(result["problems"][0].startswith("kafka1.cpus: live="), result["problems"])

    def test_tolerance_bounds(self):
        self.assertTrue(compose_tool.cpus_equal(2_249_999_872, 2.25))
        self.assertTrue(compose_tool.cpus_equal(2_000_000_000, "2.0"))
        self.assertTrue(compose_tool.cpus_equal(0, None))
        self.assertFalse(compose_tool.cpus_equal(2_249_998_000, 2.25))  # 2e-6 vCPU: beyond the 1e-6 tolerance
        self.assertFalse(compose_tool.cpus_equal(1_000_000_000, None))


if __name__ == "__main__":
    unittest.main()
