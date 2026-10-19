"""Contract of the versioned KN compose (deploy/compose/qdl-v2-stable-kn.yml, DL v2.2.5).

Static checks only (no Docker): the 17 production roles, images by variable with every release env pinning a
digest, the v2.2.4 env equal to the certified runtime images and caps, healthcheck/logging/restart on every role,
loopback-only ports, the consumer aliases, external networks/volumes, TLS sources only from the generation
variables, and no secret literal in any committed file. Live parity is ``scripts/kn_versioned_compose.py parity``.
"""
from __future__ import annotations

import json
from pathlib import Path
import re
import unittest

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


if __name__ == "__main__":
    unittest.main()
