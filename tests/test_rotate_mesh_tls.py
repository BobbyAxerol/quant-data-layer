"""scripts/rotate_mesh_tls.py: generation layout contract and the pure bundle/certificate helpers.

The real generate/verify run needs root and Docker (it is executed once per window preparation and its verify
receipt is kept with the generation); here only what can be proven without them.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("rotate_mesh_tls", ROOT / "scripts/rotate_mesh_tls.py")
tool = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tool)


def self_signed(directory: Path, cn: str) -> bytes:
    key, crt = directory / f"{cn}.key", directory / f"{cn}.crt"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", f"/CN={cn}",
                    "-keyout", str(key), "-out", str(crt)], check=True, capture_output=True)
    return crt.read_bytes()


class GeneratorContractTest(unittest.TestCase):
    def test_generator_issues_the_kn_principals_and_keeps_their_keys(self):
        script = (ROOT / "scripts/phase80_generate_tls.sh").read_text()
        self.assertIn("for client in kn-projector kn-stream; do", script)
        self.assertIn("! -name 'kn-projector.key'", script)
        self.assertIn("! -name 'kn-stream.key'", script)
        self.assertIn('rm -f "${OUTPUT_DIR}"/*.csr', script)

    def test_rotation_defaults_and_layout(self):
        self.assertEqual(tool.DEFAULT_DAYS, 365)
        self.assertEqual(set(tool.STABLE_CLIENT_ROLES), {"core", "producer", "projector", "authority-dispatcher"})
        self.assertEqual(tool.STABLE_SERVER_ROLES, {"query": "stable-query", "stream": "stable-stream"})
        self.assertEqual(tool.KN_PRINCIPALS, ("kn-projector", "kn-stream"))
        self.assertEqual(set(tool.ENV_KEYS) - {"QDL_TLS_GENERATION"},
                         {"QDL_TLS_KAFKA_DIR", "QDL_TLS_STABLE_CERTS_DIR", "QDL_TLS_KN_DIR",
                          "QDL_TLS_QUERY_CLIENT_CA_FILE", "QDL_TLS_STREAM_CLIENT_CA_FILE", "QDL_TLS_TS_IDENTITY_DIR"})
        source = (ROOT / "scripts/rotate_mesh_tls.py").read_text()
        # the brokers' dynamic listener truststore path (KRaft metadata) must exist in every generation
        self.assertIn('"truststore-kn-v220.jks"', source)
        self.assertIn("QDL_PHASE8_CERT_PASSWORD", source)  # deployed store password carried, never printed

    def test_private_material_is_never_world_readable(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            for name in ("kafka1.keystore.p12", "store.password", "admin.properties", "truststore.jks", "x.key"):
                (d / name).write_text("x")
                for subtree in tool.PERMS:
                    self.assertFalse(tool._wanted_mode(d / name, subtree) & 0o004, (name, subtree))
            (d / "ca.crt").write_text("x")
            self.assertEqual(tool._wanted_mode(d / "ca.crt", "kafka"), 0o444)
            self.assertEqual(tool._wanted_mode(d, "kafka"), 0o750)
            self.assertEqual(tool._wanted_mode(d / "x.key", "kn-tls"), 0o400)
        self.assertEqual(tool.PERMS["kafka"], (1000, 1000))  # appuser of the pinned Kafka image
        self.assertEqual(tool.PERMS["stable-certs"], (10001, 10001))
        self.assertEqual(tool.PERMS["identities"], (0, 0))  # the TS writer runs as root

    def test_env_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = {k: f"/x/{k.lower()}" for k in tool.ENV_KEYS}
            path = Path(tmp) / "generation.env"
            tool.write_env(path, env, "test")
            self.assertEqual(tool.read_env(path), env)
            path.write_text("QDL_TLS_GENERATION=x\n")
            with self.assertRaises(ValueError):
                tool.read_env(path)


@unittest.skipUnless(shutil.which("openssl"), "openssl CLI not installed")
class BundleHelpersTest(unittest.TestCase):
    def test_replace_mesh_ca_keeps_every_other_ca_in_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            old, ext1, ext2, new = (self_signed(d, cn) for cn in ("old-mesh", "external-1", "external-2", "new-mesh"))
            bundle = old + ext1 + ext2
            out = tool.replace_mesh_ca(bundle, tool.cert_facts(old)["sha256_fingerprint"], new)
            names = [tool.cert_facts(b)["cn"] for b in tool.pem_blocks(out)]
            self.assertEqual(names, ["new-mesh", "external-1", "external-2"])
            with self.assertRaises(RuntimeError):  # the old CA must be present exactly once
                tool.replace_mesh_ca(ext1 + ext2, tool.cert_facts(old)["sha256_fingerprint"], new)

    def test_cert_facts_reads_cn_ca_flag_and_validity(self):
        with tempfile.TemporaryDirectory() as tmp:
            pem = self_signed(Path(tmp), "probe-ca")
            facts = tool.cert_facts(pem)
            self.assertEqual(facts["cn"], "probe-ca")
            self.assertTrue(facts["ca"])
            self.assertRegex(facts["not_after"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
            self.assertEqual(len(facts["sha256_fingerprint"]), 64)
            self.assertEqual(facts["public_key_sha256"], tool.key_public_sha256(Path(tmp) / "probe-ca.key"))


if __name__ == "__main__":
    unittest.main()
