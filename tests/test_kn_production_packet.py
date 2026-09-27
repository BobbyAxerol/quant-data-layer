"""KN-5 packet safety: native read plane never mounts the production spool."""
import json
from pathlib import Path
from unittest.mock import patch

import tempfile
import unittest

from scripts import kn_production_packet as packet


def check_query_settings_keep_admission_and_quota_not_sqlite():
    original = {"QDL_STABLE_REDIS_URL": "redis://quota:6379/0",
                "QDL_STABLE_PROVIDER_ADMISSION_URL": "http://core:8300",
                "QDL_STABLE_EXECUTION_MARK_INDEX_URLS_JSON": '["https://old-stream"]'}
    result = packet.query_environment(original, topic_id="topic", generation="v220", replica=2)
    assert result["QDL_STABLE_REDIS_URL"] == original["QDL_STABLE_REDIS_URL"]
    assert result["QDL_STABLE_PROVIDER_ADMISSION_URL"] == original["QDL_STABLE_PROVIDER_ADMISSION_URL"]
    assert result["QDL_STABLE_EXECUTION_MARK_INDEX_URLS_JSON"] == "[]"
    assert result["QDL_STABLE_QUERY_BACKEND"] == "kn3"
    assert result["QDL_STABLE_STATE_DIR"] == "/var/lib/qdl-kn/runtime"
    assert result["QDL_KN_ROUTE_GENERATION"] == "v220"
    assert original["QDL_STABLE_EXECUTION_MARK_INDEX_URLS_JSON"] != "[]"


def check_prepare_never_overwrites_runtime(tmp_path):
    with unittest.TestCase().assertRaisesRegex(ValueError, "overwrite"):
        packet.prepare(tmp_path, Path("unused"), "topic", "generation")


def check_exact_packet_shape_and_private_files(tmp_path):
    original = {"Config": {"Env": ["QDL_DATA_JWT_ISSUER=issuer", "QDL_DATA_JWT_AUDIENCE=aud",
        'QDL_DATA_JWT_KEYS_JSON={"key":"PUBLIC"}', 'QDL_DATA_JWT_KEY_SUBJECTS_JSON={"key":"subject"}',
        "QDL_STABLE_REDIS_URL=redis://quota:6379/0", "QDL_STABLE_REDIS_PREFIX=qdl:identity"],
        "Healthcheck": {"Test": ["CMD", "probe"], "Interval": 10000000000, "Retries": 3}},
        "Mounts": [{"Source": "/approved/runtime", "Destination": "/runtime"},
                   {"Source": "/approved/state", "Destination": "/var/lib/qdl-stable"}]}
    bundle = tmp_path / "bundle.json"
    bundle.write_text(json.dumps({"catalog": {"catalog_revision": 11}}))
    out = tmp_path / "output"
    with patch.object(packet, "inspected", return_value=original), patch.object(packet.os, "chown"):
        receipt = packet.prepare(out, bundle, "topic", "generation")
    compose = json.loads((out / "compose.json").read_text())
    assert len(compose["services"]) == 7
    assert receipt["production_mutations"] == []
    assert (out / "cursor-keys.json").stat().st_mode & 0o777 == 0o640
    for name, service in compose["services"].items():
        assert "ports" not in service or all(p.startswith("127.0.0.1:") for p in service["ports"])
        assert service["mem_limit"] and service["cpus"] > 0
        assert not any("/approved/state:" in v for v in service.get("volumes", []))
        assert not any("sqlite" in v for v in service.get("volumes", []))
        if name.startswith("query"):
            assert service["healthcheck"]["interval"] == "10000000000ns"
        if name.startswith("stream"):
            assert service["environment"]["QDL_KN_READ_VIEW_URLS"] == "https://qdl-v2-query:8200"
    assert not (out / "authority.json").exists()


class ProductionPacketTests(unittest.TestCase):
    def test_query_settings(self):
        check_query_settings_keep_admission_and_quota_not_sqlite()

    def test_refuse_overwrite(self):
        with tempfile.TemporaryDirectory() as root:
            check_prepare_never_overwrites_runtime(Path(root))

    def test_exact_packet(self):
        with tempfile.TemporaryDirectory() as root:
            check_exact_packet_shape_and_private_files(Path(root))


class RollbackPacketTests(unittest.TestCase):
    def test_effective_role_preserved_not_compose_defaults(self):
        source = {"Image": "sha256:actual", "Name": "/running-core", "Mounts": [
            {"Type": "bind", "Source": "/old/core.json", "Destination": "/runtime/core.json", "RW": False},
            {"Type": "volume", "Name": "tls", "Source": "/docker/tls", "Destination": "/cert", "RW": False}],
            "Config": {"Env": ["SECRET=private$F"], "Cmd": ["/runtime/core.json"], "Entrypoint": ["core"],
                "User": "10001:10001", "WorkingDir": "/app", "StopTimeout": 45},
            "HostConfig": {"ReadonlyRootfs": True, "RestartPolicy": {"Name": "unless-stopped"},
                "LogConfig": {"Type": "json-file", "Config": {"max-size": "20m"}}, "NanoCpus": 1000000000,
                "Memory": 268435456, "Init": True, "CapDrop": ["ALL"]},
            "NetworkSettings": {"Networks": {"private": {"Aliases": ["core", "core"]}}}}
        service = packet.service_from_inspect(source)
        result = packet.external_compose({"rust_core": service})
        assert service["image"] == "sha256:actual"
        assert service["cpus"] == 1
        assert service["environment"] == {"SECRET": "private$F"}
        assert service["volumes"][0]["source"] == "/old/core.json"
        assert service["volumes"][1]["source"] == "tls"
        assert result["volumes"]["tls"] == {"external": True, "name": "tls"}
        assert service["networks"]["private"]["aliases"] == ["core"]


class ComposeLiteralTests(unittest.TestCase):
    def test_docker_values_do_not_expand_again(self):
        result = packet.compose_literal({"healthcheck": ["test $F = ${EXPECTED}"],
                                         "environment": {"KEY": "private$F"}, "cpus": 0.5})
        assert result["healthcheck"] == ["test $$F = $${EXPECTED}"]
        assert result["environment"]["KEY"] == "private$$F"
        assert result["cpus"] == 0.5


class RevisionBoundaryTests(unittest.TestCase):
    def fixture(self):
        import copy
        binding = {"provider": "VENUE", "native_symbol": "PAIR", "native_channel": "trade",
                   "instrument_catalog_revision": 11}
        cores = {role: {"core": {"bindings": [copy.deepcopy(binding)]}}
                 for role in ("rust_core", "rust_core_2", "rust_core_3")}
        env = {"QDL_PROVIDER_ADMISSION_" + key: "1" for key in (
            "ENABLED", "LISTEN_ADDR", "POLICY_PATH", "POLICY_SHA256", "REDIS_PREFIX", "REDIS_URL", "SECRET")}
        ingestors = {role: {"bindings": [copy.deepcopy(binding)]}
                     for role in ("ingestor_binance_usdm", "ingestor_okx_swap")}
        return [cores, ingestors, {r: dict(env) for r in cores},
                {"catalog_revision": 11, "bindings": [{"binding_id": "b"}]},
                {"bindings": [{"binding_id": "b"}]}]

    def test_effective_all_replicas(self):
        self.assertEqual(packet.validate_revision_boundary(*self.fixture())["cores"], 3)

    def test_distinct_legacy_mount_rejected(self):
        args = self.fixture(); args[0]["rust_core_2"]["core"]["bindings"][0]["instrument_catalog_revision"] = 9
        with self.assertRaisesRegex(ValueError, "rust_core_2"):
            packet.validate_revision_boundary(*args)

    def test_missing_admission_env_rejected(self):
        args = self.fixture(); del args[2]["rust_core_3"]["QDL_PROVIDER_ADMISSION_REDIS_PREFIX"]
        with self.assertRaisesRegex(ValueError, "incomplete"):
            packet.validate_revision_boundary(*args)

    def test_mixed_ingestor_revision_rejected(self):
        args = self.fixture(); args[1]["ingestor_okx_swap"]["bindings"][0]["instrument_catalog_revision"] = 9
        with self.assertRaisesRegex(ValueError, "revision mismatch"):
            packet.validate_revision_boundary(*args)

    def test_catalogue_only_roll_rejected(self):
        args = self.fixture(); args[4]["bindings"] = []
        with self.assertRaisesRegex(ValueError, "binding sets"):
            packet.validate_revision_boundary(*args)

    def test_missing_replica_and_subscription_rejected(self):
        args = self.fixture(); del args[0]["rust_core_3"]
        with self.assertRaisesRegex(ValueError, "three"):
            packet.validate_revision_boundary(*args)
        args = self.fixture(); args[1]["ingestor_binance_usdm"]["bindings"][0]["native_channel"] = "unmapped"
        with self.assertRaisesRegex(ValueError, "missing from core"):
            packet.validate_revision_boundary(*args)

    def test_physical_component_identity_is_not_logical_mark_index_identity(self):
        args = self.fixture()
        for core in args[0].values():
            b = core["core"]["bindings"][0]
            b.update(native_symbol="LOGICAL-SWAP", native_channel="mark_index",
                     physical_native_symbol="PAIR", physical_native_channel="trade")
        self.assertEqual(packet.validate_revision_boundary(*args)["cores"], 3)
