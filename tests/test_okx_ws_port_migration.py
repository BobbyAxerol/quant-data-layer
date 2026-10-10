"""OKX WebSocket port 8443 retirement (OKX changelog, cutover 2026-10-31).

Covers the endpoint owner, the acquisition-plan guard, the rule that an
endpoint move is not an identity change, and the transport-only migration of a
live ingestor runtime config.
"""

from __future__ import annotations

import dataclasses
import json
import os
import tempfile
import unittest
from pathlib import Path

import yaml

from qdl.adapters.okx import client as okx_client
from qdl.adapters.okx_endpoints import (
    OKX_BUSINESS_WS,
    OKX_PUBLIC_WS,
    require_okx_websocket_url,
    without_retired_port,
)
from qdl.runtime import l2_demand, production_catalog
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan, stable_authority_record
from scripts import migrate_okx_ws_port as tool

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
OLD_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
OLD_BUSINESS = "wss://ws.okx.com:8443/ws/v5/business"


class EndpointOwnerTests(unittest.TestCase):
    def test_owner_urls_carry_no_port(self):
        self.assertEqual(OKX_PUBLIC_WS, "wss://ws.okx.com/ws/v5/public")
        self.assertEqual(OKX_BUSINESS_WS, "wss://ws.okx.com/ws/v5/business")

    def test_every_consumer_uses_the_owner(self):
        self.assertIs(okx_client.OKX_PUBLIC_WS, OKX_PUBLIC_WS)
        self.assertIs(okx_client.OKX_BUSINESS_WS, OKX_BUSINESS_WS)
        self.assertIs(production_catalog.OKX_PUBLIC_WS, OKX_PUBLIC_WS)
        self.assertIs(production_catalog.OKX_BUSINESS_WS, OKX_BUSINESS_WS)
        self.assertIs(l2_demand.OKX_PUBLIC_WS, OKX_PUBLIC_WS)

    def test_retired_port_is_refused_with_the_cutover_date(self):
        with self.assertRaisesRegex(ValueError, "2026-10-31"):
            require_okx_websocket_url(OLD_PUBLIC, "public")
        with self.assertRaisesRegex(ValueError, "2026-10-31"):
            require_okx_websocket_url(OLD_BUSINESS, "business")

    def test_admission_rule(self):
        for url, service in (
            (OKX_PUBLIC_WS, "public"),
            (OKX_BUSINESS_WS, "business"),
            ("wss://ws.okx.com:443/ws/v5/public", "public"),
            ("wss://wspap.okx.com/ws/v5/private", "private"),
        ):
            with self.subTest(url=url):
                self.assertEqual(require_okx_websocket_url(url, service), url)
        for url, service in (
            ("ws://ws.okx.com/ws/v5/public", "public"),
            ("wss://ws.okx.com.evil.test/ws/v5/public", "public"),
            ("wss://ws.okx.com:9443/ws/v5/public", "public"),
            (OKX_PUBLIC_WS, "business"),
            ("wss://ws.okx.com/ws/v5/public?x=1", "public"),
            (None, "public"),
        ):
            with self.subTest(url=url, service=service):
                with self.assertRaises(ValueError):
                    require_okx_websocket_url(url, service)

    def test_port_removal_keeps_host_and_path_and_is_idempotent(self):
        self.assertEqual(without_retired_port(OLD_PUBLIC), OKX_PUBLIC_WS)
        self.assertEqual(
            without_retired_port("wss://wspap.okx.com:8443/ws/v5/business"),
            "wss://wspap.okx.com/ws/v5/business",
        )
        self.assertEqual(without_retired_port(OKX_PUBLIC_WS), OKX_PUBLIC_WS)


class AcquisitionPlanTests(unittest.TestCase):
    def setUp(self):
        self.catalog = StableSourceCatalog.load(CATALOG)
        self.plan = StableAcquisitionPlan.load(ACQUISITION, catalog=self.catalog)

    def test_committed_plan_uses_port_443_for_every_okx_native_binding(self):
        self.assertNotIn(b":8443", ACQUISITION.read_bytes())
        okx = [
            item for item in self.plan.bindings
            if item.runtime == "OKX" and item.mode == "RUST_NATIVE"
        ]
        self.assertTrue(okx)
        for item in okx:
            self.assertEqual(item.websocket_url, OKX_PUBLIC_WS)
            self.assertEqual(item.business_websocket_url, OKX_BUSINESS_WS)

    def test_a_plan_that_still_names_port_8443_does_not_load(self):
        payload = yaml.safe_load(ACQUISITION.read_text(encoding="utf-8"))
        target = next(
            item for item in payload["bindings"]
            if item["runtime"] == "OKX" and item["mode"] == "RUST_NATIVE"
        )
        target["business_websocket_url"] = OLD_BUSINESS
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "acquisition.yaml"
            path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "8443"):
                StableAcquisitionPlan.load(path, catalog=self.catalog)

    def test_an_endpoint_move_is_not_an_identity_change(self):
        moved = dataclasses.replace(
            self.plan,
            bindings=tuple(
                dataclasses.replace(
                    item,
                    websocket_url=OLD_PUBLIC if item.websocket_url else None,
                    business_websocket_url=OLD_BUSINESS if item.business_websocket_url else None,
                )
                if item.runtime == "OKX" else item
                for item in self.plan.bindings
            ),
        )
        self.assertNotEqual(moved.bindings, self.plan.bindings)
        self.assertEqual(moved.revision, self.plan.revision)
        self.assertEqual(moved.identity_digest(), self.plan.identity_digest())

    def test_identity_changes_are_still_detected(self):
        first = self.plan.bindings[0]
        renamed = dataclasses.replace(
            self.plan,
            bindings=(dataclasses.replace(first, native_channel=first.native_channel + "x"),)
            + self.plan.bindings[1:],
        )
        disabled = dataclasses.replace(
            self.plan,
            bindings=(dataclasses.replace(first, enabled=not first.enabled),)
            + self.plan.bindings[1:],
        )
        bumped = dataclasses.replace(self.plan, revision=self.plan.revision + 1)
        digests = {
            self.plan.identity_digest(), renamed.identity_digest(),
            disabled.identity_digest(), bumped.identity_digest(),
        }
        self.assertEqual(len(digests), 4)

    def test_generated_okx_ingestor_config_carries_the_new_urls_only(self):
        authority = stable_authority_record(
            rust_image_digest="sha256:" + "a" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION.read_bytes(),
            effective_at_ns=1,
        )
        configs = self.plan.native_ingestor_configs(catalog=self.catalog, authority=authority)
        okx = {key: value for key, value in configs.items() if value["runtime"] == "OKX"}
        self.assertTrue(okx)
        for value in okx.values():
            self.assertEqual(value["websocket_url"], OKX_PUBLIC_WS)
            self.assertEqual(value["business_websocket_url"], OKX_BUSINESS_WS)
            self.assertEqual(value["config_revision"], self.plan.revision)
        for value in configs.values():
            self.assertNotIn(":8443", json.dumps(value))


def _live_like_config() -> dict:
    """An OKX ingestor config shaped like the live one, still on port 8443."""
    return {
        "runtime": "OKX",
        "websocket_url": OLD_PUBLIC,
        "business_websocket_url": OLD_BUSINESS,
        "market_websocket_url": None,
        "config_revision": 20,
        "shard_id": "qdl-v2-stable-okx-swap",
        "authority": {"revision": 1, "partition_plan_digest": "b" * 64},
        "bindings": [
            {"native_channel": "trades", "native_symbol": "BTC-USDT-SWAP",
             "instrument_catalog_revision": 11, "subscription_id": "okx:x"},
        ],
    }


def _encode(payload: dict) -> bytes:
    return (json.dumps(payload, indent=2, sort_keys=True, separators=(",", ": ")) + "\n").encode()


class RuntimeConfigMigrationTests(unittest.TestCase):
    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.path = self.root / "ingestor_okx_swap.json"
        self.original = _encode(_live_like_config())
        self.path.write_bytes(self.original)
        self.path.chmod(0o640)

    def tearDown(self):
        self._directory.cleanup()

    def _backups(self):
        return sorted(self.root.glob(self.path.name + tool.BACKUP_SUFFIX + "*"))

    def test_dry_run_writes_nothing(self):
        report = tool.migrate(self.path, apply=False)
        self.assertEqual(report["status"], "DRY_RUN")
        self.assertEqual(self.path.read_bytes(), self.original)
        self.assertEqual(self._backups(), [])
        self.assertEqual(sorted(report["changes"]), ["business_websocket_url", "websocket_url"])

    def test_apply_changes_only_the_two_urls_and_keeps_a_backup(self):
        report = tool.migrate(self.path, apply=True, clock=lambda: 7)
        self.assertEqual(report["status"], "APPLIED")
        after = json.loads(self.path.read_bytes())
        before = json.loads(self.original)
        self.assertEqual(after["websocket_url"], OKX_PUBLIC_WS)
        self.assertEqual(after["business_websocket_url"], OKX_BUSINESS_WS)
        for key in before:
            if key not in ("websocket_url", "business_websocket_url"):
                self.assertEqual(after[key], before[key], key)
        self.assertEqual(tool.identity_sha256(after), tool.identity_sha256(before))
        # Byte-level: exactly the two URL lines differ.
        diff = [
            (a, b) for a, b in zip(self.original.splitlines(), self.path.read_bytes().splitlines())
            if a != b
        ]
        self.assertEqual(len(diff), 2)
        self.assertEqual(len(self.original.splitlines()), len(self.path.read_bytes().splitlines()))
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o640)
        (backup,) = self._backups()
        self.assertEqual(backup.read_bytes(), self.original)
        self.assertEqual(report["backup"], str(backup))
        self.assertEqual(list(self.root.glob(".*staging*")), [])

    def test_second_run_is_a_no_op(self):
        tool.migrate(self.path, apply=True, clock=lambda: 1)
        migrated = self.path.read_bytes()
        report = tool.migrate(self.path, apply=True, clock=lambda: 2)
        self.assertEqual(report["status"], "NO_CHANGE")
        self.assertEqual(self.path.read_bytes(), migrated)
        self.assertEqual(len(self._backups()), 1)

    def test_rollback_restores_the_exact_bytes(self):
        report = tool.migrate(self.path, apply=True, clock=lambda: 3)
        result = tool.rollback(self.path, Path(report["backup"]))
        self.assertEqual(result["status"], "ROLLED_BACK")
        self.assertEqual(self.path.read_bytes(), self.original)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o640)

    def test_rollback_refuses_a_backup_with_a_different_identity(self):
        report = tool.migrate(self.path, apply=True, clock=lambda: 4)
        backup = Path(report["backup"])
        tampered = _live_like_config()
        tampered["config_revision"] = 21
        backup.write_bytes(_encode(tampered))
        with self.assertRaisesRegex(ValueError, "identity"):
            tool.rollback(self.path, backup)
        with self.assertRaisesRegex(ValueError, "not a backup"):
            tool.rollback(self.path, self.root / "other.json")

    def test_refuses_non_okx_and_foreign_hosts(self):
        binance = _live_like_config() | {"runtime": "BINANCE"}
        self.path.write_bytes(_encode(binance))
        with self.assertRaisesRegex(ValueError, "not an OKX"):
            tool.migrate(self.path, apply=False)
        foreign = _live_like_config() | {"websocket_url": "wss://example.test:8443/ws/v5/public"}
        self.path.write_bytes(_encode(foreign))
        with self.assertRaises(ValueError):
            tool.migrate(self.path, apply=False)
        self.assertEqual(self._backups(), [])

    def test_compact_json_is_rewritten_textually(self):
        compact = json.dumps(_live_like_config(), separators=(",", ":")).encode()
        self.path.write_bytes(compact)
        tool.migrate(self.path, apply=True, clock=lambda: 5)
        self.assertEqual(
            self.path.read_bytes(),
            compact.replace(b":8443", b""),
        )


if __name__ == "__main__":
    unittest.main()
