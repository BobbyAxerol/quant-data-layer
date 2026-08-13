from __future__ import annotations

import ast
import os
import unittest
from pathlib import Path
from unittest import mock

from qdl.runtime.roles import RuntimeRole, RuntimeRoleConfig


ROOT = Path(__file__).resolve().parents[1]

try:
    from qdl.runtime.app_factory import create_role_app

    HAS_APP_DEPENDENCIES = True
except ModuleNotFoundError:
    HAS_APP_DEPENDENCIES = False


class RuntimeRoleConfigTests(unittest.TestCase):
    def test_api_role_cannot_claim_ingestion_ownership(self):
        with self.assertRaisesRegex(ValueError, "contradicts"):
            RuntimeRoleConfig.for_entrypoint(
                RuntimeRole.API,
                {"QDL_RUNTIME_ROLE": "api", "QDL_OWNS_LIVE_INGESTION": "true"},
            )

    def test_entrypoint_role_mismatch_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "entrypoint role"):
            RuntimeRoleConfig.for_entrypoint(
                RuntimeRole.HISTORY,
                {"QDL_RUNTIME_ROLE": "api"},
            )

    def test_combined_facade_is_the_only_phase1_ingestion_owner(self):
        for role in (RuntimeRole.API, RuntimeRole.CONTROL, RuntimeRole.HISTORY):
            self.assertFalse(RuntimeRoleConfig.for_entrypoint(role, {}).owns_live_ingestion)
        self.assertTrue(
            RuntimeRoleConfig.for_entrypoint(RuntimeRole.COMPAT_COMBINED, {}).owns_live_ingestion
        )


class StaticRoleBoundaryTests(unittest.TestCase):
    def test_dark_entrypoints_do_not_import_ingestion_or_venue_stream_modules(self):
        forbidden = {
            "app.main",
            "app.ingestion",
            "app.stream.async_live_feed",
            "app.stream.binance_ws",
            "app.stream.dnse_ws",
            "app.stream.vnstock_poller",
        }
        paths = list((ROOT / "app" / "entrypoints").glob("*.py"))
        paths.extend((ROOT / "qdl" / "runtime").glob("*.py"))
        for path in sorted(paths):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            imports = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imports.update(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imports.add(node.module)
            self.assertFalse(
                {
                    module
                    for module in imports
                    if module in forbidden or module.startswith("app.ingestion.")
                },
                path.name,
            )


@unittest.skipUnless(HAS_APP_DEPENDENCIES, "runtime topology tests run in the application image")
class RuntimeRoleTopologyTests(unittest.TestCase):
    def test_three_api_replicas_create_zero_venue_connections(self):
        with mock.patch.dict(os.environ, {"QDL_RUNTIME_ROLE": "api", "QDL_OWNS_LIVE_INGESTION": "false"}, clear=False):
            apps = [create_role_app(RuntimeRole.API) for _ in range(3)]

        for app in apps:
            manifest = app.state.runtime_manifest
            self.assertFalse(manifest["owns_live_ingestion"])
            self.assertNotIn("live_ingestion", manifest["owned_capabilities"])
            paths = {route.path for route in app.routes}
            self.assertIn("/v1/binance/price/{symbol}", paths)
            self.assertNotIn("/v1/control/feed-leases", paths)
            self.assertEqual(
                type(app.state.context.binance_stream_supervisor).__name__,
                "PassiveExternalStreamStatus",
            )

    def test_role_routes_are_non_overlapping_by_ownership(self):
        route_sets = {}
        for role in (RuntimeRole.API, RuntimeRole.CONTROL, RuntimeRole.HISTORY):
            with mock.patch.dict(os.environ, {"QDL_RUNTIME_ROLE": role.value, "QDL_OWNS_LIVE_INGESTION": "false"}, clear=False):
                role_app = create_role_app(role)
            route_sets[role] = {route.path for route in role_app.routes if route.path.startswith("/v1")}

        self.assertIn("/v1/control/feed-leases", route_sets[RuntimeRole.CONTROL])
        self.assertIn("/v1/crypto/ohlcv/{provider}/{symbol}", route_sets[RuntimeRole.HISTORY])
        self.assertTrue(route_sets[RuntimeRole.API].isdisjoint(route_sets[RuntimeRole.CONTROL]))
        self.assertTrue(route_sets[RuntimeRole.CONTROL].isdisjoint(route_sets[RuntimeRole.HISTORY]))


if __name__ == "__main__":
    unittest.main()
