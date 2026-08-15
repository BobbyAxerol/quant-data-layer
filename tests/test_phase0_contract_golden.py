import json
import unittest
from pathlib import Path

from scripts.phase0_audit import build_contract_snapshot


CONTRACT_ROOT = Path(__file__).resolve().parents[1] / "contracts" / "v1"


try:
    import fastapi  # noqa: F401

    HAS_APP_DEPENDENCIES = True
except ModuleNotFoundError:
    HAS_APP_DEPENDENCIES = False


@unittest.skipUnless(HAS_APP_DEPENDENCIES, "exact OpenAPI golden test runs in the application image")
class Phase0ContractGoldenTests(unittest.TestCase):
    def test_v1_openapi_and_sdk_surface_match_frozen_contract(self):
        current_openapi, current_manifest = build_contract_snapshot()
        expected_openapi = json.loads((CONTRACT_ROOT / "openapi.snapshot.json").read_text())
        expected_manifest = json.loads((CONTRACT_ROOT / "public-surface.snapshot.json").read_text())

        expected_manifest.pop("redis_payload_shapes_sha256", None)
        self.assertEqual(current_openapi, expected_openapi)
        self.assertEqual(current_manifest, expected_manifest)

    def test_redis_payload_shape_snapshot_exists(self):
        shapes = json.loads((CONTRACT_ROOT / "redis-payload-shapes.snapshot.json").read_text())

        self.assertIn("trade:price:binance_usdm:*", shapes)
        self.assertIn("kline:1m:*", shapes)
        self.assertIn("vn:quote:*", shapes)


if __name__ == "__main__":
    unittest.main()
