from __future__ import annotations

import json
import unittest
from pathlib import Path

from pydantic import ValidationError

from qdl.api_v2.models import MAX_REQUIREMENT_FRESHNESS_MS, RequirementModel
from scripts.generate_phase5_openapi import build_openapi


ROOT = Path(__file__).resolve().parents[1]


class Phase5OpenApiContractTests(unittest.TestCase):
    def test_final_bar_freshness_bound_is_uniform_and_fail_closed(self):
        payload = {
            "instrument_uid": "BINANCE.USDM.PERPETUAL.BTC-USDT",
            "feed": "BAR",
            "consumer_grade": "ALPHA",
            "source_policy_id": "alpha_crypto_primary_v2",
            "interval": "1w",
            "max_freshness_ms": MAX_REQUIREMENT_FRESHNESS_MS,
        }
        self.assertEqual(
            RequirementModel.model_validate(payload).max_freshness_ms,
            MAX_REQUIREMENT_FRESHNESS_MS,
        )
        with self.assertRaises(ValidationError):
            RequirementModel.model_validate(
                {**payload, "max_freshness_ms": MAX_REQUIREMENT_FRESHNESS_MS + 1}
            )

        openapi = build_openapi()
        requirement_schema = openapi["components"]["schemas"]["RequirementModel"]
        self.assertEqual(
            requirement_schema["properties"]["max_freshness_ms"]["anyOf"][0]["maximum"],
            float(MAX_REQUIREMENT_FRESHNESS_MS),
        )
        for route in (
            "/v2/market-data/{instrument_uid}/snapshot",
            "/v2/market-data/{instrument_uid}/warmup",
            "/v2/market-data/{instrument_uid}/history",
            "/v2/feeds/{instrument_uid}/status",
        ):
            parameters = openapi["paths"][route]["get"]["parameters"]
            freshness = next(
                item for item in parameters if item["name"] == "max_freshness_ms"
            )
            self.assertEqual(
                freshness["schema"]["anyOf"][0]["maximum"],
                MAX_REQUIREMENT_FRESHNESS_MS,
            )

    def test_v2_openapi_matches_frozen_snapshot_and_has_typed_public_responses(self):
        expected = json.loads(
            (ROOT / "contracts/v2/openapi.snapshot.json").read_text(encoding="utf-8")
        )
        current = build_openapi()
        self.assertEqual(current, expected)
        paths = current["paths"]
        self.assertEqual(len(paths), 11)
        self.assertIn("/v2/market-data/reference:batch", paths)
        reference = paths["/v2/market-data/reference:batch"]["post"]
        self.assertIn("application/json", reference["requestBody"]["content"])
        self.assertIn("application/json", reference["responses"]["200"]["content"])
        for path, operations in paths.items():
            for method, operation in operations.items():
                if method not in {"get", "post"}:
                    continue
                success = operation["responses"]["200"]["content"]["application/json"]
                self.assertIn("schema", success, f"untyped success response: {method} {path}")
                self.assertEqual(operation["security"], [{
                    "QDLWorkloadBearer": [],
                    "QDLConsumerIdentity": [],
                }])


if __name__ == "__main__":
    unittest.main()
