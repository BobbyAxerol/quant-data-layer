from __future__ import annotations

import json
import unittest
from pathlib import Path

from qdl.admission import (
    ADMISSION_AUTHORITY,
    ADMISSION_SCHEMA,
    AdmissionContractError,
    AdmissionDisposition,
    AdmissionPriority,
    AdmissionRequest,
    ProviderLane,
    RustAdmissionProjection,
)


LANE = ProviderLane("BINANCE", "USDM", "REFERENCE_NATIVE_BASIS")


def granted(request: AdmissionRequest) -> dict[str, object]:
    return {
        "schema": ADMISSION_SCHEMA,
        "authority": ADMISSION_AUTHORITY,
        "lane": request.lane.as_mapping(),
        "request_id": request.request_id,
        "disposition": "GRANTED",
        "defer_reason": None,
        "retry_after_ms": None,
        "lease_expires_at_ns": 1_000_000,
        "coalesced": False,
    }


class _Client:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.requests: list[dict[str, object]] = []

    async def decide(self, request: dict[str, object]) -> dict[str, object]:
        self.requests.append(request)
        return self.response


class ProviderAdmissionBindingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = AdmissionRequest(
            lane=LANE,
            request_id="basis:BTCUSDT:abc123",
            priority=AdmissionPriority.BATCH,
            token_cost=1,
        )

    async def test_python_relays_exact_rust_grant_without_local_policy(self):
        client = _Client(granted(self.request))
        decision = await RustAdmissionProjection(client).admit(self.request)
        self.assertEqual(decision.disposition, AdmissionDisposition.GRANTED)
        self.assertEqual(client.requests, [self.request.as_mapping()])

    async def test_python_rejects_cross_lane_or_request_identity(self):
        response = granted(self.request)
        response["request_id"] = "basis:ETHUSDT:abc123"
        with self.assertRaises(AdmissionContractError):
            await RustAdmissionProjection(_Client(response)).admit(self.request)
        response = granted(self.request)
        response["lane"] = ProviderLane("OKX", "SWAP", "REFERENCE_NATIVE_BASIS").as_mapping()
        with self.assertRaises(AdmissionContractError):
            await RustAdmissionProjection(_Client(response)).admit(self.request)

    async def test_python_rejects_widened_or_malformed_defer(self):
        response = granted(self.request)
        response.update({
            "disposition": "DEFERRED",
            "defer_reason": "COOLDOWN",
            "retry_after_ms": 1,
            "lease_expires_at_ns": None,
            "coalesced": True,
        })
        with self.assertRaises(AdmissionContractError):
            await RustAdmissionProjection(_Client(response)).admit(self.request)
        response = granted(self.request)
        response["authority"] = "PYTHON_OVERRIDE"
        with self.assertRaises(AdmissionContractError):
            await RustAdmissionProjection(_Client(response)).admit(self.request)

    async def test_unknown_fields_fail_closed(self):
        response = granted(self.request)
        response["override"] = "grant"
        with self.assertRaises(AdmissionContractError):
            await RustAdmissionProjection(_Client(response)).admit(self.request)

    def test_zero_token_cost_fails_closed_before_any_rust_transport_call(self):
        with self.assertRaises(AdmissionContractError):
            AdmissionRequest(
                lane=LANE,
                request_id="basis:BTCUSDT:zero",
                priority=AdmissionPriority.BATCH,
                token_cost=0,
            )

    def test_json_schema_keeps_the_same_authority_and_decision_shapes(self):
        schema_path = Path(__file__).parents[1] / "contracts" / "provider-admission.v1.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        self.assertEqual(schema["properties"]["schema"], {"const": ADMISSION_SCHEMA})
        self.assertEqual(schema["properties"]["authority"], {"const": ADMISSION_AUTHORITY})
        self.assertEqual(len(schema["allOf"]), 2)
        granted_shape, deferred_shape = schema["allOf"]
        self.assertEqual(
            granted_shape["then"]["properties"]["lease_expires_at_ns"]["type"], "integer"
        )
        self.assertEqual(
            deferred_shape["then"]["properties"]["coalesced"], {"const": False}
        )


if __name__ == "__main__":
    unittest.main()
