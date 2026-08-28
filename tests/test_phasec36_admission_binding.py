"""C3.6-C.2 tests for the private Rust admission binding.

These tests intentionally use only deterministic fake provider responses.  They
prove Python relays an exact signed request, leaves all admission policy in
Rust, and preserves the existing local behavior when the reviewed binding is
absent.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import unittest
from pathlib import Path

import httpx

from app.providers.binance.rest import BinanceProviderError
from qdl.adapters.binance.reference import BinanceUsdmReferenceAdapter
from qdl.admission import (
    ADMISSION_AUTHORITY,
    ADMISSION_SCHEMA,
    AdmissionDecision,
    AdmissionDeferReason,
    AdmissionDisposition,
    AdmissionPriority,
    AdmissionRequest,
    AdmissionTransportError,
    ProviderLane,
    RustHttpProviderAdmission,
)
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    ProductType,
)
from qdl.reference.batch import ReferenceBatch
from qdl.reference.contracts import (
    BasisSeries,
    ReferenceProduct,
    ReferenceRequest,
    ReferenceStatus,
)


_SECRET = b"c36-provider-admission-test-secret-32"
_LANE = ProviderLane("BINANCE", "USDM", "REFERENCE_NATIVE_BASIS")
ROOT = Path(__file__).resolve().parents[1]


def _instrument(symbol: str = "BTCUSDT") -> InstrumentRecord:
    return InstrumentRecord(
        identity=InstrumentIdentity.create(
            venue="BINANCE",
            market="USDM",
            product_type=ProductType.PERPETUAL,
            canonical_symbol="BTC-USDT",
        ),
        metadata_revision=1,
        asset_class=AssetClass.DERIVATIVE,
        native_symbol=symbol,
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        price_tick=CanonicalDecimal.from_text("0.01"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
    )


def _basis_request() -> ReferenceRequest:
    return ReferenceRequest(
        instrument=_instrument(),
        product=ReferenceProduct.BASIS,
        start_ms=1_000,
        end_ms=2_000,
        interval="1h",
        limit=1,
        page_size=1,
        max_pages=1,
        basis_series=BasisSeries.NATIVE,
        basis_contract_type="PERPETUAL",
    )


def _decision(
    request: AdmissionRequest,
    *,
    disposition: AdmissionDisposition = AdmissionDisposition.GRANTED,
) -> AdmissionDecision:
    if disposition is AdmissionDisposition.GRANTED:
        return AdmissionDecision(
            lane=request.lane,
            request_id=request.request_id,
            disposition=disposition,
            defer_reason=None,
            retry_after_ms=None,
            lease_expires_at_ns=1_900_000_000_000_000_000,
            coalesced=False,
        )
    return AdmissionDecision(
        lane=request.lane,
        request_id=request.request_id,
        disposition=disposition,
        defer_reason=AdmissionDeferReason.COOLDOWN,
        retry_after_ms=60_000,
        lease_expires_at_ns=None,
        coalesced=False,
    )


class _RecordingAdmission:
    def __init__(self, *, disposition: AdmissionDisposition = AdmissionDisposition.GRANTED):
        self.disposition = disposition
        self.admits: list[AdmissionRequest] = []
        self.completions: list[tuple[ProviderLane, str]] = []
        self.rate_limits: list[tuple[ProviderLane, str | None, int | None, int | None, int | None]] = []

    async def admit(self, request: AdmissionRequest) -> AdmissionDecision:
        self.admits.append(request)
        return _decision(request, disposition=self.disposition)

    async def complete(self, lane: ProviderLane, request_id: str) -> bool:
        self.completions.append((lane, request_id))
        return True

    async def record_rate_limit(
        self,
        lane: ProviderLane,
        request_id: str | None,
        *,
        http_status: int | None,
        provider_code: int | None,
        retry_after_ms: int | None,
    ) -> AdmissionDecision:
        self.rate_limits.append((lane, request_id, http_status, provider_code, retry_after_ms))
        request = AdmissionRequest(
            lane=lane,
            request_id=request_id or "rate-limit",
            priority=AdmissionPriority.BATCH,
            token_cost=1,
        )
        return _decision(request, disposition=AdmissionDisposition.DEFERRED)


class RustAdmissionHttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_signed_admit_complete_and_rate_limit_use_exact_private_wire(self):
        requests: list[dict[str, object]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = bytes(request.content)
            expected = "sha256=" + hmac.new(_SECRET, body, hashlib.sha256).hexdigest()
            self.assertEqual(str(request.url), "http://rust_core:8300/internal/provider-admission/v1")
            self.assertEqual(request.headers["content-type"], "application/json")
            self.assertEqual(request.headers["x-qdl-stable-signature"], expected)
            payload = json.loads(body)
            requests.append(payload)
            lane = payload["lane"]
            operation = payload["operation"]
            if operation == "COMPLETE":
                return httpx.Response(200, json={
                    "schema": ADMISSION_SCHEMA,
                    "authority": ADMISSION_AUTHORITY,
                    "operation": "COMPLETE",
                    "lane": lane,
                    "request_id": payload["request_id"],
                    "completed": True,
                })
            request_id = payload.get("request", {}).get("request_id") or payload.get("request_id")
            return httpx.Response(200, json={
                "schema": ADMISSION_SCHEMA,
                "authority": ADMISSION_AUTHORITY,
                "lane": lane,
                "request_id": request_id,
                "disposition": "GRANTED",
                "defer_reason": None,
                "retry_after_ms": None,
                "lease_expires_at_ns": 1_900_000_000_000_000_000,
                "coalesced": False,
            })

        client = httpx.AsyncClient(
            base_url="http://rust_core:8300",
            transport=httpx.MockTransport(handler),
        )
        admission = RustHttpProviderAdmission(
            base_url="http://rust_core:8300", secret=_SECRET, client=client
        )
        request = AdmissionRequest(
            lane=_LANE,
            request_id="basis:test-1",
            priority=AdmissionPriority.BATCH,
            token_cost=1,
        )
        self.assertEqual((await admission.admit(request)).disposition, AdmissionDisposition.GRANTED)
        self.assertTrue(await admission.complete(_LANE, "basis:test-1"))
        self.assertEqual(
            (await admission.record_rate_limit(
                _LANE, "basis:test-1", http_status=418, provider_code=None, retry_after_ms=60_000
            )).disposition,
            AdmissionDisposition.GRANTED,
        )
        self.assertEqual([item["operation"] for item in requests], ["ADMIT", "COMPLETE", "RATE_LIMIT"])
        await client.aclose()

    async def test_private_wire_failure_is_not_reinterpreted_as_a_grant(self):
        client = httpx.AsyncClient(
            base_url="http://rust_core:8300",
            transport=httpx.MockTransport(lambda _request: httpx.Response(503)),
        )
        admission = RustHttpProviderAdmission(
            base_url="http://rust_core:8300", secret=_SECRET, client=client
        )
        with self.assertRaises(AdmissionTransportError):
            await admission.admit(AdmissionRequest(
                lane=_LANE,
                request_id="basis:test-2",
                priority=AdmissionPriority.BATCH,
                token_cost=1,
            ))
        await client.aclose()


class RustAdmissionRuntimeComposeTests(unittest.TestCase):
    def test_private_listener_is_image_sealed_and_c2_only_enables_existing_core(self):
        policy = ROOT / "config/v2/provider-admission-policy-v1.json"
        digest = hashlib.sha256(policy.read_bytes()).hexdigest()
        stable = (ROOT / "docker-compose.v2-stable.yml").read_text(encoding="utf-8")
        c2 = (ROOT / "docker-compose.phase105c-c2.override.yml").read_text(encoding="utf-8")
        self.assertIn(
            "QDL_PROVIDER_ADMISSION_ENABLED: ${QDL_STABLE_PROVIDER_ADMISSION_ENABLED:-false}",
            stable,
        )
        self.assertIn(
            "QDL_PROVIDER_ADMISSION_POLICY_PATH: /opt/qdl/config/v2/provider-admission-policy-v1.json",
            stable,
        )
        self.assertIn(f"QDL_PROVIDER_ADMISSION_POLICY_SHA256: {digest}", stable)
        self.assertIn(
            "COPY config/v2/provider-admission-policy-v1.json /opt/qdl/config/v2/provider-admission-policy-v1.json",
            (ROOT / "Dockerfile.phase8-rust").read_text(encoding="utf-8"),
        )
        self.assertIn("  rust_core:\n    environment:\n      QDL_PROVIDER_ADMISSION_ENABLED: \"true\"", c2)
        self.assertNotIn("ports:", c2)


class GovernedNativeBasisTests(unittest.IsolatedAsyncioTestCase):
    async def test_granted_rust_lease_runs_one_fetch_and_completes_without_legacy_pacing(self):
        admission = _RecordingAdmission()
        calls = []
        sleeps = []

        async def sleep(seconds: float) -> None:
            sleeps.append(seconds)

        def basis(pair, *_args, **_kwargs):
            calls.append(pair)
            return {"data": [{
                "pair": pair,
                "contractType": "PERPETUAL",
                "basis": "10.0000",
                "timestamp": "1500",
            }]}

        result = await ReferenceBatch({("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(
            basis_fetcher=basis,
            max_attempts=1,
            sleep=sleep,
            native_basis_admission=admission,
        )}).fetch_one(_basis_request())

        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual(calls, ["BTCUSDT"])
        self.assertEqual(sleeps, [])
        self.assertEqual(len(admission.admits), 1)
        self.assertEqual(admission.admits[0].lane, _LANE)
        self.assertEqual(admission.completions, [(_LANE, admission.admits[0].request_id)])
        self.assertEqual(admission.rate_limits, [])

    async def test_deferred_rust_lease_avoids_vendor_call_and_preserves_retry_hint(self):
        admission = _RecordingAdmission(disposition=AdmissionDisposition.DEFERRED)
        calls = []

        def basis(*_args, **_kwargs):
            calls.append("called")
            raise AssertionError("deferred Rust admission must not call Binance")

        result = await ReferenceBatch({("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(
            basis_fetcher=basis,
            max_attempts=1,
            native_basis_admission=admission,
        )}).fetch_one(_basis_request())

        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "PROVIDER_RETRY_EXHAUSTED")
        self.assertEqual(result.retry_after_ms, 60_000)
        self.assertEqual(calls, [])
        self.assertEqual(admission.completions, [])

    async def test_documented_binance_rate_limit_is_reported_to_rust_without_local_retry(self):
        admission = _RecordingAdmission()
        calls = []

        def basis(*_args, **_kwargs):
            calls.append("called")
            raise BinanceProviderError(
                "rate limited",
                attempts=[{"status_code": 418, "provider_code": -1003}],
                retry_after_ms=45_000,
            )

        result = await ReferenceBatch({("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(
            basis_fetcher=basis,
            max_attempts=3,
            native_basis_admission=admission,
        )}).fetch_one(_basis_request())

        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "PROVIDER_RETRY_EXHAUSTED")
        self.assertEqual(result.retry_after_ms, 45_000)
        self.assertEqual(calls, ["called"])
        self.assertEqual(admission.completions, [])
        self.assertEqual(
            admission.rate_limits,
            [(_LANE, admission.admits[0].request_id, 418, -1003, 45_000)],
        )
