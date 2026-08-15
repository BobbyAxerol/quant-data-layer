from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import jwt
from fastapi.testclient import TestClient

from qdl.observability import (
    AlertDecision,
    BoundedTelemetry,
    CorrelationContext,
    SLOEvidence,
    SLOPolicy,
    evaluate_slo,
)
from qdl.runtime.app_factory import create_role_app
from qdl.runtime.roles import RuntimeRole
from qdl.security import (
    AuditChain,
    EgressPolicy,
    PayloadPolicy,
    Permission,
    RbacAuthorizer,
    RegisteredTarget,
    ServiceTokenVerifier,
    redact_secrets,
)


class ObservabilityTests(unittest.TestCase):
    def test_context_uses_bounded_instrument_bucket_and_never_event_label(self):
        context = CorrelationContext(
            environment="paper",
            service_name="qdl-ingestor",
            service_version="2.0.0",
            instance_id="ingestor-1",
            runtime_role="ingestion",
            venue="BINANCE",
            market="USDM",
            instrument_id="BINANCE.USDM.PERPETUAL.BTC-USDT",
            event_id="event-never-a-label",
        )
        labels = context.metric_labels()
        self.assertIn("instrument_bucket", labels)
        self.assertNotIn("instrument_id", labels)
        self.assertNotIn("event_id", labels)
        self.assertEqual(context.fields()["event_id"], "event-never-a-label")

    def test_telemetry_bounds_series_and_histogram_memory(self):
        telemetry = BoundedTelemetry(max_series=2, max_points_per_histogram=3)
        self.assertTrue(telemetry.add("qdl_canonical_event_total", labels={"venue": "BINANCE"}))
        self.assertTrue(telemetry.observe("qdl_canonicalize_latency_seconds", 0.1))
        self.assertFalse(telemetry.set("qdl_ingest_queue_depth", 1, labels={"venue": "OKX"}))
        for value in (0.1, 0.2, 0.3, 0.4):
            telemetry.observe("qdl_canonicalize_latency_seconds", value)
        snapshot = telemetry.snapshot()
        self.assertEqual(snapshot.rejected_series, 1)
        self.assertEqual(len(snapshot.histograms["qdl_canonicalize_latency_seconds"]), 3)
        with self.assertRaisesRegex(ValueError, "unsupported metric labels"):
            telemetry.add("qdl_canonical_event_total", labels={"event_id": "unbounded"})

    def test_slo_drop_or_completeness_violation_is_sev1(self):
        policy = SLOPolicy(0.999, 1.0, 250.0)
        healthy = evaluate_slo(policy, SLOEvidence(1.0, 1.0, 20.0, 0, 1000))
        self.assertEqual(healthy.decision, AlertDecision.PASS)
        failed = evaluate_slo(policy, SLOEvidence(1.0, 0.999, 20.0, 1, 1000))
        self.assertEqual(failed.decision, AlertDecision.SEV1)
        self.assertEqual(failed.violations, ("COMPLETENESS", "CANONICAL_DROP"))


class SecurityPolicyTests(unittest.TestCase):
    def setUp(self):
        self.now = int(time.time())
        self.secret = "phase6-test-secret-with-sufficient-length"
        self.verifier = ServiceTokenVerifier(
            issuer="https://identity.qdl.test",
            audience="qdl-control",
            keys_by_id={"test-key": self.secret},
            algorithms=("HS256",),
        )

    def token(self, **overrides):
        claims = {
            "sub": "service:market-ops",
            "iss": "https://identity.qdl.test",
            "aud": "qdl-control",
            "iat": self.now,
            "exp": self.now + 300,
            "jti": "phase6-token-1",
            "environment": "paper",
            "roles": ["venue_operator"],
            "venues": ["BINANCE"],
        }
        claims.update(overrides)
        return jwt.encode(claims, self.secret, algorithm="HS256", headers={"kid": "test-key"})

    def test_identity_rbac_environment_and_venue_scope(self):
        principal = self.verifier.verify(self.token(), expected_environment="paper")
        RbacAuthorizer().require(
            principal, Permission.VENUE_OPERATE, environment="paper", venue="BINANCE"
        )
        with self.assertRaisesRegex(PermissionError, "venue scope"):
            RbacAuthorizer().require(
                principal, Permission.VENUE_OPERATE, environment="paper", venue="OKX"
            )
        with self.assertRaisesRegex(PermissionError, "environment"):
            self.verifier.verify(self.token(), expected_environment="production")

    def test_egress_is_registered_and_blocks_ssrf_targets(self):
        policy = EgressPolicy((RegisteredTarget(
            source_id="binance.usdm.public",
            schemes=frozenset({"https", "wss"}),
            hosts=frozenset({"fapi.binance.com", "fstream.binance.com"}),
            ports=frozenset({443}),
            path_prefixes=("/fapi/", "/ws/"),
        ),))
        self.assertEqual(
            policy.validate("binance.usdm.public", "https://fapi.binance.com/fapi/v1/time"),
            "https://fapi.binance.com/fapi/v1/time",
        )
        for url in (
            "http://127.0.0.1/fapi/v1/time",
            "https://169.254.169.254/fapi/metadata",
            "https://evil.example/fapi/v1/time",
            "https://fapi.binance.com/admin",
        ):
            with self.subTest(url=url), self.assertRaises(PermissionError):
                policy.validate("binance.usdm.public", url)

    def test_payload_limits_and_secret_redaction(self):
        policy = PayloadPolicy(max_bytes=128, max_nesting_depth=2, max_decompression_ratio=5)
        self.assertEqual(policy.validate_json(b'{"price":"1.25"}')["price"], "1.25")
        with self.assertRaisesRegex(ValueError, "nesting"):
            policy.validate_json(b'{"a":{"b":{"c":1}}}')
        with self.assertRaisesRegex(ValueError, "decompression"):
            policy.validate_json(b'{"x":"1234567890"}', compressed_bytes=1)
        self.assertEqual(
            redact_secrets({"Authorization": "Bearer secret", "nested": {"api_key": "x"}}),
            {"Authorization": "[REDACTED]", "nested": {"api_key": "[REDACTED]"}},
        )


class AuditAndControlPlaneTests(unittest.TestCase):
    def test_audit_chain_redacts_and_detects_tamper(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.jsonl"
            chain = AuditChain(path)
            chain.append(
                actor="operator",
                action="feed.authority.update",
                environment="paper",
                request_id="request-1",
                result="200",
                details={"api_key": "must-not-leak", "revision": 2},
            )
            sequence, digest = AuditChain.verify(path)
            self.assertEqual(sequence, 1)
            self.assertEqual(len(digest), 64)
            self.assertNotIn("must-not-leak", path.read_text())
            payload = json.loads(path.read_text())
            payload["result"] = "500"
            path.write_text(json.dumps(payload) + "\n")
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                AuditChain.verify(path)

    def test_control_role_fails_closed_and_accepts_scoped_token(self):
        with self.assertRaisesRegex(RuntimeError, "identity configuration"):
            with mock.patch.dict(os.environ, {"QDL_RUNTIME_ROLE": "control"}, clear=True):
                create_role_app(RuntimeRole.CONTROL)

        now = int(time.time())
        secret = "phase6-control-secret-with-sufficient-length"
        token = jwt.encode(
            {
                "sub": "service:ops",
                "iss": "https://identity.qdl.test",
                "aud": "qdl-control",
                "iat": now,
                "exp": now + 300,
                "jti": "control-1",
                "environment": "paper",
                "roles": ["venue_operator"],
                "venues": ["BINANCE"],
            },
            secret,
            algorithm="HS256",
            headers={"kid": "test"},
        )
        with tempfile.TemporaryDirectory() as directory:
            environment = {
                "QDL_RUNTIME_ROLE": "control",
                "QDL_OWNS_LIVE_INGESTION": "false",
                "QDL_ENVIRONMENT": "paper",
                "QDL_CONTROL_JWT_ISSUER": "https://identity.qdl.test",
                "QDL_CONTROL_JWT_AUDIENCE": "qdl-control",
                "QDL_CONTROL_JWT_KEYS_JSON": json.dumps({"test": secret}),
                "QDL_CONTROL_JWT_ALGORITHMS": "HS256",
                "QDL_CONTROL_AUDIT_PATH": str(Path(directory) / "control.jsonl"),
            }
            with mock.patch.dict(os.environ, environment, clear=True):
                client = TestClient(create_role_app(RuntimeRole.CONTROL))
                self.assertEqual(client.get("/v1/control/runtime-roles").status_code, 401)
                response = client.get(
                    "/v1/control/runtime-roles",
                    headers={"Authorization": f"Bearer {token}"},
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["current_container_role"], "control")


if __name__ == "__main__":
    unittest.main()
