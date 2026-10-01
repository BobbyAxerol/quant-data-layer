"""Pure execution proof contract shared by Query and consumer-side validation."""

import subprocess
import sys
import unittest

from qdl.data_quality.execution_mark_index import validate_quiet_execution_mark_index_evidence as query_validate
from qdl_sdk.execution_quality import ExecutionEvidenceError, validate_quiet_execution_mark_index_evidence

NOW = 1_800_000_000_000_000_000


def labels():
    return {
        "event_recency_policy": "OBSERVE",
        "recency_mode": "COMPONENT_SESSION_LIVE",
        "provider_session_state": "LIVE",
        "provider_session_liveness_ms": "100",
        "provider_session_checked_at_ns": str(NOW),
        "component_mark_received_at_ns": str(NOW - 1_000_000),
        "component_index_received_at_ns": str(NOW - 2_000_000),
        "component_mark_quiet_after_ms": "15000",
        "component_index_quiet_after_ms": "70000",
    }


class ExecutionProofTests(unittest.TestCase):
    def test_query_and_sdk_use_same_validator(self):
        self.assertIs(query_validate, validate_quiet_execution_mark_index_evidence)

    def test_valid_proof_does_not_mutate_input(self):
        original = labels()
        copy = dict(original)
        proof = query_validate(original, at_ns=NOW, max_session_liveness_ms=2000)
        self.assertEqual(proof.component_mark_age_ms, 1)
        self.assertEqual(proof.component_index_age_ms, 2)
        self.assertEqual(original, copy)

    def test_session_expiry_is_typed_and_boundary_is_inclusive(self):
        query_validate(labels(), at_ns=NOW + 1900_000_000, max_session_liveness_ms=2000)
        with self.assertRaises(ExecutionEvidenceError) as error:
            query_validate(labels(), at_ns=NOW + 1901_000_000, max_session_liveness_ms=2000)
        self.assertEqual(error.exception.reason, "SESSION_EXPIRED")
        self.assertIsInstance(error.exception, ValueError)

    def test_component_expiry_cannot_be_healed_by_fresh_session(self):
        value = labels()
        value["component_mark_received_at_ns"] = str(NOW - 15001_000_000)
        with self.assertRaises(ExecutionEvidenceError) as error:
            query_validate(value, at_ns=NOW, max_session_liveness_ms=2000)
        self.assertEqual(error.exception.reason, "COMPONENT_EXPIRED")

    def test_invalid_evidence_is_not_expiry(self):
        cases = [
            ("provider_session_state", "DISCONNECTED", "SESSION_NOT_LIVE"),
            ("provider_session_checked_at_ns", str(NOW + 1), "SESSION_CLOCK_INVALID"),
            ("component_index_received_at_ns", "0", "COMPONENT_EVIDENCE_INVALID"),
            ("component_index_quiet_after_ms", "bad", "EVIDENCE_MALFORMED"),
        ]
        for key, value, reason in cases:
            with self.subTest(key=key):
                data = labels()
                data[key] = value
                with self.assertRaises(ExecutionEvidenceError) as error:
                    query_validate(data, at_ns=NOW, max_session_liveness_ms=2000)
                self.assertEqual(error.exception.reason, reason)

    def test_missing_policy_cannot_use_quiet_proof(self):
        with self.assertRaises(ExecutionEvidenceError) as error:
            query_validate(labels(), at_ns=NOW, max_session_liveness_ms=None)
        self.assertEqual(error.exception.reason, "CONTRACT_INCOMPLETE")


class ColdImportTests(unittest.TestCase):
    def test_public_contracts_and_service_exports_in_each_cold_order(self):
        for first in (
            "qdl.data_quality.execution_mark_index",
            "qdl_sdk.execution_quality",
            "qdl.query.service",
            "qdl_sdk",
        ):
            with self.subTest(first=first):
                code = f"import {first}\n" + (
                    "import qdl.query as query\n"
                    "import qdl.query.service as service\n"
                    "from qdl.data_quality.execution_mark_index import validate_quiet_execution_mark_index_evidence as a\n"
                    "from qdl_sdk.execution_quality import validate_quiet_execution_mark_index_evidence as b\n"
                    "assert a is b\n"
                    "for name in query.__all__: assert getattr(query, name) is not None\n"
                    "for name in query._SERVICE_EXPORTS: assert getattr(query, name) is getattr(service, name)\n"
                    "assert query._SERVICE_EXPORTS <= set(dir(query))\n"
                    "assert not hasattr(query, 'unknown_execution_contract')\n"
                    "from qdl.query import *\n"
                    "assert V2QueryService is service.V2QueryService\n"
                )
                result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
