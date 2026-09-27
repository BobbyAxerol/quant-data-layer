import json
import unittest

from pydantic import BaseModel, ConfigDict

from qdl_sdk.models import BatchResponse, ProblemDetails, ProblemDiagnostics


class LegacyProblem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str
    title: str
    status: int
    code: str
    detail: str
    request_id: str
    retryable: bool
    retry_after_ms: int | None = None
    instrument_uid: str | None = None
    quality_state: str | None = None


class ProblemCompatibilityTests(unittest.TestCase):
    def problem(self, **extra):
        return ProblemDetails(type="urn:qdl:DATA_STALE", title="DATA_STALE",
            status=409, code="DATA_STALE", detail="component age exceeded",
            request_id="test-only", retryable=True, **extra)

    def test_original_fields_and_null_values_remain_parseable(self):
        data = self.problem().model_dump(mode="json")
        self.assertNotIn("diagnostics", data)
        self.assertIn("retry_after_ms", data)
        self.assertIsNone(data["retry_after_ms"])
        self.assertEqual(LegacyProblem.model_validate(data).code, "DATA_STALE")

    def test_nested_batch_preserves_typed_refusal_for_old_sdk(self):
        batch = BatchResponse(request_id="test-only", partial=True,
            success_count=0, error_count=1, results=[dict(instrument_uid="test",
            status="ERROR", problem=self.problem())])
        wire = json.loads(batch.model_dump_json(by_alias=True))
        self.assertIsNone(wire["results"][0]["data"])
        LegacyProblem.model_validate(wire["results"][0]["problem"])
        self.assertEqual(BatchResponse.model_validate(wire), batch)

    def test_real_diagnostics_are_not_discarded(self):
        diagnostic = ProblemDiagnostics(evaluated_at_ns=123, state="LIVE",
            freshness_ms=4000, event_recency_state="STALE",
            provider_session_state="LIVE", execution_eligible=False,
            gap_open=False, complete=True)
        problem = self.problem(diagnostics=diagnostic)
        wire = json.loads(problem.model_dump_json())
        self.assertEqual(wire["diagnostics"]["freshness_ms"], 4000)
        self.assertEqual(ProblemDetails.model_validate(wire), problem)

    def test_exclude_and_alias_options_are_preserved(self):
        self.assertNotIn("retry_after_ms", self.problem().model_dump(exclude_none=True))
        self.assertNotIn("detail", self.problem().model_dump(exclude={"detail"}))
