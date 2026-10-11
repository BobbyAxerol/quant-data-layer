"""D2: additive response compatibility does not weaken requests or domain checks."""
import copy
import unittest
from pydantic import ValidationError
from qdl_sdk.models import (Feed, SnapshotResponse, WarmupSpecification,
                            WarmupTimeRange, DecimalValue, ProblemDetails)
from qdl_sdk.reference import ReferenceRequirement, ReferenceBatchRequest, ReferenceBatchResponse
from tests.test_qdl_sdk_stream_projection import template, payload_fixture


def added_fields(value):
    if isinstance(value, dict):
        return {**{k: added_fields(v) for k, v in value.items()},
                "future_server_field": {"revision": 3}}
    if isinstance(value, list):
        return [added_fields(v) for v in value]
    return value


class AdditiveResponseTests(unittest.TestCase):
    def test_old_server_and_additive_server_all_feed_payloads(self):
        for feed in Feed:
            if feed is Feed.UNSPECIFIED:
                continue
            with self.subTest(feed=feed):
                try:
                    payload_fixture(feed)
                except KeyError:
                    continue
                original = {"request_id": "d2", "data": template(feed).model_dump(mode="json")}
                old = SnapshotResponse.model_validate(original)
                new = SnapshotResponse.model_validate(added_fields(original))
                self.assertEqual(new.model_dump(), old.model_dump())
                self.assertEqual(SnapshotResponse.model_validate_json(new.model_dump_json()), old)

    def test_problem_and_nested_reference_response_ignore_additions(self):
        problem = dict(type="test", title="stale", status=409, code="DATA_STALE",
                       detail="stale", request_id="d2", retryable=False)
        self.assertEqual(ProblemDetails.model_validate(added_fields(problem)),
                         ProblemDetails.model_validate(problem))
        response = dict(schema="qdl.reference.batch.v2", request_id="d2", partial=True,
                        success_count=0, error_count=1,
                        results=[dict(instrument_uid="test", product="MARK_INDEX_PRICE",
                                      status="ERROR", problem=problem)])
        self.assertEqual(ReferenceBatchResponse.model_validate(added_fields(response)),
                         ReferenceBatchResponse.model_validate(response))

    def test_known_response_fields_still_reject_invalid_domain_values(self):
        original = added_fields({"request_id": "d2", "data": template(Feed.QUOTE).model_dump(mode="json")})
        for key, value in (("feed", "UNKNOWN"), ("quality", {"state": "BOGUS"}),
                           ("payload", {"feed": "QUOTE"})):
            with self.subTest(key=key):
                malformed = copy.deepcopy(original)
                malformed["data"][key] = value
                with self.assertRaises(ValidationError):
                    SnapshotResponse.model_validate(malformed)
        with self.assertRaises(ValidationError):
            DecimalValue(coefficient="100", scale=2, source_text="2", new_field=True)

    def test_requests_remain_closed(self):
        for model, payload in (
            (WarmupTimeRange, dict(start_time_ns=1, end_time_ns=2)),
            (WarmupSpecification, dict(rows=700)),
            (ReferenceRequirement, dict(instrument_uid="test", product="MARK_INDEX_PRICE")),
            (ReferenceBatchRequest, dict(requirements=[])),
        ):
            with self.subTest(model=model.__name__):
                with self.assertRaises(ValidationError) as caught:
                    model.model_validate({**payload, "future_request_field": True})
                self.assertIn("extra_forbidden", [e["type"] for e in caught.exception.errors()])
