from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import replace
import io
import json
import tempfile
import unittest
from pathlib import Path

from qdl.certification.phase105_consumer_acceptance import PHASE105_PAPER_CONSUMER_IDS
from qdl.certification.phase105_release import (
    certify_stable_release,
    parse_release_observations,
)
from qdl.consumer import RealtimeRoute, ReleaseRouteObservation, StableReleaseRoutePlan
from scripts.phase105_certify_stable_release import main


ROOT = Path(__file__).resolve().parents[1]
ROUTE_PATH = ROOT / "config/v2/stable-v2-release-routing.yaml"


class Phase105StableReleaseCertificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = StableReleaseRoutePlan.load(ROUTE_PATH, manifest_root=ROOT)

    def observations(self) -> tuple[ReleaseRouteObservation, ...]:
        values = []
        for consumer_id, product in self.plan.products():
            route = (
                RealtimeRoute.V1_PRIMARY.value
                if product.route == "V1_PRIMARY"
                else RealtimeRoute.V2_PRIMARY.value
            )
            values.append(ReleaseRouteObservation(
                consumer_id=consumer_id,
                requirement_key=product.requirement_key,
                route=route,
                reason="V1_EXCLUDED" if route == "V1_PRIMARY" else "V2_READY",
                v2_source_age_ms=None if route == "V1_PRIMARY" else 10,
                v2_receive_age_ms=None if route == "V1_PRIMARY" else 12,
                v2_gap_open=False,
                v1_source_age_ms=20 if route == "V1_PRIMARY" else None,
                v1_receive_age_ms=22 if route == "V1_PRIMARY" else None,
                consumer_lag=3,
                cpu_millicores=250,
                rss_bytes=64 * 1024 * 1024,
            ))
        return tuple(values)

    def v1_provenance(self) -> dict[str, object]:
        return {
            "schema": "qdl.phase105.v1-fallback-provenance.v1",
            "status": "PASS",
            "image_id": "sha256:" + "1" * 64,
            "source_commit": self.plan.v1_fallback.source_commit,
            "source_tree": "a" * 40,
            "dockerfile_sha256": "b" * 64,
            "version": self.plan.v1_fallback.release_tag,
        }

    def runtime_handoff(self) -> dict[str, object]:
        return {
            "schema": "qdl.phase105c.runtime-handoff-evidence.v1",
            "status": "PASS",
            "release_route_plan_sha256": self.plan.digest,
            "v2_python_image": "sha256:" + "2" * 64,
            "v2_rust_image": "sha256:" + "3" * 64,
            "authority_mode": "RUST_PRIMARY",
            "demanded_slices_status": "PASS",
            "order_actions": 0,
            "test_provenance": False,
        }

    def consumer_acceptance(self) -> dict[str, object]:
        products: list[dict[str, object]] = []
        for consumer_id, product in self.plan.products():
            if (
                consumer_id not in PHASE105_PAPER_CONSUMER_IDS
                or product.route != "V2_PRIMARY"
            ):
                continue
            instrument_uid, feed, interval, policy = product.requirement_key.split(":", 3)
            products.append({
                "consumer_id": consumer_id,
                "instrument_uid": instrument_uid,
                "feed": feed,
                "interval": interval or None,
                "source_policy_id": policy,
                "primary_content_sha256": "c" * 64,
            })
        return {
            "schema": "qdl.phase105.v2-identity-acceptance.v1",
            "status": "PASS_V2_DATA_PLANE_ONLY",
            "release_route_plan_sha256": self.plan.digest,
            "authority_revision": 1,
            "scope_sha256": "d" * 64,
            "product_count": len(products),
            "durable_product_count": len(products),
            "provider_connections": 0,
            "order_actions": 0,
            "cursor_directory_removed": True,
            "secret_values_recorded": False,
            "test_provenance": False,
            "products": products,
        }

    def fallback_drill(self) -> dict[str, object]:
        routes: list[dict[str, object]] = []
        for consumer_id, product in self.plan.products():
            if product.route == "V2_PRIMARY" and product.fallback == "V1":
                routes.append({
                    "consumer_id": consumer_id,
                    "requirement_key": product.requirement_key,
                    "before_route": "V2_PRIMARY",
                    "fallback_route": "V1_FALLBACK",
                    "returned_route": "V2_PRIMARY",
                })
        return {
            "schema": "qdl.phase105.v1-fallback-return.v1",
            "status": "PASS",
            "release_route_plan_sha256": self.plan.digest,
            "routes": routes,
            "provider_connections": 0,
            "order_actions": 0,
            "cursor_directory_removed": True,
            "secret_values_recorded": False,
            "test_provenance": False,
        }

    def certify(self, **overrides: object) -> dict[str, object]:
        values: dict[str, object] = {
            "observations": self.observations(),
            "v1_provenance": self.v1_provenance(),
            "runtime_handoff": self.runtime_handoff(),
            "consumer_acceptance": self.consumer_acceptance(),
            "fallback_drill": self.fallback_drill(),
        }
        values.update(overrides)
        return certify_stable_release(self.plan, **values)  # type: ignore[arg-type]

    def gate_status(self, result: dict[str, object], gate_id: str) -> str:
        gates = result["gates"]
        self.assertIsInstance(gates, list)
        return next(item["status"] for item in gates if item["gate_id"] == gate_id)

    def test_complete_exact_evidence_is_eligible_and_secret_free(self) -> None:
        result = self.certify()
        self.assertEqual(result["status"], "PASS")
        self.assertTrue(all(item["status"] == "PASS" for item in result["gates"]))
        self.assertEqual(result["release_route_plan_sha256"], self.plan.digest)
        encoded = json.dumps(result, sort_keys=True)
        self.assertNotIn("primary_content", encoded)
        self.assertNotIn("acknowledged_offset", encoded)
        self.assertNotIn("PRIVATE KEY", encoded)

    def test_every_required_evidence_gate_fails_closed(self) -> None:
        observations = list(self.observations())
        first_v2 = next(index for index, item in enumerate(observations) if item.route == "V2_PRIMARY")
        observations[first_v2] = replace(observations[first_v2], v2_gap_open=True)
        result = self.certify(observations=tuple(observations))
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(self.gate_status(result, "release_route_readiness"), "BLOCKED")

        provenance = self.v1_provenance()
        provenance["source_commit"] = "0" * 40
        result = self.certify(v1_provenance=provenance)
        self.assertEqual(self.gate_status(result, "v1_fallback_provenance"), "BLOCKED")

        handoff = self.runtime_handoff()
        handoff["release_route_plan_sha256"] = "0" * 64
        result = self.certify(runtime_handoff=handoff)
        self.assertEqual(self.gate_status(result, "runtime_handoff"), "BLOCKED")

        acceptance = self.consumer_acceptance()
        acceptance["products"] = acceptance["products"][:-1]
        result = self.certify(consumer_acceptance=acceptance)
        self.assertEqual(self.gate_status(result, "consumer_v2_primary"), "BLOCKED")

        fallback = self.fallback_drill()
        fallback["routes"][0]["returned_route"] = "V1_PRIMARY"
        result = self.certify(fallback_drill=fallback)
        self.assertEqual(self.gate_status(result, "v1_fallback_return"), "BLOCKED")

    def test_observation_parser_rejects_hidden_or_invalid_public_data(self) -> None:
        raw = [item.public_record() for item in self.observations()]
        raw[0]["hidden"] = "ignored"
        with self.assertRaisesRegex(ValueError, "fields differ"):
            parse_release_observations(raw)

        raw = [item.public_record() for item in self.observations()]
        raw[0]["v2_gap_open"] = "false"
        with self.assertRaisesRegex(ValueError, "must be boolean"):
            parse_release_observations(raw)

    def test_secret_like_evidence_is_rejected_before_it_can_be_hashed(self) -> None:
        runtime = self.runtime_handoff()
        runtime["private_key"] = "not-permitted"
        with self.assertRaisesRegex(ValueError, "secret-like"):
            self.certify(runtime_handoff=runtime)

    def test_missing_or_duplicate_observation_is_rejected_deterministically(self) -> None:
        observations = self.observations()
        with self.assertRaisesRegex(ValueError, "differ from frozen manifest"):
            self.certify(observations=observations[:-1])
        with self.assertRaisesRegex(ValueError, "differ from frozen manifest"):
            self.certify(observations=observations + (observations[0],))

    def test_review_only_cli_exits_nonzero_for_blocked_certificate(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            paths = {
                "observations": root / "observations.json",
                "v1": root / "v1.json",
                "runtime": root / "runtime.json",
                "acceptance": root / "acceptance.json",
                "fallback": root / "fallback.json",
            }
            paths["observations"].write_text(json.dumps([
                item.public_record() for item in self.observations()
            ]), encoding="utf-8")
            paths["v1"].write_text(json.dumps(self.v1_provenance()), encoding="utf-8")
            paths["runtime"].write_text(json.dumps(self.runtime_handoff()), encoding="utf-8")
            paths["acceptance"].write_text(json.dumps(self.consumer_acceptance()), encoding="utf-8")
            paths["fallback"].write_text(json.dumps(self.fallback_drill()), encoding="utf-8")
            args = [
                "--observations", str(paths["observations"]),
                "--v1-provenance", str(paths["v1"]),
                "--runtime-handoff", str(paths["runtime"]),
                "--consumer-acceptance", str(paths["acceptance"]),
                "--fallback-drill", str(paths["fallback"]),
            ]
            with redirect_stdout(io.StringIO()) as output:
                self.assertEqual(main(args), 0)
            self.assertIn('"status":"PASS"', output.getvalue())

            fallback = self.fallback_drill()
            fallback["routes"] = []
            paths["fallback"].write_text(json.dumps(fallback), encoding="utf-8")
            with redirect_stdout(io.StringIO()):
                self.assertEqual(main(args), 2)

            paths["observations"].write_text(json.dumps([]), encoding="utf-8")
            with redirect_stdout(io.StringIO()) as output:
                self.assertEqual(main(args), 2)
            self.assertEqual(json.loads(output.getvalue())["reason"], "INVALID_EVIDENCE")


if __name__ == "__main__":
    unittest.main()
