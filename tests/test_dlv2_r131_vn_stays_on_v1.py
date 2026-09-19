"""R1.31: the VN plane is declared, routed to V1, and not deployed - on purpose.

The four DNSE bindings are the one place where the catalog and the live spool do
not reconcile: `dnse-fpt-bar-stable-001`, `dnse-fpt-trade-stable-001`,
`dnse-vn30f1m-bar-stable-001` and `dnse-vn30f1m-trade-stable-001` are declared
and have no partition. That is not a fault and these tests say why, so the next
reconciliation does not have to rediscover it:

* the routing plane sends every `vn_primary_v2` requirement to V1,
* the bar edge's recurring poll admits `BINANCE` and `OKX` and no other runtime, and
* `vn_edge_v2` sits behind a Compose profile the stable stack does not activate.

The owner's decision on 2026-09-19 was to keep it that way for this release - VN
is served by V1 and V2 does not claim it. What these tests protect is that the
decision cannot drift into a V2 route without someone editing a test that says
out loud what the drift would mean.
"""

from __future__ import annotations

import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ROUTING = ROOT / "config/v2/stable-v2-release-routing.yaml"
COMPOSE = ROOT / "docker-compose.v2-stable.yml"

VN_POLICY = "vn_primary_v2"
VN_PROVIDER = "DNSE_DIRECT"
# The profile that must keep `vn_edge_v2` out of the stable stack. A role the
# stack does not start cannot fill a partition, which is the whole reason the
# four bindings reconcile to nothing.
VN_PROFILE = "stable-vn"


def _routes(document: object) -> list[dict]:
    found: list[dict] = []

    def walk(node: object) -> None:
        if isinstance(node, dict):
            if "requirement_key" in node:
                found.append(node)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(document)
    return found


class VnStaysOnV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = yaml.safe_load(CATALOG.read_text())
        cls.routing = yaml.safe_load(ROUTING.read_text())
        cls.compose = yaml.safe_load(COMPOSE.read_text())
        cls.routes = _routes(cls.routing)

    def _vn_bindings(self) -> list[dict]:
        return [
            binding
            for binding in self.catalog["bindings"]
            if binding["source"]["provider"] == VN_PROVIDER
        ]

    def test_the_catalog_still_declares_the_four_vn_bindings(self) -> None:
        """De-scoping from V2 is a routing decision, not a reason to delete lineage."""
        bindings = self._vn_bindings()
        self.assertEqual(len(bindings), 4)
        self.assertEqual(
            {binding["source"]["source_id"] for binding in bindings},
            {
                "dnse-fpt-bar-stable-001",
                "dnse-fpt-trade-stable-001",
                "dnse-vn30f1m-bar-stable-001",
                "dnse-vn30f1m-trade-stable-001",
            },
        )

    def test_every_vn_requirement_routes_to_v1(self) -> None:
        vn = [route for route in self.routes if VN_POLICY in route["requirement_key"]]
        self.assertTrue(vn, "the routing plane must say something about VN")
        for route in vn:
            with self.subTest(requirement=route["requirement_key"]):
                self.assertEqual(route["route"], "V1_PRIMARY")

    def test_no_vn_requirement_is_routed_to_v2(self) -> None:
        """The assertion the owner's decision actually rests on."""
        offenders = [
            route["requirement_key"]
            for route in self.routes
            if VN_POLICY in route["requirement_key"] and "V2" in route["route"]
        ]
        self.assertEqual(offenders, [])

    def test_a_vn_requirement_has_no_v2_fallback_to_fall_back_to(self) -> None:
        """`fallback: NONE` is the honest value when V2 never serves the feed."""
        for route in self.routes:
            if VN_POLICY in route["requirement_key"]:
                with self.subTest(requirement=route["requirement_key"]):
                    self.assertEqual(route.get("fallback"), "NONE")

    def test_the_reason_names_the_gate_rather_than_the_symptom(self) -> None:
        for route in self.routes:
            if VN_POLICY in route["requirement_key"]:
                with self.subTest(requirement=route["requirement_key"]):
                    self.assertEqual(route.get("reason"), "VN_REAL_PROVIDER_GATE_UNEXERCISED")

    def test_the_acquisition_plane_puts_vn_on_its_own_runtime(self) -> None:
        plan = yaml.safe_load((ROOT / "config/v2/stable-acquisition-bindings.yaml").read_text())
        vn = [row for row in plan["bindings"] if row["binding_id"].startswith("dnse-")]
        self.assertEqual(len(vn), 4)
        for row in vn:
            with self.subTest(binding=row["binding_id"]):
                self.assertEqual(row["runtime"], "DNSE")
                self.assertEqual(row["mode"], "PYTHON_VENDOR_SDK")

    def test_the_bar_edge_refuses_the_vn_runtime_and_admits_the_two_crypto_ones(self) -> None:
        """The predicate the edge actually calls, against the real plan's values.

        Behaviour, not source text: every runtime named in the committed
        acquisition plan is fed to the real predicate, and DNSE is the one it
        must reject. If a third runtime is ever admitted, the VN bindings start
        being polled by a role holding no DNSE credentials, and the failure
        surfaces as a provider outage rather than a config change.
        """
        from qdl.runtime.stable_bar_edge import BAR_EDGE_RUNTIMES, admits_runtime

        plan = yaml.safe_load((ROOT / "config/v2/stable-acquisition-bindings.yaml").read_text())
        runtimes = {row["runtime"] for row in plan["bindings"]}
        self.assertIn("DNSE", runtimes, "the plan must still declare the VN runtime")

        self.assertFalse(admits_runtime("DNSE"))
        self.assertTrue(admits_runtime("BINANCE"))
        self.assertTrue(admits_runtime("OKX"))
        self.assertEqual(BAR_EDGE_RUNTIMES, frozenset({"BINANCE", "OKX"}))
        self.assertEqual(
            {runtime for runtime in runtimes if admits_runtime(runtime)},
            runtimes - {"DNSE"},
        )

    def test_the_vn_edge_is_behind_a_profile_the_stable_stack_does_not_activate(self) -> None:
        service = self.compose["services"]["vn_edge_v2"]
        self.assertIn(VN_PROFILE, service.get("profiles", []))

    def test_no_other_stable_role_shares_the_vn_profile(self) -> None:
        """Activating `stable-vn` must bring up the VN edge and nothing else."""
        sharing = [
            name
            for name, service in self.compose["services"].items()
            if VN_PROFILE in (service or {}).get("profiles", []) and name != "vn_edge_v2"
        ]
        self.assertEqual(sharing, [])

    def test_the_seventeen_stable_roles_carry_no_profile_so_they_start_by_default(self) -> None:
        """If a serving role acquired a profile, the stack would come up short."""
        profiled = {
            name
            for name, service in self.compose["services"].items()
            if (service or {}).get("profiles")
        }
        self.assertNotIn("stable_projector", profiled)
        self.assertNotIn("rust_core", profiled)
        self.assertNotIn("ingestor_binance_usdm", profiled)
        self.assertNotIn("ingestor_okx_swap", profiled)


if __name__ == "__main__":
    unittest.main()
