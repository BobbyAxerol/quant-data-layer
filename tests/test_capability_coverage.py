from __future__ import annotations

import unittest
from pathlib import Path

import yaml

from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
CAPABILITIES_PATH = ROOT / "config/v2/stable-capabilities.yaml"
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
COMPOSE_PATH = ROOT / "docker-compose.v2-stable.yml"

# A capability may only claim this once a bounded run against the real provider
# has been recorded. Anything else must be gated as not stable.
CERTIFIED = "BOUNDED_REAL_PROVIDER_SHADOW"
UNEXERCISED = "UNEXERCISED"
# The manifest names BBO; the catalog and requirements call the same feed QUOTE.
FEED_ALIASES = {"BBO": "QUOTE"}


class _Loader(yaml.SafeLoader):
    pass


_Loader.add_multi_constructor(
    "!",
    lambda loader, suffix, node: (
        loader.construct_sequence(node, deep=True)
        if isinstance(node, yaml.SequenceNode)
        else loader.construct_mapping(node, deep=True)
        if isinstance(node, yaml.MappingNode)
        else loader.construct_scalar(node)
    ),
)


def _capabilities() -> dict:
    return yaml.safe_load(CAPABILITIES_PATH.read_text(encoding="utf-8"))


class CapabilityCoverageTests(unittest.TestCase):
    """Advertising a capability is a claim; this test makes it a checkable one.

    VN was advertised as PROVIDER_DERIVED_DURABLE_REPLAY while its role sat
    behind a Compose profile that had never been started and no VN partition
    had ever appeared in the spool. Nothing failed, because nothing checked.
    """

    def setUp(self) -> None:
        self.manifest = _capabilities()
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.gates = self.manifest.get("capability_gates", {})

    def _catalog_feeds(self, venue: str, market: str, product: str) -> set[str]:
        return {
            binding.feed.value
            for binding in self.catalog.bindings
            if binding.instrument.identity.venue == venue
            and binding.instrument.identity.market == market
            and binding.instrument.identity.product_type.value == product
        }

    def test_every_advertised_feed_has_a_catalog_binding(self):
        for entry in self.manifest["capabilities"]:
            available = self._catalog_feeds(
                entry["venue"], entry["market"], entry["product_type"]
            )
            for feed in entry["feeds"]:
                resolved = FEED_ALIASES.get(feed, feed)
                with self.subTest(venue=entry["venue"], feed=feed):
                    self.assertIn(
                        resolved, available,
                        "an advertised feed must have a binding in the catalog",
                    )

    def test_certification_values_are_from_the_known_set(self):
        for entry in self.manifest["capabilities"]:
            with self.subTest(venue=entry["venue"], market=entry["market"]):
                self.assertIn(entry["certification"], {CERTIFIED, UNEXERCISED})

    def test_an_unexercised_capability_must_be_gated(self):
        gated_venues = {
            key.split("_")[0] for key, value in self.gates.items()
            if value.get("stable") is False
        }
        for entry in self.manifest["capabilities"]:
            if entry["certification"] != UNEXERCISED:
                continue
            with self.subTest(venue=entry["venue"], market=entry["market"]):
                self.assertIn(
                    entry["venue"], {"HNX", "HOSE"} | gated_venues,
                    "an unexercised capability must carry a gate saying so",
                )
                for feed in entry["feeds"]:
                    gate = f"VN_{FEED_ALIASES.get(feed, feed)}"
                    self.assertIn(
                        gate, self.gates,
                        f"unexercised feed {feed} needs an explicit gate",
                    )
                    self.assertFalse(self.gates[gate]["stable"])

    def test_every_gate_states_a_reason(self):
        for name, gate in self.gates.items():
            with self.subTest(gate=name):
                self.assertIn("reason", gate)
                self.assertGreater(len(str(gate["reason"]).strip()), 20)

    def test_a_profile_gated_role_cannot_serve_a_certified_capability(self):
        compose = yaml.load(COMPOSE_PATH.read_text(encoding="utf-8"), Loader=_Loader)
        profiled = {
            name for name, service in compose["services"].items()
            if service and service.get("profiles")
        }
        # The VN edge is the only profile-gated producer today. A capability it
        # serves may not claim certification while the role never starts.
        self.assertIn("vn_edge_v2", profiled)
        for entry in self.manifest["capabilities"]:
            if entry["venue"] in {"HNX", "HOSE"}:
                with self.subTest(venue=entry["venue"]):
                    self.assertEqual(entry["certification"], UNEXERCISED)


if __name__ == "__main__":
    unittest.main()
