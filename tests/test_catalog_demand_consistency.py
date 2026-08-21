from __future__ import annotations

import unittest
from pathlib import Path

import yaml

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
CONSUMER_DIR = ROOT / "consumers/stable"

# Bindings that no registered consumer requires. Program rule 6 says demand
# controls cost, so a zero-demand feed should be disabled by configuration
# rather than acquired. These are recorded as reviewable debt: the set may
# shrink freely, but a new entry fails this test so the debt cannot grow
# unnoticed. See plan section C.16.
KNOWN_ZERO_DEMAND = {
    "binance-spot-btcusdt-bar-1m",
    "binance-spot-btcusdt-quote",
    "binance-spot-btcusdt-trade",
    "okx-spot-btcusdt-bar-1m",
    "okx-spot-btcusdt-quote",
    "okx-spot-btcusdt-trade",
    "dnse-fpt-bar-1m",
    "dnse-fpt-trade",
}


def _requirement_keys() -> dict[tuple[str, str, str | None], set[str]]:
    keys: dict[tuple[str, str, str | None], set[str]] = {}
    for path in sorted(CONSUMER_DIR.glob("*.yaml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        consumer_id = payload["metadata"]["id"]
        for requirement in payload["spec"]["requirements"]:
            key = (
                requirement["instrument_uid"],
                requirement["feed"],
                requirement.get("interval") or None,
            )
            keys.setdefault(key, set()).add(consumer_id)
    return keys


class CatalogDemandConsistencyTests(unittest.TestCase):
    """Bind the catalog to the consumers it exists for.

    Drift between a consumer manifest and the catalog is invisible until a
    request fails at runtime, which is how the ETH revision mismatch reached a
    rollout plan.
    """

    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.acquisition = StableAcquisitionPlan.load(
            ACQUISITION_PATH, catalog=self.catalog
        )
        self.binding_keys = {
            (
                binding.instrument.identity.instrument_uid,
                binding.feed.value,
                binding.interval or None,
            ): binding.binding_id
            for binding in self.catalog.bindings
        }

    def test_every_consumer_requirement_resolves_to_a_binding(self):
        unresolved = {
            key: consumers
            for key, consumers in _requirement_keys().items()
            if key not in self.binding_keys
        }
        self.assertEqual(unresolved, {})

    def test_acquisition_plan_covers_exactly_the_catalog(self):
        catalog_ids = {binding.binding_id for binding in self.catalog.bindings}
        acquisition_ids = {item.binding_id for item in self.acquisition.bindings}
        self.assertEqual(acquisition_ids, catalog_ids)

    def test_every_instrument_backs_at_least_one_binding(self):
        referenced = {
            binding.instrument.identity.instrument_uid
            for binding in self.catalog.bindings
        }
        declared = {
            item["instrument_uid"]
            for item in yaml.safe_load(
                CATALOG_PATH.read_text(encoding="utf-8")
            )["instruments"]
        }
        self.assertEqual(declared - referenced, set())

    def test_zero_demand_bindings_do_not_grow(self):
        demanded = set(_requirement_keys())
        zero_demand = {
            binding_id
            for key, binding_id in self.binding_keys.items()
            if key not in demanded
        }
        self.assertEqual(
            zero_demand - KNOWN_ZERO_DEMAND,
            set(),
            "a new zero-demand binding was added; either register a consumer "
            "requirement for it or record it in plan section C.16",
        )
        self.assertTrue(
            KNOWN_ZERO_DEMAND - zero_demand <= KNOWN_ZERO_DEMAND,
            "shrinking the zero-demand set is expected and allowed",
        )


if __name__ == "__main__":
    unittest.main()
