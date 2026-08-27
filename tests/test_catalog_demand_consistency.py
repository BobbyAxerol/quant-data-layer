from __future__ import annotations

import unittest
from pathlib import Path

import yaml

from qdl.consumer.manifest import ConsumerManifestLoader
from qdl.query.contracts import RecoveryPolicy
from qdl.runtime.provider_history import pass_through_eligible
from qdl.runtime.production_catalog import ProductionDemandManifest
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
CONSUMER_DIR = ROOT / "consumers/stable"
DEMAND_PATH = ROOT / "config/v2/stable-crypto-demand.yaml"

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


def _production_demand_keys(
    catalog: StableSourceCatalog,
) -> set[tuple[str, str, str | None]]:
    """Resolve the versioned stable demand into catalog requirement keys."""
    by_provider_identity = {
        (
            item.instrument.identity.venue,
            item.instrument.identity.market,
            item.instrument.native_symbol,
            item.feed.value,
            item.interval or None,
        ): (
            item.instrument.identity.instrument_uid,
            item.feed.value,
            item.interval or None,
        )
        for item in catalog.bindings
    }
    result = set()
    for requirement in ProductionDemandManifest.load_many([DEMAND_PATH]).demands:
        key = (
            requirement.venue,
            requirement.market,
            requirement.native_symbol,
            requirement.feed.value,
            requirement.interval or None,
        )
        try:
            result.add(by_provider_identity[key])
        except KeyError as error:
            raise AssertionError(
                "versioned production demand has no catalog binding: " + repr(key)
            ) from error
    return result


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

    def test_every_consumer_requirement_is_servable(self):
        """A requirement must resolve to a binding or to the pass-through.

        Requiring a binding for every requirement was right while the spool was
        the only source. It is now too strong: a `FRESH_SNAPSHOT` BAR request
        for a declared instrument is served by the pass-through with no binding
        at all, and the check has to say so rather than fail a manifest the
        runtime serves correctly.

        It stays a real check. A requirement that neither source can answer —
        an undeclared instrument, an unbound TRADE feed, or a BAR request that
        asks for replay continuity — still fails here.
        """
        unservable: dict[tuple[str, str, str | None], set[str]] = {}
        for path in sorted(CONSUMER_DIR.glob("*.yaml")):
            manifest = ConsumerManifestLoader.load(path)
            for requirement in manifest.requirements:
                key = (
                    requirement.instrument_uid,
                    requirement.feed.value,
                    requirement.interval or None,
                )
                if key in self.binding_keys:
                    continue
                if pass_through_eligible(self.catalog, requirement):
                    continue
                unservable.setdefault(key, set()).add(manifest.consumer_id)
        self.assertEqual(unservable, {})

    def test_non_minute_bar_freshness_covers_its_interval(self):
        interval_ms = {"15m": 15 * 60_000, "1h": 60 * 60_000}
        checked: list[tuple[str, str]] = []
        for path in sorted(CONSUMER_DIR.glob("alpha-*-paper.yaml")):
            manifest = ConsumerManifestLoader.load(path)
            for requirement in manifest.requirements:
                if (
                    requirement.feed.value != "BAR"
                    or requirement.interval not in interval_ms
                ):
                    continue
                self.assertGreaterEqual(
                    requirement.max_freshness_ms or 0,
                    interval_ms[requirement.interval],
                    f"{manifest.consumer_id}:{requirement.interval} freshness "
                    "is shorter than its bar interval",
                )
                checked.append((manifest.consumer_id, requirement.interval))
        self.assertEqual(
            checked,
            [
                ("alpha.binance.paper.stable", "15m"),
                ("alpha.binance.paper.stable", "15m"),
                ("alpha.okx.paper.stable", "1h"),
                ("alpha.okx.paper.stable", "1h"),
            ],
        )

    def test_a_requirement_no_source_can_answer_still_fails(self):
        """Guards the check above from having been weakened into nothing."""
        from dataclasses import replace

        manifest = ConsumerManifestLoader.load(
            CONSUMER_DIR / "alpha-binance-paper.yaml"
        )
        served = next(
            item for item in manifest.requirements
            if item.recovery is RecoveryPolicy.FRESH_SNAPSHOT
        )
        self.assertTrue(pass_through_eligible(self.catalog, served))

        # Asking for replay continuity: only a binding can promise that.
        replay = replace(
            served,
            interval="2d",
            recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY,
        )
        self.assertNotIn(
            (replay.instrument_uid, replay.feed.value, replay.interval),
            self.binding_keys,
        )
        self.assertFalse(pass_through_eligible(self.catalog, replay))

        # An instrument the catalog never declared.
        unknown = replace(
            served, instrument_uid="00000000-0000-5000-8000-000000000000"
        )
        self.assertFalse(pass_through_eligible(self.catalog, unknown))

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
        demanded = set(_requirement_keys()) | _production_demand_keys(self.catalog)
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
