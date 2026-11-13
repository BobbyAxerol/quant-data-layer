"""DL v2.3.0 declaration-only mechanism: accept-set, selectors, provenance, checkpoint crossing.

Backward compatibility is the first property under test: an unchanged manifest
still accepts exactly its own token revision, a manifest without selectors
loads to the same rows, a legacy bar-edge checkpoint still refuses a revision
crossing it cannot prove. Then the new behaviour: an additive declaration is
accepted (older tokens, kept watermarks, selector rows) and anything
non-additive is refused unless an explicit record says otherwise.
"""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import tempfile
import time
import unittest
from pathlib import Path

import yaml

from qdl.consumer import ConsumerManifestLoader
from qdl.consumer.manifest_compat import (
    additive_problems,
    declared_floor_problems,
    next_min_token_revision,
)
from qdl.consumer.selectors import SelectorContext
from qdl.query import ConsumerGrade, DataRequirement, FeedType
from qdl.runtime.bar_edge_checkpoint import (
    MIGRATION_SCHEMA,
    CheckpointCrossingRefused,
    plan_crossing,
)
from qdl.runtime.declaration import (
    DeclarationError,
    config_file_record,
    declared_universe_document,
    effective_declaration,
    enforce_declaration,
    load_universe,
)
from qdl.runtime.heartbeat import write_heartbeat
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan, stable_authority_record
from qdl.security.data_plane import DataPlaneAccessError
from tests.phase7_support import make_identity, make_token, manifest_mapping

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
SUBJECT = "spiffe://qdl/paper/v230-test"
UID = "fb26214c-7b9b-5961-95b2-55154755af0f"


def _manifest(revision: int, *, floor: int | None = None, **changes):
    payload = manifest_mapping(consumer_id="v230.consumer", subject=SUBJECT, instrument_uid=UID)
    payload["metadata"]["revision"] = revision
    if floor is not None:
        payload["metadata"]["min_token_revision"] = floor
    for key, value in changes.items():
        payload["spec"][key] = value
    return ConsumerManifestLoader.from_mapping(payload)


class AcceptSetTests(unittest.TestCase):
    def _authenticate(self, manifest, token_revision):
        identity = make_identity(manifest)
        return identity.authenticate(
            make_token(SUBJECT, manifest_revision=token_revision), consumer_id="v230.consumer"
        )

    def test_a_manifest_without_an_accept_set_keeps_the_exact_revision_rule(self):
        manifest = _manifest(4)
        self.assertIsNone(manifest.min_token_revision)
        self.assertEqual(self._authenticate(manifest, 4).manifest.manifest_revision, 4)
        for revision in (3, 5):
            with self.subTest(token=revision), self.assertRaises(DataPlaneAccessError) as caught:
                self._authenticate(manifest, revision)
            self.assertEqual(caught.exception.status_code, 401)

    def test_an_accept_set_admits_older_additive_tokens_and_never_newer_ones(self):
        manifest = _manifest(5, floor=3)
        for revision in (3, 4, 5):
            with self.subTest(token=revision):
                self.assertEqual(self._authenticate(manifest, revision).manifest.manifest_revision, 5)
        for revision in (2, 6):
            with self.subTest(token=revision), self.assertRaises(DataPlaneAccessError):
                self._authenticate(manifest, revision)

    def test_a_floor_outside_the_revision_is_refused_at_load(self):
        for floor in (0, 6, True):
            with self.subTest(floor=floor), self.assertRaises(ValueError):
                _manifest(5, floor=floor)

    def test_the_renderer_rule_keeps_the_floor_only_for_additive_successors(self):
        old = _manifest(4, floor=2)
        quotas = dict(manifest_mapping(consumer_id="x", subject="y", instrument_uid=UID)["spec"]["quotas"])
        additive = _manifest(5, quotas={**quotas, "max_streams": quotas["max_streams"] + 1})
        self.assertEqual(additive_problems(old, additive), [])
        self.assertEqual(next_min_token_revision(old, additive), 2)
        lowered = _manifest(5, quotas={**quotas, "max_streams": quotas["max_streams"] - 1})
        self.assertIn("quota max_streams lowered", additive_problems(old, lowered))
        self.assertEqual(next_min_token_revision(old, lowered), 5)
        fewer_purposes = _manifest(5, purposes=["INTERNAL_EXECUTION"])
        self.assertTrue(additive_problems(old, fewer_purposes))
        self.assertEqual(next_min_token_revision(None, additive), 5)
        overclaimed = _manifest(5, floor=2, quotas={**quotas, "max_streams": 1})
        self.assertTrue(declared_floor_problems(old, overclaimed))
        self.assertEqual(declared_floor_problems(old, _manifest(5, floor=2)), [])

    def test_repository_manifests_load_unchanged_and_compile_without_new_bundle_fields(self):
        spec = importlib.util.spec_from_file_location("kn_gateway_bundle", ROOT / "scripts/kn_gateway_bundle.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        paths = sorted((ROOT / "consumers/stable").glob("*.yaml"))
        document = module.compile_bundle(environment="paper", catalog_path=CATALOG, manifest_paths=paths)
        for entry in document["manifests"]:
            self.assertNotIn("min_token_revision", entry)
            self.assertNotIn("selector_expansions", entry)
        for path in paths:
            manifest = ConsumerManifestLoader.load(path)
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
            self.assertEqual(len(manifest.requirements), len(raw["spec"]["requirements"]))
            self.assertEqual(manifest.accepted_token_revisions, range(manifest.manifest_revision, manifest.manifest_revision + 1))


class SelectorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = StableSourceCatalog.load(CATALOG)
        okx = sorted(
            record.native_symbol for record in cls.catalog.instruments
            if record.identity.venue == "OKX" and record.identity.market == "SWAP"
        )
        cls.members = okx[:3]

    def _context(self, directory: Path, members=None, *, venue="OKX", market="SWAP"):
        document = declared_universe_document(
            universe_id="okx-swap-test", revision=2, venue=venue, market=market,
            members=self.members if members is None else members,
        )
        (directory / "okx-swap-test.json").write_text(json.dumps(document), encoding="utf-8")
        return SelectorContext.from_catalog(self.catalog, universe_dir=directory)

    def _payload(self, grade="ALPHA", intervals=("1m", "1H")):
        payload = manifest_mapping(consumer_id="v230.alpha", subject=SUBJECT, instrument_uid=UID)
        explicit = payload["spec"]["requirements"][0]
        template = {key: value for key, value in explicit.items() if key not in {"instrument_uid", "interval"}}
        template["consumer_grade"] = grade
        payload["spec"]["selectors"] = [{
            "universe": "okx-swap-test", "venue": "OKX", "market": "SWAP",
            "intervals": list(intervals), "template": template,
        }]
        return payload

    def test_a_selector_expands_deterministically_and_is_audited(self):
        with tempfile.TemporaryDirectory() as directory:
            context = self._context(Path(directory))
            first = ConsumerManifestLoader.from_mapping(self._payload(), selector_context=context)
            second = ConsumerManifestLoader.from_mapping(self._payload(), selector_context=context)
        self.assertEqual(first.requirements, second.requirements)
        self.assertEqual(len(first.requirements), 1 + len(self.members) * 2)
        self.assertEqual(first.requirements[0].instrument_uid, UID, "explicit rows stay first")
        (audit,) = first.selector_expansions
        self.assertEqual((audit["universe_id"], audit["universe_revision"]), ("okx-swap-test", 2))
        self.assertEqual(audit["members"], len(self.members))
        self.assertEqual(audit["requirements"], len(self.members) * 2)
        self.assertEqual(len(audit["requirements_sha256"]), 64)
        row = first.requirements[1]
        self.assertTrue(first.requirement_allowed(row))
        self.assertFalse(first.requirement_allowed(DataRequirement(
            instrument_uid=row.instrument_uid, feed=FeedType.BAR, consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id=row.source_policy_id, interval="4H",
        )))

    def test_selectors_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            context = self._context(Path(directory))
            with self.assertRaisesRegex(ValueError, "no universe/catalogue"):
                ConsumerManifestLoader.from_mapping(self._payload())
            with self.assertRaisesRegex(ValueError, "ALPHA/RESEARCH"):
                ConsumerManifestLoader.from_mapping(self._payload(grade="EXECUTION"), selector_context=context)
            missing = self._context(Path(directory), [*self.members, "NOT-A-REAL-USDT-SWAP"])
            with self.assertRaisesRegex(ValueError, "catalogue does not declare"):
                ConsumerManifestLoader.from_mapping(self._payload(), selector_context=missing)
            wrong_venue = self._context(Path(directory), venue="BINANCE", market="USDM")
            with self.assertRaisesRegex(ValueError, "selector asks OKX/SWAP"):
                ConsumerManifestLoader.from_mapping(self._payload(), selector_context=wrong_venue)
            path = Path(directory) / "okx-swap-test.json"
            self._context(Path(directory))
            tampered = json.loads(path.read_text())
            tampered["members"].append("ANOTHER-USDT-SWAP")
            path.write_text(json.dumps(tampered))
            with self.assertRaisesRegex(ValueError, "membership hash mismatch"):
                ConsumerManifestLoader.from_mapping(self._payload(), selector_context=context)

    def test_the_d48_market_cap_membership_is_a_universe_too(self):
        universe = load_universe(ROOT / "config/v2/universes/crypto-top300-1d.json", venue="OKX", market="SWAP")
        self.assertEqual(universe.universe_id, "crypto-top300-1d")
        self.assertIn("BTC-USDT-SWAP", universe.members)


class DeclarationProvenanceTests(unittest.TestCase):
    def test_image_files_are_named_and_the_guards_are_opt_in(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "image").mkdir()
            image_file = root / "image/stable-source-bindings.yaml"
            mounted = root / "stable-acquisition-bindings.yaml"
            image_file.write_text("a")
            mounted.write_text("b")
            records = [
                config_file_record(image_file, kind="catalog", revision=11, image_root=root / "image"),
                config_file_record(mounted, kind="acquisition", revision=20, image_root=root / "image"),
            ]
            declaration = effective_declaration(records, role="test")
            self.assertEqual(declaration["image_files"], 1)
            self.assertEqual(
                declaration["digest"], effective_declaration(reversed(records), role="test")["digest"]
            )
            self.assertIsNone(enforce_declaration(declaration, env={}))
            with self.assertRaisesRegex(DeclarationError, "loads image config"):
                enforce_declaration(declaration, env={"QDL_STABLE_REQUIRE_MOUNTED_DECLARATION": "1"})
            index = root / "declaration-index.json"
            files = {
                "stable-source-bindings.yaml": hashlib.sha256(b"a").hexdigest(),
                "stable-acquisition-bindings.yaml": hashlib.sha256(b"b").hexdigest(),
            }
            index.write_text(json.dumps({
                "schema": "qdl.v2.declaration-index.v1", "declaration_revision": 3, "files": files,
            }))
            self.assertEqual(
                enforce_declaration(declaration, env={"QDL_STABLE_DECLARATION_INDEX": str(index)})["declaration_revision"],
                3,
            )
            files["stable-acquisition-bindings.yaml"] = "0" * 64
            index.write_text(json.dumps({
                "schema": "qdl.v2.declaration-index.v1", "declaration_revision": 3, "files": files,
            }))
            with self.assertRaisesRegex(DeclarationError, "sha256 differs"):
                enforce_declaration(declaration, env={"QDL_STABLE_DECLARATION_INDEX": str(index)})

    def test_the_heartbeat_carries_additive_keys_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "hb.json"
            write_heartbeat(path, role="r", detail="d", min_interval_ns=0,
                            extra={"declaration": {"digest": "x"}, "role": "spoof"})
            body = json.loads(path.read_text())
        self.assertEqual(body["schema"], "qdl.role-heartbeat.v1")
        self.assertEqual(body["role"], "r")
        self.assertEqual(body["declaration"], {"digest": "x"})


def _digests(ids, value="a"):
    return {item: hashlib.sha256(f"{item}{value}".encode()).hexdigest() for item in ids}


def _record(before, after, *, removed=(), changed=(), previous=None):
    record = {
        "schema": MIGRATION_SCHEMA,
        "from": {"catalog_revision": before[0], "acquisition_revision": before[1]},
        "to": {"catalog_revision": after[0], "acquisition_revision": after[1]},
        "removed_binding_ids": list(removed),
        "changed_binding_ids": list(changed),
        "reason": "test", "approved_by": "test",
    }
    if previous is not None:
        record["previous_binding_digests"] = previous
    return record


class CheckpointCrossingPlanTests(unittest.TestCase):
    OLD = ("a", "b", "c")

    def _checkpoint(self, *, digests=True, revisions=(12, 20)):
        checkpoint = {
            "catalog_revision": revisions[0], "acquisition_revision": revisions[1],
            "binding_ids": list(self.OLD),
        }
        if digests:
            checkpoint["binding_digests"] = _digests(self.OLD)
        return checkpoint

    @staticmethod
    def _runtime(ids, revisions=(13, 21)):
        return {"catalog_revision": revisions[0], "acquisition_revision": revisions[1], "binding_ids": list(ids)}

    def test_same_revision_restart_is_unchanged(self):
        crossing = plan_crossing(
            checkpoint=self._checkpoint(), runtime=self._runtime(self.OLD, (12, 20)),
            current_digests=_digests(self.OLD),
        )
        self.assertFalse(crossing.crossed)
        self.assertEqual(crossing.retained, frozenset(self.OLD))
        with self.assertRaisesRegex(CheckpointCrossingRefused, "without a revision"):
            plan_crossing(checkpoint=self._checkpoint(), runtime=self._runtime(self.OLD, (12, 20)),
                          current_digests=_digests(self.OLD, "changed"))

    def test_an_additive_crossing_keeps_every_unchanged_watermark(self):
        new = (*self.OLD, "d", "e")
        crossing = plan_crossing(checkpoint=self._checkpoint(), runtime=self._runtime(new),
                                 current_digests=_digests(new))
        self.assertTrue(crossing.crossed)
        self.assertEqual(crossing.retained, frozenset(self.OLD))
        self.assertEqual(crossing.added, frozenset({"d", "e"}))
        self.assertFalse(crossing.migration_used)
        # A catalogue-only revision (stage A adds QUOTE rows) keeps everything.
        same_bars = plan_crossing(checkpoint=self._checkpoint(), runtime=self._runtime(self.OLD),
                                  current_digests=_digests(self.OLD))
        self.assertEqual(same_bars.retained, frozenset(self.OLD))

    def test_removal_and_source_mode_change_need_an_exact_record(self):
        new = ("a", "b")
        digests = _digests(new)
        with self.assertRaisesRegex(CheckpointCrossingRefused, "not additive"):
            plan_crossing(checkpoint=self._checkpoint(), runtime=self._runtime(new), current_digests=digests)
        record = _record((12, 20), (13, 21), removed=["c"])
        crossing = plan_crossing(checkpoint=self._checkpoint(), runtime=self._runtime(new),
                                 current_digests=digests, migration=record)
        self.assertEqual(crossing.dropped, frozenset({"c"}))
        self.assertTrue(crossing.migration_used)
        with self.assertRaisesRegex(CheckpointCrossingRefused, "exactly"):
            plan_crossing(checkpoint=self._checkpoint(), runtime=self._runtime(new), current_digests=digests,
                          migration=_record((12, 20), (13, 21), removed=["b", "c"]))
        moved = {**_digests(self.OLD), "b": "f" * 64}
        with self.assertRaisesRegex(CheckpointCrossingRefused, "changed="):
            plan_crossing(checkpoint=self._checkpoint(), runtime=self._runtime(self.OLD), current_digests=moved)
        crossing = plan_crossing(
            checkpoint=self._checkpoint(), runtime=self._runtime(self.OLD), current_digests=moved,
            migration=_record((12, 20), (13, 21), changed=["b"]),
        )
        self.assertEqual(crossing.retained, frozenset({"a", "c"}))
        self.assertEqual(crossing.dropped, frozenset({"b"}))

    def test_revisions_never_move_backwards(self):
        with self.assertRaisesRegex(CheckpointCrossingRefused, "forward declaration"):
            plan_crossing(checkpoint=self._checkpoint(), runtime=self._runtime(self.OLD, (11, 21)),
                          current_digests=_digests(self.OLD))

    def test_a_legacy_checkpoint_crosses_only_with_previous_digests(self):
        new = (*self.OLD, "d")
        with self.assertRaisesRegex(CheckpointCrossingRefused, "legacy checkpoint cannot prove"):
            plan_crossing(checkpoint=self._checkpoint(digests=False), runtime=self._runtime(new),
                          current_digests=_digests(new))
        stale = _record((10, 18), (11, 19), previous=_digests(self.OLD))
        with self.assertRaisesRegex(CheckpointCrossingRefused, "legacy checkpoint cannot prove"):
            plan_crossing(checkpoint=self._checkpoint(digests=False), runtime=self._runtime(new),
                          current_digests=_digests(new), migration=stale)
        record = _record((12, 20), (13, 21), previous=_digests(self.OLD))
        crossing = plan_crossing(checkpoint=self._checkpoint(digests=False), runtime=self._runtime(new),
                                 current_digests=_digests(new), migration=record)
        self.assertEqual(crossing.retained, frozenset(self.OLD))
        self.assertEqual(crossing.added, frozenset({"d"}))

    def test_a_stale_record_does_not_block_an_additive_crossing(self):
        new = (*self.OLD, "d")
        crossing = plan_crossing(
            checkpoint=self._checkpoint(), runtime=self._runtime(new), current_digests=_digests(new),
            migration=_record((1, 1), (2, 2), removed=["z"]),
        )
        self.assertEqual(crossing.added, frozenset({"d"}))
        self.assertFalse(crossing.migration_used)


class _Publisher:
    def publish_many(self, values):
        return tuple(range(len(tuple(values))))


class BarEdgeCheckpointCrossingTests(unittest.TestCase):
    """The real edge restores across a revision: additive kept, removal refused."""

    @classmethod
    def setUpClass(cls):
        cls.catalog_raw = json.loads(CATALOG.read_text(encoding="utf-8"))
        cls.acquisition_raw = yaml.safe_load(ACQUISITION.read_text(encoding="utf-8"))
        cls.authority = stable_authority_record(
            rust_image_digest="d" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION.read_bytes(),
            effective_at_ns=time.time_ns(),
        )
        bars = [item for item in cls.catalog_raw["bindings"] if item["feed"] == "BAR"
                and item["binding_id"].startswith("okx-swap") and item.get("interval") == "1m"]
        cls.dropped = sorted(item["binding_id"] for item in bars[:2])

    def _plan(self, directory: Path, *, without=(), delta=0):
        catalog = copy.deepcopy(self.catalog_raw)
        acquisition = copy.deepcopy(self.acquisition_raw)
        catalog["bindings"] = [item for item in catalog["bindings"] if item["binding_id"] not in without]
        acquisition["bindings"] = [item for item in acquisition["bindings"] if item["binding_id"] not in without]
        catalog["catalog_revision"] += delta
        acquisition["revision"] += delta
        name = f"r{delta}-{len(without)}"
        catalog_path = directory / f"{name}-catalog.yaml"
        acquisition_path = directory / f"{name}-acquisition.yaml"
        catalog_path.write_text(json.dumps(catalog), encoding="utf-8")
        acquisition_path.write_text(yaml.safe_dump(acquisition), encoding="utf-8")
        loaded = StableSourceCatalog.load(catalog_path)
        return loaded, StableAcquisitionPlan.load(acquisition_path, catalog=loaded)

    def _edge(self, plan, state_path, **kwargs):
        catalog, acquisition = plan
        return StableBinanceBarEdge(
            catalog=catalog, acquisition=acquisition, authority=self.authority,
            publisher=_Publisher(), warmup_rows=2, state_path=state_path,
            clock=lambda: 180.0, generation_clock_ns=lambda: 100, **kwargs,
        )

    def _seed(self, directory: Path, plan):
        state_path = directory / "checkpoint.json"
        seed = self._edge(plan, state_path)
        one_minute = [
            source.binding_id
            for source, _acquisition in seed.history_bindings + seed.history_okx_bindings
            if source.interval == "1m"
        ]
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema"], "qdl.stable-bar-edge-state.v5")
        payload["last_open_ms"] = {binding: 1_789_732_800_000 for binding in one_minute}
        state_path.write_text(json.dumps(payload), encoding="utf-8")
        return state_path, set(one_minute)

    def test_an_additive_revision_keeps_watermarks_and_bootstraps_only_the_added(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path, watermarked = self._seed(root, self._plan(root, without=self.dropped))
            edge = self._edge(self._plan(root, delta=1), state_path)
        self.assertTrue(edge.checkpoint_crossing.crossed)
        self.assertEqual(edge.checkpoint_crossing.added, frozenset(self.dropped))
        self.assertEqual(set(edge._last_open_ms), watermarked)
        self.assertFalse(edge._history_bootstrapped, "the added bindings still bootstrap")

    def test_a_removal_is_refused_without_a_record_and_accepted_with_one(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            full = self._plan(root)
            state_path, watermarked = self._seed(root, full)
            reduced = self._plan(root, without=self.dropped, delta=1)
            with self.assertRaisesRegex(RuntimeError, "not additive"):
                self._edge(reduced, state_path)
            before = (full[0].catalog_revision, full[1].revision)
            after = (reduced[0].catalog_revision, reduced[1].revision)
            edge = self._edge(reduced, state_path,
                              checkpoint_migration=_record(before, after, removed=self.dropped))
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertTrue(edge.checkpoint_crossing.migration_used)
        self.assertEqual(set(edge._last_open_ms), watermarked - set(self.dropped))
        self.assertEqual(persisted["catalog_revision"], after[0])
        self.assertEqual(persisted["acquisition_revision"], after[1])
        self.assertFalse(set(self.dropped) & set(persisted["binding_digests"]))


if __name__ == "__main__":
    unittest.main()


class QueryDeclarationTests(unittest.TestCase):
    def test_a_plain_manifest_never_builds_the_selector_context(self):
        from qdl.consumer.selectors import LazySelectorContext

        built = []
        context = LazySelectorContext(lambda: built.append(1) or None)
        ConsumerManifestLoader.load(ROOT / "consumers/stable/trading-system-paper.yaml",
                                    selector_context=context)
        self.assertEqual(built, [])

    def test_the_query_declaration_names_catalog_acquisition_and_every_manifest(self):
        from types import SimpleNamespace

        from qdl.consumer import ConsumerManifestRegistry
        from qdl.runtime.stable import _declaration_probe, stable_effective_declaration

        path = ROOT / "consumers/stable/trading-system-paper.yaml"
        manifest = ConsumerManifestLoader.load(path)
        registry = ConsumerManifestRegistry((manifest,))
        registry.declaration_records = (config_file_record(
            path, kind="manifest", revision=manifest.manifest_revision, identity=manifest.consumer_id,
        ),)
        config = SimpleNamespace(role="query_v2", source_bindings_path=CATALOG,
                                 acquisition_bindings_path=ACQUISITION)
        declaration = stable_effective_declaration(
            config, catalog=SimpleNamespace(catalog_revision=12),
            acquisition=SimpleNamespace(revision=20), manifests=registry,
        )
        kinds = {(item["kind"], item.get("identity"), item["revision"]) for item in declaration["files"]}
        self.assertEqual(kinds, {
            ("catalog", None, 12), ("acquisition", None, 20),
            ("manifest", "trading-system.paper.stable", manifest.manifest_revision),
        })
        self.assertEqual(declaration["image_files"], 0)
        probe_detail = _declaration_probe(declaration)
        self.assertEqual(probe_detail.name, "declaration")


class ReleaseRoutePlanSelectorTests(unittest.TestCase):
    def test_the_release_route_loader_passes_the_selector_context_through(self):
        import inspect

        from qdl.consumer.release import StableReleaseRoutePlan

        for method in (StableReleaseRoutePlan.load, StableReleaseRoutePlan.from_mapping,
                       StableReleaseRoutePlan._consumer):
            parameter = inspect.signature(method).parameters["selector_context"]
            self.assertIsNone(parameter.default, method.__name__)
        plan = StableReleaseRoutePlan.load(
            ROOT / "config/v2/stable-v2-release-routing.yaml", manifest_root=ROOT,
        )
        self.assertTrue(plan.consumers)


class SeriesDigestDerivedTests(unittest.TestCase):
    """A DERIVED_FINAL acquisition (derived-bars slice) fingerprints its base interval."""

    def test_derived_from_base_interval_is_part_of_the_series_identity(self):
        from types import SimpleNamespace

        from qdl.runtime.bar_edge_checkpoint import binding_series_digest

        catalog = StableSourceCatalog.load(CATALOG)
        acquisition = StableAcquisitionPlan.load(ACQUISITION, catalog=catalog)
        source = next(item for item in catalog.bindings if item.feed.value == "BAR")
        native = next(item for item in acquisition.bindings if item.binding_id == source.binding_id)
        plain = binding_series_digest(source, native)
        self.assertEqual(plain, binding_series_digest(source, SimpleNamespace(**{
            name: getattr(native, name) for name in ("runtime", "mode", "provider_kind", "native_channel")
        }, derived_from=None)), "no derived_from keeps the existing digest")
        derived = SimpleNamespace(runtime=native.runtime, mode="DERIVED_FINAL",
                                  provider_kind=native.provider_kind, native_channel=native.native_channel,
                                  derived_from=SimpleNamespace(interval="1m"))
        first = binding_series_digest(source, derived)
        self.assertNotEqual(first, plain)
        derived.derived_from = SimpleNamespace(interval="5m")
        self.assertNotEqual(binding_series_digest(source, derived), first)
        derived.derived_from = "1m"
        self.assertEqual(binding_series_digest(source, derived), first, "a plain string hashes the same")
        derived.derived_from = SimpleNamespace(interval="5m")
        self.assertNotEqual(binding_series_digest(source, derived), first)
        derived.derived_from = SimpleNamespace(base_interval="1m")
        with self.assertRaisesRegex(ValueError, "no base interval"):
            binding_series_digest(source, derived)
