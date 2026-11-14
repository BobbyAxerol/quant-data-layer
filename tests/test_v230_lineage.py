"""DL v2.3.0 slice 2d: per-binding contract lineage, cursor lineage, and the contract-line schema digest.

Regression first: the stage A declaration (catalog 12 -> 13, acquisition 20 -> 21, 40 bindings added, none changed;
`tests/fixtures/v230_stage_a_delta.json`, from `git diff 098c94b..c29c216`) moved the lineage of every untouched
binding. Unpinned, the gate must see exactly that; pinned, every retained binding keeps catalog 12 / config 20 on
its raw envelopes (ingestor config), core config, bundle, served contract and cursors, and only the 40 added
bindings carry 13 / 21. Backward compatibility: an unpinned catalogue behaves exactly as before.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import tempfile
import time
import unittest
from pathlib import Path

import yaml

from qdl.replay.cursor_v3 import (
    CursorV3Claims,
    CursorV3Expectation,
    CursorV3Expired,
    SignedCursorV3Codec,
)
from qdl.runtime.lineage import binding_lineage, pin_lineage, retained_lineage_problems, retained_unchanged_ids
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan, stable_authority_record

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
DELTA = json.loads((ROOT / "tests/fixtures/v230_stage_a_delta.json").read_text(encoding="utf-8"))


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write(directory: Path, name: str, catalog_doc, plan_doc):
    catalog_path = directory / f"{name}-catalog.yaml"
    plan_path = directory / f"{name}-acquisition.yaml"
    catalog_path.write_text(json.dumps(catalog_doc), encoding="utf-8")
    plan_path.write_text(yaml.safe_dump(plan_doc), encoding="utf-8")
    catalog = StableSourceCatalog.load(catalog_path)
    return catalog, StableAcquisitionPlan.load(plan_path, catalog=catalog), catalog_path


class StageAShapeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.new_catalog_doc = json.loads(CATALOG.read_text(encoding="utf-8"))
        cls.new_plan_doc = yaml.safe_load(ACQUISITION.read_text(encoding="utf-8"))
        assert (cls.new_catalog_doc["catalog_revision"], cls.new_plan_doc["revision"]) == (13, 21)
        added = set(DELTA["added_binding_ids"])
        cls.added = added
        cls.old_catalog_doc = copy.deepcopy(cls.new_catalog_doc)
        cls.old_plan_doc = copy.deepcopy(cls.new_plan_doc)
        cls.old_catalog_doc["bindings"] = [b for b in cls.old_catalog_doc["bindings"] if b["binding_id"] not in added]
        cls.old_plan_doc["bindings"] = [b for b in cls.old_plan_doc["bindings"] if b["binding_id"] not in added]
        cls.old_catalog_doc["catalog_revision"] = 12
        cls.old_plan_doc["revision"] = 20
        cls.authority = stable_authority_record(
            rust_image_digest="d" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION.read_bytes(),
            effective_at_ns=time.time_ns(),
        )
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        cls.old = _write(root, "old", cls.old_catalog_doc, cls.old_plan_doc)
        cls.unpinned = _write(root, "unpinned", cls.new_catalog_doc, cls.new_plan_doc)
        pinned_catalog, pinned_plan, cls.report = pin_lineage(
            cls.old_catalog_doc, cls.old_plan_doc, cls.new_catalog_doc, cls.new_plan_doc
        )
        cls.pinned = _write(root, "pinned", pinned_catalog, pinned_plan)
        cls.retained = retained_unchanged_ids(
            cls.old_catalog_doc, cls.old_plan_doc, cls.new_catalog_doc, cls.new_plan_doc
        )

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_the_delta_is_the_stage_a_shape(self):
        self.assertEqual(len(self.added), 40)
        self.assertEqual(len(self.retained), len(self.new_catalog_doc["bindings"]) - 40)
        self.assertEqual(self.report, {"retained": len(self.retained), "new_or_changed": 40})

    def test_unpinned_the_gate_sees_every_retained_binding_move(self):
        problems = retained_lineage_problems(self.old[:2], self.unpinned[:2], self.retained)
        self.assertTrue(problems)
        self.assertIn(f"{len(self.retained)} retained bindings", problems[0])

    def test_pinned_retained_lineage_is_unchanged_and_added_bindings_are_new(self):
        self.assertEqual(retained_lineage_problems(self.old[:2], self.pinned[:2], self.retained), [])
        catalog, plan = self.pinned[:2]
        sample = sorted(self.retained)[0]
        self.assertEqual(binding_lineage(catalog, plan, sample)["instrument_catalog_revision"], 12)
        self.assertEqual(binding_lineage(catalog, plan, sample)["config_revision"], 20)
        for binding in self.added:
            lineage = binding_lineage(catalog, plan, binding)
            self.assertEqual((lineage["instrument_catalog_revision"], lineage["config_revision"]), (13, 21))
        migrated = retained_lineage_problems(
            self.old[:2], self.unpinned[:2], self.retained, migration_binding_ids=self.retained
        )
        self.assertEqual(migrated, [], "a migration record may name the bindings it moves")

    def test_runtime_configs_stamp_the_pinned_lineage(self):
        catalog, plan = self.pinned[:2]
        configs = plan.native_ingestor_configs(catalog=catalog, authority=self.authority)
        okx = configs["okx-swap"]
        self.assertEqual(okx["config_revision"], 21)
        by_source = {src.source_id: src.binding_id for src in catalog.bindings}
        seen = {"retained": 0, "added": 0}
        for row in okx["bindings"]:
            binding = by_source[row["subscription_id"]]
            kind = "added" if binding in self.added else "retained"
            seen[kind] += 1
            expected = (13, 21) if kind == "added" else (12, 20)
            self.assertEqual((row["instrument_catalog_revision"], row.get("config_revision")), expected,
                             (binding, row["native_channel"]))
        self.assertTrue(seen["retained"] and seen["added"], seen)
        core = plan.core_config(catalog=catalog, authority=self.authority)["core"]["bindings"]
        self.assertTrue({item["instrument_catalog_revision"] for item in core} <= {12, 13})

    def test_unpinned_configs_are_unchanged(self):
        catalog, plan = self.unpinned[:2]
        okx = plan.native_ingestor_configs(catalog=catalog, authority=self.authority)["okx-swap"]
        self.assertTrue(all("config_revision" not in row for row in okx["bindings"]))
        self.assertTrue(all(row["instrument_catalog_revision"] == 13 for row in okx["bindings"]))

    def test_the_bundle_carries_lineage_only_when_pinned(self):
        module = _load_module("kn_gateway_bundle", ROOT / "scripts/kn_gateway_bundle.py")
        manifests = sorted((ROOT / "consumers/stable").glob("*.yaml"))
        unpinned = module.compile_bundle(environment="paper", catalog_path=self.unpinned[2], manifest_paths=manifests)
        self.assertTrue(all("lineage_revision" not in row for row in unpinned["catalog"]["bindings"]))
        pinned = module.compile_bundle(environment="paper", catalog_path=self.pinned[2], manifest_paths=manifests)
        lineage = {row["binding_id"]: row["lineage_revision"] for row in pinned["catalog"]["bindings"]}
        self.assertEqual({lineage[b] for b in self.added}, {13})
        self.assertEqual({lineage[b] for b in self.retained}, {12})

    def test_a_cursor_for_a_retained_product_survives_the_declaration(self):
        catalog = self.pinned[0]
        retained_source = next(src for src in catalog.bindings if src.binding_id in self.retained)
        product = f"lpk1|paper|{retained_source.instrument.identity.venue}|x|{retained_source.binding_id[:20]}"
        codec = SignedCursorV3Codec({"k1": b"k" * 32}, active_key_id="k1")

        def token(catalog_revision: int) -> str:
            return codec.encode(CursorV3Claims(
                key_id="k1", environment="paper", consumer_id="c", requirement_digest="a" * 64,
                schema_major=2, stream="md.canonical.v2", product_key=product, snapshot_id="s",
                source_topic_id="t", source_partition=0, source_offset=1, partition_plan_epoch=1,
                source_policy_revision=1, catalog_revision=catalog_revision, route_generation="r",
                issued_at_ns=1, expires_at_ns=10**18,
            ))

        base = dict(environment="paper", stream="md.canonical.v2", source_topic_id="t", partition_plan_epoch=1,
                    source_policy_revision=1, catalog_revision=13, route_generation="r")
        pinned = CursorV3Expectation(**base, catalog_revision_by_product={product: 12})
        legacy = CursorV3Expectation(**base)

        def verify(value: str, expected: CursorV3Expectation):
            return codec.verify(value, consumer_id="c", environment="paper", requirement_digest_value="a" * 64,
                                expected=expected, now_ns=2)

        self.assertEqual(verify(token(12), pinned).catalog_revision, 12)
        with self.assertRaises(CursorV3Expired):
            verify(token(13), pinned)
        with self.assertRaises(CursorV3Expired):
            verify(token(12), legacy)  # the pre-v2.3.0 behaviour: a declaration expires it
        self.assertEqual(verify(token(13), legacy).catalog_revision, 13)


class LineageValidationTests(unittest.TestCase):
    def test_lineage_cannot_be_newer_than_its_document(self):
        catalog_doc = json.loads(CATALOG.read_text(encoding="utf-8"))
        plan_doc = yaml.safe_load(ACQUISITION.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            bad = copy.deepcopy(catalog_doc)
            bad["bindings"][0]["lineage_revision"] = bad["catalog_revision"] + 1
            with self.assertRaisesRegex(ValueError, "newer than the catalogue"):
                _write(Path(directory), "bad-catalog", bad, plan_doc)
            bad = copy.deepcopy(catalog_doc)
            bad["bindings"][0]["lineage_revision"] = 0
            with self.assertRaises(ValueError):
                _write(Path(directory), "zero", bad, plan_doc)
            bad_plan = copy.deepcopy(plan_doc)
            bad_plan["bindings"][0]["lineage_config_revision"] = bad_plan["revision"] + 1
            with self.assertRaisesRegex(ValueError, "newer than the plan"):
                _write(Path(directory), "bad-plan", catalog_doc, bad_plan)

    def test_an_unpinned_catalogue_keeps_the_global_revision(self):
        catalog = StableSourceCatalog.load(CATALOG)
        plan = StableAcquisitionPlan.load(ACQUISITION, catalog=catalog)
        binding = catalog.bindings[0]
        self.assertIsNone(binding.lineage_revision)
        self.assertEqual(catalog.lineage_revision_for(binding), catalog.catalog_revision)
        self.assertEqual(plan.lineage_config_revision_for(binding.binding_id), plan.revision)


class ContractLineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tool = _load_module("schema_line", ROOT / "scripts/schema_line.py")

    def test_the_line_digest_is_the_deployed_schema_digest(self):
        digest = self.tool.schema_digest(ROOT)
        compose = (ROOT / "deploy/compose/qdl-v2-stable-kn.yml").read_text(encoding="utf-8")
        literals = {line.split(":", 1)[1].strip() for line in compose.splitlines()
                    if line.strip().startswith("QDL_STABLE_SCHEMA_DIGEST:")}
        self.assertEqual(literals, {digest})

    def test_check_passes_additive_and_refuses_breaking_changes(self):
        calls = []

        def buf(rc):
            def run(args):
                calls.append(list(args))
                return rc
            return run

        passing = self.tool.check(ROOT, buf(0))
        self.assertEqual(passing["status"], "PASS")
        self.assertEqual(passing["line"], 1)
        self.assertEqual(calls[-1][:2], ["breaking", "--against"])
        failing = self.tool.check(ROOT, buf(100))
        self.assertEqual(failing["status"], "FAIL")
        self.assertIn("new contract line", failing["problems"][0])

    def test_a_tampered_baseline_image_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for relative in ("contracts/schema-lines/qdl-v2-contract-lines.json",
                             "contracts/proto/qdl/marketdata/v2/market_data.proto"):
                (root / relative).parent.mkdir(parents=True, exist_ok=True)
                (root / relative).write_bytes((ROOT / relative).read_bytes())
            image = root / "contracts/baseline/qdl-v2-contract-line-1.binpb"
            image.parent.mkdir(parents=True)
            image.write_bytes(b"not the baseline")
            verdict = self.tool.check(root, lambda _args: 0)
        self.assertEqual(verdict["status"], "FAIL")
        self.assertIn("baseline image", verdict["problems"][0])


if __name__ == "__main__":
    unittest.main()


class LiveConfigLineageTests(unittest.TestCase):
    """Review H2: config lineage comes from what the live ingestors stamp, not the plan revision."""

    def test_pins_follow_the_live_ingestor_stamp_and_the_gate_checks_it(self):
        from qdl.runtime.lineage import live_config_lineage, live_config_lineage_problems

        catalog_doc = json.loads(CATALOG.read_text(encoding="utf-8"))
        plan_doc = yaml.safe_load(ACQUISITION.read_text(encoding="utf-8"))
        catalog = StableSourceCatalog.load(CATALOG)
        binance = next(b for b in catalog.bindings if b.binding_id.startswith("binance-usdm") and b.feed.value == "QUOTE")
        okx = next(b for b in catalog.bindings if b.binding_id.startswith("okx-swap") and b.feed.value == "QUOTE")
        configs = [
            {"config_revision": 19, "bindings": [{"subscription_id": binance.source_id}]},
            {"config_revision": 21, "bindings": [{"subscription_id": okx.source_id}]},
        ]
        live = live_config_lineage(catalog, configs)
        self.assertEqual(live, {binance.binding_id: 19, okx.binding_id: 21})
        pinned_catalog, pinned_plan, _ = pin_lineage(catalog_doc, plan_doc, catalog_doc, plan_doc, live_config=live)
        with tempfile.TemporaryDirectory() as directory:
            _, plan, _ = _write(Path(directory), "live", pinned_catalog, pinned_plan)
            self.assertEqual(live_config_lineage_problems(plan, live), [])
            self.assertEqual(plan.lineage_config_revision_for(binance.binding_id), 19)
            _, unpinned, _ = _write(Path(directory), "doc", *pin_lineage(catalog_doc, plan_doc, catalog_doc, plan_doc)[:2])
            problems = live_config_lineage_problems(unpinned, live)
            self.assertTrue(problems and "1 bindings" in problems[0], problems)
        with self.assertRaisesRegex(ValueError, "two live config revisions"):
            live_config_lineage(catalog, configs + [{"config_revision": 20, "bindings": [{"subscription_id": okx.source_id}]}])


class SharedSourceLineageTests(unittest.TestCase):
    """Re-review H2: BOOK_SNAPSHOT and BOOK_DELTA share one subscription; both get the live stamp."""

    def test_both_book_bindings_of_one_source_are_stamped_and_gated(self):
        from qdl.runtime.lineage import live_config_lineage, live_config_lineage_problems

        catalog_doc = json.loads(CATALOG.read_text(encoding="utf-8"))
        plan_doc = yaml.safe_load(ACQUISITION.read_text(encoding="utf-8"))
        catalog = StableSourceCatalog.load(CATALOG)
        books = {}
        for item in catalog.bindings:
            if item.binding_id.startswith("binance-usdm") and item.feed.value in {"BOOK_SNAPSHOT", "BOOK_DELTA"}:
                books.setdefault(item.source_id, []).append(item.binding_id)
        source, pair = next((s, ids) for s, ids in books.items() if len(ids) == 2)
        live = live_config_lineage(catalog, [{"config_revision": 19, "bindings": [{"subscription_id": source}]}])
        self.assertEqual(live, {binding: 19 for binding in pair})
        with tempfile.TemporaryDirectory() as directory:
            pinned = pin_lineage(catalog_doc, plan_doc, catalog_doc, plan_doc, live_config=live)
            _, plan, _ = _write(Path(directory), "pinned", *pinned[:2])
            self.assertEqual(live_config_lineage_problems(plan, live, catalog=catalog), [])
            self.assertEqual({plan.lineage_config_revision_for(b) for b in pair}, {19})
            # The old single-binding map dropped one book binding; plan-side coverage reports it.
            dropped = {pair[0]: 19}
            problems = live_config_lineage_problems(plan, dropped, catalog=catalog)
            self.assertTrue(any(pair[1] in item for item in problems), problems)
