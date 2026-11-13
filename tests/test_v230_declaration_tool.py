"""DL v2.3.0 declaration tool (scripts/dl_declaration.py): golden, additivity, BLOCKED, gate, migration.

* Stage A golden (repository part, always runs): the v2.2.6 stage A declaration rendered against this tree's r12
  revision set reproduces, byte for byte, the six repository files of the stage A worktree (demand r9, catalogue r13,
  acquisition r21, promotion scope r12, routing r30, Trading System Paper manifest r12).
* Stage A golden (runtime part): with ``QDL_V230_STAGE_A_RUNTIME_DIR`` pointing at the deployed base role files
  (layout in ``_runtime_declaration``), the render also reproduces the pinned ingestor, three cores, both gateway
  bundles, the sealed binding r24 and the Query runtime catalogue + acquisition. Skipped without the directory
  (the base files are host state, not repository content); the expected hashes are the pinned SHA256SUMS.
* 150-instrument declaration (dry): typed BLOCKED for FET/LUNA, the accept-set floors and the capacity verdict.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

import yaml

from qdl.runtime.bar_edge_checkpoint import CheckpointCrossingRefused, plan_crossing
from scripts import dl_declaration as tool

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/v230_declarations"
STAGE_A_REPO = {
    "config/v2/stable-crypto-demand.yaml": "f0736d7bdc3c54f43c24a9a9563a3162f0e6fe6b869e96eaa79db59975dab975",
    "config/v2/stable-source-bindings.yaml": "f51ef5c44e46e04a319f1e8e1e59362cee165abbf1da659fa427c811e57bd926",
    "config/v2/stable-acquisition-bindings.yaml": "4533da057ce2e521c998657fb19e89c5549746b9e084002cd9fff698bf73f4e7",
    "config/v2/stable-authority-promotion-scope.yaml":
        "65d21991e61f0d8831b77ea0a156bd3089b18644420b39f66b7d6fd1de6b80ba",
    "config/v2/stable-v2-release-routing.yaml": "fcaf0e67e8038db6d8359ca91bb369d315032f4b24f000eed92099e8f63833ee",
    "consumers/stable/trading-system-paper.yaml": "9c21d9fb752d8dc26a5fad99f2b399a53c5b6f2005528752b20b905ec4e533c1",
}
# dlcov/stageA-pinned/SHA256SUMS (inputs under base/, then the pinned outputs)
STAGE_A_RUNTIME_INPUTS = {
    "base/rust_core.json": "e11550dc6eac8d9311469afca7a42f567596ebd7b4625925c85a7f773521a482",
    "base/rust_core_2.json": "9d8ed362ed8ddb4881bb3679900f798ee87be71423bb099610cbd1ad01e8f229",
    "base/rust_core_3.json": "f79a6b99860560b23f0e0901b7ea839a037eacba1b2ab91aade322ff6b538d42",
    "base/realm-bundle.json": "e8d934e1c0c2764495dab1373ac7171776e289c5c4a1e9cd709ce2f3b35044e2",
    "base/kn-bundle.json": "26cec8cf51170dc34d27a4e0f996fd9c6ebf7412c53e6e7e89bc12f4c6099a07",
    "ingestor-okx-swap.json": "eb982facfccce78a825c835e5d7ecdebce5dba6e457252a946da69e808ed0460",
    "ingestor-binance-usdm.json": "5833bc4ea35cb4558a931288d34b61a94e99d260f1ba6b0e56596423995930ed",
    "authority.json": "33b3ff2509b30217598737002e8cec052d2abcf83170324997dd943a463c49de",
    "trading-system.paper.stable-r23-v3.json": "685fdf6f7d9faaa5e75e75d6f9621155b3f1bb26310b17611870155af843e997",
}
STAGE_A_RUNTIME_OUTPUTS = {
    "ingestor/ingestor-okx-swap.json": "d62f36b4927dbf2c7b5966654033dd320f8f130bafd608f2ea172e1ed988dbbb",
    "ingestor/ingestor-binance-usdm.json": "5833bc4ea35cb4558a931288d34b61a94e99d260f1ba6b0e56596423995930ed",
    "cores/rust_core.json": "22e139ffbaa2e49843867fcf93c79ae0232ba23f3171f08a4c5dd06026c3769c",
    "cores/rust_core_2.json": "26d6dad9ee9c3e5ecb2d35e66b52bc4d540cc11995411d62c9de3ba1e2416493",
    "cores/rust_core_3.json": "c962191bf8a2374f71662f512de007735871c378ed8e9b6c4697245ab9e1e4fa",
    "bundles/kn-bundle.json": "9136beba5280c6852240c8473dfb6976c3280b93f047d8c61125694c18fa1267",
    "bundles/realm-bundle.json": "1a80a9f42f7729964bc53124b267ee3ce7abc1030df99eb4ea1f56e3ef279ab4",
    "bindings/trading-system.paper.stable-r24.json":
        "0524d2000669541822266cd7ac7b000dd47ad422675ff75de7a034a45c8f8983",
    "runtime/stable-acquisition-bindings.yaml": "bf9fbad47242e62229f43ea1bc8445b21cee4b3eac7e55e401b39acb333c7659",
    "runtime/stable-source-bindings.yaml": "f51ef5c44e46e04a319f1e8e1e59362cee165abbf1da659fa427c811e57bd926",
    "consumer-realms/trading-system.paper.stable-r12.yaml":
        "9c21d9fb752d8dc26a5fad99f2b399a53c5b6f2005528752b20b905ec4e533c1",
}
RUNTIME_DIR = os.environ.get("QDL_V230_STAGE_A_RUNTIME_DIR")


def _sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _write_declaration(directory: Path, document: dict, name: str = "declaration.yaml") -> Path:
    path = directory / name
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


def _fixture(name: str) -> dict:
    document = yaml.safe_load((FIXTURES / name).read_text(encoding="utf-8"))
    document["base"]["repo"] = str(ROOT)

    def absolute(value):
        return str(FIXTURES / value)

    for consumer in document["consumers"]:
        consumer["ts_requirements"] = [absolute(v) for v in consumer.get("ts_requirements") or []]
        for group in consumer.get("instruments") or []:
            if "symbols_file" in group:
                group["symbols_file"] = absolute(group["symbols_file"])
        for universe in consumer.get("universes") or []:
            if "symbols_file" in universe:
                universe["symbols_file"] = absolute(universe["symbols_file"])
            hosting = universe.get("alpha_hosting")
            if hosting:
                for key in ("registry", "manifest", "universe"):
                    hosting[key] = absolute(hosting[key])
    return document


def _runtime_declaration(document: dict, runtime_dir: Path) -> dict:
    result = copy.deepcopy(document)
    result["base"]["runtime"] = {
        "authority": str(runtime_dir / "authority.json"),
        "ingestors": {name: str(runtime_dir / name) for name in ("ingestor-okx-swap.json", "ingestor-binance-usdm.json")},
        "cores": [str(runtime_dir / "base" / name) for name in ("rust_core.json", "rust_core_2.json", "rust_core_3.json")],
        "query_acquisition": str(ROOT / "upgrade/evidence/releases/v2.2.5/consumer-realms/"
                                 "stable-acquisition-bindings.runtime-r20.yaml"),
        "bundles": {"realm": str(runtime_dir / "base/realm-bundle.json"),
                    "kn": str(runtime_dir / "base/kn-bundle.json")},
        "route_bindings": {"trading-system.paper.stable": str(runtime_dir / "trading-system.paper.stable-r23-v3.json")},
    }
    return result


class StageARepositoryGoldenTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.tmp.name)
        cls.declaration = _write_declaration(cls.root, _fixture("stage_a.yaml"))
        cls.out = cls.root / "out"
        cls.report = tool.render(cls.declaration, cls.out)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_reproduces_the_stage_a_repository_files(self):
        for rel, digest in STAGE_A_REPO.items():
            self.assertEqual(_sha(self.out / "repo" / rel), digest, rel)
        self.assertEqual(self.report["blocked"], [])
        self.assertEqual(self.report["summary"]["catalog"]["revision"], [12, 13])
        self.assertEqual(self.report["summary"]["catalog"]["new_bindings"], 40)

    def test_never_writes_into_an_existing_directory(self):
        with self.assertRaisesRegex(tool.DeclarationRefused, "never overwrite"):
            tool.render(self.declaration, self.out)

    def test_render_is_idempotent_on_its_own_output(self):
        document = _fixture("stage_a.yaml")
        document["base"]["repo"] = str(self.out / "repo")
        again = tool.render(_write_declaration(self.root, document, "again.yaml"), self.root / "again")
        for rel in STAGE_A_REPO:
            self.assertEqual((self.root / "again/repo" / rel).read_bytes(), (self.out / "repo" / rel).read_bytes())
        self.assertEqual(again["summary"]["catalog"]["revision"], [13, 13])
        self.assertEqual(again["summary"]["demand"]["added_rows"], 0)
        self.assertFalse(again["summary"]["manifests"]["trading-system.paper.stable"]["changed"])

    def test_render_is_deterministic(self):
        second = self.root / "second"
        tool.render(self.declaration, second)
        for path in sorted(self.out.rglob("*")):
            if path.is_file() and path.name != "render-inputs.json":
                self.assertEqual(path.read_bytes(), (second / path.relative_to(self.out)).read_bytes(), path)

    def test_gate_passes_and_catches_tampering(self):
        result = tool.gate(self.out)
        self.assertEqual(result["status"], "PASS", result["failing"])
        tampered = self.root / "tampered"
        import shutil
        shutil.copytree(self.out, tampered)
        scope_path = tampered / "repo/config/v2/stable-authority-promotion-scope.yaml"
        scope = yaml.safe_load(scope_path.read_text(encoding="utf-8"))
        scope["binding_ids"] = scope["binding_ids"][1:]  # a removal the loaders accept
        scope_path.write_text(yaml.safe_dump(scope, sort_keys=False), encoding="utf-8")
        failing = tool.gate(tampered)["failing"]
        self.assertIn("declaration_index_matches_files", failing)
        self.assertIn("config_strictly_additive", failing)

    def test_checkpoint_migration_record_for_an_additive_crossing(self):
        record = tool.checkpoint_migration(
            previous_catalog=(ROOT / "config/v2/stable-source-bindings.yaml").read_bytes(),
            previous_acquisition=(ROOT / "config/v2/stable-acquisition-bindings.yaml").read_bytes(),
            catalog=self.out / "runtime/stable-source-bindings.yaml",
            acquisition=self.out / "repo/config/v2/stable-acquisition-bindings.yaml",
            reason="test", approved_by="test")
        self.assertEqual((record["from"], record["to"]),
                         ({"catalog_revision": 12, "acquisition_revision": 20},
                          {"catalog_revision": 13, "acquisition_revision": 21}))
        self.assertEqual((record["removed_binding_ids"], record["changed_binding_ids"]), ([], []))
        self.assertEqual(len(record["previous_binding_digests"]), 640)
        # a legacy (v4) checkpoint crosses only with the record
        legacy = {**record["from"], "binding_ids": sorted(record["previous_binding_digests"])}
        runtime = {**record["to"], "binding_ids": sorted(record["previous_binding_digests"])}
        digests = dict(record["previous_binding_digests"])
        with self.assertRaisesRegex(CheckpointCrossingRefused, "legacy checkpoint"):
            plan_crossing(checkpoint=legacy, runtime=runtime, current_digests=digests)
        self.assertTrue(plan_crossing(checkpoint=legacy, runtime=runtime, current_digests=digests,
                                      migration=record).migration_used)

    def test_checkpoint_migration_refuses_a_backwards_crossing(self):
        with self.assertRaisesRegex(CheckpointCrossingRefused, "forward declaration"):
            tool.checkpoint_migration(
                previous_catalog=(self.out / "runtime/stable-source-bindings.yaml").read_bytes(),
                previous_acquisition=(self.out / "repo/config/v2/stable-acquisition-bindings.yaml").read_bytes(),
                catalog=ROOT / "config/v2/stable-source-bindings.yaml",
                acquisition=ROOT / "config/v2/stable-acquisition-bindings.yaml", reason="t", approved_by="t")


class BlockedAndAdapterTests(unittest.TestCase):
    def _ts_document(self, rows):
        return {"schema": tool.TS_REQUIREMENTS_SCHEMA, "consumer_id": "trading-system.paper.stable",
                "declared_by": {"tool": "test"}, "policy": {}, "requirements": rows,
                "requirements_sha256": hashlib.sha256(
                    json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}

    def test_unresolved_rows_are_typed_blocked_never_dropped(self):
        rows = [
            {"venue": "OKX", "market": "SWAP", "native_symbol": "FET-USDT-SWAP", "feed": "QUOTE",
             "consumer_grade": "EXECUTION", "role": "READ", "purposes": ["MARKET"],
             "canonical_instrument_id": "OKX:SWAP:FET-USDT-SWAP"},
            {"venue": "OKX", "market": "SWAP", "native_symbol": "ARB-USDT-SWAP", "feed": "QUOTE",
             "consumer_grade": "EXECUTION", "role": "SUPPLY", "supplies": ["X"],
             "canonical_instrument_id": "OKX:SWAP:ARB-USDT-SWAP"},
            {"venue": "OKX", "market": "SWAP", "native_symbol": "ARB-USDT-SWAP", "feed": "TRADE",
             "consumer_grade": "EXECUTION", "role": "READ", "purposes": ["LAST"],
             "canonical_instrument_id": "OKX:SWAP:ARB-USDT-SWAP"},
            {"venue": "BINANCE", "market": "USD_M", "native_symbol": "ARBUSDT", "feed": "QUOTE",
             "consumer_grade": "EXECUTION", "role": "READ", "purposes": ["MARKET"],
             "canonical_instrument_id": "BINANCE:USD_M:ARBUSDT"},
            {"venue": "OKX", "market": "SWAP", "native_symbol": "ARB-USDT-SWAP", "feed": "QUOTE",
             "consumer_grade": "EXECUTION", "role": "READ", "purposes": ["MARKET"],
             "canonical_instrument_id": "OKX:SWAP:ARB-USDT-SWAP"},
        ]
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "ts.json").write_text(json.dumps(self._ts_document(rows)), encoding="utf-8")
            document = _fixture("stage_a.yaml")
            document["consumers"][0].update(instruments=[], ts_requirements=[str(root / "ts.json")])
            report = tool.render(_write_declaration(root, document), None, dry_run=True)
        reasons = {(r["native_symbol"], r["feed"]): r["reason"] for r in report["blocked"]}
        self.assertEqual(reasons[("FET-USDT-SWAP", "QUOTE")], "NOT_IN_CATALOG")
        self.assertEqual(reasons[("ARB-USDT-SWAP", "TRADE")], "FEED_NOT_DECLARABLE")
        self.assertEqual(reasons[("ARBUSDT", "QUOTE")], "EXECUTION_MARKET_NOT_DECLARABLE")
        self.assertIn("ROLE_NOT_A_DL_READ_GRANT", report["blocked_by_reason"])
        self.assertEqual(report["summary"]["catalog"]["new_bindings"], 1)  # only ARB QUOTE is granted
        self.assertEqual(report["summary"]["manifests"]["trading-system.paper.stable"]["requirements"], 66)

    def test_ts_requirements_with_a_wrong_digest_are_refused(self):
        document = self._ts_document([])
        document["requirements_sha256"] = "0" * 64
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "ts.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(tool.DeclarationRefused, "requirements_sha256"):
                tool.ts_requirement_rows(path, consumer_id="trading-system.paper.stable")

    def test_real_ts_gate_output_adapts_to_96_instruments(self):
        rows, blocked = tool.ts_requirement_rows(FIXTURES / "ts-declared-requirements-crossec96.json",
                                                 consumer_id="trading-system.paper.stable")
        self.assertEqual(blocked, [])
        self.assertEqual(len({r["native_symbol"] for r in rows}), 96)
        self.assertEqual({r["feed"] for r in rows}, {"QUOTE", "MARK_INDEX_PRICE"})

    def test_alpha_hosting_adapter(self):
        universe = tool.alpha_hosting_universe({
            "registry": FIXTURES / "alpha-data-requirements.yaml", "profile": "multi_symbol_bar",
            "manifest": FIXTURES / "crossec_ema_pfl.paper.manifest.json",
            "universe": FIXTURES / "crossec_ema_pfl.paper.universe.json"})
        self.assertEqual((universe["venue"], universe["market"]), ("OKX", "SWAP"))
        self.assertEqual(len(universe["symbols"]), 96)
        self.assertEqual(universe["intervals"], ["1h", "12h"])


class Okx150DryRenderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.report = tool.render(_write_declaration(Path(cls.tmp.name), _fixture("okx150.yaml")), None,
                                 dry_run=True)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_fet_and_luna_are_blocked_for_both_consumers(self):
        blocked = {(r["consumer_id"], r["native_symbol"]) for r in self.report["blocked"]
                   if r["reason"] == "NOT_IN_CATALOG"}
        for symbol in ("FET-USDT-SWAP", "LUNA-USDT-SWAP"):
            self.assertIn(("trading-system.paper.stable", symbol), blocked)
            self.assertIn(("alpha.okx.paper.stable", symbol), blocked)
        self.assertEqual(self.report["summary"]["universes"]["okx-swap-tier-e"]["members"], 148)

    def test_new_coverage_and_accept_set_floors(self):
        summary = self.report["summary"]
        self.assertEqual(summary["catalog"]["new_bindings_by_feed"]["QUOTE"], 143)
        self.assertEqual(summary["catalog"]["new_bindings_by_feed"]["MARK_INDEX_PRICE"], 143)
        manifests = summary["manifests"]
        self.assertEqual(manifests["trading-system.paper.stable"]["revision"], [11, 12])
        self.assertEqual(manifests["trading-system.paper.stable"]["min_token_revision"], 11)
        self.assertEqual(manifests["alpha.okx.paper.stable"]["revision"], [15, 16])
        self.assertEqual(manifests["alpha.okx.paper.stable"]["min_token_revision"], 15)
        self.assertTrue(all(not item["additive_problems"] for item in manifests.values()))

    def test_derived_intervals_follow_the_loader(self):
        derived_blocked = [r for r in self.report["blocked"] if r["reason"] == "DERIVED_FINAL_UNSUPPORTED_BY_LOADER"]
        if tool._derived_final_supported():
            self.assertEqual(derived_blocked, [])
            self.assertGreater(self.report["summary"]["derived_bindings"], 0)
        else:
            self.assertEqual(sorted(r["interval"] for r in derived_blocked),
                             sorted(["3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "2d", "3d", "1w"]))
            self.assertEqual(self.report["summary"]["catalog"]["new_bindings_by_feed"]["BAR"], 143)

    def test_capacity_is_reported_against_the_envelope(self):
        capacity = self.report["capacity"]
        self.assertEqual(capacity["mark_index_flush_ms"], 500)
        self.assertEqual(capacity["status"], "EXCEEDS_ENVELOPE")  # ~6.1 vCPU vs 5.0 (study section 5.2)
        self.assertGreater(capacity["projected_dl_vcpu"], 5.0)


@unittest.skipUnless(RUNTIME_DIR, "set QDL_V230_STAGE_A_RUNTIME_DIR to the deployed stage A base role files")
class StageARuntimeGoldenTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = Path(RUNTIME_DIR)
        for rel, digest in STAGE_A_RUNTIME_INPUTS.items():
            if _sha(cls.runtime / rel) != digest:
                raise unittest.SkipTest(f"runtime input {rel} is not the pinned stage A base")
        cls.tmp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.tmp.name)
        cls.out = cls.root / "out"
        document = _runtime_declaration(_fixture("stage_a.yaml"), cls.runtime)
        cls.report = tool.render(_write_declaration(cls.root, document), cls.out)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_reproduces_the_pinned_runtime_set(self):
        for rel, digest in STAGE_A_RUNTIME_OUTPUTS.items():
            self.assertEqual(_sha(self.out / rel), digest, rel)
        self.assertEqual(self.report["consumer_environment"]["trading-system.paper.stable"], {
            "DATA_LAYER_V2_JWT_MANIFEST_REVISION": "12",
            "DATA_LAYER_V2_RELEASE_MANIFEST_SHA256":
                "ca5b8477542984b89fa0bd4ddae4023b8b02f75cbd8f5f4af01192f06211f54f"})

    def test_execution_scope_matches_the_ts_reader_contract(self):
        scope = json.loads((self.out / "execution-scope/trading-system.paper.stable.json").read_text())
        self.assertEqual(scope["binding_digest"], STAGE_A_RUNTIME_OUTPUTS["bindings/trading-system.paper.stable-r24.json"])
        self.assertEqual((scope["manifest_revision"], scope["catalog_revision"]), (12, 13))
        keys = [(r["venue"], r["market"], r["native_symbol"]) for r in scope["instruments"]]
        self.assertEqual(keys, sorted(set(keys)))
        self.assertTrue(all(r["feeds"] == sorted(set(r["feeds"])) and set(r["feeds"]) <= set(tool.SCOPE_FEEDS)
                            for r in scope["instruments"]))
        self.assertEqual(len(scope["instruments"]), 31)
        index = json.loads((self.out / "runtime/declaration-index.json").read_text())
        self.assertIn("execution-scope/trading-system.paper.stable.json", index["files"])

    def test_gate_passes(self):
        result = tool.gate(self.out)
        self.assertEqual(result["status"], "PASS", result["failing"])

    def test_sticky_lanes_and_flush_map_on_150(self):
        document = _runtime_declaration(_fixture("okx150.yaml"), self.runtime)
        out = self.root / "out150"
        report = tool.render(_write_declaration(self.root, document, "okx150.yaml"), out)
        okx = report["summary"]["ingestors"]["ingestor-okx-swap.json"]
        self.assertEqual(okx["lane_plan"], "PINNED")
        for lane, feed in (("okx-public-001", "BOOK"), ("okx-public-002", "TRADE"), ("okx-public-003", "QUOTE"),
                           ("okx-public-004", "MARK_INDEX"), ("okx-business-001", "BAR")):
            self.assertEqual(okx["lanes"][lane]["feed"], feed)
        self.assertTrue(all(lane["members"] <= 100 for lane in okx["lanes"].values()))
        self.assertEqual(okx["latest_state_flush_ms_by_feed"], {"MARK_INDEX": 500})
        pinned = json.loads((out / "ingestor/ingestor-okx-swap.json").read_text())
        self.assertTrue(all(isinstance(b.get("lane_shard"), int) for b in pinned["bindings"]))
        gate = tool.gate(out)
        self.assertEqual(gate["failing"], ["capacity_envelope"])


if __name__ == "__main__":
    unittest.main()
