"""DL-DEF-2026-11-26-J: the alpha binding compiler honours consumer-manifest selector grants as the data plane does.

Fixtures:
- ``tests/fixtures/dl_def_j_crossec/crossec-rev6-data-requirements.json``: the hosted Crossec revision-6 inventory
  (``inventory_sha256`` 00c952f6...), 94 OKX swaps x (BAR 1h, BAR 12h, FUNDING_RATE).
- ``...deployed-binding.expectation.json``: digests of the deployed revision-6 binding (540c7d87...).
- The stage C r8 evidence (catalogue, ``alpha.okx.paper.stable`` r17 realm with the crossec-94 selectors, universes),
  already in the repository.
"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/compile_alpha_deployment_bindings.py"
FIXTURES = ROOT / "tests/fixtures/dl_def_j_crossec"
R8 = ROOT / "upgrade/evidence/releases/v2.3.1/declaration-stage-c-r8"
SEVEN = ("HYPE", "LIT", "ZRO", "MUBARAK", "GRASS", "USELESS", "CAP")
ADD7 = "okx-swap-crossec-owner50-add7"


def _module():
    spec = importlib.util.spec_from_file_location("alpha_deployment_bindings_def_j", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _load(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class CompilerSelectorGrantTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tool = _module()
        cls.catalog_path = R8 / "runtime/stable-source-bindings.yaml"
        cls.manifest_path = R8 / "consumer-realms/alpha.okx.paper.stable-r17.yaml"
        cls.universes = R8 / "runtime/universes"
        cls.reference_path = ROOT / "consumers/stable/reference-l2-stable.yaml"
        cls.release_path = ROOT / "config/v2/stable-v2-release-routing.yaml"
        cls.policy_path = ROOT / "config/v2/universal-release-policy.yaml"
        cls.inventory = json.loads((FIXTURES / "crossec-rev6-data-requirements.json").read_text(encoding="utf-8"))
        cls.expected = json.loads(
            (FIXTURES / "crossec-rev6-deployed-binding.expectation.json").read_text(encoding="utf-8")
        )
        cls.tmp = Path(tempfile.mkdtemp(prefix="dl-def-j-"))

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmp, ignore_errors=True)

    # helpers -------------------------------------------------------------------------------------------------
    def _compile(self, inventory, *, catalog_path: Path, manifests=None, reference_path: Path | None = None):
        reference_path = reference_path or self.reference_path
        return self.tool.compile_inventory(
            inventory=inventory,
            catalog=_load(catalog_path),
            reference_manifest=_load(reference_path),
            release_routing=_load(self.release_path),
            release_routing_sha256=hashlib.sha256(self.release_path.read_bytes()).hexdigest(),
            policy=_load(self.policy_path),
            policy_sha256=hashlib.sha256(self.policy_path.read_bytes()).hexdigest(),
            catalog_sha256=hashlib.sha256(catalog_path.read_bytes()).hexdigest(),
            reference_manifest_sha256=hashlib.sha256(reference_path.read_bytes()).hexdigest(),
            consumer_manifests=manifests,
        )

    def _manifests(self, manifest_path: Path, *, catalog_path: Path, universe_dir: Path):
        return self.tool.load_consumer_manifests([manifest_path], catalog_path=catalog_path, universe_dir=universe_dir)

    def _plus_seven_inventory(self):
        inventory = copy.deepcopy(self.inventory)
        deployment = inventory["deployments"][0]
        template = [row for row in deployment["routes"] if row["native_symbol"] == "AAVE-USDT-SWAP"]
        self.assertEqual(len(template), 3)
        for name in SEVEN:
            for row in template:
                added = copy.deepcopy(row)
                added["native_symbol"] = f"{name}-USDT-SWAP"
                deployment["routes"].append(added)
        inventory.pop("inventory_sha256")
        inventory["inventory_sha256"] = hashlib.sha256(_canonical(inventory)).hexdigest()
        return inventory

    def _bar_overlay_catalog(self) -> Path:
        """The r8 catalogue plus BAR 1h/12h rows for the 7, copied from STRK (stands in for an r11 render)."""
        catalog = _load(self.catalog_path)
        uid = {row["native_symbol"]: row["instrument_uid"] for row in catalog["instruments"] if row["venue"] == "OKX"}
        strk = [
            row for row in catalog["bindings"]
            if row["instrument_uid"] == uid["STRK-USDT-SWAP"] and row["feed"] == "BAR" and row["interval"] in ("1h", "12h")
        ]
        self.assertEqual(len(strk), 2)
        for name in SEVEN:
            low = f"{name}-usdt-swap".lower()
            for row in strk:
                added = copy.deepcopy(row)
                added["instrument_uid"] = uid[f"{name}-USDT-SWAP"]
                added["binding_id"] = row["binding_id"].replace("strk-usdt-swap", low)
                added["source"]["source_id"] = row["source"]["source_id"].replace("strk-usdt-swap", low)
                catalog["bindings"].append(added)
        path = self.tmp / "catalog-plus7-bars.yaml"
        path.write_text(yaml.safe_dump(catalog, sort_keys=False), encoding="utf-8")
        return path

    def _add7_grants(self) -> tuple[Path, Path]:
        """The r17 manifest plus the crossec-94 selectors repeated for a 7-member universe, and its universe dir."""
        universe_dir = self.tmp / "universes-add7"
        if not universe_dir.exists():
            shutil.copytree(self.universes, universe_dir)
            members = sorted(f"{name}-USDT-SWAP" for name in SEVEN)
            base = json.loads((self.universes / "okx-swap-crossec-94.json").read_text(encoding="utf-8"))
            base.update(
                universe_id=ADD7, members=members,
                membership_sha256=hashlib.sha256(json.dumps(members, separators=(",", ":")).encode()).hexdigest(),
            )
            (universe_dir / f"{ADD7}.json").write_text(json.dumps(base, sort_keys=True), encoding="utf-8")
        manifest = _load(self.manifest_path)
        added = [copy.deepcopy(item) for item in manifest["spec"]["selectors"] if item["universe"] == "okx-swap-crossec-94"]
        self.assertEqual(len(added), 5)
        for item in added:
            item["universe"] = ADD7
        manifest["spec"]["selectors"].extend(added)
        manifest["metadata"]["revision"] += 1
        path = self.tmp / "alpha.okx.paper.stable-add7.yaml"
        path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
        return path, universe_dir

    @staticmethod
    def _only(report):
        (deployment,) = report["deployments"]
        return deployment

    # tests ---------------------------------------------------------------------------------------------------
    def test_defect_reproduced_without_consumer_manifest(self) -> None:
        deployment = self._only(self._compile(self.inventory, catalog_path=self.catalog_path))
        self.assertEqual(deployment["status"], "BLOCKED")
        self.assertEqual(deployment["reason"], "CAPABILITY_UNAVAILABLE:FUNDING_RATE:crypto_liquid_v2")

    def test_revision6_compiles_to_the_deployed_binding_modulo_consumer_id(self) -> None:
        manifests = self._manifests(self.manifest_path, catalog_path=self.catalog_path, universe_dir=self.universes)
        report = self._compile(self.inventory, catalog_path=self.catalog_path, manifests=manifests)
        deployment = self._only(report)
        self.assertEqual(deployment["status"], "ADMITTED", deployment["reason"])
        binding = deployment["binding"]
        self.assertEqual(binding["consumer_id"], "alpha.okx.paper.stable")
        self.assertEqual({row["consumer_id"] for row in binding["products"]}, {"alpha.okx.paper.stable"})
        header = {k: v for k, v in binding.items() if k not in ("products", "binding_sha256", "consumer_id")}
        self.assertEqual(header, self.expected["header"])
        self.assertEqual(len(binding["products"]), self.expected["products"])
        stripped = sorted(
            ({k: v for k, v in row.items() if k != "consumer_id"} for row in binding["products"]),
            key=lambda row: row["requirement_id"],
        )
        self.assertEqual(hashlib.sha256(_canonical(stripped)).hexdigest(),
                         self.expected["products_without_consumer_id_sha256"])
        # Same bytes as the TS-side preview compile against the running r17 catalogue (2026-11-26).
        self.assertEqual(binding["binding_sha256"],
                         "bc956d8249e1bbc989678708165cc2159590a15105054a18527934db3254a90e")
        (summary,) = report["consumer_manifests"]
        self.assertEqual(summary["consumer_id"], "alpha.okx.paper.stable")
        self.assertIn("okx-swap-crossec-94", {item["universe_id"] for item in summary["selector_expansions"]})

    def test_plus_seven_compiles_without_the_workaround(self) -> None:
        catalog = self._bar_overlay_catalog()
        manifest, universes = self._add7_grants()
        manifests = self._manifests(manifest, catalog_path=catalog, universe_dir=universes)
        deployment = self._only(self._compile(self._plus_seven_inventory(), catalog_path=catalog, manifests=manifests))
        self.assertEqual(deployment["status"], "ADMITTED", deployment["reason"])
        products = deployment["binding"]["products"]
        self.assertEqual(len(products), 303)
        added = {(row["native_symbol"], row["feed"], row["interval"]) for row in products
                 if row["native_symbol"].split("-")[0] in SEVEN}
        self.assertEqual(added, {(f"{n}-USDT-SWAP", feed, interval) for n in SEVEN
                                 for feed, interval in (("BAR", "1h"), ("BAR", "12h"), ("FUNDING_RATE", None))})
        # Revision-6 products are untouched by the addition.
        baseline = self._only(self._compile(
            self.inventory, catalog_path=catalog, manifests=manifests))["binding"]["products"]
        self.assertTrue({_canonical(row) for row in baseline} <= {_canonical(row) for row in products})

    def test_plus_seven_on_the_real_catalogue_still_needs_bar_bindings(self) -> None:
        manifest, universes = self._add7_grants()
        manifests = self._manifests(manifest, catalog_path=self.catalog_path, universe_dir=universes)
        deployment = self._only(self._compile(
            self._plus_seven_inventory(), catalog_path=self.catalog_path, manifests=manifests))
        self.assertEqual(deployment["status"], "BLOCKED")
        self.assertEqual(deployment["reason"], "CAPABILITY_UNAVAILABLE:BAR:crypto_primary_v2")

    def test_bound_but_ungranted_route_is_blocked_as_the_data_plane_refuses_it(self) -> None:
        catalog = self._bar_overlay_catalog()
        manifests = self._manifests(self.manifest_path, catalog_path=catalog, universe_dir=self.universes)
        deployment = self._only(self._compile(self._plus_seven_inventory(), catalog_path=catalog, manifests=manifests))
        self.assertEqual(deployment["status"], "BLOCKED")
        self.assertTrue(deployment["reason"].startswith("CONSUMER_ENTITLEMENT_UNAVAILABLE:"), deployment["reason"])
        # The same row the data plane would refuse: load the runtime manifest and ask requirement_allowed's index.
        manifest = manifests["alpha.okx.paper.stable"]
        catalog_rows = _load(catalog)
        cap = next(row["instrument_uid"] for row in catalog_rows["instruments"] if row["native_symbol"] == "CAP-USDT-SWAP")
        self.assertFalse(any(item.instrument_uid == cap and item.interval == "1h" for item in manifest.requirements))

    def test_missing_consumer_manifest_for_the_subject_is_typed(self) -> None:
        other = ROOT / "consumers/stable/reference-l2-stable.yaml"
        manifests = self.tool.load_consumer_manifests([other], catalog_path=self.catalog_path, universe_dir=self.universes)
        deployment = self._only(self._compile(self.inventory, catalog_path=self.catalog_path, manifests=manifests))
        self.assertEqual(deployment["status"], "BLOCKED")
        self.assertEqual(deployment["reason"], "CONSUMER_MANIFEST_UNAVAILABLE:alpha.okx.paper.stable")

    def test_cli_accepts_consumer_manifest(self) -> None:
        out = self.tmp / "cli-out"
        rc = self.tool.main([
            "--inventory", str(FIXTURES / "crossec-rev6-data-requirements.json"),
            "--catalog", str(self.catalog_path),
            "--consumer-manifest", str(self.manifest_path),
            "--universe-dir", str(self.universes),
            "--output-dir", str(out), "--write",
        ])
        self.assertEqual(rc, 0)
        binding = json.loads((out / "crossec_ema_pfl_v1-paper-okx.binding.json").read_text(encoding="utf-8"))
        self.assertEqual(binding["binding_sha256"],
                         "bc956d8249e1bbc989678708165cc2159590a15105054a18527934db3254a90e")


if __name__ == "__main__":
    unittest.main()
