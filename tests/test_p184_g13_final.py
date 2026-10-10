"""Plan logic of scripts/p184_g13_final.py: selection, coverage, image-pin and deployment refusals, argv, cleanup."""
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout

from scripts import p184_g13_final as g13

PY = "sha256:" + "a1" * 32
RUST = "sha256:" + "b2" * 32
WRITER = "sha256:" + "c3" * 32
FINAL = {"python": PY, "rust": RUST, "python_ref": "qdl-v2-python:2.2.3-abcdef1", "writer": WRITER,
         "ts_sha": "1" * 39 + "2", "dl_sha": "3" * 39 + "4"}
PINS_OK = {"images": {"query": PY, "python": PY, "core": RUST, "stream": RUST, "kafka": "sha256:" + "d" * 64},
           "query_image_ref": FINAL["python_ref"], "query_image_digest": PY}
INPUTS = {"dr05_public_export": "/x/export.json", "redis_image": "redis:7.2-alpine", "postgres_image": "pg@sha256:1",
          "dr01": {"inventory": "/x/inv.json", "deployments": {"BINANCE": "db", "OKX": "do"}, "compose": "/x/c.json",
                   "alpha_service": "alpha"},
          "alpha": {"containers": {"BINANCE": "ab", "OKX": "ao"}, "symbols": {"BINANCE": "ETHUSDT",
                                                                              "OKX": "ETH-USDT-SWAP"}},
          "ts_container": "market_data_service_p184w",
          "dr07": {"connections": "/x/conn.json", "fx_account": "acct"},
          "_runtime_approval": {"path": "/x/packet.md", "sha256": "e" * 64}}
NOW = datetime(2026, 10, 10, 9, 0, 0, tzinfo=timezone.utc)


def make_plan(phase="all", only=(), inputs=None, pins=None):
    return g13.plan(FINAL, INPUTS if inputs is None else inputs, phase, only, Path("/ts"), Path("/state"), NOW,
                    PINS_OK if pins is None else pins)


class CoverageTests(unittest.TestCase):
    def test_every_dr_cell_has_one_producer_or_is_listed_open(self):
        cov = g13.coverage()
        self.assertEqual(cov["cells"], 65)  # frozen TS matrix p184-r2-case-matrix.v1: 65 DR cells
        self.assertEqual((cov["duplicates"], cov["missing"], cov["extra"]), ([], [], []))
        self.assertEqual(cov["produced"] + len(cov["no_producer"]), 65)
        self.assertEqual(cov["no_producer"], ["DR01.WRONG_ACCOUNT_DENIED@BINANCE", "DR01.WRONG_ACCOUNT_DENIED@OKX"])

    def test_every_dr_has_a_decision(self):
        self.assertEqual(sorted(g13.DECISIONS), sorted(g13.DR_CELLS))


class SelectionTests(unittest.TestCase):
    def test_isolated_runs_before_runtime_in_matrix_order(self):
        self.assertEqual(g13.select("all"), list(g13.STEPS))
        self.assertEqual(g13.select("isolated"), ["dr05-upstream", "dr08-isolated"])
        self.assertEqual(g13.select("all", ("dr07-reference", "dr08-isolated")), ["dr08-isolated", "dr07-reference"])

    def test_unknown_or_empty_selection_refused(self):
        with self.assertRaises(g13.Blocked):
            g13.select("all", ("dr09",))
        with self.assertRaises(g13.Blocked):
            g13.select("isolated", ("dr07-reference",))

    def test_inherited_dr_is_not_selected(self):
        original = g13.DECISIONS["DR07"]
        g13.DECISIONS["DR07"] = ("INHERIT", "test")
        try:
            self.assertNotIn("dr07-reference", g13.select("all"))
        finally:
            g13.DECISIONS["DR07"] = original


class FinalIdentityTests(unittest.TestCase):
    def test_development_zero_digests_and_shas_refused(self):
        for key, value in (("python", "sha256:" + "0" * 64), ("writer", "latest"), ("ts_sha", "0" * 40),
                           ("dl_sha", "abc"), ("rust", PY), ("python_ref", "2.2.3")):
            with self.subTest(key=key), self.assertRaises(g13.Blocked):
                g13.validate_final({**FINAL, key: value})
        g13.validate_final(FINAL)

    def test_harness_pins_read_without_import(self):
        source = ('QUERY_IMAGE_REF = "qdl-v2-python:2.2.3-rc2-43bbcf0"\nQUERY_IMAGE_DIGEST = "sha256:77"\n'
                  'IMAGES = {"query": QUERY_IMAGE_DIGEST, "core": "sha256:65", "stream": "sha256:1f",'
                  ' "python": "sha256:77"}\nimport not_installed_module\n')
        pins = g13.harness_pins(source)
        self.assertEqual(pins["images"]["query"], "sha256:77")
        self.assertEqual(pins["query_image_ref"], "qdl-v2-python:2.2.3-rc2-43bbcf0")
        self.assertEqual(len(g13.pin_mismatches(pins, FINAL)), 6)
        with self.assertRaises(g13.Blocked):
            g13.harness_pins("X = 1\n")

    def test_current_harness_pins_are_rc2_and_block_isolated_steps(self):
        pins = g13.harness_pins((g13.DL_ROOT / "scripts/p184_dr08_isolated.py").read_text())
        self.assertTrue(g13.pin_mismatches(pins, FINAL))
        result = make_plan("isolated", pins=pins)
        self.assertEqual(sorted(result["blocked"]), ["dr05-upstream", "dr08-isolated"])
        self.assertTrue(all("commands" not in e for e in result["steps"]))
        self.assertTrue(all(b[0].startswith("HARNESS_IMAGE_PIN") for b in result["blocked"].values()))

    def test_deployed_inventory_must_match_final(self):
        good = {g13.STACK + r + "-1": (PY if k == "python" else RUST) for r, k in g13.ROLE_IMAGE.items()}
        good[g13.WRITER] = WRITER
        self.assertEqual(g13.deployed_mismatches(good, FINAL), [])
        bad = {**good, g13.STACK + "rust_core_2-1": "sha256:" + "6" * 64, g13.WRITER: None}
        self.assertEqual(len(g13.deployed_mismatches(bad, FINAL)), 2)


class PlanTests(unittest.TestCase):
    def test_full_plan_binds_final_identity_everywhere(self):
        result = make_plan()
        self.assertEqual(result["blocked"], {})
        self.assertEqual(result["receipts"], "/state/p184-g13-20261010T090000Z")
        self.assertEqual(result["est_minutes_total"], sum(s[4] for s in g13.STEP_SPEC))
        steps = {e["step"]: e for e in result["steps"]}
        dr08 = steps["dr08-isolated"]["commands"][0]["argv"]
        self.assertNotIn("--development", dr08)
        self.assertNotIn("--stream-binary", dr08)
        self.assertEqual(dr08[dr08.index("--restart-samples") + 1], "5")
        self.assertEqual(dr08[dr08.index("--ts-image-digest") + 1], WRITER)
        upstream = steps["dr05-upstream"]["commands"]
        self.assertEqual([c["argv"][3] for c in upstream[:3] + upstream[4:]], ["--init", "--up", "--ready", "--down"])
        self.assertNotIn("--core-binary", sum((c["argv"] for c in upstream), []))
        adapter = upstream[3]["argv"]
        self.assertEqual(adapter[adapter.index("--image") + 1], WRITER)
        self.assertEqual(adapter[adapter.index("--source-sha") + 1], FINAL["ts_sha"])
        for step in ("dr02-dr03-alpha", "dr04-dr06-views", "dr05-transport", "dr07-reference"):
            argv = steps[step]["commands"][-1]["argv"]
            self.assertEqual(argv[argv.index("--image-digest") + 1], WRITER, step)
        dr01 = steps["dr01-deployed"]["commands"]
        self.assertEqual(len(dr01), 5)
        self.assertEqual(dr01[1]["cwd"], "/home/bobby/execution_alpha")
        self.assertNotIn("--ci-confirmed", dr01[-1]["argv"])
        self.assertTrue(all(c["argv"][-1] != "-" for c in dr01))
        for e in result["steps"]:
            for c in e["commands"]:
                outputs = [a for a in c["argv"] if a.startswith(result["receipts"])] + [c["stdout"] or ""]
                self.assertTrue(all(o == "" or o.startswith(result["receipts"] + "/" + e["step"]) for o in outputs))

    def test_runtime_requires_approval_and_inputs(self):
        inputs = {k: v for k, v in INPUTS.items() if k not in ("_runtime_approval", "dr07")}
        result = make_plan("runtime", inputs=inputs)
        self.assertEqual(sorted(result["blocked"]), sorted(s for s in g13.STEPS if s not in g13.select("isolated")))
        self.assertIn("MISSING_INPUTS: dr07.connections, dr07.fx_account", result["blocked"]["dr07-reference"])
        isolated = make_plan("isolated", inputs={k: v for k, v in INPUTS.items() if k != "_runtime_approval"})
        self.assertEqual(isolated["blocked"], {})

    def test_concatenation_targets_match_record_rows(self):
        steps = {e["step"]: e for e in make_plan()["steps"]}
        for step, (target, parts) in g13.CONCAT.items():
            commands = steps[step]["commands"]
            self.assertEqual([Path(c["stdout"]).name for c in commands[:2]], list(parts))
            record = commands[-1]["argv"]
            self.assertEqual(Path(record[record.index("--rows") + 1]).name, target)


class HelperTests(unittest.TestCase):
    def test_channel_argv_matches_joint_construction(self):
        def helper_command(ns, work, args, *, network):
            return ["docker", "run", "--rm", "--network", network, "--entrypoint", "python", "IMG", "-B",
                    "scripts/p184_dr05_dl_fault.py", *args]
        argv = g13.dr05_channel_argv(helper_command, "IMG", "p184-dr05-dl-0123456789abcdef", Path("/w"))
        self.assertEqual(argv[:3], ["docker", "run", "-i"])
        self.assertIn("scripts/p184_dr05_channel.py", argv)
        self.assertNotIn("scripts/p184_dr05_dl_fault.py", argv)
        self.assertEqual(argv[argv.index("IMG") - 2:argv.index("IMG")],
                         ["-e", "P184_DR05_ISOLATED_NAMESPACE=p184-dr05-dl-0123456789abcdef"])
        self.assertIn("container:p184-dr05-dl-0123456789abcdef-kafka", argv)

    def test_cleanup_only_new_owned_containers(self):
        before = {"1": "p184-dr05-old", "2": "qdl_v2_stable_candidate-rust_core-1"}
        after = {**before, "3": "p184-dr05-writer-x", "4": "unrelated", "5": "p184-dr08-y"}
        self.assertEqual(g13.new_owned(before, after, ("p184-dr05-",)), {"3": "p184-dr05-writer-x"})
        self.assertEqual(g13.new_owned(before, after, ()), {})

    def test_dry_run_prints_plan_and_creates_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = io.StringIO()
            with redirect_stdout(out):
                code = g13.main(["--python-image", PY, "--rust-image", RUST, "--python-image-ref",
                                 FINAL["python_ref"], "--writer-image", WRITER, "--ts-source-sha", FINAL["ts_sha"],
                                 "--dl-source-sha", FINAL["dl_sha"], "--phase", "isolated", "--receipts", tmp,
                                 "--dry-run"])
            result = json.loads(out.getvalue())
            self.assertEqual(code, 3)  # the checked-in harness still pins rc2 images
            self.assertEqual(sorted(result["blocked"]), ["dr05-upstream", "dr08-isolated"])
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_bad_identity_exits_before_plan(self):
        out = io.StringIO()
        with redirect_stdout(out):
            code = g13.main(["--python-image", "sha256:" + "0" * 64, "--rust-image", RUST, "--python-image-ref",
                             "x", "--writer-image", WRITER, "--ts-source-sha", FINAL["ts_sha"], "--dl-source-sha",
                             FINAL["dl_sha"], "--dry-run"])
        self.assertEqual(code, 2)
        self.assertIn("FINAL_DIGEST_REQUIRED", out.getvalue())


if __name__ == "__main__":
    unittest.main()
