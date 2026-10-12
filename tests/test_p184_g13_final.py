"""Plan logic of scripts/p184_g13_final.py: selection, coverage, image-pin and deployment refusals, argv, cleanup."""
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

from scripts import p184_g13_final as g13

PY = "sha256:" + "a1" * 32
RUST = "sha256:" + "b2" * 32
WRITER = "sha256:" + "c3" * 32
FINAL = {"python": PY, "rust": RUST, "python_ref": "qdl-v2-python:2.2.3-abcdef1", "writer": WRITER,
         "ts_sha": "1" * 39 + "2", "dl_sha": "3" * 39 + "4"}
TS_MATRIX_V2 = Path("/home/bobby/.worktrees/trading-system-next-upgrade") / g13.MATRIX_V2


def matrix_of(dr_cells):
    return {"schema": "p184.r2-case-matrix.v2", "sha256": "f" * 64,
            "cells": [{"id": c, "case": c.split(".")[0]} for c in dr_cells] + [{"id": "OR01.X@OKX", "case": "OR01"}]}


MATRIX_OK = matrix_of([c for case in g13.DR_CELLS.values() for c in case])
INPUTS = {"dr05_public_export": "/x/export.json", "dr05_catalog": "/x/catalog-r12.yaml", "redis_image": "redis:7.2-alpine", "postgres_image": "pg@sha256:1",
          "dr01": {"inventory": "/x/inv.json", "deployments": {"BINANCE": "db", "OKX": "do"}, "compose": "/x/c.json",
                   "alpha_service": {"BINANCE": "alpha_b", "OKX": "alpha_o"}, "manifest_dir": "/x/reader-public",
                   "manifest_revision": 15},
          "alpha": {"containers": {"BINANCE": "ab", "OKX": "ao"}, "symbols": {"BINANCE": "ETHUSDT",
                                                                              "OKX": "ETH-USDT-SWAP"}},
          "ts_container": "market_data_service_p184w",
          "gateway_container": "gateway_service", "gateway_image_digest": "sha256:" + "e" * 64,
          "dr01_wrong_account": {"run_id": "g13r3abc"},
          "dr07": {"connections": "/x/conn.json", "fx_account": "acct", "env_file": "/x/dsn.env",
                   "_connections": {v: {"cursor_file": f"/x/cursors/{v.lower()}.json",
                                        "tls_ca_file": f"/x/id/alpha-{v.lower()}/tls/ca.crt",
                                        "tls_certificate_file": f"/x/id/alpha-{v.lower()}/tls/client.crt",
                                        "tls_private_key_file": f"/x/id/alpha-{v.lower()}/tls/client.key",
                                        "jwt_private_key_file": f"/x/id/alpha-{v.lower()}/jwt/signing.key",
                                        "credential_file": "", "native_symbol": "S", "market": "M"}
                                    for v in ("BINANCE", "OKX")}},
          "_runtime_approval": {"path": "/x/packet.md", "sha256": "e" * 64}}
NOW = datetime(2026, 10, 10, 9, 0, 0, tzinfo=timezone.utc)


def make_plan(phase="all", only=(), inputs=None, matrix=None):
    return g13.plan(FINAL, INPUTS if inputs is None else inputs, phase, only, Path("/ts"), Path("/state"), NOW,
                    MATRIX_OK if matrix is None else matrix)


class CoverageTests(unittest.TestCase):
    def test_every_dr_cell_has_one_producer_or_is_listed_open(self):
        cov = g13.coverage()
        self.assertEqual(cov["cells"], 76)  # TS matrix v2: 65 DR cells of v1 + 11 OKX_INVERSE
        self.assertEqual((cov["duplicates"], cov["missing"], cov["extra"]), ([], [], []))
        self.assertEqual(cov["produced"] + len(cov["no_producer"]), 76)
        # Every cell has a producer: WRONG_ACCOUNT via the TS Gateway runner, inverse DR05 via the scoped harnesses.
        self.assertEqual(cov["no_producer"], [])
        self.assertEqual(cov["produced"], 76)
        self.assertEqual(len(g13.INV_DR04 + g13.INV_DR05 + g13.INV_DR06), 11)

    def test_dr_cells_equal_the_committed_ts_matrix_v2(self):
        if not TS_MATRIX_V2.is_file():
            self.skipTest("TS worktree not mounted")
        matrix = json.loads(TS_MATRIX_V2.read_text())
        self.assertEqual(matrix["schema"], "p184.r2-case-matrix.v2")
        self.assertEqual(g13.matrix_mismatch(matrix), {})

    def test_matrix_mismatch_blocks_every_step(self):
        drifted = matrix_of([c for case in g13.DR_CELLS.values() for c in case if c not in g13.INV_DR04]
                            + ["DR09.NEW@ANY"])
        result = make_plan("all", matrix=drifted)
        self.assertEqual(sorted(result["blocked"]), sorted(g13.select("all")))
        self.assertTrue(all(b[0].startswith("CASE_MATRIX_MISMATCH") for b in result["blocked"].values()))
        with self.assertRaises(g13.Blocked):
            g13.matrix_dr_cells({"schema": "p184.r2-case-matrix.v9", "cells": []})

    def test_every_dr_has_a_decision(self):
        self.assertEqual(sorted(g13.DECISIONS), sorted(g13.DR_CELLS))


class SelectionTests(unittest.TestCase):
    def test_isolated_runs_before_runtime_in_matrix_order(self):
        self.assertEqual(g13.select("all"), list(g13.STEPS))
        self.assertEqual(g13.select("isolated"), ["dr05-upstream", "dr08-isolated"])
        self.assertIn("dr01-wrong-account", g13.select("runtime"))
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

    def test_harness_images_are_the_final_images_not_the_rc2_pins(self):
        args = g13.harness_image_args(FINAL)
        pairs = dict(a.split("=", 1) for a in args[1::2])
        self.assertEqual(args[0::2], ["--image"] * 4)
        self.assertEqual(pairs, {"query": PY, "python": PY, "core": RUST, "stream": RUST})
        result = make_plan("isolated")
        self.assertEqual(result["blocked"], {})
        steps = {e["step"]: e for e in result["steps"]}
        init = steps["dr05-upstream"]["commands"][0]["argv"]
        dr08 = steps["dr08-isolated"]["commands"][0]["argv"]
        for argv in (init, dr08):
            self.assertIn("python=" + PY, argv)
            self.assertIn("core=" + RUST, argv)
        self.assertEqual(dr08[dr08.index("--query-image-ref") + 1], FINAL["python_ref"])
        self.assertNotIn("--image", steps["dr05-upstream"]["commands"][1]["argv"])  # --up reads handoff.json

    def test_harness_clis_accept_the_runner_flags(self):
        from scripts import p184_dr05_dl_fault as dr, p184_dr08_isolated as r
        self.assertEqual(r.parse_image_overrides(g13.harness_image_args(FINAL)[1::2]),
                         {"query": PY, "python": PY, "core": RUST, "stream": RUST})
        self.assertEqual(dr.resolve_images(r.parse_image_overrides(g13.harness_image_args(FINAL)[1::2]))["core"], RUST)

    def test_deployed_inventory_must_match_final(self):
        good = deployed({r: (PY if k == "python" else RUST) for r, k in g13.ROLE_IMAGE.items()})
        self.assertEqual(g13.deployed_mismatches(good, FINAL), [])
        bad = {**good, g13.STACK + "rust_core_2-1": running("sha256:" + "6" * 64), g13.WRITER: None}
        self.assertEqual(len(g13.deployed_mismatches(bad, FINAL)), 2)


# The planned mixed rollout: query python rc15, bar edge python rc14, rust_core rc16, other rust roles rc14.
Q15 = "sha256:" + "a2201057" * 8
P14 = "sha256:" + "08f356a8" * 8
C16 = "sha256:" + "c16c16c1" * 8
R14 = "sha256:" + "ce629061" * 8
MIXED = {"query_kn_1": Q15, "query_kn_2": Q15, "binance_bar_edge": P14,
         "rust_core": C16, "rust_core_2": C16, "rust_core_3": C16,
         "ingestor_binance_usdm": R14, "ingestor_okx_swap": R14, "market_projector_1": R14,
         "market_projector_2": R14, "stream_kn_1": R14, "stream_kn_2": R14}
FINAL_MIXED = {"roles": MIXED, "python_ref": "qdl-v2-python:2.2.3-rc15-x", "writer": WRITER,
               "ts_sha": FINAL["ts_sha"], "dl_sha": FINAL["dl_sha"]}
INFRA_IMAGE = "sha256:" + "9" * 63 + "8"
INIT_ROLES = ("stable_tls_init", "stable_state_init")  # one-shot init containers: exited 0


def running(image):
    return {"image": image, "status": "running", "running": True, "exit_code": 0, "oom_killed": False,
            "restart_count": 0}


def exited(image, code=0):
    return {"image": image, "status": "exited", "running": False, "exit_code": code, "oom_killed": False,
            "restart_count": 0}


INFRA = {g13.STACK + r + "-1": (exited if r in INIT_ROLES else running)(INFRA_IMAGE)
         for r in g13.INFRA_ROLES}


def deployed(roles, **extra):
    inv = {g13.STACK + r + "-1": running(d) for r, d in roles.items()}
    inv.update(INFRA)
    inv[g13.WRITER] = running(WRITER)
    inv.update({k: running(v) if isinstance(v, str) else v for k, v in extra.items()})
    return inv


class MixedImageTests(unittest.TestCase):
    """B1: roles run different images after a partial rollout; the expected map is per role, fail-closed."""

    def test_exact_mixed_match_has_no_mismatch(self):
        g13.validate_final(FINAL_MIXED)
        self.assertEqual(g13.deployed_mismatches(deployed(MIXED), FINAL_MIXED), [])

    def test_one_role_off_is_refused(self):
        for role, other in (("binance_bar_edge", Q15), ("rust_core_2", R14), ("query_kn_2", P14)):
            with self.subTest(role=role):
                found = g13.deployed_mismatches(deployed({**MIXED, role: other}), FINAL_MIXED)
                self.assertEqual(len(found), 1)
                self.assertIn(g13.STACK + role + "-1", found[0])

    def test_absent_role_is_refused(self):
        inv = deployed(MIXED)
        del inv[g13.STACK + "stream_kn_2-1"]
        self.assertEqual(len(g13.deployed_mismatches(inv, FINAL_MIXED)), 1)

    def test_unlisted_deployed_role_is_refused(self):
        found = g13.deployed_mismatches(deployed(MIXED, **{g13.STACK + "query_kn_3-1": Q15}), FINAL_MIXED)
        self.assertEqual(len(found), 1)
        self.assertTrue(found[0].startswith("UNLISTED_ROLE: " + g13.STACK + "query_kn_3-1"))
        # containers outside the stack prefix (alphas, gateway) are not the DL stack
        self.assertEqual(g13.deployed_mismatches(deployed(MIXED, other_container=Q15), FINAL_MIXED), [])

    def test_stopped_mapped_role_with_the_right_image_is_refused(self):
        """G13-R3: the image matches but the container is stopped/paused/restarting/OOM-killed."""
        name = g13.STACK + "rust_core_2-1"
        for record, reason in ((exited(C16), "CONTAINER_NOT_RUNNING"), (exited(C16, 137), "CONTAINER_NOT_RUNNING"),
                               ({**running(C16), "status": "paused"}, "CONTAINER_NOT_RUNNING"),
                               ({**running(C16), "status": "restarting"}, "CONTAINER_NOT_RUNNING"),
                               ({**running(C16), "oom_killed": True}, "CONTAINER_OOM_KILLED")):
            with self.subTest(record=record):
                found = g13.deployed_mismatches(deployed(MIXED, **{name: record}), FINAL_MIXED)
                self.assertEqual(len(found), 1, found)
                self.assertTrue(found[0].startswith(reason + ": " + name), found)
        # restart count is recorded, not refused
        self.assertEqual(g13.deployed_mismatches(
            deployed(MIXED, **{name: {**running(C16), "restart_count": 3}}), FINAL_MIXED), [])

    def test_infra_must_run_and_init_one_shots_must_have_exited_zero(self):
        for role in ("kafka1", "kafka2", "kafka3", "market_cache", "stable_redis"):
            name = g13.STACK + role + "-1"
            with self.subTest(role=role):
                found = g13.deployed_mismatches(deployed(MIXED, **{name: exited(INFRA_IMAGE)}), FINAL_MIXED)
                self.assertEqual([f.split(":")[0] for f in found], ["CONTAINER_NOT_RUNNING"])
                inv = deployed(MIXED)
                del inv[name]
                self.assertEqual([f.split(":")[0] for f in g13.deployed_mismatches(inv, FINAL_MIXED)],
                                 ["CONTAINER_ABSENT"])
        for role in INIT_ROLES:
            name = g13.STACK + role + "-1"
            for record in (exited(INFRA_IMAGE, 1), running(INFRA_IMAGE), {**exited(INFRA_IMAGE), "status": "created"}):
                with self.subTest(role=role, record=record):
                    found = g13.deployed_mismatches(deployed(MIXED, **{name: record}), FINAL_MIXED)
                    self.assertEqual([f.split(":")[0] for f in found], ["INIT_NOT_COMPLETED"])

    def test_stopped_writer_is_refused(self):
        found = g13.deployed_mismatches(deployed(MIXED, **{g13.WRITER: exited(WRITER)}), FINAL_MIXED)
        self.assertEqual(found, [f"CONTAINER_NOT_RUNNING: {g13.WRITER} status exited exit 0"])

    def test_inventory_inspects_state_not_only_image(self):
        """G13-R3: deployed_inventory reads State from docker inspect; a stopped role surfaces as a mismatch."""
        from unittest import mock

        def row(name):
            role = name[len(g13.STACK):-2] if name.startswith(g13.STACK) else None
            image = MIXED.get(role, INFRA_IMAGE) if role else WRITER
            stopped = role == "query_kn_2" or role in INIT_ROLES
            return {"Image": image, "RestartCount": 0,
                    "State": {"Status": "exited" if stopped else "running", "Running": not stopped,
                              "ExitCode": 0, "OOMKilled": False}}

        def fake_sh(argv, timeout=60):
            if argv[1] == "ps":
                return "\n".join(g13.STACK + r + "-1" for r in (*MIXED, *g13.INFRA_ROLES))
            self.assertEqual(argv[:2], ["docker", "inspect"])
            return json.dumps([row(argv[-1])])

        with mock.patch.object(g13, "sh", fake_sh):
            inv = g13.deployed_inventory()
        self.assertEqual(inv[g13.STACK + "query_kn_2-1"]["status"], "exited")
        self.assertEqual(g13.deployed_mismatches(inv, FINAL_MIXED),
                         [f"CONTAINER_NOT_RUNNING: {g13.STACK}query_kn_2-1 status exited exit 0"])

    def test_role_map_missing_or_unknown_role_is_refused(self):
        partial = {k: v for k, v in MIXED.items() if k != "binance_bar_edge"}
        with self.assertRaisesRegex(g13.Blocked, "ROLE_MAP_INCOMPLETE"):
            g13.validate_final({**FINAL_MIXED, "roles": partial})
        with self.assertRaisesRegex(g13.Blocked, "ROLE_UNKNOWN"):
            g13.resolve_final_roles(pairs=[f"{r}={d}" for r, d in MIXED.items()] + ["query_kn_3=" + Q15])
        with self.assertRaisesRegex(g13.Blocked, "ROLE_IMAGES_DUPLICATE"):
            g13.resolve_final_roles(pairs=["rust_core=" + C16], file_map=MIXED)
        with self.assertRaisesRegex(g13.Blocked, "ROLE_IMAGES_AMBIGUOUS"):
            g13.resolve_final_roles(PY, RUST, file_map=MIXED)
        with self.assertRaisesRegex(g13.Blocked, "FINAL_DIGEST_REQUIRED"):
            g13.resolve_final_roles(PY, None)
        with self.assertRaisesRegex(g13.Blocked, "FINAL_DIGEST_REQUIRED"):
            g13.validate_final({**FINAL_MIXED, "roles": {**MIXED, "rust_core": Q15}})  # python image on a rust role

    def test_harness_images_follow_their_roles(self):
        self.assertEqual(g13.harness_images(FINAL_MIXED), {"query": Q15, "python": Q15, "core": C16, "stream": R14})
        steps = {e["step"]: e for e in g13.plan(FINAL_MIXED, INPUTS, "all", (), Path("/ts"), Path("/state"), NOW,
                                                 MATRIX_OK)["steps"]}
        dr08 = steps["dr08-isolated"]["commands"][0]["argv"]
        for pair in ("query=" + Q15, "python=" + Q15, "core=" + C16, "stream=" + R14):
            self.assertIn(pair, dr08)
        prepare = steps["dr01-deployed"]["commands"][0]["argv"]
        self.assertEqual(prepare[prepare.index("--entrypoint") + 2], Q15)
        with self.assertRaisesRegex(g13.Blocked, "HARNESS_IMAGE_AMBIGUOUS"):
            g13.validate_final({**FINAL_MIXED, "roles": {**MIXED, "rust_core_3": R14}})

    def test_receipt_records_the_exact_role_map(self):
        result = g13.plan(FINAL_MIXED, INPUTS, "isolated", (), Path("/ts"), Path("/state"), NOW, MATRIX_OK)
        self.assertEqual(result["final"]["roles"], MIXED)
        self.assertEqual(result["final"]["harness_images"]["core"], C16)
        legacy = make_plan("isolated")["final"]
        self.assertEqual(legacy["roles"], {r: PY if k == "python" else RUST for r, k in g13.ROLE_IMAGE.items()})
        self.assertNotIn("python", legacy)

    def test_legacy_flags_expand_to_the_same_map(self):
        self.assertEqual(g13.resolve_final_roles(PY, RUST), g13.role_images(FINAL))
        self.assertEqual(g13.resolve_final_roles(pairs=[f"{r}={d}" for r, d in MIXED.items()]), MIXED)
        self.assertEqual(g13.resolve_final_roles(file_map=MIXED), MIXED)

    def test_query_ref_must_resolve_to_the_query_image(self):
        self.assertIsNone(g13.image_ref_problem("qdl-v2-python:x", Q15, Q15))
        self.assertIn("QUERY_IMAGE_REF_MISMATCH", g13.image_ref_problem("qdl-v2-python:x", Q15, P14))
        self.assertIn("QUERY_IMAGE_REF_MISMATCH", g13.image_ref_problem("qdl-v2-python:x", Q15, None))

    def test_cli_role_map_dry_run(self):
        if not TS_MATRIX_V2.is_file():
            self.skipTest("TS worktree not mounted")
        with tempfile.TemporaryDirectory() as tmp:
            roles = Path(tmp) / "roles.json"
            roles.write_text(json.dumps(MIXED))
            receipts = Path(tmp) / "r"
            receipts.mkdir()
            base = ["--python-image-ref", FINAL_MIXED["python_ref"], "--writer-image", WRITER, "--ts-source-sha",
                    FINAL["ts_sha"], "--dl-source-sha", FINAL["dl_sha"], "--phase", "isolated", "--receipts",
                    str(receipts), "--only", "dr08-isolated", "--dry-run", "--ts-root", str(TS_MATRIX_V2.parents[4])]
            for flags in (["--role-images", str(roles)], [a for r, d in MIXED.items() for a in ("--role-image",
                                                                                                f"{r}={d}")]):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = g13.main(flags + base)
                with self.subTest(flags=flags[0]):
                    self.assertEqual(code, 0, out.getvalue())
                    self.assertEqual(json.loads(out.getvalue())["final"]["roles"], MIXED)
            out = io.StringIO()
            with redirect_stdout(out):
                code = g13.main(["--role-images", str(roles), "--python-image", PY] + base)
            self.assertEqual(code, 2)
            self.assertIn("ROLE_IMAGES_AMBIGUOUS", out.getvalue())
            self.assertEqual(list(receipts.iterdir()), [])


class PerVenueRevisionTests(unittest.TestCase):
    """B2: Binance and OKX alphas may run different JWT manifest revisions."""

    @staticmethod
    def prepares(dr01):
        inputs = copy_inputs()
        inputs["dr01"].update(dr01)
        entry = runtime_steps(inputs)["dr01-deployed"]
        if "commands" not in entry:
            return entry["blocked"], None
        out = {}
        for c in entry["commands"]:
            argv = c["argv"]
            if "prepare" in argv:
                tail = argv[argv.index("prepare"):]
                venue = tail[tail.index("--venue") + 1]
                out[venue] = {f: tail[tail.index(f) + 1] if f in tail else None
                              for f in ("--manifest-revision", "--wrong-revision", "--manifest-dir")}
                out[venue]["mounts"] = [argv[i + 1] for i, a in enumerate(argv) if a == "-v"]
        return out, entry

    def test_scalar_applies_to_both_venues(self):
        out, entry = self.prepares({"manifest_revision": 15})
        self.assertEqual({v: o["--manifest-revision"] for v, o in out.items()}, {"BINANCE": "15", "OKX": "15"})
        self.assertEqual({v: o["--wrong-revision"] for v, o in out.items()}, {"BINANCE": None, "OKX": None})
        self.assertEqual(entry["manifest_revisions"]["OKX"], {"manifest_revision": 15, "wrong_revision": None})

    def test_map_gives_each_venue_its_revision(self):
        out, entry = self.prepares({"manifest_revision": {"BINANCE": 16, "OKX": 15},
                                    "manifest_dir": "/x/reader-public-r16"})
        self.assertEqual({v: o["--manifest-revision"] for v, o in out.items()}, {"BINANCE": "16", "OKX": "15"})
        self.assertEqual({v: o["--manifest-dir"] for v, o in out.items()},
                         {"BINANCE": "/x/reader-public-r16", "OKX": "/x/reader-public-r16"})
        self.assertEqual(entry["manifest_revisions"], {"BINANCE": {"manifest_revision": 16, "wrong_revision": None},
                                                       "OKX": {"manifest_revision": 15, "wrong_revision": None}})

    def test_wrong_revision_scalar_and_map(self):
        out, _ = self.prepares({"manifest_revision": {"BINANCE": 16, "OKX": 15}, "wrong_revision": 14})
        self.assertEqual({v: o["--wrong-revision"] for v, o in out.items()}, {"BINANCE": "14", "OKX": "14"})
        out, _ = self.prepares({"manifest_revision": {"BINANCE": 16, "OKX": 15},
                                "wrong_revision": {"BINANCE": 15, "OKX": 14}})
        self.assertEqual({v: o["--wrong-revision"] for v, o in out.items()}, {"BINANCE": "15", "OKX": "14"})
        # a scalar wrong revision equal to one venue's revision is refused before any container
        blocked, entry = self.prepares({"manifest_revision": {"BINANCE": 16, "OKX": 15}, "wrong_revision": 15})
        self.assertIsNone(entry)
        self.assertTrue(any("DR01_REVISION_INVALID" in b for b in blocked), blocked)

    def test_per_venue_manifest_dir_is_mounted_per_venue(self):
        out, _ = self.prepares({"manifest_revision": {"BINANCE": 16, "OKX": 15},
                                "manifest_dir": {"BINANCE": "/x/realm-b", "OKX": "/x/realm-o"}})
        self.assertEqual(out["BINANCE"]["--manifest-dir"], "/x/realm-b")
        self.assertIn("/x/realm-b:/x/realm-b:ro", out["BINANCE"]["mounts"])
        self.assertNotIn("/x/realm-o:/x/realm-o:ro", out["BINANCE"]["mounts"])
        self.assertIn("/x/realm-o:/x/realm-o:ro", out["OKX"]["mounts"])

    def test_bad_revision_forms_refused(self):
        for value in ({"BINANCE": 16}, {"BINANCE": 16, "OKX": 15, "X": 1}, 0, True, "r16", {"BINANCE": 16,
                                                                                           "OKX": None}):
            with self.subTest(value=value):
                blocked, entry = self.prepares({"manifest_revision": value})
                self.assertIsNone(entry)
                self.assertTrue(any("PER_VENUE_INVALID" in b or "DR01_REVISION_INVALID" in b for b in blocked),
                                blocked)

    def test_harness_accepts_each_venue_argv(self):
        from scripts import p184_dr01_deployed as dr01
        self.assertEqual(dr01.revisions(16), (16, 15))
        self.assertEqual(dr01.revisions(15, 14), (15, 14))

    def test_dr07_connections_revision_must_match_dr01_per_venue(self):
        inputs = copy_inputs()
        inputs["dr01"]["manifest_revision"] = {"BINANCE": 16, "OKX": 15}
        inputs["dr07"]["_connections"]["BINANCE"]["jwt_manifest_revision"] = 16
        inputs["dr07"]["_connections"]["OKX"]["jwt_manifest_revision"] = 15
        self.assertIn("commands", runtime_steps(inputs)["dr07-reference"])
        inputs["dr07"]["_connections"]["OKX"]["jwt_manifest_revision"] = 16
        entry = runtime_steps(inputs)["dr07-reference"]
        self.assertNotIn("commands", entry)
        self.assertTrue(any("DR07_REVISION_MISMATCH" in b for b in entry["blocked"]), entry["blocked"])


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
        prepare = dr01[0]["argv"]
        self.assertEqual(prepare[prepare.index("--manifest-dir") + 1], "/x/reader-public")
        self.assertEqual(prepare[prepare.index("--manifest-revision") + 1], "15")
        self.assertNotIn("--wrong-revision", prepare)
        inputs = json.loads(json.dumps(INPUTS)); inputs["dr01"]["wrong_revision"] = 13
        steps = {e["step"]: e for e in make_plan("runtime", inputs=inputs)["steps"]}
        prepare = steps["dr01-deployed"]["commands"][0]["argv"]
        self.assertEqual(prepare[prepare.index("--wrong-revision") + 1], "13")
        del inputs["dr01"]["manifest_revision"]
        self.assertIn("dr01.manifest_revision", g13.missing_inputs(["dr01-deployed"], inputs)["dr01-deployed"])
        for e in result["steps"]:
            for c in e["commands"]:
                # the sealed DL source export lives under the receipts root but is a read-only input (G13-U1)
                outputs = [a for a in c["argv"] if a.startswith(result["receipts"])
                           and not a.startswith(result["dl_source_dir"])] + [c["stdout"] or ""]
                self.assertTrue(all(o == "" or o.startswith(result["receipts"] + "/" + e["step"]) for o in outputs))

    def test_runtime_requires_approval_and_inputs(self):
        inputs = {k: v for k, v in INPUTS.items() if k not in ("_runtime_approval", "dr07")}
        result = make_plan("runtime", inputs=inputs)
        self.assertEqual(sorted(result["blocked"]), sorted(s for s in g13.STEPS if s not in g13.select("isolated")))
        self.assertIn("MISSING_INPUTS: dr07.connections, dr07.fx_account, dr07.env_file",
                      result["blocked"]["dr07-reference"])
        isolated = make_plan("isolated", inputs={k: v for k, v in INPUTS.items() if k != "_runtime_approval"})
        self.assertEqual(isolated["blocked"], {})

    def test_concatenation_targets_match_record_rows(self):
        steps = {e["step"]: e for e in make_plan()["steps"]}
        for step, (target, parts) in g13.CONCAT.items():
            commands = steps[step]["commands"]
            self.assertEqual([Path(c["stdout"]).name for c in commands[:len(parts)]], list(parts))
            record = commands[-1]["argv"]
            self.assertEqual(Path(record[record.index("--rows") + 1]).name, target)


def runtime_steps(inputs=None):
    return {e["step"]: e for e in make_plan("runtime", inputs=inputs)["steps"]}


def copy_inputs():
    return json.loads(json.dumps(INPUTS))


class Dr01PerVenueServiceTests(unittest.TestCase):
    """B5: the probe validates the alpha identity per venue, so each venue probes in its own alpha service."""

    def test_each_venue_probes_in_its_own_service(self):
        dr01 = runtime_steps()["dr01-deployed"]["commands"]
        probes = [c["argv"] for c in dr01 if c["argv"][:2] == ["docker", "compose"]]
        self.assertEqual(len(probes), 2)
        for argv, service, venue in zip(probes, ("alpha_b", "alpha_o"), ("binance", "okx")):
            self.assertEqual(argv[argv.index("-B") - 1], service)
            self.assertIn(f"dr01-{venue}-packet.json:/run/p184/dr01-packet.json:ro", argv[argv.index("-v") + 1])

    def test_single_or_shared_service_refused(self):
        for value in ("alpha", {"BINANCE": "same", "OKX": "same"}, {"BINANCE": "a", "OKX": "b", "X": "c"},
                      {"BINANCE": "a b", "OKX": "b"}):
            inputs = copy_inputs()
            inputs["dr01"]["alpha_service"] = value
            with self.subTest(value=value):
                blocked = make_plan("runtime", inputs=inputs)["blocked"]["dr01-deployed"]
                self.assertTrue(any("DR01_ALPHA_SERVICE_PER_VENUE" in b or "MISSING_INPUTS: dr01.alpha_service" in b
                                    for b in blocked), blocked)
                self.assertNotIn("commands", runtime_steps(inputs)["dr01-deployed"])


class ContainerStepTests(unittest.TestCase):
    """B6: dr01 prepare and dr07 need qdl/redis/asyncpg/grpc, container DNS and the alpha identity dirs."""

    @staticmethod
    def mounts(argv):
        return [argv[i + 1] for i, a in enumerate(argv) if a == "-v"]

    def test_dr01_prepare_runs_offline_in_the_final_python_image(self):
        work = "/state/p184-g13-20261010T090000Z/dr01-deployed"
        dr01 = runtime_steps()["dr01-deployed"]["commands"]
        for venue, c in zip(("binance", "okx"), (dr01[0], dr01[2])):
            argv = c["argv"]
            self.assertEqual(argv[:3], ["docker", "run", "--rm"])
            self.assertIn("--pull=never", argv)
            self.assertEqual(argv[argv.index("--name") + 1], f"p184-g13-dr01-prepare-{venue}")
            self.assertEqual(argv[argv.index("--network") + 1], "none")
            self.assertEqual(argv[argv.index("--user") + 1], f"{g13.os.getuid()}:{g13.os.getgid()}")
            self.assertIn("--read-only", argv)
            self.assertEqual(argv[argv.index("--cap-drop") + 1], "ALL")
            self.assertNotIn("--cap-add", argv)
            self.assertEqual(argv[argv.index("--entrypoint") + 1:argv.index("--entrypoint") + 4], ["python", PY, "-B"])
            dl = "/state/p184-g13-20261010T090000Z/dl-source"  # the sealed export, never the worktree (G13-U1)
            self.assertEqual(self.mounts(argv), [f"{dl}:{dl}:ro", "/x/inv.json:/x/inv.json:ro",
                                                 "/x/reader-public:/x/reader-public:ro", f"{work}:{work}"])
            self.assertEqual(argv[argv.index("-w") + 1], dl)
            prepare = argv[argv.index("-B") + 1:]
            self.assertEqual(prepare[:2], ["scripts/p184_dr01_deployed.py", "prepare"])
            self.assertEqual(prepare[prepare.index("--output") + 1], f"{work}/dr01-{venue}-packet.json")
        self.assertTrue(g13.STEP_SPEC[2][5][0].startswith("p184-g13-dr01-"))

    def test_dr01_catalog_is_mounted_when_declared(self):
        inputs = copy_inputs()
        inputs["dr01"]["catalog"] = "/x/sealed/active-catalog.yaml"
        argv = runtime_steps(inputs)["dr01-deployed"]["commands"][0]["argv"]
        self.assertIn("/x/sealed/active-catalog.yaml:/x/sealed/active-catalog.yaml:ro", self.mounts(argv))
        self.assertEqual(argv[argv.index("--catalog") + 1], "/x/sealed/active-catalog.yaml")
        inputs["dr01"]["catalog"] = "relative/catalog.yaml"
        self.assertIn("PATH_INVALID", make_plan("runtime", inputs=inputs)["blocked"]["dr01-deployed"][0])

    def test_dr07_runs_in_the_ts_image_on_both_networks_with_exact_mounts(self):
        work = "/state/p184-g13-20261010T090000Z/dr07-reference"
        (c,) = runtime_steps()["dr07-reference"]["commands"]
        argv = c["argv"]
        self.assertEqual(argv[:3], ["docker", "run", "--rm"])
        self.assertEqual(argv[argv.index("--name") + 1], "p184-g13-dr07-reference")
        self.assertEqual([argv[i + 1] for i, a in enumerate(argv) if a == "--network"],
                         ["qdl_v2_native_consumer", "executor_network"])
        self.assertEqual(argv[argv.index("--user") + 1], "0")
        self.assertEqual([argv[i + 1] for i, a in enumerate(argv) if a in ("--cap-drop", "--cap-add")],
                         ["ALL", "DAC_OVERRIDE"])
        self.assertEqual(argv[argv.index("--entrypoint") + 2], WRITER)  # default: the writer image
        self.assertEqual(argv[argv.index("--env-file") + 1], "/x/dsn.env")
        self.assertEqual(argv[argv.index("-w") + 1], "/ts")
        ro = sorted(m for m in self.mounts(argv) if m.endswith(":ro"))
        rw = sorted(m for m in self.mounts(argv) if not m.endswith(":ro"))
        self.assertEqual(ro, sorted(f"{d}:{d}:ro" for d in (
            "/ts", "/x", "/x/id/alpha-binance/jwt", "/x/id/alpha-binance/tls", "/x/id/alpha-okx/jwt",
            "/x/id/alpha-okx/tls")))
        self.assertEqual(rw, sorted([f"{work}:{work}", "/x/cursors:/x/cursors"]))
        harness = argv[argv.index("-B") + 1:]
        self.assertEqual(harness[0], "scripts/p184_dr07_reference.py")
        self.assertEqual(harness[harness.index("--dsn-env") + 1], "POSTGRES_DSN")
        self.assertEqual(harness[harness.index("--fx-account") + 1], "acct")
        self.assertFalse(any("postgres" in a.lower() and "://" in a for a in argv))  # DSN only via --env-file

    def test_dr07_image_and_networks_override_and_refusals(self):
        inputs = copy_inputs()
        inputs["dr07"].update(image=WRITER, networks=["netx"])  # an override may only restate the writer image
        argv = runtime_steps(inputs)["dr07-reference"]["commands"][0]["argv"]
        self.assertEqual(argv[argv.index("--entrypoint") + 2], WRITER)
        self.assertEqual([argv[i + 1] for i, a in enumerate(argv) if a == "--network"], ["netx"])
        for change, reason in ((dict(image="local/ts:latest"), "DR07_IMAGE_DIGEST_REQUIRED"),
                               (dict(image="sha256:" + "d4" * 32), "DR07_IMAGE_NOT_WRITER"),
                               (dict(networks=["bad net"]), "DR07_NETWORKS_INVALID"),
                               (dict(env_file="dsn.env"), "PATH_INVALID")):
            bad = copy_inputs()
            bad["dr07"].update(change)
            with self.subTest(change=change):
                self.assertTrue(make_plan("runtime", inputs=bad)["blocked"]["dr07-reference"][0].startswith(reason))
        bad = copy_inputs()
        bad["dr07"]["_connections"]["OKX"]["cursor_file"] = "cursor.json"
        self.assertIn("PATH_INVALID", make_plan("runtime", inputs=bad)["blocked"]["dr07-reference"][0])
        del bad["dr07"]["_connections"]
        self.assertIn("DR07_CONNECTIONS_UNREADABLE", make_plan("runtime", inputs=bad)["blocked"]["dr07-reference"][0])

    def test_dr07_image_is_the_image_its_rows_claim(self):
        """G13-R1: the image that runs DR07 equals the --image-digest its rows record."""
        for image in (None, "", WRITER, "sha256:" + "d4" * 32):
            inputs = copy_inputs()
            if image is not None:
                inputs["dr07"]["image"] = image
            entry = runtime_steps(inputs)["dr07-reference"]
            with self.subTest(image=image):
                if image not in (None, "", WRITER):
                    self.assertNotIn("commands", entry)
                    self.assertTrue(entry["blocked"][0].startswith("DR07_IMAGE_NOT_WRITER"), entry["blocked"])
                    continue
                argv = entry["commands"][0]["argv"]
                self.assertEqual(argv[argv.index("--entrypoint") + 2], argv[argv.index("--image-digest") + 1])

    def test_connections_and_env_file_are_checked_on_the_host(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = Path(tmp) / "conn.json"
            conn.write_text(json.dumps(INPUTS["dr07"]["_connections"]))
            inputs = {"dr07": {"connections": str(conn)}}
            g13.load_dr07_connections(inputs)
            self.assertEqual(inputs["dr07"]["_connections"], INPUTS["dr07"]["_connections"])
            inputs = {"dr07": {"connections": str(Path(tmp) / "absent.json")}}
            g13.load_dr07_connections(inputs)
            self.assertNotIn("_connections", inputs["dr07"])
            env = Path(tmp) / "dsn.env"
            env.write_text("POSTGRES_DSN=x\n")
            env.chmod(0o644)
            self.assertTrue(g13.env_file_problem(str(env)).startswith("DR07_ENV_FILE_MODE"))
            env.chmod(0o600)
            self.assertIsNone(g13.env_file_problem(str(env)))
            self.assertTrue(g13.env_file_problem(str(Path(tmp) / "none")).startswith("DR07_ENV_FILE_UNREADABLE"))


class WrongAccountRunIdTests(unittest.TestCase):
    """B7: the Gateway writer scope is an exact account list checked before the API key."""

    ACCOUNTS = ["paper-p184-dr01-g13r3abc-a-binance", "paper-p184-dr01-g13r3abc-a-okx",
                "paper-p184-dr01-g13r3abc-b-binance", "paper-p184-dr01-g13r3abc-b-okx"]

    def test_fixed_run_id_is_passed_and_its_accounts_are_emitted(self):
        entry = runtime_steps()["dr01-wrong-account"]
        probe = entry["commands"][0]["argv"]
        self.assertEqual(probe[-3:], ["probe", "--run-id", "g13r3abc"])
        self.assertEqual(entry["writer_scope_accounts"], self.ACCOUNTS)

    def test_run_id_is_required_and_validated(self):
        inputs = copy_inputs()
        del inputs["dr01_wrong_account"]
        self.assertIn("MISSING_INPUTS: dr01_wrong_account.run_id",
                      make_plan("runtime", inputs=inputs)["blocked"]["dr01-wrong-account"])
        for bad in ("ABCDEF12", "abc", "a" * 33, "abc-def1", 12345678):
            inputs["dr01_wrong_account"] = {"run_id": bad}
            with self.subTest(run_id=bad):
                self.assertTrue(make_plan("runtime", inputs=inputs)["blocked"]["dr01-wrong-account"][0]
                                .startswith("DR01_RUN_ID_INVALID"))

    def test_derivation_equals_the_ts_probe(self):
        script = TS_MATRIX_V2.parents[4] / "scripts/p184_dr01_wrong_account.py"
        if not script.is_file():
            self.skipTest("TS worktree not mounted")
        import importlib.util
        spec = importlib.util.spec_from_file_location("ts_dr01_wrong_account", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for run_id in ("g13r3abc", "20261011t190000abcdef"):
            self.assertEqual(g13.wrong_account_identities(run_id), module.identities(run_id))

    def test_writer_scope_check(self):
        acc = self.ACCOUNTS
        self.assertIsNone(g13.writer_scope_problem(None, acc))  # scope disabled
        self.assertIsNone(g13.writer_scope_problem(json.dumps({"include": ["other", *acc]}), acc))
        self.assertIsNone(g13.writer_scope_problem(json.dumps({"exclude": ["other"]}), acc))
        self.assertEqual(g13.writer_scope_problem(json.dumps({"include": acc[:3]}), acc),
                         "WRITER_SCOPE_DENIES_RUNNER_ACCOUNTS: " + acc[3])
        self.assertTrue(g13.writer_scope_problem(json.dumps({"exclude": [acc[0]]}), acc).endswith(acc[0]))
        for raw in ("{", "[]", json.dumps({"include": "x"}), json.dumps({"include": [], "exclude": []})):
            self.assertEqual(g13.writer_scope_problem(raw, acc), "WRITER_SCOPE_INVALID")

    def test_scope_denial_stops_the_step_before_the_probe(self):
        from unittest import mock
        entry = runtime_steps()["dr01-wrong-account"]
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(g13, "containers", return_value={}), \
                mock.patch.object(g13, "gateway_state", return_value={"running": True,
                                                                      "image": INPUTS["gateway_image_digest"]}), \
                mock.patch.object(g13, "gateway_writer_scope", return_value=json.dumps({"include": ["x"]})), \
                mock.patch.object(g13, "bounded") as bounded:
            receipt = g13.run_step(entry, FINAL, {**INPUTS, "_root": tmp}, Path("/ts"))
        bounded.assert_not_called()
        self.assertNotIn("deferred", receipt)
        self.assertIn("WRITER_SCOPE_DENIES_RUNNER_ACCOUNTS", receipt["error"])


class InverseDr05Tests(unittest.TestCase):
    def test_dr05_steps_run_every_scope_including_the_inverse(self):
        steps = {e["step"]: e for e in make_plan("all")["steps"]}
        self.assertEqual(set(g13.INV_DR05) - set(steps["dr05-upstream"]["cells"]) - set(steps["dr05-transport"]["cells"]),
                         set())
        self.assertIn("DR05.GAP_REFUSAL@OKX_INVERSE", steps["dr05-upstream"]["cells"])
        self.assertIn("DR05.STALE_TYPED@OKX_INVERSE", steps["dr05-transport"]["cells"])
        init = steps["dr05-upstream"]["commands"][0]["argv"]
        self.assertEqual([init[i + 1] for i, a in enumerate(init) if a == "--scope"], ["BINANCE", "OKX", "OKX_INVERSE"])
        self.assertEqual(init[init.index("--catalog") + 1], INPUTS["dr05_catalog"])
        for argv in (steps["dr05-upstream"]["commands"][3]["argv"], steps["dr05-transport"]["commands"][0]["argv"]):
            self.assertEqual(argv[argv.index("--venues") + 1:argv.index("--venues") + 4], ["BINANCE", "OKX", "OKX_INVERSE"])

    def test_dr05_upstream_needs_the_catalog_input(self):
        inputs = {k: v for k, v in INPUTS.items() if k != "dr05_catalog"}
        self.assertIn("MISSING_INPUTS: dr05_catalog", make_plan("isolated", inputs=inputs)["blocked"]["dr05-upstream"])

    def test_g13_init_argv_is_accepted_by_the_dl_fault_cli(self):
        from scripts import p184_dr05_dl_fault as dr
        init = {e["step"]: e for e in make_plan("isolated")["steps"]}["dr05-upstream"]["commands"][0]["argv"]
        with tempfile.TemporaryDirectory() as tmp:
            catalog = Path(tmp) / "c.yaml"
            catalog.write_text("x: 1\n")
            argv = [str(catalog) if a == INPUTS["dr05_catalog"] else a for a in init[3:]]
            argv[argv.index("--init")] = "--prepare"
            argv[argv.index("--prepare") + 1] = str(Path(tmp) / "case")
            del argv[argv.index("--core-config"):argv.index("--core-config") + 2]
            with redirect_stdout(io.StringIO()):
                self.assertEqual(dr.dispatch(argv), 0)
            self.assertEqual(dr.scope_set(Path(tmp) / "case"), g13.DR05_SCOPES)
            self.assertEqual(dr.image_set(Path(tmp) / "case")["core"], RUST)


class GatewayStepTests(unittest.TestCase):
    def test_wrong_account_step_probes_inside_the_gateway_and_records_with_its_image(self):
        steps = {e["step"]: e for e in make_plan("runtime")["steps"]}
        entry = steps["dr01-wrong-account"]
        self.assertTrue(entry["gateway_dependent"])
        self.assertEqual(entry["cells"], ["DR01.WRONG_ACCOUNT_DENIED@BINANCE", "DR01.WRONG_ACCOUNT_DENIED@OKX"])
        probe, record = entry["commands"]
        self.assertEqual(probe["argv"][:4], ["docker", "exec", "-i", "gateway_service"])
        self.assertEqual(probe["argv"][-4:], ["-", "probe", "--run-id", "g13r3abc"])
        self.assertTrue(probe["stdin"].endswith("scripts/p184_dr01_wrong_account.py"))
        argv = record["argv"]
        self.assertEqual(argv[argv.index("--gateway-image-digest") + 1], INPUTS["gateway_image_digest"])
        self.assertEqual(argv[argv.index("--source-sha") + 1], FINAL["ts_sha"])
        self.assertEqual(argv[argv.index("--rows") + 1], probe["stdout"])
        self.assertFalse(steps["dr01-deployed"]["gateway_dependent"])

    def test_missing_gateway_inputs_block_only_that_step(self):
        inputs = {k: v for k, v in INPUTS.items() if k != "gateway_container"}
        result = make_plan("runtime", inputs=inputs)
        self.assertEqual(list(result["blocked"]), ["dr01-wrong-account"])

    def test_gateway_not_running_is_typed(self):
        image = INPUTS["gateway_image_digest"]
        self.assertIsNone(g13.gateway_problem({"running": True, "image": image}, image))
        for state in (None, {"running": False, "image": image}, {"running": True, "image": PY}):
            self.assertTrue(g13.gateway_problem(state, image).startswith("GATEWAY_NOT_RUNNING"))

    def test_absent_gateway_defers_the_step_without_failing_the_run(self):
        from unittest import mock
        entry = {e["step"]: e for e in make_plan("runtime")["steps"]}["dr01-wrong-account"]
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(g13, "containers", return_value={}), \
                mock.patch.object(g13, "gateway_state", return_value=None), \
                mock.patch.object(g13, "bounded") as bounded:
            receipt = g13.run_step(entry, FINAL, {**INPUTS, "_root": tmp}, Path("/ts"))
        bounded.assert_not_called()
        self.assertFalse(receipt["harness_ok"])
        self.assertTrue(receipt["deferred"].startswith("GATEWAY_NOT_RUNNING"))
        self.assertIn("GATEWAY_NOT_RUNNING", receipt["error"])


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

    def test_checkout_dirty_anywhere_in_the_tracked_tree_is_refused(self):
        """G13-R2: no pathspec; a tracked edit outside qdl/rust/scripts/... (qdl_sdk, app, Dockerfile) is seen."""
        from unittest import mock
        calls = []

        def git_sh(argv, timeout=60):
            calls.append(argv)
            if "rev-parse" in argv:
                return FINAL["dl_sha"] + "\n"
            # real git semantics: a pathspec restricts the output to the listed paths
            dirty = [" M qdl_sdk/client.py", " M Dockerfile.python", " M poetry.lock"]
            spec = argv[argv.index("--") + 1:] if "--" in argv else None
            return "\n".join(d for d in dirty if spec is None or d[3:].split("/")[0] in spec)

        with mock.patch.object(g13, "sh", git_sh):
            found = g13.checkout_problems(FINAL["dl_sha"])
        status = next(c for c in calls if "status" in c)
        self.assertNotIn("--", status)
        self.assertEqual(len(found), 1)
        self.assertTrue(found[0].startswith("DL_CHECKOUT_DIRTY"))
        for path in ("qdl_sdk/client.py", "Dockerfile.python", "poetry.lock"):
            self.assertIn(path, found[0])
        with mock.patch.object(g13, "sh", lambda argv, timeout=60: FINAL["dl_sha"] if "rev-parse" in argv else ""):
            self.assertEqual(g13.checkout_problems(FINAL["dl_sha"]), [])

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
                                 "--only", "dr08-isolated", "--dry-run", "--ts-root", str(TS_MATRIX_V2.parents[4])])
            result = json.loads(out.getvalue())
            if not TS_MATRIX_V2.is_file():
                self.assertEqual(code, 2)  # no TS matrix v2 to bind DR_CELLS to
                return
            self.assertEqual(code, 0)  # final images are passed to the harnesses; nothing pins rc2 any more
            self.assertEqual(result["blocked"], {})
            self.assertEqual(result["case_matrix"]["schema"], "p184.r2-case-matrix.v2")
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_bad_identity_exits_before_plan(self):
        out = io.StringIO()
        with redirect_stdout(out):
            code = g13.main(["--python-image", "sha256:" + "0" * 64, "--rust-image", RUST, "--python-image-ref",
                             "x", "--writer-image", WRITER, "--ts-source-sha", FINAL["ts_sha"], "--dl-source-sha",
                             FINAL["dl_sha"], "--dry-run"])
        self.assertEqual(code, 2)
        self.assertIn("FINAL_DIGEST_REQUIRED", out.getvalue())


# G13-U1 fixture: committed files and their git tree id (computed with git 2.43 on the host: git init, these three
# entries, git add -A, git commit, git rev-parse HEAD^{tree}).
FIXTURE_TREE = "29fd85159e1c5e6ef02aa3892a48eff4d5390d0d"


def write_fixture(root, extra=()):
    root = Path(root)
    (root / "qdl").mkdir(parents=True)
    (root / "scripts").mkdir()
    (root / "qdl/__init__.py").write_text("x = 1\n")
    (root / "scripts/run.sh").write_text("#!/bin/sh\n")
    (root / "scripts/run.sh").chmod(0o755)
    (root / "link.py").symlink_to("qdl/__init__.py")
    for rel, text in extra:
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text)
    return root


def tar_of(root):
    import tarfile
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for path in sorted(Path(root).rglob("*")):
            tar.add(path, arcname=str(path.relative_to(root)), recursive=False)
    return buf.getvalue()


class FakeGit:
    """sh/subprocess.run as seen by export_source: rev-parse -> FIXTURE_TREE, `git archive` -> a tar of `archived`
    (what git would have exported); every other subprocess (tar -x) runs for real."""

    def __init__(self, archived, docker_image=None):
        import subprocess
        self.archive, self.real_run, self.docker_image = tar_of(archived), subprocess.run, docker_image

    def sh(self, argv, timeout=60):
        if argv[:1] == ["git"] and "rev-parse" in argv and "--verify" in argv:
            return FIXTURE_TREE + "\n"
        if argv[:3] == ["docker", "image", "inspect"]:
            return (self.docker_image or "") + "\n"
        raise AssertionError(f"unexpected sh {argv}")

    def run(self, argv, **kw):
        import subprocess
        if argv[:1] == ["git"] and "archive" in argv:
            return subprocess.CompletedProcess(argv, 0, self.archive, b"")
        return self.real_run(argv, **kw)

    def patch(self):
        from unittest import mock
        from contextlib import ExitStack
        stack = ExitStack()
        stack.enter_context(mock.patch.object(g13, "sh", self.sh))
        stack.enter_context(mock.patch.object(g13.subprocess, "run", self.run))
        return stack


class SourceExportTests(unittest.TestCase):
    """G13-U1: harnesses run from a sealed `git archive <dl_sha>` export, never from the worktree."""

    def test_no_planned_command_reaches_the_worktree(self):
        result = make_plan("all")
        export = "/state/p184-g13-20261010T090000Z/dl-source"
        self.assertEqual(result["dl_source_dir"], export)
        dl_cwds = 0
        for entry in result["steps"]:
            for c in entry.get("commands", ()):
                text = json.dumps(c)
                with self.subTest(step=entry["step"]):
                    self.assertNotIn(str(g13.DL_ROOT), text)
                with self.subTest(step=entry["step"], cwd=c["cwd"]):
                    # TS harnesses run in the TS root, the DR01 probe in the alpha compose dir, all DL ones in the export
                    self.assertIn(c["cwd"], ("/ts", "/home/bobby/execution_alpha", export))
                dl_cwds += c["cwd"] == export
        self.assertGreater(dl_cwds, 5)
        dr01 = {e["step"]: e for e in result["steps"]}["dr01-deployed"]["commands"]
        self.assertEqual(dr01[1]["stdin"], export + "/scripts/p184_dr01_deployed.py")
        argv = dr01[0]["argv"]
        self.assertIn(f"PYTHONPATH={export}:{export}/generated/python", argv)
        up = {e["step"]: e for e in result["steps"]}["dr05-upstream"]["commands"][3]["argv"]
        self.assertEqual(up[up.index("--dl-root") + 1], export)

    def test_git_tree_id_equals_git(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = write_fixture(Path(tmp) / "src")
            (root / "empty").mkdir()  # git has no empty trees
            self.assertEqual(g13.git_tree_id(root), FIXTURE_TREE)
            (root / "scripts/run.sh").chmod(0o644)
            self.assertNotEqual(g13.git_tree_id(root), FIXTURE_TREE)

    def test_export_is_sealed_and_holds_only_the_commit(self):
        with tempfile.TemporaryDirectory() as tmp:
            committed = write_fixture(Path(tmp) / "committed")
            dest = Path(tmp) / "export"
            with FakeGit(committed).patch():
                record = g13.export_source(FINAL["dl_sha"], dest)
            self.assertEqual((record["tree"], record["files"], record["commit"]), (FIXTURE_TREE, 3, FINAL["dl_sha"]))
            self.assertFalse(os.access(dest / "qdl", os.W_OK))
            self.assertFalse(os.access(dest / "qdl/__init__.py", os.W_OK))
            with self.assertRaises(PermissionError):
                (dest / "qdl/evil.py").write_text("x")
            self.assertTrue(g13.remove_source(dest))
            self.assertFalse(dest.exists())

    def test_export_with_an_extra_module_is_refused(self):
        """An archive (or a race) that adds sitecustomize.py / a .pth / a stray module does not match the tree."""
        for rel in ("sitecustomize.py", "qdl/evil.py", "x.pth"):
            with tempfile.TemporaryDirectory() as tmp, self.subTest(rel=rel):
                archived = write_fixture(Path(tmp) / "a", extra=[(rel, "import os\n")])
                with FakeGit(archived).patch(), self.assertRaisesRegex(g13.Blocked, "SOURCE_EXPORT_MISMATCH"):
                    g13.export_source(FINAL["dl_sha"], Path(tmp) / "export")

    def test_untracked_worktree_module_is_not_importable_from_the_export(self):
        """Real git: untracked sitecustomize.py and qdl/evil.py, ignored x.pth in the worktree; neither runs from
        the export (skipped where git is absent, e.g. the python image)."""
        import shutil
        import subprocess
        if not shutil.which("git"):
            self.skipTest("git not installed")
        from unittest import mock
        with tempfile.TemporaryDirectory() as tmp:
            repo = write_fixture(Path(tmp) / "repo")
            env = {**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null"}
            git = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@invalid"]
            for args in (["init", "-q"], ["add", "-A"], ["commit", "-qm", "t"]):
                subprocess.run(git[:3] + args if args[0] == "init" else git + args, check=True, env=env)
            sha = subprocess.run(git + ["rev-parse", "HEAD"], check=True, env=env, capture_output=True,
                                 text=True).stdout.strip()
            (repo / ".gitignore").write_text("*.pth\n")  # untracked itself, ignored rule for x.pth
            (repo / "x.pth").write_text("import sys; print('PTH-RAN')\n")
            (repo / "sitecustomize.py").write_text("print('SITE-RAN')\n")
            (repo / "qdl/evil.py").write_text("print('EVIL-RAN')\n")
            control = subprocess.run([sys.executable, "-B", "-c", "import qdl.evil"], cwd=repo, capture_output=True,
                                     text=True, env={**env, "PYTHONPATH": str(repo)})
            self.assertIn("SITE-RAN", control.stdout)  # control: from the worktree both would run
            self.assertIn("EVIL-RAN", control.stdout)
            dest = Path(tmp) / "export"
            with mock.patch.object(g13, "DL_ROOT", repo):
                record = g13.export_source(sha, dest)
            self.assertEqual(record["tree"], FIXTURE_TREE)
            self.assertEqual(sorted(p.name for p in dest.rglob("*")), ["__init__.py", "link.py", "qdl", "run.sh",
                                                                         "scripts"])
            run = subprocess.run([sys.executable, "-B", "-c", "import qdl.evil"], cwd=dest, capture_output=True,
                                 text=True, env={**env, "PYTHONPATH": str(dest)})
            self.assertNotEqual(run.returncode, 0)
            self.assertNotIn("RAN", run.stdout)
            g13.remove_source(dest)

    def test_run_exports_before_the_first_step_and_removes_after(self):
        if not TS_MATRIX_V2.is_file():
            self.skipTest("TS worktree not mounted")
        from unittest import mock
        seen = []

        def fake_step(entry, final, inputs, ts_root):
            source = Path(inputs["_root"]) / g13.SOURCE_DIR
            seen.append((entry["commands"][0]["cwd"] == str(source), inputs["_dl_source"]["tree"],
                         os.access(source, os.W_OK), (source / "qdl/__init__.py").is_file()))
            return {"step": entry["step"], "cells": entry["cells"], "duration_s": 0.0, "exit_codes": [0],
                    "error": None, "harness_ok": True, "removed_leftovers": {}}

        with tempfile.TemporaryDirectory() as tmp:
            committed = write_fixture(Path(tmp) / "committed")
            receipts = Path(tmp) / "r"
            receipts.mkdir()
            with FakeGit(committed, docker_image=PY).patch(), \
                    mock.patch.object(g13, "checkout_problems", return_value=[]), \
                    mock.patch.object(g13, "run_step", side_effect=fake_step), redirect_stdout(io.StringIO()):
                code = g13.main(["--python-image", PY, "--rust-image", RUST, "--python-image-ref",
                                 FINAL["python_ref"], "--writer-image", WRITER, "--ts-source-sha", FINAL["ts_sha"],
                                 "--dl-source-sha", FINAL["dl_sha"], "--phase", "isolated", "--only",
                                 "dr05-upstream", "--inputs", self.inputs_file(tmp), "--receipts", str(receipts),
                                 "--ts-root", str(TS_MATRIX_V2.parents[4])])
            self.assertEqual(code, 0)
            self.assertEqual(seen, [(True, FIXTURE_TREE, False, True)])
            (root,) = receipts.iterdir()
            self.assertFalse((root / g13.SOURCE_DIR).exists())
            plan_json = json.loads((root / "plan.json").read_text())
            summary = json.loads((root / "summary.json").read_text())
            self.assertEqual(plan_json["dl_source"]["tree"], FIXTURE_TREE)
            self.assertEqual((summary["dl_source"]["tree"], summary["dl_source"]["removed"]), (FIXTURE_TREE, True))

    @staticmethod
    def inputs_file(tmp):
        path = Path(tmp) / "inputs.json"
        path.write_text(json.dumps({k: INPUTS[k] for k in ("dr05_public_export", "dr05_catalog", "redis_image",
                                                            "postgres_image")}))
        return str(path)


if __name__ == "__main__":
    unittest.main()
