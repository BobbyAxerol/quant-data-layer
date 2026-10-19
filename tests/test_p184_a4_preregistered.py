"""P18.4 stage-100 amendment A4 v2 (PRE_REGISTERED, owner 2026-10-18 ~16:40Z): the A4 rules frozen before the final
run and bound to its runtime identity by a pre-load binding and a post-run close receipt.

Negatives: an inverse p95 over the pooled limit -> FAIL; MARK BINANCE max 1200.1 ms / p95 500.1 ms -> FAIL; an A4 v2
file that differs from the owner decision (post_hoc, label, an external exemption, a rule, a pin) is invalid; a bind
with the campaign directory present, an incomplete identity, a code pin mismatch, an excluded run or an A4 v2 frozen
after the bind is refused; a missing binding or close receipt, a binding after the campaign start, a changed A4 v2
file, images or caps changed during the run and a preflight identity that differs from the binding are not applied.
Parity: without a binding A4 v2 is not applied and the frozen scorer reproduces the stored gates of 053803Z / 135719Z /
074624Z / 092255Z / 123222Z (123222Z byte for byte in place). Positive replay: 123222Z with a synthetic pre-run
binding gives the same verdicts and the same G2/G4 content as the post-hoc A4. Host-runnable (stdlib only)."""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts import p184_a4_post_hoc as a4v1
from scripts import p184_a4_preregistered as v2
from scripts import p184_gate_campaign as gc
from tests.p184_receipt_support import mirror
from tests.test_p184_a4_post_hoc import ADDENDUM, TABLE, bucket, event, mark, retention, strip_paths, window

STATE = Path.home() / ".local/state/qdl-v2"
COUNTED = STATE / "p184-final-campaign-20261018T123222Z"
RECEIPTS = [STATE / name for name in ("p184-final-campaign-20261018T053803Z", "p184-final-campaign-20261017T135719Z",
                                      "p184-final-campaign-20261018T074624Z", "p184-final-campaign-20261018T092255Z",
                                      "p184-final-campaign-20261018T123222Z")]
FROZEN = json.loads(v2.A4V2_PATH.read_text())
IMAGES = {role: "sha256:" + "a" * 64 for role in gc.A3_RUNTIME_ROLES + ("writer",)}
RESOURCES = {role: {k: 1 for k in v2.RESOURCE_KEYS} for role in v2.RESOURCE_ROLES + ("writer",)}
IDENTITY = {"runtime_images": IMAGES, "runtime_resources": RESOURCES, "writer_running": True,
            "runtime_identity": {"writer_image": IMAGES["writer"], "writer_started": "2026-10-18T00:00:00Z",
                                 "writer_restarts": 0}}


def amendment(**over):
    out = copy.deepcopy(FROZEN)
    out.update(over)
    return out


class RuleTests(unittest.TestCase):
    """The A4 v1 rule functions through the A4 v2 relabelling wrappers."""

    def g4(self, windows, held=None):
        held = held if held is not None else [{key: None for key in a4v1.G4_ROWS} for _ in windows]
        return v2.v2_score_g4(held)(windows, TABLE, ADDENDUM)

    def test_inverse_p95_over_the_pooled_limit_fails(self):
        windows = [window({"book_delta|OKX_INVERSE|": bucket(120, 291.0, 300.0, lower=290.0)})]
        held = [{"book_delta|OKX_INVERSE|": retention(120, [event(300.0)]), "book_snapshot|OKX_INVERSE|": None,
                 "quote|OKX_INVERSE|": None}]
        state, rows, why = self.g4(windows, held)
        self.assertEqual((state, why), ("FAIL", ["over limit: book_delta|OKX_INVERSE|"]))
        row = {r["row"]: r for r in rows}["book_delta|OKX_INVERSE|"]
        self.assertEqual((row["label"], row["post_hoc"], row["amendment_version"], row["basis"]),
                         ("PRE_REGISTERED", False, 2, "a4_v2_preregistered"))

    def test_inside_pooled_p95_passes_and_other_rows_are_untouched(self):
        windows = [window({"quote|OKX_INVERSE|": bucket(167, 213.0, 531.9)})]
        held = [{"quote|OKX_INVERSE|": retention(167, [event(531.9)], threshold=322.6),
                 "book_delta|OKX_INVERSE|": None, "book_snapshot|OKX_INVERSE|": None}]
        state, rows, why = self.g4(windows, held)
        self.assertEqual((state, why), ("PASS", []))
        _, frozen, _ = gc.score_g4(windows, TABLE, ADDENDUM)
        keep = lambda rows: [r for r in rows if r["row"] not in a4v1.G4_ROWS]  # noqa: E731
        self.assertEqual(v2.canonical(keep(frozen)), v2.canonical(keep(rows)))

    def test_mark_binance_max_1200_inclusive_and_p95_unchanged(self):
        score = lambda **kw: v2.v2_score_mark()(mark(**kw))  # noqa: E731
        self.assertEqual(score(binance=(9000, 418.6, 1200.0))[0], "PASS")
        state, rows, why = score(binance=(9000, 418.6, 1200.1))
        self.assertEqual((state, why), ("FAIL", ["outside: MARK|BINANCE"]))
        self.assertEqual((rows[0]["a4"]["label"], rows[0]["a4"]["post_hoc"]), ("PRE_REGISTERED", False))
        self.assertEqual(score(binance=(9000, 500.1, 900.0))[0], "FAIL")
        self.assertEqual(score(okx=(100, 300.0, 1000.0))[2], ["outside: MARK|OKX"])  # MARK|OKX keeps max < 1000


class FrozenFileTests(unittest.TestCase):
    def test_the_frozen_file_states_the_owner_decisions(self):
        self.assertEqual(v2.validate_a4v2(FROZEN), [])
        self.assertIn(COUNTED.name, FROZEN["applies_to"]["excluded_campaigns"])
        v1 = json.loads(a4v1.A4_PATH.read_text())
        self.assertEqual(v2.canonical({k: FROZEN["rules"]["G2"][k] for k in ("row", "max_le_ms", "p95_le_ms")}),
                         v2.canonical({k: v1["rules"]["G2"][k] for k in ("row", "max_le_ms", "p95_le_ms")}))
        self.assertEqual(v2.canonical(FROZEN["rules"]["G4"]["rows"]), v2.canonical(v1["rules"]["G4"]["rows"]))
        self.assertEqual(FROZEN["external_exemptions"], [])

    def test_pins_are_the_working_files(self):
        self.assertEqual({k: FROZEN["pinned"][k]["sha256"] for k in v2.CODE_PINS}, v2.code_sha256())
        for name, path in (("budget", "config/v2/p184-stage100-budget.json"),
                           ("g4_addendum", "config/v2/p184-stage100-g4-addendum.json"),
                           ("g4_amendment_1", "config/v2/p184-stage100-g4-amendment-1.json"),
                           ("g4_amendment_2", "config/v2/p184-stage100-g4-amendment-2.json")):
            self.assertEqual(FROZEN["pinned"][name]["sha256"], gc.sha256_file(gc.ROOT / path), name)
        a3_pins = json.loads(gc.A3_PATH.read_text())["pinned"]
        self.assertEqual(FROZEN["pinned"]["scorer"]["sha256"], a3_pins["gate_campaign_tool"]["new_sha256"])

    def test_a_file_that_differs_from_the_owner_decision_is_invalid(self):
        rules = FROZEN["rules"]
        bad = [{"schema": "qdl.p184.stage100-a4.v1"}, {"post_hoc": True}, {"label": "POST_HOC_OWNER_DECISION"},
               {"version": 1}, {"decision_owner": None}, {"frozen_at_unix": None},
               {"external_exemptions": ["EXTERNAL_VENUE_WIDE"]}, {"external_exemptions": None},
               {"rules": {**rules, "G2": {**rules["G2"], "max_le_ms": 1300.0}}},
               {"rules": {**rules, "G2": {**rules["G2"], "p95_le_ms": 600.0}}},
               {"rules": {**rules, "G4": {**rules["G4"], "max": "GATED"}}},
               {"rules": {**rules, "G4": {**rules["G4"], "min_samples_per_window": 5}}},
               {"applies_to": {**FROZEN["applies_to"], "selection": "ANY"}},
               {"applies_to": {**FROZEN["applies_to"], "sessions": 50}},
               {"pinned": {k: v for k, v in FROZEN["pinned"].items() if k != "scorer"}}]
        for over in bad:
            self.assertTrue(v2.validate_a4v2(amendment(**over)), over)


class BindCloseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.path = self.root / "a4v2.json"
        self.path.write_text(json.dumps(FROZEN))
        self.d = self.root / "p184-final-campaign-20261019T000000Z"

    def tearDown(self):
        self.tmp.cleanup()

    def bind(self, **kw):
        kw.setdefault("identity", IDENTITY)
        kw.setdefault("now", FROZEN["frozen_at_unix"] + 10)
        return v2.bind(self.d, amendment_path=kw.pop("path", self.path), **kw)

    def test_bind_records_the_identity_once_before_the_campaign_exists(self):
        path, binding = self.bind()
        self.assertEqual(path, v2.binding_path_for(self.d.resolve()))
        self.assertEqual((binding["runtime_images"], binding["a4_v2"]["sha256"]), (IMAGES, gc.sha256_file(self.path)))
        with self.assertRaises(FileExistsError):
            self.bind()

    def test_bind_refusals(self):
        self.d.mkdir()
        with self.assertRaisesRegex(RuntimeError, "must precede the campaign"):
            self.bind()
        self.d.rmdir()
        with self.assertRaisesRegex(RuntimeError, "runtime identity incomplete"):
            self.bind(identity={**IDENTITY, "runtime_images": {**IMAGES, "rust_core": None}})
        with self.assertRaisesRegex(RuntimeError, "not frozen before the binding"):
            self.bind(now=FROZEN["frozen_at_unix"] - 1)
        wrong = self.root / "wrong.json"
        wrong.write_text(json.dumps(amendment(pinned={**FROZEN["pinned"], "scorer": {"sha256": "0" * 64}})))
        with self.assertRaisesRegex(RuntimeError, "working code differs"):
            self.bind(path=wrong)
        excluded = self.root / "excluded.json"
        excluded.write_text(json.dumps(amendment(applies_to={**FROZEN["applies_to"],
                                                             "excluded_campaigns": [self.d.name]})))
        with self.assertRaisesRegex(RuntimeError, "excludes"):
            self.bind(path=excluded)
        self.assertFalse(v2.binding_path_for(self.d.resolve()).exists())

    def test_close_needs_the_binding_and_the_finished_campaign(self):
        with self.assertRaisesRegex(RuntimeError, "no binding"):
            v2.close(self.d, identity=IDENTITY)
        self.bind()
        self.d.mkdir()
        with self.assertRaisesRegex(RuntimeError, "not written scorecard.json"):
            v2.close(self.d, identity=IDENTITY)
        (self.d / "scorecard.json").write_text("{}")
        path, record = v2.close(self.d, identity=IDENTITY)
        self.assertEqual(record["binding_sha256"], gc.sha256_file(v2.binding_path_for(self.d.resolve())))


@unittest.skipUnless(all((d / "scorecard.json").exists() for d in RECEIPTS), "existing receipts not on this host")
class ReceiptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.tmp.name)
        cls.campaign = json.loads((COUNTED / "campaign.json").read_text())
        cls.stored = json.loads((COUNTED / "scorecard.json").read_text())
        started = cls.campaign["started_unix"]
        # Replay amendment: the frozen A4 v2 with a freeze time before 123222Z started and no exclusion.
        cls.replay_path = cls.root / "a4v2-replay.json"
        cls.replay_path.write_text(json.dumps(amendment(frozen_at_unix=started - 200, applies_to={
            **FROZEN["applies_to"], "excluded_campaigns": []})))
        cls.replay = json.loads(cls.replay_path.read_text())
        cls.identity = {"runtime_images": cls.campaign["a3_runtime_images"], "runtime_resources": RESOURCES,
                        "runtime_identity": cls.campaign["runtime_identity"]}
        cls.binding_path, cls.close_path = cls.write_pair(cls.root / "replay", started - 100, cls.replay_path)
        cls.result = v2.rescore(COUNTED, amendment_path=cls.replay_path, binding_path=cls.binding_path,
                                close_path=cls.close_path)
        cls.post_hoc = a4v1.rescore(COUNTED, a4v1.A4_PATH)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    @classmethod
    def write_pair(cls, base: Path, created: float, amendment_path: Path, *, close_over=None, binding_over=None):
        base.mkdir()
        binding = {"schema": v2.BINDING_SCHEMA, "campaign_id": COUNTED.name, "created_unix": created,
                   "a4_v2": {"path": str(amendment_path), "sha256": gc.sha256_file(amendment_path)},
                   "code_sha256": v2.code_sha256(), **cls.identity, **(binding_over or {})}
        bpath = base / "binding.json"
        bpath.write_text(json.dumps(binding))
        record = {"schema": v2.CLOSE_SCHEMA, "campaign_id": COUNTED.name, "binding_sha256": gc.sha256_file(bpath),
                  "runtime_images": cls.identity["runtime_images"],
                  "runtime_resources": cls.identity["runtime_resources"], **(close_over or {})}
        cpath = base / "close.json"
        cpath.write_text(json.dumps(record))
        return bpath, cpath

    def problems(self, name, *, created=None, amendment_path=None, close_over=None, binding_over=None, drop=None):
        path = amendment_path or self.replay_path
        bpath, cpath = self.write_pair(self.root / name, self.campaign["started_unix"] - 100 if created is None
                                       else created, self.replay_path, close_over=close_over,
                                       binding_over=binding_over)
        if drop:
            (bpath if drop == "binding" else cpath).unlink()
        return v2.applicability(COUNTED, self.campaign, self.stored, self.result["frozen"],
                                json.loads(Path(path).read_text()), Path(path),
                                v2.gc.load_json(bpath) if bpath.exists() else None, bpath,
                                v2.gc.load_json(cpath) if cpath.exists() else None)

    # ---- parity: no A4 v2 -> the frozen scorer's verdicts, unchanged
    def test_without_a_binding_nothing_is_applied_and_stored_gates_are_reproduced(self):
        (self.root / "mirror").mkdir()
        # Older receipts recorded configs in removed run worktrees (mirror them); 123222Z is scored in place.
        dirs = [d if d == COUNTED else mirror(d, self.root / "mirror") for d in RECEIPTS]
        for d in dirs:
            result = v2.rescore(d)
            self.assertFalse(result["applied"], d.name)
            self.assertIsNone(result["card"], d.name)
            self.assertTrue(any("no A4 v2 binding" in p for p in result["problems"]), result["problems"])
            stored = json.loads((d / "scorecard.json").read_text())
            self.assertEqual(json.dumps(strip_paths(result["frozen"]["gates"]), sort_keys=True),
                             json.dumps(strip_paths(stored["gates"]), sort_keys=True), d.name)
        self.assertEqual(v2.canonical(self.result["frozen"]), v2.canonical(self.stored))  # in place, byte for byte
        self.assertEqual(hashlib.sha256((COUNTED / "scorecard.json").read_bytes()).hexdigest(),
                         json.loads(a4v1.A4_PATH.read_text())["applies_to"]["scorecard_sha256"])

    def test_the_frozen_a4_v2_does_not_apply_to_123222z(self):
        bpath, cpath = self.write_pair(self.root / "frozen", self.campaign["started_unix"] - 100, v2.A4V2_PATH)
        result = v2.applicability(COUNTED, self.campaign, self.stored, self.result["frozen"], FROZEN, v2.A4V2_PATH,
                                  v2.gc.load_json(bpath), bpath, v2.gc.load_json(cpath))
        self.assertIn(f"A4 v2 excludes {COUNTED.name} (A4 v1 post-hoc is its decision)", result)
        self.assertTrue(any(p.startswith("not frozen_at < binding") for p in result), result)

    # ---- missing or broken pin -> not applied
    def test_missing_or_broken_pins_are_not_applied(self):
        started = self.campaign["started_unix"]
        changed = self.root / "changed.json"
        changed.write_text(json.dumps({**self.replay, "approval_reference": "edited after the binding"}))
        other = {**self.campaign["a3_runtime_images"], "rust_core": "sha256:" + "f" * 64}
        cases = {
            "no binding": (dict(drop="binding"), "no A4 v2 binding"),
            "no close": (dict(drop="close"), "no A4 v2 close receipt"),
            "bound after start": (dict(created=started + 1), "not frozen_at < binding"),
            "bound before freeze": (dict(created=started - 300), "not frozen_at < binding"),
            "A4 v2 edited after bind": (dict(amendment_path=changed), "A4 v2 changed after the binding"),
            "roll during run": (dict(close_over={"runtime_images": other}), "runtime images changed during the run"),
            "cap change during run": (dict(close_over={"runtime_resources": {**RESOURCES, "kafka2": None}}),
                                      "runtime cgroup caps changed during the run"),
            "preflight images differ": (dict(binding_over={"runtime_images": other},
                                             close_over={"runtime_images": other}),
                                        "runtime images at preflight differ from the binding"),
            "writer identity differs": (dict(binding_over={"runtime_identity": {
                **self.campaign["runtime_identity"], "writer_started": "x"}}),
                "writer identity at preflight differs from the binding"),
            "bind-time code differs": (dict(binding_over={"code_sha256": {**v2.code_sha256(), "scorer": "0" * 64}}),
                                       "scripts/p184_gate_campaign.py differs from the A4 v2 pin"),
        }
        for i, (name, (kw, expected)) in enumerate(cases.items()):
            problems = self.problems(f"case{i}", **kw)
            self.assertTrue(any(expected in p for p in problems), (name, problems))
        self.assertEqual(self.problems("clean"), [])

    # ---- positive replay: same verdicts as the post-hoc A4
    def test_replay_on_123222z_gives_the_post_hoc_a4_verdicts(self):
        self.assertTrue(self.result["applied"], self.result["problems"])
        self.assertTrue(self.post_hoc["applied"], self.post_hoc["problems"])
        card, ref = self.result["card"], self.post_hoc["card"]
        self.assertEqual(card["summary"], ref["summary"])
        self.assertIsNone(card["summary"].get("FAIL"))
        self.assertEqual(card["a4"]["summary_without_a4"]["FAIL"], ["G2", "G4"])
        gates, ref_gates = ({g["id"]: g for g in c["gates"]} for c in (card, ref))
        for gid in ("G2", "G4"):
            self.assertEqual((gates[gid]["verdict"], gates[gid]["a4"]["verdict_without_a4"]), ("PASS", "FAIL"))
            self.assertEqual((gates[gid]["a4"]["label"], gates[gid]["a4"]["post_hoc"]), ("PRE_REGISTERED", False))
            self.assertEqual(v2.canonical(normalise(gates[gid])), v2.canonical(normalise(ref_gates[gid])), gid)
        for gid in set(gates) - set(v2.SCOPED_GATES):
            self.assertEqual(v2.canonical(gates[gid]), v2.canonical(ref_gates[gid]), gid)
        rows = {r["row"]: r for r in gates["G4"]["measured"] if r["row"] in a4v1.G4_ROWS}
        self.assertEqual({k: r["verdict"] for k, r in rows.items()}, dict.fromkeys(a4v1.G4_ROWS, "PASS"))
        self.assertEqual(sum(len(r["retained_over_replaced_limit"]) for r in rows.values()), 8)
        self.assertEqual(card["a4"]["external_exemptions"], [])
        self.assertTrue(card["a4"]["a3_recorded"]["applied"])

    def test_the_scorer_functions_are_restored(self):
        self.assertEqual((gc.score_g4.__name__, gc.score_g4.__module__), ("score_g4", "scripts.p184_gate_campaign"))
        self.assertEqual((gc.score_mark.__name__, gc.score_mark.__module__),
                         ("score_mark", "scripts.p184_gate_campaign"))


def normalise(gate: dict) -> dict:
    """A G2/G4 gate without the fields that only name the amendment version (label, post_hoc, version, basis)."""
    names = {"label", "post_hoc", "amendment_version", "basis"}

    def walk(value):
        if isinstance(value, dict):
            return {k: walk(v) for k, v in value.items() if k not in names}
        if isinstance(value, list):
            return [walk(v) for v in value]
        return value
    return walk({k: v for k, v in gate.items() if k not in ("rule", "a4")})


if __name__ == "__main__":
    unittest.main()
