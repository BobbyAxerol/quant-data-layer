"""Unit tests for scripts/build_qdl_release_images.py: arguments, lock/disk guards and post-check verdicts.

No docker call is made: only the pure helpers and git reads of this repository are exercised.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import build_qdl_release_images as b


REPO = Path(__file__).resolve().parents[1]
SHA = "a" * 64


def git_has(*revs: str) -> bool:
    """True when a git binary exists and every revision is resolvable in this checkout (CI images carry no git,
    and a shallow checkout lacks history); the tests below read git itself, so they need both."""
    if shutil.which("git") is None:
        return False
    return all(subprocess.run(["git", "-C", str(REPO), "cat-file", "-e", rev + "^{commit}"],
                              capture_output=True).returncode == 0 for rev in revs)


PRE_BUMP_REV = "87db2159f411e7c4d7d49033e7573b42a75fc91d"


def shas(value: str = SHA) -> dict[str, str]:
    return {name: value for name in b.RUST_BINARIES}


class ArgumentTests(unittest.TestCase):
    def test_default_is_dry_run_all_components(self):
        args = b.parse_args([])
        self.assertFalse(args.build)
        self.assertEqual(args.components, ["rust", "python"])

    def test_build_requires_lock_owner(self):
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit):
            b.parse_args(["--build"])

    def test_component_selection_and_disk_floor(self):
        self.assertEqual(b.parse_args(["--component", "rust"]).components, ["rust"])
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit):
            b.parse_args(["--min-free-gib", "0"])

    def test_identity_names_release_tags_and_receipt_dir(self):
        rev = "0123456789abcdef0123456789abcdef01234567"
        ident = b.identity(rev, Path("/state"))
        self.assertEqual(ident["rust_tag"], f"qdl-v2-rust:{b.RELEASE}-0123456")
        self.assertEqual(ident["python_tag"], f"qdl-v2-python:{b.RELEASE}-0123456")
        self.assertEqual(ident["build_dir"], f"/state/build-v{b.RELEASE}-0123456")

    @unittest.skipUnless(git_has("HEAD"), "git binary or checkout unavailable: resolve_rev reads git")
    def test_resolve_rev_rejects_non_commit(self):
        with self.assertRaises(b.BuildError):
            b.resolve_rev(REPO, "no-such-ref-p184")


class GuardTests(unittest.TestCase):
    def test_lock_must_be_held_by_owner(self):
        now = dt.datetime(2026, 10, 16, 14, 0, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            lock = Path(tmp) / "lock"
            with self.assertRaisesRegex(b.BuildError, "no host lock"):
                b.check_lock(lock, "me", now)
            lock.write_text(json.dumps({"owner": "other", "until_utc": "2026-10-16T15:00:00Z"}))
            with self.assertRaisesRegex(b.BuildError, "held by 'other'"):
                b.check_lock(lock, "me", now)
            lock.write_text(json.dumps({"owner": "me", "until_utc": "2026-10-16T15:00:00Z"}))
            self.assertEqual(b.check_lock(lock, "me", now)["owner"], "me")
            with self.assertRaisesRegex(b.BuildError, "lock-owner"):
                b.check_lock(lock, None, now)

    def test_same_owner_needs_a_live_bounded_window(self):
        """Codex B1: an owned lock past, without or with a malformed until_utc never permits a build."""
        now = dt.datetime(2026, 10, 16, 14, 0, tzinfo=dt.timezone.utc)
        cases = {"expired": ({"owner": "me", "until_utc": "2026-10-16T13:59:59Z"}, "expired"),
                 "at the deadline": ({"owner": "me", "until_utc": "2026-10-16T14:00:00+00:00"}, "expired"),
                 "missing": ({"owner": "me"}, "until_utc"),
                 "malformed": ({"owner": "me", "until_utc": "soon"}, "until_utc"),
                 "naive": ({"owner": "me", "until_utc": "2026-10-16T15:00:00"}, "until_utc"),
                 "not a string": ({"owner": "me", "until_utc": 1792170000}, "until_utc"),
                 "beyond 3 h": ({"owner": "me", "until_utc": "2026-10-16T17:00:01Z"}, "maximum")}
        with tempfile.TemporaryDirectory() as tmp:
            lock = Path(tmp) / "lock"
            for name, (doc, message) in cases.items():
                with self.subTest(case=name):
                    lock.write_text(json.dumps(doc))
                    with self.assertRaisesRegex(b.BuildError, message):
                        b.check_lock(lock, "me", now)
            lock.write_text("{not json")
            with self.assertRaisesRegex(b.BuildError, "unreadable"):
                b.check_lock(lock, "me", now)

    def test_build_revalidates_the_window_at_each_expensive_stage(self):
        stages = []
        runner = mock.Mock()
        runner.run.side_effect = lambda argv, step, **kw: stages.append(("run", step)) or ""

        def guard(stage):
            stages.append(("guard", stage))
            if stage == "rust-compile":
                raise b.BuildError("host lock of 'me' expired")
        with tempfile.TemporaryDirectory() as tmp, self.assertRaisesRegex(b.BuildError, "expired"):
            b.build_rust(runner, Path(tmp), "a" * 40, "qdl-v2-rust:x", guard)
        self.assertEqual(stages, [("guard", "rust-compile")])  # the compile never started
    def test_disk_floor(self):
        usage = mock.Mock(free=10 * 2**30)
        with mock.patch.object(b.shutil, "disk_usage", return_value=usage):
            with self.assertRaises(b.BuildError):
                b.check_disk(Path("/"), 40)
            self.assertEqual(b.check_disk(Path("/"), 5), 10.0)


class RuntimeBaseTests(unittest.TestCase):
    """v2.4.0 R0: the runtime base is a pinned recipe with an apt upgrade, pinned by its full image id."""

    def test_base_recipe_upgrades_and_keeps_the_runtime_stage(self):
        recipe = b.RUST_BASE_RECIPE
        self.assertTrue(recipe.startswith(f"FROM {b.DEBIAN_BASE}\n"))
        self.assertRegex(b.DEBIAN_BASE, r"^debian@sha256:[0-9a-f]{64}$")
        self.assertIn("apt-get upgrade -y", recipe)
        for package in ("ca-certificates", "libssl3", "libzstd1", "zlib1g"):
            self.assertIn(package, recipe)
        self.assertIn("useradd --uid 10001 --gid 10001", recipe)
        dockerfile = (REPO / "Dockerfile.qdl-rust-runtime").read_text()
        self.assertIn(f"FROM {b.DEBIAN_BASE}", dockerfile)  # same debian as the runtime stage

    def test_base_is_pinned_by_full_id_and_named_in_the_build_method(self):
        self.assertRegex(b.RUST_BASE_ID, r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(b.RUST_BASE_ID, "sha256:b33ec0cb161e80f3300684f96fa956eedabc49325e401a170fb25c02d4b2eef7")
        self.assertTrue(b.BUILD_METHOD.endswith("runtime-overlay-" + b.RUST_BASE_ID[7:15]))

    def test_build_base_refuses_to_move_an_existing_tag(self):
        with mock.patch.object(b, "image_id", return_value="sha256:" + "a" * 64), mock.patch("builtins.print") as out:
            self.assertEqual(b.main(["--build-base", "qdl-v2-rust-base:x"]), 2)
        self.assertIn("never move a tag", out.call_args.args[0])


class HostClassTests(unittest.TestCase):
    """AGENTS 34b: a build is correctness work (activity record + load gate); measurement keeps the lock."""

    def test_default_class_is_correctness_and_measurement_is_explicit(self):
        self.assertEqual(b.parse_args(["--build", "--lock-owner", "me"]).klass, "correctness")
        self.assertEqual(b.parse_args(["--build", "--lock-owner", "me", "--class", "measurement"]).klass,
                         "measurement")

    def test_correctness_record_is_written_checked_and_cleared_without_any_lock(self):
        now = dt.datetime(2026, 11, 18, 14, 0, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            activity = Path(tmp) / "host-activity"
            record = b.record_activity(activity, "dl-v240-r0-build", "build", 90, now)
            self.assertEqual((record["class"], record["until_utc"]), ("correctness", "2026-11-18T15:30:00Z"))
            self.assertIn("rust_compile", record["cpu_cap"])
            self.assertEqual(b.check_activity(activity, "dl-v240-r0-build", now)["owner"], "dl-v240-r0-build")
            with self.assertRaisesRegex(b.BuildError, "ended"):
                b.check_activity(activity, "dl-v240-r0-build", now + dt.timedelta(minutes=91))
            with self.assertRaisesRegex(b.BuildError, "missing"):
                b.check_activity(activity, "someone-else", now)
            b.clear_activity(activity, "dl-v240-r0-build")
            self.assertFalse((activity / "dl-v240-r0-build.json").exists())
            for bad in (None, "../x", "a b"):
                with self.subTest(owner=bad), self.assertRaises(b.BuildError):
                    b.record_activity(activity, bad, "build", 10, now)
            with self.assertRaises(b.BuildError):
                b.record_activity(activity, "me", "build", b.MAX_LOCK_MINUTES + 1, now)

    def test_load_gate_waits_bounded_then_refuses(self):
        loads = iter([(25.0,), (21.0,), (19.5,)])
        slept = []
        self.assertEqual(b.check_load(20.0, 60, read=lambda: next(loads), sleep=slept.append,
                                      clock=lambda: 0.0), 19.5)
        self.assertEqual(len(slept), 2)
        clock = iter([0.0, 0.0, 100.0])
        with self.assertRaisesRegex(b.BuildError, "stayed"):
            b.check_load(20.0, 60, read=lambda: (30.0,), sleep=lambda _s: None, clock=lambda: next(clock))

    def test_measurement_class_still_requires_the_lock(self):
        now = dt.datetime(2026, 11, 18, 14, 0, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as tmp, self.assertRaisesRegex(b.BuildError, "no host lock"):
            b.check_lock(Path(tmp) / "lock", "me", now)

    def test_probe_and_compile_containers_are_cpu_capped(self):
        for argv in (b.probe_argv("img", "true"), b.extract_argv("sha256:" + "1" * 64, "qdl-projector"),
                     b.rust_compile_argv(Path("/s/build-v2.4.0-abc1234"), Path("/w"))):
            self.assertIn("--cpus", argv)


class CommandTests(unittest.TestCase):
    def test_rust_compile_is_offline_pinned_and_read_only_source(self):
        argv = b.rust_compile_argv(Path("/s/build-v2.2.5-abc1234"), Path("/w"))
        self.assertIn("--network", argv)
        self.assertEqual(argv[argv.index("--network") + 1], "none")
        self.assertIn("--pull=never", argv)
        self.assertIn(b.BUILDER_IMAGE, argv)
        self.assertIn("/s/build-v2.2.5-abc1234/src:/src:ro", argv)
        self.assertIn("CARGO_TARGET_DIR=/work/dl-target", argv)
        script = argv[-1]
        self.assertEqual(script.count("cargo build --offline --locked --release"), 4)
        self.assertNotIn("--features", script)

    def test_cargo_bins_match_binary_set(self):
        bins = sorted(word for cmd in b.CARGO_COMMANDS for prev, word in zip(cmd.split(), cmd.split()[1:])
                      if prev == "--bin")
        self.assertEqual(tuple(bins), tuple(sorted(b.RUST_BINARIES)))

    def test_rust_dockerfile_is_release_labelled_without_test_label(self):
        text = b.rust_dockerfile("f" * 40)
        self.assertTrue(text.startswith(f"FROM {b.RUST_BASE_TAG}\n"))
        self.assertIn(f'org.opencontainers.image.version="{b.RELEASE}"', text)
        self.assertIn(f'qdl.build.method="{b.BUILD_METHOD}"', text)
        self.assertEqual(b.BUILD_METHOD, f"cargo-cache-{b.BUILDER_IMAGE[7:15]}+runtime-overlay-b33ec0cb")
        self.assertNotIn("qdl.test.only", text)
        self.assertIn("USER 10001:10001", text)

    def test_builder_and_base_are_digest_pinned_and_follow_the_committed_recipe(self):
        for ref in (b.BUILDER_IMAGE, b.RUST_BASE_ID):
            self.assertRegex(ref, r"^sha256:[0-9a-f]{64}$")
        dockerfile = (REPO / "Dockerfile.qdl-rust-runtime").read_text()
        builder_from = dockerfile.split("FROM ", 2)[1].split()[0]
        self.assertEqual(f"FROM {builder_from}", b.BUILDER_RECIPE.splitlines()[0])
        apt = dockerfile.split("apt-get install -y --no-install-recommends", 1)[1].split("&&", 1)[0]
        self.assertEqual(tuple(apt.replace("\\", " ").split()), b.BUILDER_APT_PACKAGES)
        toolchain = (REPO / "rust-toolchain.toml").read_text()
        self.assertIn('channel = "1.82.0"', toolchain)
        self.assertIn("rustup component add --toolchain 1.82.0 clippy rustfmt", b.BUILDER_RECIPE)
        self.assertIn("FROM debian@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241", dockerfile)

    def test_rust_image_build_has_no_network_and_no_pull(self):
        argv = b.rust_image_argv(Path("/s/d"), "t")
        self.assertIn("--pull=false", argv)
        self.assertEqual(argv[argv.index("--network") + 1], "none")

    def test_probes_are_isolated(self):
        argv = b.probe_argv("img", "true")
        for flag in ("--rm", "--read-only", "--pull=never"):
            self.assertIn(flag, argv)
        self.assertEqual(argv[argv.index("--network") + 1], "none")

    def test_python_build_carries_revision_and_release(self):
        argv = b.python_image_argv(Path("/s/d"), "e" * 40, "t")
        self.assertIn("QDL_GIT_SHA=" + "e" * 40, argv)
        self.assertIn(f"QDL_RELEASE={b.RELEASE}", argv)
        self.assertIn("/s/d/src/Dockerfile", argv)


class ParseTests(unittest.TestCase):
    def test_parse_sha256sum(self):
        text = f"{SHA}  qdl-projector\n{'b' * 64}  ./qdl/x.py\n\n"
        self.assertEqual(b.parse_sha256sum(text), {"qdl-projector": SHA, "qdl/x.py": "b" * 64})
        with self.assertRaises(b.BuildError):
            b.parse_sha256sum("nothex  file\n")

    def test_runtime_probe(self):
        good = ("ldd_not_found qdl-realtime-core 0\nldd_not_found qdl-projector 0\n"
                "ldd_not_found qdl-stream-gateway 0\nldd_not_found qdl-native-raw-ingestor 0\n"
                "env_test_clock 0\nuid 10001\n")
        self.assertTrue(b.parse_runtime_probe(good)["ok"])
        self.assertFalse(b.parse_runtime_probe(good.replace("uid 10001", "uid 0"))["ok"])
        self.assertFalse(b.parse_runtime_probe(good.replace("env_test_clock 0", "env_test_clock 1"))["ok"])
        self.assertFalse(b.parse_runtime_probe(good.replace("qdl-projector 0", "qdl-projector 2"))["ok"])

    def test_feature_markers(self):
        with tempfile.TemporaryDirectory() as tmp:
            clean = Path(tmp) / "clean"
            clean.write_bytes(b"xx QDL_TEST_DATA_CLOCK_NAMESPACE TEST_DATA_CLOCK_INVALID xx")
            self.assertEqual(b.forbidden_markers(clean), [])
            dirty = Path(tmp) / "dirty"
            dirty.write_bytes(b"..p184.test-data-clock.v1../work/TEST_ONLY..")
            self.assertEqual(b.forbidden_markers(dirty), ["p184.test-data-clock.v1", "/work/TEST_ONLY"])


def elf(sections: list[tuple[str, int, int, bytes]]) -> bytes:
    """Minimal ELF64 LE file: (name, type, flags, body) sections after a NULL one, plus .shstrtab."""
    import struct
    names = b"\0" + b"".join(n.encode() + b"\0" for n, *_ in sections) + b".shstrtab\0"
    bodies, offset, rows, pos = b"", 64, [(0, 0, 0, 0, 0)], 1
    for name, kind, flags, body in sections:
        rows.append((pos, kind, flags, offset + len(bodies), len(body)))
        pos += len(name) + 1
        bodies += body
    rows.append((pos, 3, 0, offset + len(bodies), len(names)))
    bodies += names
    shoff = offset + len(bodies)
    header = b"\x7fELF\x02\x01\x01" + b"\0" * 9 + struct.pack("<HHIQQQIHHHHHH", 3, 62, 1, 0, 0, shoff, 0, 64, 56,
                                                                0, 64, len(rows), len(rows) - 1)
    table = b"".join(struct.pack("<IIQQQQIIQQ", n, k, f, 0, o, sz, 0, 0, 1, 0) for n, k, f, o, sz in rows)
    return header + bodies + table


class SectionTests(unittest.TestCase):
    def base(self, text=b"code", strtab=b"_ZN3foo17h1111E", rodata=b"/work/cargo/registry/x"):
        return elf([(".text", 1, 6, text), (".rodata", 1, 2, rodata), (".symtab", 2, 0, b"sym"),
                    (".strtab", 3, 0, strtab)])

    def test_parser_reads_names_flags_and_hashes(self):
        secs = b.elf_sections(self.base())
        self.assertEqual([s["name"] for s in secs], ["", ".text", ".rodata", ".symtab", ".strtab", ".shstrtab"])
        self.assertTrue(secs[1]["loaded"])
        self.assertFalse(secs[4]["loaded"])
        with self.assertRaises(b.BuildError):
            b.elf_sections(b"not elf" * 20)

    def test_symbol_table_only_difference_is_equal_loaded_sections(self):
        got = b.loaded_section_verdict(self.base(strtab=b"_ZN3foo17h2222E"), self.base())
        self.assertEqual(got["observed"], "EQUAL_LOADED_SECTIONS")
        self.assertEqual(got["differing_sections"], [".strtab"])

    def test_build_id_and_debug_may_differ_but_rodata_may_not(self):
        def with_note(note, rodata=b"r", debug=b"d"):
            return elf([(".note.gnu.build-id", 7, 2, note), (".text", 1, 6, b"code"), (".rodata", 1, 2, rodata),
                        (".debug_info", 1, 0, debug), (".strtab", 3, 0, b"s")])
        got = b.loaded_section_verdict(with_note(b"id2", debug=b"e"), with_note(b"id1"))
        self.assertEqual(got["observed"], "EQUAL_LOADED_SECTIONS")
        self.assertEqual(got["differing_sections"], [".note.gnu.build-id", ".debug_info"])
        self.assertEqual(b.loaded_section_verdict(with_note(b"id2", rodata=b"x"), with_note(b"id1"))["observed"],
                         "DIFFERENT")

    def test_loaded_section_difference_is_different(self):
        got = b.loaded_section_verdict(self.base(text=b"cod2", strtab=b"_ZN3foo17h2222E"), self.base())
        self.assertEqual(got["observed"], "DIFFERENT")
        self.assertIn(".text", got["differing_sections"])

    def test_build_path_classification(self):
        release = self.base(rodata=b"/work/cargo/registry/a /work/cargo/registry/b")
        reference = self.base(text=b"cod2", rodata=b"/usr/local/cargo/registry/a /usr/local/cargo/registry/b")
        got = b.build_path_verdict(release, reference)
        self.assertEqual(got["observed"], "DIFFERENT_BUILD_PATH")
        self.assertEqual(got["reference_method"], ["dockerfile"])
        # same path method, a different count, or a different .text size is not a build-path difference
        self.assertEqual(b.build_path_verdict(release, self.base(text=b"cod2",
                         rodata=b"/work/cargo/registry/a /work/cargo/registry/c"))["observed"], "DIFFERENT")
        self.assertEqual(b.build_path_verdict(release, self.base(
            rodata=b"/usr/local/cargo/registry/a"))["observed"], "DIFFERENT")
        self.assertEqual(b.build_path_verdict(release, self.base(text=b"longer code",
                         rodata=b"/usr/local/cargo/registry/a /usr/local/cargo/registry/b"))["observed"], "DIFFERENT")

    def test_extract_is_isolated_and_bounded_to_known_binaries(self):
        argv = b.extract_argv("sha256:" + "1" * 64, "qdl-projector")
        self.assertIn("--network", argv)
        self.assertIn("--read-only", argv)
        self.assertEqual(argv[-1], "/usr/local/bin/qdl-projector")
        with self.assertRaises(b.BuildError):
            b.extract_argv("sha256:" + "1" * 64, "../etc/passwd")


class VerdictTests(unittest.TestCase):
    """Fixture = the v2.3.2 rebuild: every binary differs from the production rc2 image (source or locked-dependency
    delta in every crate graph since 832c02c4)."""

    def setUp(self):
        # The fixture is rc2-only: the v2.4.4 rows against newer references are pinned by
        # SourceTests.test_v244_rows_follow_the_rust_diff_since_the_v242_build.
        rc2_only = {name: [row for row in rows if row[0] == "v230_rc2"] for name, rows in b.BINARY_EXPECTATIONS.items()}
        patcher = mock.patch.dict(b.BINARY_EXPECTATIONS, rc2_only)
        patcher.start()
        self.addCleanup(patcher.stop)

    def references(self):
        return {"v230_rc2": shas("1" * 64)}

    def release(self):
        return {name: f"{index:x}".rjust(2, "d") * 32 for index, name in enumerate(b.RUST_BINARIES)}

    def test_expected_mix_passes(self):
        verdict = b.binary_verdicts(self.release(), self.references())
        self.assertTrue(verdict["ok"], verdict["unexpected"])
        # The core carries the v2.3.1 dependency refresh: it is not byte-identical to the production core.
        self.assertFalse(verdict["core_equal_to_measured"])

    def test_a_binary_equal_to_production_is_unexpected(self):
        rel = self.release()
        rel["qdl-realtime-core"] = self.references()["v230_rc2"]["qdl-realtime-core"]
        verdict = b.binary_verdicts(rel, self.references())
        self.assertFalse(verdict["ok"])
        self.assertTrue(verdict["core_equal_to_measured"])
        self.assertEqual({r["binary"] for r in verdict["unexpected"]}, {"qdl-realtime-core"})

    def test_section_expectation_needs_an_analysis(self):
        rows = {"qdl-stream-gateway": [("v230_rc2", "DIFFERENT_BUILD_PATH", "fixture")]}
        with mock.patch.dict(b.BINARY_EXPECTATIONS, rows):
            verdict = b.binary_verdicts(self.release(), self.references())
            self.assertEqual([(r["binary"], r["reference"], r["observed"]) for r in verdict["unexpected"]],
                             [("qdl-stream-gateway", "v230_rc2", "NOT_ANALYSED")])
            ok = {("qdl-stream-gateway", "v230_rc2"): {"observed": "DIFFERENT_BUILD_PATH"}}
            self.assertTrue(b.binary_verdicts(self.release(), self.references(), ok)["ok"])
            bad = {("qdl-stream-gateway", "v230_rc2"): {"observed": "DIFFERENT"}}
            self.assertFalse(b.binary_verdicts(self.release(), self.references(), bad)["ok"])

    def test_loaded_sections_expectation_is_satisfied_by_whole_file_equal(self):
        rows = {"qdl-projector": [("v230_rc2", "EQUAL_LOADED_SECTIONS", "fixture")]}
        with mock.patch.dict(b.BINARY_EXPECTATIONS, rows):
            rel = self.release()
            self.assertEqual([(r["binary"], r["observed"]) for r in
                              b.binary_verdicts(rel, self.references())["unexpected"]],
                             [("qdl-projector", "NOT_ANALYSED")])
            rel["qdl-projector"] = "1" * 64
            self.assertTrue(b.binary_verdicts(rel, self.references())["ok"])

    def test_missing_binary_or_unread_reference_fails(self):
        rel = self.release()
        del rel["qdl-kafka-smoke"]
        self.assertEqual(b.binary_verdicts(rel, self.references())["missing"], ["qdl-kafka-smoke"])
        refs = self.references()
        del refs["v230_rc2"]["qdl-projector"]
        unexpected = b.binary_verdicts(self.release(), refs)["unexpected"]
        self.assertEqual([(r["binary"], r["observed"]) for r in unexpected], [("qdl-projector", "NOT_READ")])

    def test_every_binary_has_an_expectation_and_production_refs_are_declared(self):
        self.assertEqual(set(b.BINARY_EXPECTATIONS), set(b.RUST_BINARIES))
        for rows in b.BINARY_EXPECTATIONS.values():
            for ref, expected, _ in rows:
                self.assertIn(ref, b.REFERENCES)
                self.assertIn(expected, b.EXPECTATIONS)
        self.assertEqual(b.REFERENCES[b.PRODUCTION_RUST_REFERENCE][1],
                         "sha256:58bdd28216139f0c1a40abeee3077834f1cf922cf6412e7fb1ce8845591a0512")

    def test_python_file_verdict(self):
        ref = {"qdl/a.py": "1", "qdl/b.py": "2", "app/c.py": "3"}
        rel = {"qdl/a.py": "1", "qdl/b.py": "9", "app/c.py": "3", "qdl/new.py": "4"}
        ok = b.python_file_verdict(rel, ref, ["qdl/b.py", "qdl/new.py"])
        self.assertTrue(ok["ok"])
        bad = b.python_file_verdict(rel, ref, ["qdl/b.py"])
        self.assertFalse(bad["ok"])
        self.assertEqual(bad["unexpected_different"], ["qdl/new.py"])
        missing = b.python_file_verdict(rel, ref, ["qdl/b.py", "qdl/new.py", "app/c.py"])
        self.assertEqual(missing["expected_but_equal"], ["app/c.py"])

    def test_freeze_verdict_lists_differences(self):
        verdict = b.freeze_verdict("a==1\nb==2\n", "a==1\nb==3\n")
        self.assertFalse(verdict["equal"])
        self.assertEqual(verdict["only_release"], ["b==2"])
        self.assertEqual(verdict["only_reference"], ["b==3"])


@unittest.skipUnless(git_has("HEAD"), "git binary or checkout unavailable: these tests read this repository's git history")
class SourceTests(unittest.TestCase):
    """git reads of this repository at HEAD (the release source carries the SDK bump)."""

    def head(self):
        return subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"]).decode().strip()

    def test_head_meets_source_preconditions(self):
        result = b.source_preconditions(REPO, self.head())
        self.assertTrue(result["ok"], result["problems"])

    @unittest.skipUnless(git_has(PRE_BUMP_REV), "pre-bump revision not in this (shallow?) checkout")
    def test_pre_bump_source_is_refused(self):
        result = b.source_preconditions(REPO, "87db2159f411e7c4d7d49033e7573b42a75fc91d")
        self.assertFalse(result["ok"])
        self.assertTrue(any(b.SDK_VERSION in p for p in result["problems"]))

    @unittest.skipUnless(git_has(*(tree for _, _, tree in b.PYTHON_REFERENCES.values())),
                         "reference revision not in this (shallow?) checkout")
    def test_expected_python_diff_is_the_git_diff_against_each_production_tree(self):
        self.assertEqual({ref_id for _, ref_id, _ in b.PYTHON_REFERENCES.values()},
                         {"sha256:5ea7a7a40ef2d1df920d0f7cf94f22431d53087f8002929e7dffdb4208f7dcc4",
                          "sha256:396a4654014758f70362b3097559e03dcb412d36904d94d3cb7c973f6ccb1637"})
        for _, _, tree in b.PYTHON_REFERENCES.values():
            diff = b.expected_python_diff(REPO, self.head(), tree)
            expected = subprocess.check_output(["git", "-C", str(REPO), "diff", "--name-only", "--no-renames",
                                                tree, self.head(), "--", *b.PYTHON_HASH_ROOTS]).decode().split()
            self.assertEqual(diff, sorted(expected))

    @unittest.skipUnless(git_has(b.PRODUCTION_RUST_REVISION), "rc2 revision not in this (shallow?) checkout")
    def test_every_binary_has_a_rust_delta_since_the_production_image(self):
        """DIFFERENT against rc2 is meaningful only if each binary's package graph changed since 832c02c4."""
        import tomllib
        old = tomllib.loads(subprocess.check_output(["git", "-C", str(REPO), "show",
                                                     b.PRODUCTION_RUST_REVISION + ":Cargo.lock"]).decode())
        new = tomllib.loads(subprocess.check_output(["git", "-C", str(REPO), "show", self.head() + ":Cargo.lock"]).decode())
        before = {p["name"]: p["version"] for p in old["package"]}
        deps = {p["name"]: [d.split()[0] for d in p.get("dependencies", [])] for p in new["package"]}
        changed = {p["name"] for p in new["package"] if before.get(p["name"]) != p["version"]}
        for crate in ("qdl-contracts", "qdl-stream-gateway"):
            if subprocess.check_output(["git", "-C", str(REPO), "diff", "--name-only", b.PRODUCTION_RUST_REVISION,
                                        self.head(), "--", f"rust/{crate}/src"]).strip():
                changed.add(crate)

        def closure(name, seen):
            for dep in deps.get(name, []):
                if dep not in seen:
                    seen.add(dep)
                    closure(dep, seen)
            return seen
        for binary, package in b.BINARY_PACKAGE.items():
            with self.subTest(binary=binary):
                self.assertTrue(closure(package, {package}) & changed, f"{binary}: no delta, DIFFERENT is not justified")
                rc2 = [e for ref, e, _ in b.BINARY_EXPECTATIONS[binary] if ref == b.PRODUCTION_RUST_REFERENCE]
                self.assertEqual(rc2, ["DIFFERENT"])

    def test_v244_rows_follow_the_rust_diff_since_the_v242_build(self):
        """DL v2.4.4: only the ingestor and the stream gateway changed since 48137eb; every other binary is expected
        equal (loaded sections) to the v2.4.2 rust build, and the two changed ones differ from their role image."""
        changed = subprocess.check_output(["git", "-C", str(REPO), "diff", "--name-only", "48137ebf952966e4c8d31c64060791087535fbdc",
                                           self.head(), "--", "rust", "Cargo.toml", "Cargo.lock", "generated/rust"]).decode().split()
        self.assertTrue(changed)
        self.assertTrue(all(p == "rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs"
                            or p.startswith("rust/qdl-stream-gateway/") for p in changed), changed)
        for binary, rows in b.BINARY_EXPECTATIONS.items():
            with self.subTest(binary=binary):
                newer = [(ref, e) for ref, e, _ in rows if ref != b.PRODUCTION_RUST_REFERENCE]
                if binary == "qdl-stream-gateway":
                    self.assertEqual(newer, [("v240_streams", "DIFFERENT")])
                elif binary == "qdl-native-raw-ingestor":
                    self.assertEqual(newer, [("v242_ingestors", "DIFFERENT")])
                else:
                    self.assertEqual(newer, [("v242_ingestors", "EQUAL_LOADED_SECTIONS")])

    def test_plan_is_dry(self):
        with mock.patch.object(b.subprocess, "run", wraps=subprocess.run) as run:
            result = b.plan(REPO, self.head(), ["rust", "python"], Path("/state"))
        self.assertFalse(any(call.args and call.args[0][:1] == ["docker"] for call in run.call_args_list))
        self.assertEqual(result["identity"]["build_dir"], f"/state/build-v{b.RELEASE}-{self.head()[:7]}")
        self.assertTrue(result["source_preconditions"]["ok"])


if __name__ == "__main__":
    unittest.main()
