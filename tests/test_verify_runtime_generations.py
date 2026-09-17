"""Tests for the configuration-drift checker.

Every case is one of the drifts actually found on 2026-09-17, or one of the
false positives the first version of this checker produced. Both matter: a
checker that cries wolf is worse than none, because the next real finding is
read as noise.

Docker is stubbed at the three seams the script uses, so these run anywhere.
"""

from __future__ import annotations

import json
import unittest
from unittest import mock

from scripts import verify_runtime_generations as verify


def report() -> verify.Report:
    return verify.Report()


class CpuCeilingTests(unittest.TestCase):
    """Compose's declared ceiling against the cgroup quota actually in force."""

    def test_a_live_docker_update_is_reported(self) -> None:
        # The exact drift this session created: projectors raised to 1.00 with
        # `docker update` while compose still said 0.50, so the next recreate
        # would have silently halved them.
        r = report()
        with mock.patch.object(verify, "_exec", return_value="100000 100000"):
            verify.check_cpu_ceilings(r, {"projector_v2": {"cpus": 0.50}}, ["projector_v2"])
        self.assertEqual(r.failed, 1)
        self.assertIn("compose 0.50, running 1.00", r.findings[0].detail)

    def test_agreement_passes(self) -> None:
        r = report()
        with mock.patch.object(verify, "_exec", return_value="125000 100000"):
            verify.check_cpu_ceilings(r, {"kafka2": {"cpus": 1.25}}, ["kafka2"])
        self.assertEqual(r.failed, 0)
        self.assertEqual(r.findings[0].status, verify.OK)

    def test_a_role_without_a_quota_is_skipped_not_passed(self) -> None:
        r = report()
        with mock.patch.object(verify, "_exec", return_value="max 100000"):
            verify.check_cpu_ceilings(r, {"x": {"cpus": 1.0}}, ["x"])
        self.assertEqual(r.findings[0].status, verify.SKIP)
        self.assertEqual(r.failed, 0)

    def test_a_role_compose_does_not_declare_is_skipped(self) -> None:
        r = report()
        verify.check_cpu_ceilings(r, {"x": {}}, ["x"])
        self.assertEqual(r.findings[0].status, verify.SKIP)

    def test_an_unreadable_container_is_skipped_not_passed(self) -> None:
        # An unreadable subject is not evidence of agreement.
        r = report()
        with mock.patch.object(verify, "_exec", side_effect=RuntimeError("no such container")):
            verify.check_cpu_ceilings(r, {"x": {"cpus": 1.0}}, ["x"])
        self.assertEqual(r.findings[0].status, verify.SKIP)
        self.assertEqual(r.failed, 0)


class ComposeChainTests(unittest.TestCase):
    """Whether replaying a role's own chain would move the image it runs."""

    def test_a_chain_naming_a_deleted_file_fails(self) -> None:
        # stable_redis records a compose file from a worktree that was removed,
        # so it cannot be recreated from its own chain at all.
        r = report()
        with mock.patch.object(
            verify, "_inspect",
            side_effect=["/gone/docker-compose.v2-stable.yml", "sha256:abc"],
        ):
            verify.check_compose_chain(r, ["stable_redis"])
        self.assertEqual(r.failed, 1)
        self.assertIn("no longer exist", r.findings[0].detail)

    def test_a_tag_that_resolves_to_the_running_digest_passes(self) -> None:
        # The first version compared a tag to a digest and reported both stream
        # roles as drifted against their own image.
        r = report()
        resolved = "services:\n  stream_v2_active:\n    image: qdl-v2-python:2.0.16-df4b8aa\n"
        with mock.patch.object(verify, "_inspect", side_effect=["/tmp/a.yml", "sha256:3c1af2c7"]), \
             mock.patch("pathlib.Path.is_file", return_value=True), \
             mock.patch.object(verify, "_run", side_effect=[resolved, "sha256:3c1af2c7\n"]):
            verify.check_compose_chain(r, ["stream_v2_active"])
        self.assertEqual(r.failed, 0)
        self.assertEqual(r.findings[0].status, verify.OK)

    def test_a_chain_resolving_to_a_different_image_fails(self) -> None:
        # The R1 trap: a chain missing the stream image override resolves 2.0.12.
        r = report()
        resolved = "services:\n  stream_v2_active:\n    image: qdl-v2-python:2.0.12-d8cb75b\n"
        with mock.patch.object(verify, "_inspect", side_effect=["/tmp/a.yml", "sha256:3c1af2c7"]), \
             mock.patch("pathlib.Path.is_file", return_value=True), \
             mock.patch.object(verify, "_run", side_effect=[resolved, "sha256:8bd10da6\n"]):
            verify.check_compose_chain(r, ["stream_v2_active"])
        self.assertEqual(r.failed, 1)
        self.assertIn("2.0.12", r.findings[0].detail)

    def test_a_chain_compose_cannot_resolve_is_skipped(self) -> None:
        r = report()
        with mock.patch.object(verify, "_inspect", side_effect=["/tmp/a.yml", "sha256:abc"]), \
             mock.patch("pathlib.Path.is_file", return_value=True), \
             mock.patch.object(verify, "_run", side_effect=RuntimeError("needs an env file")):
            verify.check_compose_chain(r, ["x"])
        self.assertEqual(r.findings[0].status, verify.SKIP)
        self.assertEqual(r.failed, 0)


class BarCheckpointTests(unittest.TestCase):
    """The stranded-checkpoint failure, caught before the restart instead of by it."""

    ENV = "\n".join((
        "QDL_STABLE_BAR_STATE_PATH=/var/lib/qdl-stable/runtime/state.json",
        "QDL_STABLE_SOURCE_BINDINGS=/app/config/v2/stable-source-bindings.yaml",
    ))

    def test_a_stranded_checkpoint_fails_before_it_crash_loops(self) -> None:
        r = report()
        with mock.patch.object(verify, "_inspect", return_value=self.ENV), \
             mock.patch.object(verify, "_exec", side_effect=[json.dumps({"catalog_revision": 7}), "8\n"]):
            verify.check_bar_edge_checkpoint(r)
        self.assertEqual(r.failed, 1)
        self.assertIn("crash-loop", r.findings[0].detail)
        self.assertIn("7", r.findings[0].detail)

    def test_a_matching_checkpoint_passes(self) -> None:
        r = report()
        with mock.patch.object(verify, "_inspect", return_value=self.ENV), \
             mock.patch.object(verify, "_exec", side_effect=[json.dumps({"catalog_revision": 8}), "8\n"]):
            verify.check_bar_edge_checkpoint(r)
        self.assertEqual(r.failed, 0)

    def test_no_checkpoint_yet_is_not_a_failure(self) -> None:
        # A fresh state path has no file; the next start bootstraps, which is
        # the designed path, not drift.
        r = report()
        with mock.patch.object(verify, "_inspect", return_value=self.ENV), \
             mock.patch.object(verify, "_exec", side_effect=RuntimeError("No such file")):
            verify.check_bar_edge_checkpoint(r)
        self.assertEqual(r.failed, 0)
        self.assertEqual(r.findings[0].status, verify.OK)

    def test_a_role_without_a_bar_state_path_is_skipped(self) -> None:
        r = report()
        with mock.patch.object(verify, "_inspect", return_value="FOO=bar"):
            verify.check_bar_edge_checkpoint(r)
        self.assertEqual(r.findings[0].status, verify.SKIP)


class BarOwnerTests(unittest.TestCase):
    """Ownership proved by observation, because the mode rule reports false alarms."""

    def test_a_venue_whose_bars_stopped_fails(self) -> None:
        # OKX: 70 partitions, newest record over half an hour old, because the
        # catalog moved them to the Rust path and the Rust runtime config never
        # declared them.
        r = report()
        payload = json.dumps({"okx": [70, 2198.0, 3000.0], "binance": [70, 27.0, 40.0]})
        with mock.patch.object(verify, "_exec", return_value=payload):
            verify.check_bar_owners(r)
        by_subject = {f.subject: f for f in r.findings}
        self.assertEqual(by_subject["okx"].status, verify.FAIL)
        self.assertEqual(by_subject["binance"].status, verify.OK)
        self.assertIn("no component is publishing", by_subject["okx"].detail)

    def test_the_mode_based_rule_would_have_flagged_a_working_venue(self) -> None:
        # Binance bars are RUST_NATIVE in the catalog and are published anyway,
        # because the bar edge filters by mode only for OKX. Observation gets
        # this right where inference did not.
        r = report()
        payload = json.dumps({"binance": [70, 27.0, 40.0]})
        with mock.patch.object(verify, "_exec", return_value=payload):
            verify.check_bar_owners(r)
        self.assertEqual(r.failed, 0)

    def test_the_staleness_bound_is_configurable(self) -> None:
        r = report()
        payload = json.dumps({"okx": [70, 700.0, 800.0]})
        with mock.patch.object(verify, "_exec", return_value=payload):
            verify.check_bar_owners(r, stale_seconds=1200)
        self.assertEqual(r.failed, 0)

    def test_an_unreadable_spool_is_skipped(self) -> None:
        r = report()
        with mock.patch.object(verify, "_exec", side_effect=RuntimeError("disk is full")):
            verify.check_bar_owners(r)
        self.assertEqual(r.findings[0].status, verify.SKIP)
        self.assertEqual(r.failed, 0)


class SpoolHeadroomTests(unittest.TestCase):
    """The bound that failed every canonical write closed on 2026-09-17."""

    def test_a_spool_near_the_physical_bound_fails(self) -> None:
        # Expressed against the declared bound, not a byte count, so raising the
        # bound cannot silently turn this check into a pass.
        r = report()
        main = int(verify.SPOOL_MAX_STORAGE_BYTES * 0.9)
        payload = json.dumps([main, 16 * 1024 * 1024, 1900544])
        with mock.patch.object(verify, "_exec", return_value=payload):
            verify.check_spool_headroom(r)
        finding = r.findings[0]
        self.assertEqual(finding.status, verify.FAIL)
        self.assertIn("fail closed", finding.detail)

    def test_a_spool_with_headroom_passes(self) -> None:
        r = report()
        main = int(verify.SPOOL_MAX_STORAGE_BYTES * 0.4)
        payload = json.dumps([main, 33554432, 1900544])
        with mock.patch.object(verify, "_exec", return_value=payload):
            verify.check_spool_headroom(r)
        self.assertEqual(r.findings[0].status, verify.OK)
        self.assertEqual(r.failed, 0)

    def test_a_wal_far_past_its_declared_limit_fails_before_the_bound_does(self) -> None:
        # The WAL is the part that ran away; catching it early is the point.
        r = report()
        payload = json.dumps([104857600, verify.JOURNAL_SIZE_LIMIT_BYTES * 5, 1048576])
        with mock.patch.object(verify, "_exec", return_value=payload):
            verify.check_spool_headroom(r)
        finding = r.findings[0]
        self.assertEqual(finding.status, verify.FAIL)
        self.assertIn("journal_size_limit", finding.detail)

    def test_an_unreadable_spool_is_skipped_not_passed(self) -> None:
        r = report()
        with mock.patch.object(verify, "_exec", side_effect=RuntimeError("no such role")):
            verify.check_spool_headroom(r)
        self.assertEqual(r.findings[0].status, verify.SKIP)
        self.assertEqual(r.failed, 0)

    def test_the_warn_ratio_is_configurable(self) -> None:
        r = report()
        payload = json.dumps([verify.SPOOL_MAX_STORAGE_BYTES // 2, 0, 0])
        with mock.patch.object(verify, "_exec", return_value=payload):
            verify.check_spool_headroom(r, warn_ratio=0.4)
        self.assertEqual(r.findings[0].status, verify.FAIL)


class BarOwnerQueryShapeTests(unittest.TestCase):
    """The check must not scan the spool to answer a liveness question."""

    def test_the_probe_seeks_by_key_instead_of_grouping_the_table(self) -> None:
        r = report()
        captured = {}

        def fake_exec(role, *command):
            captured["script"] = command[-1]
            return json.dumps({"okx": [70, 14.0, 900.0]})

        with mock.patch.object(verify, "_exec", side_effect=fake_exec):
            verify.check_bar_owners(r)
        script = captured["script"]
        self.assertIn("from partitions", script)
        self.assertIn("order by logical_offset desc limit 1", script)
        self.assertNotIn("group by", script.lower())
        self.assertNotIn("max(committed_at_ns)", script.lower())
        self.assertEqual(r.failed, 0)


class ReportTests(unittest.TestCase):
    def test_skips_never_fail_the_run(self) -> None:
        r = report()
        r.add("c", "s", verify.SKIP, "")
        r.add("c", "s2", verify.OK, "")
        self.assertEqual(r.failed, 0)
        self.assertEqual(r.skipped, 1)

    def test_one_failure_fails_the_run(self) -> None:
        r = report()
        r.add("c", "s", verify.FAIL, "")
        self.assertEqual(r.failed, 1)


if __name__ == "__main__":
    unittest.main()
