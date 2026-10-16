"""Pass watch: cleaner-log pass parsing (real 4.2 line shapes), partition-dir listing, consumer-group describe
parsing, and the compare verdict (offsets may advance, never disappear or decrease)."""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import kafka_offsets_pass_watch as watch

P = "__consumer_offsets-43"
LOG_DIR = "Log(dir=/var/lib/kafka/data/__consumer_offsets-43, topicId=DJzF, topic=__consumer_offsets, partition=43)"
CLEANER = f"""[2026-10-16 11:02:35,700] INFO Cleaner 0: Beginning cleaning of log {P} (org.apache.kafka.storage.internals.log.Cleaner)
[2026-10-16 11:02:35,717] INFO Cleaner 0: Building offset map for log {P} for 105 segments in offset range [351590308, 361115496). (x)
[2026-10-16 11:06:25,034] INFO Cleaner 0: Swapping in cleaned segment LogSegment(baseOffset=0, size=0, lastModifiedTime=1, largestRecordTimestamp=-1) for segment(s) [LogSegment(baseOffset=0, size=29327464)] in log {LOG_DIR} (x)
[2026-10-16 11:07:46,091] INFO Cleaner 0: Swapping in cleaned segment LogSegment(baseOffset=323936729, size=57908318, lastModifiedTime=1, largestRecordTimestamp=1) for segment(s) [LogSegment(baseOffset=323936729, size=87457018)] in log {LOG_DIR} (x)
[2026-10-16 11:08:00,000] INFO Cleaner 0: Swapping in cleaned segment LogSegment(baseOffset=5, size=0, lastModifiedTime=1, largestRecordTimestamp=-1) for segment(s) [LogSegment(baseOffset=5, size=9)] in log Log(dir=/var/lib/kafka/data/__consumer_offsets-4, topicId=x) (x)
[2026-10-16 11:18:17,254] INFO [kafka-log-cleaner-thread-0]:
\tLog cleaner thread 0 cleaned log {P} (dirty section = [351590308, 361115496])
\t1,865.5 MB of log processed in 941.6 seconds (2.0 MB/sec).
\tStart size: 1,865.5 MB (18,732,843 messages)
\tEnd size: 965.7 MB (12,564,250 messages)
\t48.2% size reduction (32.9% fewer messages)
 (org.apache.kafka.storage.internals.log.LogCleaner$CleanerThread)
[2026-10-16 11:18:17,274] INFO Cleaner 0: Beginning cleaning of log __transaction_state-6 (x)
[2026-10-16 11:18:20,000] INFO [kafka-log-cleaner-thread-0]:
\tLog cleaner thread 0 cleaned log __transaction_state-6 (dirty section = [1, 2])
\tStart size: 9.0 MB (10 messages)
\tEnd size: 1.0 MB (1 messages)
[2026-10-16 12:20:00,000] INFO Cleaner 0: Beginning cleaning of log {P} (x)
"""

DIR = """00000000000000000000.log 0 1791943358
00000000000317807915.log 0 1791958410
00000000000323936729.log 57908318 1791986795
00000000000360774317.log 9227691 1792148541
#cleaner
0
2
__consumer_offsets 43 361115496
__consumer_offsets 4 100
#start
0
1
__consumer_offsets 43 0
"""

OFFSETS = """
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
core            md.raw          0          100             120             20              c-1             /10.0.0.5       rdkafka
core            md.raw          1          200             200             0               c-2             /10.0.0.6       rdkafka

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
idle            md.out          0          7               9               2               -               -               -
idle            md.out          1          -               9               -               -               -               -
"""

STATES = """
GROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS
core            kafka2:9092  (2)          range                Stable               3

GROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS
idle            kafka1:9092  (1)          -                    Empty                0
"""


def snap(at, offsets, states=None, passes=(), size=100):
    replica = {"size_bytes": size, "segment_count": 2, "empty_segments": 0, "cleaner_checkpoint": 1,
               "log_start_checkpoint": 0, "passes": list(passes)}
    return {"partition": P, "at_utc": at, "replicas": {"1": dict(replica)},
            "group_offsets": offsets, "group_states": states or {}}


class CleanerLog(unittest.TestCase):
    def test_finished_pass_then_open_pass_only_for_the_partition(self):
        passes = watch.parse_cleaner_passes(CLEANER, P)
        self.assertEqual(len(passes), 2)
        done, running = passes
        self.assertEqual(done["started_utc"], "2026-10-16T11:02:35.700000+00:00")
        self.assertEqual(done["finished_utc"], "2026-10-16T11:18:17.254000+00:00")
        self.assertEqual(done["dirty_section"], [351590308, 361115496])
        self.assertEqual((done["start_mb"], done["start_messages"]), (1865.5, 18732843))
        self.assertEqual((done["end_mb"], done["end_messages"]), (965.7, 12564250))
        self.assertEqual((done["swaps"], done["emptied_segments"]), (2, 1))  # partition 4's swap is not counted
        self.assertIsNone(running["finished_utc"])
        self.assertEqual(running["started_utc"], "2026-10-16T12:20:00+00:00")

    def test_other_partition_summary_never_closes_this_pass(self):
        text = CLEANER.split("[2026-10-16 11:18:17,254]")[0] + (
            "[2026-10-16 11:18:20,000] INFO [kafka-log-cleaner-thread-0]: \n"
            "\tLog cleaner thread 0 cleaned log __consumer_offsets-4 (dirty section = [1, 2])\n"
            "\tStart size: 9.0 MB (10 messages)\n")
        (only,) = watch.parse_cleaner_passes(text, P)
        self.assertIsNone(only["finished_utc"])
        self.assertIsNone(only["start_mb"])

    def test_hourly_files_oldest_first_then_live(self):
        now = dt.datetime(2026, 10, 16, 12, 30, tzinfo=dt.timezone.utc)
        self.assertEqual(watch.cleaner_files(now, 2), [
            "/opt/kafka/logs/log-cleaner.log.2026-10-16-10", "/opt/kafka/logs/log-cleaner.log.2026-10-16-11",
            "/opt/kafka/logs/log-cleaner.log"])


class PartitionDir(unittest.TestCase):
    def test_segments_totals_and_checkpoints(self):
        body = watch.parse_partition_dir(DIR, P)
        self.assertEqual(body["segment_count"], 4)
        self.assertEqual(body["empty_segments"], 2)
        self.assertEqual(body["size_bytes"], 57908318 + 9227691)
        self.assertEqual(body["cleaner_checkpoint"], 361115496)  # partition 4's line is ignored
        self.assertEqual(body["log_start_checkpoint"], 0)

    def test_reader_is_read_only_and_networkless(self):
        argv = watch.dir_argv(2, P)
        self.assertEqual(argv[:3], ["docker", "run", "--rm"])
        self.assertIn("none", argv)
        self.assertIn("--read-only", argv)
        self.assertIn("qdl_v2_stable_candidate_kafka2_data:/data:ro", argv)

    def test_partition_name_is_validated(self):
        with self.assertRaises(watch.Refused):
            watch.dir_argv(1, "__consumer_offsets-43; rm -rf /")
        with self.assertRaises(watch.Refused):
            watch.dir_argv(1, "md.latest.v2-0")


class Groups(unittest.TestCase):
    def test_offsets_with_missing_value(self):
        self.assertEqual(watch.parse_group_offsets(OFFSETS), {
            "core": {"md.raw:0": 100, "md.raw:1": 200}, "idle": {"md.out:0": 7, "md.out:1": None}})

    def test_states_read_from_both_ends(self):
        self.assertEqual(watch.parse_group_states(STATES), {
            "core": {"state": "Stable", "members": 3, "coordinator": "2"},
            "idle": {"state": "Empty", "members": 0, "coordinator": "1"}})


class Compare(unittest.TestCase):
    BEFORE = {"core": {"md.raw:0": 100, "md.raw:1": 200}, "idle": {"md.out:0": 7, "md.out:1": None}}

    def test_identical_or_advanced_is_ok(self):
        after = {"core": {"md.raw:0": 150, "md.raw:1": 200}, "idle": {"md.out:0": 7}, "new": {"x:0": 1}}
        self.assertEqual(watch.compare_offsets(self.BEFORE, after), [])

    def test_disappeared_decreased_and_lost_group(self):
        after = {"core": {"md.raw:0": 99}}
        kinds = {(v["group"], v["kind"]) for v in watch.compare_offsets(self.BEFORE, after)}
        self.assertEqual(kinds, {("core", "offset_decreased"), ("core", "offset_disappeared"),
                                 ("idle", "group_disappeared")})

    def test_group_without_any_committed_offset_may_vanish(self):
        self.assertEqual(watch.compare_offsets({"g": {"t:0": None}}, {}), [])

    def test_report_pass_window_sizes_and_verdict(self):
        old_pass = {"started_utc": "2026-10-16T11:02:35+00:00", "finished_utc": "2026-10-16T11:18:17+00:00"}
        new_pass = {"started_utc": "2026-10-16T12:20:00+00:00", "finished_utc": "2026-10-16T12:40:00+00:00"}
        before = snap("2026-10-16T12:15:00+00:00", self.BEFORE, {"core": {"state": "Stable"}},
                      passes=[old_pass], size=1000)
        after = snap("2026-10-16T12:45:00+00:00", {"core": {"md.raw:0": 120, "md.raw:1": 200},
                                                   "idle": {"md.out:0": 7}},
                     {"core": {"state": "PreparingRebalance"}}, passes=[old_pass, new_pass], size=400)
        report = watch.compare(before, after)
        self.assertTrue(report["ok"])
        self.assertTrue(report["pass_finished_on_all_replicas"])
        self.assertEqual(report["replicas"]["1"]["passes_in_window"], [new_pass])
        self.assertEqual(report["replicas"]["1"]["size_bytes"]["delta"], -600)
        self.assertEqual(report["offsets_advanced"], 1)
        self.assertEqual(report["group_state_changes"], {"core": {"before": "Stable", "after": "PreparingRebalance"}})
        after["group_offsets"]["idle"] = {}
        self.assertFalse(watch.compare(before, after)["ok"])

    def test_pass_before_the_snapshot_is_not_counted(self):
        old_pass = {"started_utc": "2026-10-16T11:02:35+00:00", "finished_utc": "2026-10-16T11:18:17+00:00"}
        before = snap("2026-10-16T12:15:00+00:00", {}, passes=[old_pass])
        report = watch.compare(before, snap("2026-10-16T12:45:00+00:00", {}, passes=[old_pass]))
        self.assertFalse(report["replicas"]["1"]["pass_started"])
        self.assertFalse(report["pass_finished_on_all_replicas"])


class Main(unittest.TestCase):
    def _main(self, argv):
        err, out = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(out):
            code = watch.main(argv)
        return code, out.getvalue(), err.getvalue()

    def test_snapshot_refuses_a_foreign_lock_before_any_container(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(watch, "foreign_lock", return_value={"owner": "other"}) as lock, \
                mock.patch.object(watch, "snapshot") as snapshot:
            code, _, err = self._main(["--snapshot", f"{tmp}/s.json", "--cert-dir", tmp, "--lock-owner", "me"])
        self.assertEqual(code, 4)
        self.assertIn("lock", err)
        lock.assert_called_once_with(own_owner="me")
        snapshot.assert_not_called()

    def test_snapshot_needs_cert_dir_and_never_overwrites(self):
        self.assertEqual(self._main(["--snapshot", "/nonexistent/s.json"])[0], 4)
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(watch, "foreign_lock", return_value=None):
            Path(tmp, "s.json").write_text("{}")
            self.assertEqual(self._main(["--snapshot", f"{tmp}/s.json", "--cert-dir", tmp])[0], 4)

    def test_compare_exit_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            b, a = Path(tmp, "b.json"), Path(tmp, "a.json")
            b.write_text(json.dumps(snap("2026-10-16T12:00:00+00:00", {"g": {"t:0": 5}})))
            a.write_text(json.dumps(snap("2026-10-16T12:30:00+00:00", {"g": {"t:0": 6}})))
            self.assertEqual(self._main(["--compare", str(b), str(a)])[0], 0)
            a.write_text(json.dumps(snap("2026-10-16T12:30:00+00:00", {"g": {"t:0": 4}})))
            self.assertEqual(self._main(["--compare", str(b), str(a)])[0], 1)


if __name__ == "__main__":
    unittest.main()
